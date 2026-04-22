-- Bot admin users snapshot v2
-- Keeps Users search/filter/sort inside SQL/RPC path instead of JS fallback.

create extension if not exists pg_trgm with schema extensions;

alter table public.users
  add column if not exists first_name text;

alter table public.users
  add column if not exists last_name text;

create index if not exists users_username_trgm_idx
  on public.users
  using gin ((lower(coalesce(username, ''))) extensions.gin_trgm_ops);

create index if not exists users_display_name_trgm_idx
  on public.users
  using gin ((
    lower(
      coalesce(
        nullif(
          btrim(
            coalesce(first_name, '') || ' ' || coalesce(last_name, '')
          ),
          ''
        ),
        ''
      )
    )
  ) extensions.gin_trgm_ops);

create index if not exists users_user_id_text_trgm_idx
  on public.users
  using gin (((user_id)::text) extensions.gin_trgm_ops);

create or replace function public.admin_bot_users_snapshot_page_v2(
  p_page integer default 1,
  p_page_size integer default 50,
  p_search text default null,
  p_filter text default 'all',
  p_sort text default 'newest'
)
returns jsonb
language plpgsql
security definer
set search_path = public, extensions
as $$
declare
  v_page integer := greatest(1, coalesce(p_page, 1));
  v_page_size integer := greatest(1, least(coalesce(p_page_size, 50), 200));
  v_search text := lower(coalesce(nullif(btrim(p_search), ''), ''));
  v_search_no_at text := regexp_replace(v_search, '^@+', '');
  v_filter text := lower(coalesce(nullif(btrim(p_filter), ''), 'all'));
  v_sort text := lower(coalesce(nullif(btrim(p_sort), ''), 'newest'));
  v_result jsonb;
begin
  if not (auth.role() = 'service_role' or public.is_admin()) then
    raise exception 'forbidden';
  end if;

  if v_filter not in ('all', 'with_revenue', 'without_revenue', 'with_orders') then
    v_filter := 'all';
  end if;

  if v_sort not in (
    'newest',
    'oldest',
    'username_asc',
    'username_desc',
    'revenue_desc',
    'revenue_asc',
    'order_count_desc',
    'order_count_asc'
  ) then
    v_sort := 'newest';
  end if;

  with order_stats as (
    select
      o.user_id,
      count(*)::bigint as order_count,
      coalesce(sum(coalesce(o.price, 0)), 0)::bigint as total_paid
    from public.orders o
    group by o.user_id
  ),
  base_users as (
    select
      u.user_id,
      u.username,
      nullif(
        btrim(
          coalesce(u.first_name, '') || ' ' || coalesce(u.last_name, '')
        ),
        ''
      ) as display_name,
      lower(coalesce(u.username, '')) as username_search,
      lower(
        coalesce(
          nullif(
            btrim(
              coalesce(u.first_name, '') || ' ' || coalesce(u.last_name, '')
            ),
            ''
          ),
          ''
        )
      ) as display_name_search,
      (u.user_id)::text as user_id_search,
      coalesce(u.balance, 0) as balance,
      coalesce(u.balance_usdt, 0) as balance_usdt,
      u.language,
      u.created_at,
      coalesce(os.order_count, 0)::bigint as order_count,
      coalesce(os.total_paid, 0)::bigint as total_paid
    from public.users u
    left join order_stats os on os.user_id = u.user_id
  ),
  filtered as materialized (
    select *
    from base_users bu
    where
      (
        v_search = ''
        or bu.user_id_search ilike '%' || v_search_no_at || '%'
        or bu.username_search like '%' || v_search || '%'
        or bu.username_search like '%' || v_search_no_at || '%'
        or bu.display_name_search like '%' || v_search || '%'
      )
      and (
        v_filter = 'all'
        or (v_filter = 'with_revenue' and bu.total_paid > 0)
        or (v_filter = 'without_revenue' and bu.total_paid <= 0)
        or (v_filter = 'with_orders' and bu.order_count > 0)
      )
  ),
  totals as (
    select
      count(*)::bigint as total_count,
      greatest(1, ceiling(count(*)::numeric / v_page_size)::integer) as total_pages
    from filtered
  ),
  meta as (
    select
      total_count,
      total_pages,
      least(v_page, total_pages) as safe_page
    from totals
  ),
  ranked as (
    select
      f.*,
      row_number() over (
        order by
          case when v_sort = 'newest' then f.created_at end desc nulls last,
          case when v_sort = 'oldest' then f.created_at end asc nulls last,
          case when v_sort = 'username_asc' then f.username_search end asc nulls last,
          case when v_sort = 'username_desc' then f.username_search end desc nulls last,
          case when v_sort = 'revenue_desc' then f.total_paid end desc nulls last,
          case when v_sort = 'revenue_asc' then f.total_paid end asc nulls last,
          case when v_sort = 'order_count_desc' then f.order_count end desc nulls last,
          case when v_sort = 'order_count_asc' then f.order_count end asc nulls last,
          f.created_at desc nulls last,
          f.user_id desc
      ) as row_number
    from filtered f
  ),
  paged as (
    select r.*
    from ranked r
    cross join meta m
    where
      r.row_number > ((m.safe_page - 1) * v_page_size)
      and r.row_number <= (m.safe_page * v_page_size)
  )
  select jsonb_build_object(
    'users',
    coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'user_id', p.user_id,
            'username', p.username,
            'display_name', p.display_name,
            'balance', p.balance,
            'balance_usdt', p.balance_usdt,
            'language', p.language,
            'created_at', p.created_at,
            'order_count', p.order_count,
            'total_paid', p.total_paid
          )
          order by p.row_number
        )
        from paged p
      ),
      '[]'::jsonb
    ),
    'page', coalesce((select safe_page from meta), 1),
    'pageSize', v_page_size,
    'totalCount', coalesce((select total_count from meta), 0),
    'totalPages', coalesce((select total_pages from meta), 1)
  )
  into v_result;

  return coalesce(
    v_result,
    jsonb_build_object(
      'users', '[]'::jsonb,
      'page', 1,
      'pageSize', v_page_size,
      'totalCount', 0,
      'totalPages', 1
    )
  );
end;
$$;

grant execute on function public.admin_bot_users_snapshot_page_v2(integer, integer, text, text, text) to authenticated;
