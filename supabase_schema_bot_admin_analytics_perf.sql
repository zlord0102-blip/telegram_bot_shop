-- Bot admin analytics performance refinements.
-- Apply after supabase_schema_bot_admin_analytics.sql.

alter table public.users
  add column if not exists first_name text;

alter table public.users
  add column if not exists last_name text;

create index if not exists users_created_user_idx
  on public.users (created_at desc, user_id desc);

create or replace function public.admin_bot_dashboard_snapshot(
  p_recent_limit integer default 6
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_recent_limit integer := greatest(1, least(coalesce(p_recent_limit, 6), 50));
  v_latest_orders jsonb;
begin
  if not (auth.role() = 'service_role' or public.is_admin()) then
    raise exception 'forbidden';
  end if;

  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'id', recent.id,
          'user_id', recent.user_id,
          'username', recent.username,
          'display_name', recent.display_name,
          'product_id', recent.product_id,
          'product_name', recent.product_name,
          'price', recent.price,
          'quantity', recent.quantity,
          'created_at', recent.created_at
        )
        order by recent.created_at desc nulls last, recent.id desc
      ),
      '[]'::jsonb
    )
  into v_latest_orders
  from (
    select
      o.id,
      o.user_id,
      u.username,
      nullif(
        trim(
          concat_ws(
            ' ',
            nullif(btrim(u.first_name), ''),
            nullif(btrim(u.last_name), '')
          )
        ),
        ''
      ) as display_name,
      o.product_id,
      coalesce(nullif(btrim(p.name), ''), '#' || coalesce(o.product_id::text, '-')) as product_name,
      coalesce(o.price, 0) as price,
      coalesce(o.quantity, 0) as quantity,
      o.created_at
    from public.orders o
    left join public.users u on u.user_id = o.user_id
    left join public.products p on p.id = o.product_id
    order by o.created_at desc nulls last, o.id desc
    limit v_recent_limit
  ) recent;

  return jsonb_build_object(
    'stats',
    jsonb_build_object(
      'users', (select count(*)::bigint from public.users),
      'orders', (select count(*)::bigint from public.orders),
      'revenue', (select coalesce(sum(price), 0)::bigint from public.orders)
    ),
    'pendingDeposits', (select count(*)::bigint from public.deposits where status = 'pending'),
    'pendingWithdrawals', (select count(*)::bigint from public.withdrawals where status = 'pending'),
    'orders', coalesce(v_latest_orders, '[]'::jsonb)
  );
end;
$$;

create or replace function public.admin_bot_users_snapshot_page(
  p_page integer default 1,
  p_page_size integer default 50,
  p_search text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_page integer := greatest(1, coalesce(p_page, 1));
  v_page_size integer := greatest(1, least(coalesce(p_page_size, 50), 200));
  v_search text := lower(coalesce(nullif(btrim(p_search), ''), ''));
  v_search_no_at text := regexp_replace(v_search, '^@+', '');
  v_total_count bigint := 0;
  v_total_pages integer := 1;
  v_safe_page integer := 1;
  v_users jsonb;
begin
  if not (auth.role() = 'service_role' or public.is_admin()) then
    raise exception 'forbidden';
  end if;

  with filtered_users as (
    select u.user_id
    from public.users u
    where
      v_search = ''
      or u.user_id::text ilike '%' || v_search_no_at || '%'
      or coalesce(lower(u.username), '') like '%' || v_search || '%'
      or coalesce(lower(u.username), '') like '%' || v_search_no_at || '%'
      or coalesce(
        lower(
          trim(
            concat_ws(
              ' ',
              nullif(btrim(u.first_name), ''),
              nullif(btrim(u.last_name), '')
            )
          )
        ),
        ''
      ) like '%' || v_search || '%'
  )
  select count(*)::bigint into v_total_count
  from filtered_users;

  v_total_pages := greatest(1, ceiling(v_total_count::numeric / v_page_size)::integer);
  v_safe_page := least(v_page, v_total_pages);

  with filtered_users as (
    select
      u.user_id,
      u.username,
      u.first_name,
      u.last_name,
      coalesce(u.balance, 0) as balance,
      coalesce(u.balance_usdt, 0) as balance_usdt,
      u.language,
      u.created_at
    from public.users u
    where
      v_search = ''
      or u.user_id::text ilike '%' || v_search_no_at || '%'
      or coalesce(lower(u.username), '') like '%' || v_search || '%'
      or coalesce(lower(u.username), '') like '%' || v_search_no_at || '%'
      or coalesce(
        lower(
          trim(
            concat_ws(
              ' ',
              nullif(btrim(u.first_name), ''),
              nullif(btrim(u.last_name), '')
            )
          )
        ),
        ''
      ) like '%' || v_search || '%'
  ),
  paged_users as (
    select *
    from filtered_users
    order by created_at desc nulls last, user_id desc
    offset (v_safe_page - 1) * v_page_size
    limit v_page_size
  ),
  order_stats as (
    select
      o.user_id,
      count(*)::bigint as order_count,
      coalesce(sum(coalesce(o.price, 0)), 0)::bigint as total_paid
    from public.orders o
    join paged_users pu on pu.user_id = o.user_id
    group by o.user_id
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'user_id', pu.user_id,
          'username', pu.username,
          'display_name',
            nullif(
              trim(
                concat_ws(
                  ' ',
                  nullif(btrim(pu.first_name), ''),
                  nullif(btrim(pu.last_name), '')
                )
              ),
              ''
            ),
          'balance', pu.balance,
          'balance_usdt', pu.balance_usdt,
          'language', pu.language,
          'created_at', pu.created_at,
          'order_count', coalesce(os.order_count, 0),
          'total_paid', coalesce(os.total_paid, 0)
        )
        order by pu.created_at desc nulls last, pu.user_id desc
      ),
      '[]'::jsonb
    )
  into v_users
  from paged_users pu
  left join order_stats os on os.user_id = pu.user_id;

  return jsonb_build_object(
    'users', coalesce(v_users, '[]'::jsonb),
    'page', v_safe_page,
    'pageSize', v_page_size,
    'totalCount', v_total_count,
    'totalPages', v_total_pages
  );
end;
$$;

grant execute on function public.admin_bot_dashboard_snapshot(integer) to authenticated;
grant execute on function public.admin_bot_users_snapshot_page(integer, integer, text) to authenticated;
