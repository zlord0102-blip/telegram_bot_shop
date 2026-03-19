-- Bot admin analytics snapshot helpers.
-- New file on purpose: do not append this into older SQL files.

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

create or replace function public.admin_bot_reports_snapshot(
  p_tz text default 'Asia/Ho_Chi_Minh',
  p_expire_minutes integer default 10
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_tz text := coalesce(nullif(btrim(p_tz), ''), 'Asia/Ho_Chi_Minh');
  v_now_local timestamp := timezone(v_tz, now());
  v_today_start_local timestamp := date_trunc('day', v_now_local);
  v_yesterday_start_local timestamp := v_today_start_local - interval '1 day';
  v_start7_utc timestamptz := now() - interval '7 day';
  v_start_prev7_utc timestamptz := now() - interval '14 day';
  v_start30_utc timestamptz := now() - interval '30 day';
  v_pending_expired_before timestamptz := now() - make_interval(mins => greatest(1, coalesce(p_expire_minutes, 10)));
  v_revenue jsonb;
  v_order_ops jsonb;
  v_direct_order_stats jsonb;
  v_daily_trend jsonb;
  v_top_products jsonb;
begin
  if not (auth.role() = 'service_role' or public.is_admin()) then
    raise exception 'forbidden';
  end if;

  select
    jsonb_build_object(
      'today', coalesce(sum(coalesce(o.price, 0)) filter (where timezone(v_tz, o.created_at) >= v_today_start_local), 0)::bigint,
      'yesterday', coalesce(sum(coalesce(o.price, 0)) filter (
        where timezone(v_tz, o.created_at) >= v_yesterday_start_local
          and timezone(v_tz, o.created_at) < v_today_start_local
      ), 0)::bigint,
      'last7', coalesce(sum(coalesce(o.price, 0)) filter (where o.created_at >= v_start7_utc), 0)::bigint,
      'previous7', coalesce(sum(coalesce(o.price, 0)) filter (
        where o.created_at >= v_start_prev7_utc
          and o.created_at < v_start7_utc
      ), 0)::bigint,
      'last30', coalesce(sum(coalesce(o.price, 0)) filter (where o.created_at >= v_start30_utc), 0)::bigint
    ),
    jsonb_build_object(
      'ordersToday', count(*) filter (where timezone(v_tz, o.created_at) >= v_today_start_local),
      'ordersLast7', count(*) filter (where o.created_at >= v_start7_utc),
      'ordersLast30', count(*) filter (where o.created_at >= v_start30_utc),
      'averageOrderValue30',
        coalesce(
          coalesce(sum(coalesce(o.price, 0)) filter (where o.created_at >= v_start30_utc), 0)::numeric
          / nullif(count(*) filter (where o.created_at >= v_start30_utc), 0),
          0
        ),
      'averageQuantity30',
        coalesce(
          coalesce(sum(coalesce(o.quantity, 0)) filter (where o.created_at >= v_start30_utc), 0)::numeric
          / nullif(count(*) filter (where o.created_at >= v_start30_utc), 0),
          0
        )
    )
  into
    v_revenue,
    v_order_ops
  from public.orders o;

  select jsonb_build_object(
    'total', total_count,
    'confirmed', confirmed_count,
    'failed', failed_count,
    'cancelled', cancelled_count,
    'pending', pending_count,
    'pendingExpired', pending_expired_count,
    'confirmedRate',
      case
        when processed_count > 0 then (confirmed_count::numeric / processed_count::numeric) * 100
        else 0
      end,
    'failedRate',
      case
        when processed_count > 0 then ((failed_count + cancelled_count)::numeric / processed_count::numeric) * 100
        else 0
      end
  )
  into v_direct_order_stats
  from (
    select
      count(*)::bigint as total_count,
      count(*) filter (where status = 'confirmed')::bigint as confirmed_count,
      count(*) filter (where status = 'failed')::bigint as failed_count,
      count(*) filter (where status = 'cancelled')::bigint as cancelled_count,
      count(*) filter (where status = 'pending')::bigint as pending_count,
      count(*) filter (where status = 'pending' and created_at < v_pending_expired_before)::bigint as pending_expired_count,
      (
        count(*) filter (where status = 'confirmed')
        + count(*) filter (where status = 'failed')
        + count(*) filter (where status = 'cancelled')
      )::bigint as processed_count
    from public.direct_orders
  ) stats;

  with trend_seed as (
    select generate_series(
      v_today_start_local - interval '6 day',
      v_today_start_local,
      interval '1 day'
    ) as day_start
  ),
  trend_rows as (
    select
      ts.day_start::date as day_date,
      to_char(ts.day_start, 'DD/MM') as label,
      count(o.id)::bigint as orders,
      coalesce(sum(coalesce(o.price, 0)), 0)::bigint as revenue
    from trend_seed ts
    left join public.orders o
      on timezone(v_tz, o.created_at) >= ts.day_start
     and timezone(v_tz, o.created_at) < ts.day_start + interval '1 day'
    group by ts.day_start
    order by ts.day_start
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'dateKey', to_char(tr.day_date, 'YYYY-MM-DD'),
          'label', tr.label,
          'orders', tr.orders,
          'revenue', tr.revenue
        )
        order by tr.day_date
      ),
      '[]'::jsonb
    )
  into v_daily_trend
  from trend_rows tr;

  with top_rows as (
    select
      o.product_id,
      coalesce(nullif(btrim(p.name), ''), '#' || coalesce(o.product_id::text, '-')) as product_name,
      count(*)::bigint as orders,
      coalesce(sum(coalesce(o.quantity, 0)), 0)::bigint as quantity,
      coalesce(sum(coalesce(o.price, 0)), 0)::bigint as revenue
    from public.orders o
    left join public.products p on p.id = o.product_id
    where o.created_at >= v_start30_utc
    group by o.product_id, p.name
    order by revenue desc, quantity desc, orders desc
    limit 8
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'productId', coalesce(tr.product_id::text, '-'),
          'productName', tr.product_name,
          'orders', tr.orders,
          'quantity', tr.quantity,
          'revenue', tr.revenue
        )
        order by tr.revenue desc, tr.quantity desc, tr.orders desc
      ),
      '[]'::jsonb
    )
  into v_top_products
  from top_rows tr;

  return jsonb_build_object(
    'revenue', coalesce(v_revenue, '{}'::jsonb),
    'orderOps', coalesce(v_order_ops, '{}'::jsonb),
    'directOrderStats', coalesce(v_direct_order_stats, '{}'::jsonb),
    'dailyTrend', coalesce(v_daily_trend, '[]'::jsonb),
    'topProducts', coalesce(v_top_products, '[]'::jsonb)
  );
end;
$$;

create or replace function public.admin_bot_users_snapshot(
  p_limit integer default 200
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_limit integer := greatest(1, least(coalesce(p_limit, 200), 1000));
  v_users jsonb;
begin
  if not (auth.role() = 'service_role' or public.is_admin()) then
    raise exception 'forbidden';
  end if;

  with base_users as (
    select
      u.user_id,
      u.username,
      coalesce(u.balance, 0) as balance,
      coalesce(u.balance_usdt, 0) as balance_usdt,
      u.language,
      u.created_at
    from public.users u
    order by u.created_at desc nulls last, u.user_id desc
    limit v_limit
  ),
  order_stats as (
    select
      o.user_id,
      count(*)::bigint as order_count,
      coalesce(sum(coalesce(o.price, 0)), 0)::bigint as total_paid
    from public.orders o
    join base_users bu on bu.user_id = o.user_id
    group by o.user_id
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'user_id', bu.user_id,
          'username', bu.username,
          'balance', bu.balance,
          'balance_usdt', bu.balance_usdt,
          'language', bu.language,
          'created_at', bu.created_at,
          'order_count', coalesce(os.order_count, 0),
          'total_paid', coalesce(os.total_paid, 0)
        )
        order by bu.created_at desc nulls last, bu.user_id desc
      ),
      '[]'::jsonb
    )
  into v_users
  from base_users bu
  left join order_stats os on os.user_id = bu.user_id;

  return jsonb_build_object(
    'users', coalesce(v_users, '[]'::jsonb)
  );
end;
$$;

grant execute on function public.admin_bot_dashboard_snapshot(integer) to authenticated;
grant execute on function public.admin_bot_reports_snapshot(text, integer) to authenticated;
grant execute on function public.admin_bot_users_snapshot(integer) to authenticated;
