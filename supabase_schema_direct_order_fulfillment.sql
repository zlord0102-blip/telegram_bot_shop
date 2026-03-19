-- Shared direct-order fulfillment helpers for Bot + Website.
-- New file on purpose: do not append this into older SQL files.

create or replace function public.fulfill_bot_direct_order(
  p_direct_order_id bigint,
  p_order_group text default null,
  p_expire_minutes integer default 10
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_order public.direct_orders%rowtype;
  v_product_name text;
  v_description text;
  v_format_data text;
  v_deliver_quantity integer;
  v_order_group text;
  v_stock_ids bigint[];
  v_items jsonb;
  v_stock_count integer;
  v_created_order_id bigint;
  v_total_price bigint;
begin
  if not (auth.role() = 'service_role' or public.is_admin()) then
    raise exception 'forbidden';
  end if;

  select *
  into v_order
  from public.direct_orders
  where id = p_direct_order_id
  for update;

  if not found then
    raise exception 'direct_order_not_found';
  end if;

  if coalesce(v_order.status, '') <> 'pending' then
    raise exception 'direct_order_not_pending';
  end if;

  if v_order.created_at is not null
    and v_order.created_at <= now() - make_interval(mins => greatest(1, coalesce(p_expire_minutes, 10)))
  then
    update public.direct_orders
    set status = 'cancelled'
    where id = v_order.id;

    raise exception 'direct_order_expired';
  end if;

  select
    p.name,
    p.description,
    p.format_data
  into
    v_product_name,
    v_description,
    v_format_data
  from public.products p
  where p.id = v_order.product_id;

  v_deliver_quantity := greatest(1, coalesce(v_order.quantity, 0) + greatest(0, coalesce(v_order.bonus_quantity, 0)));

  with locked_stock as (
    select s.id, s.content
    from public.stock s
    where s.product_id = v_order.product_id
      and s.sold = false
    order by s.id
    for update skip locked
    limit v_deliver_quantity
  )
  select
    array_agg(ls.id order by ls.id),
    coalesce(jsonb_agg(ls.content order by ls.id), '[]'::jsonb),
    count(*)
  into
    v_stock_ids,
    v_items,
    v_stock_count
  from locked_stock ls;

  if coalesce(v_stock_count, 0) < v_deliver_quantity then
    update public.direct_orders
    set status = 'failed'
    where id = v_order.id;

    raise exception 'not_enough_stock';
  end if;

  update public.stock
  set sold = true
  where id = any(v_stock_ids);

  v_order_group := coalesce(
    nullif(btrim(p_order_group), ''),
    'PAY' || v_order.user_id::text || to_char(now(), 'YYYYMMDDHH24MISS')
  );
  v_total_price := coalesce(v_order.amount, coalesce(v_order.unit_price, 0) * greatest(1, coalesce(v_order.quantity, 0)));

  insert into public.orders (
    user_id,
    product_id,
    content,
    price,
    quantity,
    order_group,
    created_at
  )
  values (
    v_order.user_id,
    v_order.product_id,
    v_items::text,
    v_total_price,
    v_deliver_quantity,
    v_order_group,
    now()
  )
  returning id into v_created_order_id;

  update public.direct_orders
  set status = 'confirmed'
  where id = v_order.id;

  return jsonb_build_object(
    'direct_order_id', v_order.id,
    'order_id', v_created_order_id,
    'user_id', v_order.user_id,
    'product_id', v_order.product_id,
    'product_name', coalesce(nullif(btrim(v_product_name), ''), '#' || v_order.product_id::text),
    'description', coalesce(v_description, ''),
    'format_data', coalesce(v_format_data, ''),
    'quantity', coalesce(v_order.quantity, 1),
    'bonus_quantity', coalesce(v_order.bonus_quantity, 0),
    'delivered_quantity', v_deliver_quantity,
    'unit_price', coalesce(v_order.unit_price, 0),
    'amount', v_total_price,
    'code', coalesce(v_order.code, ''),
    'order_group', v_order_group,
    'items', coalesce(v_items, '[]'::jsonb)
  );
end;
$$;

create or replace function public.fulfill_website_direct_order(
  p_website_direct_order_id bigint,
  p_order_group text default null,
  p_expire_minutes integer default 10
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_order public.website_direct_orders%rowtype;
  v_mirror public.direct_orders%rowtype;
  v_product_name text;
  v_deliver_quantity integer;
  v_order_group text;
  v_stock_ids bigint[];
  v_items jsonb;
  v_stock_count integer;
  v_created_order_id bigint;
  v_total_price bigint;
begin
  if not (auth.role() = 'service_role' or public.is_admin()) then
    raise exception 'forbidden';
  end if;

  select *
  into v_order
  from public.website_direct_orders
  where id = p_website_direct_order_id
  for update;

  if not found then
    raise exception 'website_direct_order_not_found';
  end if;

  if coalesce(v_order.status, '') <> 'pending' then
    raise exception 'website_direct_order_not_pending';
  end if;

  if v_order.created_at is not null
    and v_order.created_at <= now() - make_interval(mins => greatest(1, coalesce(p_expire_minutes, 10)))
  then
    update public.website_direct_orders
    set status = 'cancelled',
        updated_at = now()
    where id = v_order.id;

    update public.direct_orders
    set status = 'cancelled'
    where code = v_order.code
      and status = 'pending';

    raise exception 'website_direct_order_expired';
  end if;

  select *
  into v_mirror
  from public.direct_orders
  where code = v_order.code
  order by id desc
  limit 1
  for update;

  if not found then
    raise exception 'mirror_direct_order_not_found';
  end if;

  if coalesce(v_mirror.status, '') <> 'pending' then
    raise exception 'mirror_direct_order_not_pending';
  end if;

  select
    coalesce(nullif(btrim(p.website_name), ''), p.name)
  into
    v_product_name
  from public.products p
  where p.id = v_order.product_id;

  v_deliver_quantity := greatest(1, coalesce(v_order.quantity, 0) + greatest(0, coalesce(v_order.bonus_quantity, 0)));

  with locked_stock as (
    select s.id, s.content
    from public.stock s
    where s.product_id = v_order.product_id
      and s.sold = false
    order by s.id
    for update skip locked
    limit v_deliver_quantity
  )
  select
    array_agg(ls.id order by ls.id),
    coalesce(jsonb_agg(ls.content order by ls.id), '[]'::jsonb),
    count(*)
  into
    v_stock_ids,
    v_items,
    v_stock_count
  from locked_stock ls;

  if coalesce(v_stock_count, 0) < v_deliver_quantity then
    update public.website_direct_orders
    set status = 'failed',
        updated_at = now()
    where id = v_order.id;

    update public.direct_orders
    set status = 'failed'
    where id = v_mirror.id;

    raise exception 'not_enough_stock';
  end if;

  update public.stock
  set sold = true
  where id = any(v_stock_ids);

  v_order_group := coalesce(
    nullif(btrim(p_order_group), ''),
    'WEB' || to_char(now(), 'YYYYMMDDHH24MISS')
  );
  v_total_price := coalesce(v_order.amount, coalesce(v_order.unit_price, 0) * greatest(1, coalesce(v_order.quantity, 0)));

  insert into public.website_orders (
    auth_user_id,
    user_email,
    product_id,
    content,
    price,
    quantity,
    order_group,
    source_direct_code,
    created_at
  )
  values (
    v_order.auth_user_id,
    v_order.user_email,
    v_order.product_id,
    v_items::text,
    v_total_price,
    v_deliver_quantity,
    v_order_group,
    v_order.code,
    now()
  )
  returning id into v_created_order_id;

  update public.website_direct_orders
  set status = 'confirmed',
      confirmed_at = now(),
      updated_at = now(),
      fulfilled_order_id = v_created_order_id
  where id = v_order.id;

  update public.direct_orders
  set status = 'confirmed'
  where id = v_mirror.id;

  return jsonb_build_object(
    'website_direct_order_id', v_order.id,
    'direct_order_id', v_mirror.id,
    'website_order_id', v_created_order_id,
    'auth_user_id', coalesce(v_order.auth_user_id, ''),
    'user_email', coalesce(v_order.user_email, ''),
    'product_id', v_order.product_id,
    'product_name', coalesce(nullif(btrim(v_product_name), ''), '#' || v_order.product_id::text),
    'quantity', coalesce(v_order.quantity, 1),
    'bonus_quantity', coalesce(v_order.bonus_quantity, 0),
    'delivered_quantity', v_deliver_quantity,
    'unit_price', coalesce(v_order.unit_price, 0),
    'amount', v_total_price,
    'code', coalesce(v_order.code, ''),
    'order_group', v_order_group,
    'items', coalesce(v_items, '[]'::jsonb)
  );
end;
$$;

grant execute on function public.fulfill_bot_direct_order(bigint, text, integer) to authenticated;
grant execute on function public.fulfill_website_direct_order(bigint, text, integer) to authenticated;
