-- Atomic helper for Bot instant balance / USDT purchases.
-- New file on purpose: do not append this into older SQL files.

create or replace function public.fulfill_bot_balance_purchase(
  p_user_id bigint,
  p_product_id bigint,
  p_quantity integer,
  p_bonus_quantity integer default 0,
  p_order_price_per_item bigint default 0,
  p_order_total_price bigint default 0,
  p_charge_balance bigint default 0,
  p_charge_balance_usdt numeric default 0,
  p_order_group text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_product public.products%rowtype;
  v_balance bigint;
  v_balance_usdt numeric;
  v_required_stock integer;
  v_stock_ids bigint[];
  v_items jsonb;
  v_stock_count integer;
  v_order_group text;
  v_created_order_id bigint;
  v_unit_price bigint;
  v_total_price bigint;
  v_new_balance bigint;
  v_new_balance_usdt numeric;
begin
  if not (auth.role() = 'service_role' or public.is_admin()) then
    raise exception 'forbidden';
  end if;

  insert into public.users (user_id, created_at)
  values (p_user_id, now())
  on conflict (user_id) do nothing;

  select *
  into v_product
  from public.products
  where id = p_product_id
  for update;

  if not found then
    raise exception 'product_not_found';
  end if;

  select
    coalesce(u.balance, 0),
    coalesce(u.balance_usdt, 0)
  into
    v_balance,
    v_balance_usdt
  from public.users u
  where u.user_id = p_user_id
  for update;

  if not found then
    raise exception 'user_not_found';
  end if;

  if greatest(0, coalesce(p_charge_balance, 0)) > v_balance then
    raise exception 'insufficient_balance';
  end if;

  if greatest(0, coalesce(p_charge_balance_usdt, 0)) > v_balance_usdt then
    raise exception 'insufficient_usdt_balance';
  end if;

  v_required_stock := greatest(1, coalesce(p_quantity, 0) + greatest(0, coalesce(p_bonus_quantity, 0)));

  with locked_stock as (
    select s.id, s.content
    from public.stock s
    where s.product_id = p_product_id
      and s.sold = false
    order by s.id
    for update skip locked
    limit v_required_stock
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

  if coalesce(v_stock_count, 0) < v_required_stock then
    raise exception 'not_enough_stock';
  end if;

  update public.stock
  set sold = true
  where id = any(v_stock_ids);

  update public.users
  set balance = coalesce(balance, 0) - greatest(0, coalesce(p_charge_balance, 0)),
      balance_usdt = coalesce(balance_usdt, 0) - greatest(0, coalesce(p_charge_balance_usdt, 0))
  where user_id = p_user_id
  returning balance, balance_usdt into v_new_balance, v_new_balance_usdt;

  v_order_group := coalesce(
    nullif(btrim(p_order_group), ''),
    'ORD' || p_user_id::text || to_char(now(), 'YYYYMMDDHH24MISS')
  );
  v_unit_price := greatest(0, coalesce(p_order_price_per_item, 0));
  v_total_price := greatest(
    0,
    coalesce(
      p_order_total_price,
      v_unit_price * greatest(1, coalesce(p_quantity, 0))
    )
  );

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
    p_user_id,
    p_product_id,
    v_items::text,
    v_total_price,
    v_required_stock,
    v_order_group,
    now()
  )
  returning id into v_created_order_id;

  return jsonb_build_object(
    'order_id', v_created_order_id,
    'user_id', p_user_id,
    'product_id', p_product_id,
    'product_name', coalesce(nullif(btrim(v_product.name), ''), '#' || p_product_id::text),
    'description', coalesce(v_product.description, ''),
    'format_data', coalesce(v_product.format_data, ''),
    'quantity', greatest(1, coalesce(p_quantity, 0)),
    'bonus_quantity', greatest(0, coalesce(p_bonus_quantity, 0)),
    'delivered_quantity', v_required_stock,
    'order_group', v_order_group,
    'items', coalesce(v_items, '[]'::jsonb),
    'order_total_price', v_total_price,
    'charged_balance', greatest(0, coalesce(p_charge_balance, 0)),
    'charged_balance_usdt', greatest(0, coalesce(p_charge_balance_usdt, 0)),
    'new_balance', coalesce(v_new_balance, 0),
    'new_balance_usdt', coalesce(v_new_balance_usdt, 0)
  );
end;
$$;

grant execute on function public.fulfill_bot_balance_purchase(
  bigint,
  bigint,
  integer,
  integer,
  bigint,
  bigint,
  bigint,
  numeric,
  text
) to authenticated;
