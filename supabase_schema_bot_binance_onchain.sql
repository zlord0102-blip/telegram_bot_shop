-- Binance on-chain auto payment for Telegram Bot direct orders
-- Run after base schema / previous migrations.

-- Remove legacy manual Binance deposit flow.
drop function if exists public.admin_confirm_binance_deposit(bigint);
drop function if exists public.admin_cancel_binance_deposit(bigint);
drop table if exists public.binance_deposits cascade;

-- Direct-order payment-channel metadata.
alter table public.direct_orders add column if not exists payment_channel text default 'vietqr';
alter table public.direct_orders add column if not exists payment_asset text;
alter table public.direct_orders add column if not exists payment_network text;
alter table public.direct_orders add column if not exists payment_amount_asset numeric;
alter table public.direct_orders add column if not exists payment_rate_vnd numeric;
alter table public.direct_orders add column if not exists payment_address text;
alter table public.direct_orders add column if not exists payment_address_tag text;
alter table public.direct_orders add column if not exists external_payment_id text;
alter table public.direct_orders add column if not exists external_tx_id text;
alter table public.direct_orders add column if not exists external_paid_at timestamptz;

update public.direct_orders
set payment_channel = 'vietqr'
where payment_channel is null;

create index if not exists direct_orders_status_payment_channel_idx
  on public.direct_orders (status, payment_channel, created_at desc);

create index if not exists direct_orders_external_payment_idx
  on public.direct_orders (external_payment_id);

create index if not exists direct_orders_external_tx_idx
  on public.direct_orders (external_tx_id);

create unique index if not exists direct_orders_pending_binance_amount_idx
  on public.direct_orders (payment_channel, payment_asset, payment_network, payment_amount_asset)
  where status = 'pending'
    and payment_channel = 'binance_onchain'
    and payment_amount_asset is not null;

-- Binance idempotency table.
create table if not exists public.binance_processed_deposits (
  payment_id text primary key,
  tx_id text,
  direct_order_id bigint references public.direct_orders(id) on delete set null,
  amount_asset numeric,
  payment_asset text,
  payment_network text,
  processed_at timestamptz default now()
);

create index if not exists binance_processed_deposits_tx_id_idx
  on public.binance_processed_deposits (tx_id);

alter table public.binance_processed_deposits enable row level security;
drop policy if exists "Admins can access binance processed deposits" on public.binance_processed_deposits;
create policy "Admins can access binance processed deposits" on public.binance_processed_deposits
  for all using (public.is_admin()) with check (public.is_admin());

-- Rebuild reset_sequences so it no longer references the removed legacy table.
create or replace function public.reset_sequences()
returns void
language plpgsql
security definer
as $$
begin
  perform setval(pg_get_serial_sequence('public.products', 'id'), coalesce((select max(id) from public.products), 1), true);
  perform setval(pg_get_serial_sequence('public.stock', 'id'), coalesce((select max(id) from public.stock), 1), true);
  perform setval(pg_get_serial_sequence('public.orders', 'id'), coalesce((select max(id) from public.orders), 1), true);
  perform setval(pg_get_serial_sequence('public.deposits', 'id'), coalesce((select max(id) from public.deposits), 1), true);
  perform setval(pg_get_serial_sequence('public.withdrawals', 'id'), coalesce((select max(id) from public.withdrawals), 1), true);
  perform setval(pg_get_serial_sequence('public.usdt_withdrawals', 'id'), coalesce((select max(id) from public.usdt_withdrawals), 1), true);
  perform setval(pg_get_serial_sequence('public.direct_orders', 'id'), coalesce((select max(id) from public.direct_orders), 1), true);
  perform setval(pg_get_serial_sequence('public.format_templates', 'id'), coalesce((select max(id) from public.format_templates), 1), true);
end $$;

-- Atomic helper for Telegram Bot Binance direct orders.
create or replace function public.create_binance_direct_order(
  p_user_id bigint,
  p_product_id bigint,
  p_quantity integer,
  p_bonus_quantity integer,
  p_unit_price bigint,
  p_amount bigint,
  p_code text,
  p_payment_asset text,
  p_payment_network text,
  p_payment_amount_asset numeric,
  p_payment_rate_vnd numeric,
  p_payment_address text,
  p_payment_address_tag text
)
returns table (
  direct_order_id bigint,
  code text,
  payment_asset text,
  payment_network text,
  payment_amount_asset numeric,
  payment_address text,
  payment_address_tag text,
  created_at timestamptz
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_order public.direct_orders%rowtype;
begin
  if not (auth.role() = 'service_role' or public.is_admin()) then
    raise exception 'forbidden';
  end if;

  insert into public.direct_orders (
    user_id,
    product_id,
    quantity,
    bonus_quantity,
    unit_price,
    amount,
    code,
    payment_channel,
    payment_asset,
    payment_network,
    payment_amount_asset,
    payment_rate_vnd,
    payment_address,
    payment_address_tag,
    created_at
  )
  values (
    p_user_id,
    p_product_id,
    p_quantity,
    p_bonus_quantity,
    p_unit_price,
    p_amount,
    p_code,
    'binance_onchain',
    p_payment_asset,
    p_payment_network,
    p_payment_amount_asset,
    p_payment_rate_vnd,
    p_payment_address,
    nullif(p_payment_address_tag, ''),
    now()
  )
  returning * into v_order;

  return query
  select
    v_order.id,
    v_order.code,
    v_order.payment_asset,
    v_order.payment_network,
    v_order.payment_amount_asset,
    v_order.payment_address,
    v_order.payment_address_tag,
    v_order.created_at;
end $$;

grant execute on function public.create_binance_direct_order(
  bigint,
  bigint,
  integer,
  integer,
  bigint,
  bigint,
  text,
  text,
  text,
  numeric,
  numeric,
  text,
  text
) to authenticated;
