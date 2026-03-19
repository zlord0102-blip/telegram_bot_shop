-- Atomic helpers for Bot Dashboard manual financial actions.
-- New file on purpose: do not append this into older SQL files.

create or replace function public.admin_confirm_deposit(p_id bigint)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_deposit public.deposits%rowtype;
  v_new_balance bigint;
begin
  if not public.is_admin() then
    raise exception 'forbidden';
  end if;

  select * into v_deposit
  from public.deposits
  where id = p_id
  for update;

  if not found then
    raise exception 'deposit_not_found';
  end if;
  if coalesce(v_deposit.status, '') <> 'pending' then
    raise exception 'deposit_not_pending';
  end if;

  update public.users
  set balance = coalesce(balance, 0) + coalesce(v_deposit.amount, 0)
  where user_id = v_deposit.user_id
  returning balance into v_new_balance;

  if v_new_balance is null then
    raise exception 'user_not_found';
  end if;

  update public.deposits
  set status = 'confirmed'
  where id = v_deposit.id;

  return jsonb_build_object(
    'record_id', v_deposit.id,
    'user_id', v_deposit.user_id,
    'status', 'confirmed',
    'amount', v_deposit.amount,
    'new_balance', v_new_balance
  );
end;
$$;

create or replace function public.admin_cancel_deposit(p_id bigint)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_deposit public.deposits%rowtype;
begin
  if not public.is_admin() then
    raise exception 'forbidden';
  end if;

  select * into v_deposit
  from public.deposits
  where id = p_id
  for update;

  if not found then
    raise exception 'deposit_not_found';
  end if;
  if coalesce(v_deposit.status, '') <> 'pending' then
    raise exception 'deposit_not_pending';
  end if;

  update public.deposits
  set status = 'cancelled'
  where id = v_deposit.id;

  return jsonb_build_object(
    'record_id', v_deposit.id,
    'status', 'cancelled'
  );
end;
$$;

create or replace function public.admin_confirm_withdrawal(p_id bigint)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_withdrawal public.withdrawals%rowtype;
  v_balance bigint;
  v_new_balance bigint;
begin
  if not public.is_admin() then
    raise exception 'forbidden';
  end if;

  select * into v_withdrawal
  from public.withdrawals
  where id = p_id
  for update;

  if not found then
    raise exception 'withdrawal_not_found';
  end if;
  if coalesce(v_withdrawal.status, '') <> 'pending' then
    raise exception 'withdrawal_not_pending';
  end if;

  select balance into v_balance
  from public.users
  where user_id = v_withdrawal.user_id
  for update;

  if not found then
    raise exception 'user_not_found';
  end if;
  if coalesce(v_balance, 0) < coalesce(v_withdrawal.amount, 0) then
    raise exception 'insufficient_balance';
  end if;

  update public.users
  set balance = coalesce(balance, 0) - coalesce(v_withdrawal.amount, 0)
  where user_id = v_withdrawal.user_id
  returning balance into v_new_balance;

  update public.withdrawals
  set status = 'confirmed'
  where id = v_withdrawal.id;

  return jsonb_build_object(
    'record_id', v_withdrawal.id,
    'user_id', v_withdrawal.user_id,
    'status', 'confirmed',
    'amount', v_withdrawal.amount,
    'new_balance', v_new_balance
  );
end;
$$;

create or replace function public.admin_cancel_withdrawal(p_id bigint)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_withdrawal public.withdrawals%rowtype;
begin
  if not public.is_admin() then
    raise exception 'forbidden';
  end if;

  select * into v_withdrawal
  from public.withdrawals
  where id = p_id
  for update;

  if not found then
    raise exception 'withdrawal_not_found';
  end if;
  if coalesce(v_withdrawal.status, '') <> 'pending' then
    raise exception 'withdrawal_not_pending';
  end if;

  update public.withdrawals
  set status = 'cancelled'
  where id = v_withdrawal.id;

  return jsonb_build_object(
    'record_id', v_withdrawal.id,
    'status', 'cancelled'
  );
end;
$$;

create or replace function public.admin_confirm_binance_deposit(p_id bigint)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_deposit public.binance_deposits%rowtype;
  v_new_balance numeric;
begin
  if not public.is_admin() then
    raise exception 'forbidden';
  end if;

  select * into v_deposit
  from public.binance_deposits
  where id = p_id
  for update;

  if not found then
    raise exception 'binance_deposit_not_found';
  end if;
  if coalesce(v_deposit.status, '') <> 'pending' then
    raise exception 'binance_deposit_not_pending';
  end if;

  update public.users
  set balance_usdt = coalesce(balance_usdt, 0) + coalesce(v_deposit.usdt_amount, 0)
  where user_id = v_deposit.user_id
  returning balance_usdt into v_new_balance;

  if v_new_balance is null then
    raise exception 'user_not_found';
  end if;

  update public.binance_deposits
  set status = 'confirmed'
  where id = v_deposit.id;

  return jsonb_build_object(
    'record_id', v_deposit.id,
    'user_id', v_deposit.user_id,
    'status', 'confirmed',
    'amount', v_deposit.usdt_amount,
    'new_balance', v_new_balance
  );
end;
$$;

create or replace function public.admin_cancel_binance_deposit(p_id bigint)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_deposit public.binance_deposits%rowtype;
begin
  if not public.is_admin() then
    raise exception 'forbidden';
  end if;

  select * into v_deposit
  from public.binance_deposits
  where id = p_id
  for update;

  if not found then
    raise exception 'binance_deposit_not_found';
  end if;
  if coalesce(v_deposit.status, '') <> 'pending' then
    raise exception 'binance_deposit_not_pending';
  end if;

  update public.binance_deposits
  set status = 'cancelled'
  where id = v_deposit.id;

  return jsonb_build_object(
    'record_id', v_deposit.id,
    'status', 'cancelled'
  );
end;
$$;

create or replace function public.admin_confirm_usdt_withdrawal(p_id bigint)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_withdrawal public.usdt_withdrawals%rowtype;
  v_balance numeric;
  v_new_balance numeric;
begin
  if not public.is_admin() then
    raise exception 'forbidden';
  end if;

  select * into v_withdrawal
  from public.usdt_withdrawals
  where id = p_id
  for update;

  if not found then
    raise exception 'usdt_withdrawal_not_found';
  end if;
  if coalesce(v_withdrawal.status, '') <> 'pending' then
    raise exception 'usdt_withdrawal_not_pending';
  end if;

  select balance_usdt into v_balance
  from public.users
  where user_id = v_withdrawal.user_id
  for update;

  if not found then
    raise exception 'user_not_found';
  end if;
  if coalesce(v_balance, 0) < coalesce(v_withdrawal.usdt_amount, 0) then
    raise exception 'insufficient_usdt_balance';
  end if;

  update public.users
  set balance_usdt = coalesce(balance_usdt, 0) - coalesce(v_withdrawal.usdt_amount, 0)
  where user_id = v_withdrawal.user_id
  returning balance_usdt into v_new_balance;

  update public.usdt_withdrawals
  set status = 'confirmed'
  where id = v_withdrawal.id;

  return jsonb_build_object(
    'record_id', v_withdrawal.id,
    'user_id', v_withdrawal.user_id,
    'status', 'confirmed',
    'amount', v_withdrawal.usdt_amount,
    'new_balance', v_new_balance
  );
end;
$$;

create or replace function public.admin_cancel_usdt_withdrawal(p_id bigint)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_withdrawal public.usdt_withdrawals%rowtype;
begin
  if not public.is_admin() then
    raise exception 'forbidden';
  end if;

  select * into v_withdrawal
  from public.usdt_withdrawals
  where id = p_id
  for update;

  if not found then
    raise exception 'usdt_withdrawal_not_found';
  end if;
  if coalesce(v_withdrawal.status, '') <> 'pending' then
    raise exception 'usdt_withdrawal_not_pending';
  end if;

  update public.usdt_withdrawals
  set status = 'cancelled'
  where id = v_withdrawal.id;

  return jsonb_build_object(
    'record_id', v_withdrawal.id,
    'status', 'cancelled'
  );
end;
$$;

grant execute on function public.admin_confirm_deposit(bigint) to authenticated;
grant execute on function public.admin_cancel_deposit(bigint) to authenticated;
grant execute on function public.admin_confirm_withdrawal(bigint) to authenticated;
grant execute on function public.admin_cancel_withdrawal(bigint) to authenticated;
grant execute on function public.admin_confirm_binance_deposit(bigint) to authenticated;
grant execute on function public.admin_cancel_binance_deposit(bigint) to authenticated;
grant execute on function public.admin_confirm_usdt_withdrawal(bigint) to authenticated;
grant execute on function public.admin_cancel_usdt_withdrawal(bigint) to authenticated;
