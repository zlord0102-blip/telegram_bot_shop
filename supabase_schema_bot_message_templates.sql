-- Telegram Bot message templates.
-- Lets Dashboard admins edit high-traffic bot messages without code deploys.

create table if not exists public.bot_message_templates (
  template_key text not null,
  language text not null default 'vi' check (language in ('vi', 'en')),
  title text not null,
  description text,
  body_text text not null,
  custom_emoji_id text,
  fallback_emoji text,
  enabled boolean not null default true,
  variables jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (template_key, language)
);

create index if not exists bot_message_templates_language_idx
  on public.bot_message_templates (language, template_key);

comment on table public.bot_message_templates is
  'Editable Telegram Bot message copy with optional Telegram custom emoji prefix.';

comment on column public.bot_message_templates.custom_emoji_id is
  'Optional Telegram custom emoji ID rendered as a custom emoji prefix before body_text.';

create or replace function public.set_row_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_bot_message_templates_updated_at on public.bot_message_templates;
create trigger trg_bot_message_templates_updated_at
before update on public.bot_message_templates
for each row
execute function public.set_row_updated_at();

alter table public.bot_message_templates enable row level security;

drop policy if exists "Admins can access bot message templates" on public.bot_message_templates;
create policy "Admins can access bot message templates" on public.bot_message_templates
  for all using (public.is_admin()) with check (public.is_admin());

insert into public.bot_message_templates (
  template_key,
  language,
  title,
  description,
  body_text,
  custom_emoji_id,
  fallback_emoji,
  variables
)
values
  (
    'welcome',
    'vi',
    'Chào mừng',
    'Tin nhắn đầu tiên sau /start.',
    'Chào {name}!',
    null,
    '👋',
    '["name"]'::jsonb
  ),
  (
    'welcome',
    'en',
    'Welcome',
    'First message after /start.',
    'Welcome {name}!',
    null,
    '👋',
    '["name"]'::jsonb
  ),
  (
    'shop_intro',
    'vi',
    'Danh mục sản phẩm',
    'Tin nhắn mở danh mục sản phẩm.',
    'Danh sách sản phẩm\nChọn một mục bên dưới để xem giá, tồn kho và thanh toán.',
    null,
    '🛍',
    '[]'::jsonb
  ),
  (
    'shop_intro',
    'en',
    'Product catalog',
    'Product catalog opening message.',
    'Product catalog\nChoose an item below to view price, stock, and checkout options.',
    null,
    '🛍',
    '[]'::jsonb
  ),
  (
    'sale_entry_button',
    'vi',
    'Nút vào Sale',
    'Nút inline ở đầu danh mục Shop khi có Sale đang mở.',
    'SALE đang mở',
    '6055192572056309981',
    '🔥',
    '[]'::jsonb
  ),
  (
    'sale_entry_button',
    'en',
    'Sale entry button',
    'Inline button at the top of the Shop catalog when Sale is open.',
    'SALE is open',
    '6055192572056309981',
    '🔥',
    '[]'::jsonb
  ),
  (
    'sale_intro',
    'vi',
    'Sale đang mở',
    'Tin nhắn mở danh mục Sale.',
    'SALE đang mở.\nCác deal có thời hạn và số lượng stock riêng, hết là dừng.',
    '6055192572056309981',
    '🔥',
    '[]'::jsonb
  ),
  (
    'sale_intro',
    'en',
    'Sale is open',
    'Sale catalog opening message.',
    'SALE is open.\nThese deals have limited time and limited reserved stock.',
    '6055192572056309981',
    '🔥',
    '[]'::jsonb
  ),
  (
    'sale_empty',
    'vi',
    'Sale trống',
    'Tin nhắn khi chưa có món Sale.',
    'Hiện chưa có món Sale đang hoạt động. Bạn quay lại Shop sau nhé.',
    '6055192572056309981',
    '🔥',
    '[]'::jsonb
  ),
  (
    'sale_empty',
    'en',
    'No active Sale',
    'Message when no Sale item is active.',
    'No active Sale item right now. Please check the Shop again later.',
    '6055192572056309981',
    '🔥',
    '[]'::jsonb
  ),
  (
    'support_panel',
    'vi',
    'Hỗ trợ',
    'Tin nhắn khi user bấm Hỗ trợ.',
    'HỖ TRỢ\n\nNhấn nút bên dưới để liên hệ hỗ trợ:',
    null,
    '💬',
    '[]'::jsonb
  ),
  (
    'support_panel',
    'en',
    'Support',
    'Message when user opens support.',
    'SUPPORT\n\nTap a button below to contact support:',
    null,
    '💬',
    '[]'::jsonb
  ),
  (
    'history_empty',
    'vi',
    'Lịch sử trống',
    'Tin nhắn khi user chưa có đơn hàng.',
    'Bạn chưa có đơn hàng nào!',
    null,
    '📜',
    '[]'::jsonb
  ),
  (
    'history_empty',
    'en',
    'Empty history',
    'Message when user has no orders.',
    'You have no orders yet!',
    null,
    '📜',
    '[]'::jsonb
  ),
  (
    'product_payment_options',
    'vi',
    'Chi tiết sản phẩm',
    'Tin nhắn khi user mở sản phẩm và chọn phương thức thanh toán.',
    '{product_summary}{balance_summary}\n\n{payment_prompt}',
    null,
    null,
    '["product_summary","balance_summary","payment_prompt","product_name","price_vnd","price_usdt","stock","balance_vnd","balance_usdt","max_vnd","max_usdt","payment_mode"]'::jsonb
  ),
  (
    'product_payment_options',
    'en',
    'Product details',
    'Message when user opens a product and chooses a payment method.',
    '{product_summary}{balance_summary}\n\n{payment_prompt}',
    null,
    null,
    '["product_summary","balance_summary","payment_prompt","product_name","price_vnd","price_usdt","stock","balance_vnd","balance_usdt","max_vnd","max_usdt","payment_mode"]'::jsonb
  ),
  (
    'sale_payment_options',
    'vi',
    'Chi tiết Sale',
    'Tin nhắn khi user mở sản phẩm Sale và chọn phương thức thanh toán.',
    '{product_summary}{balance_summary}\n\n{payment_prompt}',
    null,
    null,
    '["product_summary","balance_summary","payment_prompt","product_name","price_vnd","price_usdt","stock","balance_vnd","balance_usdt","max_vnd","max_usdt","payment_mode"]'::jsonb
  ),
  (
    'sale_payment_options',
    'en',
    'Sale details',
    'Message when user opens a Sale item and chooses a payment method.',
    '{product_summary}{balance_summary}\n\n{payment_prompt}',
    null,
    null,
    '["product_summary","balance_summary","payment_prompt","product_name","price_vnd","price_usdt","stock","balance_vnd","balance_usdt","max_vnd","max_usdt","payment_mode"]'::jsonb
  ),
  (
    'quantity_quick_prompt',
    'vi',
    'Chọn số lượng nhanh',
    'Tin nhắn chọn nhanh số lượng mua.',
    '{error_block}💳 Cách thanh toán: {payment_label}\n📦 Sản phẩm: {product_name}\n💰 Số dư hiện tại: {balance_text}\n🧮 Mua tối đa: {max_can_buy}\n\nChọn nhanh số lượng bên dưới hoặc bấm "Nhập tay".',
    null,
    null,
    '["error_block","payment_label","product_name","balance_text","max_can_buy"]'::jsonb
  ),
  (
    'quantity_quick_prompt',
    'en',
    'Quick quantity',
    'Quick quantity picker message.',
    '{error_block}💳 Payment method: {payment_label}\n📦 Product: {product_name}\n💰 Current balance: {balance_text}\n🧮 Max quantity: {max_can_buy}\n\nChoose a quick quantity below or tap "Enter manually".',
    null,
    null,
    '["error_block","payment_label","product_name","balance_text","max_can_buy"]'::jsonb
  ),
  (
    'quantity_manual_prompt',
    'vi',
    'Nhập số lượng',
    'Tin nhắn hướng dẫn user nhập số lượng thủ công.',
    '{error_block}💳 Cách thanh toán: {payment_label}\n📦 Sản phẩm: {product_name}\n💰 Số dư hiện tại: {balance_text}\n🧮 Mua tối đa: {max_can_buy}\n\n✍️ Gửi số lượng bạn muốn mua vào chat.\nVui lòng nhập số nguyên từ 1 đến {max_can_buy}.',
    null,
    null,
    '["error_block","payment_label","product_name","balance_text","max_can_buy"]'::jsonb
  ),
  (
    'quantity_manual_prompt',
    'en',
    'Manual quantity',
    'Message that asks the user to type a quantity manually.',
    '{error_block}💳 Payment method: {payment_label}\n📦 Product: {product_name}\n💰 Current balance: {balance_text}\n🧮 Max quantity: {max_can_buy}\n\n✍️ Send the quantity you want to buy in chat.\nPlease enter a whole number from 1 to {max_can_buy}.',
    null,
    null,
    '["error_block","payment_label","product_name","balance_text","max_can_buy"]'::jsonb
  ),
  (
    'quantity_force_reply_prompt',
    'vi',
    'ForceReply số lượng',
    'Tin nhắn ForceReply ngắn khi user nhập số lượng.',
    '✍️ Nhập số lượng từ 1 đến {max_can_buy}.',
    null,
    null,
    '["max_can_buy"]'::jsonb
  ),
  (
    'quantity_force_reply_prompt',
    'en',
    'Quantity ForceReply',
    'Short ForceReply message for manual quantity input.',
    '✍️ Reply with a quantity from 1 to {max_can_buy}.',
    null,
    null,
    '["max_can_buy"]'::jsonb
  ),
  (
    'direct_payment_options',
    'vi',
    'Chọn thanh toán trực tiếp',
    'Tin nhắn chọn VietQR/Binance khi tạo đơn thanh toán trực tiếp.',
    '🏦 Chọn cách thanh toán\n\n📦 Sản phẩm: {product_name}\n🔢 Số lượng mua: {quantity}\n📥 Số lượng nhận: {delivered_quantity}{bonus_line}\n💰 Tổng thanh toán: {total_price}\n\nChọn một phương thức bên dưới để tạo đơn.',
    null,
    null,
    '["product_name","quantity","delivered_quantity","bonus_quantity","bonus_line","total_price"]'::jsonb
  ),
  (
    'direct_payment_options',
    'en',
    'Direct payment options',
    'Message for choosing VietQR/Binance when creating a direct order.',
    '🏦 Choose a payment method\n\n📦 Product: {product_name}\n🔢 Paid quantity: {quantity}\n📥 Delivered quantity: {delivered_quantity}{bonus_line}\n💰 Total: {total_price}\n\nChoose a method below to create the order.',
    null,
    null,
    '["product_name","quantity","delivered_quantity","bonus_quantity","bonus_line","total_price"]'::jsonb
  ),
  (
    'feature_disabled',
    'vi',
    'Tính năng tạm tắt',
    'Tin nhắn chung khi một tính năng bị tắt từ Dashboard.',
    'Tính năng này đang tạm tắt.',
    null,
    '⚠️',
    '[]'::jsonb
  ),
  (
    'feature_disabled',
    'en',
    'Feature disabled',
    'Generic message when a feature is disabled from Dashboard.',
    'This feature is temporarily disabled.',
    null,
    '⚠️',
    '[]'::jsonb
  )
on conflict (template_key, language) do nothing;

insert into public.bot_message_templates (
  template_key,
  language,
  title,
  description,
  body_text,
  custom_emoji_id,
  fallback_emoji,
  variables
)
select
  seed.template_key,
  localized.language,
  localized.title,
  seed.description,
  localized.body_text,
  null,
  null,
  '[]'::jsonb
from (
  values
    ('reply.shop', 'Nút reply Shop', 'Reply keyboard button mở danh mục Shop.', '🛒 Mua hàng', '🛒 Shop'),
    ('reply.balance', 'Nút reply Số dư', 'Reply keyboard button xem số dư.', '💰 Số dư', '💰 Balance'),
    ('reply.deposit', 'Nút reply Nạp tiền', 'Reply keyboard button tạo lệnh nạp tiền.', '➕ Nạp tiền', '➕ Deposit'),
    ('reply.withdraw', 'Nút reply Rút tiền', 'Reply keyboard button tạo yêu cầu rút tiền.', '💸 Rút tiền', '💸 Withdraw'),
    ('reply.history', 'Nút reply Lịch sử', 'Reply keyboard button mở lịch sử mua.', '📜 Lịch sử mua', '📜 History'),
    ('reply.support', 'Nút reply Hỗ trợ', 'Reply keyboard button mở hỗ trợ.', '💬 Hỗ trợ', '💬 Support'),
    ('reply.language', 'Nút reply Ngôn ngữ', 'Reply keyboard button đổi ngôn ngữ.', '🌐 Ngôn ngữ', '🌐 Language'),
    ('reply.cancel', 'Nút reply Hủy', 'Reply keyboard button hủy thao tác.', '❌ Hủy', '❌ Cancel'),
    ('button.delete', 'Nút Xóa', 'Inline button xóa/ẩn tin nhắn bot.', '🗑 Xóa', '🗑 Delete'),
    ('button.back', 'Nút Quay lại', 'Inline button quay lại màn trước.', '🔙 Quay lại', '🔙 Back'),
    ('button.back_shop', 'Nút quay lại Shop', 'Inline button quay lại danh mục Shop.', '🔙 Shop', '🔙 Shop'),
    ('button.back_product', 'Nút quay lại sản phẩm', 'Inline button quay lại chi tiết sản phẩm.', '🔙 Quay lại sản phẩm', '🔙 Back to product'),
    ('button.refresh', 'Nút Cập nhật', 'Inline button refresh danh sách.', '🔄 Cập nhật', '🔄 Refresh'),
    ('button.prev', 'Nút trang trước', 'Inline pagination previous button.', '⬅️ Trước', '⬅️ Prev'),
    ('button.next', 'Nút trang sau', 'Inline pagination next button.', 'Sau ➡️', 'Next ➡️'),
    ('button.check_status', 'Nút kiểm tra trạng thái', 'Inline button kiểm tra trạng thái đơn thanh toán.', '🔄 Kiểm tra trạng thái', '🔄 Check status'),
    ('button.history', 'Nút Lịch sử', 'Inline button mở lịch sử mua.', '📜 Lịch sử', '📜 History'),
    ('button.support', 'Nút Hỗ trợ', 'Inline button mở hỗ trợ.', '💬 Hỗ trợ', '💬 Support'),
    ('button.account', 'Nút Tài khoản', 'Inline button mở thông tin tài khoản.', '👤 Tài khoản', '👤 Account'),
    ('button.open_shop', 'Nút mở danh mục', 'Inline button mở danh mục Shop.', '🛒 Mở danh mục', '🛒 Open shop'),
    ('button.main_shop', 'Nút menu Shop', 'Inline main menu Shop button.', '🛒 Mua hàng', '🛒 Shop'),
    ('button.main_deposit', 'Nút menu Nạp tiền', 'Inline main menu deposit button.', '💰 Nạp tiền', '💰 Deposit'),
    ('button.rebuy', 'Nút mua lại', 'Inline button mua lại từ lịch sử đơn.', '🛒 Mua lại', '🛒 Buy again'),
    ('button.quick_quantity', 'Nút chọn nhanh số lượng', 'Inline button quay lại chọn số lượng nhanh.', '⚡ Chọn nhanh', '⚡ Quick pick'),
    ('button.manual_quantity', 'Nút nhập tay số lượng', 'Inline button nhập số lượng thủ công.', '✍️ Nhập tay', '✍️ Enter manually'),
    ('button.pay_vnd', 'Nút ví VNĐ', 'Inline button thanh toán bằng ví VNĐ.', '💰 Ví VNĐ', '💰 VND wallet'),
    ('button.pay_usdt', 'Nút ví USDT', 'Inline button thanh toán bằng ví USDT.', '💵 Ví USDT', '💵 USDT wallet'),
    ('button.vietqr', 'Nút VietQR', 'Inline button thanh toán VietQR.', '💳 VietQR', '💳 VietQR'),
    ('button.binance', 'Nút Binance', 'Inline button thanh toán Binance.', '🟡 Binance', '🟡 Binance')
) as seed(template_key, title, description, body_vi, body_en)
cross join lateral (
  values
    ('vi', seed.title, seed.body_vi),
    ('en', seed.title || ' EN', seed.body_en)
) as localized(language, title, body_text)
on conflict (template_key, language) do nothing;

-- ============================================================================
-- END: supabase_schema_bot_message_templates.sql
-- ============================================================================
