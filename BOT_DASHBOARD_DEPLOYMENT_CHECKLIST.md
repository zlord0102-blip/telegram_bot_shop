# Checklist deploy Telegram Bot + Bot Dashboard

Mục tiêu: giữ Telegram Bot và Bot Admin Dashboard đồng bộ về schema, RPC, settings và các luồng bán hàng. File này tập trung vào phần Bot/Dashboard; `storefront-web` không nằm trong phạm vi chính.

## Nguyên tắc chung

- Backup database trước khi apply SQL trên production.
- Luồng ưu tiên hiện tại: apply lại một lần file `supabase_schema_all_in_one.sql`.
- Các file SQL rời vẫn được giữ để review, rollback hoặc apply chọn lọc khi cần, nhưng phần SQL mới phải được mirror vào `supabase_schema_all_in_one.sql`.
- Apply SQL trong Supabase SQL Editor hoặc migration runner với quyền đủ cao.
- Nếu function `RETURNS TABLE` đổi shape, phải `DROP FUNCTION` rồi `CREATE FUNCTION`; `CREATE OR REPLACE` không đủ.
- Không apply lại một file cũ sau file canonical nếu file cũ có thể tạo lại RPC cũ, trừ khi sau đó apply lại all-in-one/canonical.
- Sau khi apply, restart/redeploy Bot và Dashboard để runtime đọc lại contract mới.

## Cách apply đề nghị

1. Backup database.
2. Apply `supabase_schema_all_in_one.sql`.
3. Restart/redeploy Telegram Bot.
4. Restart/redeploy Bot Dashboard.
5. Chạy smoke test bên dưới.

## Thứ tự section bên trong all-in-one

`supabase_schema_all_in_one.sql` cần bao gồm các nhóm logic sau. Danh sách này dùng để kiểm tra nội dung all-in-one, không phải để bạn apply từng file:

1. Schema nền.
2. Admin/users/analytics:
   - `supabase_schema_bot_user_profile_names.sql`
   - `supabase_schema_bot_admin_analytics.sql`
   - `supabase_schema_bot_admin_analytics_perf.sql`
   - `supabase_schema_bot_admin_users_snapshot_v2.sql`
   - `supabase_schema_admin_ops_hardening.sql`
3. Product và catalog Bot:
   - `supabase_schema_product_position.sql`
   - `supabase_schema_product_soft_delete.sql`
   - `supabase_schema_bot_product_folders.sql`
   - `supabase_schema_bot_product_rpc_canonical.sql`
   - `supabase_schema_bot_message_templates.sql`
4. Fulfillment và delivery:
   - `supabase_schema_bot_balance_purchase_fulfillment.sql`
   - `supabase_schema_direct_order_fulfillment.sql`
   - `supabase_schema_bot_delivery_outbox.sql`
5. Finance và payment:
   - `supabase_schema_bot_manual_finance_actions.sql`
   - `supabase_schema_bot_binance_onchain.sql`
6. Broadcast:
   - `supabase_schema_telegram_broadcast_jobs.sql`
7. License Management nếu dùng tab Licenses/API license:
   - `supabase_schema_license_management.sql`
   - `supabase_schema_license_multi_device_keys.sql`

## File canonical quan trọng

`supabase_schema_bot_product_rpc_canonical.sql` đã được mirror vào cuối `supabase_schema_all_in_one.sql`. Section này tạo lại:

- `public.get_products_with_stock()`
- `public.get_product_with_stock(bigint)`
- `public.bot_message_templates`

Contract cần có:

- lọc hidden/soft-delete
- `sort_position`
- `bot_folder_id`
- `telegram_icon`
- `telegram_icon_custom_emoji_id`
- pricing tiers
- promo buy/bonus quantity
- website compatibility fields
- stock count
- editable Bot message copy/custom emoji ID

Nếu apply `supabase_schema_product_soft_delete.sql` sau all-in-one/canonical, RPC có thể mất `bot_folder_id`. Khi đó cần apply lại `supabase_schema_all_in_one.sql` hoặc file canonical.

## Env production tối thiểu

### Telegram Bot

- `BOT_TOKEN`
- `ADMIN_IDS`
- `SUPABASE_URL`
- `SUPABASE_SECRET_KEY`
- `SUPABASE_NETWORK_RETRY_ATTEMPTS=2`
- `SUPABASE_NETWORK_RETRY_DELAY=0.35`
- SePay/Binance env nếu không cấu hình qua bảng `settings`
  - Với Binance, Dashboard `Settings` nên có `binance_pay_id` nếu muốn hiện Cách 1 Binance Pay.
  - Dashboard `Settings` nên có `binance_direct_address` riêng cho Cách 2 BEP20/on-chain; API Key/Secret chỉ dùng cho checker/API, không được nhập vào ô address.

Lưu ý: Bot hiện chỉ dùng Supabase. Bot chỉ nên dùng `SUPABASE_SECRET_KEY` mới cho runtime Supabase. Không dùng `SUPABASE_SERVICE_ROLE_KEY` legacy nếu key đó đã bị lộ, và không dùng publishable/anon key cho Bot vì Bot cần thao tác stock/order/finance/RPC có quyền cao.

### Bot Dashboard

- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY`
- `SUPABASE_SECRET_KEY`
- `ADMIN_ALLOW_UNSAFE_MUTATION_FALLBACK=false` trên production
- `CRON_SECRET` nếu dùng stock custom-check cron
- Tùy chọn custom-check provider nếu muốn self-host/thay endpoint:
  - `STOCK_CUSTOM_CHECK_TEMPMAIL_API_BASE`
  - `STOCK_CUSTOM_CHECK_TINYHOST_API_URL`
  - `STOCK_CUSTOM_CHECK_HOTMAIL_PROXY_URL`
  - `STOCK_CUSTOM_CHECK_HOTMAIL_CLIENT_ID`

Lưu ý custom-check: stock/mail credentials có thể được gửi tới provider đã cấu hình. Production nên dùng endpoint tự host hoặc endpoint bạn kiểm soát.

## Smoke test sau deploy

### SQL/RPC

Chạy query kiểm tra shape RPC:

```sql
select
  id,
  name,
  telegram_icon,
  telegram_icon_custom_emoji_id,
  sort_position,
  bot_folder_id,
  stock
from public.get_products_with_stock()
limit 5;
```

Kết quả mong đợi:

- Query chạy thành công.
- Có cột `sort_position`.
- Có cột `bot_folder_id`.
- Có cột `telegram_icon` và `telegram_icon_custom_emoji_id`.
- Sản phẩm hidden/deleted không xuất hiện trong RPC.

Chạy health snapshot:

```sql
select public.admin_ops_health_snapshot(5);
```

Kết quả mong đợi:

- Query chạy thành công sau khi đã apply all-in-one.
- JSON có nhóm `schema`, `queues`, `delivery`, `stock`, `settings`.

Chạy kiểm tra Bot message templates:

```sql
select template_key, language, custom_emoji_id, enabled
from public.bot_message_templates
order by template_key, language
limit 10;
```

Kết quả mong đợi:

- Query chạy thành công.
- Có các key như `welcome`, `shop_intro`, `sale_intro`, `support_panel`, `product_payment_options`, `quantity_quick_prompt`, `direct_payment_options`.
- `sale_intro` có thể dùng custom emoji ID `6055192572056309981`.
- Trong `/bot-messages`, thử thêm `{emoji:6055192572056309981}` vào đầu một dòng `body_text`; Bot sẽ render thành Telegram custom emoji khi gửi.

### Dashboard

- Login admin thành công.
- Mở `/bot-messages`, sửa thử một template không quan trọng, lưu thành công, rồi đổi lại.
- Products:
  - tạo/sửa sản phẩm
  - nhập fallback icon và Telegram custom emoji ID
  - gán folder Bot
  - đổi vị trí
  - ẩn/bỏ ẩn
  - xóa mềm/khôi phục
- Stock:
  - thêm stock
  - xóa stock
  - bulk edit/delete
- Sales:
  - tạo campaign Sale có `starts_at` / `ends_at`
  - thêm món Sale bằng stock có sẵn
  - thêm món Sale bằng stock mới
  - custom emoji ID mặc định là `6055192572056309981`
  - active/pause/end campaign
  - kiểm tra số stock trống/đang giữ/đã bán
- Direct Orders:
  - VietQR pending có thể duyệt tay
  - pending order có thể mark failed/cancelled
  - Binance on-chain không duyệt tay trong Dashboard
- System Health:
  - mở được trang Health
  - thấy schema checklist, outbox, low-stock, finance pending
  - audit log hiện thao tác admin mới nhất sau khi thử update setting/product/stock
- Users:
  - search theo `user_id`
  - search theo username
  - xem modal đơn theo user
- Reports:
  - đổi period
  - custom month
  - export CSV
- Licenses nếu dùng:
  - tạo key
  - key `Không giới hạn thiết bị` activate được nhiều fingerprint
  - reset một activation

### Telegram Bot

- `/start` hiện menu đúng ngôn ngữ.
- `/help`, `/settings`, `/search <từ khóa>` hoạt động.
- `/sale` mở danh sách Sale đang active.
- Shop top-level hiện folder trước, sản phẩm không folder ở top-level.
- Shop top-level hiện nút Sale khi có Sale active.
- Sale item có custom emoji mặc định, giá Sale, giá gốc, thời gian kết thúc và stock riêng.
- Mua Sale bằng số dư VNĐ/USDT chạy qua RPC Sale và ghi snapshot đơn.
- VietQR/Binance Sale tạo pending direct order, giữ stock tạm thời, checker confirm/fulfill và release hold khi hết hạn.
- Bấm folder hiện đúng sản phẩm trong folder.
- Sort product theo `sort_position`.
- Product button hiện fallback icon hoặc Telegram custom emoji nếu bot có quyền dùng emoji đó.
- Mua bằng số dư trừ stock và tạo order.
- VietQR direct payment tạo pending order và checker confirm/fulfill.
- Binance direct payment tạo pending order và checker match deposit.
- Lịch sử mua hiện order mới.
- Admin Telegram xem stock/sold code/transaction được.
- `/status` admin hiển thị pending queues, outbox và low-stock preview.
- `/emojiid` lấy được custom emoji ID từ message/reply có custom emoji.

## Checklist rollback nhanh

Nếu sau deploy Bot không hiện folder/sản phẩm:

1. Chạy query RPC shape ở mục Smoke test.
2. Nếu thiếu `bot_folder_id`, apply lại `supabase_schema_all_in_one.sql` hoặc `supabase_schema_bot_product_rpc_canonical.sql`.
3. Restart Bot runtime.
4. Clear/reload Dashboard page Products/Stock.

Nếu license API lỗi:

1. Kiểm tra deploy env có `SUPABASE_SECRET_KEY`.
2. Apply `supabase_schema_license_management.sql`.
3. Apply `supabase_schema_license_multi_device_keys.sql`.
4. Restart Dashboard/API.
5. Test lại `/api/licenses/validate`.

## Bước tiếp theo sau checklist này

1. Smoke test production sau khi apply SQL và restart/redeploy.
2. Chạy browser/UI test có đăng nhập khi tool `@browser-use` khả dụng.
3. Theo dõi Health/Audit trong 1-2 ngày đầu để bắt thiếu migration, outbox failed hoặc low-stock.
4. Nếu cần tối ưu sâu hơn: detail drawer cho Products/Stock/Licenses, low-stock Telegram alert, coupon/pinned products, profit stats.
