# Báo cáo kiểm tra tổng thể Project

Phạm vi kiểm tra: toàn bộ repository, bỏ qua `storefront-web`.

- Project Dashboard: `admin_dashboard_telegram_bot`, tập trung vào Dashboard quản trị Telegram Bot trong `app/(admin)`.
- Project Telegram Bot: các file Python/SQL/hỗ trợ ở root repository như `run.py`, `sepay_checker.py`, `database`, `handlers`, `helpers`, `keyboards`, `locales`, `scripts` và các file schema SQL.
- Ghi chú: trong `admin_dashboard_telegram_bot` vẫn có thư mục `app/(website-dashboard)` từ các đợt phát triển Website trước đó. Báo cáo này chỉ xem nó khi phần code/schema dùng chung có ảnh hưởng đến Bot Dashboard.

## Trạng thái kiểm tra UI Dashboard

Yêu cầu dùng `@browser-use` đã được xử lý theo khả năng hiện có: skill `browser-use` đã được đọc, nhưng tool điều khiển browser thực tế không xuất hiện trong danh sách callable của phiên này. Vì vậy chưa thể click-test UI bằng browser thật.

Kiểm tra thay thế đã thực hiện:

- Chạy Dashboard dev server tại `http://127.0.0.1:3001`.
- Trước và sau audit, các route Bot Dashboard đều trả HTTP 200: `/login`, `/`, `/products`, `/stock`, `/orders`, `/direct-orders`, `/deposits`, `/withdrawals`, `/usdt`, `/users`, `/reports`, `/settings`, `/licenses`; batch hardening sau đó bổ sung thêm route `/health`.
- Cảnh báo font `Newsreader` đã được xử lý trong batch hardening sau audit bằng cách bỏ font đó khỏi Dashboard layout.

Kết luận: route/page compile được, nhưng chưa xác nhận được đầy đủ trạng thái đăng nhập, click, modal, form stock custom-check, thao tác duyệt đơn, và các luồng UI có dữ liệu thật.

## Kết luận đồng bộ Dashboard và Bot

Dashboard và Telegram Bot đã đồng bộ khá tốt ở tầng code ứng dụng. Các luồng chính như sản phẩm, folder Bot, stock, giá theo số lượng, khuyến mãi, đơn hàng, direct order, nạp/rút, settings, người dùng, báo cáo, health và audit đều đang dùng cùng nhóm bảng/hàm Supabase.

Rủi ro deploy/schema chính đã được giảm bằng section canonical ở cuối `supabase_schema_all_in_one.sql`, nhưng production vẫn cần apply lại all-in-one một lần để có đủ RPC/bảng/cột mới.

## Ma trận đồng bộ

| Mảng | Dashboard | Telegram Bot | Trạng thái |
| --- | --- | --- | --- |
| Sản phẩm | CRUD, ẩn/xóa mềm/khôi phục, folder Bot, vị trí, giá VND/USDT, tier, promo, mô tả, format, fallback icon, Telegram custom emoji ID | Đọc sản phẩm đang hiển thị, sort theo vị trí, áp folder/tier/promo/stock, hiển thị icon button | Đồng bộ sau khi apply all-in-one |
| Folder Bot | CRUD bảng `bot_product_folders`, gán sản phẩm vào folder | Menu shop hiển thị folder ở top-level, mở folder để xem sản phẩm con | Đồng bộ nếu SQL/RPC đúng bản |
| Stock | Thêm/xóa/sửa/bulk, đếm tổng/sold/còn lại, custom-check | Cấp stock, đánh dấu đã bán, export/quản trị stock | Đồng bộ |
| Orders | Danh sách `orders`, lookup user/product/display name | Tạo order qua mua bằng số dư, USDT hoặc fulfillment direct payment | Đồng bộ |
| Direct Orders | Lọc trạng thái, duyệt tay VietQR, chặn duyệt tay Binance, mark failed/cancel qua API | Tạo pending order, SePay/Binance checker tự xác nhận và fulfill | Đồng bộ |
| Delivery outbox | API fulfillment/health kiểm tra outbox khi giao hàng | Checker gửi, retry, reconcile đơn đã confirm nhưng thiếu giao hàng | Đồng bộ |
| Nạp/rút | Duyệt/từ chối VND deposit, VND withdrawal, USDT withdrawal | User tạo yêu cầu, Bot/Admin xác nhận hoặc hủy | Đồng bộ |
| Settings | Bank, SePay, Binance, support, menu flags, page size, payment mode, relay, secret masking qua server API | Runtime Bot đọc cùng key từ `settings` | Đồng bộ |
| Users | Search/filter/sort, stats mua hàng, đơn theo user, broadcast jobs | Tạo user, ngôn ngữ, số dư, lịch sử, danh sách user broadcast | Đồng bộ |
| Reports | Doanh thu, đơn, direct-order metrics, trend, top sản phẩm, export CSV | Dữ liệu nguồn là bảng Bot | Đồng bộ |
| Health/Audit | System Health page, schema/RPC/settings/queue snapshot, admin audit log | `/status` admin hiển thị pending/outbox/low-stock | Đồng bộ sau khi apply all-in-one |
| Licenses | Quản lý extension/key/activation và API activate/validate | Không thuộc runtime Bot chính | Tính năng riêng của Dashboard |
| SQL deploy | Nhiều migration rời và all-in-one | Bot phụ thuộc shape RPC/table hiện tại | Cần siết lại |

## Trạng thái các vấn đề tối ưu/cải tiến

1. Cần chuẩn hóa migration RPC sản phẩm.
   - `supabase_schema_product_soft_delete.sql` tạo lại `get_products_with_stock()` và `get_product_with_stock(bigint)` nhưng không trả `bot_folder_id`.
   - `supabase_schema_bot_product_folders.sql` tạo lại cùng RPC và có `bot_folder_id`.
   - `supabase_schema_all_in_one.sql` hiện đang kết thúc bằng bản folder-aware, nhưng nếu apply file rời sai thứ tự thì vẫn có thể downgrade RPC.
   - Đã có file canonical `supabase_schema_bot_product_rpc_canonical.sql` và đã mirror vào cuối `supabase_schema_all_in_one.sql`.

2. License schema chưa nằm trong all-in-one.
   - Dashboard có Licenses UI/API đầy đủ, nhưng `supabase_schema_all_in_one.sql` chưa chứa các bảng/RPC `license_extensions`, `license_keys`, `license_activations`, logs và RPC activate/validate.
   - Nên có checklist deploy bắt buộc hoặc một file schema license canonical riêng.

3. Trang Orders đã khớp hơn với Dashboard home.
   - Orders page đã lấy/hiển thị display name.

4. Cảnh báo font `Newsreader` đã dọn.
   - Dashboard layout hiện dùng sans font ổn định hơn cho ops UI.

5. Custom-check stock đang dùng endpoint ngoài hardcode.
   - `app/api/stock/custom-check/shared.ts` hardcode TempMail/TinyHost/Hotmail proxy.
   - Đây là luồng nhạy cảm vì có thể gửi stock/mail credentials ra dịch vụ ngoài.
   - Provider URL đã được đưa vào env với default tương thích cũ; production vẫn nên ưu tiên endpoint tự host hoặc endpoint bạn kiểm soát.

6. Bot hiện đã chuyển sang Supabase-only.
   - `database/__init__.py` import Supabase backend trực tiếp.
   - Runtime Bot cần `SUPABASE_URL` và `SUPABASE_SECRET_KEY`.
   - Không còn hỗ trợ SQLite/local DB fallback; dữ liệu production phải nằm trong Supabase.

7. Settings nhạy cảm đã đi qua server API.
   - SePay token, Binance secret/key và relay bot token không được trả lại browser sau khi lưu.
   - API chỉ báo trạng thái đã cấu hình và chỉ cập nhật secret khi admin nhập giá trị mới.

8. Nhiều mutation quan trọng đã chuyển sang API server-side.
   - Settings secret, product hide/delete/restore, stock add/update/delete/bulk, direct-order fulfill/failed/cancel và finance actions đã có API/audit path.

9. Sale Bot-first đã được bổ sung.
   - Có schema/RPC campaign Sale, item Sale và stock reservation.
   - Dashboard có trang Sales và API server-side để tạo campaign, reserve stock có sẵn hoặc thêm stock mới.
   - Bot có `/sale`, Sale catalog, checkout bằng số dư/VietQR/Binance và snapshot đơn Sale.

10. Keyboard admin Bot có nút cancel rỗng label đã được sửa.

11. Cần chạy UI test thật khi tool browser hoạt động.
   - HTTP route check chỉ xác nhận compile và HTML response.
   - Chưa thay thế được kiểm tra đăng nhập, click, form, modal, trạng thái loading/error và logic thao tác admin.

## Bước tiếp theo được đề xuất

### Ưu tiên 1: deploy/smoke production

1. Backup database.
2. Apply `supabase_schema_all_in_one.sql`.
3. Restart/redeploy Telegram Bot và Admin Dashboard.
4. Kiểm tra `BOT_DASHBOARD_DEPLOYMENT_CHECKLIST.md`, đặc biệt:
   - product RPC có `bot_folder_id`, `telegram_icon`, `telegram_icon_custom_emoji_id`
   - `admin_ops_health_snapshot(5)` chạy được
   - Health page và Audit log hiển thị được
   - Bot `/status`, `/search`, `/emojiid`, `/sale` hoạt động
   - Dashboard Sales tạo được campaign và reserve stock

### Ưu tiên 2: kiểm thử UI và flow thật

1. Khi `@browser-use` khả dụng, chạy lại test có đăng nhập admin.
2. Kiểm tra tối thiểu các flow:
   - Products: tạo/sửa/ẩn/xóa mềm/restore/folder/sort.
   - Stock: thêm/xóa/bulk/custom-check.
   - Sales: tạo campaign, thêm item bằng stock có sẵn, thêm item bằng stock mới, active/pause/end.
   - Direct Orders: duyệt VietQR, chặn Binance manual, trạng thái failed.
   - Users: search/filter/broadcast job.
   - Reports: đổi period/custom month/export CSV.
   - Health/Audit: xem cảnh báo schema/outbox/low-stock và thao tác mới nhất.
   - Licenses: tạo key, unlimited device, reset activation.
3. Test Bot thật trên Telegram với sản phẩm/folder/stock/direct payment và Sale checkout.

### Ưu tiên 3: tối ưu sâu hơn nếu cần

1. Tách Products/Stock/Licenses thành table + detail drawer để giảm độ dài page và thao tác nhanh hơn.
2. Thêm Telegram alert chủ động cho low-stock, delivery outbox failed và checker lỗi liên tiếp.
3. Mở rộng Sale thành coupon/promo code, profit stats hoặc Mini App catalog nếu cần cho vận hành bán hàng quy mô lớn.
