# Tính năng Admin Dashboard Telegram Bot

Phạm vi: project `admin_dashboard_telegram_bot`, tập trung vào Dashboard quản trị Bot trong `app/(admin)`.

## Nền tảng và phân quyền

- Ứng dụng Next.js 14, React 18, TypeScript.
- Kết nối Supabase phía browser cho phiên admin đã đăng nhập.
- Có API server-side cho:
  - xác thực admin session
  - analytics/reports
  - finance actions
  - direct-order fulfillment
  - direct-order failed/cancel
  - product/stock mutations rủi ro cao
  - settings có secret masking
  - system health/schema snapshot
  - admin audit log
  - license APIs
  - Telegram broadcast jobs
  - rate limit
  - cache/timing helpers
  - service-role operations
- Admin shell bảo vệ toàn bộ route qua `/api/admin/session`.
- Sidebar Bot Dashboard được nhóm theo luồng vận hành:
  - Monitor: Dashboard, Reports
  - Catalog: Products, Stock
  - Fulfillment: Orders, Direct Orders
  - Finance: Deposits, Withdrawals, USDT
  - Customers: Users, Licenses
  - System: Health, Settings
- Sidebar có badge trạng thái Ops dựa trên health snapshot.
- Có trang login và màn hình báo không có quyền admin.

## Dashboard tổng quan

- Hiển thị snapshot vận hành:
  - tổng user
  - tổng order
  - doanh thu
  - pending finance/direct-order activity
  - direct order pending quá hạn
  - delivery outbox failed
  - low-stock products
- Bảng đơn mới nhất có:
  - order ID
  - user ID
  - username
  - display name
  - sản phẩm
  - số lượng
  - giá
  - thời gian
- Dữ liệu lấy qua analytics API/RPC fallback, không chỉ đọc bảng trực tiếp từ client.

## Quản lý sản phẩm

- Danh sách sản phẩm tách theo:
  - đang hiển thị
  - đang ẩn
  - đã xóa mềm
- Tạo/sửa sản phẩm với các trường Bot:
  - tên sản phẩm
  - giá VND
  - giá USDT
  - vị trí sắp xếp trên Bot
  - folder Bot tùy chọn
  - icon fallback dạng emoji/text cho Telegram
  - Telegram custom emoji ID cho icon native trong inline button
  - mô tả
  - format dữ liệu giao hàng
  - giá theo số lượng
  - khuyến mãi mua X tặng Y
- Khi chèn sản phẩm vào một vị trí, Dashboard có logic đẩy vị trí các sản phẩm sau.
- Có ẩn/bỏ ẩn sản phẩm mà không mất lịch sử đơn.
- Có xóa mềm/khôi phục sản phẩm để giữ khóa ngoại với đơn hàng cũ.
- Các thao tác ẩn/bỏ ẩn, xóa mềm/khôi phục, xóa folder đi qua server API và ghi audit log khi DB hỗ trợ.
- Quản lý folder sản phẩm Bot:
  - tạo folder
  - sửa tên/vị trí folder
  - xóa folder
  - gán sản phẩm vào folder
  - xóa folder chỉ gỡ liên kết, không xóa sản phẩm
- Quản lý format template dùng lại cho stock/output.
- Có thông báo tương thích khi database thiếu cột/bảng như `sort_position`, `is_hidden`, `is_deleted`, `bot_product_folders`.

## Quản lý stock

- Chọn sản phẩm theo nhóm active/inactive.
- Thống kê stock:
  - tổng stock
  - đã bán
  - còn lại
- Thêm stock hàng loạt bằng textarea.
- Xóa stock bằng cách paste nội dung cần khớp.
- Bảng stock có phân trang và checkbox chọn dòng.
- Sửa một stock.
- Xóa một stock.
- Bulk edit trạng thái sold/available.
- Bulk delete stock đã chọn.
- Các thao tác thêm/sửa/xóa/bulk stock đi qua server API và ghi audit log khi DB hỗ trợ.
- Custom-check stock:
  - kiểm tra toàn bộ stock của sản phẩm đang chọn
  - hoặc kiểm tra các stock đang chọn trong bảng
  - nguồn check: Hotmail, TempMail, TinyHost
  - cấu hình mail column, sender filter, subject filter, concurrency
  - lưu lịch sử nhập gần đây trong localStorage để gợi ý nhanh
  - chia kết quả thành True, False, Error
  - cho phép xóa stock theo nhóm kết quả
- API custom-check yêu cầu admin session và giới hạn số lượng/concurrency ở server.

## Orders

- Danh sách order có phân trang.
- Hiển thị:
  - order ID
  - user ID
  - username
  - display name
  - sản phẩm
  - số lượng
  - giá
  - thời gian
- Lookup tên sản phẩm, username và display name để bảng dễ đọc hơn.

## Direct Orders

- Danh sách Bot `direct_orders`.
- Lọc theo:
  - pending
  - confirmed
  - failed
  - cancelled
  - all
- Hiển thị sản phẩm, user, số lượng, tổng tiền, mã thanh toán, trạng thái, kênh thanh toán, thời gian.
- Đơn VietQR có thể duyệt tay qua `/api/direct-orders/fulfill`.
- Đơn Binance on-chain không cho duyệt tay tại page này vì được checker tự xác nhận.
- Có thể đánh failed/cancelled cho đơn pending qua server API.
- API fulfillment/status kiểm tra admin session, ưu tiên RPC và phối hợp với delivery outbox/audit log.

## Nạp/rút và tài chính

- Deposits:
  - xem yêu cầu nạp VND pending
  - duyệt hoặc từ chối qua `/api/admin-finance`
- Withdrawals:
  - xem yêu cầu rút VND pending
  - duyệt hoặc từ chối qua `/api/admin-finance`
- USDT:
  - xem yêu cầu rút USDT pending
  - duyệt hoặc từ chối qua `/api/admin-finance`
- Admin finance API ưu tiên RPC để thao tác atomic.
- Unsafe mutation fallback chỉ dùng ngoài môi trường production-like.

## Users và broadcast

- Bảng user có phân trang.
- Tìm theo `user_id` hoặc username.
- Lọc:
  - tất cả
  - có doanh thu
  - chưa có doanh thu
  - có đơn hàng
- Sort:
  - mới nhất
  - cũ nhất
  - username A-Z/Z-A
  - doanh thu
  - số đơn
- Hiển thị:
  - user ID
  - username
  - display name
  - số đơn
  - tổng đã thanh toán
  - số dư VND/USDT
  - ngôn ngữ
  - ngày tạo
- Modal xem đơn theo từng user.
- Broadcast Telegram:
  - soạn nội dung
  - preset title lưu trong `settings`
  - modal xác nhận gửi
  - tạo broadcast job
  - poll tiến độ job
  - hiển thị trạng thái job

## Reports

- Bộ lọc thời gian:
  - hôm nay
  - tháng
  - quý
  - tháng tùy chọn
  - từ trước đến nay
- Có chọn tháng tùy chỉnh và so sánh tháng.
- Metrics doanh thu:
  - doanh thu hiện tại
  - doanh thu kỳ trước
  - delta
  - AOV
- Metrics vận hành order.
- Metrics direct-order:
  - số confirmed
  - số failed/cancelled
  - số pending
  - số pending quá hạn
  - tổng direct order
  - tỷ lệ duyệt
  - tỷ lệ thất bại
- Bảng trend theo ngày/kỳ.
- Bảng top sản phẩm theo doanh thu.
- Có export CSV nhanh từ dữ liệu report đang xem.
- Dùng admin analytics API với RPC-first và fallback logic.

## Licenses

- Quản lý extension:
  - tạo extension code/name/description
  - bật/tắt extension
  - sửa thông tin extension
  - chặn xóa extension khi đã có license key
- Quản lý license key:
  - tạo key theo extension
  - hạn sử dụng
  - ghi chú nội bộ
  - lọc active/expired/revoked
  - revoke/reactivate
  - reset tất cả bind
  - chế độ thiết bị: 1 thiết bị hoặc không giới hạn thiết bị
  - hiển thị số bind active và fingerprint summary
- Quản lý activation:
  - lọc theo extension
  - chỉ xem active
  - reset một activation/fingerprint cụ thể
- Public API:
  - `/api/licenses/activate`
  - `/api/licenses/validate`
  - có rate limit
  - lỗi hạ tầng/RPC trả kiểu service-unavailable để extension không hiểu nhầm là key sai

## Settings

- Bank/SePay:
  - tên ngân hàng
  - số tài khoản
  - tên chủ tài khoản
  - SePay token
- Binance direct payment:
  - bật/tắt
  - API key
  - API secret
  - coin
  - network
  - rate
- Support:
  - admin contact
  - danh sách support contacts
  - nội dung intro shop
  - nội dung panel hỗ trợ
- Payment notification relay:
  - relay bot token
  - target user/chat ID
- Hành vi Bot:
  - số sản phẩm mỗi trang
  - payment mode: balance, direct, hybrid
  - bật/tắt menu shop, balance, deposit, withdraw, history, language, support
- Settings page đọc/ghi qua server API; secret không được trả lại browser sau khi lưu, chỉ hiển thị trạng thái đã cấu hình.

## Sales

- Trang Sales nằm trong nhóm Catalog.
- Quản lý campaign Sale:
  - tạo campaign theo thời gian bắt đầu/kết thúc
  - trạng thái draft/scheduled/active/paused/ended/cancelled
  - giới hạn tổng và giới hạn mỗi user
  - bật cờ notify khi bắt đầu/sắp kết thúc để dành cho broadcast/automation sau này
- Quản lý món Sale:
  - chọn sản phẩm gốc
  - đặt giá Sale VNĐ/USDT
  - giữ nguyên giá gốc bằng snapshot, không sửa `products.price`
  - custom emoji ID mặc định `6055192572056309981`
  - promo mua X tặng Y riêng cho Sale
  - thêm bằng stock có sẵn hoặc thêm stock mới
  - hiển thị stock trống/đang giữ/đã bán
  - bật/tắt item nhanh
- API `/api/admin/sales` chạy server-side và ghi audit cho tạo campaign, đổi trạng thái, reserve stock, thêm stock mới, bật/tắt item.

## System Health và Audit

- Trang Health hiển thị nhanh:
  - schema/table/RPC/column readiness
  - queue direct-order pending/expired
  - delivery outbox pending/retry/failed
  - finance pending
  - low-stock products
  - trạng thái các setting thanh toán quan trọng
- Có audit log admin cho các thao tác nhạy cảm khi migration đã được apply:
  - settings update
  - product hide/delete/restore/folder delete
  - stock add/update/delete/bulk actions
  - direct-order fulfill/failed/cancel
  - finance confirm/cancel
- Audit metadata được redact các khóa nhạy cảm như token/secret/password/key.

## Bước tiếp theo cho Dashboard

### Đã xử lý trong batch hiện tại

1. Orders có display name và Dashboard home có thêm health cards.
2. Sidebar/shell được tổ chức lại theo nhóm vận hành, style thiên về ops dashboard gọn hơn.
3. Settings secret, stock mutations, product hide/delete/restore, direct-order status và finance actions đi qua server API.
4. Thêm audit log + System Health page + RPC/schema checklist.
5. Reports có export CSV.
6. Products hỗ trợ fallback emoji và Telegram custom emoji ID cho inline-button icon.

### Còn cần sau deploy

1. Apply `supabase_schema_all_in_one.sql`, restart/redeploy Bot và Dashboard.
2. Smoke test các luồng chính: Products, Stock, Sales, Direct Orders, Finance, Reports CSV, Health, Audit.
3. Chạy click-test bằng `@browser-use` khi công cụ khả dụng trong phiên làm việc.
4. Nếu muốn tối ưu sâu hơn: tách Products/Stock/Licenses thành table + detail drawer để giảm độ dài page và thao tác nhanh hơn.
