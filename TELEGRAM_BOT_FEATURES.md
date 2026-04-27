# Tính năng Telegram Bot

Phạm vi: project Python Telegram Bot ở root repository, không bao gồm `admin_dashboard_telegram_bot` và `storefront-web`.

## Runtime và cấu hình

- Bot chạy bằng `python-telegram-bot`.
- Entrypoint chính: `run.py`.
- Cấu hình qua `.env` và `config.py`.
- Admin lấy từ `ADMIN_IDS`.
- Runtime settings có thể lấy từ env và bảng `settings`.
- Có hỗ trợ `uvloop` trên nền tảng không phải Windows.
- Khi start, Bot chạy Telegram polling và tạo background task cho SePay/Binance checker.
- Callback query answer thường chạy nền với timeout ngắn để inline button không bị cảm giác "quay lâu"; cấu hình qua `BOT_CALLBACK_ANSWER_BACKGROUND` và `BOT_CALLBACK_ANSWER_TIMEOUT`.
- Catalog/folder/Sale list có cache ngắn hạn để giảm round-trip Supabase khi user bấm qua lại menu.
- Các truy vấn đọc Supabase cho catalog/settings/template có retry ngắn và stale-cache fallback; lỗi mạng tạm thời như `httpx.ReadError` không còn làm vỡ toàn bộ handler Shop.

## Lớp database

- `database/__init__.py` import Supabase backend trực tiếp.
- Bot hiện dùng Supabase-only; không còn SQLite/local DB fallback.
- Nhóm thao tác dữ liệu chính:
  - user và ngôn ngữ
  - số dư VND/USDT
  - sản phẩm và folder Bot
  - stock và trạng thái sold
  - orders
  - deposits/withdrawals
  - direct orders
  - Binance direct orders
  - USDT withdrawals
  - settings
  - Telegram message logs
  - delivery outbox
- Supabase backend ưu tiên RPC atomic cho fulfillment và finance.

## Menu người dùng và ngôn ngữ

- `/start` tạo/lấy user và xử lý ngôn ngữ.
- Telegram command menu được đăng ký khi Bot start, gồm lệnh user và lệnh admin theo scope.
- Có các lệnh nhanh:
  - `/help`
  - `/settings`
  - `/shop`
  - `/balance`
  - `/history`
  - `/support`
  - `/search <từ khóa>`
- Hỗ trợ tiếng Việt và tiếng Anh qua `locales`.
- Reply menu có thể bật/tắt từng mục bằng settings:
  - danh mục/shop
  - số dư/tài khoản
  - nạp tiền
  - rút tiền
  - lịch sử
  - ngôn ngữ
  - hỗ trợ
- Panel hỗ trợ đọc `admin_contact` và nhiều link support từ settings.
- Có callback quay về menu chính và xóa message.

## Bot message templates

- Một số message traffic cao có thể chỉnh từ Dashboard thay vì sửa code:
  - welcome
  - shop intro
  - product/Sale payment options
  - quantity quick/manual prompts
  - direct payment options
  - Sale intro/empty
  - support panel
  - history empty
  - feature disabled
- Template hỗ trợ biến như `{name}`.
- Mỗi template có thể gắn Telegram custom emoji ID để Bot render emoji native ở đầu message.
- Trong `body_text`, admin có thể đặt custom emoji ở từng dòng/vị trí bằng cú pháp `{emoji:TELEGRAM_CUSTOM_EMOJI_ID}`.
- Bot cache template trong thời gian ngắn để không làm chậm mỗi lần gửi message.

## Danh mục shop

- Chỉ hiển thị sản phẩm active, không hidden, không deleted.
- Sản phẩm sort theo `sort_position`, sau đó theo ID.
- Hỗ trợ folder/category Bot một cấp:
  - top-level hiển thị folder trước
  - sản phẩm không có folder vẫn nằm ở top-level
  - bấm folder để xem sản phẩm trong folder
  - có nút quay lại top-level đúng page trước đó
- Intro shop lấy từ settings.
- Số sản phẩm mỗi trang lấy từ `shop_page_size`.
- Product card/text hiển thị stock, giá VND, giá USDT, tier pricing, promo và mô tả khi phù hợp.
- Product inline button dùng label dài hơn theo ngưỡng `BOT_INLINE_BUTTON_TEXT_MAX` để tận dụng không gian Telegram tốt hơn.
- Product button hỗ trợ:
  - fallback emoji/text từ `products.telegram_icon`
  - Telegram custom emoji native từ `products.telegram_icon_custom_emoji_id`
- User có thể tìm sản phẩm bằng `/search <từ khóa>`.

## Giá bán và khuyến mãi

- Tính giá theo số lượng.
- Hỗ trợ promo mua X tặng Y.
- Tính số stock cần giao bao gồm bonus.
- Tính tổng tiền VND và USDT.
- Tính số lượng tối đa theo stock và theo số dư.
- Pricing snapshot dùng cho prompt mua hàng và message fulfillment.

## Luồng mua hàng

- User bấm sản phẩm từ catalog.
- Bot hiển thị chi tiết sản phẩm và chọn phương thức thanh toán.
- Payment mode:
  - chỉ số dư
  - chỉ direct payment
  - hybrid số dư/direct
- Có nút chọn nhanh số lượng: 1, 3, 5, 10 và nhập tay.
- Khi nhập tay, Bot dùng `ForceReply` để user trả lời đúng prompt số lượng.
- Nếu nhập số lượng sai hoặc vượt giới hạn, Bot giữ user trong context mua hàng.
- Mua bằng số dư dùng helper/RPC atomic `fulfill_bot_balance_purchase`.
- Sau khi mua thành công, Bot gửi stock đã bán dưới dạng message/file tùy số lượng và nội dung.
- User có thể xem lịch sử mua và chi tiết từng đơn.
- Chi tiết đơn có nút `Mua lại`, quay về lịch sử và mở hỗ trợ theo ngữ cảnh.

## Thanh toán trực tiếp

- VietQR direct order:
  - tạo pending direct order
  - sinh mã thanh toán
  - sinh link VietQR từ bank settings
  - chờ SePay checker hoặc admin duyệt tay
- Binance on-chain direct order:
  - đọc Binance direct settings
  - tính exact amount/suffix để match giao dịch
  - tạo pending order với asset/network/address/payment amount
  - checker tự match Binance deposit
- Đơn direct quá hạn có thể bị auto-cancel.

## Nạp tiền và rút tiền

- Nạp VND:
  - nhập số tiền
  - validate amount
  - tạo mã thanh toán
  - chờ checker/admin xác nhận
- Rút VND:
  - nhập số tiền
  - nhập ngân hàng/số tài khoản hoặc thông tin nhận tiền
  - chặn khi còn yêu cầu pending
  - chờ admin duyệt
- Rút USDT:
  - nhập amount/wallet/network
  - chờ admin duyệt
- Trang tài khoản hiển thị số dư.
- Lịch sử hiển thị đơn đã mua và chi tiết order.

## Admin trong Telegram

- `/admin` mở menu quản trị cho user nằm trong `ADMIN_IDS`.
- Quản lý sản phẩm:
  - xem danh sách sản phẩm
  - thêm sản phẩm
  - xóa sản phẩm qua flow admin Telegram
- Quản lý stock:
  - chọn sản phẩm
  - thêm stock từ text hoặc file `.txt`
  - xem stock theo trang
  - xem chi tiết stock
  - sửa stock
  - xóa stock
  - export stock chưa bán
  - xóa stock chưa bán hoặc toàn bộ stock
- Công cụ code đã bán:
  - xem sold codes theo sản phẩm
  - export sold codes
  - tìm sold codes theo user
- Giao dịch:
  - xem nạp tiền pending
  - confirm/cancel deposit
  - xem rút VND pending
  - xem chi tiết withdrawal
  - confirm/cancel withdrawal
  - xem rút USDT pending
  - confirm/cancel USDT withdrawal
- Cài đặt ngân hàng:
  - bank name
  - account number
  - account name
  - SePay token
  - refresh bank info
- `/notification` gửi broadcast tới user Bot.
- `/emojiid` giúp admin lấy custom emoji ID từ emoji trong message/reply để paste vào Dashboard Products.
- `/status` cho admin xem nhanh trạng thái Supabase env, hàng đợi pending, checker health, delivery outbox và low-stock preview.
- Có reply keyboard admin và flow thoát admin.

## SePay/Binance checker

- Checker chạy nền theo interval trong `run.py`.
- Lưu/đọc last seen SePay transaction ID.
- Gọi API SePay để lấy giao dịch mới.
- Match mã/thông tin thanh toán của direct order Bot.
- Fulfill direct order qua RPC/helper.
- Có code xử lý website direct order trong checker dùng chung, nhưng storefront nằm ngoài phạm vi báo cáo này.
- Xử lý Binance on-chain direct order.
- Lưu external payment ID để tránh xử lý trùng.
- Gửi payment relay notification nếu được cấu hình.
- Auto-cancel direct order quá hạn.
- Quản lý delivery outbox:
  - enqueue
  - gửi hàng
  - retry có delay
  - mark sent/failed
  - reconcile đơn đã confirmed nhưng thiếu delivery

## Logging và an toàn vận hành

- Chat logger ghi incoming Telegram message.
- Tracking processed SePay transaction để chống duplicate.
- Tracking processed Binance deposit để chống duplicate on-chain fulfillment.
- Helper fulfillment map lỗi đã biết sang message dễ hiểu cho user/admin.
- Các RPC fulfillment giúp giảm rủi ro race condition khi bán stock.

## SQL/migration liên quan Bot

- Schema nền:
  - `supabase_schema.sql`
  - `supabase_schema_all_in_one.sql`
- Analytics/reporting Bot.
- Product sort position.
- Product hidden/soft-delete.
- Bot product folders.
- Direct order fulfillment.
- Bot balance purchase fulfillment.
- Bot delivery outbox.
- Manual finance actions.
- Binance on-chain.
- Bot Sale campaigns:
  - campaign có thời gian bắt đầu/kết thúc
  - sale item dùng stock reserve riêng từ stock có sẵn hoặc stock mới
  - custom emoji ID mặc định `6055192572056309981`
  - checkout Sale bằng số dư VNĐ/USDT, VietQR direct hoặc Binance direct
  - order/direct order lưu snapshot giá gốc, giá Sale, discount, campaign/item ID
  - pending direct Sale giữ stock tạm thời và release khi hết hạn
- Telegram broadcast jobs.
- User profile names.
- Admin ops hardening:
  - audit logs
  - health/schema snapshot
  - product icon/custom emoji fields
- License SQL nằm ở root nhưng là tính năng Dashboard/API, không phải runtime Bot chính.

## Bước tiếp theo cho Telegram Bot

### Nên làm ngay

1. Test lại Bot thật với:
   - `/help`, `/settings`, `/search`
   - `/sale`
   - folder sản phẩm
   - Sale catalog, Sale item detail, giới hạn stock và thanh toán Sale
   - sort position
   - product button custom emoji ID
   - mua bằng số dư
   - VietQR direct payment
   - Binance direct payment
   - lịch sử đơn
   - nút `Mua lại`
   - nhập tay số lượng bằng `ForceReply`
   - `/status` admin có outbox/low-stock
   - nạp/rút VND và USDT
2. Kiểm tra production env chỉ còn `SUPABASE_URL` + `SUPABASE_SECRET_KEY` cho Bot.
3. Apply lại `supabase_schema_all_in_one.sql` để production có đủ RPC/bảng/cột mới.

### Nên làm sau

1. Thêm audit log trực tiếp cho các thao tác admin thực hiện trong Telegram, nếu muốn Dashboard và Bot cùng có một lịch sử audit đầy đủ.
2. Bổ sung alert chủ động:
   - low stock gửi Telegram admin
   - delivery outbox failed
   - checker lỗi nhiều lần liên tiếp
3. Tách rõ code Website trong `sepay_checker.py` hoặc đánh dấu section để tránh nhầm phạm vi Bot/Website.
4. Đưa AST/static check Python vào CI bằng kiểu không ghi `__pycache__`.
5. Viết runbook production:
   - env bắt buộc
   - thứ tự apply SQL
   - cách test SePay/Binance
   - cách xử lý delivery outbox failed
6. Tùy nhu cầu kinh doanh: pinned/bestseller products, coupon/discount, profit stats, hoặc Mini App catalog nếu số sản phẩm tăng nhiều.
