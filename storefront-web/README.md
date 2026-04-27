# Storefront Web (NextJS)

Website storefront tách riêng cho Telegram Shop Bot.

## Tính năng chính

- Giao diện shop kiểu marketplace theo mẫu shopmmo.
- Lấy sản phẩm + tồn kho trực tiếp từ Supabase (`get_products_with_stock`).
- Checkout VietQR tạo `direct_orders` qua Supabase RPC (`create_direct_order_and_get_bank_settings`).
- Poll trạng thái đơn (`pending/confirmed/cancelled/failed`) để bám luồng SePay checker hiện có.
- Tra cứu đơn theo `Telegram ID` (cả `direct_orders` và `orders` đã giao).
- Áp dụng logic từ settings Dashboard:
  - `show_shop`, `show_support`, `show_history`
  - `shop_page_size`
  - `payment_mode` (website ưu tiên flow VietQR/SePay, mode `balance` sẽ báo dùng bot)
- Tự xử lý hiển thị direct order hết hạn 10 phút (đồng bộ logic checker).

## Chạy local

1. Tạo file env:

```bash
cp .env.example .env.local
```

2. Cài dependencies:

```bash
npm install
```

3. Chạy dev:

```bash
npm run dev
```

Mặc định app chạy ở `http://localhost:3000`.

## Lưu ý tích hợp

- Cần `SUPABASE_SECRET_KEY` để API route đọc/ghi bảng đang bị admin-only RLS. Không dùng legacy `SUPABASE_SERVICE_ROLE_KEY` nếu key đó đã bị lộ.
- SePay checker ở bot sẽ tự xử lý xác nhận chuyển khoản và giao hàng sau khi đơn chuyển `confirmed`.
- Người mua phải nhập đúng `Telegram ID` để nhận hàng tự động từ bot.
- Nếu cần auth website riêng (không nhập Telegram ID thủ công), có thể mở rộng ở bước tiếp theo.
