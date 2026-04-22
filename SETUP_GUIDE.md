# 📚 Hướng Dẫn Cài Đặt Telegram Shop Bot

Tài liệu hướng dẫn chi tiết từ A-Z để setup bot bán hàng Telegram tự động.

---

## 📋 Mục Lục

1. [Yêu cầu hệ thống](#-yêu-cầu-hệ-thống)
2. [Tạo Bot Telegram](#-bước-1-tạo-bot-telegram)
3. [Lấy Admin ID](#-bước-2-lấy-admin-id)
4. [Đăng ký SePay](#-bước-3-đăng-ký-sepay-nạp-tiền-tự-động)
5. [Cài đặt Bot](#-bước-4-cài-đặt-bot)
6. [Cấu hình .env](#-bước-5-cấu-hình-env)
7. [Chạy Bot](#-bước-6-chạy-bot)
8. [Sử dụng Bot](#-bước-7-sử-dụng-bot)
9. [Deploy với Docker](#-deploy-với-docker-khuyên-dùng)
10. [Xử lý lỗi thường gặp](#-xử-lý-lỗi-thường-gặp)

---

## 💻 Yêu cầu hệ thống

- Python 3.10 trở lên
- Tài khoản Telegram
- Tài khoản ngân hàng (để nhận tiền qua SePay)
- VPS/Server (nếu muốn chạy 24/7)

---

## 🤖 Bước 1: Tạo Bot Telegram

### 1.1. Mở BotFather

1. Mở Telegram, tìm kiếm `@BotFather`
2. Hoặc click trực tiếp: https://t.me/BotFather

### 1.2. Tạo Bot mới

1. Gửi lệnh `/newbot`
2. Nhập **tên hiển thị** cho bot (ví dụ: `Shop Bán Hàng`)
3. Nhập **username** cho bot (phải kết thúc bằng `bot`, ví dụ: `myshop_vn_bot`)

### 1.3. Lưu Bot Token

Sau khi tạo xong, BotFather sẽ gửi cho bạn một **Token** có dạng:
```
7123456789:AAHxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

⚠️ **QUAN TRỌNG**: Lưu token này cẩn thận, không chia sẻ cho ai!

### 1.4. Cấu hình Bot (Tùy chọn)

Gửi các lệnh sau cho BotFather để tùy chỉnh bot:

```
/setdescription - Mô tả bot
/setabouttext - Giới thiệu bot
/setuserpic - Ảnh đại diện bot
/setcommands - Thiết lập menu lệnh
```

Để set commands, gửi:
```
start - Bắt đầu sử dụng bot
admin - Mở panel quản trị (chỉ admin)
```

---

## 🆔 Bước 2: Lấy Admin ID

Admin ID là Telegram User ID của bạn, dùng để phân quyền admin trong bot.

### Cách 1: Dùng @userinfobot

1. Mở Telegram, tìm `@userinfobot`
2. Gửi bất kỳ tin nhắn nào
3. Bot sẽ trả về ID của bạn (dạng số: `123456789`)

### Cách 2: Dùng @RawDataBot

1. Tìm `@RawDataBot` trên Telegram
2. Gửi `/start`
3. Tìm dòng `"id":` trong phần `"from"`

### Thêm nhiều Admin

Nếu muốn có nhiều admin, lấy ID của từng người và phân cách bằng dấu phẩy:
```
ADMIN_IDS=123456789,987654321,111222333
```

---

## 💳 Bước 3: Đăng ký SePay (Nạp tiền tự động)

SePay là dịch vụ giúp bot tự động xác nhận khi có người chuyển khoản.

### 3.1. Đăng ký tài khoản

1. Truy cập https://sepay.vn
2. Click **Đăng ký** và tạo tài khoản
3. Xác thực email

### 3.2. Thêm tài khoản ngân hàng

1. Đăng nhập SePay
2. Vào **Tài khoản ngân hàng** → **Thêm tài khoản**
3. Chọn ngân hàng và nhập thông tin:
   - Số tài khoản
   - Tên chủ tài khoản
4. Liên kết với app ngân hàng (theo hướng dẫn của SePay)

### 3.3. Lấy API Token

1. Vào **Cài đặt** → **API**
2. Tạo **API Token** mới
3. Copy token và lưu lại

⚠️ **Lưu ý**: Token này dùng để bot kiểm tra giao dịch tự động.

---

## 📦 Bước 4: Cài đặt Bot

### 4.1. Clone/Download source code

```bash
# Clone từ git (nếu có)
git clone <repository_url>
cd telegram-shop-bot

# Hoặc giải nén file zip
```

### 4.2. Tạo môi trường ảo (Khuyên dùng)

**Windows:**
```cmd
python -m venv venv
venv\Scripts\activate
```

**Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 4.3. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

---

## ⚙️ Bước 5: Cấu hình .env

### 5.1. Tạo file .env

Copy file mẫu:
```bash
# Windows
copy .env.example .env

# Linux/Mac
cp .env.example .env
```

### 5.2. Chỉnh sửa .env

Mở file `.env` và điền thông tin:

```env
# === BẮT BUỘC ===
BOT_TOKEN=your_telegram_bot_token
ADMIN_IDS=123456789

# === SEPAY (Nạp tiền tự động) ===
SEPAY_API_TOKEN=your_sepay_api_token
SEPAY_BANK_NAME=MBBank
SEPAY_ACCOUNT_NUMBER=0123456789
SEPAY_ACCOUNT_NAME=NGUYEN VAN A
```

### 5.3. Giải thích các biến

| Biến | Mô tả | Ví dụ |
|------|-------|-------|
| `BOT_TOKEN` | Token từ BotFather | `7123456789:AAH...` |
| `ADMIN_IDS` | ID Telegram của admin | `123456789,987654321` |
| `SEPAY_API_TOKEN` | Token API từ SePay | `sepay_xxx...` |
| `SEPAY_BANK_NAME` | Tên ngân hàng | `MBBank`, `Vietcombank`, `Techcombank` |
| `SEPAY_ACCOUNT_NUMBER` | Số tài khoản | `0123456789` |
| `SEPAY_ACCOUNT_NAME` | Tên chủ tài khoản (IN HOA) | `NGUYEN VAN A` |

---

## 🚀 Bước 6: Chạy Bot

### Chạy trực tiếp

```bash
python run.py
```

Nếu thành công, bạn sẽ thấy:
```
✅ Database initialized!
🤖 Bot is starting...
🔄 SePay auto-checker enabled (30s interval)
```

### Dừng bot

Nhấn `Ctrl + C`

---

## 📱 Bước 7: Sử dụng Bot

### Cho User

1. Mở bot trên Telegram
2. Gửi `/start` để bắt đầu
3. Sử dụng các nút:
   - 🆔 **User ID** - Xem ID của bạn
   - 💰 **Số dư** - Xem số dư tài khoản
   - 🛒 **Danh mục** - Xem và mua sản phẩm
   - ➕ **Nạp tiền** - Nạp tiền vào tài khoản
   - 💸 **Rút tiền** - Yêu cầu rút tiền
   - 📜 **Lịch sử** - Xem lịch sử giao dịch

### Cho Admin

1. Gửi `/admin` để mở panel quản trị
2. Các chức năng:
   - 📦 **Quản lý SP** - Thêm/Xóa sản phẩm
   - 📥 **Thêm stock** - Thêm nội dung sản phẩm
   - 💳 **Duyệt rút tiền** - Duyệt yêu cầu rút tiền
   - 🏦 **Cài đặt NH** - Cấu hình ngân hàng

### Thêm sản phẩm

1. `/admin` → 📦 Quản lý SP → ➕ Thêm sản phẩm
2. Nhập tên sản phẩm
3. Nhập giá (VNĐ)
4. Sau đó vào 📥 Thêm stock để thêm nội dung

### Thêm Stock (Nội dung sản phẩm)

1. `/admin` → 📥 Thêm stock
2. Chọn sản phẩm
3. Gửi nội dung (mỗi dòng = 1 stock)

Ví dụ thêm 3 tài khoản:
```
user1@email.com|password1
user2@email.com|password2
user3@email.com|password3
```

---

## 🐳 Deploy với Docker (Khuyên dùng)

Docker giúp chạy bot dễ dàng trên bất kỳ máy nào mà không cần cài Python.

### Bước 1: Cài Docker

**Windows:**
1. Tải Docker Desktop: https://www.docker.com/products/docker-desktop
2. Cài đặt và khởi động lại máy
3. Mở Docker Desktop và đợi nó chạy (icon cá voi xanh ở taskbar)

**Mac:**
1. Tải Docker Desktop: https://www.docker.com/products/docker-desktop
2. Kéo vào Applications và mở

**Linux (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install docker.io docker-compose -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
# Logout và login lại
```

### Bước 2: Chuẩn bị source code

1. Copy toàn bộ thư mục bot vào máy mới
2. Mở Terminal/CMD tại thư mục đó
3. Tạo file `.env` và điền thông tin (xem Bước 5)

### Bước 3: Chạy Bot

```bash
# Build và chạy (lần đầu)
docker-compose up -d --build

# Xem logs
docker-compose logs -f

# Dừng bot
docker-compose stop

# Khởi động lại
docker-compose start

# Xóa hoàn toàn
docker-compose down
```

### Bước 4: Tự động chạy khi bật máy (Tùy chọn)

**Windows:**
- Mở Docker Desktop → Settings → General
- Bật "Start Docker Desktop when you log in"
- Bot sẽ tự chạy vì đã có `restart: always` trong config

**Linux:**
```bash
sudo systemctl enable docker
```

### Lưu ý quan trọng

- ✅ **Data không mất** khi tắt máy (lưu trong thư mục `data/`)
- ✅ **Không cần cài Python** - Docker đã bao gồm tất cả
- ⚠️ Mỗi lần bật máy, đợi Docker khởi động xong (1-2 phút)
- ⚠️ Nếu bot không tự chạy, gõ `docker-compose up -d`

### Chạy với Docker thuần (Nâng cao)

```bash
# Build image
docker build -t telegram-shop-bot .

# Chạy container
docker run -d \
  --name shop_bot \
  --env-file .env \
  -v $(pwd)/data:/app/data \
  --restart always \
  telegram-shop-bot
```

---

## ❓ Xử lý lỗi thường gặp

### 1. Bot không phản hồi

**Nguyên nhân**: Token sai hoặc bot chưa chạy

**Giải pháp**:
- Kiểm tra `BOT_TOKEN` trong `.env`
- Đảm bảo bot đang chạy (`python run.py`)

### 2. Không có quyền Admin

**Nguyên nhân**: `ADMIN_IDS` chưa đúng

**Giải pháp**:
- Kiểm tra ID của bạn bằng @userinfobot
- Cập nhật `ADMIN_IDS` trong `.env`
- Restart bot

### 3. Nạp tiền không tự động cộng

**Nguyên nhân**: SePay chưa cấu hình đúng

**Giải pháp**:
- Kiểm tra `SEPAY_API_TOKEN` đã đúng chưa
- Đảm bảo tài khoản ngân hàng đã liên kết với SePay
- Kiểm tra nội dung chuyển khoản đúng mã nạp tiền

### 4. Lỗi "No module named..."

**Nguyên nhân**: Chưa cài đủ dependencies

**Giải pháp**:
```bash
pip install -r requirements.txt
```

### 5. Lỗi Database

**Nguyên nhân**: File database bị lỗi

**Giải pháp**:
```bash
# Backup và xóa database cũ
mv data/shop.db data/shop.db.backup

# Restart bot (sẽ tạo database mới)
python run.py
```

---

## �️H Chạy trên máy mới (Tóm tắt nhanh)

### Cách 1: Dùng Docker (Đơn giản nhất)

1. Cài Docker Desktop (Windows/Mac) hoặc `docker.io` (Linux)
2. Copy thư mục bot vào máy mới
3. Tạo file `.env` với nội dung:
   ```env
   BOT_TOKEN=your_bot_token
   ADMIN_IDS=your_telegram_id
   ```
4. Mở Terminal tại thư mục bot, chạy:
   ```bash
   docker-compose up -d --build
   ```
5. Done! Bot đang chạy.

### Cách 2: Chạy trực tiếp Python

1. Cài Python 3.10+ từ https://python.org
2. Copy thư mục bot vào máy mới
3. Mở Terminal tại thư mục bot:
   ```bash
   pip install -r requirements.txt
   ```
4. Tạo file `.env` (như trên)
5. Chạy:
   ```bash
   python run.py
   ```

### Checklist trước khi chạy

- [ ] Đã có file `.env` với `BOT_TOKEN` và `ADMIN_IDS`
- [ ] Thư mục `data/` tồn tại (hoặc sẽ được tạo tự động)
- [ ] Docker đang chạy (nếu dùng Docker)

---

## 📞 Hỗ trợ

Nếu gặp vấn đề, hãy:
1. Kiểm tra file `bot.log` để xem lỗi chi tiết
2. Đảm bảo đã làm đúng các bước trong hướng dẫn
3. Liên hệ admin để được hỗ trợ

---

## 📝 Ghi chú

- Bot hỗ trợ Supabase (Postgres + Auth + Storage). SQLite vẫn có thể dùng cho local.
- Logs được lưu tại `bot.log`
- Nên backup thư mục `data/` định kỳ
- Khi deploy production, nên dùng Docker để dễ quản lý

---

## ☁️ Supabase (Postgres + Auth + Storage)

### 1) Tạo schema
Chạy file `supabase_schema.sql` trong Supabase SQL editor.

### 2) Cập nhật .env
Thêm các biến sau (xem mẫu `.env.example`):
```
USE_SUPABASE=true
SUPABASE_URL=...
SUPABASE_PUBLISHABLE_KEY=...
SUPABASE_SECRET_KEY=...
```
```
# Chọn mode thanh toán sản phẩm
# direct: luôn tạo VietQR khi mua hàng
# hybrid: chỉ tạo VietQR khi thiếu balance
# balance: phải nạp balance trước khi mua
PAYMENT_MODE=hybrid
```

### 3) Migrate dữ liệu từ SQLite
```
python scripts/migrate_sqlite_to_supabase.py
```

### 4) Tạo admin cho Dashboard
1. Tạo user trong Supabase Auth (email/password).
2. Lấy `user_id` (UUID).
3. Insert vào bảng `public.admin_users` với role `superadmin` hoặc `admin`.

---

## 🧭 Admin Dashboard (Next.js)

Dashboard nằm trong thư mục `admin-dashboard/`.

### Cấu hình env
Tạo file `admin-dashboard/.env.local`:
```
NEXT_PUBLIC_SUPABASE_URL=...
NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY=...
SUPABASE_SECRET_KEY=...
```

### Chạy local
```
cd admin-dashboard
npm install
npm run dev
```
