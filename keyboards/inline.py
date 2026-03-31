from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
import math


def _format_vnd_dot(amount) -> str:
    try:
        value = int(amount or 0)
    except (TypeError, ValueError):
        value = 0
    return f"{value:,}".replace(",", ".")

def user_reply_keyboard(lang: str = 'vi', flags: dict | None = None):
    flags = flags or {}
    def enabled(key: str, default: bool = True) -> bool:
        return bool(flags.get(key, default))

    def build_rows(buttons: list[str | KeyboardButton]) -> list[list[KeyboardButton]]:
        rows: list[list[KeyboardButton]] = []
        row: list[KeyboardButton] = []
        for label in buttons:
            if isinstance(label, KeyboardButton):
                row.append(label)
            else:
                row.append(KeyboardButton(label))
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        return rows

    if lang == 'en':
        # English: no direct deposit button in reply keyboard; checkout offers direct rails when needed.
        buttons: list[str | KeyboardButton] = []
        if enabled("show_shop"):
            buttons.append("🛒 Shop")
        if enabled("show_balance"):
            buttons.append("💰 Balance")
        if enabled("show_history"):
            buttons.append("📜 History")
        if enabled("show_support"):
            buttons.append("💬 Support")
        if enabled("show_language"):
            buttons.append("🌐 Language")
        keyboard = build_rows(buttons)
    else:
        # Vietnamese: no legacy Binance deposit button in reply keyboard; checkout offers direct rails when needed.
        buttons: list[str | KeyboardButton] = []
        if enabled("show_shop"):
            buttons.append("🛒 Danh mục")
        if enabled("show_balance"):
            buttons.append("💰 Số dư")
        if enabled("show_deposit"):
            buttons.append("➕ Nạp tiền")
        if enabled("show_withdraw"):
            buttons.append("💸 Rút tiền")
        if enabled("show_history"):
            buttons.append("📜 Lịch sử")
        if enabled("show_support"):
            buttons.append("💬 Hỗ trợ")
        if enabled("show_language"):
            buttons.append("🌐 Ngôn ngữ")
        keyboard = build_rows(buttons)
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def admin_reply_keyboard():
    keyboard = [
        [KeyboardButton("📦 Quản lý SP"), KeyboardButton("📥 Thêm stock")],
        [KeyboardButton("📋 Xem stock"), KeyboardButton("📜 Code đã bán")],
        [KeyboardButton("✅ Duyệt giao dịch"), KeyboardButton("🏦 Cài đặt NH")],
        [KeyboardButton("🚪 Thoát Admin")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def main_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton(" Mua hàng", callback_data="shop")],
        [InlineKeyboardButton(" Nạp tiền", callback_data="deposit")],
        [InlineKeyboardButton(" Tài khoản", callback_data="account")],
        [InlineKeyboardButton(" Lịch sử mua", callback_data="history")],
    ]
    return InlineKeyboardMarkup(keyboard)

def admin_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("📦 Quản lý sản phẩm", callback_data="admin_products")],
        [InlineKeyboardButton("📥 Thêm stock", callback_data="admin_add_stock")],
        [InlineKeyboardButton("📋 Xem stock", callback_data="admin_manage_stock")],
        [InlineKeyboardButton("📜 Xem code đã bán", callback_data="admin_sold_codes")],
        [InlineKeyboardButton("💸 Duyệt rút tiền", callback_data="admin_withdraws")],
        [InlineKeyboardButton("🏦 Cài đặt ngân hàng", callback_data="admin_bank_settings")],
        [InlineKeyboardButton("🔙 Quay lại", callback_data="back_main")],
    ]
    return InlineKeyboardMarkup(keyboard)

def admin_sold_codes_keyboard(products):
    """Keyboard chọn sản phẩm để xem code đã bán"""
    keyboard = []
    for p in products:
        keyboard.append([InlineKeyboardButton(f"📦 {p['name']}", callback_data=f"admin_soldby_product_{p['id']}")])
    keyboard.append([InlineKeyboardButton("🔍 Tìm theo User ID", callback_data="admin_soldby_user")])
    keyboard.append([InlineKeyboardButton("🔙 Quay lại", callback_data="admin")])
    return InlineKeyboardMarkup(keyboard)

def products_keyboard(products, lang: str = 'vi', page: int = 0, page_size: int = 10):
    keyboard = []
    total_products = len(products or [])
    total_pages = max(1, math.ceil(total_products / max(1, page_size)))
    safe_page = max(0, min(page, total_pages - 1))

    start = safe_page * page_size
    end = start + page_size
    page_products = (products or [])[start:end]

    for p in page_products:
        if lang == 'en':
            # English: show USDT price only
            stock_text = f"📦 {p['stock']}" if p['stock'] > 0 else "❌ out"
            if p.get('price_usdt') and p['price_usdt'] > 0:
                price_text = f"{p['price_usdt']} USDT"
            else:
                price_text = "N/A"
            label = f"{p['name']} | {price_text} | {stock_text}"
        else:
            # Vietnamese: show VND price (USDT option available when buying)
            stock_text = f"📦 {p['stock']}" if p['stock'] > 0 else "❌ Hết"
            price_text = f"{_format_vnd_dot(p.get('price'))} đ"
            label = f"{p['name']} | {price_text} | {stock_text}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"buy_{p['id']}")])

    if total_pages > 1:
        prev_text = "⬅️ Prev" if lang == "en" else "⬅️ Trước"
        next_text = "Next ➡️" if lang == "en" else "Sau ➡️"
        prev_page = safe_page - 1 if safe_page > 0 else safe_page
        next_page = safe_page + 1 if safe_page < total_pages - 1 else safe_page
        keyboard.append([
            InlineKeyboardButton(prev_text, callback_data=f"shop_{prev_page}"),
            InlineKeyboardButton(f"{safe_page + 1}/{total_pages}", callback_data=f"shop_{safe_page}"),
            InlineKeyboardButton(next_text, callback_data=f"shop_{next_page}"),
        ])

    refresh_text = "🔄 Refresh" if lang == 'en' else "🔄 Cập nhật"
    delete_text = "🗑 Delete" if lang == 'en' else "🗑 Xóa"
    keyboard.append([InlineKeyboardButton(refresh_text, callback_data=f"shop_{safe_page}")])
    keyboard.append([InlineKeyboardButton(delete_text, callback_data="delete_msg")])
    return InlineKeyboardMarkup(keyboard)

def confirm_buy_keyboard(product_id, stock=1, max_can_buy=1):
    keyboard = [[InlineKeyboardButton(" Hủy", callback_data="shop")]]
    return InlineKeyboardMarkup(keyboard)

def deposit_amounts_keyboard():
    amounts = [10000, 20000, 50000, 100000, 200000, 500000]
    keyboard = []
    for i in range(0, len(amounts), 2):
        row = [InlineKeyboardButton(f"{amounts[i]:,}đ", callback_data=f"deposit_{amounts[i]}")]
        if i + 1 < len(amounts):
            row.append(InlineKeyboardButton(f"{amounts[i+1]:,}đ", callback_data=f"deposit_{amounts[i+1]}"))
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🗑 Xóa", callback_data="delete_msg")])
    return InlineKeyboardMarkup(keyboard)

def back_keyboard(callback_data="back_main"):
    return InlineKeyboardMarkup([[InlineKeyboardButton(" Quay lại", callback_data=callback_data)]])

def delete_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🗑 Xóa", callback_data="delete_msg")]])

def admin_products_keyboard(products):
    keyboard = []
    for p in products:
        keyboard.append([
            InlineKeyboardButton(f"📦 {p['name']} - {p['price']:,}đ", callback_data=f"admin_viewprod_{p['id']}"),
            InlineKeyboardButton("❌", callback_data=f"admin_del_{p['id']}")
        ])
    keyboard.append([InlineKeyboardButton("➕ Thêm sản phẩm", callback_data="admin_add_product")])
    keyboard.append([InlineKeyboardButton("🔙 Quay lại", callback_data="admin")])
    return InlineKeyboardMarkup(keyboard)

def admin_stock_keyboard(products):
    keyboard = []
    for p in products:
        keyboard.append([InlineKeyboardButton(f"{p['name']} (còn {p['stock']})", callback_data=f"admin_stock_{p['id']}")])
    keyboard.append([InlineKeyboardButton(" Quay lại", callback_data="admin")])
    return InlineKeyboardMarkup(keyboard)

def admin_view_stock_keyboard(products):
    keyboard = []
    for p in products:
        keyboard.append([InlineKeyboardButton(f" {p['name']} ({p['stock']} stock)", callback_data=f"admin_viewstock_{p['id']}")])
    keyboard.append([InlineKeyboardButton(" Quay lại", callback_data="admin")])
    return InlineKeyboardMarkup(keyboard)

def admin_stock_list_keyboard(stocks, product_id, page=0, per_page=10):
    keyboard = []
    start = page * per_page
    end = start + per_page
    page_stocks = stocks[start:end]
    for s in page_stocks:
        stock_id, content, sold = s
        status = "" if sold else ""
        short_content = content[:20] + "..." if len(content) > 20 else content
        keyboard.append([InlineKeyboardButton(f"{status} {short_content}", callback_data=f"admin_stockdetail_{stock_id}")])
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(" Trước", callback_data=f"admin_stockpage_{product_id}_{page-1}"))
    if end < len(stocks):
        nav_row.append(InlineKeyboardButton("Sau ", callback_data=f"admin_stockpage_{product_id}_{page+1}"))
    if nav_row:
        keyboard.append(nav_row)
    keyboard.append([InlineKeyboardButton(" Quay lại", callback_data="admin_manage_stock")])
    return InlineKeyboardMarkup(keyboard)

def admin_stock_detail_keyboard(stock_id, product_id):
    keyboard = [
        [InlineKeyboardButton(" Sửa nội dung", callback_data=f"admin_editstock_{stock_id}")],
        [InlineKeyboardButton(" Xóa stock", callback_data=f"admin_delstock_{stock_id}_{product_id}")],
        [InlineKeyboardButton(" Quay lại", callback_data=f"admin_viewstock_{product_id}")],
    ]
    return InlineKeyboardMarkup(keyboard)

def pending_deposits_keyboard(deposits):
    keyboard = []
    for d in deposits:
        keyboard.append([
            InlineKeyboardButton(f" #{d[0]} - {d[2]:,}đ", callback_data=f"admin_confirm_{d[0]}"),
            InlineKeyboardButton("", callback_data=f"admin_cancel_{d[0]}")
        ])
    keyboard.append([InlineKeyboardButton(" Quay lại", callback_data="admin")])
    return InlineKeyboardMarkup(keyboard)

def pending_withdrawals_keyboard(withdrawals):
    keyboard = []
    for w in withdrawals:
        keyboard.append([InlineKeyboardButton(f" #{w[0]} - {w[2]:,}đ", callback_data=f"admin_view_{w[0]}")])
    keyboard.append([InlineKeyboardButton(" Quay lại", callback_data="admin")])
    return InlineKeyboardMarkup(keyboard)


def pending_usdt_withdrawals_keyboard(withdrawals):
    """Keyboard cho danh sách yêu cầu rút USDT"""
    keyboard = []
    for w in withdrawals:
        # w: (id, user_id, usdt_amount, wallet_address, network, created_at)
        keyboard.append([InlineKeyboardButton(f"💸 #{w[0]} - {w[2]} USDT", callback_data=f"admin_viewusdt_{w[0]}")])
    keyboard.append([InlineKeyboardButton("🔙 Quay lại", callback_data="admin")])
    return InlineKeyboardMarkup(keyboard)
