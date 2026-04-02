from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from locales import get_text


HISTORY_PAGE_SIZE = 5
HISTORY_NAME_PREVIEW = 18


def _format_price_short(price: int) -> str:
    amount = int(price or 0)
    if amount >= 1_000_000:
        return f"{amount // 1_000_000}tr"
    if amount >= 1_000:
        return f"{amount // 1_000}k"
    return str(amount)


def _page_label(lang: str, current_page: int, total_pages: int) -> str:
    if str(lang or "vi").strip().lower() == "en":
        return f"Page {current_page}/{total_pages}"
    return f"Trang {current_page}/{total_pages}"


def _truncate_product_name(name: str) -> str:
    text = str(name or "").strip()
    if len(text) <= HISTORY_NAME_PREVIEW:
        return text
    return f"{text[:HISTORY_NAME_PREVIEW - 1].rstrip()}…"


def build_history_menu(orders: list, lang: str, page: int = 0):
    total_orders = len(orders or [])
    total_pages = max(1, (total_orders + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE)
    safe_page = max(0, min(int(page or 0), total_pages - 1))
    start_index = safe_page * HISTORY_PAGE_SIZE
    page_orders = list((orders or [])[start_index:start_index + HISTORY_PAGE_SIZE])

    keyboard = []
    for order in page_orders:
        order_id, product_name, _content, price, _created_at, quantity = order
        quantity = quantity or 1
        short_name = _truncate_product_name(product_name)
        keyboard.append([
            InlineKeyboardButton(
                f"#{order_id} {short_name} x{quantity} • {_format_price_short(price)}",
                callback_data=f"order_detail_{order_id}",
            )
        ])

    if total_pages > 1:
        nav_row = []
        if safe_page > 0:
            nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"history_page_{safe_page - 1}"))
        nav_row.append(
            InlineKeyboardButton(
                _page_label(lang, safe_page + 1, total_pages),
                callback_data=f"history_page_{safe_page}",
            )
        )
        if safe_page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("➡️", callback_data=f"history_page_{safe_page + 1}"))
        keyboard.append(nav_row)

    keyboard.append([InlineKeyboardButton("🗑 Xóa", callback_data="delete_msg")])

    text = get_text(lang, "history_title")

    return text, InlineKeyboardMarkup(keyboard), safe_page, total_pages
