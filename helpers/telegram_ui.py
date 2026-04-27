import os


DEFAULT_PRODUCT_ICON = "📦"
DEFAULT_FOLDER_ICON = "📁"
DEFAULT_SALE_CUSTOM_EMOJI_ID = "6055192572056309981"
TELEGRAM_BUTTON_TEXT_LIMIT = 64


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def get_inline_button_text_limit() -> int:
    raw_value = os.getenv("BOT_INLINE_BUTTON_TEXT_MAX", str(TELEGRAM_BUTTON_TEXT_LIMIT))
    try:
        value = int(str(raw_value).strip())
    except (TypeError, ValueError):
        value = TELEGRAM_BUTTON_TEXT_LIMIT
    return max(32, min(96, value))


def clean_single_line(value, fallback: str = "") -> str:
    text = " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split())
    return text or fallback


def clip_text(value, limit: int | None = None) -> str:
    text = clean_single_line(value)
    safe_limit = max(1, int(limit or get_inline_button_text_limit()))
    if len(text) <= safe_limit:
        return text
    if safe_limit <= 1:
        return "…"
    return f"{text[:safe_limit - 1].rstrip()}…"


def normalize_product_icon(value, fallback: str = DEFAULT_PRODUCT_ICON) -> str:
    icon = clean_single_line(value)
    if not icon:
        return fallback
    return icon[:16]


def normalize_custom_emoji_id(value) -> str:
    text = clean_single_line(value)
    digits = "".join(char for char in text if char.isdigit())
    return digits[:64]


def get_product_custom_emoji_id(product: dict) -> str:
    return normalize_custom_emoji_id(product.get("telegram_icon_custom_emoji_id"))


def build_product_button_kwargs(product: dict) -> dict:
    custom_emoji_id = get_product_custom_emoji_id(product)
    if not custom_emoji_id:
        return {}
    return {"icon_custom_emoji_id": custom_emoji_id}


def format_vnd_dot(value) -> str:
    return f"{_safe_int(value):,}".replace(",", ".")


def format_price_short(value) -> str:
    amount = _safe_int(value)
    if amount >= 1_000_000:
        return f"{amount // 1_000_000}tr"
    if amount >= 1_000:
        return f"{amount // 1_000}k"
    return str(amount)


def fit_button_text(text: str, limit: int | None = None) -> str:
    return clip_text(text, limit or get_inline_button_text_limit())


def build_folder_button_label(folder: dict, lang: str = "vi", limit: int | None = None) -> str:
    safe_limit = int(limit or get_inline_button_text_limit())
    prefix = f"{DEFAULT_FOLDER_ICON} "
    name = clip_text(folder.get("name") or "", max(1, safe_limit - len(prefix)))
    return fit_button_text(f"{prefix}{name}", safe_limit)


def build_product_button_label(product: dict, lang: str = "vi", limit: int | None = None) -> str:
    safe_limit = int(limit or get_inline_button_text_limit())
    custom_emoji_id = get_product_custom_emoji_id(product)
    icon = "" if custom_emoji_id else normalize_product_icon(product.get("telegram_icon"))
    prefix = f"{icon} " if icon else ""
    stock = _safe_int(product.get("stock"))

    is_sale = bool(product.get("is_sale"))

    if str(lang or "vi").strip().lower() == "en":
        if float(product.get("price_usdt") or 0) > 0:
            price_text = f"{product.get('price_usdt')} USDT"
        else:
            price_text = f"{format_vnd_dot(product.get('price'))}đ"
        status_text = "(sold out)" if stock <= 0 else f"({stock} left)"
    else:
        price_text = f"{format_vnd_dot(product.get('price'))}đ"
        status_text = "(hết hàng)" if stock <= 0 else f"(còn {stock})"

    sale_prefix = "SALE " if is_sale else ""
    suffix = f" - {sale_prefix}{price_text} {status_text}"
    name_limit = safe_limit - len(prefix) - len(suffix)
    if name_limit < 8:
        compact_status = "(0)" if stock <= 0 else f"({stock})"
        compact_price = f"{format_price_short(product.get('price'))}đ"
        compact_sale = "S " if is_sale else ""
        suffix = f" - {compact_sale}{compact_price} {compact_status}"
        name_limit = safe_limit - len(prefix) - len(suffix)

    name = clip_text(product.get("name") or "Product", max(1, name_limit))
    return fit_button_text(f"{prefix}{name}{suffix}", safe_limit)


def build_history_button_label(order: tuple, lang: str = "vi", limit: int | None = None) -> str:
    safe_limit = int(limit or get_inline_button_text_limit())
    order_id, product_name, _content, price, _created_at, quantity = order
    quantity = _safe_int(quantity, 1) or 1
    suffix = f" x{quantity} • {format_price_short(price)}"
    prefix = f"#{order_id} "
    name_limit = safe_limit - len(prefix) - len(suffix)
    name = clip_text(product_name or "Product", max(1, name_limit))
    return fit_button_text(f"{prefix}{name}{suffix}", safe_limit)


def build_product_title(product: dict) -> str:
    custom_emoji_id = get_product_custom_emoji_id(product)
    icon = "" if custom_emoji_id and not product.get("telegram_icon") else normalize_product_icon(product.get("telegram_icon"))
    name = clean_single_line(product.get("name") or "Product")
    return f"{icon} {name}" if icon else name
