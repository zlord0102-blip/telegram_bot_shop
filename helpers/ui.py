from keyboards import user_reply_keyboard
from database import get_setting, get_ui_flags as _get_ui_flags
from helpers.bot_messages import get_cached_common_button_label, warm_bot_button_labels
from locales import get_text


async def get_ui_flags() -> dict:
    try:
        return await _get_ui_flags()
    except Exception:
        return {}


async def get_user_keyboard(lang: str):
    flags = await get_ui_flags()
    await warm_bot_button_labels(lang)
    labels = {
        "shop": get_cached_common_button_label("reply.shop", lang),
        "balance": get_cached_common_button_label("reply.balance", lang),
        "deposit": get_cached_common_button_label("reply.deposit", lang),
        "withdraw": get_cached_common_button_label("reply.withdraw", lang),
        "history": get_cached_common_button_label("reply.history", lang),
        "support": get_cached_common_button_label("reply.support", lang),
        "language": get_cached_common_button_label("reply.language", lang),
    }
    return user_reply_keyboard(lang, flags, labels=labels)


def _parse_shop_page_size(raw_value: str, default: int = 10) -> int:
    try:
        value = int(str(raw_value).strip())
    except (TypeError, ValueError):
        value = default
    return max(1, min(50, value))


async def get_shop_page_size(default: int = 10) -> int:
    try:
        raw_value = await get_setting("shop_page_size", str(default))
        return _parse_shop_page_size(raw_value, default=default)
    except Exception:
        return default


def _normalize_message_block(raw_value: str, fallback: str) -> str:
    text = str(raw_value or "").strip()
    return text if text else fallback


async def get_shop_menu_text(lang: str) -> str:
    fallback = get_text(lang, "select_product")
    try:
        raw_value = await get_setting("shop_intro_text", "")
    except Exception:
        raw_value = ""
    return _normalize_message_block(raw_value, fallback)


async def get_support_panel_text(lang: str) -> str:
    fallback = (
        "💬 HỖ TRỢ\n\nNhấn nút bên dưới để liên hệ hỗ trợ:"
        if lang != "en"
        else "💬 SUPPORT\n\nTap a button below to contact support:"
    )
    try:
        raw_value = await get_setting("support_panel_text", "")
    except Exception:
        raw_value = ""
    return _normalize_message_block(raw_value, fallback)


async def is_feature_enabled(key: str) -> bool:
    flags = await get_ui_flags()
    return bool(flags.get(key, True))
