import asyncio
import logging

from database import get_active_sale_products, get_bot_product_folders, get_products
from keyboards.inline import folder_products_keyboard, products_keyboard, sale_products_keyboard
from helpers.bot_messages import render_bot_button, render_bot_message, warm_bot_button_labels
from helpers.telegram_ui import DEFAULT_SALE_CUSTOM_EMOJI_ID
from helpers.ui import get_shop_menu_text, get_shop_page_size
from locales import get_text


logger = logging.getLogger(__name__)


def _safe_optional_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_folder_groups(products, folders):
    folder_map = {
        _safe_optional_int(folder.get("id")): folder
        for folder in (folders or [])
        if _safe_optional_int(folder.get("id")) is not None
    }
    grouped_products = {folder_id: [] for folder_id in folder_map.keys()}
    ungrouped_products = []

    for product in products or []:
        folder_id = _safe_optional_int(product.get("bot_folder_id"))
        if folder_id is None or folder_id not in folder_map:
            ungrouped_products.append(product)
            continue
        grouped_products.setdefault(folder_id, []).append(product)

    visible_folders = [
        folder
        for folder_id, folder in folder_map.items()
        if grouped_products.get(folder_id)
    ]
    return visible_folders, grouped_products, ungrouped_products


def _catalog_result(value, fallback, label: str):
    if isinstance(value, Exception):
        logger.warning("Catalog dependency %s failed; using fallback: %s", label, value)
        return fallback
    return value


def _sale_catalog_texts(lang: str):
    if lang == "en":
        return (
            "SALE is open.\n"
            "These deals have limited time and limited reserved stock.",
            "No active Sale item right now. Please check the Shop again later.",
        )
    return (
        "SALE đang mở.\n"
        "Các deal có thời hạn và số lượng stock riêng, hết là dừng.",
        "Hiện chưa có món Sale đang hoạt động. Bạn quay lại Shop sau nhé.",
    )


def _sale_entry_button_fallback(lang: str) -> str:
    return "SALE is open" if lang == "en" else "SALE đang mở"


async def build_shop_top_level_view(lang: str, page: int = 0):
    products_result, sale_products_result, folders_result, page_size_result, text_result, sale_button_result, _labels_result = await asyncio.gather(
        get_products(),
        get_active_sale_products(),
        get_bot_product_folders(),
        get_shop_page_size(),
        get_shop_menu_text(lang),
        render_bot_button(
            "sale_entry_button",
            lang,
            _sale_entry_button_fallback(lang),
            fallback_emoji="🔥",
            fallback_custom_emoji_id=DEFAULT_SALE_CUSTOM_EMOJI_ID,
        ),
        warm_bot_button_labels(lang),
        return_exceptions=True,
    )
    products = _catalog_result(products_result, [], "products")
    sale_products = _catalog_result(sale_products_result, [], "sale_products")
    folders = _catalog_result(folders_result, [], "folders")
    page_size = _catalog_result(page_size_result, 10, "page_size")
    text = _catalog_result(text_result, get_text(lang, "select_product"), "shop_text")
    sale_button = _catalog_result(sale_button_result, None, "sale_button")

    visible_folders, _, _ = _build_folder_groups(products, folders)
    markup = products_keyboard(
        products,
        lang=lang,
        page=page,
        page_size=page_size,
        folders=visible_folders,
        has_sale=bool(sale_products),
        sale_button_text=getattr(sale_button, "text", _sale_entry_button_fallback(lang)),
        sale_button_custom_emoji_id=getattr(sale_button, "custom_emoji_id", DEFAULT_SALE_CUSTOM_EMOJI_ID),
    )
    return text, markup


async def build_shop_top_level_message(lang: str, page: int = 0):
    text, markup = await build_shop_top_level_view(lang, page=page)
    rendered = await render_bot_message(
        "shop_intro",
        lang,
        text,
        fallback_emoji="🛍",
    )
    return rendered, markup


async def build_shop_folder_view(folder_id: int, lang: str, page: int = 0, origin_top_page: int = 0):
    products_result, folders_result, page_size_result, _labels_result = await asyncio.gather(
        get_products(),
        get_bot_product_folders(),
        get_shop_page_size(),
        warm_bot_button_labels(lang),
        return_exceptions=True,
    )
    products = _catalog_result(products_result, [], "folder_products")
    folders = _catalog_result(folders_result, [], "folders")
    page_size = _catalog_result(page_size_result, 10, "page_size")

    visible_folders, grouped_products, _ = _build_folder_groups(products, folders)
    target_folder = next(
        (folder for folder in visible_folders if _safe_optional_int(folder.get("id")) == folder_id),
        None,
    )
    if not target_folder:
        return None

    if lang == "en":
        text = f"📁 {target_folder['name']}\nChoose an item below to view price, stock, and checkout options."
    else:
        text = f"📁 {target_folder['name']}\nChọn sản phẩm bên dưới để xem giá, tồn kho và thanh toán."

    markup = folder_products_keyboard(
        grouped_products.get(folder_id, []),
        folder_id=folder_id,
        origin_top_page=origin_top_page,
        lang=lang,
        page=page,
        page_size=page_size,
    )
    return text, markup


async def build_sale_catalog_view(lang: str, page: int = 0):
    sale_products_result, page_size_result, _labels_result = await asyncio.gather(
        get_active_sale_products(),
        get_shop_page_size(),
        warm_bot_button_labels(lang),
        return_exceptions=True,
    )
    sale_products = _catalog_result(sale_products_result, [], "sale_products")
    page_size = _catalog_result(page_size_result, 10, "page_size")
    text, empty_text = _sale_catalog_texts(lang)

    if not sale_products:
        from keyboards.inline import back_keyboard

        return empty_text, back_keyboard("shop")

    markup = sale_products_keyboard(
        sale_products,
        lang=lang,
        page=page,
        page_size=page_size,
    )
    return text, markup


async def build_sale_catalog_message(lang: str, page: int = 0):
    sale_products_result, page_size_result, _labels_result = await asyncio.gather(
        get_active_sale_products(),
        get_shop_page_size(),
        warm_bot_button_labels(lang),
        return_exceptions=True,
    )
    sale_products = _catalog_result(sale_products_result, [], "sale_products")
    page_size = _catalog_result(page_size_result, 10, "page_size")
    text, empty_text = _sale_catalog_texts(lang)

    if not sale_products:
        from keyboards.inline import back_keyboard

        rendered = await render_bot_message(
            "sale_empty",
            lang,
            empty_text,
            fallback_emoji="🔥",
        )
        return rendered, back_keyboard("shop")

    markup = sale_products_keyboard(
        sale_products,
        lang=lang,
        page=page,
        page_size=page_size,
    )
    rendered = await render_bot_message(
        "sale_intro",
        lang,
        text,
        fallback_emoji="🔥",
    )
    return rendered, markup
