from database import get_bot_product_folders, get_products
from keyboards.inline import folder_products_keyboard, products_keyboard
from helpers.ui import get_shop_menu_text, get_shop_page_size


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


async def build_shop_top_level_view(lang: str, page: int = 0):
    products = await get_products()
    folders = await get_bot_product_folders()
    page_size = await get_shop_page_size()
    text = await get_shop_menu_text(lang)
    visible_folders, _, _ = _build_folder_groups(products, folders)
    markup = products_keyboard(products, lang=lang, page=page, page_size=page_size, folders=visible_folders)
    return text, markup


async def build_shop_folder_view(folder_id: int, lang: str, page: int = 0, origin_top_page: int = 0):
    products = await get_products()
    folders = await get_bot_product_folders()
    page_size = await get_shop_page_size()
    visible_folders, grouped_products, _ = _build_folder_groups(products, folders)
    target_folder = next(
        (folder for folder in visible_folders if _safe_optional_int(folder.get("id")) == folder_id),
        None,
    )
    if not target_folder:
        return None

    if lang == "en":
        text = f"📁 {target_folder['name']}\n\nChoose a product below."
    else:
        text = f"📁 {target_folder['name']}\n\nChọn sản phẩm bên dưới."

    markup = folder_products_keyboard(
        grouped_products.get(folder_id, []),
        folder_id=folder_id,
        origin_top_page=origin_top_page,
        lang=lang,
        page=page,
        page_size=page_size,
    )
    return text, markup
