import logging
import random
import string
import io
import html
from decimal import Decimal, ROUND_HALF_UP
from telegram import Update, InputFile, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, ForceReply
from telegram.error import BadRequest
from telegram.ext import ContextTypes, ConversationHandler
from database import (
    get_active_sale_product, get_product, get_balance, search_products,
    get_user_orders, create_deposit_with_settings, get_or_create_user,
    create_direct_order_with_settings, create_sale_direct_order_with_settings,
    get_user_direct_order_by_code,
    create_binance_direct_order, create_binance_sale_direct_order,
    get_user_language, get_balance_usdt,
    fulfill_bot_balance_purchase, fulfill_bot_sale_balance_purchase,
    DirectOrderFulfillmentError, BinanceDirectOrderError
)
from keyboards import (
    products_keyboard, confirm_buy_keyboard,
    main_menu_keyboard, delete_keyboard, back_keyboard
)
from helpers.ui import get_user_keyboard, is_feature_enabled
from helpers.history_menu import build_history_menu
from helpers.menu import delete_last_menu_message, set_last_menu_message, clear_last_menu_message
from helpers.sepay_state import mark_vietqr_message, mark_bot_message
from helpers.formatting import format_stock_items
from helpers.bot_messages import (
    edit_bot_message_text,
    get_cached_common_button_label,
    render_bot_message,
    reply_bot_message,
    warm_bot_button_labels,
)
from helpers.shop_catalog import (
    build_sale_catalog_message,
    build_shop_folder_view,
    build_shop_top_level_message,
)
from helpers.purchase_messages import (
    build_delivery_message,
    build_purchase_summary_text,
)
from helpers.telegram_ui import build_product_button_kwargs, build_product_title
from helpers.telegram_resilience import edit_or_reply_callback_message, safe_answer_callback_query, telegram_api_call
from helpers.binance_client import (
    BinanceApiError,
    BinanceConfigError,
    compute_binance_exact_amount,
    compute_binance_exact_amount_from_asset,
    format_binance_amount,
    get_binance_direct_settings,
    get_binance_direct_runtime,
)
from helpers.pricing import (
    get_max_affordable_quantity,
    get_max_quantity_by_stock,
    get_pricing_snapshot,
    normalize_price_tiers,
)
from config import MOMO_PHONE, MOMO_NAME, ADMIN_IDS, SEPAY_ACCOUNT_NUMBER, SEPAY_BANK_NAME, SEPAY_ACCOUNT_NAME, USDT_RATE, PAYMENT_MODE
from locales import get_text

logger = logging.getLogger(__name__)
QUICK_QUANTITY_CHOICES = (1, 3, 5, 10)


async def search_products_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    keyword = " ".join(context.args or []).strip()

    if not keyword:
        await update.message.reply_text(
            "Gõ /search <tên sản phẩm> để tìm nhanh.\nVí dụ: /search gpt"
            if lang != "en"
            else "Use /search <product name>.\nExample: /search gpt",
            reply_markup=delete_keyboard()
        )
        return

    results = await search_products(keyword, limit=10)
    if not results:
        await update.message.reply_text(
            f"Không tìm thấy sản phẩm khớp: {keyword}"
            if lang != "en"
            else f"No products found for: {keyword}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🛒 Mở danh mục" if lang != "en" else "🛒 Open shop", callback_data="shop")],
                [InlineKeyboardButton("🗑 Xóa" if lang != "en" else "🗑 Delete", callback_data="delete_msg")],
            ])
        )
        return

    keyboard = [
        [
            InlineKeyboardButton(
                build_product_title(product),
                callback_data=f"buy_{int(product['id'])}",
                **build_product_button_kwargs(product),
            )
        ]
        for product in results
    ]
    keyboard.append([
        InlineKeyboardButton("🛒 Danh mục" if lang != "en" else "🛒 Shop", callback_data="shop"),
        InlineKeyboardButton("🗑 Xóa" if lang != "en" else "🗑 Delete", callback_data="delete_msg"),
    ])

    title = "Kết quả tìm kiếm" if lang != "en" else "Search results"
    await update.message.reply_text(
        f"🔎 {title}: {keyword}\n"
        + ("\nChọn sản phẩm bên dưới để xem chi tiết." if lang != "en" else "\nChoose a product below."),
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

def make_file(items: list, header: str = "") -> io.BytesIO:
    """Tạo file nhanh từ list items"""
    if header:
        content = header + "\n" + "="*40 + "\n\n" + "\n\n".join(items)
    else:
        content = "\n\n".join(items)
    buf = io.BytesIO(content.encode('utf-8'))
    buf.seek(0)
    return buf


def order_detail_actions_keyboard(product_id: int | None, lang: str = "vi") -> InlineKeyboardMarkup:
    rows = []
    if product_id:
        rebuy_text = get_cached_common_button_label("button.rebuy", lang)
        rows.append([InlineKeyboardButton(rebuy_text, callback_data=f"buy_{int(product_id)}")])
    history_text = get_cached_common_button_label("button.history", lang)
    support_text = get_cached_common_button_label("button.support", lang)
    rows.append([
        InlineKeyboardButton(history_text, callback_data="history"),
        InlineKeyboardButton(support_text, callback_data="support"),
    ])
    rows.append([InlineKeyboardButton(get_cached_common_button_label("button.delete", lang), callback_data="delete_msg")])
    return InlineKeyboardMarkup(rows)

def format_pricing_rules(product: dict, lang: str = "vi") -> str:
    lines: list[str] = []
    tiers = normalize_price_tiers(product.get("price_tiers"))
    if tiers:
        lines.append("📉 Quantity pricing:" if lang == "en" else "📉 Giá theo số lượng:")
        if lang == "en":
            lines.extend([f"       • From {tier['min_quantity']}: {tier['unit_price']:,}đ" for tier in tiers])
        else:
            lines.extend([f"       • Từ {tier['min_quantity']}: {tier['unit_price']:,}đ" for tier in tiers])

    buy_qty = int(product.get("promo_buy_quantity") or 0)
    bonus_qty = int(product.get("promo_bonus_quantity") or 0)
    if buy_qty > 0 and bonus_qty > 0:
        if lines:
            lines.append("")
        if lang == "en":
            lines.append(f"🎁 Promotion: buy {buy_qty} get {bonus_qty}")
        else:
            lines.append(f"🎁 Khuyến mãi: mua {buy_qty} tặng {bonus_qty}")

    return "\n".join(lines)


def format_product_overview(product: dict, include_usdt_price: bool = False, lang: str = "vi") -> str:
    title = build_product_title(product)
    is_sale = bool(product.get("is_sale"))
    original_price = int(product.get("original_price") or 0)
    if lang == "en":
        lines = [
            title,
            f"💰 {'Sale price' if is_sale else 'Price'}: {int(product['price']):,}đ",
        ]
    else:
        lines = [
            title,
            f"💰 {'Giá SALE' if is_sale else 'Giá'}: {int(product['price']):,}đ",
        ]
    if is_sale and original_price > int(product.get("price") or 0):
        if lang == "en":
            lines.append(f"🏷 Original price: {original_price:,}đ")
        else:
            lines.append(f"🏷 Giá gốc: {original_price:,}đ")
    if is_sale and product.get("ends_at"):
        if lang == "en":
            lines.append(f"⏳ Ends at: {product.get('ends_at')}")
        else:
            lines.append(f"⏳ Kết thúc: {product.get('ends_at')}")
    if include_usdt_price and float(product.get("price_usdt") or 0) > 0:
        lines.append(
            f"💵 USDT Price: {product['price_usdt']} USDT"
            if lang == "en"
            else f"💵 Giá USDT: {product['price_usdt']} USDT"
        )
    lines.append(
        f"📦 In stock: {int(product['stock'])}"
        if lang == "en"
        else f"📦 Còn: {int(product['stock'])}"
    )

    pricing_rules = format_pricing_rules(product, lang=lang)
    if pricing_rules:
        lines.append(pricing_rules)
    return "\n".join(lines)


CHECKOUT_ROUTE_WALLET_VND = "wallet_vnd"
CHECKOUT_ROUTE_WALLET_USDT = "wallet_usdt"
CHECKOUT_ROUTE_VIETQR = "vietqr"
CHECKOUT_ROUTE_BINANCE = "binance"


def normalize_checkout_route(route: str | None) -> str | None:
    route_value = str(route or "").strip().lower()
    if route_value in {
        CHECKOUT_ROUTE_WALLET_VND,
        CHECKOUT_ROUTE_WALLET_USDT,
        CHECKOUT_ROUTE_VIETQR,
        CHECKOUT_ROUTE_BINANCE,
    }:
        return route_value
    return None


def checkout_route_currency(route: str | None) -> str:
    return "usdt" if normalize_checkout_route(route) == CHECKOUT_ROUTE_WALLET_USDT else "vnd"


def checkout_route_has_price(product: dict, route: str | None) -> bool:
    route = normalize_checkout_route(route)
    vnd_price = int(product.get("price") or 0)
    usdt_price = float(product.get("price_usdt") or 0)
    if route == CHECKOUT_ROUTE_WALLET_USDT:
        return usdt_price > 0
    if route == CHECKOUT_ROUTE_BINANCE:
        return usdt_price > 0 or vnd_price > 0
    return vnd_price > 0


def checkout_direct_route_for_language(lang: str) -> str:
    return CHECKOUT_ROUTE_BINANCE if lang == "en" else CHECKOUT_ROUTE_VIETQR


def checkout_wallet_route_for_language(product: dict, lang: str) -> str:
    if lang == "en" and float(product.get("price_usdt") or 0) > 0:
        return CHECKOUT_ROUTE_WALLET_USDT
    return CHECKOUT_ROUTE_WALLET_VND


def checkout_route_label(route: str, product: dict, lang: str) -> str:
    route = normalize_checkout_route(route) or CHECKOUT_ROUTE_WALLET_VND
    if route == CHECKOUT_ROUTE_VIETQR:
        return get_cached_common_button_label("button.vietqr", lang)
    if route == CHECKOUT_ROUTE_BINANCE:
        return get_cached_common_button_label("button.binance", lang)
    if route == CHECKOUT_ROUTE_WALLET_USDT:
        price_usdt = float(product.get("price_usdt") or 0)
        price_text = f" • {price_usdt:g} USDT" if price_usdt > 0 else ""
        return ("💰 Wallet" if lang == "en" else "💵 Ví USDT") + price_text

    preview_vnd_price = int(get_pricing_snapshot(product, 1, "vnd")["unit_price"])
    price_text = f" • {preview_vnd_price:,}đ" if preview_vnd_price > 0 else ""
    return ("💰 Wallet" if lang == "en" else "💰 Ví") + price_text


def build_payment_method_keyboard(
    *,
    product: dict,
    product_id: int,
    lang: str,
    payment_mode: str,
    max_vnd: int,
    max_usdt: int,
    is_sale: bool = False,
) -> InlineKeyboardMarkup:
    keyboard = []
    route_prefix = "salepayroute" if is_sale else "payroute"
    pay_prefix = "salepay" if is_sale else "pay"
    payment_mode = (payment_mode or "hybrid").lower()

    if payment_mode == "direct":
        route = checkout_direct_route_for_language(lang)
        if checkout_route_has_price(product, route):
            keyboard.append([
                InlineKeyboardButton(
                    checkout_route_label(route, product, lang),
                    callback_data=f"{route_prefix}_{route}_{product_id}",
                )
            ])
    elif payment_mode == "hybrid":
        wallet_route = checkout_wallet_route_for_language(product, lang)
        if wallet_route == CHECKOUT_ROUTE_WALLET_USDT:
            if float(product.get("price_usdt") or 0) > 0:
                keyboard.append([
                    InlineKeyboardButton(
                        checkout_route_label(wallet_route, product, lang),
                        callback_data=f"{route_prefix}_{wallet_route}_{product_id}",
                    )
                ])
        elif int(product.get("price") or 0) > 0:
            keyboard.append([
                InlineKeyboardButton(
                    checkout_route_label(wallet_route, product, lang),
                    callback_data=f"{route_prefix}_{wallet_route}_{product_id}",
                )
            ])

        direct_route = checkout_direct_route_for_language(lang)
        if checkout_route_has_price(product, direct_route):
            keyboard.append([
                InlineKeyboardButton(
                    checkout_route_label(direct_route, product, lang),
                    callback_data=f"{route_prefix}_{direct_route}_{product_id}",
                )
            ])
    else:
        if int(product.get("price") or 0) > 0 and max_vnd > 0:
            keyboard.append([
                InlineKeyboardButton(
                    checkout_route_label(CHECKOUT_ROUTE_WALLET_VND, product, lang),
                    callback_data=f"{pay_prefix}_vnd_{product_id}",
                )
            ])

        if float(product.get("price_usdt") or 0) > 0 and max_usdt > 0:
            keyboard.append([
                InlineKeyboardButton(
                    checkout_route_label(CHECKOUT_ROUTE_WALLET_USDT, product, lang),
                    callback_data=f"{pay_prefix}_usdt_{product_id}",
                )
            ])

    keyboard.append([InlineKeyboardButton(get_cached_common_button_label("button.delete", lang), callback_data="delete_msg")])
    return InlineKeyboardMarkup(keyboard)


def clear_buy_state(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("buying_product_id", None)
    context.user_data.pop("buying_sale_item_id", None)
    context.user_data.pop("buying_max", None)
    context.user_data.pop("buying_currency", None)
    context.user_data.pop("buying_payment_route", None)


def persistent_reply_keyboard(
    keyboard: list[list[KeyboardButton]],
    *,
    placeholder: str | None = None,
) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=True,
        input_field_placeholder=placeholder,
    )


async def build_checkout_purchase_context(product: dict, user_id: int, currency: str, route: str | None = None) -> dict:
    payment_mode = await get_payment_mode()
    route = normalize_checkout_route(route)
    user_balance = await get_balance(user_id)
    user_balance_usdt = await get_balance_usdt(user_id)
    max_by_stock = get_max_quantity_by_stock(product, product["stock"])

    if currency == "usdt":
        max_can_buy = (
            get_max_affordable_quantity(product, user_balance_usdt, product["stock"], currency="usdt")
            if product.get("price_usdt", 0) > 0
            else 0
        )
        balance_text_vi = f"{user_balance_usdt:.2f} USDT"
        balance_text_en = f"{user_balance_usdt:.2f} USDT"
        payment_label_vi = "USDT"
        payment_label_en = "USDT"
    else:
        if route == CHECKOUT_ROUTE_WALLET_VND or payment_mode == "balance":
            max_can_buy = (
                get_max_affordable_quantity(product, user_balance, product["stock"], currency="vnd")
                if product.get("price", 0) > 0
                else 0
            )
        elif route == CHECKOUT_ROUTE_BINANCE:
            max_can_buy = max_by_stock if checkout_route_has_price(product, route) else 0
        else:
            max_can_buy = max_by_stock if checkout_route_has_price(product, route) else 0
        balance_text_vi = f"{user_balance:,}đ"
        balance_text_en = f"{user_balance:,}đ"
        payment_label_vi = "VNĐ"
        payment_label_en = "VND"

    if route == CHECKOUT_ROUTE_VIETQR:
        payment_label_vi = "VietQR"
        payment_label_en = "VietQR"
        balance_text_vi = "Không dùng ví"
        balance_text_en = "No wallet balance required"
    elif route == CHECKOUT_ROUTE_BINANCE:
        payment_label_vi = "Binance"
        payment_label_en = "Binance"
        balance_text_vi = "Không dùng ví"
        balance_text_en = "No wallet balance required"
    elif route == CHECKOUT_ROUTE_WALLET_VND:
        payment_label_vi = "Ví"
        payment_label_en = "Wallet"
    elif route == CHECKOUT_ROUTE_WALLET_USDT:
        payment_label_vi = "Ví USDT"
        payment_label_en = "Wallet"

    return {
        "payment_mode": payment_mode,
        "payment_route": route,
        "user_balance": user_balance,
        "user_balance_usdt": user_balance_usdt,
        "max_can_buy": int(max_can_buy),
        "balance_text_vi": balance_text_vi,
        "balance_text_en": balance_text_en,
        "payment_label_vi": payment_label_vi,
        "payment_label_en": payment_label_en,
    }


def build_quantity_keyboard(
    *,
    product_id: int,
    currency: str,
    max_can_buy: int,
    lang: str,
    manual_entry: bool = False,
    is_sale: bool = False,
) -> InlineKeyboardMarkup:
    keyboard: list[list[InlineKeyboardButton]] = []
    delete_text = get_cached_common_button_label("button.delete", lang)
    qty_prefix = "salebuyqty" if is_sale else "buyqty"

    if manual_entry:
        quick_text = get_cached_common_button_label("button.quick_quantity", lang)
        keyboard.append([InlineKeyboardButton(quick_text, callback_data=f"{qty_prefix}quick_{currency}_{product_id}")])
    else:
        quick_buttons = [
            InlineKeyboardButton(str(quantity), callback_data=f"{qty_prefix}_{currency}_{product_id}_{quantity}")
            for quantity in QUICK_QUANTITY_CHOICES
            if quantity <= max_can_buy
        ]

        for index in range(0, len(quick_buttons), 2):
            keyboard.append(quick_buttons[index:index + 2])

        manual_text = get_cached_common_button_label("button.manual_quantity", lang)
        keyboard.append([InlineKeyboardButton(manual_text, callback_data=f"{qty_prefix}manual_{currency}_{product_id}")])

    keyboard.append([InlineKeyboardButton(delete_text, callback_data="delete_msg")])
    return InlineKeyboardMarkup(keyboard)


def build_quantity_prompt_text(
    *,
    product_name: str,
    payment_label: str,
    balance_text: str,
    max_can_buy: int,
    lang: str,
    manual_entry: bool = False,
    error_text: str | None = None,
) -> str:
    if lang == "en":
        prompt = (
            f"💳 Payment method: {payment_label}\n"
            f"📦 Product: {product_name}\n"
            f"💰 Current balance: {balance_text}\n"
            f"🧮 Max quantity: {max_can_buy}\n\n"
        )
        prompt += (
            f'✍️ Send the quantity you want to buy in chat.\nPlease enter a whole number from 1 to {max_can_buy}.'
            if manual_entry
            else 'Choose a quick quantity below or tap "Enter manually".'
        )
    else:
        prompt = (
            f"💳 Cách thanh toán: {payment_label}\n"
            f"📦 Sản phẩm: {product_name}\n"
            f"💰 Số dư hiện tại: {balance_text}\n"
            f"🧮 Mua tối đa: {max_can_buy}\n\n"
        )
        prompt += (
            f"✍️ Gửi số lượng bạn muốn mua vào chat.\nVui lòng nhập số nguyên từ 1 đến {max_can_buy}."
            if manual_entry
            else 'Chọn nhanh số lượng bên dưới hoặc bấm "Nhập tay".'
        )

    if error_text:
        return f"{error_text}\n\n{prompt}"
    return prompt


async def render_quantity_prompt_message(
    *,
    product_name: str,
    payment_label: str,
    balance_text: str,
    max_can_buy: int,
    lang: str,
    manual_entry: bool = False,
    error_text: str | None = None,
):
    fallback = build_quantity_prompt_text(
        product_name=product_name,
        payment_label=payment_label,
        balance_text=balance_text,
        max_can_buy=max_can_buy,
        lang=lang,
        manual_entry=manual_entry,
        error_text=error_text,
    )
    error_block = f"{error_text}\n\n" if error_text else ""
    return await render_bot_message(
        "quantity_manual_prompt" if manual_entry else "quantity_quick_prompt",
        lang,
        fallback,
        variables={
            "product_name": product_name,
            "payment_label": payment_label,
            "balance_text": balance_text,
            "max_can_buy": max_can_buy,
            "error_block": error_block,
        },
    )


async def render_quantity_force_reply_message(*, lang: str, max_can_buy: int):
    fallback = (
        f"✍️ Nhập số lượng từ 1 đến {max_can_buy}."
        if lang == "vi"
        else f"✍️ Reply with a quantity from 1 to {max_can_buy}."
    )
    return await render_bot_message(
        "quantity_force_reply_prompt",
        lang,
        fallback,
        variables={"max_can_buy": max_can_buy},
    )


def build_payment_options_template_payload(
    *,
    product: dict,
    lang: str,
    payment_mode: str,
    user_balance: int,
    user_balance_usdt,
    max_vnd: int,
    max_usdt: int,
    is_sale: bool = False,
    include_usdt_price: bool = False,
) -> tuple[str, dict[str, object]]:
    product_summary = format_product_overview(product, include_usdt_price=include_usdt_price, lang=lang)
    balance_lines: list[str] = []
    product_price = int(product.get("price") or 0)
    product_price_usdt = Decimal(str(product.get("price_usdt") or 0))
    balance_usdt = Decimal(str(user_balance_usdt or 0))

    if lang == "en":
        if payment_mode == "balance":
            if product_price > 0:
                balance_lines.append(f"💳 VND Balance: {int(user_balance or 0):,}đ (max buy {max_vnd})")
            if product_price_usdt > 0:
                balance_lines.append(f"💵 USDT Balance: {balance_usdt:.2f} USDT (max buy {max_usdt})")
            payment_prompt = "❌ Insufficient balance!" if (not is_sale and max_vnd == 0 and max_usdt == 0) else "Select payment method:"
        else:
            if product_price_usdt > 0:
                balance_lines.append(f"💵 USDT Balance: {balance_usdt:.2f} USDT (max buy {max_usdt})")
            payment_prompt = "Select payment method:"
    else:
        if payment_mode == "balance":
            balance_lines.append(f"💳 Số dư VNĐ: {int(user_balance or 0):,}đ (mua tối đa {max_vnd})")
            if is_sale:
                if product_price_usdt > 0:
                    balance_lines.append(f"💵 Số dư USDT: {balance_usdt:.2f} (mua tối đa {max_usdt})")
            else:
                balance_lines.append(f"💵 Số dư USDT: {balance_usdt:.2f} (mua tối đa {max_usdt})")

        if is_sale:
            payment_prompt = "💳 Chọn cách thanh toán Sale:"
        elif payment_mode == "balance" and max_vnd == 0 and max_usdt == 0:
            payment_prompt = "❌ Số dư không đủ. Vui lòng nạp thêm."
        else:
            payment_prompt = "💳 Chọn cách thanh toán:"

    balance_summary = f"\n\n{chr(10).join(balance_lines)}" if balance_lines else ""
    fallback = f"{product_summary}{balance_summary}\n\n{payment_prompt}"
    variables = {
        "product_name": str(product.get("name") or ""),
        "product_summary": product_summary,
        "balance_summary": balance_summary,
        "payment_prompt": payment_prompt,
        "price_vnd": f"{product_price:,}đ",
        "price_usdt": f"{product_price_usdt} USDT",
        "stock": int(product.get("stock") or 0),
        "balance_vnd": f"{int(user_balance or 0):,}đ",
        "balance_usdt": f"{balance_usdt:.2f} USDT",
        "max_vnd": max_vnd,
        "max_usdt": max_usdt,
        "payment_mode": payment_mode,
    }
    return fallback, variables


async def send_quantity_prompt(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    product: dict,
    product_id: int,
    currency: str,
    lang: str,
    max_can_buy: int,
    balance_text: str,
    payment_label: str,
    manual_entry: bool = False,
    error_text: str | None = None,
    is_sale: bool = False,
    message=None,
    query=None,
):
    rendered = await render_quantity_prompt_message(
        product_name=str(product["name"]),
        payment_label=payment_label,
        balance_text=balance_text,
        max_can_buy=max_can_buy,
        lang=lang,
        manual_entry=manual_entry,
        error_text=error_text,
    )
    payload = rendered.to_telegram_kwargs()
    reply_markup = build_quantity_keyboard(
        product_id=product_id,
        currency=currency,
        max_can_buy=max_can_buy,
        lang=lang,
        manual_entry=manual_entry,
        is_sale=is_sale,
    )

    if query is not None:
        try:
            await telegram_api_call(
                lambda: query.edit_message_text(**payload, reply_markup=reply_markup),
                action="send_quantity_prompt.edit_message_text",
            )
        except BadRequest as exc:
            if "Message is not modified" not in str(exc):
                raise
        if manual_entry:
            force_payload = (await render_quantity_force_reply_message(lang=lang, max_can_buy=max_can_buy)).to_telegram_kwargs()
            force_msg = await telegram_api_call(
                lambda: context.bot.send_message(
                    chat_id=query.message.chat_id,
                    **force_payload,
                    reply_markup=ForceReply(
                        selective=True,
                        input_field_placeholder=("Ví dụ: 1" if lang == "vi" else "Example: 1"),
                    ),
                ),
                action="send_quantity_prompt.force_reply",
            )
            set_last_menu_message(context, force_msg)
            return force_msg
        set_last_menu_message(context, query.message)
        return query.message

    if message is not None:
        if manual_entry:
            prompt_msg = await telegram_api_call(
                lambda: message.reply_text(
                    **payload,
                    reply_markup=ForceReply(
                        selective=True,
                        input_field_placeholder=("Ví dụ: 1" if lang == "vi" else "Example: 1"),
                    ),
                ),
                action="send_quantity_prompt.reply_force",
            )
            set_last_menu_message(context, prompt_msg)
            return prompt_msg
        prompt_msg = await telegram_api_call(
            lambda: message.reply_text(**payload, reply_markup=reply_markup),
            action="send_quantity_prompt.reply_text",
        )
        set_last_menu_message(context, prompt_msg)
        return prompt_msg

    return None


async def send_purchase_delivery_result(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    purchased_items: list[str],
    format_data,
    header_lines: list[str],
    filename_base: str,
    success_text: str,
    description: str = "",
    lang: str = "vi",
    reply_markup,
    message=None,
    query=None,
):
    file_buf = make_file(format_stock_items(purchased_items, format_data, html=False), "\n".join(header_lines))
    filename = f"{filename_base}_{len(purchased_items)}.txt"

    if len(purchased_items) > 5:
        if message is not None:
            await message.reply_document(
                document=file_buf,
                filename=filename,
                caption=success_text,
                reply_markup=reply_markup,
            )
        elif query is not None:
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=file_buf,
                filename=filename,
                caption=success_text,
                reply_markup=reply_markup,
            )
        return

    text = build_delivery_message(
        summary_text=success_text,
        purchased_items=purchased_items,
        format_data=format_data,
        description=description,
        lang=lang,
        html=True,
    )

    if message is not None:
        await message.reply_text(text, parse_mode="HTML", reply_markup=reply_markup)
    elif query is not None:
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=reply_markup)

# Bank codes cho VietQR
BANK_CODES = {
    "VietinBank": "970415",
    "Vietcombank": "970436",
    "BIDV": "970418",
    "Agribank": "970405",
    "MBBank": "970422",
    "MB": "970422",
    "Techcombank": "970407",
    "ACB": "970416",
    "VPBank": "970432",
    "TPBank": "970423",
    "Sacombank": "970403",
    "HDBank": "970437",
    "VIB": "970441",
    "SHB": "970443",
    "Eximbank": "970431",
    "MSB": "970426",
    "OCB": "970448",
    "LienVietPostBank": "970449",
    "SeABank": "970440",
    "NamABank": "970428",
    "PVcomBank": "970412",
    "BacABank": "970409",
    "VietABank": "970427",
    "ABBank": "970425",
    "BaoVietBank": "970438",
    "NCB": "970419",
    "Kienlongbank": "970452",
    "VietBank": "970433",
    "MoMo": "MOMO",
    "Momo": "MOMO",
    "momo": "MOMO",
}

def generate_vietqr_url(bank_name: str, account_number: str, account_name: str, amount: int, content: str) -> str:
    """Tạo URL ảnh QR từ VietQR API"""
    bank_code = BANK_CODES.get(bank_name, "970415")  # Default VietinBank
    # VietQR API format
    qr_url = f"https://img.vietqr.io/image/{bank_code}-{account_number}-compact2.png?amount={amount}&addInfo={content}&accountName={account_name.replace(' ', '%20')}"
    return qr_url


async def get_payment_mode() -> str:
    mode = PAYMENT_MODE or "hybrid"
    try:
        from database import get_setting
        mode = await get_setting("payment_mode", PAYMENT_MODE)
    except Exception:
        pass
    mode = (mode or "hybrid").lower()
    if mode not in ("direct", "hybrid", "balance"):
        mode = "hybrid"
    return mode


async def get_binance_runtime_safe():
    try:
        settings = await get_binance_direct_settings()
        if settings.get("valid"):
            return settings
    except Exception as exc:
        logger.warning("Failed to read Binance direct settings for checkout button: %s", exc)
        return None

    try:
        runtime = await get_binance_direct_runtime()
        if runtime.get("available"):
            return runtime
    except (BinanceConfigError, BinanceApiError) as exc:
        logger.warning("Binance direct runtime check failed while rendering checkout button: %s", exc)
        return None
    return None


def _format_vnd(amount: int | float | None) -> str:
    return f"{int(amount or 0):,}đ"


def _html_pre_block(value: object) -> str:
    return f"<pre>{html.escape(str(value or '').strip(), quote=False)}</pre>"


def build_missing_balance_keyboard(
    missing_amount: int,
    lang: str,
    product_id: int | None = None,
    back_callback: str | None = None,
) -> InlineKeyboardMarkup:
    amount = max(5000, int(missing_amount or 0))
    deposit_label = (
        f"➕ Top up {_format_vnd(amount)}"
        if lang == "en"
        else f"➕ Nạp thiếu {_format_vnd(amount)}"
    )
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(deposit_label, callback_data=f"deposit_{amount}")]
    ]
    if back_callback:
        back_label = get_cached_common_button_label("button.back_product", lang)
        rows.append([InlineKeyboardButton(back_label, callback_data=back_callback)])
    elif product_id:
        back_label = get_cached_common_button_label("button.back_product", lang)
        rows.append([InlineKeyboardButton(back_label, callback_data=f"buy_{product_id}")])
    rows.append([InlineKeyboardButton(get_cached_common_button_label("button.delete", lang), callback_data="delete_msg")])
    return InlineKeyboardMarkup(rows)


def build_direct_order_actions_keyboard(
    code: str,
    lang: str,
) -> InlineKeyboardMarkup:
    check_text = get_cached_common_button_label("button.check_status", lang)
    history_text = get_cached_common_button_label("button.history", lang)
    support_text = get_cached_common_button_label("button.support", lang)
    delete_text = get_cached_common_button_label("button.delete", lang)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(check_text, callback_data=f"directstatus:{code}")],
        [
            InlineKeyboardButton(history_text, callback_data="history"),
            InlineKeyboardButton(support_text, callback_data="support"),
        ],
        [InlineKeyboardButton(delete_text, callback_data="delete_msg")],
    ])


async def dismiss_checkout_prompt(query, *, lang: str) -> None:
    if query is None or getattr(query, "message", None) is None:
        return
    try:
        await query.message.delete()
        return
    except Exception:
        pass
    fallback_text = (
        "✅ Đã tạo đơn. Vui lòng xem thông tin thanh toán bên dưới."
        if lang != "en"
        else "✅ Order created. Please use the payment details below."
    )
    try:
        await query.edit_message_text(fallback_text, reply_markup=delete_keyboard())
    except Exception:
        return


def build_direct_order_status_text(order: dict, product_name: str, lang: str) -> str:
    status = str(order.get("status") or "pending").lower()
    channel = str(order.get("payment_channel") or "vietqr")
    channel_text = "Binance" if channel == "binance_onchain" else "VietQR/Bank"
    amount_text = _format_vnd(order.get("amount"))
    code_text = html.escape(str(order.get("code") or ""))
    product_text = html.escape(product_name or f"#{order.get('product_id')}")

    if lang == "en":
        if status == "pending":
            status_line = "⏳ Waiting for payment confirmation."
            hint = "If you have just transferred, wait 1-2 minutes and tap Check status again."
        elif status == "confirmed":
            status_line = "✅ Payment confirmed. The bot should deliver automatically."
            hint = "Open History if you want to view delivered items again."
        elif status == "cancelled":
            status_line = "❌ This order was cancelled."
            hint = "Create a new order if you still want to buy this product."
        else:
            status_line = f"⚠️ Status: {html.escape(status)}."
            hint = "Contact support if this looks wrong."
        return (
            f"🧾 Order status\n\n"
            f"📦 Product: <code>{product_text}</code>\n"
            f"💳 Channel: <code>{channel_text}</code>\n"
            f"💰 Amount: <code>{amount_text}</code>\n"
            f"📝 Code: <code>{code_text}</code>\n\n"
            f"{status_line}\n{hint}"
        )

    if status == "pending":
        status_line = "⏳ Đơn đang chờ hệ thống xác nhận thanh toán."
        hint = "Nếu bạn vừa chuyển khoản, chờ 1-2 phút rồi bấm Kiểm tra trạng thái lại."
    elif status == "confirmed":
        status_line = "✅ Đơn đã được xác nhận. Bot sẽ tự giao hàng nếu chưa gửi."
        hint = "Bạn có thể mở Lịch sử để xem lại hàng đã nhận."
    elif status == "cancelled":
        status_line = "❌ Đơn này đã bị hủy."
        hint = "Nếu vẫn muốn mua, hãy tạo đơn mới."
    else:
        status_line = f"⚠️ Trạng thái: {html.escape(status)}."
        hint = "Liên hệ hỗ trợ nếu trạng thái này không đúng."
    return (
        f"🧾 Trạng thái đơn\n\n"
        f"📦 Sản phẩm: <code>{product_text}</code>\n"
        f"💳 Kênh: <code>{channel_text}</code>\n"
        f"💰 Số tiền: <code>{amount_text}</code>\n"
        f"📝 Mã thanh toán: <code>{code_text}</code>\n\n"
        f"{status_line}\n{hint}"
    )


async def direct_checkout_keyboard(product_id: int, quantity: int, *, lang: str, top_up_amount: int = 0):
    await warm_bot_button_labels(lang)
    route = checkout_direct_route_for_language(lang)
    if route == CHECKOUT_ROUTE_BINANCE:
        rows = [[InlineKeyboardButton(get_cached_common_button_label("button.binance", lang), callback_data=f"directpay_binance_{product_id}_{quantity}")]]
    else:
        rows = [[InlineKeyboardButton(get_cached_common_button_label("button.vietqr", lang), callback_data=f"directpay_vietqr_{product_id}_{quantity}")]]
    if top_up_amount > 0:
        top_up_text = (
            f"➕ Top up missing {_format_vnd(top_up_amount)}"
            if lang == "en"
            else f"➕ Nạp phần thiếu {_format_vnd(top_up_amount)}"
        )
        rows.append([InlineKeyboardButton(top_up_text, callback_data=f"deposit_{max(5000, int(top_up_amount))}")])
    rows.append([InlineKeyboardButton(get_cached_common_button_label("button.delete", lang), callback_data="delete_msg")])
    return InlineKeyboardMarkup(rows)


async def sale_direct_checkout_keyboard(sale_item_id: int, quantity: int, *, lang: str, top_up_amount: int = 0):
    await warm_bot_button_labels(lang)
    route = checkout_direct_route_for_language(lang)
    if route == CHECKOUT_ROUTE_BINANCE:
        rows = [[InlineKeyboardButton(get_cached_common_button_label("button.binance", lang), callback_data=f"saledirectpay_binance_{sale_item_id}_{quantity}")]]
    else:
        rows = [[InlineKeyboardButton(get_cached_common_button_label("button.vietqr", lang), callback_data=f"saledirectpay_vietqr_{sale_item_id}_{quantity}")]]
    if top_up_amount > 0:
        top_up_text = (
            f"➕ Top up missing {_format_vnd(top_up_amount)}"
            if lang == "en"
            else f"➕ Nạp phần thiếu {_format_vnd(top_up_amount)}"
        )
        rows.append([InlineKeyboardButton(top_up_text, callback_data=f"deposit_{max(5000, int(top_up_amount))}")])
    rows.append([InlineKeyboardButton(get_cached_common_button_label("button.delete", lang), callback_data="delete_msg")])
    return InlineKeyboardMarkup(rows)


async def prompt_direct_payment_options(
    *,
    product: dict,
    quantity: int,
    total_price: int,
    bonus_quantity: int,
    lang: str,
    product_id: int,
    top_up_amount: int = 0,
    is_sale: bool = False,
    message=None,
    query=None,
):
    delivered_quantity = quantity + max(0, int(bonus_quantity or 0))
    bonus_line = f"\n🎁 Tặng thêm: {bonus_quantity}" if bonus_quantity else ""
    fallback = (
        "🏦 Chọn cách thanh toán\n\n"
        f"📦 Sản phẩm: {product['name']}\n"
        f"🔢 Số lượng mua: {quantity}\n"
        f"📥 Số lượng nhận: {delivered_quantity}"
        f"{bonus_line}\n"
        f"💰 Tổng thanh toán: {int(total_price):,}đ\n\n"
        "Chọn một phương thức bên dưới để tạo đơn."
    )
    if lang == "en":
        bonus_line = f"\n🎁 Bonus: {bonus_quantity}" if bonus_quantity else ""
        fallback = (
            "🏦 Choose a payment method\n\n"
            f"📦 Product: {product['name']}\n"
            f"🔢 Paid quantity: {quantity}\n"
            f"📥 Delivered quantity: {delivered_quantity}"
            f"{bonus_line}\n"
            f"💰 Total: {int(total_price):,}đ\n\n"
            "Choose a method below to create the order."
        )
    rendered = await render_bot_message(
        "direct_payment_options",
        lang,
        fallback,
        variables={
            "product_name": str(product.get("name") or ""),
            "quantity": quantity,
            "delivered_quantity": delivered_quantity,
            "bonus_quantity": int(bonus_quantity or 0),
            "bonus_line": bonus_line,
            "total_price": f"{int(total_price):,}đ",
        },
    )
    payload = rendered.to_telegram_kwargs()

    if is_sale:
        keyboard = await sale_direct_checkout_keyboard(product_id, quantity, lang=lang, top_up_amount=top_up_amount)
    else:
        keyboard = await direct_checkout_keyboard(product_id, quantity, lang=lang, top_up_amount=top_up_amount)
    if message is not None:
        prompt_msg = await telegram_api_call(
            lambda: message.reply_text(**payload, reply_markup=keyboard),
            action="prompt_direct_payment_options.reply_text",
        )
        return prompt_msg
    if query is not None:
        await telegram_api_call(
            lambda: query.edit_message_text(**payload, reply_markup=keyboard),
            action="prompt_direct_payment_options.edit_message_text",
        )
        return query.message
    return None


async def send_direct_payment(context: ContextTypes.DEFAULT_TYPE, chat_id: int, lang: str, user_id: int,
                              product_id: int, product_name: str, quantity: int, unit_price: int, total_price: int,
                              bonus_quantity: int = 0, sale_item_id: int | None = None):
    pay_code = f"SESALE {user_id}{random.randint(1000, 9999)}" if sale_item_id else f"SEBUY {user_id}{random.randint(1000, 9999)}"
    if sale_item_id:
        bank_settings = await create_sale_direct_order_with_settings(
            user_id=user_id,
            sale_item_id=int(sale_item_id),
            quantity=quantity,
            code=pay_code,
        )
    else:
        bank_settings = await create_direct_order_with_settings(
            user_id=user_id,
            product_id=product_id,
            quantity=quantity,
            unit_price=unit_price,
            amount=int(total_price),
            code=pay_code,
            bonus_quantity=bonus_quantity,
        )
    bank_name = bank_settings['bank_name'] or SEPAY_BANK_NAME
    account_number = bank_settings['account_number'] or SEPAY_ACCOUNT_NUMBER
    account_name = bank_settings['account_name'] or SEPAY_ACCOUNT_NAME

    if account_number:
        delivered_quantity = quantity + max(0, int(bonus_quantity or 0))
        bonus_line = f"🎁 Tặng thêm: <code>{bonus_quantity}</code>\n" if bonus_quantity else ""
        if lang == "en":
            bonus_line = f"🎁 Bonus: <code>{bonus_quantity}</code>\n" if bonus_quantity else ""
        qr_url = generate_vietqr_url(bank_name, account_number, account_name, int(total_price), pay_code)
        if lang == "en":
            text = (
                f"{'🏷 SALE payment details' if sale_item_id else '🏦 Payment details'}\n\n"
                f"📦 Product: <code>{product_name}</code>\n"
                f"🔢 Paid quantity: <code>{quantity}</code>\n"
                f"{bonus_line}"
                f"📥 Delivered quantity: <code>{delivered_quantity}</code>\n\n"
                f"🏦 Bank: <code>{bank_name}</code>\n"
                f"🔢 Account number: <code>{account_number}</code>\n"
                f"👤 Account name: <code>{account_name}</code>\n\n"
                f"💰 Amount: <code>{int(total_price):,}đ</code>\n"
                f"📝 Transfer content: <code>{pay_code}</code>\n\n"
                f"✅ The bot will auto-deliver after the system confirms payment."
            )
        else:
            text = (
                f"{'🏷 Thông tin thanh toán SALE' if sale_item_id else '🏦 Thông tin thanh toán'}\n\n"
                f"📦 Sản phẩm: <code>{product_name}</code>\n"
                f"🔢 Số lượng mua: <code>{quantity}</code>\n"
                f"{bonus_line}"
                f"📥 Số lượng nhận: <code>{delivered_quantity}</code>\n\n"
                f"🏦 Ngân hàng: <code>{bank_name}</code>\n"
                f"🔢 Số tài khoản: <code>{account_number}</code>\n"
                f"👤 Chủ tài khoản: <code>{account_name}</code>\n\n"
                f"💰 Số tiền: <code>{int(total_price):,}đ</code>\n"
                f"📝 Nội dung chuyển khoản: <code>{pay_code}</code>\n\n"
                f"✅ Sau khi hệ thống nhận tiền, bot sẽ tự giao sản phẩm."
            )
        photo_msg = await telegram_api_call(
            lambda: context.bot.send_photo(
                chat_id=chat_id,
                photo=qr_url,
                caption=text,
                parse_mode="HTML",
                reply_markup=build_direct_order_actions_keyboard(pay_code, lang),
            ),
            action="send_direct_payment.send_photo",
        )
        mark_vietqr_message(chat_id, photo_msg.message_id)
    else:
        if lang == "en":
            text = (
                f"📱 MoMo payment\n\n"
                f"📞 Phone: {MOMO_PHONE}\n"
                f"👤 Account name: {MOMO_NAME}\n"
                f"💰 Amount: {int(total_price):,}đ\n"
                f"📝 Transfer content: {pay_code}\n\n"
                f"✅ The bot will auto-deliver after the system confirms payment."
            )
        else:
            text = (
                f"📱 Thanh toán MoMo\n\n"
                f"📞 Số điện thoại: {MOMO_PHONE}\n"
                f"👤 Tên tài khoản: {MOMO_NAME}\n"
                f"💰 Số tiền: {int(total_price):,}đ\n"
                f"📝 Nội dung chuyển khoản: {pay_code}\n\n"
                f"✅ Sau khi hệ thống nhận tiền, bot sẽ tự giao sản phẩm."
            )
        msg = await telegram_api_call(
            lambda: context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=build_direct_order_actions_keyboard(pay_code, lang),
            ),
            action="send_direct_payment.send_message",
        )
        mark_bot_message(chat_id, msg.message_id)
    return {
        "code": pay_code,
        "amount_text": f"{int(total_price):,}đ",
    }


async def send_binance_direct_payment(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    lang: str,
    user_id: int,
    product_id: int,
    product_name: str,
    quantity: int,
    unit_price: int,
    total_price: int,
    bonus_quantity: int = 0,
    quoted_total_asset: float | None = None,
    sale_item_id: int | None = None,
):
    runtime = await get_binance_direct_runtime()
    if not runtime.get("available"):
        raise BinanceConfigError("binance_direct_unavailable")

    code = f"BNSALE {user_id}{random.randint(1000, 9999)}" if sale_item_id else f"BNBUY {user_id}{random.randint(1000, 9999)}"
    created_order = None
    exact_amount_text = ""

    rate_vnd = Decimal(str(runtime["rate_vnd"]))
    quoted_asset_amount = Decimal(str(quoted_total_asset or 0)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP) if quoted_total_asset else Decimal("0")
    amount_vnd_for_order = int(total_price)
    unit_price_vnd_for_order = int(unit_price)
    if quoted_asset_amount > 0 and quantity > 0:
        amount_vnd_for_order = int((quoted_asset_amount * rate_vnd).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        per_unit_asset = (quoted_asset_amount / Decimal(quantity)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        unit_price_vnd_for_order = int((per_unit_asset * rate_vnd).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

    for _ in range(50):
        suffix = random.randint(1, 999)
        if quoted_asset_amount > 0:
            exact_amount = compute_binance_exact_amount_from_asset(quoted_asset_amount, suffix)
        else:
            exact_amount = compute_binance_exact_amount(
                int(total_price),
                rate_vnd,
                suffix,
            )
        exact_amount_text = format_binance_amount(exact_amount)
        try:
            if sale_item_id:
                created_order = await create_binance_sale_direct_order(
                    user_id=user_id,
                    sale_item_id=int(sale_item_id),
                    quantity=quantity,
                    code=code,
                    payment_asset=str(runtime["coin"]),
                    payment_network=str(runtime["network"]),
                    payment_amount_asset=exact_amount_text,
                    payment_rate_vnd=format(rate_vnd, "f"),
                    payment_address=str(runtime["address"]),
                    payment_address_tag=str(runtime.get("address_tag") or ""),
                )
            else:
                created_order = await create_binance_direct_order(
                    user_id=user_id,
                    product_id=product_id,
                    quantity=quantity,
                    unit_price=unit_price_vnd_for_order,
                    amount=amount_vnd_for_order,
                    code=code,
                    bonus_quantity=bonus_quantity,
                    payment_asset=str(runtime["coin"]),
                    payment_network=str(runtime["network"]),
                    payment_amount_asset=exact_amount_text,
                    payment_rate_vnd=format(rate_vnd, "f"),
                    payment_address=str(runtime["address"]),
                    payment_address_tag=str(runtime.get("address_tag") or ""),
                )
            break
        except BinanceDirectOrderError as exc:
            if exc.code == "duplicate_binance_amount":
                continue
            raise

    if not created_order:
        raise BinanceDirectOrderError("duplicate_binance_amount")

    delivered_quantity = quantity + max(0, int(bonus_quantity or 0))
    runtime_address = str(runtime.get("address") or "").strip()
    payment_address = str(created_order.get("payment_address") or runtime_address).strip()
    if payment_address and runtime_address and payment_address != runtime_address:
        logger.warning(
            "Binance direct address mismatch for code=%s: stored order address differs from runtime address "
            "(order_len=%s runtime_len=%s source=%s)",
            str(created_order.get("code") or code),
            len(payment_address),
            len(runtime_address),
            runtime.get("address_source"),
        )
    payment_asset = str(created_order.get("payment_asset") or runtime.get("coin") or "").strip()
    tag_value = str(created_order.get("payment_address_tag") or runtime.get("address_tag") or "").strip()
    product_name_html = html.escape(str(product_name), quote=False)
    payment_asset_html = html.escape(payment_asset, quote=False)
    network_label = str(runtime.get("network_label") or runtime["network"])
    network_label_html = html.escape(network_label, quote=False)
    pay_id = str(runtime.get("pay_id") or "").strip()
    pay_id_block = _html_pre_block(pay_id) if pay_id else ""
    payment_address_block = _html_pre_block(payment_address)
    exact_amount_block = _html_pre_block(exact_amount_text)
    support_code_block = _html_pre_block(str(created_order["code"]))
    tag_label = "Memo/Tag" if tag_value else ""
    tag_block_vi = f"🏷 {tag_label}:\n{_html_pre_block(tag_value)}\n" if tag_value else ""
    tag_block_en = f"🏷 {tag_label}:\n{_html_pre_block(tag_value)}\n" if tag_value else ""
    bonus_line = f"\n🎁 Tặng thêm: <code>{bonus_quantity}</code>" if bonus_quantity else ""
    wallet_step = 2 if pay_id else 1
    pay_section_vi = ""
    pay_section_en = ""
    if pay_id:
        pay_section_vi = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🔶 CÁCH 1: BINANCE PAY (Miễn phí network)\n\n"
            "🆔 Binance ID:\n"
            f"{pay_id_block}\n"
            f"💵 Số tiền ({payment_asset_html}):\n"
            f"{exact_amount_block}\n"
            "📝 Ghi chú:\n"
            f"{support_code_block}\n"
            "⚠️ Ghi chú PHẢI đúng.\n\n"
        )
        pay_section_en = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🔶 OPTION 1: BINANCE PAY (No network fee)\n\n"
            "🆔 Binance ID:\n"
            f"{pay_id_block}\n"
            f"💵 Amount ({payment_asset_html}):\n"
            f"{exact_amount_block}\n"
            "📝 Note:\n"
            f"{support_code_block}\n"
            "⚠️ The note must be exact.\n\n"
        )
    wallet_section_vi = (
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"💳 CÁCH {wallet_step}: CHUYỂN VÍ ({network_label_html})\n\n"
        "📍 Địa chỉ:\n"
        f"{payment_address_block}\n"
        f"{tag_block_vi}"
        f"🌐 Network: <b>{network_label_html}</b>\n"
        f"💵 Số tiền ({payment_asset_html}):\n"
        f"{exact_amount_block}\n"
        "📝 Mã hỗ trợ:\n"
        f"{support_code_block}\n\n"
    )
    wallet_section_en = (
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"💳 OPTION {wallet_step}: WALLET TRANSFER ({network_label_html})\n\n"
        "📍 Address:\n"
        f"{payment_address_block}\n"
        f"{tag_block_en}"
        f"🌐 Network: <b>{network_label_html}</b>\n"
        f"💵 Amount ({payment_asset_html}):\n"
        f"{exact_amount_block}\n"
        "📝 Support code:\n"
        f"{support_code_block}\n\n"
    )

    text = (
        f"{'🟡 THÔNG TIN THANH TOÁN BINANCE SALE' if sale_item_id else '🟡 THÔNG TIN THANH TOÁN BINANCE'}\n\n"
        f"📦 Sản phẩm: <code>{product_name_html}</code>\n"
        f"🔢 Số lượng mua: <code>{quantity}</code>\n"
        f"📥 Số lượng nhận: <code>{delivered_quantity}</code>"
        f"{bonus_line}\n\n"
        f"{pay_section_vi}"
        f"{wallet_section_vi}"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ Chuyển đúng network và đúng số tiền.\n"
        "⚠️ Không cần gửi ảnh chụp màn hình.\n"
        "🤖 BEP20 sẽ được hệ thống tự kiểm tra trong 1-2 phút.\n"
        "ℹ️ Binance Pay dùng cùng số tiền và ghi chú ở trên để đối soát; ghi chú phải đúng."
    )
    if lang == "en":
        bonus_line = f"\n🎁 Bonus: <code>{bonus_quantity}</code>" if bonus_quantity else ""
        text = (
            f"{'🟡 BINANCE SALE PAYMENT INFO' if sale_item_id else '🟡 BINANCE PAYMENT INFO'}\n\n"
            f"📦 Product: <code>{product_name_html}</code>\n"
            f"🔢 Paid quantity: <code>{quantity}</code>\n"
            f"📥 Delivered quantity: <code>{delivered_quantity}</code>"
            f"{bonus_line}\n\n"
            f"{pay_section_en}"
            f"{wallet_section_en}"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "⚠️ Send with the correct network and exact amount.\n"
            "⚠️ No screenshot is required.\n"
            "🤖 BEP20 transfers are checked automatically every 1-2 minutes.\n"
            "ℹ️ Binance Pay uses the same amount and exact note above for reconciliation; the note must be exact."
        )

    msg = await telegram_api_call(
        lambda: context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            reply_markup=build_direct_order_actions_keyboard(str(created_order["code"]), lang),
        ),
        action="send_binance_direct_payment.send_message",
    )
    mark_bot_message(chat_id, msg.message_id)
    return {
        "code": str(created_order["code"]),
        "amount_text": f"{exact_amount_text} {runtime['coin']}",
    }


async def send_external_checkout_payment(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    lang: str,
    user_id: int,
    product: dict,
    product_id: int,
    quantity: int,
    pricing: dict,
    bonus_quantity: int,
    route: str,
    sale_item_id: int | None = None,
):
    route = normalize_checkout_route(route)
    if route == CHECKOUT_ROUTE_BINANCE:
        quoted_total_asset = None
        if float(product.get("price_usdt") or 0) > 0:
            usdt_pricing = get_pricing_snapshot(product, quantity, "usdt")
            quoted_total_asset = float(usdt_pricing["total_price"])
        return await send_binance_direct_payment(
            context=context,
            chat_id=chat_id,
            lang=lang,
            user_id=user_id,
            product_id=product_id,
            product_name=product["name"],
            quantity=quantity,
            unit_price=int(pricing["unit_price"]),
            total_price=int(pricing["total_price"]),
            bonus_quantity=bonus_quantity,
            quoted_total_asset=quoted_total_asset,
            sale_item_id=sale_item_id,
        )

    if route == CHECKOUT_ROUTE_VIETQR:
        return await send_direct_payment(
            context=context,
            chat_id=chat_id,
            lang=lang,
            user_id=user_id,
            product_id=product_id,
            product_name=product["name"],
            quantity=quantity,
            unit_price=int(pricing["unit_price"]),
            total_price=int(pricing["total_price"]),
            bonus_quantity=bonus_quantity,
            sale_item_id=sale_item_id,
        )

    raise ValueError(f"unsupported_external_checkout_route:{route}")

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# States
WAITING_DEPOSIT_AMOUNT = 1
WAITING_WITHDRAW_AMOUNT = 2
WAITING_WITHDRAW_BANK = 3
WAITING_WITHDRAW_ACCOUNT = 4
WAITING_USDT_WITHDRAW_AMOUNT = 7
WAITING_USDT_WITHDRAW_WALLET = 8

# Text handlers for reply keyboard
async def handle_shop_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    if not await is_feature_enabled("show_shop"):
        await reply_bot_message(
            update.message,
            "feature_disabled",
            lang,
            "Tính năng này đang tạm tắt.",
            fallback_emoji="⚠️",
            reply_markup=await get_user_keyboard(lang),
        )
        return
    await delete_last_menu_message(context, update.effective_chat.id)
    rendered, markup = await build_shop_top_level_message(lang, page=0)
    menu_msg = await update.message.reply_text(
        **rendered.to_telegram_kwargs(),
        reply_markup=markup,
    )
    set_last_menu_message(context, menu_msg)


async def sale_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    if not await is_feature_enabled("show_shop"):
        await reply_bot_message(
            update.message,
            "feature_disabled",
            lang,
            "Tính năng này đang tạm tắt.",
            fallback_emoji="⚠️",
            reply_markup=await get_user_keyboard(lang),
        )
        return
    await delete_last_menu_message(context, update.effective_chat.id)
    rendered, markup = await build_sale_catalog_message(lang, page=0)
    menu_msg = await update.message.reply_text(**rendered.to_telegram_kwargs(), reply_markup=markup)
    set_last_menu_message(context, menu_msg)


async def refresh_quantity_prompt(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    lang: str,
    product_id: int,
    currency: str,
    manual_entry: bool = False,
    error_text: str | None = None,
    message=None,
    query=None,
):
    product, checkout_context = await sync_purchase_context(
        context=context,
        user_id=user_id,
        product_id=product_id,
        currency=currency,
    )
    if not product or not checkout_context:
        not_found_text = get_text(lang, "product_not_found")
        if query is not None:
            await query.edit_message_text(not_found_text, reply_markup=delete_keyboard())
        elif message is not None:
            await message.reply_text(not_found_text)
        return None, None, None

    max_can_buy = int(checkout_context["max_can_buy"])

    if max_can_buy <= 0:
        clear_buy_state(context)
        no_capacity_markup = delete_keyboard()
        if product.get("stock", 0) <= 0:
            no_capacity_text = get_text(lang, "out_of_stock").format(name=product["name"])
        elif currency == "usdt":
            unit_price_for_one = float(get_pricing_snapshot(product, 1, "usdt")["total_price"])
            no_capacity_text = get_text(lang, "not_enough_balance").format(
                balance=f"{checkout_context['user_balance_usdt']:.2f} USDT",
                need=f"{unit_price_for_one:.2f} USDT",
            )
            no_capacity_markup = delete_keyboard()
        else:
            unit_price_for_one = int(get_pricing_snapshot(product, 1, "vnd")["total_price"])
            no_capacity_text = get_text(lang, "not_enough_balance").format(
                balance=f"{checkout_context['user_balance']:,}đ",
                need=f"{unit_price_for_one:,}đ",
            )
            missing_amount = max(0, unit_price_for_one - int(checkout_context["user_balance"]))
            no_capacity_markup = build_missing_balance_keyboard(missing_amount, lang, product_id)

        if query is not None:
            await query.edit_message_text(no_capacity_text, reply_markup=no_capacity_markup)
        elif message is not None:
            await message.reply_text(no_capacity_text, reply_markup=no_capacity_markup)
        return None, product, checkout_context

    prompt_msg = await send_quantity_prompt(
        context=context,
        product=product,
        product_id=product_id,
        currency=currency,
        lang=lang,
        max_can_buy=max_can_buy,
        balance_text=checkout_context["balance_text_en"] if lang == "en" else checkout_context["balance_text_vi"],
        payment_label=checkout_context["payment_label_en"] if lang == "en" else checkout_context["payment_label_vi"],
        manual_entry=manual_entry,
        error_text=error_text,
        message=message,
        query=query,
    )
    return prompt_msg, product, checkout_context


async def sync_purchase_context(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    product_id: int,
    currency: str,
):
    product = await get_product(product_id)
    if not product:
        clear_buy_state(context)
        return None, None

    checkout_context = await build_checkout_purchase_context(
        product,
        user_id,
        currency,
        route=context.user_data.get("buying_payment_route"),
    )
    context.user_data["buying_product_id"] = product_id
    context.user_data["buying_max"] = int(checkout_context["max_can_buy"])
    context.user_data["buying_currency"] = currency
    return product, checkout_context


async def process_buy_quantity_selection(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    lang: str,
    product_id: int,
    currency: str,
    quantity: int,
    message=None,
    query=None,
):
    product, checkout_context = await sync_purchase_context(
        context=context,
        user_id=user_id,
        product_id=product_id,
        currency=currency,
    )
    if not product or not checkout_context:
        error_text = get_text(lang, "product_not_found")
        if query is not None:
            await query.edit_message_text(error_text, reply_markup=delete_keyboard())
        elif message is not None:
            await message.reply_text(error_text)
        return None

    max_can_buy = int(checkout_context["max_can_buy"])
    if max_can_buy <= 0:
        return await refresh_quantity_prompt(
            context=context,
            user_id=user_id,
            lang=lang,
            product_id=product_id,
            currency=currency,
            manual_entry=message is not None,
            message=message,
            query=query,
        )

    if quantity < 1:
        return await refresh_quantity_prompt(
            context=context,
            user_id=user_id,
            lang=lang,
            product_id=product_id,
            currency=currency,
            manual_entry=True,
            error_text=get_text(lang, "invalid_quantity").format(max=max_can_buy),
            message=message,
            query=query,
        )

    if quantity > max_can_buy:
        return await refresh_quantity_prompt(
            context=context,
            user_id=user_id,
            lang=lang,
            product_id=product_id,
            currency=currency,
            manual_entry=message is not None,
            error_text=get_text(lang, "max_quantity").format(max=max_can_buy),
            message=message,
            query=query,
        )

    pricing = get_pricing_snapshot(product, quantity, currency)
    required_stock = int(pricing["delivered_quantity"])
    bonus_quantity = int(pricing["bonus_quantity"])

    if product["stock"] < required_stock:
        stock_error_text = (
            f"❌ Not enough stock for quantity + bonus. Need {required_stock}, available {product['stock']}."
            if lang == "en"
            else f"❌ Không đủ hàng cho số lượng + khuyến mãi. Cần {required_stock}, hiện còn {product['stock']}."
        )
        return await refresh_quantity_prompt(
            context=context,
            user_id=user_id,
            lang=lang,
            product_id=product_id,
            currency=currency,
            manual_entry=message is not None,
            error_text=stock_error_text,
            message=message,
            query=query,
        )

    if currency == "usdt":
        unit_price = float(pricing["unit_price"])
        total_price = float(pricing["total_price"])
        balance = await get_balance_usdt(user_id)
    else:
        unit_price = int(pricing["unit_price"])
        total_price = int(pricing["total_price"])
        balance = await get_balance(user_id)

    payment_mode = checkout_context["payment_mode"]
    payment_route = normalize_checkout_route(checkout_context.get("payment_route") or context.user_data.get("buying_payment_route"))
    if not payment_route:
        if currency == "usdt":
            payment_route = CHECKOUT_ROUTE_WALLET_USDT
        elif payment_mode == "direct":
            payment_route = checkout_direct_route_for_language(lang)
        else:
            payment_route = CHECKOUT_ROUTE_WALLET_VND

    if currency == "usdt":
        if balance < total_price:
            return await refresh_quantity_prompt(
                context=context,
                user_id=user_id,
                lang=lang,
                product_id=product_id,
                currency=currency,
                manual_entry=True,
                error_text=get_text(lang, "not_enough_balance").format(
                    balance=f"{balance:.2f} USDT",
                    need=f"{total_price:.2f} USDT",
                ),
                message=message,
                query=query,
            )
    else:
        if payment_route == CHECKOUT_ROUTE_WALLET_VND and balance < total_price:
            error_text = get_text(lang, "not_enough_balance").format(
                balance=f"{balance:,}đ",
                need=f"{total_price:,}đ",
            )
            reply_markup = build_missing_balance_keyboard(int(total_price) - int(balance), lang, product_id)
            if query is not None:
                await query.edit_message_text(error_text, reply_markup=reply_markup)
            elif message is not None:
                await message.reply_text(error_text, reply_markup=reply_markup)
            return None

        if payment_route in (CHECKOUT_ROUTE_VIETQR, CHECKOUT_ROUTE_BINANCE):
            try:
                await send_external_checkout_payment(
                    context=context,
                    chat_id=query.message.chat_id if query is not None else message.chat_id,
                    lang=lang,
                    user_id=user_id,
                    product=product,
                    product_id=product_id,
                    quantity=quantity,
                    pricing=pricing,
                    bonus_quantity=bonus_quantity,
                    route=payment_route,
                )
            except BinanceConfigError:
                error_text = (
                    "❌ Binance on-chain is not ready. Please try again later."
                    if lang == "en"
                    else "❌ Binance on-chain chưa sẵn sàng. Vui lòng thử lại sau."
                )
            except BinanceApiError:
                error_text = (
                    "❌ Could not connect to Binance right now. Please try again later."
                    if lang == "en"
                    else "❌ Không thể kết nối Binance lúc này. Vui lòng thử lại sau."
                )
            except (BinanceDirectOrderError, DirectOrderFulfillmentError):
                error_text = (
                    "❌ Could not create this payment order right now. Please try again later."
                    if lang == "en"
                    else "❌ Không thể tạo đơn thanh toán lúc này. Vui lòng thử lại sau."
                )
            else:
                if query is not None:
                    await dismiss_checkout_prompt(query, lang=lang)
                clear_buy_state(context)
                return None

            if query is not None:
                await query.edit_message_text(error_text, reply_markup=delete_keyboard())
            elif message is not None:
                await message.reply_text(error_text, reply_markup=delete_keyboard())
            clear_buy_state(context)
            return None

        if payment_mode == "direct":
            error_text = (
                "❌ Payment channel is unavailable for this language."
                if lang == "en"
                else "❌ Kênh thanh toán chưa sẵn sàng."
            )
            if query is not None:
                await query.edit_message_text(error_text, reply_markup=delete_keyboard())
            elif message is not None:
                await message.reply_text(error_text, reply_markup=delete_keyboard())
            clear_buy_state(context)
            return None

    if currency == "usdt":
        price_for_order = int(float(unit_price) * USDT_RATE)
        total_for_order = int(total_price * USDT_RATE)
    else:
        price_for_order = int(unit_price)
        total_for_order = int(total_price)

    actual_total = total_price
    try:
        purchase = await fulfill_bot_balance_purchase(
            user_id=user_id,
            product_id=product_id,
            quantity=quantity,
            bonus_quantity=bonus_quantity,
            order_price_per_item=price_for_order,
            order_total_price=total_for_order,
            charge_balance=0 if currency == "usdt" else int(actual_total),
            charge_balance_usdt=float(actual_total) if currency == "usdt" else 0.0,
        )
    except DirectOrderFulfillmentError as exc:
        clear_buy_state(context)
        if exc.code in ("not_enough_stock", "product_not_found"):
            error_text = get_text(lang, "out_of_stock").format(name=product["name"])
        elif exc.code == "insufficient_usdt_balance":
            error_text = get_text(lang, "not_enough_balance").format(
                balance=f"{balance:.2f} USDT",
                need=f"{total_price:.2f} USDT",
            )
        else:
            error_text = get_text(lang, "not_enough_balance").format(
                balance=f"{balance:,}đ",
                need=f"{int(total_price):,}đ",
            )
        if query is not None:
            await query.edit_message_text(error_text, reply_markup=delete_keyboard())
        elif message is not None:
            await message.reply_text(error_text)
        return None

    purchased_items = purchase["items"]
    if currency == "usdt":
        new_balance = float(purchase.get("new_balance_usdt") or 0.0)
        balance_text = f"{new_balance:.2f} USDT"
        total_text = f"{float(actual_total):.2f} USDT"
    else:
        new_balance = int(purchase.get("new_balance") or 0)
        balance_text = f"{new_balance:,}đ"
        total_text = f"{int(actual_total):,}đ"

    format_data = purchase.get("format_data")
    description = str(purchase.get("description") or "").strip()
    header_lines = [
        f"Product: {purchase['product_name']}",
        f"Qty: {len(purchased_items)}",
        f"Paid Qty: {quantity}",
        f"Total: {total_text}",
    ]
    if bonus_quantity:
        header_lines.append(f"Bonus: {bonus_quantity}")
    if description:
        header_lines.append(f"Description: {description}")
    success_text = build_purchase_summary_text(
        product_name=purchase["product_name"],
        delivered_quantity=len(purchased_items),
        total_text=total_text,
        bonus_quantity=bonus_quantity,
        balance_text=balance_text,
        lang=lang,
    )

    await send_purchase_delivery_result(
        context=context,
        purchased_items=purchased_items,
        format_data=format_data,
        header_lines=header_lines,
        filename_base=purchase["product_name"],
        success_text=success_text,
        description=description,
        lang=lang,
        reply_markup=await get_user_keyboard(lang),
        message=message,
        query=query,
    )

    clear_buy_state(context)
    return None


async def sync_sale_purchase_context(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    sale_item_id: int,
    currency: str,
):
    product = await get_active_sale_product(sale_item_id)
    if not product:
        clear_buy_state(context)
        return None, None

    checkout_context = await build_checkout_purchase_context(
        product,
        user_id,
        currency,
        route=context.user_data.get("buying_payment_route"),
    )
    context.user_data["buying_sale_item_id"] = sale_item_id
    context.user_data["buying_max"] = int(checkout_context["max_can_buy"])
    context.user_data["buying_currency"] = currency
    return product, checkout_context


async def refresh_sale_quantity_prompt(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    lang: str,
    sale_item_id: int,
    currency: str,
    manual_entry: bool = False,
    error_text: str | None = None,
    message=None,
    query=None,
):
    product, checkout_context = await sync_sale_purchase_context(
        context=context,
        user_id=user_id,
        sale_item_id=sale_item_id,
        currency=currency,
    )
    if not product or not checkout_context:
        text = "❌ Sale này đã kết thúc hoặc hết hàng." if lang != "en" else "❌ This Sale item is no longer available."
        if query is not None:
            await query.edit_message_text(text, reply_markup=back_keyboard("sale_0"))
        elif message is not None:
            await message.reply_text(text, reply_markup=back_keyboard("sale_0"))
        return None, None, None

    max_can_buy = int(checkout_context["max_can_buy"])
    if max_can_buy <= 0:
        text = (
            f"❌ Không thể mua Sale này ngay lúc này.\nCòn stock Sale: {int(product.get('stock') or 0)}."
            if lang != "en"
            else f"❌ This Sale item cannot be purchased right now.\nSale stock left: {int(product.get('stock') or 0)}."
        )
        if query is not None:
            await query.edit_message_text(text, reply_markup=back_keyboard(f"salebuy_{sale_item_id}"))
        elif message is not None:
            await message.reply_text(text, reply_markup=back_keyboard(f"salebuy_{sale_item_id}"))
        return None, product, checkout_context

    prompt_msg = await send_quantity_prompt(
        context=context,
        product=product,
        product_id=sale_item_id,
        currency=currency,
        lang=lang,
        max_can_buy=max_can_buy,
        balance_text=checkout_context["balance_text_en"] if lang == "en" else checkout_context["balance_text_vi"],
        payment_label=checkout_context["payment_label_en"] if lang == "en" else checkout_context["payment_label_vi"],
        manual_entry=manual_entry,
        error_text=error_text,
        is_sale=True,
        message=message,
        query=query,
    )
    return prompt_msg, product, checkout_context


async def process_sale_quantity_selection(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    lang: str,
    sale_item_id: int,
    currency: str,
    quantity: int,
    message=None,
    query=None,
):
    product, checkout_context = await sync_sale_purchase_context(
        context=context,
        user_id=user_id,
        sale_item_id=sale_item_id,
        currency=currency,
    )
    if not product or not checkout_context:
        text = "❌ Sale này đã kết thúc hoặc hết hàng." if lang != "en" else "❌ This Sale item is no longer available."
        if query is not None:
            await query.edit_message_text(text, reply_markup=back_keyboard("sale_0"))
        elif message is not None:
            await message.reply_text(text, reply_markup=back_keyboard("sale_0"))
        return None

    max_can_buy = int(checkout_context["max_can_buy"])
    if quantity < 1 or quantity > max_can_buy:
        return await refresh_sale_quantity_prompt(
            context=context,
            user_id=user_id,
            lang=lang,
            sale_item_id=sale_item_id,
            currency=currency,
            manual_entry=message is not None,
            error_text=get_text(lang, "invalid_quantity").format(max=max_can_buy),
            message=message,
            query=query,
        )

    pricing = get_pricing_snapshot(product, quantity, currency)
    required_stock = int(pricing["delivered_quantity"])
    bonus_quantity = int(pricing["bonus_quantity"])
    if int(product.get("stock") or 0) < required_stock:
        stock_error_text = (
            f"❌ Không đủ stock Sale. Cần {required_stock}, hiện còn {product['stock']}."
            if lang != "en"
            else f"❌ Not enough Sale stock. Need {required_stock}, available {product['stock']}."
        )
        return await refresh_sale_quantity_prompt(
            context=context,
            user_id=user_id,
            lang=lang,
            sale_item_id=sale_item_id,
            currency=currency,
            manual_entry=message is not None,
            error_text=stock_error_text,
            message=message,
            query=query,
        )

    if currency == "usdt":
        total_price = float(pricing["total_price"])
        balance = await get_balance_usdt(user_id)
        if balance < total_price:
            return await refresh_sale_quantity_prompt(
                context=context,
                user_id=user_id,
                lang=lang,
                sale_item_id=sale_item_id,
                currency=currency,
                manual_entry=True,
                error_text=get_text(lang, "not_enough_balance").format(
                    balance=f"{balance:.2f} USDT",
                    need=f"{total_price:.2f} USDT",
                ),
                message=message,
                query=query,
            )
    else:
        total_price = int(pricing["total_price"])
        balance = await get_balance(user_id)
        payment_mode = checkout_context["payment_mode"]
        payment_route = normalize_checkout_route(checkout_context.get("payment_route") or context.user_data.get("buying_payment_route"))
        if not payment_route:
            payment_route = checkout_direct_route_for_language(lang) if payment_mode == "direct" else CHECKOUT_ROUTE_WALLET_VND

        if payment_route == CHECKOUT_ROUTE_WALLET_VND and balance < total_price:
            error_text = get_text(lang, "not_enough_balance").format(
                balance=f"{balance:,}đ",
                need=f"{total_price:,}đ",
            )
            reply_markup = build_missing_balance_keyboard(
                int(total_price) - int(balance),
                lang,
                back_callback=f"salebuy_{sale_item_id}",
            )
            if query is not None:
                await query.edit_message_text(error_text, reply_markup=reply_markup)
            elif message is not None:
                await message.reply_text(error_text, reply_markup=reply_markup)
            return None

        if payment_route in (CHECKOUT_ROUTE_VIETQR, CHECKOUT_ROUTE_BINANCE):
            try:
                await send_external_checkout_payment(
                    context=context,
                    chat_id=query.message.chat_id if query is not None else message.chat_id,
                    lang=lang,
                    user_id=user_id,
                    product=product,
                    product_id=int(product.get("product_id") or product.get("id") or 0),
                    quantity=quantity,
                    pricing=pricing,
                    bonus_quantity=bonus_quantity,
                    route=payment_route,
                    sale_item_id=sale_item_id,
                )
            except BinanceConfigError:
                error_text = (
                    "❌ Binance on-chain is not ready. Please try again later."
                    if lang == "en"
                    else "❌ Binance on-chain chưa sẵn sàng. Vui lòng thử lại sau."
                )
            except BinanceApiError:
                error_text = (
                    "❌ Could not connect to Binance right now. Please try again later."
                    if lang == "en"
                    else "❌ Không thể kết nối Binance lúc này. Vui lòng thử lại sau."
                )
            except (BinanceDirectOrderError, DirectOrderFulfillmentError):
                error_text = (
                    "❌ Could not create this Sale payment order right now. Please try again later."
                    if lang == "en"
                    else "❌ Không thể tạo đơn thanh toán Sale lúc này. Vui lòng thử lại sau."
                )
            else:
                if query is not None:
                    await dismiss_checkout_prompt(query, lang=lang)
                clear_buy_state(context)
                return None

            if query is not None:
                await query.edit_message_text(error_text, reply_markup=back_keyboard("sale_0"))
            elif message is not None:
                await message.reply_text(error_text, reply_markup=back_keyboard("sale_0"))
            clear_buy_state(context)
            return None

        if payment_mode == "direct":
            error_text = (
                "❌ Payment channel is unavailable for this language."
                if lang == "en"
                else "❌ Kênh thanh toán chưa sẵn sàng."
            )
            if query is not None:
                await query.edit_message_text(error_text, reply_markup=back_keyboard("sale_0"))
            elif message is not None:
                await message.reply_text(error_text, reply_markup=back_keyboard("sale_0"))
            clear_buy_state(context)
            return None

    try:
        purchase = await fulfill_bot_sale_balance_purchase(
            user_id=user_id,
            sale_item_id=sale_item_id,
            quantity=quantity,
            charge_currency=currency,
        )
    except DirectOrderFulfillmentError as exc:
        clear_buy_state(context)
        if exc.code == "sale_user_limit_exceeded":
            error_text = "❌ Bạn đã đạt giới hạn mua cho Sale này." if lang != "en" else "❌ You reached the purchase limit for this Sale."
        elif exc.code in ("not_enough_stock", "sale_item_not_active"):
            error_text = "❌ Sale này đã hết hàng hoặc kết thúc." if lang != "en" else "❌ This Sale item is sold out or ended."
        else:
            error_text = "❌ Không thể hoàn tất đơn Sale lúc này." if lang != "en" else "❌ Could not complete this Sale order right now."
        if query is not None:
            await query.edit_message_text(error_text, reply_markup=back_keyboard("sale_0"))
        elif message is not None:
            await message.reply_text(error_text, reply_markup=back_keyboard("sale_0"))
        return None

    purchased_items = purchase["items"]
    if currency == "usdt":
        balance_text = f"{float(purchase.get('new_balance_usdt') or 0.0):.2f} USDT"
        total_text = f"{float(pricing['total_price']):.2f} USDT"
    else:
        balance_text = f"{int(purchase.get('new_balance') or 0):,}đ"
        total_text = f"{int(pricing['total_price']):,}đ"

    description = str(purchase.get("description") or "").strip()
    header_lines = [
        f"SALE: {purchase['product_name']}",
        f"Qty: {len(purchased_items)}",
        f"Paid Qty: {quantity}",
        f"Total: {total_text}",
    ]
    if bonus_quantity:
        header_lines.append(f"Bonus: {bonus_quantity}")
    if description:
        header_lines.append(f"Description: {description}")

    success_text = build_purchase_summary_text(
        product_name=f"SALE - {purchase['product_name']}",
        delivered_quantity=len(purchased_items),
        total_text=total_text,
        bonus_quantity=bonus_quantity,
        balance_text=balance_text,
        lang=lang,
    )
    await send_purchase_delivery_result(
        context=context,
        purchased_items=purchased_items,
        format_data=purchase.get("format_data"),
        header_lines=header_lines,
        filename_base=purchase["product_name"],
        success_text=success_text,
        description=description,
        lang=lang,
        reply_markup=await get_user_keyboard(lang),
        message=message,
        query=query,
    )
    clear_buy_state(context)
    return None


async def handle_buy_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý khi user nhập số lượng muốn mua"""
    product_id = context.user_data.get('buying_product_id')
    sale_item_id = context.user_data.get('buying_sale_item_id')
    max_can_buy = context.user_data.get('buying_max')
    currency = context.user_data.get('buying_currency')

    if not product_id and not sale_item_id:
        return  # Không trong trạng thái mua hàng

    user_id = update.effective_user.id
    lang = await get_user_language(user_id)

    if sale_item_id:
        if max_can_buy is None or not currency:
            sale_product = await get_active_sale_product(int(sale_item_id))
            if not sale_product:
                await update.message.reply_text(
                    "❌ Sale này đã kết thúc hoặc hết hàng." if lang != "en" else "❌ This Sale item is no longer available.",
                    reply_markup=back_keyboard("sale_0"),
                )
                clear_buy_state(context)
                return
            payment_mode = await get_payment_mode()
            user_balance = await get_balance(user_id)
            user_balance_usdt = await get_balance_usdt(user_id)
            max_by_stock = get_max_quantity_by_stock(sale_product, sale_product["stock"])
            max_vnd = (
                get_max_affordable_quantity(sale_product, user_balance, sale_product["stock"], currency="vnd")
                if payment_mode == "balance" and sale_product["price"] > 0
                else max_by_stock
            )
            max_usdt = (
                get_max_affordable_quantity(sale_product, user_balance_usdt, sale_product["stock"], currency="usdt")
                if sale_product["price_usdt"] > 0
                else 0
            )
            menu_msg = await update.message.reply_text(
                "⚠️ Hãy chọn cách thanh toán Sale trước rồi nhập số lượng."
                if lang != "en"
                else "⚠️ Please choose a Sale payment method first, then enter a quantity.",
                reply_markup=build_payment_method_keyboard(
                    product=sale_product,
                    product_id=int(sale_item_id),
                    lang=lang,
                    payment_mode=payment_mode,
                    max_vnd=max_vnd,
                    max_usdt=max_usdt,
                    is_sale=True,
                ),
            )
            set_last_menu_message(context, menu_msg)
            return

        try:
            quantity = int(update.message.text.strip())
        except ValueError:
            await refresh_sale_quantity_prompt(
                context=context,
                user_id=user_id,
                lang=lang,
                sale_item_id=int(sale_item_id),
                currency=str(currency),
                manual_entry=True,
                error_text=get_text(lang, "invalid_quantity").format(max=int(max_can_buy or 1)),
                message=update.message,
            )
            return

        await process_sale_quantity_selection(
            context=context,
            user_id=user_id,
            lang=lang,
            sale_item_id=int(sale_item_id),
            currency=str(currency),
            quantity=quantity,
            message=update.message,
        )
        return

    if max_can_buy is None or not currency:
        product = await get_product(product_id)
        if not product:
            await update.message.reply_text(get_text(lang, "product_not_found"))
            context.user_data.pop('buying_product_id', None)
            return

        user_balance = await get_balance(user_id)
        user_balance_usdt = await get_balance_usdt(user_id)
        payment_mode = await get_payment_mode()
        max_by_stock = get_max_quantity_by_stock(product, product["stock"])
        if payment_mode == "balance":
            max_vnd = get_max_affordable_quantity(product, user_balance, product["stock"], currency="vnd") if product["price"] > 0 else 0
        else:
            max_vnd = max_by_stock if product['price'] > 0 else 0
        max_usdt = (
            get_max_affordable_quantity(product, user_balance_usdt, product["stock"], currency="usdt")
            if product['price_usdt'] > 0
            else 0
        )

        remind_text = "⚠️ Hãy chọn cách thanh toán trước rồi nhập số lượng."
        if lang == 'en':
            remind_text = "⚠️ Please choose a payment method first, then enter a quantity."
        menu_msg = await update.message.reply_text(
            remind_text,
            reply_markup=build_payment_method_keyboard(
                product=product,
                product_id=product_id,
                lang=lang,
                payment_mode=payment_mode,
                max_vnd=max_vnd,
                max_usdt=max_usdt,
            ),
        )
        set_last_menu_message(context, menu_msg)
        return

    try:
        quantity = int(update.message.text.strip())
    except ValueError:
        if currency:
            await refresh_quantity_prompt(
                context=context,
                user_id=user_id,
                lang=lang,
                product_id=int(product_id),
                currency=str(currency),
                manual_entry=True,
                error_text=get_text(lang, "invalid_quantity").format(max=int(max_can_buy or 1)),
                message=update.message,
            )
        else:
            await update.message.reply_text(get_text(lang, "invalid_quantity").format(max=1))
        return

    await process_buy_quantity_selection(
        context=context,
        user_id=user_id,
        lang=lang,
        product_id=int(product_id),
        currency=str(currency),
        quantity=quantity,
        message=update.message,
    )

async def handle_deposit_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    if not await is_feature_enabled("show_deposit"):
        await update.message.reply_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END
    await delete_last_menu_message(context, update.effective_chat.id)
    context.user_data['waiting_deposit'] = True
    context.user_data['user_lang'] = lang

    text = get_text(lang, "deposit_title")
    cancel_text = get_text(lang, "btn_cancel")
    keyboard = [
        [KeyboardButton("20,000đ"), KeyboardButton("50,000đ")],
        [KeyboardButton(cancel_text)],
    ]
    await update.message.reply_text(
        text,
        reply_markup=persistent_reply_keyboard(keyboard, placeholder="Nhập số tiền hoặc chọn Hủy"),
    )
    return WAITING_DEPOSIT_AMOUNT

async def process_deposit_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý khi user nhập số tiền nạp"""
    text_input = update.message.text.strip()
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    if not await is_feature_enabled("show_deposit"):
        await update.message.reply_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END

    # Xử lý nút Hủy
    if text_input in ["❌ Hủy", "❌ Cancel"]:
        await update.message.reply_text(get_text(lang, "deposit_cancelled"), reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END

    try:
        amount_text = text_input.replace(",", "").replace(".", "").replace(" ", "").replace("đ", "")
        amount = int(amount_text)

        if amount < 5000:
            await update.message.reply_text(get_text(lang, "deposit_min"))
            return WAITING_DEPOSIT_AMOUNT

        # Generate unique code
        code = f"SEVQR NAP{user_id}{random.randint(1000, 9999)}"

        # Save deposit + fetch bank settings in one round-trip (Supabase)
        bank_settings = await create_deposit_with_settings(user_id, amount, code)

        # Lấy settings từ database, fallback về .env nếu chưa có
        bank_name = bank_settings['bank_name'] or SEPAY_BANK_NAME
        account_number = bank_settings['account_number'] or SEPAY_ACCOUNT_NUMBER
        account_name = bank_settings['account_name'] or SEPAY_ACCOUNT_NAME

        if account_number:
            qr_url = generate_vietqr_url(bank_name, account_number, account_name, amount, code)

            text = get_text(lang, "deposit_info").format(
                bank=bank_name, account=account_number, name=account_name,
                amount=f"{amount:,}", code=code
            )
            photo_msg = await update.message.reply_photo(
                photo=qr_url,
                caption=text,
                parse_mode="HTML",
                reply_markup=await get_user_keyboard(lang)
            )
            mark_vietqr_message(update.effective_chat.id, photo_msg.message_id)
        else:
            text = f"📱 MoMo: {MOMO_PHONE}\n👤 {MOMO_NAME}\n💰 {amount:,}đ\n📝 {code}"
            msg = await update.message.reply_text(text, reply_markup=await get_user_keyboard(lang))
            mark_bot_message(update.effective_chat.id, msg.message_id)

        context.user_data['waiting_deposit'] = False
        return ConversationHandler.END

    except ValueError:
        await update.message.reply_text(get_text(lang, "invalid_amount"))
        return WAITING_DEPOSIT_AMOUNT

async def handle_withdraw_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    if not await is_feature_enabled("show_withdraw"):
        await update.message.reply_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END
    await delete_last_menu_message(context, update.effective_chat.id)
    balance = await get_balance(user_id)

    from database import get_user_pending_withdrawal
    pending = await get_user_pending_withdrawal(user_id)

    if pending:
        await update.message.reply_text(get_text(lang, "withdraw_pending").format(amount=f"{pending:,}"))
        return ConversationHandler.END

    if balance < 10000:
        await update.message.reply_text(get_text(lang, "withdraw_low_balance").format(balance=f"{balance:,}"))
        return ConversationHandler.END

    context.user_data['withdraw_balance'] = balance
    text = get_text(lang, "withdraw_title").format(balance=f"{balance:,}")
    cancel_text = get_text(lang, "btn_cancel")
    keyboard = [[KeyboardButton(cancel_text)]]
    await update.message.reply_text(
        text,
        reply_markup=persistent_reply_keyboard(keyboard, placeholder="Nhập số tiền hoặc chọn Hủy"),
    )
    return WAITING_WITHDRAW_AMOUNT

async def process_withdraw_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý khi user nhập số tiền rút"""
    text_input = update.message.text.strip()
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    if not await is_feature_enabled("show_withdraw"):
        await update.message.reply_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END

    if text_input in ["❌ Hủy", "❌ Cancel"]:
        await update.message.reply_text(get_text(lang, "withdraw_cancelled"), reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END

    try:
        amount_text = text_input.replace(",", "").replace(".", "").replace(" ", "").replace("đ", "")
        amount = int(amount_text)

        balance = context.user_data.get('withdraw_balance', 0)

        if amount < 10000:
            await update.message.reply_text(get_text(lang, "withdraw_min"))
            return WAITING_WITHDRAW_AMOUNT

        if amount > balance:
            await update.message.reply_text(get_text(lang, "withdraw_not_enough").format(balance=f"{balance:,}"))
            return WAITING_WITHDRAW_AMOUNT

        context.user_data['withdraw_amount'] = amount

        text = get_text(lang, "withdraw_select_bank").format(amount=f"{amount:,}")
        keyboard = [
            [KeyboardButton("MoMo"), KeyboardButton("MBBank")],
            [KeyboardButton("Vietcombank"), KeyboardButton("VietinBank")],
            [KeyboardButton("BIDV"), KeyboardButton("Techcombank")],
            [KeyboardButton("ACB"), KeyboardButton("TPBank")],
            [KeyboardButton(get_text(lang, "btn_cancel"))],
        ]
        await update.message.reply_text(
            text,
            reply_markup=persistent_reply_keyboard(keyboard, placeholder="Chọn ngân hàng hoặc Hủy"),
        )
        return WAITING_WITHDRAW_BANK

    except ValueError:
        await update.message.reply_text(get_text(lang, "invalid_amount"))
        return WAITING_WITHDRAW_AMOUNT

async def process_withdraw_bank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý khi user chọn ngân hàng"""
    text_input = update.message.text.strip()
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    if not await is_feature_enabled("show_withdraw"):
        await update.message.reply_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END

    if text_input in ["❌ Hủy", "❌ Cancel"]:
        await update.message.reply_text(get_text(lang, "withdraw_cancelled"), reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END

    valid_banks = ["MoMo", "MBBank", "Vietcombank", "VietinBank", "BIDV", "Techcombank", "ACB", "TPBank"]
    if text_input not in valid_banks:
        select_text = "Please select a bank from the list!" if lang == 'en' else "Vui lòng chọn ngân hàng từ danh sách!"
        await update.message.reply_text(select_text)
        return WAITING_WITHDRAW_BANK

    context.user_data['withdraw_bank'] = text_input

    cancel_text = get_text(lang, "btn_cancel")
    keyboard = [[KeyboardButton(cancel_text)]]

    if text_input == "MoMo":
        await update.message.reply_text(
            get_text(lang, "withdraw_enter_momo"),
            reply_markup=persistent_reply_keyboard(keyboard, placeholder="Nhập số điện thoại hoặc chọn Hủy"),
        )
    else:
        await update.message.reply_text(
            get_text(lang, "withdraw_enter_account"),
            reply_markup=persistent_reply_keyboard(keyboard, placeholder="Nhập số tài khoản hoặc chọn Hủy"),
        )
    return WAITING_WITHDRAW_ACCOUNT

async def process_withdraw_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý khi user nhập số tài khoản"""
    text_input = update.message.text.strip()
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    if not await is_feature_enabled("show_withdraw"):
        await update.message.reply_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END

    if text_input in ["❌ Hủy", "❌ Cancel"]:
        await update.message.reply_text(get_text(lang, "withdraw_cancelled"), reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END

    account_number = text_input
    amount = context.user_data.get('withdraw_amount', 0)
    bank_name = context.user_data.get('withdraw_bank', '')

    from database import create_withdrawal
    bank_info = f"{bank_name} - {account_number}"
    await create_withdrawal(user_id, amount, bank_info)

    balance = await get_balance(user_id)

    text = get_text(lang, "withdraw_submitted").format(
        amount=f"{amount:,}", bank=bank_name, account=account_number, balance=f"{balance:,}"
    )
    await update.message.reply_text(text, reply_markup=await get_user_keyboard(lang))
    return ConversationHandler.END

async def show_shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    lang = await get_user_language(query.from_user.id)

    if not await is_feature_enabled("show_shop"):
        await edit_bot_message_text(
            query,
            "feature_disabled",
            lang,
            "Tính năng này đang tạm tắt.",
            fallback_emoji="⚠️",
            reply_markup=delete_keyboard(),
        )
        return

    page = 0
    try:
        parts = (query.data or "").split("_")
        if len(parts) == 2 and parts[0] == "shop":
            page = max(0, int(parts[1]))
    except (TypeError, ValueError):
        page = 0

    rendered, markup = await build_shop_top_level_message(lang, page=page)
    try:
        await query.edit_message_text(
            **rendered.to_telegram_kwargs(),
            reply_markup=markup,
        )
    except BadRequest as exc:
        if "Message is not modified" not in str(exc):
            raise
    set_last_menu_message(context, query.message)


async def show_shop_folder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)

    lang = await get_user_language(query.from_user.id)
    if not await is_feature_enabled("show_shop"):
        await edit_bot_message_text(
            query,
            "feature_disabled",
            lang,
            "Tính năng này đang tạm tắt.",
            fallback_emoji="⚠️",
            reply_markup=delete_keyboard(),
        )
        return

    folder_id = 0
    page = 0
    origin_top_page = 0
    try:
        parts = (query.data or "").split("_")
        if len(parts) == 4 and parts[0] == "shopfolder":
            folder_id = max(0, int(parts[1]))
            page = max(0, int(parts[2]))
            origin_top_page = max(0, int(parts[3]))
    except (TypeError, ValueError):
        folder_id = 0
        page = 0
        origin_top_page = 0

    view = await build_shop_folder_view(folder_id, lang, page=page, origin_top_page=origin_top_page)
    if not view:
        missing_text = (
            "📁 Danh mục này hiện không còn sản phẩm."
            if lang != "en"
            else "📁 This folder has no available products."
        )
        await query.edit_message_text(missing_text, reply_markup=back_keyboard(f"shop_{origin_top_page}"))
        set_last_menu_message(context, query.message)
        return

    text, markup = view
    try:
        await query.edit_message_text(text, reply_markup=markup)
    except BadRequest as exc:
        if "Message is not modified" not in str(exc):
            raise
    set_last_menu_message(context, query.message)


async def show_sale_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    lang = await get_user_language(query.from_user.id)

    if not await is_feature_enabled("show_shop"):
        await edit_bot_message_text(
            query,
            "feature_disabled",
            lang,
            "Tính năng này đang tạm tắt.",
            fallback_emoji="⚠️",
            reply_markup=delete_keyboard(),
        )
        return

    page = 0
    try:
        parts = (query.data or "").split("_")
        if len(parts) == 2 and parts[0] == "sale":
            page = max(0, int(parts[1]))
    except (TypeError, ValueError):
        page = 0

    rendered, markup = await build_sale_catalog_message(lang, page=page)
    try:
        await query.edit_message_text(**rendered.to_telegram_kwargs(), reply_markup=markup)
    except BadRequest as exc:
        if "Message is not modified" not in str(exc):
            raise
    set_last_menu_message(context, query.message)


async def show_sale_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)

    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    sale_item_id = int(query.data.split("_")[1])
    product = await get_active_sale_product(sale_item_id)
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    if not product:
        await query.edit_message_text(
            "❌ Sale này đã kết thúc hoặc hết hàng." if lang != "en" else "❌ This Sale item is no longer available.",
            reply_markup=back_keyboard("sale_0"),
        )
        return

    if int(product.get("stock") or 0) <= 0:
        await query.edit_message_text(
            "❌ Sale này đã hết stock." if lang != "en" else "❌ This Sale item is sold out.",
            reply_markup=back_keyboard("sale_0"),
        )
        return

    user_balance = await get_balance(user_id)
    user_balance_usdt = await get_balance_usdt(user_id)
    payment_mode = await get_payment_mode()
    max_by_stock = get_max_quantity_by_stock(product, product["stock"])
    if payment_mode == "balance":
        max_vnd = get_max_affordable_quantity(product, user_balance, product["stock"], currency="vnd") if product["price"] > 0 else 0
    else:
        max_vnd = max_by_stock if product["price"] > 0 else 0
    max_usdt = (
        get_max_affordable_quantity(product, user_balance_usdt, product["stock"], currency="usdt")
        if product["price_usdt"] > 0
        else 0
    )

    context.user_data["buying_sale_item_id"] = sale_item_id
    context.user_data.pop("buying_product_id", None)
    context.user_data.pop("buying_max", None)
    context.user_data.pop("buying_currency", None)
    context.user_data.pop("buying_payment_route", None)

    if payment_mode == "direct":
        route = checkout_direct_route_for_language(lang)
        if not checkout_route_has_price(product, route):
            await query.edit_message_text(
                "❌ Sale này chưa có giá phù hợp cho kênh thanh toán hiện tại."
                if lang != "en"
                else "❌ This Sale item is not available for the current payment channel.",
                reply_markup=back_keyboard("sale_0"),
            )
            clear_buy_state(context)
            return

        context.user_data["buying_payment_route"] = route
        await refresh_sale_quantity_prompt(
            context=context,
            user_id=user_id,
            lang=lang,
            sale_item_id=sale_item_id,
            currency=checkout_route_currency(route),
            query=query,
        )
        return

    fallback, variables = build_payment_options_template_payload(
        product=product,
        lang=lang,
        payment_mode=payment_mode,
        user_balance=user_balance,
        user_balance_usdt=user_balance_usdt,
        max_vnd=max_vnd,
        max_usdt=max_usdt,
        is_sale=True,
        include_usdt_price=True,
    )
    rendered = await render_bot_message(
        "sale_payment_options",
        lang,
        fallback,
        variables=variables,
    )

    await query.edit_message_text(
        **rendered.to_telegram_kwargs(),
        reply_markup=build_payment_method_keyboard(
            product=product,
            product_id=sale_item_id,
            lang=lang,
            payment_mode=payment_mode,
            max_vnd=max_vnd,
            max_usdt=max_usdt,
            is_sale=True,
        ),
    )
    set_last_menu_message(context, query.message)

async def show_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)

    product_id = int(query.data.split("_")[1])
    product = await get_product(product_id)
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    if not product:
        await query.edit_message_text(get_text(lang, "product_not_found"), reply_markup=delete_keyboard())
        return

    if product['stock'] <= 0:
        await query.edit_message_text(
            get_text(lang, "out_of_stock").format(name=product['name']),
            reply_markup=delete_keyboard()
        )
        return

    user_balance = await get_balance(user_id)
    user_balance_usdt = await get_balance_usdt(user_id)
    payment_mode = await get_payment_mode()

    max_by_stock = get_max_quantity_by_stock(product, product["stock"])

    if payment_mode == "balance":
        max_vnd = get_max_affordable_quantity(product, user_balance, product["stock"], currency="vnd") if product["price"] > 0 else 0
    else:
        max_vnd = max_by_stock if product['price'] > 0 else 0
    max_usdt = (
        get_max_affordable_quantity(product, user_balance_usdt, product["stock"], currency="usdt")
        if product['price_usdt'] > 0
        else 0
    )

    context.user_data['buying_product_id'] = product_id
    context.user_data.pop('buying_max', None)
    context.user_data.pop('buying_currency', None)
    context.user_data.pop("buying_payment_route", None)

    if payment_mode == "direct":
        route = checkout_direct_route_for_language(lang)
        if not checkout_route_has_price(product, route):
            await query.edit_message_text(
                "❌ Sản phẩm này chưa có giá phù hợp cho kênh thanh toán hiện tại."
                if lang != "en"
                else "❌ This product is not available for the current payment channel.",
                reply_markup=delete_keyboard(),
            )
            clear_buy_state(context)
            return

        context.user_data["buying_payment_route"] = route
        await refresh_quantity_prompt(
            context=context,
            user_id=user_id,
            lang=lang,
            product_id=product_id,
            currency=checkout_route_currency(route),
            query=query,
        )
        return

    fallback, variables = build_payment_options_template_payload(
        product=product,
        lang=lang,
        payment_mode=payment_mode,
        user_balance=user_balance,
        user_balance_usdt=user_balance_usdt,
        max_vnd=max_vnd,
        max_usdt=max_usdt,
        include_usdt_price=(lang == "en"),
    )
    rendered = await render_bot_message(
        "product_payment_options",
        lang,
        fallback,
        variables=variables,
    )

    await query.edit_message_text(
        **rendered.to_telegram_kwargs(),
        reply_markup=build_payment_method_keyboard(
            product=product,
            product_id=product_id,
            lang=lang,
            payment_mode=payment_mode,
            max_vnd=max_vnd,
            max_usdt=max_usdt,
        ),
    )
    set_last_menu_message(context, query.message)

async def select_payment_vnd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User chọn thanh toán bằng VNĐ"""
    query = update.callback_query
    await safe_answer_callback_query(query)
    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    product_id = int(query.data.split("_")[2])
    product = await get_product(product_id)
    user_id = query.from_user.id
    lang = await get_user_language(user_id)
    if not product:
        await query.edit_message_text(get_text(lang, "product_not_found"), reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    payment_mode = await get_payment_mode()
    if payment_mode == "direct":
        context.user_data["buying_payment_route"] = checkout_direct_route_for_language(lang)
    else:
        context.user_data["buying_payment_route"] = CHECKOUT_ROUTE_WALLET_VND

    await refresh_quantity_prompt(
        context=context,
        user_id=user_id,
        lang=lang,
        product_id=product_id,
        currency="vnd",
        query=query,
    )

async def select_payment_usdt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User chọn thanh toán bằng USDT"""
    query = update.callback_query
    await safe_answer_callback_query(query)
    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    product_id = int(query.data.split("_")[2])
    product = await get_product(product_id)
    user_id = query.from_user.id
    lang = await get_user_language(user_id)
    if not product:
        await query.edit_message_text(get_text(lang, "product_not_found"), reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    context.user_data["buying_payment_route"] = CHECKOUT_ROUTE_WALLET_USDT

    await refresh_quantity_prompt(
        context=context,
        user_id=user_id,
        lang=lang,
        product_id=product_id,
        currency="usdt",
        query=query,
    )


async def select_checkout_route(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    parts = (query.data or "").split("_")
    if len(parts) < 3:
        await query.edit_message_text("❌ Dữ liệu thanh toán không hợp lệ.", reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    product_id = int(parts[-1])
    route = normalize_checkout_route("_".join(parts[1:-1]))
    user_id = query.from_user.id
    lang = await get_user_language(user_id)
    if not route:
        await query.edit_message_text("❌ Dữ liệu thanh toán không hợp lệ.", reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    product = await get_product(product_id)
    if not product:
        await query.edit_message_text(get_text(lang, "product_not_found"), reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    context.user_data["buying_payment_route"] = route
    await refresh_quantity_prompt(
        context=context,
        user_id=user_id,
        lang=lang,
        product_id=product_id,
        currency=checkout_route_currency(route),
        query=query,
    )


async def select_sale_payment_vnd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    sale_item_id = int(query.data.split("_")[2])
    user_id = query.from_user.id
    lang = await get_user_language(user_id)
    payment_mode = await get_payment_mode()
    if payment_mode == "direct":
        context.user_data["buying_payment_route"] = checkout_direct_route_for_language(lang)
    else:
        context.user_data["buying_payment_route"] = CHECKOUT_ROUTE_WALLET_VND
    await refresh_sale_quantity_prompt(
        context=context,
        user_id=user_id,
        lang=lang,
        sale_item_id=sale_item_id,
        currency="vnd",
        query=query,
    )


async def select_sale_payment_usdt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    sale_item_id = int(query.data.split("_")[2])
    user_id = query.from_user.id
    lang = await get_user_language(user_id)
    context.user_data["buying_payment_route"] = CHECKOUT_ROUTE_WALLET_USDT
    await refresh_sale_quantity_prompt(
        context=context,
        user_id=user_id,
        lang=lang,
        sale_item_id=sale_item_id,
        currency="usdt",
        query=query,
    )


async def select_quick_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    parts = (query.data or "").split("_")
    if len(parts) < 4:
        await query.edit_message_text("❌ Dữ liệu số lượng không hợp lệ.", reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    currency = str(parts[1]).lower()
    product_id = int(parts[2])
    quantity = int(parts[3])
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    await process_buy_quantity_selection(
        context=context,
        user_id=user_id,
        lang=lang,
        product_id=product_id,
        currency=currency,
        quantity=quantity,
        query=query,
    )


async def prompt_manual_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query, "✍️ Hãy nhập số lượng vào chat.", show_alert=False)
    parts = (query.data or "").split("_")
    if len(parts) < 3:
        await query.edit_message_text("❌ Dữ liệu số lượng không hợp lệ.", reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    currency = str(parts[1]).lower()
    product_id = int(parts[2])
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    await refresh_quantity_prompt(
        context=context,
        user_id=user_id,
        lang=lang,
        product_id=product_id,
        currency=currency,
        manual_entry=True,
        query=query,
    )


async def prompt_quick_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    parts = (query.data or "").split("_")
    if len(parts) < 3:
        await query.edit_message_text("❌ Dữ liệu số lượng không hợp lệ.", reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    currency = str(parts[1]).lower()
    product_id = int(parts[2])
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    await refresh_quantity_prompt(
        context=context,
        user_id=user_id,
        lang=lang,
        product_id=product_id,
        currency=currency,
        manual_entry=False,
        query=query,
    )


async def select_sale_checkout_route(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    parts = (query.data or "").split("_")
    if len(parts) < 3:
        await query.edit_message_text("❌ Dữ liệu thanh toán Sale không hợp lệ.", reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    sale_item_id = int(parts[-1])
    route = normalize_checkout_route("_".join(parts[1:-1]))
    user_id = query.from_user.id
    lang = await get_user_language(user_id)
    if not route:
        await query.edit_message_text("❌ Dữ liệu thanh toán Sale không hợp lệ.", reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    product = await get_active_sale_product(sale_item_id)
    if not product:
        await query.edit_message_text(
            "❌ Sale này đã kết thúc hoặc hết hàng." if lang != "en" else "❌ This Sale item is no longer available.",
            reply_markup=back_keyboard("sale_0"),
        )
        clear_buy_state(context)
        return

    context.user_data["buying_payment_route"] = route
    await refresh_sale_quantity_prompt(
        context=context,
        user_id=user_id,
        lang=lang,
        sale_item_id=sale_item_id,
        currency=checkout_route_currency(route),
        query=query,
    )


async def select_sale_quick_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    parts = (query.data or "").split("_")
    if len(parts) < 4:
        await query.edit_message_text("❌ Dữ liệu số lượng Sale không hợp lệ.", reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    currency = str(parts[1]).lower()
    sale_item_id = int(parts[2])
    quantity = int(parts[3])
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    await process_sale_quantity_selection(
        context=context,
        user_id=user_id,
        lang=lang,
        sale_item_id=sale_item_id,
        currency=currency,
        quantity=quantity,
        query=query,
    )


async def prompt_sale_manual_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query, "✍️ Hãy nhập số lượng vào chat.", show_alert=False)
    parts = (query.data or "").split("_")
    if len(parts) < 3:
        await query.edit_message_text("❌ Dữ liệu số lượng Sale không hợp lệ.", reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    currency = str(parts[1]).lower()
    sale_item_id = int(parts[2])
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    await refresh_sale_quantity_prompt(
        context=context,
        user_id=user_id,
        lang=lang,
        sale_item_id=sale_item_id,
        currency=currency,
        manual_entry=True,
        query=query,
    )


async def prompt_sale_quick_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    parts = (query.data or "").split("_")
    if len(parts) < 3:
        await query.edit_message_text("❌ Dữ liệu số lượng Sale không hợp lệ.", reply_markup=delete_keyboard())
        clear_buy_state(context)
        return

    currency = str(parts[1]).lower()
    sale_item_id = int(parts[2])
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    await refresh_sale_quantity_prompt(
        context=context,
        user_id=user_id,
        lang=lang,
        sale_item_id=sale_item_id,
        currency=currency,
        manual_entry=False,
        query=query,
    )


async def select_direct_payment_vietqr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    parts = (query.data or "").split("_")
    if len(parts) < 4:
        await query.edit_message_text("❌ Dữ liệu thanh toán không hợp lệ.", reply_markup=delete_keyboard())
        return

    product_id = int(parts[2])
    quantity = int(parts[3])
    product = await get_product(product_id)
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    if not product:
        await query.edit_message_text(get_text(lang, "product_not_found"), reply_markup=delete_keyboard())
        return

    pricing = get_pricing_snapshot(product, quantity, "vnd")
    required_stock = int(pricing["delivered_quantity"])
    bonus_quantity = int(pricing["bonus_quantity"])
    if product["stock"] < required_stock:
        await query.edit_message_text(
            f"❌ Không đủ hàng cho số lượng + khuyến mãi. Cần {required_stock}, hiện còn {product['stock']}.",
            reply_markup=delete_keyboard(),
        )
        return

    payment_result = await send_direct_payment(
        context=context,
        chat_id=query.message.chat_id,
        lang=lang,
        user_id=user_id,
        product_id=product_id,
        product_name=product["name"],
        quantity=quantity,
        unit_price=int(pricing["unit_price"]),
        total_price=int(pricing["total_price"]),
        bonus_quantity=bonus_quantity,
    )
    await dismiss_checkout_prompt(query, lang=lang)


async def select_direct_payment_binance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    parts = (query.data or "").split("_")
    if len(parts) < 4:
        await query.edit_message_text("❌ Dữ liệu thanh toán không hợp lệ.", reply_markup=delete_keyboard())
        return

    product_id = int(parts[2])
    quantity = int(parts[3])
    product = await get_product(product_id)
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    if not product:
        await query.edit_message_text(get_text(lang, "product_not_found"), reply_markup=delete_keyboard())
        return

    pricing = get_pricing_snapshot(product, quantity, "vnd")
    required_stock = int(pricing["delivered_quantity"])
    bonus_quantity = int(pricing["bonus_quantity"])
    if product["stock"] < required_stock:
        await query.edit_message_text(
            f"❌ Không đủ hàng cho số lượng + khuyến mãi. Cần {required_stock}, hiện còn {product['stock']}.",
            reply_markup=delete_keyboard(),
        )
        return

    quoted_total_asset = None
    if float(product.get("price_usdt") or 0) > 0:
        usdt_pricing = get_pricing_snapshot(product, quantity, "usdt")
        quoted_total_asset = float(usdt_pricing["total_price"])

    try:
        payment_result = await send_binance_direct_payment(
            context=context,
            chat_id=query.message.chat_id,
            lang=lang,
            user_id=user_id,
            product_id=product_id,
            product_name=product["name"],
            quantity=quantity,
            unit_price=int(pricing["unit_price"]),
            total_price=int(pricing["total_price"]),
            bonus_quantity=bonus_quantity,
            quoted_total_asset=quoted_total_asset,
        )
    except BinanceConfigError:
        await query.edit_message_text(
            "❌ Binance on-chain chưa sẵn sàng. Kiểm tra lại Coin / Network / API key / API secret.",
            reply_markup=delete_keyboard(),
        )
        return
    except BinanceApiError:
        await query.edit_message_text(
            "❌ Không thể kết nối Binance lúc này. Kiểm tra API key permission, IP restriction hoặc thử lại sau.",
            reply_markup=delete_keyboard(),
        )
        return
    except BinanceDirectOrderError:
        await query.edit_message_text(
            "❌ Không thể tạo đơn Binance lúc này. Vui lòng thử lại sau.",
            reply_markup=delete_keyboard(),
        )
        return

    await dismiss_checkout_prompt(query, lang=lang)


async def select_sale_direct_payment_vietqr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    parts = (query.data or "").split("_")
    if len(parts) < 4:
        await query.edit_message_text("❌ Dữ liệu thanh toán Sale không hợp lệ.", reply_markup=delete_keyboard())
        return

    sale_item_id = int(parts[2])
    quantity = int(parts[3])
    product = await get_active_sale_product(sale_item_id)
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    if not product:
        await query.edit_message_text(
            "❌ Sale này đã kết thúc hoặc hết hàng." if lang != "en" else "❌ This Sale item is no longer available.",
            reply_markup=back_keyboard("sale_0"),
        )
        return

    pricing = get_pricing_snapshot(product, quantity, "vnd")
    required_stock = int(pricing["delivered_quantity"])
    bonus_quantity = int(pricing["bonus_quantity"])
    if int(product.get("stock") or 0) < required_stock:
        await query.edit_message_text(
            f"❌ Không đủ stock Sale. Cần {required_stock}, hiện còn {product['stock']}.",
            reply_markup=back_keyboard(f"salebuy_{sale_item_id}"),
        )
        return

    try:
        payment_result = await send_direct_payment(
            context=context,
            chat_id=query.message.chat_id,
            lang=lang,
            user_id=user_id,
            product_id=int(product.get("product_id") or product.get("id") or 0),
            product_name=product["name"],
            quantity=quantity,
            unit_price=int(pricing["unit_price"]),
            total_price=int(pricing["total_price"]),
            bonus_quantity=bonus_quantity,
            sale_item_id=sale_item_id,
        )
    except DirectOrderFulfillmentError as exc:
        if exc.code == "sale_user_limit_exceeded":
            error_text = "❌ Bạn đã đạt giới hạn mua cho Sale này."
        else:
            error_text = "❌ Không thể tạo đơn Sale lúc này. Sale có thể đã hết hàng."
        await query.edit_message_text(error_text, reply_markup=back_keyboard("sale_0"))
        return

    await dismiss_checkout_prompt(query, lang=lang)


async def select_sale_direct_payment_binance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    parts = (query.data or "").split("_")
    if len(parts) < 4:
        await query.edit_message_text("❌ Dữ liệu thanh toán Sale không hợp lệ.", reply_markup=delete_keyboard())
        return

    sale_item_id = int(parts[2])
    quantity = int(parts[3])
    product = await get_active_sale_product(sale_item_id)
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    if not product:
        await query.edit_message_text(
            "❌ Sale này đã kết thúc hoặc hết hàng." if lang != "en" else "❌ This Sale item is no longer available.",
            reply_markup=back_keyboard("sale_0"),
        )
        return

    pricing = get_pricing_snapshot(product, quantity, "vnd")
    required_stock = int(pricing["delivered_quantity"])
    bonus_quantity = int(pricing["bonus_quantity"])
    if int(product.get("stock") or 0) < required_stock:
        await query.edit_message_text(
            f"❌ Không đủ stock Sale. Cần {required_stock}, hiện còn {product['stock']}.",
            reply_markup=back_keyboard(f"salebuy_{sale_item_id}"),
        )
        return

    quoted_total_asset = None
    if float(product.get("price_usdt") or 0) > 0:
        usdt_pricing = get_pricing_snapshot(product, quantity, "usdt")
        quoted_total_asset = float(usdt_pricing["total_price"])

    try:
        payment_result = await send_binance_direct_payment(
            context=context,
            chat_id=query.message.chat_id,
            lang=lang,
            user_id=user_id,
            product_id=int(product.get("product_id") or product.get("id") or 0),
            product_name=product["name"],
            quantity=quantity,
            unit_price=int(pricing["unit_price"]),
            total_price=int(pricing["total_price"]),
            bonus_quantity=bonus_quantity,
            quoted_total_asset=quoted_total_asset,
            sale_item_id=sale_item_id,
        )
    except BinanceConfigError:
        await query.edit_message_text(
            "❌ Binance on-chain chưa sẵn sàng. Kiểm tra lại Coin / Network / API key / API secret.",
            reply_markup=delete_keyboard(),
        )
        return
    except BinanceApiError:
        await query.edit_message_text(
            "❌ Không thể kết nối Binance lúc này. Kiểm tra API key permission, IP restriction hoặc thử lại sau.",
            reply_markup=delete_keyboard(),
        )
        return
    except (BinanceDirectOrderError, DirectOrderFulfillmentError):
        await query.edit_message_text(
            "❌ Không thể tạo đơn Binance Sale lúc này. Vui lòng thử lại sau.",
            reply_markup=back_keyboard("sale_0"),
        )
        return

    await dismiss_checkout_prompt(query, lang=lang)


async def show_direct_order_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query, "Đang kiểm tra...")
    data = query.data or ""
    code = data.split(":", 1)[1].strip() if ":" in data else ""
    user_id = query.from_user.id
    lang = await get_user_language(user_id)

    if not code:
        await edit_or_reply_callback_message(
            query,
            text="❌ Không tìm thấy mã đơn." if lang != "en" else "❌ Order code not found.",
            reply_markup=delete_keyboard(),
            action="show_direct_order_status.missing_code",
        )
        return

    order = await get_user_direct_order_by_code(user_id, code)
    if not order:
        await edit_or_reply_callback_message(
            query,
            text=(
                "❌ Không tìm thấy đơn này trong tài khoản của bạn.\n"
                "Nếu bạn vừa tạo đơn, vui lòng bấm lại sau vài giây."
            )
            if lang != "en"
            else (
                "❌ This order was not found in your account.\n"
                "If it was just created, try again in a few seconds."
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💬 Hỗ trợ" if lang != "en" else "💬 Support", callback_data="support")],
                [InlineKeyboardButton("🗑 Xóa" if lang != "en" else "🗑 Delete", callback_data="delete_msg")],
            ]),
            action="show_direct_order_status.not_found",
        )
        return

    product = await get_product(int(order.get("product_id") or 0))
    product_name = str((product or {}).get("name") or f"#{order.get('product_id')}")
    await edit_or_reply_callback_message(
        query,
        text=build_direct_order_status_text(order, product_name, lang),
        parse_mode="HTML",
        reply_markup=build_direct_order_actions_keyboard(str(order.get("code") or code), lang),
        action="show_direct_order_status.status",
    )


async def confirm_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    clear_last_menu_message(context, query.message)
    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    # Parse callback: confirm_buy_{product_id}_{quantity}
    parts = query.data.split("_")
    product_id = int(parts[2])
    quantity = int(parts[3]) if len(parts) > 3 else 1

    product = await get_product(product_id)
    user_id = query.from_user.id

    if not product:
        await query.edit_message_text("❌ Sản phẩm không tồn tại!", reply_markup=delete_keyboard())
        return

    pricing = get_pricing_snapshot(product, quantity, "vnd")
    required_stock = int(pricing["delivered_quantity"])
    bonus_quantity = int(pricing["bonus_quantity"])

    if product['stock'] < required_stock:
        await query.edit_message_text(
            f"❌ Không đủ hàng cho số lượng + khuyến mãi. Cần {required_stock}, hiện còn {product['stock']}.",
            reply_markup=delete_keyboard(),
        )
        return

    total_price = int(pricing["total_price"])
    unit_price = int(pricing["unit_price"])
    balance = await get_balance(user_id)
    payment_mode = await get_payment_mode()
    if payment_mode == "balance" and balance < total_price:
        await query.edit_message_text(
            f"❌ Số dư hiện tại không đủ.\n\n💰 Số dư: {balance:,}đ\n💵 Cần: {total_price:,}đ ({quantity}x {product['price']:,}đ)\n\nVui lòng nạp thêm để tiếp tục.",
            reply_markup=build_missing_balance_keyboard(total_price - balance, await get_user_language(user_id), product_id)
        )
        return

    if payment_mode in ("direct", "hybrid") and balance < total_price:
        await prompt_direct_payment_options(
            product=product,
            quantity=quantity,
            total_price=total_price,
            bonus_quantity=bonus_quantity,
            lang=await get_user_language(user_id),
            product_id=product_id,
            top_up_amount=max(0, total_price - balance) if payment_mode == "hybrid" else 0,
            query=query,
        )
        return

    actual_total = total_price
    try:
        purchase = await fulfill_bot_balance_purchase(
            user_id=user_id,
            product_id=product_id,
            quantity=quantity,
            bonus_quantity=bonus_quantity,
            order_price_per_item=unit_price,
            order_total_price=total_price,
            charge_balance=actual_total,
        )
    except DirectOrderFulfillmentError as exc:
        if exc.code in ("not_enough_stock", "product_not_found"):
            await query.edit_message_text("❌ Sản phẩm đã hết hàng!", reply_markup=delete_keyboard())
        else:
            await query.edit_message_text(
                f"❌ Số dư hiện tại không đủ.\n\n💰 Số dư: {balance:,}đ\n💵 Cần: {total_price:,}đ ({quantity}x {product['price']:,}đ)\n\nVui lòng nạp thêm để tiếp tục.",
                reply_markup=build_missing_balance_keyboard(total_price - balance, await get_user_language(user_id), product_id)
            )
        return

    purchased_items = purchase["items"]
    new_balance = int(purchase.get("new_balance") or 0)
    format_data = purchase.get("format_data")
    description = str(purchase.get("description") or "").strip()
    header_lines = [
        f"Sản phẩm: {purchase['product_name']}",
        f"Số lượng: {len(purchased_items)}",
        f"Số lượng mua: {quantity}",
        f"Tổng tiền: {actual_total:,}đ",
    ]
    if bonus_quantity:
        header_lines.append(f"Tặng thêm: {bonus_quantity}")
    if description:
        header_lines.append(f"Mô tả: {description}")

    success_text = build_purchase_summary_text(
        product_name=purchase["product_name"],
        delivered_quantity=len(purchased_items),
        total_text=f"{actual_total:,}đ",
        bonus_quantity=bonus_quantity,
        balance_text=f"{new_balance:,}đ",
        lang="vi",
    )
    await send_purchase_delivery_result(
        context=context,
        purchased_items=purchased_items,
        format_data=format_data,
        header_lines=header_lines,
        filename_base=purchase["product_name"],
        success_text=success_text,
        description=description,
        lang="vi",
        reply_markup=delete_keyboard(),
        query=query,
    )

async def show_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    if not await is_feature_enabled("show_balance"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    user = await get_or_create_user(
        query.from_user.id,
        query.from_user.username,
        getattr(query.from_user, "first_name", None),
        getattr(query.from_user, "last_name", None)
    )

    text = f"""
👤 Tài khoản của bạn

🆔 ID: {user['user_id']}
👤 Username: @{user['username'] or 'Chưa có'}
💰 Số dư: {user['balance']:,}đ
"""
    await query.edit_message_text(text, reply_markup=delete_keyboard())

async def show_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    if not await is_feature_enabled("show_history"):
        await edit_or_reply_callback_message(
            query,
            text="⚠️ Tính năng này đang tạm tắt.",
            reply_markup=delete_keyboard(),
            action="show_history.feature_disabled",
        )
        return

    orders = await get_user_orders(query.from_user.id)

    if not orders:
        sent_message = await edit_or_reply_callback_message(
            query,
            text="📜 Bạn chưa có đơn hàng nào.",
            reply_markup=delete_keyboard(),
            action="show_history.empty",
        )
        set_last_menu_message(context, sent_message or query.message)
        return

    lang = await get_user_language(query.from_user.id)
    page = 0
    parts = query.data.split("_")
    if len(parts) >= 3 and parts[0] == "history" and parts[1] == "page":
        try:
            page = max(0, int(parts[2]))
        except (TypeError, ValueError):
            page = 0

    text, reply_markup, _, _ = build_history_menu(orders, lang, page=page)
    sent_message = await edit_or_reply_callback_message(
        query,
        text=text,
        reply_markup=reply_markup,
        action="show_history.menu",
    )
    set_last_menu_message(context, sent_message or query.message)

async def show_order_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xem chi tiết đơn hàng - gửi file nếu nhiều items"""
    query = update.callback_query
    clear_last_menu_message(context, query.message)
    if not await is_feature_enabled("show_history"):
        await safe_answer_callback_query(query)
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return
    lang = await get_user_language(query.from_user.id)

    order_id = int(query.data.split("_")[2])

    from database import get_order_detail
    order = await get_order_detail(order_id)

    if not order:
        await safe_answer_callback_query(query, "❌ Không tìm thấy đơn hàng!", show_alert=True)
        return

    # order: (id, user_id, product_id, product_name, content, price, created_at, quantity, description, format_data)
    _, order_user_id, product_id, product_name, content, price, created_at, quantity, description, format_data = order
    if int(order_user_id or 0) != int(query.from_user.id) and int(query.from_user.id) not in ADMIN_IDS:
        await safe_answer_callback_query(query, "❌ Bạn không có quyền xem đơn này.", show_alert=True)
        return
    quantity = quantity or 1
    product_id = int(product_id) if product_id else None
    detail_reply_markup = order_detail_actions_keyboard(product_id, lang)

    # Parse content (có thể là JSON array hoặc string đơn)
    import json
    try:
        items = json.loads(content)
        if not isinstance(items, list):
            items = [content]
    except:
        items = [content]

    # Nếu ít items -> hiển thị text
    created_text = created_at[:19] if created_at else ""
    summary_text = build_purchase_summary_text(
        product_name=product_name,
        delivered_quantity=len(items),
        total_text=f"{price:,}đ",
        lang=lang,
        title=f"📋 Chi tiết đơn hàng #{order_id}",
        extra_lines=[f"📅 Ngày mua: {created_text}"] if created_text else None,
    )

    if len(items) <= 10:
        await safe_answer_callback_query(query)
        text = build_delivery_message(
            summary_text=summary_text,
            purchased_items=items,
            format_data=format_data,
            description=description,
            lang=lang,
            html=True,
        )
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=detail_reply_markup)
    else:
        # Nhiều items -> gửi file ngay
        await safe_answer_callback_query(query)

        header_lines = [
            f"Loại hàng: {product_name}",
            f"Số lượng: {len(items)}",
            f"Tổng: {price:,}đ",
        ]
        if quantity and quantity != len(items):
            header_lines.append(f"Số lượng thanh toán: {quantity}")
        if created_text:
            header_lines.append(f"Ngày mua: {created_text}")
        if description:
            header_lines.append(f"Mô tả: {description}")
        file_buf = make_file(format_stock_items(items, format_data, html=False), "\n".join(header_lines))
        filename = f"Don_{order_id}.txt"

        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=file_buf,
            filename=filename,
            caption=summary_text,
            reply_markup=detail_reply_markup,
        )


# Deposit handlers
async def show_deposit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    if not await is_feature_enabled("show_deposit"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    text = """
💰 Nạp tiền

Chọn số tiền bạn muốn nạp:
"""
    await query.edit_message_text(text, reply_markup=deposit_amounts_keyboard())
    set_last_menu_message(context, query.message)

async def process_deposit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback_query(query)
    if not await is_feature_enabled("show_deposit"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return
    clear_last_menu_message(context, query.message)

    amount = int(query.data.split("_")[1])
    user_id = query.from_user.id

    # Generate unique code - SEVQR prefix required for VietinBank + SePay
    code = f"SEVQR NAP{user_id}{''.join(random.choices(string.digits, k=4))}"

    # Save deposit + fetch bank settings in one round-trip (Supabase)
    bank_settings = await create_deposit_with_settings(user_id, amount, code)

    # Ưu tiên SePay nếu có config (DB hoặc .env), không thì dùng MoMo
    bank_name = bank_settings['bank_name'] or SEPAY_BANK_NAME
    account_number = bank_settings['account_number'] or SEPAY_ACCOUNT_NUMBER
    account_name = bank_settings['account_name'] or SEPAY_ACCOUNT_NAME
    if account_number:
        text = f"""
🏦 Thông tin chuyển khoản

🏦 Ngân hàng: <code>{bank_name}</code>
🔢 Số tài khoản: <code>{account_number}</code>
👤 Chủ tài khoản: <code>{account_name}</code>
💰 Số tiền: <code>{amount:,}đ</code>
📝 Nội dung chuyển khoản: <code>{code}</code>

⚠️ Chuyển đúng số tiền và nội dung để hệ thống cộng tự động.
⏳ Tiền thường vào sau 1-2 phút.
🔎 Mã theo dõi: <code>{code}</code>
"""
    else:
        text = f"""
📱 Thông tin chuyển khoản MoMo

📱 Số điện thoại: <code>{MOMO_PHONE}</code>
👤 Tên tài khoản: <code>{MOMO_NAME}</code>
💰 Số tiền: <code>{amount:,}đ</code>
📝 Nội dung chuyển khoản: <code>{code}</code>

⚠️ Chuyển đúng số tiền và nội dung để hệ thống cộng tự động.
⏳ Tiền thường vào sau 1-2 phút.
🔎 Mã theo dõi: <code>{code}</code>
"""
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=delete_keyboard())
    mark_vietqr_message(query.message.chat_id, query.message.message_id)


# ============ RÚT USDT ============

async def handle_usdt_withdraw_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler cho nút Rút USDT - hiện thông báo liên hệ admin"""
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)

    balance_usdt = await get_balance_usdt(user_id)

    from database import get_setting
    admin_contact = await get_setting("admin_contact", "")
    admin_text = f"@{admin_contact}" if admin_contact else "admin"

    if lang == 'en':
        text = (f"💸 Withdraw USDT\n\n"
                f"💵 Current balance: {balance_usdt} USDT\n\n"
                f"📩 To withdraw USDT, please contact {admin_text}\n\n"
                f"⚠️ Minimum: 10 USDT\n"
                f"🌐 Supported networks: TRC20 / BEP20")
    else:
        text = (f"💸 Rút USDT\n\n"
                f"💵 Số dư hiện tại: {balance_usdt} USDT\n\n"
                f"📩 Để rút USDT, vui lòng liên hệ {admin_text}\n\n"
                f"⚠️ Tối thiểu: 10 USDT\n"
                f"🌐 Mạng hỗ trợ: TRC20 / BEP20")

    await update.message.reply_text(text, reply_markup=await get_user_keyboard(lang))
