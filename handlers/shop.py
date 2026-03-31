import logging
import random
import string
import io
from decimal import Decimal, ROUND_HALF_UP
from telegram import Update, InputFile, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from database import (
    get_products, get_product, get_balance,
    get_user_orders, create_deposit_with_settings, get_or_create_user,
    create_direct_order_with_settings,
    create_binance_direct_order,
    get_user_language, get_balance_usdt,
    fulfill_bot_balance_purchase, DirectOrderFulfillmentError, BinanceDirectOrderError
)
from keyboards import (
    products_keyboard, confirm_buy_keyboard,
    main_menu_keyboard, delete_keyboard
)
from helpers.ui import get_shop_menu_text, get_shop_page_size, get_user_keyboard, is_feature_enabled
from helpers.menu import delete_last_menu_message, set_last_menu_message, clear_last_menu_message
from helpers.sepay_state import mark_vietqr_message, mark_bot_message
from helpers.formatting import format_stock_items
from helpers.purchase_messages import (
    build_delivery_message,
    build_purchase_summary_text,
)
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

def make_file(items: list, header: str = "") -> io.BytesIO:
    """Tạo file nhanh từ list items"""
    if header:
        content = header + "\n" + "="*40 + "\n\n" + "\n\n".join(items)
    else:
        content = "\n\n".join(items)
    buf = io.BytesIO(content.encode('utf-8'))
    buf.seek(0)
    return buf

def format_pricing_rules(product: dict, lang: str = "vi") -> str:
    lines: list[str] = []
    tiers = normalize_price_tiers(product.get("price_tiers"))
    if tiers:
        lines.append("📉 Bulk pricing:" if lang == "en" else "📉 Giá theo SL:")
        lines.append("")
        if lang == "en":
            lines.extend([f"      - From {tier['min_quantity']}: {tier['unit_price']:,}đ" for tier in tiers])
        else:
            lines.extend([f"      - Từ {tier['min_quantity']}: {tier['unit_price']:,}đ" for tier in tiers])

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
    if lang == "en":
        lines = [
            f"📦 {product['name']}",
            f"💰 Price: {int(product['price']):,}đ",
        ]
    else:
        lines = [
            f"📦 {product['name']}",
            f"💰 Giá: {int(product['price']):,}đ",
        ]
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


def build_payment_method_keyboard(
    *,
    product: dict,
    product_id: int,
    lang: str,
    payment_mode: str,
    max_vnd: int,
    max_usdt: int,
) -> InlineKeyboardMarkup:
    keyboard = []
    preview_vnd_price = int(get_pricing_snapshot(product, 1, "vnd")["unit_price"])

    if product['price'] > 0 and (payment_mode != "balance" or max_vnd > 0):
        if lang == "en":
            if payment_mode == "direct":
                label = "💳 Direct Payment"
            elif payment_mode == "hybrid":
                label = f"💳 VND / Direct ({preview_vnd_price:,}đ)"
            else:
                label = f"💰 VND Balance ({preview_vnd_price:,}đ)"
        else:
            vnd_label = "💰 VNĐ"
            show_price = True
            if payment_mode == "direct":
                vnd_label = "💳 Thanh toán trực tiếp"
                show_price = False
            elif payment_mode == "hybrid":
                vnd_label = "💳 VNĐ/Direct"
            label = f"{vnd_label} (từ {preview_vnd_price:,}đ)" if show_price else vnd_label
        keyboard.append([InlineKeyboardButton(label, callback_data=f"pay_vnd_{product_id}")])

    if product['price_usdt'] > 0 and max_usdt > 0:
        usdt_label = (
            f"💵 USDT Balance ({product['price_usdt']} USDT)"
            if lang == "en"
            else f"💵 USDT ({product['price_usdt']} USDT)"
        )
        keyboard.append([InlineKeyboardButton(usdt_label, callback_data=f"pay_usdt_{product_id}")])

    keyboard.append([InlineKeyboardButton("🗑 Xóa" if lang != "en" else "🗑 Delete", callback_data="delete_msg")])
    return InlineKeyboardMarkup(keyboard)


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


async def direct_checkout_keyboard(product_id: int, quantity: int):
    rows = [[InlineKeyboardButton("💳 VietQR", callback_data=f"directpay_vietqr_{product_id}_{quantity}")]]
    if await get_binance_runtime_safe():
        rows.append([InlineKeyboardButton("🟡 Binance", callback_data=f"directpay_binance_{product_id}_{quantity}")])
    rows.append([InlineKeyboardButton("🗑 Xóa", callback_data="delete_msg")])
    return InlineKeyboardMarkup(rows)


async def prompt_direct_payment_options(
    *,
    product: dict,
    quantity: int,
    total_price: int,
    bonus_quantity: int,
    lang: str,
    product_id: int,
    message=None,
    query=None,
):
    delivered_quantity = quantity + max(0, int(bonus_quantity or 0))
    bonus_line = f"\n🎁 Tặng thêm: {bonus_quantity}" if bonus_quantity else ""
    text = (
        "💳 Chọn phương thức thanh toán trực tiếp\n\n"
        f"📦 Sản phẩm: {product['name']}\n"
        f"🔢 Số lượng mua: {quantity}\n"
        f"📥 Số lượng nhận: {delivered_quantity}"
        f"{bonus_line}\n"
        f"💰 Tổng thanh toán: {int(total_price):,}đ\n\n"
        "Vui lòng chọn một phương thức bên dưới:"
    )
    if lang == "en":
        text = (
            "💳 Choose direct payment method\n\n"
            f"📦 Product: {product['name']}\n"
            f"🔢 Paid quantity: {quantity}\n"
            f"📥 Delivered quantity: {delivered_quantity}"
            f"{bonus_line}\n"
            f"💰 Total: {int(total_price):,}đ\n\n"
            "Please choose a payment rail below:"
        )

    keyboard = await direct_checkout_keyboard(product_id, quantity)
    if message is not None:
        prompt_msg = await message.reply_text(text, reply_markup=keyboard)
        return prompt_msg
    if query is not None:
        await query.edit_message_text(text, reply_markup=keyboard)
        return query.message
    return None


async def send_direct_payment(context: ContextTypes.DEFAULT_TYPE, chat_id: int, lang: str, user_id: int,
                              product_id: int, product_name: str, quantity: int, unit_price: int, total_price: int,
                              bonus_quantity: int = 0):
    pay_code = f"SEBUY {user_id}{random.randint(1000, 9999)}"
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
        qr_url = generate_vietqr_url(bank_name, account_number, account_name, int(total_price), pay_code)
        text = (
            f"💳 THANH TOÁN ĐƠN HÀNG\n\n"
            f"📦 Sản phẩm: <code>{product_name}</code>\n"
            f"\n🔢 Số lượng mua: <code>{quantity}</code>\n"
            f"{bonus_line}"
            f"📥 Số lượng nhận: <code>{delivered_quantity}</code>\n\n"
            f"🏦 Ngân hàng: <code>{bank_name}</code>\n"
            f"🔢 Số TK: <code>{account_number}</code>\n"
            f"👤 Tên: <code>{account_name}</code>\n\n"
            f"💰 Số tiền: <code>{int(total_price):,}đ</code>\n"
            f"📝 Nội dung: <code>{pay_code}</code>\n"
            f"\n"
            f"✅ Sau khi nhận tiền, hệ thống sẽ tự gửi sản phẩm."
        )
        photo_msg = await context.bot.send_photo(
            chat_id=chat_id,
            photo=qr_url,
            caption=text,
            parse_mode="HTML",
            reply_markup=await get_user_keyboard(lang)
        )
        mark_vietqr_message(chat_id, photo_msg.message_id)
    else:
        text = (
            f"📱 MoMo: {MOMO_PHONE}\n"
            f"👤 {MOMO_NAME}\n"
            f"💰 {int(total_price):,}đ\n"
            f"📝 {pay_code}\n\n"
            f"✅ Sau khi nhận tiền, hệ thống sẽ tự gửi sản phẩm."
        )
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=await get_user_keyboard(lang)
        )
        mark_bot_message(chat_id, msg.message_id)


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
):
    runtime = await get_binance_direct_runtime()
    if not runtime.get("available"):
        raise BinanceConfigError("binance_direct_unavailable")

    code = f"BNBUY {user_id}{random.randint(1000, 9999)}"
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
    tag_value = str(runtime.get("address_tag") or "").strip()
    tag_label = "Memo/Tag" if tag_value else ""
    tag_block = f"\n🏷 {tag_label}: <code>{tag_value}</code>" if tag_value else ""
    bonus_line = f"\n🎁 Tặng thêm: <code>{bonus_quantity}</code>" if bonus_quantity else ""
    quoted_asset_block = ""
    network_label = str(runtime.get("network_label") or runtime["network"])
    if quoted_asset_amount > 0:
        quoted_asset_block = f"💵 Giá USDT gốc: <code>{format_binance_amount(quoted_asset_amount)} {runtime['coin']}</code>\n"

    text = (
        "🟡 THANH TOÁN BINANCE ON-CHAIN\n\n"
        f"📦 Sản phẩm: <code>{product_name}</code>\n"
        f"🔢 Số lượng mua: <code>{quantity}</code>\n"
        f"📥 Số lượng nhận: <code>{delivered_quantity}</code>"
        f"{bonus_line}\n\n"
        f"🪙 Coin: <code>{runtime['coin']}</code>\n"
        f"🌐 Network: <code>{network_label}</code>\n"
        f"🏦 Address: <code>{runtime['address']}</code>"
        f"{tag_block}\n"
        f"{quoted_asset_block}"
        f"💰 Số tiền chính xác: <code>{exact_amount_text} {runtime['coin']}</code>\n"
        f"📝 Mã hỗ trợ: <code>{created_order['code']}</code>\n\n"
        "⚠️ Chuyển đúng network và đúng số tiền.\n"
        "⚠️ Không cần gửi screenshot.\n"
        "✅ Sau khi Binance ghi nhận, hệ thống sẽ tự gửi sản phẩm."
    )
    if lang == "en":
        text = (
            "🟡 BINANCE ON-CHAIN PAYMENT\n\n"
            f"📦 Product: <code>{product_name}</code>\n"
            f"🔢 Paid quantity: <code>{quantity}</code>\n"
            f"📥 Delivered quantity: <code>{delivered_quantity}</code>"
            f"{bonus_line}\n\n"
            f"🪙 Coin: <code>{runtime['coin']}</code>\n"
            f"🌐 Network: <code>{network_label}</code>\n"
            f"🏦 Address: <code>{runtime['address']}</code>"
            f"{tag_block}\n"
            f"{quoted_asset_block.replace('Giá USDT gốc', 'Listed USDT price')}"
            f"💰 Exact amount: <code>{exact_amount_text} {runtime['coin']}</code>\n"
            f"📝 Support code: <code>{created_order['code']}</code>\n\n"
            "⚠️ Send with the correct network and exact amount.\n"
            "⚠️ No screenshot is required.\n"
            "✅ The system will auto-deliver after Binance confirms the deposit."
        )

    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=await get_user_keyboard(lang),
    )
    mark_bot_message(chat_id, msg.message_id)

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
        await update.message.reply_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=await get_user_keyboard(lang))
        return
    await delete_last_menu_message(context, update.effective_chat.id)
    products = await get_products()
    page_size = await get_shop_page_size()
    text = await get_shop_menu_text(lang)
    menu_msg = await update.message.reply_text(
        text,
        reply_markup=products_keyboard(products, lang, page=0, page_size=page_size),
    )
    set_last_menu_message(context, menu_msg)

async def handle_buy_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý khi user nhập số lượng muốn mua"""
    product_id = context.user_data.get('buying_product_id')
    max_can_buy = context.user_data.get('buying_max')
    currency = context.user_data.get('buying_currency')
    
    if not product_id:
        return  # Không trong trạng thái mua hàng
    
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)

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

        remind_text = "⚠️ Bạn chưa chọn phương thức thanh toán.\nVui lòng chọn bên dưới trước khi nhập số lượng."
        if lang == 'en':
            remind_text = "⚠️ Please choose a payment method first, then enter quantity."
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
        await update.message.reply_text(get_text(lang, "invalid_quantity"))
        return
    
    if quantity < 1:
        await update.message.reply_text(get_text(lang, "invalid_quantity"))
        return

    max_can_buy = int(max_can_buy)
    
    if quantity > max_can_buy:
        await update.message.reply_text(
            get_text(lang, "max_quantity").format(max=max_can_buy),
            reply_markup=await get_user_keyboard(lang)
        )
        return
    
    # Xử lý mua hàng
    product = await get_product(product_id)
    
    if not product:
        await update.message.reply_text(get_text(lang, "product_not_found"))
        context.user_data.pop('buying_product_id', None)
        return
    
    pricing = get_pricing_snapshot(product, quantity, currency)
    required_stock = int(pricing["delivered_quantity"])
    bonus_quantity = int(pricing["bonus_quantity"])

    if product['stock'] < required_stock:
        await update.message.reply_text(
            f"❌ Không đủ hàng cho số lượng + khuyến mãi. Cần {required_stock}, hiện còn {product['stock']}."
        )
        return

    # Tính giá theo loại tiền
    if currency == 'usdt':
        unit_price = float(pricing["unit_price"])
        total_price = float(pricing["total_price"])
        balance = await get_balance_usdt(user_id)
    else:
        unit_price = int(pricing["unit_price"])
        total_price = int(pricing["total_price"])
        balance = await get_balance(user_id)
    
    # Determine payment mode for VND orders
    payment_mode = PAYMENT_MODE
    if currency != 'usdt':
        try:
            from database import get_setting
            payment_mode = (await get_setting("payment_mode", PAYMENT_MODE)).lower()
        except Exception:
            payment_mode = PAYMENT_MODE

    if currency == 'usdt':
        if balance < total_price:
            await update.message.reply_text(
                get_text(lang, "not_enough_balance").format(balance=f"{balance:.2f} USDT", need=f"{total_price:.2f} USDT")
            )
            return
    else:
        if payment_mode == 'balance' and balance < total_price:
            await update.message.reply_text(
                get_text(lang, "not_enough_balance").format(balance=f"{balance:,}đ", need=f"{total_price:,}đ")
            )
            return

        should_direct = payment_mode == 'direct' or (payment_mode == 'hybrid' and balance < total_price)
        if should_direct:
            prompt_msg = await prompt_direct_payment_options(
                product=product,
                quantity=quantity,
                total_price=int(total_price),
                bonus_quantity=bonus_quantity,
                lang=lang,
                product_id=product_id,
                message=update.message,
            )
            if prompt_msg is not None:
                set_last_menu_message(context, prompt_msg)

            context.user_data.pop('buying_product_id', None)
            context.user_data.pop('buying_max', None)
            context.user_data.pop('buying_currency', None)
            return
    
    # Lưu giá theo VNĐ để thống kê
    if currency == 'usdt':
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
            charge_balance=0 if currency == 'usdt' else int(actual_total),
            charge_balance_usdt=float(actual_total) if currency == 'usdt' else 0.0,
        )
    except DirectOrderFulfillmentError as exc:
        if exc.code in ("not_enough_stock", "product_not_found"):
            await update.message.reply_text(get_text(lang, "out_of_stock").format(name=product['name']))
        elif exc.code == "insufficient_usdt_balance":
            await update.message.reply_text(
                get_text(lang, "not_enough_balance").format(balance=f"{balance:.2f} USDT", need=f"{total_price:.2f} USDT")
            )
        else:
            await update.message.reply_text(
                get_text(lang, "not_enough_balance").format(balance=f"{balance:,}đ", need=f"{int(total_price):,}đ")
            )
        context.user_data.pop('buying_product_id', None)
        context.user_data.pop('buying_max', None)
        context.user_data.pop('buying_currency', None)
        return

    purchased_items = purchase["items"]
    if currency == 'usdt':
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
        product_name=purchase['product_name'],
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
        message=update.message,
    )
    
    # Clear trạng thái mua
    context.user_data.pop('buying_product_id', None)
    context.user_data.pop('buying_max', None)
    context.user_data.pop('buying_currency', None)

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
    await update.message.reply_text(text, reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
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
    await update.message.reply_text(text, reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
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
        await update.message.reply_text(text, reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
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
        await update.message.reply_text(get_text(lang, "withdraw_enter_momo"), reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
    else:
        await update.message.reply_text(get_text(lang, "withdraw_enter_account"), reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
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
    await query.answer()

    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return

    page = 0
    try:
        parts = (query.data or "").split("_")
        if len(parts) == 2 and parts[0] == "shop":
            page = max(0, int(parts[1]))
    except (TypeError, ValueError):
        page = 0

    products = await get_products()
    page_size = await get_shop_page_size()
    lang = await get_user_language(query.from_user.id)
    text = await get_shop_menu_text(lang)
    await query.edit_message_text(
        text,
        reply_markup=products_keyboard(products, lang, page=page, page_size=page_size),
    )
    set_last_menu_message(context, query.message)

async def show_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

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

    if lang == 'en':
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
        text = format_product_overview(product, include_usdt_price=True, lang="en")
        if payment_mode == "balance":
            if product['price'] > 0:
                text += f"\n\n💳 VND Balance: {user_balance:,}đ (max buy {max_vnd})"
            if product['price_usdt'] > 0:
                text += f"\n💵 USDT Balance: {user_balance_usdt:.2f} USDT (max buy {max_usdt})"
            if max_vnd == 0 and max_usdt == 0:
                text += "\n\n❌ Insufficient balance!"
            else:
                text += "\n\nSelect payment method:"
        else:
            if product['price_usdt'] > 0:
                text += f"\n\n💵 USDT Balance: {user_balance_usdt:.2f} USDT (max buy {max_usdt})"
            text += "\n\nSelect payment method:"

        await query.edit_message_text(
            text,
            reply_markup=build_payment_method_keyboard(
                product=product,
                product_id=product_id,
                lang="en",
                payment_mode=payment_mode,
                max_vnd=max_vnd,
                max_usdt=max_usdt,
            ),
        )
        set_last_menu_message(context, query.message)
    else:
        # Vietnamese: VND or USDT choice
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
        text = format_product_overview(product, lang="vi")
        if payment_mode == "balance":
            text += f"\n\n💳 Số dư VNĐ: {user_balance:,}đ (mua tối đa {max_vnd})"
            text += f"\n💵 Số dư USDT: {user_balance_usdt:.2f} (mua tối đa {max_usdt})"

        if payment_mode == "balance" and max_vnd == 0 and max_usdt == 0:
            text += "\n\n❌ Số dư không đủ. Vui lòng nạp thêm."
        else:
            text += "\n\nChọn phương thức thanh toán:"
        
        await query.edit_message_text(
            text,
            reply_markup=build_payment_method_keyboard(
                product=product,
                product_id=product_id,
                lang="vi",
                payment_mode=payment_mode,
                max_vnd=max_vnd,
                max_usdt=max_usdt,
            ),
        )
        set_last_menu_message(context, query.message)

async def select_payment_vnd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User chọn thanh toán bằng VNĐ"""
    query = update.callback_query
    await query.answer()
    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return
    
    product_id = int(query.data.split("_")[2])
    product = await get_product(product_id)
    user_id = query.from_user.id
    lang = await get_user_language(user_id)
    user_balance = await get_balance(user_id)
    payment_mode = await get_payment_mode()
    max_by_stock = get_max_quantity_by_stock(product, product["stock"])

    if payment_mode == "balance":
        max_can_buy = get_max_affordable_quantity(product, user_balance, product["stock"], currency="vnd") if product['price'] > 0 else 0
        if max_can_buy <= 0:
            unit_price_for_one = int(get_pricing_snapshot(product, 1, "vnd")["total_price"])
            await query.edit_message_text(get_text(lang, "not_enough_balance").format(
                balance=f"{user_balance:,}đ", need=f"{unit_price_for_one:,}đ"
            ), reply_markup=delete_keyboard())
            return
    else:
        max_can_buy = max_by_stock if product['price'] > 0 else 0
    
    context.user_data['buying_product_id'] = product_id
    context.user_data['buying_max'] = max_can_buy
    context.user_data['buying_currency'] = 'vnd'
    
    text = format_product_overview(product, lang=lang)
    if lang == "en":
        if payment_mode == "balance":
            text += f"\n\n💳 VND Balance: {user_balance:,}đ"
        elif payment_mode == "hybrid":
            text += f"\n\n💳 VND Balance: {user_balance:,}đ (if insufficient, choose VietQR/Binance)"
        else:
            text += "\n\n💳 Payment: choose VietQR or Binance"
        text += f"\n🛒 Max can buy: {max_can_buy}"
        text += f"\n✍️ Enter quantity (1-{max_can_buy}):"
    else:
        if payment_mode == "balance":
            text += f"\n\n💳 Số dư: {user_balance:,}đ"
        elif payment_mode == "hybrid":
            text += f"\n\n💳 Số dư: {user_balance:,}đ (thiếu sẽ chọn VietQR/Binance)"
        else:
            text += "\n\n💳 Thanh toán: chọn VietQR hoặc Binance"
        text += f"\n🛒 Có thể mua tối đa: {max_can_buy}"
        text += f"\n✍️ Nhập số lượng (1-{max_can_buy}):"
    await query.edit_message_text(text, reply_markup=delete_keyboard())
    set_last_menu_message(context, query.message)

async def select_payment_usdt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User chọn thanh toán bằng USDT"""
    query = update.callback_query
    await query.answer()
    if not await is_feature_enabled("show_shop"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return
    
    product_id = int(query.data.split("_")[2])
    product = await get_product(product_id)
    user_id = query.from_user.id
    lang = await get_user_language(user_id)
    user_balance_usdt = await get_balance_usdt(user_id)
    
    max_can_buy = (
        get_max_affordable_quantity(product, user_balance_usdt, product["stock"], currency="usdt")
        if product['price_usdt'] > 0
        else 0
    )
    
    context.user_data['buying_product_id'] = product_id
    context.user_data['buying_max'] = max_can_buy
    context.user_data['buying_currency'] = 'usdt'
    
    text = format_product_overview(product, include_usdt_price=True, lang=lang)
    if lang == "en":
        text += f"\n\n💳 USDT Balance: {user_balance_usdt:.2f}"
        text += f"\n🛒 Max can buy: {max_can_buy}"
        text += f"\n✍️ Enter quantity (1-{max_can_buy}):"
    else:
        text += f"\n\n💳 Số dư USDT: {user_balance_usdt:.2f}"
        text += f"\n🛒 Có thể mua tối đa: {max_can_buy}"
        text += f"\n✍️ Nhập số lượng (1-{max_can_buy}):"
    await query.edit_message_text(text, reply_markup=delete_keyboard())
    set_last_menu_message(context, query.message)


async def select_direct_payment_vietqr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
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

    await send_direct_payment(
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
    await query.edit_message_text(
        "✅ Đã gửi hướng dẫn VietQR. Sau khi nhận tiền, hệ thống sẽ tự gửi sản phẩm.",
        reply_markup=delete_keyboard(),
    )


async def select_direct_payment_binance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
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
        await send_binance_direct_payment(
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

    await query.edit_message_text(
        "✅ Đã gửi hướng dẫn thanh toán Binance. Hệ thống sẽ tự gửi sản phẩm sau khi Binance ghi nhận.",
        reply_markup=delete_keyboard(),
    )


async def confirm_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
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
            f"❌ Số dư không đủ!\n\n💰 Số dư: {balance:,}đ\n💵 Cần: {total_price:,}đ ({quantity}x {product['price']:,}đ)\n\nVui lòng nạp thêm tiền.",
            reply_markup=delete_keyboard()
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
                f"❌ Số dư không đủ!\n\n💰 Số dư: {balance:,}đ\n💵 Cần: {total_price:,}đ ({quantity}x {product['price']:,}đ)\n\nVui lòng nạp thêm tiền.",
                reply_markup=delete_keyboard()
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
    await query.answer()
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
👤 THÔNG TIN TÀI KHOẢN

🆔 ID: {user['user_id']}
👤 Username: @{user['username'] or 'Chưa có'}
💰 Số dư: {user['balance']:,}đ
"""
    await query.edit_message_text(text, reply_markup=delete_keyboard())

async def show_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_feature_enabled("show_history"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return
    
    orders = await get_user_orders(query.from_user.id)
    
    if not orders:
        await query.edit_message_text("📜 Bạn chưa có đơn hàng nào!", reply_markup=delete_keyboard())
        set_last_menu_message(context, query.message)
        return
    
    text = "📜 LỊCH SỬ MUA HÀNG\n\nChọn đơn để xem chi tiết:"
    keyboard = []
    
    # Giới hạn 5 đơn gần nhất
    for order in orders[:5]:
        order_id, product_name, content, price, created_at, quantity = order
        quantity = quantity or 1
        short_name = product_name[:8] if len(product_name) > 8 else product_name
        
        # Rút gọn giá
        if price >= 1000000:
            price_str = f"{price//1000000}tr"
        elif price >= 1000:
            price_str = f"{price//1000}k"
        else:
            price_str = str(price)
        
        # Button ngắn gọn
        keyboard.append([InlineKeyboardButton(f"#{order_id} {short_name} x{quantity} {price_str}", callback_data=f"order_detail_{order_id}")])
    
    keyboard.append([InlineKeyboardButton("🗑 Xóa", callback_data="delete_msg")])
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    set_last_menu_message(context, query.message)

async def show_order_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xem chi tiết đơn hàng - gửi file nếu nhiều items"""
    query = update.callback_query
    clear_last_menu_message(context, query.message)
    if not await is_feature_enabled("show_history"):
        await query.answer()
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return
    lang = await get_user_language(query.from_user.id)
    
    order_id = int(query.data.split("_")[2])
    
    from database import get_order_detail
    order = await get_order_detail(order_id)
    
    if not order:
        await query.answer("❌ Không tìm thấy đơn hàng!", show_alert=True)
        return
    
    # order: (id, product_name, content, price, created_at, quantity, description, format_data)
    _, product_name, content, price, created_at, quantity, description, format_data = order
    quantity = quantity or 1
    
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
        title=f"📋 CHI TIẾT ĐƠN HÀNG #{order_id}",
        extra_lines=[f"📅 Ngày mua: {created_text}"] if created_text else None,
    )

    if len(items) <= 10:
        await query.answer()
        text = build_delivery_message(
            summary_text=summary_text,
            purchased_items=items,
            format_data=format_data,
            description=description,
            lang=lang,
            html=True,
        )
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=delete_keyboard())
    else:
        # Nhiều items -> gửi file ngay
        await query.answer()

        header_lines = [
            f"Loại hàng: {product_name}",
            f"Số lượng: {len(items)}",
            f"Tổng: {price:,}đ",
        ]
        if quantity and quantity != len(items):
            header_lines.append(f"SL thanh toán: {quantity}")
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
        )


# Deposit handlers
async def show_deposit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_feature_enabled("show_deposit"):
        await query.edit_message_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=delete_keyboard())
        return
    
    text = """
💰 NẠP TIỀN VÀO TÀI KHOẢN

Chọn số tiền muốn nạp:
"""
    await query.edit_message_text(text, reply_markup=deposit_amounts_keyboard())
    set_last_menu_message(context, query.message)

async def process_deposit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
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
💳 THÔNG TIN CHUYỂN KHOẢN

🏦 Ngân hàng: <code>{bank_name}</code>
🔢 Số TK: <code>{account_number}</code>
👤 Tên: <code>{account_name}</code>
💰 Số tiền: <code>{amount:,}đ</code>
📝 Nội dung: <code>{code}</code>

⚠️ LƯU Ý QUAN TRỌNG:
• Chuyển ĐÚNG số tiền và nội dung
• Tiền sẽ được cộng TỰ ĐỘNG sau 1-2 phút
• Sai nội dung = không nhận được tiền!

✅ Mã nạp tiền: {code}
"""
    else:
        text = f"""
💳 THÔNG TIN CHUYỂN KHOẢN MOMO

📱 Số điện thoại: <code>{MOMO_PHONE}</code>
👤 Tên: <code>{MOMO_NAME}</code>
💰 Số tiền: <code>{amount:,}đ</code>
📝 Nội dung: <code>{code}</code>

⚠️ LƯU Ý QUAN TRỌNG:
• Chuyển đúng số tiền và nội dung
• Tiền sẽ được cộng TỰ ĐỘNG sau 1-2 phút

✅ Mã nạp tiền: {code}
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
        text = (f"💸 WITHDRAW USDT\n\n"
                f"💵 Your balance: {balance_usdt} USDT\n\n"
                f"📩 To withdraw USDT, please contact {admin_text}\n\n"
                f"⚠️ Minimum: 10 USDT\n"
                f"🌐 Network: TRC20 / BEP20")
    else:
        text = (f"💸 RÚT USDT\n\n"
                f"💵 Số dư của bạn: {balance_usdt} USDT\n\n"
                f"📩 Để rút USDT, vui lòng liên hệ {admin_text}\n\n"
                f"⚠️ Tối thiểu: 10 USDT\n"
                f"🌐 Network: TRC20 / BEP20")
    
    await update.message.reply_text(text, reply_markup=await get_user_keyboard(lang))
