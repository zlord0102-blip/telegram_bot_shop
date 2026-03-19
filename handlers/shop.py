import random
import string
import io
from telegram import Update, InputFile, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from database import (
    get_products, get_product, get_balance,
    get_user_orders, create_deposit_with_settings, get_or_create_user,
    create_direct_order_with_settings,
    get_user_language, get_balance_usdt,
    fulfill_bot_balance_purchase, DirectOrderFulfillmentError
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
from helpers.pricing import (
    get_max_affordable_quantity,
    get_max_quantity_by_stock,
    get_pricing_snapshot,
    normalize_price_tiers,
)
from config import MOMO_PHONE, MOMO_NAME, ADMIN_IDS, SEPAY_ACCOUNT_NUMBER, SEPAY_BANK_NAME, SEPAY_ACCOUNT_NAME, BINANCE_PAY_ID, USDT_RATE, PAYMENT_MODE
from locales import get_text

def make_file(items: list, header: str = "") -> io.BytesIO:
    """Tạo file nhanh từ list items"""
    if header:
        content = header + "\n" + "="*40 + "\n\n" + "\n\n".join(items)
    else:
        content = "\n\n".join(items)
    buf = io.BytesIO(content.encode('utf-8'))
    buf.seek(0)
    return buf

def format_pricing_rules(product: dict) -> str:
    lines: list[str] = []
    tiers = normalize_price_tiers(product.get("price_tiers"))
    if tiers:
        lines.append("📉 Giá theo SL:")
        lines.append("")
        lines.extend([f"      - Từ {tier['min_quantity']}: {tier['unit_price']:,}đ" for tier in tiers])

    buy_qty = int(product.get("promo_buy_quantity") or 0)
    bonus_qty = int(product.get("promo_bonus_quantity") or 0)
    if buy_qty > 0 and bonus_qty > 0:
        if lines:
            lines.append("")
        lines.append(f"🎁 Khuyến mãi: mua {buy_qty} tặng {bonus_qty}")

    return "\n".join(lines)


def format_product_overview(product: dict, include_usdt_price: bool = False) -> str:
    lines = [
        f"📦 {product['name']}",
        f"💰 Giá: {int(product['price']):,}đ",
    ]
    if include_usdt_price and float(product.get("price_usdt") or 0) > 0:
        lines.append(f"💵 Giá USDT: {product['price_usdt']} USDT")
    lines.append(f"📦 Còn: {int(product['stock'])}")

    pricing_rules = format_pricing_rules(product)
    if pricing_rules:
        lines.append(pricing_rules)
    return "\n".join(lines)


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

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# States
WAITING_DEPOSIT_AMOUNT = 1
WAITING_WITHDRAW_AMOUNT = 2
WAITING_WITHDRAW_BANK = 3
WAITING_WITHDRAW_ACCOUNT = 4
WAITING_BINANCE_AMOUNT = 5
WAITING_BINANCE_SCREENSHOT = 6
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

        keyboard = []
        preview_vnd_price = int(get_pricing_snapshot(product, 1, "vnd")["unit_price"])
        if product['price'] > 0 and (payment_mode != "balance" or max_vnd > 0):
            vnd_label = "💰 VNĐ"
            show_price = True
            if payment_mode == "direct":
                vnd_label = "💳 VietQR"
                show_price = False
            elif payment_mode == "hybrid":
                vnd_label = "💳 VNĐ/VietQR"
            label = f"{vnd_label} (từ {preview_vnd_price:,}đ)" if show_price else vnd_label
            keyboard.append([InlineKeyboardButton(label, callback_data=f"pay_vnd_{product_id}")])
        if product['price_usdt'] > 0 and max_usdt > 0:
            keyboard.append([InlineKeyboardButton(f"💵 USDT ({product['price_usdt']} USDT)", callback_data=f"pay_usdt_{product_id}")])
        keyboard.append([InlineKeyboardButton("🗑 Xóa", callback_data="delete_msg")])

        remind_text = "⚠️ Bạn chưa chọn phương thức thanh toán.\nVui lòng chọn bên dưới trước khi nhập số lượng."
        if lang == 'en':
            remind_text = "⚠️ Please choose a payment method first, then enter quantity."
        menu_msg = await update.message.reply_text(remind_text, reply_markup=InlineKeyboardMarkup(keyboard))
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
            await send_direct_payment(
                context=context,
                chat_id=update.effective_user.id,
                lang=lang,
                user_id=user_id,
                product_id=product_id,
                product_name=product['name'],
                quantity=quantity,
                unit_price=int(unit_price),
                total_price=int(total_price),
                bonus_quantity=bonus_quantity,
            )

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
    
    pricing_rules = format_pricing_rules(product)
    max_by_stock = get_max_quantity_by_stock(product, product["stock"])

    if lang == 'en':
        # English: USDT only
        if product['price_usdt'] <= 0:
            await query.edit_message_text(
                f"❌ {product['name']} is not available for USDT payment.",
                reply_markup=delete_keyboard()
            )
            return
        max_buy = get_max_affordable_quantity(product, user_balance_usdt, product["stock"], currency="usdt")
        context.user_data['buying_product_id'] = product_id
        context.user_data.pop('buying_max', None)
        context.user_data.pop('buying_currency', None)
        context.user_data['buying_max'] = max_buy
        context.user_data['buying_currency'] = 'usdt'
        
        text = (
            f"📦 {product['name']}\n"
            f"💵 Price: {product['price_usdt']} USDT\n"
            f"📦 In stock: {product['stock']}\n\n"
            f"💳 Your balance: {user_balance_usdt:.2f} USDT\n"
            f"🛒 Max can buy: {max_buy}"
        )
        if pricing_rules:
            text += f"\n\n{pricing_rules}"
        if max_buy > 0:
            text += f"\n\n📝 Enter quantity (1-{max_buy}):"
        else:
            text += "\n\n❌ Insufficient balance!"
        await query.edit_message_text(text, reply_markup=delete_keyboard())
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
        text = format_product_overview(product)
        if payment_mode == "balance":
            text += f"\n\n💳 Số dư VNĐ: {user_balance:,}đ (mua tối đa {max_vnd})"
            text += f"\n💵 Số dư USDT: {user_balance_usdt:.2f} (mua tối đa {max_usdt})"

        if payment_mode == "balance" and max_vnd == 0 and max_usdt == 0:
            text += "\n\n❌ Số dư không đủ. Vui lòng nạp thêm."
        else:
            text += "\n\nChọn phương thức thanh toán:"
        
        keyboard = []
        preview_vnd_price = int(get_pricing_snapshot(product, 1, "vnd")["unit_price"])
        if product['price'] > 0 and (payment_mode != "balance" or max_vnd > 0):
            vnd_label = "💰 VNĐ"
            show_price = True
            if payment_mode == "direct":
                vnd_label = "💳 VietQR"
                show_price = False
            elif payment_mode == "hybrid":
                vnd_label = "💳 VNĐ/VietQR"
            label = f"{vnd_label} (từ {preview_vnd_price:,}đ)" if show_price else vnd_label
            keyboard.append([InlineKeyboardButton(label, callback_data=f"pay_vnd_{product_id}")])
        if product['price_usdt'] > 0 and max_usdt > 0:
            keyboard.append([InlineKeyboardButton(f"💵 USDT ({product['price_usdt']} USDT)", callback_data=f"pay_usdt_{product_id}")])
        keyboard.append([InlineKeyboardButton("🗑 Xóa", callback_data="delete_msg")])

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
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
    
    text = format_product_overview(product)
    if payment_mode == "balance":
        text += f"\n\n💳 Số dư: {user_balance:,}đ"
    elif payment_mode == "hybrid":
        text += f"\n\n💳 Số dư: {user_balance:,}đ (thiếu sẽ dùng VietQR)"
    else:
        text += "\n\n💳 Thanh toán: VietQR"
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
    
    text = format_product_overview(product, include_usdt_price=True)
    text += f"\n\n💳 Số dư USDT: {user_balance_usdt:.2f}"
    text += f"\n🛒 Có thể mua tối đa: {max_can_buy}"
    text += f"\n✍️ Nhập số lượng (1-{max_can_buy}):"
    await query.edit_message_text(text, reply_markup=delete_keyboard())
    set_last_menu_message(context, query.message)

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
        await send_direct_payment(
            context=context,
            chat_id=query.message.chat_id,
            lang=await get_user_language(user_id),
            user_id=user_id,
            product_id=product_id,
            product_name=product['name'],
            quantity=quantity,
            unit_price=unit_price,
            total_price=total_price,
            bonus_quantity=bonus_quantity,
        )
        await query.edit_message_text(
            "✅ Đã gửi VietQR thanh toán. Sau khi nhận tiền, hệ thống sẽ tự gửi sản phẩm.",
            reply_markup=delete_keyboard()
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


# ============ BINANCE PAY DEPOSIT ============

async def handle_binance_deposit_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler khi user bấm nút Nạp Binance"""
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    if not await is_feature_enabled("show_usdt"):
        await update.message.reply_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END
    await delete_last_menu_message(context, update.effective_chat.id)
    
    # Lấy Binance ID từ database
    from database import get_setting
    binance_id = await get_setting("binance_pay_id", "")
    
    if not binance_id and not BINANCE_PAY_ID:
        error_text = "❌ Binance not configured!" if lang == 'en' else "❌ Chức năng nạp Binance chưa được cấu hình!"
        await update.message.reply_text(error_text)
        return ConversationHandler.END
    
    # Ưu tiên database, fallback về config
    context.user_data['binance_id'] = binance_id or BINANCE_PAY_ID
    
    text = get_text(lang, "binance_title")
    cancel_text = get_text(lang, "btn_cancel")
    keyboard = [[KeyboardButton(cancel_text)]]
    await update.message.reply_text(text, reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
    return WAITING_BINANCE_AMOUNT

async def process_binance_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý khi user nhập số USDT"""
    text_input = update.message.text.strip()
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    if not await is_feature_enabled("show_usdt"):
        await update.message.reply_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END
    
    if text_input in ["❌ Hủy", "❌ Cancel"]:
        await update.message.reply_text(get_text(lang, "deposit_cancelled"), reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END
    
    try:
        usdt_amount = float(text_input.replace(",", "."))
        
        if usdt_amount < 1:
            await update.message.reply_text(get_text(lang, "binance_min"))
            return WAITING_BINANCE_AMOUNT
        
        if usdt_amount > 10000:
            max_text = "❌ Maximum is 10,000 USDT." if lang == 'en' else "❌ Số tiền tối đa là 10,000 USDT."
            await update.message.reply_text(max_text)
            return WAITING_BINANCE_AMOUNT
        
        vnd_amount = int(usdt_amount * USDT_RATE)
        code = f"BN{user_id}{random.randint(1000, 9999)}"
        
        # Lấy Binance ID từ context
        binance_id = context.user_data.get('binance_id', BINANCE_PAY_ID)
        
        from database import create_binance_deposit
        await create_binance_deposit(user_id, usdt_amount, vnd_amount, code)
        
        context.user_data['binance_deposit_code'] = code
        context.user_data['binance_usdt'] = usdt_amount
        context.user_data['binance_vnd'] = vnd_amount
        
        text = get_text(lang, "binance_info").format(id=binance_id, amount=usdt_amount, code=code)
        cancel_text = get_text(lang, "btn_cancel")
        keyboard = [[KeyboardButton(cancel_text)]]
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
        return WAITING_BINANCE_SCREENSHOT
        
    except ValueError:
        await update.message.reply_text(get_text(lang, "invalid_amount"))
        return WAITING_BINANCE_AMOUNT

async def process_binance_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý khi user gửi screenshot"""
    user_id = update.effective_user.id
    lang = await get_user_language(user_id)
    if not await is_feature_enabled("show_usdt"):
        await update.message.reply_text("⚠️ Tính năng này đang tạm tắt.", reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END
    
    if update.message.text and update.message.text.strip() in ["❌ Hủy", "❌ Cancel"]:
        await update.message.reply_text(get_text(lang, "deposit_cancelled"), reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END
    
    if not update.message.photo:
        await update.message.reply_text(get_text(lang, "binance_send_screenshot"))
        return WAITING_BINANCE_SCREENSHOT
    
    photo = update.message.photo[-1]
    file_id = photo.file_id
    
    code = context.user_data.get('binance_deposit_code')
    usdt_amount = context.user_data.get('binance_usdt')
    vnd_amount = context.user_data.get('binance_vnd')
    
    if not code:
        await update.message.reply_text(get_text(lang, "error"), reply_markup=await get_user_keyboard(lang))
        return ConversationHandler.END
    
    from database import update_binance_deposit_screenshot
    await update_binance_deposit_screenshot(user_id, code, file_id)
    
    # Thông báo cho admin (tiếng Việt) - không gửi cho chính user đang nạp
    for admin_id in ADMIN_IDS:
        if admin_id == user_id:
            continue  # Không gửi thông báo cho chính mình
        try:
            await context.bot.send_photo(
                chat_id=admin_id,
                photo=file_id,
                caption=f"🔔 YÊU CẦU NẠP USDT MỚI!\n\n"
                        f"👤 User: {user_id}\n"
                        f"💵 Số tiền: {usdt_amount} USDT\n"
                        f"📝 Code: {code}\n\n"
                        f"Vào Admin → 🔶 Duyệt Binance để xử lý."
            )
        except:
            pass
    
    await update.message.reply_text(
        get_text(lang, "binance_submitted").format(amount=usdt_amount, code=code),
        reply_markup=await get_user_keyboard(lang)
    )
    
    context.user_data.pop('binance_deposit_code', None)
    context.user_data.pop('binance_usdt', None)
    context.user_data.pop('binance_vnd', None)
    
    return ConversationHandler.END

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
