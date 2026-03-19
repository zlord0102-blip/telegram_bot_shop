"""
Tự động check giao dịch từ SePay API (không cần webhook/domain)
"""
import asyncio
import aiohttp
import os
import io
import logging
from datetime import datetime
from config import SEPAY_API_TOKEN
from helpers.sepay_state import has_latest_vietqr_message, mark_bot_message
from helpers.formatting import format_stock_items
from helpers.purchase_messages import (
    build_delivery_message,
    build_display_name,
    build_purchase_summary_text,
)

USE_SUPABASE = os.getenv("USE_SUPABASE", "true").lower() in ("1", "true", "yes") and os.getenv("SUPABASE_URL")
SEPAY_DEBUG = os.getenv("SEPAY_DEBUG", "").lower() in ("1", "true", "yes")
SEPAY_LIMIT = os.getenv("SEPAY_LIMIT", "").strip()
SEPAY_FROM_DATE = os.getenv("SEPAY_FROM_DATE", "").strip()
SEPAY_TO_DATE = os.getenv("SEPAY_TO_DATE", "").strip()
SEPAY_LAST_SEEN_TX_ID_KEY = "sepay_last_seen_tx_id"
PAYMENT_RELAY_NOTIFY_TOKEN_KEY = "payment_notify_bot_token"
PAYMENT_RELAY_NOTIFY_USER_ID_KEY = "payment_notify_user_id"


def _env_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return max(1, int(str(raw).strip()))
    except (TypeError, ValueError):
        return default


SEPAY_DEFAULT_LIMIT = _env_positive_int("SEPAY_DEFAULT_LIMIT", 200)
DIRECT_ORDER_PENDING_EXPIRE_MINUTES = _env_positive_int("DIRECT_ORDER_PENDING_EXPIRE_MINUTES", 10)
DIRECT_ORDER_PENDING_EXPIRE_SECONDS = DIRECT_ORDER_PENDING_EXPIRE_MINUTES * 60
logger = logging.getLogger(__name__)

if USE_SUPABASE:
    from database import (
        get_setting,
        set_setting,
        get_pending_deposits,
        update_balance,
        get_balance,
        is_processed_transaction,
        mark_processed_transaction,
        set_deposit_status,
        get_pending_direct_orders,
        set_direct_order_status,
        fulfill_bot_direct_order,
        fulfill_website_direct_order,
        DirectOrderFulfillmentError,
        get_pending_website_direct_orders,
        set_website_direct_order_status,
    )
else:
    import aiosqlite

DB_PATH = "data/shop.db"
_SEPAY_TOKEN_WARNED = False
_SEPAY_TOKEN_OK = False


def make_file(items: list, header: str = "") -> io.BytesIO:
    if header:
        content = header + "\n" + "=" * 40 + "\n\n" + "\n\n".join(items)
    else:
        content = "\n\n".join(items)
    buf = io.BytesIO(content.encode("utf-8"))
    buf.seek(0)
    return buf

def _parse_chat_id(raw_value):
    if raw_value is None:
        return None
    text = str(raw_value).strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


async def get_payment_relay_target():
    if USE_SUPABASE:
        token = str(await get_setting(PAYMENT_RELAY_NOTIFY_TOKEN_KEY, "") or "").strip()
        chat_id = _parse_chat_id(await get_setting(PAYMENT_RELAY_NOTIFY_USER_ID_KEY, ""))
        return token, chat_id

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (PAYMENT_RELAY_NOTIFY_TOKEN_KEY,))
        token_row = await cursor.fetchone()
        token = str(token_row[0] if token_row else "").strip()

        cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (PAYMENT_RELAY_NOTIFY_USER_ID_KEY,))
        chat_row = await cursor.fetchone()
        chat_id = _parse_chat_id(chat_row[0] if chat_row else "")
        return token, chat_id


async def send_payment_relay_notification(relay_token: str, relay_chat_id: int | None, text: str):
    if not relay_token or relay_chat_id is None:
        return False

    url = f"https://api.telegram.org/bot{relay_token}/sendMessage"
    payload = {
        "chat_id": relay_chat_id,
        "text": str(text or "").strip(),
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("Relay notify failed (HTTP %s): %s", resp.status, body[:200])
                    return False
                data = await resp.json()
                if not data.get("ok"):
                    logger.warning("Relay notify failed: %s", data)
                    return False
                return True
        except Exception as e:
            logger.warning("Relay notify exception: %s", e)
            return False


async def resolve_user_display_name(user_id: int) -> str:
    try:
        from database import get_or_create_user

        user = await get_or_create_user(user_id, None, None, None)
        if isinstance(user, dict):
            return build_display_name(
                user.get("first_name"),
                user.get("last_name"),
                user.get("username"),
                fallback="-",
            )
    except Exception:
        pass
    return "-"


def build_bot_payment_relay_text(
    *,
    direct_order_id: int,
    user_id: int,
    display_name: str,
    code: str,
    tx_id: int | str,
    amount: int,
    expected_amount: int,
    product_name: str,
    quantity: int,
    delivered_quantity: int,
    bonus_quantity: int,
) -> str:
    return "\n".join(
        [
            "✅ Thanh toán thành công (Bot)",
            f"Mã đơn hệ thống: {direct_order_id}",
            f"Mã người dùng: {user_id}",
            f"Tên người dùng: {display_name}",
            f"Mã thanh toán: {code}",
            f"Mã giao dịch: {tx_id}",
            "",
            f"Số tiền nhận: {amount:,}đ",
            f"Số tiền kỳ vọng: {expected_amount:,}đ",
            "",
            f"Sản phẩm: {product_name}",
            f"SL thanh toán: {quantity}",
            f"SL giao: {delivered_quantity}",
            f"SL khuyến mãi: {bonus_quantity}",
        ]
    )


def _resolve_product_name(product: dict | None, product_id: int) -> str:
    if isinstance(product, dict):
        for key in ("website_name", "name"):
            value = str(product.get(key) or "").strip()
            if value:
                return value
    return f"#{product_id}"


def _content_preview(value: str, max_len: int = 120) -> str:
    compact = " ".join(str(value or "").split())
    return compact[:max_len]


def _log_tx_seen(tx_id: str, amount: int, content: str):
    logger.info("TX id=%s amount=%s content=%s", tx_id, amount, _content_preview(content))


def _tx_id_to_int(tx_id: str | None):
    if tx_id is None:
        return None
    text = str(tx_id).strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _pick_newer_tx_id(current_tx_id: str | None, candidate_tx_id: str | None):
    current = str(current_tx_id or "").strip()
    candidate = str(candidate_tx_id or "").strip()
    if not candidate:
        return current
    if not current:
        return candidate

    current_int = _tx_id_to_int(current)
    candidate_int = _tx_id_to_int(candidate)
    if current_int is not None and candidate_int is not None:
        return candidate if candidate_int > current_int else current
    return current


def _is_tx_newer_than_checkpoint(tx_id: str, checkpoint_tx_id: str) -> bool:
    checkpoint = str(checkpoint_tx_id or "").strip()
    value = str(tx_id or "").strip()
    if not value or not checkpoint:
        return bool(value)

    tx_int = _tx_id_to_int(value)
    checkpoint_int = _tx_id_to_int(checkpoint)
    if tx_int is not None and checkpoint_int is not None:
        return tx_int > checkpoint_int
    return True


async def _load_last_seen_tx_id() -> str:
    if USE_SUPABASE:
        return str(await get_setting(SEPAY_LAST_SEEN_TX_ID_KEY, "") or "").strip()

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (SEPAY_LAST_SEEN_TX_ID_KEY,))
        row = await cursor.fetchone()
        return str(row[0] if row else "").strip()


async def _save_last_seen_tx_id(tx_id: str):
    tx_text = str(tx_id or "").strip()
    if not tx_text:
        return

    if USE_SUPABASE:
        await set_setting(SEPAY_LAST_SEEN_TX_ID_KEY, tx_text)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (SEPAY_LAST_SEEN_TX_ID_KEY, tx_text),
        )
        await db.commit()


async def get_sepay_token():
    """Lấy SePay token từ database"""
    if USE_SUPABASE:
        token = await get_setting("sepay_token", "")
        return token or SEPAY_API_TOKEN
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT value FROM settings WHERE key = 'sepay_token'")
        row = await cursor.fetchone()
        return (row[0] if row else "") or SEPAY_API_TOKEN

async def get_recent_transactions():
    """Lấy giao dịch gần đây từ SePay"""
    SEPAY_API_TOKEN = await get_sepay_token()
    if not SEPAY_API_TOKEN:
        global _SEPAY_TOKEN_WARNED
        if not _SEPAY_TOKEN_WARNED:
            logger.warning("SePay token missing. Set settings.sepay_token or SEPAY_API_TOKEN in .env.")
            _SEPAY_TOKEN_WARNED = True
        return []
    global _SEPAY_TOKEN_OK
    if not _SEPAY_TOKEN_OK:
        logger.info("✅ SePay token loaded.")
        _SEPAY_TOKEN_OK = True
    
    url = "https://my.sepay.vn/userapi/transactions/list"
    headers = {
        "Authorization": f"Bearer {SEPAY_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    params = {}
    resolved_limit = _env_positive_int("SEPAY_LIMIT", SEPAY_DEFAULT_LIMIT)
    params["limit"] = str(resolved_limit)
    if SEPAY_FROM_DATE:
        params["from_date"] = SEPAY_FROM_DATE
    if SEPAY_TO_DATE:
        params["to_date"] = SEPAY_TO_DATE

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, params=params or None) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    transactions = data.get('transactions', []) or data.get('data', []) or []
                    if SEPAY_DEBUG:
                        logger.info("SePay payload keys: %s", list(data.keys()))
                        logger.info("SePay transactions count: %s", len(transactions))
                        logger.info("SePay status: %s | error: %s | messages: %s", data.get("status"), data.get("error"), data.get("messages"))
                    if data.get("status") is False or data.get("error"):
                        logger.warning("SePay API returned error: %s | messages: %s", data.get("error"), data.get("messages"))
                    return transactions
                body = await resp.text()
                logger.warning("SePay API error %s: %s", resp.status, body[:200])
        except Exception as e:
            logger.exception("Error fetching SePay transactions: %s", e)
    return []

def _normalize_content(value: str) -> str:
    return "".join(str(value).upper().split())

def _pick_content(tx: dict) -> str:
    for key in ("transaction_content", "content", "description", "note", "memo"):
        val = tx.get(key)
        if val:
            return str(val)
    return ""

def _pick_amount(tx: dict) -> float:
    for key in ("amount_in", "amount", "amount_in_vnd"):
        if tx.get(key) is not None:
            raw = str(tx.get(key)).replace(",", "").strip()
            try:
                return float(raw)
            except ValueError:
                continue
    return 0.0

def _pick_tx_id(tx: dict) -> str:
    for key in ("id", "transaction_id", "ref_id", "reference", "transaction_ref"):
        val = tx.get(key)
        if val is not None:
            return str(val)
    return ""


def _parse_created_at(value: str | None):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _is_direct_order_expired(created_at: str | None) -> bool:
    parsed = _parse_created_at(created_at)
    if not parsed:
        return False
    now_dt = datetime.now(parsed.tzinfo) if parsed.tzinfo else datetime.now()
    return (now_dt - parsed).total_seconds() >= DIRECT_ORDER_PENDING_EXPIRE_SECONDS


async def _auto_cancel_expired_direct_orders(pending_direct_orders, bot_app=None):
    active_orders = []
    for order in pending_direct_orders:
        order_id, user_id, _product_id, _quantity, _bonus_quantity, _unit_price, _expected_amount, code, created_at = order
        if not _is_direct_order_expired(created_at):
            active_orders.append(order)
            continue

        await set_direct_order_status(order_id, "cancelled")
        website_order = _find_website_direct_order(code, _website_orders_by_code_upper, _website_orders_by_code_norm)
        if website_order:
            try:
                await set_website_direct_order_status(website_order[0], "cancelled")
            except Exception:
                pass
            _remove_website_direct_order_from_maps(website_order)
        logger.info("⏱️ Auto-cancel direct order #%s after %sm pending.", order_id, DIRECT_ORDER_PENDING_EXPIRE_MINUTES)
        if bot_app and not website_order:
            try:
                await bot_app.bot.send_message(
                    user_id,
                    f"⌛ Đơn thanh toán #{order_id} đã hết hạn sau {DIRECT_ORDER_PENDING_EXPIRE_MINUTES} phút và đã tự hủy."
                )
            except Exception:
                pass
    return active_orders


_website_orders_by_code_upper = {}
_website_orders_by_code_norm = {}


def _build_website_direct_order_maps(pending_website_direct_orders):
    _website_orders_by_code_upper.clear()
    _website_orders_by_code_norm.clear()
    for row in pending_website_direct_orders:
        code = str(row[8] or "").strip()
        if not code:
            continue
        _website_orders_by_code_upper[code.upper()] = row
        _website_orders_by_code_norm[_normalize_content(code)] = row


def _find_website_direct_order(code: str, by_code_upper: dict, by_code_norm: dict):
    code_text = str(code or "").strip()
    if not code_text:
        return None
    return by_code_upper.get(code_text.upper()) or by_code_norm.get(_normalize_content(code_text))


def _remove_website_direct_order_from_maps(row):
    code = str(row[8] or "").strip()
    if not code:
        return
    _website_orders_by_code_upper.pop(code.upper(), None)
    _website_orders_by_code_norm.pop(_normalize_content(code), None)

async def process_transactions(bot_app=None):
    """Xử lý giao dịch và cộng tiền tự động"""
    transactions = await get_recent_transactions()
    last_seen_tx_id = await _load_last_seen_tx_id()
    latest_seen_tx_id = str(last_seen_tx_id or "").strip()
    for tx in transactions:
        latest_seen_tx_id = _pick_newer_tx_id(latest_seen_tx_id, _pick_tx_id(tx))

    relay_token, relay_chat_id = await get_payment_relay_target()
    if USE_SUPABASE:
        pending_deposits = await get_pending_deposits()
        pending_direct_orders = await get_pending_direct_orders()
        pending_website_direct_orders = await get_pending_website_direct_orders()
        _build_website_direct_order_maps(pending_website_direct_orders)
        pending_direct_orders = await _auto_cancel_expired_direct_orders(pending_direct_orders, bot_app)
        if SEPAY_DEBUG:
            logger.info(
                "Pending deposits: %s | pending direct orders: %s | pending website direct orders: %s",
                len(pending_deposits),
                len(pending_direct_orders),
                len(pending_website_direct_orders),
            )
        for tx in transactions:
            amount_in = _pick_amount(tx)
            if float(amount_in) <= 0:
                continue

            content = _pick_content(tx)
            content_upper = str(content).upper().strip()
            content_norm = _normalize_content(content)
            amount = int(float(amount_in))
            tx_id = _pick_tx_id(tx)

            if not tx_id:
                continue
            if not _is_tx_newer_than_checkpoint(tx_id, last_seen_tx_id):
                continue
            _log_tx_seen(tx_id, amount, content)
            if await is_processed_transaction(tx_id):
                continue

            matched = False
            for deposit in pending_deposits:
                deposit_id, user_id, _expected_amount, code, _created_at = deposit
                code_upper = code.upper()
                code_norm = _normalize_content(code)
                if code_upper in content_upper or code_norm in content_norm:
                    await set_deposit_status(deposit_id, "confirmed")
                    await update_balance(user_id, amount)
                    await mark_processed_transaction(tx_id)

                    print(f"✅ Confirmed: User {user_id}, Amount {amount:,}đ")

                    if bot_app:
                        try:
                            new_balance = await get_balance(user_id)
                            msg = await bot_app.bot.send_message(
                                user_id,
                                f"✅ NẠP TIỀN THÀNH CÔNG!\n\n"
                                f"💰 Số tiền: {amount:,}đ\n"
                                f"💳 Số dư hiện tại: {new_balance:,}đ"
                            )
                            mark_bot_message(user_id, msg.message_id)
                        except:
                            pass
                    matched = True
                    break

            if matched:
                continue

            for order in pending_direct_orders:
                order_id, user_id, product_id, quantity, bonus_quantity, unit_price, expected_amount, code, _created_at = order
                code_upper = code.upper()
                code_norm = _normalize_content(code)
                website_direct_order = _find_website_direct_order(code, _website_orders_by_code_upper, _website_orders_by_code_norm)
                if (code_upper in content_upper or code_norm in content_norm) and amount >= expected_amount:
                    order_group = f"PAY{user_id}{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    try:
                        if website_direct_order:
                            fulfillment = await fulfill_website_direct_order(
                                website_direct_order[0],
                                order_group=order_group,
                                expire_minutes=DIRECT_ORDER_PENDING_EXPIRE_MINUTES,
                            )
                        else:
                            fulfillment = await fulfill_bot_direct_order(
                                order_id,
                                order_group=order_group,
                                expire_minutes=DIRECT_ORDER_PENDING_EXPIRE_MINUTES,
                            )
                    except DirectOrderFulfillmentError as exc:
                        if website_direct_order and exc.code in (
                            "website_direct_order_not_pending",
                            "website_direct_order_expired",
                            "mirror_direct_order_not_pending",
                            "mirror_direct_order_not_found",
                            "not_enough_stock",
                        ):
                            _remove_website_direct_order_from_maps(website_direct_order)

                        if exc.code == "not_enough_stock":
                            if not website_direct_order and bot_app:
                                await bot_app.bot.send_message(
                                    user_id,
                                    "❌ Thanh toán đã nhận nhưng sản phẩm hiện hết hàng. Vui lòng liên hệ admin."
                                )
                            await mark_processed_transaction(tx_id)
                            matched = True
                            break

                        if exc.code in (
                            "direct_order_not_pending",
                            "direct_order_expired",
                            "website_direct_order_not_pending",
                            "website_direct_order_expired",
                            "mirror_direct_order_not_pending",
                            "mirror_direct_order_not_found",
                        ):
                            logger.info("Skip fulfill for code=%s tx=%s reason=%s", code, tx_id, exc.code)
                            await mark_processed_transaction(tx_id)
                            matched = True
                            break

                        raise

                    await mark_processed_transaction(tx_id)
                    if website_direct_order:
                        _remove_website_direct_order_from_maps(website_direct_order)
                        logger.info(
                            "✅ Website direct order confirmed: code=%s website_direct_order_id=%s website_order_id=%s",
                            fulfillment.get("code"),
                            fulfillment.get("website_direct_order_id"),
                            fulfillment.get("website_order_id"),
                        )
                        await send_payment_relay_notification(
                            relay_token,
                            relay_chat_id,
                            "\n".join([
                                "✅ Thanh toán thành công (Website)",
                                f"Mã đơn hệ thống: {fulfillment.get('direct_order_id')}",
                                f"Mã direct order website: {fulfillment.get('website_direct_order_id')}",
                                f"Mã đơn website: {fulfillment.get('website_order_id')}",
                                f"Mã thanh toán: {fulfillment.get('code')}",
                                f"Mã giao dịch: {tx_id}",
                                f"Số tiền nhận: {amount:,}đ",
                                f"Số tiền kỳ vọng: {expected_amount:,}đ",
                                f"Mã user website: {fulfillment.get('auth_user_id')}",
                                f"Sản phẩm: {fulfillment.get('product_name')}",
                                f"SL thanh toán: {fulfillment.get('quantity')}",
                                f"SL giao: {len(fulfillment.get('items') or [])}",
                                f"SL khuyến mãi: {int(fulfillment.get('bonus_quantity') or 0)}",
                            ]),
                        )
                    else:
                        purchased_items = [str(item or "") for item in (fulfillment.get("items") or [])]
                        product_name = str(fulfillment.get("product_name") or f"#{product_id}")
                        display_name = await resolve_user_display_name(user_id)
                        await send_payment_relay_notification(
                            relay_token,
                            relay_chat_id,
                            build_bot_payment_relay_text(
                                direct_order_id=int(fulfillment.get("direct_order_id") or 0),
                                user_id=int(fulfillment.get("user_id") or user_id),
                                display_name=display_name,
                                code=str(fulfillment.get("code") or ""),
                                tx_id=tx_id,
                                amount=int(amount or 0),
                                expected_amount=int(expected_amount or 0),
                                product_name=product_name,
                                quantity=int(fulfillment.get("quantity") or quantity),
                                delivered_quantity=len(purchased_items),
                                bonus_quantity=int(fulfillment.get("bonus_quantity") or 0),
                            ),
                        )

                        if bot_app:
                            try:
                                description = str(fulfillment.get("description") or "").strip()
                                format_data = fulfillment.get("format_data")
                                total_text = f"{int(fulfillment.get('amount') or expected_amount):,}đ"
                                header_lines = [
                                    f"Loại hàng: {product_name}",
                                    f"Số lượng: {len(purchased_items)}",
                                    f"SL thanh toán: {int(fulfillment.get('quantity') or quantity)}",
                                    f"Tổng: {total_text}",
                                ]
                                if fulfillment.get("bonus_quantity"):
                                    header_lines.append(f"Tặng thêm: {int(fulfillment.get('bonus_quantity') or 0)}")
                                if description:
                                    header_lines.append(f"Mô tả: {description}")
                                header = "\n".join(header_lines)
                                formatted_items_plain = format_stock_items(purchased_items, format_data, html=False)
                                file_buf = make_file(formatted_items_plain, header)
                                filename = f"{product_name}_{len(purchased_items)}.txt"

                                success_text = build_purchase_summary_text(
                                    product_name=product_name,
                                    delivered_quantity=len(purchased_items),
                                    total_text=total_text,
                                    bonus_quantity=int(fulfillment.get("bonus_quantity") or 0),
                                    lang="vi",
                                )
                                if len(purchased_items) > 5:
                                    msg = await bot_app.bot.send_document(
                                        chat_id=user_id,
                                        document=file_buf,
                                        filename=filename,
                                        caption=success_text
                                    )
                                    mark_bot_message(user_id, msg.message_id)
                                else:
                                    msg = await bot_app.bot.send_message(
                                        chat_id=user_id,
                                        text=build_delivery_message(
                                            summary_text=success_text,
                                            purchased_items=purchased_items,
                                            format_data=format_data,
                                            description=description,
                                            lang="vi",
                                            html=True,
                                        ),
                                        parse_mode="HTML"
                                    )
                                    mark_bot_message(user_id, msg.message_id)
                            except Exception:
                                pass
                    matched = True
                    break
            if matched:
                continue
        if latest_seen_tx_id and latest_seen_tx_id != last_seen_tx_id:
            await _save_last_seen_tx_id(latest_seen_tx_id)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        # Auto-cancel stale direct orders in sqlite mode as well.
        cursor = await db.execute(
            "SELECT id, user_id, created_at FROM direct_orders WHERE status = 'pending'"
        )
        sqlite_pending_direct_orders = await cursor.fetchall()
        for order_id, user_id, created_at in sqlite_pending_direct_orders:
            if not _is_direct_order_expired(created_at):
                continue
            await db.execute(
                "UPDATE direct_orders SET status = 'cancelled' WHERE id = ?",
                (order_id,)
            )
            logger.info("⏱️ Auto-cancel direct order #%s after %sm pending. (sqlite)", order_id, DIRECT_ORDER_PENDING_EXPIRE_MINUTES)
            if bot_app:
                try:
                    await bot_app.bot.send_message(
                        user_id,
                        f"⌛ Đơn thanh toán #{order_id} đã hết hạn sau {DIRECT_ORDER_PENDING_EXPIRE_MINUTES} phút và đã tự hủy."
                    )
                except Exception:
                    pass
        await db.commit()

        # Lấy pending deposits
        cursor = await db.execute(
            "SELECT id, user_id, amount, code FROM deposits WHERE status = 'pending'"
        )
        pending_deposits = await cursor.fetchall()

        for tx in transactions:
            # Lấy thông tin giao dịch (API trả về amount_in cho tiền vào)
            amount_in = _pick_amount(tx)
            if float(amount_in) <= 0:
                continue

            content = _pick_content(tx)
            content_upper = str(content).upper().strip()
            content_norm = _normalize_content(content)
            amount = int(float(amount_in))
            tx_id = _pick_tx_id(tx)

            # Kiểm tra đã xử lý chưa
            if not tx_id:
                continue
            if not _is_tx_newer_than_checkpoint(tx_id, last_seen_tx_id):
                continue
            _log_tx_seen(tx_id, amount, content)
            cursor = await db.execute(
                "SELECT 1 FROM processed_transactions WHERE tx_id = ?", (tx_id,)
            )
            if await cursor.fetchone():
                continue

            # Tìm deposit khớp
            for deposit in pending_deposits:
                deposit_id, user_id, expected_amount, code = deposit

                code_upper = code.upper()
                code_norm = _normalize_content(code)
                if code_upper in content_upper or code_norm in content_norm:
                    # Cộng tiền
                    await db.execute(
                        "UPDATE deposits SET status = 'confirmed' WHERE id = ?",
                        (deposit_id,)
                    )
                    await db.execute(
                        "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                        (amount, user_id)
                    )
                    # Đánh dấu đã xử lý
                    await db.execute(
                        "INSERT INTO processed_transactions (tx_id) VALUES (?)",
                        (tx_id,)
                    )
                    await db.commit()

                    print(f"✅ Confirmed: User {user_id}, Amount {amount:,}đ")

                    # Thông báo user
                    if bot_app:
                        try:
                            # Lấy số dư mới
                            cursor = await db.execute(
                                "SELECT balance FROM users WHERE user_id = ?", (user_id,)
                            )
                            new_balance = (await cursor.fetchone())[0]

                            await bot_app.bot.send_message(
                                user_id,
                                f"✅ NẠP TIỀN THÀNH CÔNG!\n\n"
                                f"💰 Số tiền: {amount:,}đ\n"
                                f"💳 Số dư hiện tại: {new_balance:,}đ"
                            )
                        except:
                            pass
                    break
        if latest_seen_tx_id and latest_seen_tx_id != last_seen_tx_id:
            await _save_last_seen_tx_id(latest_seen_tx_id)

async def init_checker_db():
    """Tạo bảng lưu giao dịch đã xử lý"""
    if USE_SUPABASE:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS processed_transactions (
                tx_id TEXT PRIMARY KEY,
                processed_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def run_checker(bot_app=None, interval=30):
    """Chạy checker định kỳ"""
    await init_checker_db()
    logger.info("🔄 SePay checker started (interval: %ss, supabase=%s)", interval, USE_SUPABASE)
    last_mode = None
    
    while True:
        try:
            await process_transactions(bot_app)
        except Exception as e:
            logger.exception("Checker error: %s", e)
        fast_mode = has_latest_vietqr_message()
        mode = "fast" if fast_mode else "normal"
        if mode != last_mode:
            logger.info("SePay checker mode: %s", mode)
            last_mode = mode
        await asyncio.sleep(5 if fast_mode else interval)

if __name__ == "__main__":
    asyncio.run(run_checker())
