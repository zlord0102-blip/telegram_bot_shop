"""
Tự động check giao dịch từ SePay API (không cần webhook/domain)
"""
import asyncio
import aiohttp
import os
import io
import json
import logging
import ssl
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import certifi
from config import SEPAY_API_TOKEN
from helpers.binance_client import BinanceApiError, BinanceConfigError, get_binance_direct_runtime
from helpers.sepay_state import has_latest_vietqr_message, mark_bot_message
from helpers.formatting import format_stock_items
from helpers.purchase_messages import (
    build_delivery_message,
    build_display_name,
    build_purchase_summary_text,
)
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError, TimedOut
from database import (
    build_bot_delivery_payload_for_direct_order,
    ensure_bot_delivery_outbox,
    get_setting,
    get_due_bot_delivery_outbox,
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
    get_pending_binance_direct_orders,
    record_direct_order_external_payment,
    is_processed_binance_deposit,
    mark_processed_binance_deposit,
    get_pending_website_direct_orders,
    set_website_direct_order_status,
    mark_bot_delivery_outbox_failed,
    mark_bot_delivery_outbox_sending,
    mark_bot_delivery_outbox_sent,
    schedule_bot_delivery_outbox_retry,
    get_recent_confirmed_direct_orders_missing_delivery,
)

SEPAY_DEBUG = os.getenv("SEPAY_DEBUG", "").lower() in ("1", "true", "yes")
SEPAY_LIMIT = os.getenv("SEPAY_LIMIT", "").strip()
SEPAY_FROM_DATE = os.getenv("SEPAY_FROM_DATE", "").strip()
SEPAY_TO_DATE = os.getenv("SEPAY_TO_DATE", "").strip()
SEPAY_LAST_SEEN_TX_ID_KEY = "sepay_last_seen_tx_id"
BINANCE_LAST_CHECKED_INSERT_TIME_KEY = "binance_last_checked_insert_time_ms"
PAYMENT_RELAY_NOTIFY_TOKEN_KEY = "payment_notify_bot_token"
PAYMENT_RELAY_NOTIFY_USER_ID_KEY = "payment_notify_user_id"
CHECKER_HEALTH_KEY = "bot_checker_health"


def _env_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return max(1, int(str(raw).strip()))
    except (TypeError, ValueError):
        return default


def _env_positive_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        return max(0.1, float(str(raw).strip()))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


SEPAY_DEFAULT_LIMIT = _env_positive_int("SEPAY_DEFAULT_LIMIT", 200)
DIRECT_ORDER_PENDING_EXPIRE_MINUTES = _env_positive_int("DIRECT_ORDER_PENDING_EXPIRE_MINUTES", 10)
DIRECT_ORDER_PENDING_EXPIRE_SECONDS = DIRECT_ORDER_PENDING_EXPIRE_MINUTES * 60
BINANCE_DIRECT_HISTORY_LIMIT = _env_positive_int("BINANCE_DIRECT_HISTORY_LIMIT", 200)
BINANCE_DIRECT_HISTORY_BUFFER_MS = _env_positive_int("BINANCE_DIRECT_HISTORY_BUFFER_MS", 15 * 60 * 1000)
_BINANCE_AMOUNT_QUANT = Decimal("0.000001")
BOT_DELIVERY_RETRY_BASE_SECONDS = _env_positive_int("BOT_DELIVERY_RETRY_BASE_SECONDS", 60)
BOT_DELIVERY_RETRY_MAX_SECONDS = _env_positive_int("BOT_DELIVERY_RETRY_MAX_SECONDS", 30 * 60)
BOT_DELIVERY_RETRY_BATCH_LIMIT = _env_positive_int("BOT_DELIVERY_RETRY_BATCH_LIMIT", 20)
PAYMENT_RELAY_TIMEOUT_SECONDS = _env_positive_float("PAYMENT_RELAY_TIMEOUT_SECONDS", 20.0)
PAYMENT_RELAY_SSL_VERIFY = not _env_bool("PAYMENT_RELAY_SSL_NO_VERIFY", False)
logger = logging.getLogger(__name__)
_SEPAY_TOKEN_WARNED = False
_SEPAY_TOKEN_OK = False
_PAYMENT_RELAY_SSL_WARNING_LOGGED = False


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
    token = str(await get_setting(PAYMENT_RELAY_NOTIFY_TOKEN_KEY, "") or "").strip()
    chat_id = _parse_chat_id(await get_setting(PAYMENT_RELAY_NOTIFY_USER_ID_KEY, ""))
    return token, chat_id


def _build_payment_relay_ssl_context():
    global _PAYMENT_RELAY_SSL_WARNING_LOGGED

    if not PAYMENT_RELAY_SSL_VERIFY:
        if not _PAYMENT_RELAY_SSL_WARNING_LOGGED:
            logger.warning(
                "PAYMENT_RELAY_SSL_NO_VERIFY is enabled; SSL verification is disabled only for payment relay notify."
            )
            _PAYMENT_RELAY_SSL_WARNING_LOGGED = True
        return False

    ca_bundle = (
        os.getenv("PAYMENT_RELAY_CA_BUNDLE", "").strip()
        or os.getenv("SSL_CERT_FILE", "").strip()
        or os.getenv("REQUESTS_CA_BUNDLE", "").strip()
        or certifi.where()
    )
    try:
        return ssl.create_default_context(cafile=ca_bundle)
    except Exception as exc:
        logger.warning(
            "Unable to load payment relay CA bundle %s (%s); falling back to certifi.",
            ca_bundle,
            exc,
        )
        return ssl.create_default_context(cafile=certifi.where())


async def send_payment_relay_notification(relay_token: str, relay_chat_id: int | None, text: str):
    if not relay_token or relay_chat_id is None:
        return False

    url = f"https://api.telegram.org/bot{relay_token}/sendMessage"
    payload = {
        "chat_id": relay_chat_id,
        "text": str(text or "").strip(),
    }

    timeout = aiohttp.ClientTimeout(total=PAYMENT_RELAY_TIMEOUT_SECONDS)
    ssl_context = _build_payment_relay_ssl_context()

    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(url, json=payload, ssl=ssl_context) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("Relay notify failed (HTTP %s): %s", resp.status, body[:200])
                    return False
                data = await resp.json()
                if not data.get("ok"):
                    logger.warning("Relay notify failed: %s", data)
                    return False
                return True
        except aiohttp.ClientConnectorCertificateError as e:
            logger.warning(
                "Relay notify SSL verification failed: %s. "
                "Install the intercepting CA and set PAYMENT_RELAY_CA_BUNDLE, "
                "or temporarily set PAYMENT_RELAY_SSL_NO_VERIFY=true only for this relay path.",
                e,
            )
            return False
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


def build_bot_binance_payment_relay_text(
    *,
    direct_order_id: int,
    user_id: int,
    display_name: str,
    code: str,
    payment_id: str,
    tx_id: str,
    amount_asset: str,
    payment_asset: str,
    payment_network: str,
    expected_amount_vnd: int,
    product_name: str,
    quantity: int,
    delivered_quantity: int,
    bonus_quantity: int,
) -> str:
    return "\n".join(
        [
            "✅ Thanh toán thành công (Bot - Binance)",
            f"Mã đơn hệ thống: {direct_order_id}",
            f"Mã người dùng: {user_id}",
            f"Tên người dùng: {display_name}",
            f"Mã thanh toán: {code}",
            f"Mã deposit Binance: {payment_id}",
            f"Mã giao dịch: {tx_id}",
            "",
            f"Số tiền nhận: {amount_asset} {payment_asset}",
            f"Mạng: {payment_network}",
            f"Quy đổi lúc tạo đơn: {expected_amount_vnd:,}đ",
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
    return await _load_setting_value(SEPAY_LAST_SEEN_TX_ID_KEY)


async def _save_last_seen_tx_id(tx_id: str):
    tx_text = str(tx_id or "").strip()
    if not tx_text:
        return
    await _save_setting_value(SEPAY_LAST_SEEN_TX_ID_KEY, tx_text)


async def _load_setting_value(key: str) -> str:
    return str(await get_setting(key, "") or "").strip()


async def _save_setting_value(key: str, value: str):
    text = str(value or "").strip()
    await set_setting(key, text)


async def _load_checker_health_state() -> dict:
    raw_value = await _load_setting_value(CHECKER_HEALTH_KEY)
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


async def _save_checker_health_state(state: dict):
    payload = {
        "heartbeatAt": str(state.get("heartbeatAt") or "").strip() or None,
        "lastSuccessAt": str(state.get("lastSuccessAt") or "").strip() or None,
        "lastError": str(state.get("lastError") or "").strip()[:500] or None,
        "mode": str(state.get("mode") or "normal").strip() or "normal",
        "loopState": str(state.get("loopState") or "unknown").strip() or "unknown",
        "intervalSeconds": max(1, int(state.get("intervalSeconds") or 30)),
        "sleepSeconds": max(1, int(state.get("sleepSeconds") or 30)),
        "lastDurationMs": max(0, int(state.get("lastDurationMs") or 0)),
        "runtime": "supabase",
    }
    await _save_setting_value(CHECKER_HEALTH_KEY, json.dumps(payload, ensure_ascii=False))


async def get_sepay_token():
    """Lấy SePay token từ database"""
    token = await get_setting("sepay_token", "")
    return token or SEPAY_API_TOKEN

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


def _normalize_binance_text(value: str | None) -> str:
    return str(value or "").strip().upper()


def _normalize_binance_amount(value) -> Decimal:
    if isinstance(value, Decimal):
        return value.quantize(_BINANCE_AMOUNT_QUANT, rounding=ROUND_HALF_UP)
    text = str(value or "").strip()
    if not text:
        return Decimal("0").quantize(_BINANCE_AMOUNT_QUANT, rounding=ROUND_HALF_UP)
    try:
        return Decimal(text).quantize(_BINANCE_AMOUNT_QUANT, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return Decimal("0").quantize(_BINANCE_AMOUNT_QUANT, rounding=ROUND_HALF_UP)


def _binance_lookup_key(asset: str, network: str, address: str, tag: str, amount) -> tuple[str, str, str, str, str]:
    return (
        _normalize_binance_text(asset),
        _normalize_binance_text(network),
        _normalize_binance_text(address),
        _normalize_binance_text(tag),
        format(_normalize_binance_amount(amount), "f"),
    )


def _parse_ms_timestamp(value) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


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


def _created_at_to_ms(value: str | None) -> int | None:
    parsed = _parse_created_at(value)
    if not parsed:
        return None
    return int(parsed.timestamp() * 1000)


def _iso_from_timestamp_ms(value) -> str | None:
    parsed_ms = _parse_ms_timestamp(value)
    if parsed_ms is None:
        return None
    return datetime.fromtimestamp(parsed_ms / 1000, tz=timezone.utc).isoformat()


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


async def _auto_cancel_expired_binance_orders(pending_orders, bot_app=None):
    active_orders = []
    for order in pending_orders:
        if not _is_direct_order_expired(order.get("created_at")):
            active_orders.append(order)
            continue

        order_id = int(order.get("id") or 0)
        user_id = int(order.get("user_id") or 0)
        await set_direct_order_status(order_id, "cancelled")
        logger.info("⏱️ Auto-cancel Binance direct order #%s after %sm pending.", order_id, DIRECT_ORDER_PENDING_EXPIRE_MINUTES)
        if bot_app and user_id:
            try:
                await bot_app.bot.send_message(
                    user_id,
                    f"⌛ Đơn thanh toán Binance #{order_id} đã hết hạn sau {DIRECT_ORDER_PENDING_EXPIRE_MINUTES} phút và đã tự hủy."
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


def _payload_value(payload: dict, *keys, default=None):
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return payload.get(key)
    return default


def _build_bot_delivery_outbox_payload(fulfillment: dict, fallback_amount: int) -> dict:
    items = [str(item or "") for item in (fulfillment.get("items") or [])]
    return {
        "directOrderId": int(fulfillment.get("direct_order_id") or 0),
        "orderId": int(fulfillment.get("order_id") or 0) or None,
        "userId": int(fulfillment.get("user_id") or 0),
        "productId": int(fulfillment.get("product_id") or 0),
        "productName": str(fulfillment.get("product_name") or f"#{fulfillment.get('product_id') or ''}"),
        "description": str(fulfillment.get("description") or ""),
        "formatData": str(fulfillment.get("format_data") or ""),
        "quantity": int(fulfillment.get("quantity") or len(items) or 1),
        "bonusQuantity": int(fulfillment.get("bonus_quantity") or 0),
        "deliveredQuantity": int(fulfillment.get("delivered_quantity") or len(items) or 1),
        "amount": int(fulfillment.get("amount") or fallback_amount or 0),
        "code": str(fulfillment.get("code") or ""),
        "orderGroup": str(fulfillment.get("order_group") or ""),
        "items": items,
    }


def _compute_bot_delivery_retry_delay(attempt_count: int, retry_after_seconds: int | None = None) -> int:
    if retry_after_seconds and retry_after_seconds > 0:
        return min(BOT_DELIVERY_RETRY_MAX_SECONDS, retry_after_seconds)
    return min(
        BOT_DELIVERY_RETRY_MAX_SECONDS,
        BOT_DELIVERY_RETRY_BASE_SECONDS * max(1, 2 ** max(0, attempt_count - 1)),
    )


def _classify_bot_delivery_exception(exc: Exception) -> tuple[bool, str, int | None]:
    if isinstance(exc, RetryAfter):
        retry_after = getattr(exc, "retry_after", None)
        try:
            retry_after_int = int(retry_after) if retry_after is not None else None
        except (TypeError, ValueError):
            retry_after_int = None
        return False, f"RetryAfter: {exc}", retry_after_int

    if isinstance(exc, Forbidden):
        return True, str(exc), None

    if isinstance(exc, BadRequest):
        lowered = str(exc).lower()
        terminal = (
            "chat not found" in lowered
            or "bot was blocked by the user" in lowered
            or "user is deactivated" in lowered
        )
        return terminal, str(exc), None

    if isinstance(exc, (TimedOut, NetworkError)):
        return False, str(exc), None

    if isinstance(exc, TelegramError):
        lowered = str(exc).lower()
        terminal = (
            "forbidden" in lowered
            or "chat not found" in lowered
            or "bot was blocked by the user" in lowered
            or "user is deactivated" in lowered
        )
        return terminal, str(exc), None

    lowered = str(exc).lower()
    terminal = (
        "forbidden" in lowered
        or "chat not found" in lowered
        or "bot was blocked by the user" in lowered
        or "user is deactivated" in lowered
    )
    return terminal, str(exc), None


async def _send_bot_fulfillment_delivery(bot_app, user_id: int, fulfillment: dict, fallback_amount: int):
    if not bot_app:
        return {"ok": False, "terminal": False, "error": "Bot app unavailable"}

    purchased_items = [str(item or "") for item in (_payload_value(fulfillment, "items", default=[]) or [])]
    product_name = str(
        _payload_value(
            fulfillment,
            "product_name",
            "productName",
            default=f"#{_payload_value(fulfillment, 'product_id', 'productId', default='')}",
        )
    )
    description = str(_payload_value(fulfillment, "description", default="") or "").strip()
    format_data = _payload_value(fulfillment, "format_data", "formatData")
    total_text = f"{int(_payload_value(fulfillment, 'amount', default=fallback_amount) or 0):,}đ"
    header_lines = [
        f"Loại hàng: {product_name}",
        f"Số lượng: {len(purchased_items)}",
        f"SL thanh toán: {int(_payload_value(fulfillment, 'quantity', default=len(purchased_items)) or 0)}",
        f"Tổng: {total_text}",
    ]
    if _payload_value(fulfillment, "bonus_quantity", "bonusQuantity"):
        header_lines.append(
            f"Tặng thêm: {int(_payload_value(fulfillment, 'bonus_quantity', 'bonusQuantity', default=0) or 0)}"
        )
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
        bonus_quantity=int(_payload_value(fulfillment, "bonus_quantity", "bonusQuantity", default=0) or 0),
        lang="vi",
    )
    try:
        if len(purchased_items) > 5:
            msg = await bot_app.bot.send_document(
                chat_id=user_id,
                document=file_buf,
                filename=filename,
                caption=success_text,
            )
            mark_bot_message(user_id, msg.message_id)
            return {"ok": True, "terminal": False, "error": None}

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
            parse_mode="HTML",
        )
        mark_bot_message(user_id, msg.message_id)
        return {"ok": True, "terminal": False, "error": None}
    except Exception as exc:
        terminal, error_message, retry_after_seconds = _classify_bot_delivery_exception(exc)
        return {
            "ok": False,
            "terminal": terminal,
            "error": error_message,
            "retry_after_seconds": retry_after_seconds,
        }


async def _process_bot_delivery_outbox_row(bot_app, outbox: dict):
    if not outbox:
        return

    payload = dict(outbox.get("payload") or {})
    outbox_id = int(outbox.get("id") or 0)
    direct_order_id = int(outbox.get("direct_order_id") or payload.get("directOrderId") or 0)
    user_id = int(outbox.get("user_id") or payload.get("userId") or 0)
    attempt_count = int(outbox.get("attempt_count") or 0) + 1

    if outbox_id <= 0 or user_id <= 0:
        return

    await mark_bot_delivery_outbox_sending(outbox_id, attempt_count)
    delivery_result = await _send_bot_fulfillment_delivery(
        bot_app,
        user_id,
        payload,
        int(payload.get("amount") or 0),
    )

    if delivery_result.get("ok"):
        await mark_bot_delivery_outbox_sent(outbox_id, attempt_count)
        logger.info("Delivery send success: direct_order=%s outbox=%s attempts=%s", direct_order_id, outbox_id, attempt_count)
        return

    error_message = str(delivery_result.get("error") or "delivery_failed")
    if delivery_result.get("terminal"):
        await mark_bot_delivery_outbox_failed(outbox_id, attempt_count, error_message)
        logger.warning("Delivery send terminal failure: direct_order=%s outbox=%s error=%s", direct_order_id, outbox_id, error_message)
        return

    next_retry_seconds = _compute_bot_delivery_retry_delay(
        attempt_count,
        delivery_result.get("retry_after_seconds"),
    )
    next_retry_at = datetime.fromtimestamp(datetime.now().timestamp() + next_retry_seconds).isoformat()
    await schedule_bot_delivery_outbox_retry(outbox_id, attempt_count, error_message, next_retry_at)
    logger.warning(
        "Delivery retry scheduled: direct_order=%s outbox=%s retry_in=%ss error=%s",
        direct_order_id,
        outbox_id,
        next_retry_seconds,
        error_message,
    )


async def _enqueue_bot_delivery_outbox_and_send(bot_app, fulfillment: dict, fallback_amount: int):
    payload = _build_bot_delivery_outbox_payload(fulfillment, fallback_amount)
    direct_order_id = int(payload.get("directOrderId") or 0)
    user_id = int(payload.get("userId") or 0)

    outbox = await ensure_bot_delivery_outbox(
        direct_order_id,
        user_id,
        payload,
        reset_status=False,
    )
    if not outbox:
        logger.warning("Delivery outbox unavailable; using direct-send fallback for direct_order=%s", direct_order_id)
        delivery_result = await _send_bot_fulfillment_delivery(bot_app, user_id, payload, int(payload.get("amount") or 0))
        if not delivery_result.get("ok"):
            logger.warning(
                "Delivery send failed without outbox: direct_order=%s error=%s",
                direct_order_id,
                delivery_result.get("error") or "delivery_failed",
            )
        return

    logger.info("Delivery outbox enqueued: direct_order=%s outbox=%s", direct_order_id, outbox.get("id"))
    if not bot_app:
        return
    await _process_bot_delivery_outbox_row(bot_app, outbox)


async def _reconcile_bot_delivery_outbox():
    try:
        recent_orders = await get_recent_confirmed_direct_orders_missing_delivery(
            limit=BOT_DELIVERY_RETRY_BATCH_LIMIT,
            hours=48,
        )
    except Exception as exc:
        logger.warning("Unable to load delivery-outbox reconcile candidates: %s", exc)
        return

    for row in recent_orders:
        direct_order_id = int(row.get("id") or 0)
        if direct_order_id <= 0:
            continue
        try:
            payload = await build_bot_delivery_payload_for_direct_order(direct_order_id)
        except Exception as exc:
            logger.warning("Unable to rebuild delivery payload for order %s: %s", direct_order_id, exc)
            continue
        if not payload:
            continue
        try:
            outbox = await ensure_bot_delivery_outbox(
                direct_order_id,
                int(payload.get("userId") or 0),
                payload,
                reset_status=False,
            )
        except Exception as exc:
            logger.warning("Unable to reconcile delivery outbox for order %s: %s", direct_order_id, exc)
            continue
        if outbox:
            logger.info("Delivery outbox reconciled: direct_order=%s outbox=%s", direct_order_id, outbox.get("id"))


async def _process_due_bot_delivery_outbox(bot_app):
    if not bot_app:
        return

    await _reconcile_bot_delivery_outbox()

    try:
        due_rows = await get_due_bot_delivery_outbox(BOT_DELIVERY_RETRY_BATCH_LIMIT)
    except Exception as exc:
        logger.warning("Unable to load due delivery outbox rows: %s", exc)
        return

    for outbox in due_rows:
        try:
            await _process_bot_delivery_outbox_row(bot_app, outbox)
        except Exception as exc:
            logger.warning("Unexpected error while processing delivery outbox row: %s", exc)


def _get_binance_history_start_ms(pending_orders, checkpoint_ms: int | None) -> int | None:
    created_ms_values = [
        created_ms
        for created_ms in (_created_at_to_ms(order.get("created_at")) for order in pending_orders)
        if created_ms is not None
    ]
    earliest_pending_ms = min(created_ms_values) if created_ms_values else None
    candidates = []
    if checkpoint_ms is not None and checkpoint_ms > 0:
        candidates.append(max(0, checkpoint_ms - BINANCE_DIRECT_HISTORY_BUFFER_MS))
    if earliest_pending_ms is not None and earliest_pending_ms > 0:
        candidates.append(max(0, earliest_pending_ms - BINANCE_DIRECT_HISTORY_BUFFER_MS))
    if not candidates:
        return None
    return max(candidates)


def _binance_deposit_matches_order(deposit: dict, order: dict) -> bool:
    deposit_key = _binance_lookup_key(
        deposit.get("coin"),
        deposit.get("network"),
        deposit.get("address"),
        deposit.get("addressTag"),
        deposit.get("amount"),
    )
    order_key = _binance_lookup_key(
        order.get("payment_asset"),
        order.get("payment_network"),
        order.get("payment_address"),
        order.get("payment_address_tag"),
        order.get("payment_amount_asset"),
    )
    if deposit_key != order_key:
        return False

    deposit_ms = _parse_ms_timestamp(deposit.get("insertTime")) or _parse_ms_timestamp(deposit.get("completeTime"))
    order_created_ms = _created_at_to_ms(order.get("created_at"))
    if deposit_ms is None or order_created_ms is None:
        return True

    earliest_ms = max(0, order_created_ms - 60_000)
    latest_ms = order_created_ms + (DIRECT_ORDER_PENDING_EXPIRE_SECONDS * 1000)
    return earliest_ms <= deposit_ms <= latest_ms


async def _process_binance_direct_orders(bot_app=None, relay_token: str = "", relay_chat_id: int | None = None):
    try:
        pending_orders = await get_pending_binance_direct_orders()
    except Exception as exc:
        logger.warning("Unable to load pending Binance direct orders: %s", exc)
        return

    if not pending_orders:
        return

    pending_orders = await _auto_cancel_expired_binance_orders(pending_orders, bot_app)
    if not pending_orders:
        return

    try:
        runtime = await get_binance_direct_runtime()
    except (BinanceConfigError, BinanceApiError) as exc:
        logger.warning("Skip Binance direct checker: %s", exc)
        return

    if not runtime.get("available"):
        return

    checkpoint_ms = _parse_ms_timestamp(await _load_setting_value(BINANCE_LAST_CHECKED_INSERT_TIME_KEY))
    start_time_ms = _get_binance_history_start_ms(pending_orders, checkpoint_ms)

    try:
        deposits = await runtime["client"].get_deposit_history(
            coin=str(runtime["coin"]),
            status=1,
            start_time=start_time_ms,
            limit=BINANCE_DIRECT_HISTORY_LIMIT,
        )
    except (BinanceConfigError, BinanceApiError) as exc:
        logger.warning("Unable to fetch Binance deposit history: %s", exc)
        return

    latest_seen_insert_ms = checkpoint_ms or 0
    orders_by_key = {
        _binance_lookup_key(
            order.get("payment_asset"),
            order.get("payment_network"),
            order.get("payment_address"),
            order.get("payment_address_tag"),
            order.get("payment_amount_asset"),
        ): order
        for order in pending_orders
    }

    for deposit in sorted(deposits, key=lambda item: _parse_ms_timestamp(item.get("insertTime")) or 0):
        payment_id = str(deposit.get("id") or "").strip()
        tx_id = str(deposit.get("txId") or "").strip()
        if not payment_id or not tx_id:
            continue

        insert_ms = _parse_ms_timestamp(deposit.get("insertTime")) or _parse_ms_timestamp(deposit.get("completeTime")) or 0
        latest_seen_insert_ms = max(latest_seen_insert_ms, insert_ms)

        if await is_processed_binance_deposit(payment_id):
            continue

        lookup_key = _binance_lookup_key(
            deposit.get("coin"),
            deposit.get("network"),
            deposit.get("address"),
            deposit.get("addressTag"),
            deposit.get("amount"),
        )
        order = orders_by_key.get(lookup_key)
        if not order or not _binance_deposit_matches_order(deposit, order):
            continue

        order_id = int(order.get("id") or 0)
        user_id = int(order.get("user_id") or 0)
        order_group = f"BNPAY{user_id}{datetime.now().strftime('%Y%m%d%H%M%S')}"

        try:
            fulfillment = await fulfill_bot_direct_order(
                order_id,
                order_group=order_group,
                expire_minutes=DIRECT_ORDER_PENDING_EXPIRE_MINUTES,
            )
        except DirectOrderFulfillmentError as exc:
            logger.info("Skip Binance auto-fulfill for order=%s payment=%s reason=%s", order_id, payment_id, exc.code)
            await mark_processed_binance_deposit(
                payment_id,
                tx_id=tx_id,
                direct_order_id=order_id or None,
                amount_asset=format(_normalize_binance_amount(deposit.get("amount")), "f"),
                payment_asset=str(deposit.get("coin") or runtime.get("coin") or ""),
                payment_network=str(deposit.get("network") or runtime.get("network") or ""),
            )
            orders_by_key.pop(lookup_key, None)
            continue

        paid_at = _iso_from_timestamp_ms(deposit.get("completeTime")) or _iso_from_timestamp_ms(deposit.get("insertTime"))
        try:
            await record_direct_order_external_payment(
                order_id,
                payment_id=payment_id,
                tx_id=tx_id,
                paid_at=paid_at,
            )
        except Exception as exc:
            logger.warning("Unable to persist Binance external payment metadata for order %s: %s", order_id, exc)

        await mark_processed_binance_deposit(
            payment_id,
            tx_id=tx_id,
            direct_order_id=int(fulfillment.get("direct_order_id") or order_id or 0) or None,
            amount_asset=format(_normalize_binance_amount(deposit.get("amount")), "f"),
            payment_asset=str(deposit.get("coin") or runtime.get("coin") or ""),
            payment_network=str(deposit.get("network") or runtime.get("network") or ""),
        )

        purchased_items = [str(item or "") for item in (fulfillment.get("items") or [])]
        await send_payment_relay_notification(
            relay_token,
            relay_chat_id,
            build_bot_binance_payment_relay_text(
                direct_order_id=int(fulfillment.get("direct_order_id") or order_id or 0),
                user_id=int(fulfillment.get("user_id") or user_id),
                display_name=await resolve_user_display_name(user_id),
                code=str(fulfillment.get("code") or order.get("code") or ""),
                payment_id=payment_id,
                tx_id=tx_id,
                amount_asset=format(_normalize_binance_amount(deposit.get("amount")), "f"),
                payment_asset=str(deposit.get("coin") or runtime.get("coin") or ""),
                payment_network=str(deposit.get("network") or runtime.get("network") or ""),
                expected_amount_vnd=int(fulfillment.get("amount") or order.get("amount") or 0),
                product_name=str(fulfillment.get("product_name") or f"#{order.get('product_id') or ''}"),
                quantity=int(fulfillment.get("quantity") or order.get("quantity") or 0),
                delivered_quantity=len(purchased_items),
                bonus_quantity=int(fulfillment.get("bonus_quantity") or 0),
            ),
        )
        await _enqueue_bot_delivery_outbox_and_send(
            bot_app,
            fulfillment,
            int(fulfillment.get("amount") or order.get("amount") or 0),
        )
        orders_by_key.pop(lookup_key, None)
        logger.info(
            "✅ Binance direct order confirmed: order_id=%s payment_id=%s tx_id=%s amount=%s %s",
            order_id,
            payment_id,
            tx_id,
            format(_normalize_binance_amount(deposit.get("amount")), "f"),
            str(deposit.get("coin") or runtime.get("coin") or ""),
        )

    if latest_seen_insert_ms and latest_seen_insert_ms != (checkpoint_ms or 0):
        await _save_setting_value(BINANCE_LAST_CHECKED_INSERT_TIME_KEY, str(latest_seen_insert_ms))

async def process_transactions(bot_app=None):
    """Xử lý giao dịch và cộng tiền tự động"""
    transactions = await get_recent_transactions()
    last_seen_tx_id = await _load_last_seen_tx_id()
    latest_seen_tx_id = str(last_seen_tx_id or "").strip()
    for tx in transactions:
        latest_seen_tx_id = _pick_newer_tx_id(latest_seen_tx_id, _pick_tx_id(tx))

    relay_token, relay_chat_id = await get_payment_relay_target()
    if True:
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
                        await _enqueue_bot_delivery_outbox_and_send(
                            bot_app,
                            fulfillment,
                            int(expected_amount or 0),
                        )
                    matched = True
                    break
            if matched:
                continue
        if latest_seen_tx_id and latest_seen_tx_id != last_seen_tx_id:
            await _save_last_seen_tx_id(latest_seen_tx_id)
        await _process_binance_direct_orders(bot_app, relay_token, relay_chat_id)
        await _process_due_bot_delivery_outbox(bot_app)
        return

async def init_checker_db():
    """Compatibility no-op; checker state is stored in Supabase."""
    return

async def run_checker(bot_app=None, interval=30):
    """Chạy checker định kỳ"""
    await init_checker_db()
    logger.info("🔄 SePay checker started (interval: %ss, runtime=supabase)", interval)
    last_mode = None
    health_state = await _load_checker_health_state()
    health_state.update(
        {
            "loopState": "starting",
            "mode": "normal",
            "intervalSeconds": max(1, int(interval or 30)),
            "sleepSeconds": max(1, int(interval or 30)),
            "useSupabase": True,
        }
    )
    try:
        await _save_checker_health_state(health_state)
    except Exception as exc:
        logger.warning("Unable to persist initial checker health: %s", exc)
    
    while True:
        loop_started_at = datetime.now(timezone.utc)
        error_message = None
        loop_state = "ok"
        try:
            await process_transactions(bot_app)
        except Exception as e:
            error_message = str(e or "checker_error")
            loop_state = "error"
            logger.exception("Checker error: %s", e)
        fast_mode = has_latest_vietqr_message()
        mode = "fast" if fast_mode else "normal"
        if mode != last_mode:
            logger.info("SePay checker mode: %s", mode)
            last_mode = mode
        sleep_seconds = 5 if fast_mode else interval
        heartbeat_at = datetime.now(timezone.utc).isoformat()
        duration_ms = max(0, int((datetime.now(timezone.utc) - loop_started_at).total_seconds() * 1000))
        health_state.update(
            {
                "heartbeatAt": heartbeat_at,
                "mode": mode,
                "loopState": loop_state,
                "intervalSeconds": max(1, int(interval or 30)),
                "sleepSeconds": max(1, int(sleep_seconds or 1)),
                "lastDurationMs": duration_ms,
                "useSupabase": True,
                "lastError": error_message[:500] if error_message else None,
            }
        )
        if not error_message:
            health_state["lastSuccessAt"] = heartbeat_at
        try:
            await _save_checker_health_state(health_state)
        except Exception as exc:
            logger.warning("Unable to persist checker health: %s", exc)
        await asyncio.sleep(sleep_seconds)

if __name__ == "__main__":
    asyncio.run(run_checker())
