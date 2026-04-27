import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .supabase_client import get_supabase_client


DEFAULT_SALE_CUSTOM_EMOJI_ID = "6055192572056309981"


def _now_iso() -> str:
    return datetime.now().isoformat()


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _safe_custom_emoji_id(value: Any) -> str:
    text = str(value or "").strip()
    return "".join(char for char in text if char.isdigit())[:64]


class DirectOrderFulfillmentError(RuntimeError):
    def __init__(self, code: str, message: Optional[str] = None):
        super().__init__(message or code)
        self.code = code


class BinanceDirectOrderError(RuntimeError):
    def __init__(self, code: str, message: Optional[str] = None):
        super().__init__(message or code)
        self.code = code


def _normalize_rpc_payload(data: Any) -> Dict[str, Any]:
    if isinstance(data, list):
        if data and isinstance(data[0], dict):
            return dict(data[0])
        return {}
    if isinstance(data, dict):
        return dict(data)
    return {}


def _is_missing_rpc_error_message(message: str) -> bool:
    lowered = str(message or "").lower()
    return (
        "could not find the function" in lowered
        or "schema cache" in lowered
        or "pgrst202" in lowered
    )


def _map_fulfillment_error_from_message(message: str, expire_minutes: int) -> DirectOrderFulfillmentError:
    lowered = str(message or "").lower()
    if "forbidden" in lowered:
        return DirectOrderFulfillmentError("forbidden", "forbidden")
    if "user_not_found" in lowered:
        return DirectOrderFulfillmentError("user_not_found")
    if "product_not_found" in lowered:
        return DirectOrderFulfillmentError("product_not_found")
    if "sale_item_not_active" in lowered:
        return DirectOrderFulfillmentError("sale_item_not_active")
    if "sale_user_limit_exceeded" in lowered:
        return DirectOrderFulfillmentError("sale_user_limit_exceeded")
    if "sale_usdt_not_available" in lowered:
        return DirectOrderFulfillmentError("sale_usdt_not_available")
    if "website_direct_order_not_found" in lowered:
        return DirectOrderFulfillmentError("website_direct_order_not_found")
    if "mirror_direct_order_not_found" in lowered:
        return DirectOrderFulfillmentError("mirror_direct_order_not_found")
    if "direct_order_not_found" in lowered:
        return DirectOrderFulfillmentError("direct_order_not_found")
    if "insufficient_usdt_balance" in lowered:
        return DirectOrderFulfillmentError("insufficient_usdt_balance")
    if "insufficient_balance" in lowered:
        return DirectOrderFulfillmentError("insufficient_balance")
    if "website_direct_order_not_pending" in lowered:
        return DirectOrderFulfillmentError("website_direct_order_not_pending")
    if "mirror_direct_order_not_pending" in lowered:
        return DirectOrderFulfillmentError("mirror_direct_order_not_pending")
    if "direct_order_not_pending" in lowered:
        return DirectOrderFulfillmentError("direct_order_not_pending")
    if "website_direct_order_expired" in lowered:
        return DirectOrderFulfillmentError(
            "website_direct_order_expired",
            f"expired_after_{max(1, expire_minutes)}m",
        )
    if "direct_order_expired" in lowered:
        return DirectOrderFulfillmentError(
            "direct_order_expired",
            f"expired_after_{max(1, expire_minutes)}m",
        )
    if "not_enough_stock" in lowered:
        return DirectOrderFulfillmentError("not_enough_stock")
    return DirectOrderFulfillmentError("fulfillment_failed", str(message or "fulfillment_failed"))


def _map_binance_order_error_from_message(message: str) -> BinanceDirectOrderError:
    lowered = str(message or "").lower()
    if "duplicate key value" in lowered or "duplicate_binance_amount" in lowered or "direct_orders_pending_binance_amount_idx" in lowered:
        return BinanceDirectOrderError("duplicate_binance_amount")
    if "forbidden" in lowered:
        return BinanceDirectOrderError("forbidden", "forbidden")
    return BinanceDirectOrderError("binance_direct_order_failed", str(message or "binance_direct_order_failed"))


def _safe_str_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item or "") for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item or "") for item in parsed]
        except Exception:
            pass
    return []


def _safe_json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    return {}


def _is_missing_relation_error_message(message: str) -> bool:
    lowered = str(message or "").lower()
    return "does not exist" in lowered or "relation" in lowered or "schema cache" in lowered


def _normalize_balance_purchase_payload(data: Any) -> Dict[str, Any]:
    payload = _normalize_rpc_payload(data)
    if not payload:
        return {}

    payload["items"] = _safe_str_list(payload.get("items"))
    payload["order_id"] = _safe_optional_int(payload.get("order_id"))
    payload["user_id"] = _safe_int(payload.get("user_id"))
    payload["product_id"] = _safe_int(payload.get("product_id"))
    payload["product_name"] = str(payload.get("product_name") or f"#{payload['product_id']}")
    payload["description"] = str(payload.get("description") or "")
    payload["format_data"] = str(payload.get("format_data") or "")
    payload["quantity"] = _safe_int(payload.get("quantity"), 1)
    payload["bonus_quantity"] = _safe_int(payload.get("bonus_quantity"), 0)
    payload["delivered_quantity"] = _safe_int(payload.get("delivered_quantity"), len(payload["items"]))
    payload["order_group"] = str(payload.get("order_group") or "")
    payload["order_total_price"] = _safe_int(payload.get("order_total_price"))
    payload["charged_balance"] = _safe_int(payload.get("charged_balance"))
    payload["charged_balance_usdt"] = _safe_float(payload.get("charged_balance_usdt"))
    payload["new_balance"] = _safe_int(payload.get("new_balance"))
    payload["new_balance_usdt"] = _safe_float(payload.get("new_balance_usdt"))
    payload["sale_campaign_id"] = _safe_optional_int(payload.get("sale_campaign_id"))
    payload["sale_item_id"] = _safe_optional_int(payload.get("sale_item_id"))
    payload["charge_currency"] = str(payload.get("charge_currency") or "").strip().lower()
    payload["sale_ends_at"] = payload.get("sale_ends_at")
    payload["sale_snapshot"] = _safe_json_object(payload.get("sale_snapshot"))
    return payload


def _normalize_sale_product_row(row: Dict[str, Any]) -> Dict[str, Any]:
    sale_item_id = _safe_int(row.get("sale_item_id"))
    product_id = _safe_int(row.get("product_id"))
    custom_emoji_id = (
        _safe_custom_emoji_id(row.get("telegram_icon_custom_emoji_id"))
        or DEFAULT_SALE_CUSTOM_EMOJI_ID
    )
    return {
        "id": product_id,
        "product_id": product_id,
        "sale_item_id": sale_item_id,
        "sale_campaign_id": _safe_int(row.get("sale_campaign_id")),
        "is_sale": True,
        "name": row.get("name") or f"#{product_id}",
        "telegram_icon": row.get("telegram_icon") or "SALE",
        "telegram_icon_custom_emoji_id": custom_emoji_id,
        "price": _safe_int(row.get("price")),
        "price_usdt": _safe_float(row.get("price_usdt")),
        "original_price": _safe_int(row.get("original_price")),
        "original_price_usdt": _safe_float(row.get("original_price_usdt")),
        "discount_percent": _safe_float(row.get("discount_percent")),
        "price_tiers": _safe_list(row.get("price_tiers")),
        "promo_buy_quantity": _safe_int(row.get("promo_buy_quantity")),
        "promo_bonus_quantity": _safe_int(row.get("promo_bonus_quantity")),
        "description": row.get("description") or "",
        "format_data": row.get("format_data") or "",
        "sort_position": _safe_optional_int(row.get("sort_position")),
        "stock": _safe_int(row.get("stock")),
        "campaign_name": row.get("campaign_name") or "",
        "starts_at": row.get("starts_at"),
        "ends_at": row.get("ends_at"),
        "per_user_limit": _safe_optional_int(row.get("per_user_limit")),
        "quantity_limit": _safe_optional_int(row.get("quantity_limit")),
        "sold_quantity": _safe_int(row.get("sold_quantity")),
    }


def _sale_product_sort_key(product: Dict[str, Any]):
    sort_position = _safe_optional_int(product.get("sort_position"))
    sale_item_id = _safe_optional_int(product.get("sale_item_id"))
    id_fallback = sale_item_id if sale_item_id is not None else 10**12
    return (
        1 if sort_position is None else 0,
        sort_position if sort_position is not None else 10**11,
        id_fallback,
    )


def _parse_created_at(value: Any) -> Optional[datetime]:
    if value is None:
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


def _is_direct_order_expired(created_at: Any, expire_minutes: int) -> bool:
    parsed = _parse_created_at(created_at)
    if not parsed:
        return False
    now_dt = datetime.now(parsed.tzinfo) if parsed.tzinfo else datetime.now()
    return (now_dt - parsed).total_seconds() >= max(1, expire_minutes) * 60


def _product_sort_key(product: Dict[str, Any]):
    sort_position = _safe_optional_int(product.get("sort_position"))
    product_id = _safe_optional_int(product.get("id"))
    id_fallback = product_id if product_id is not None else 10**12
    return (
        1 if sort_position is None else 0,
        sort_position if sort_position is not None else 10**11,
        id_fallback,
    )


def _sort_products_by_position(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(products, key=_product_sort_key)


def _folder_sort_key(folder: Dict[str, Any]):
    sort_position = _safe_optional_int(folder.get("sort_position"))
    folder_id = _safe_optional_int(folder.get("id"))
    id_fallback = folder_id if folder_id is not None else 10**12
    return (
        1 if sort_position is None else 0,
        sort_position if sort_position is not None else 10**11,
        id_fallback,
    )


def _sort_folders_by_position(folders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(folders, key=_folder_sort_key)


async def _fetch_product_positions(product_ids: List[Any]) -> Dict[str, Optional[int]]:
    ids = [pid for pid in product_ids if pid is not None]
    if not ids:
        return {}

    def _fetch():
        return _get_table("products").select("id, sort_position").in_("id", ids).execute()

    try:
        resp = await _to_thread(_fetch)
    except Exception:
        return {}

    position_map: Dict[str, Optional[int]] = {}
    for row in resp.data or []:
        product_id = row.get("id")
        if product_id is None:
            continue
        position_map[str(product_id)] = _safe_optional_int(row.get("sort_position"))
    return position_map


async def _to_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


def _get_table(name: str):
    supabase = get_supabase_client()
    return supabase.table(name)

_settings_cache: Dict[str, Dict[str, Any]] = {"values": {}, "ts": 0.0}
_SETTINGS_TTL_SECONDS = 60
_USER_CACHE_TTL_SECONDS = 30
_user_lang_cache: Dict[int, Tuple[str, float]] = {}


def _cache_get(cache: Dict[int, Tuple[Any, float]], key: int, ttl: int):
    entry = cache.get(key)
    if entry and (time.time() - entry[1] <= ttl):
        return entry[0]
    return None


def _cache_set(cache: Dict[int, Tuple[Any, float]], key: int, value: Any):
    cache[key] = (value, time.time())


def _parse_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("0", "false", "off", "no"):
        return False
    if text in ("1", "true", "on", "yes"):
        return True
    return default


async def init_db():
    # Ensure Supabase client can be created
    await _to_thread(get_supabase_client)


def _dt_to_utc_iso(value: Optional[datetime]) -> str:
    if not value:
        return datetime.now(timezone.utc).isoformat()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


async def log_telegram_message(
    chat_id: int,
    message_id: int,
    direction: str,
    message_type: str = "text",
    text: Optional[str] = None,
    payload: Any = None,
    sent_at: Optional[datetime] = None,
):
    """
    Best-effort chat history logging for the admin dashboard.
    Logging must never break the bot flow, so errors are swallowed.
    """
    if not chat_id or not message_id:
        return

    direction_clean = str(direction).strip().lower()
    if direction_clean not in ("in", "out"):
        direction_clean = "out"

    message_type_clean = str(message_type or "text").strip().lower() or "text"

    row = {
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "direction": direction_clean,
        "message_type": message_type_clean,
        "text": text,
        "payload": payload,
        "sent_at": _dt_to_utc_iso(sent_at),
    }

    def _write():
        table = _get_table("telegram_messages")
        try:
            # Some versions support explicit conflict targets; fall back to plain insert otherwise.
            return table.upsert(row, on_conflict="chat_id,message_id").execute()
        except TypeError:
            return table.insert(row).execute()

    try:
        await _to_thread(_write)
    except Exception:
        return


# User functions
async def get_or_create_user(
    user_id: int,
    username: str = None,
    first_name: str = None,
    last_name: str = None
):
    def _fetch():
        return _get_table("users").select("*").eq(
            "user_id", user_id
        ).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        def _insert():
            payload = {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "language": None,
                "created_at": _now_iso(),
            }
            try:
                return _get_table("users").insert(payload).execute()
            except Exception:
                payload.pop("first_name", None)
                payload.pop("last_name", None)
                return _get_table("users").insert(payload).execute()

        await _to_thread(_insert)
        _cache_set(_user_lang_cache, user_id, "vi")
        return {
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
            "balance": 0,
            "balance_usdt": 0,
            "language": None
        }

    row = data[0]
    def _update_profile():
        payload = {}
        if username:
            payload["username"] = username
        if first_name:
            payload["first_name"] = first_name
        if last_name:
            payload["last_name"] = last_name
        if not payload:
            return None
        try:
            return _get_table("users").update(payload).eq("user_id", user_id).execute()
        except Exception:
            payload.pop("first_name", None)
            payload.pop("last_name", None)
            if not payload:
                return None
            return _get_table("users").update(payload).eq("user_id", user_id).execute()

    await _to_thread(_update_profile)
    balance = _safe_int(row.get("balance"))
    balance_usdt = _safe_float(row.get("balance_usdt"))
    language = row.get("language")
    if language:
        _cache_set(_user_lang_cache, user_id, language)
    return {
        "user_id": row.get("user_id"),
        "username": username or row.get("username"),
        "first_name": first_name or row.get("first_name"),
        "last_name": last_name or row.get("last_name"),
        "balance": balance,
        "balance_usdt": balance_usdt,
        "language": language,
    }


async def get_user_language(user_id: int) -> str:
    cached = _cache_get(_user_lang_cache, user_id, _USER_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached

    def _fetch():
        return _get_table("users").select("language").eq("user_id", user_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data or not data[0].get("language"):
        _cache_set(_user_lang_cache, user_id, "vi")
        return "vi"
    lang = data[0]["language"]
    _cache_set(_user_lang_cache, user_id, lang)
    return lang


async def set_user_language(user_id: int, language: str):
    def _update():
        return _get_table("users").update({"language": language}).eq("user_id", user_id).execute()

    await _to_thread(_update)
    _cache_set(_user_lang_cache, user_id, language)


async def get_balance(user_id: int):
    def _fetch():
        return _get_table("users").select("balance").eq("user_id", user_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    return _safe_int(data[0].get("balance")) if data else 0


async def get_balance_usdt(user_id: int):
    def _fetch():
        return _get_table("users").select("balance_usdt").eq("user_id", user_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    return _safe_float(data[0].get("balance_usdt")) if data else 0


async def _set_balance(user_id: int, new_balance: int):
    def _update():
        return _get_table("users").update({"balance": new_balance}).eq("user_id", user_id).execute()

    await _to_thread(_update)


async def _set_balance_usdt(user_id: int, new_balance: float):
    def _update():
        return _get_table("users").update({"balance_usdt": new_balance}).eq("user_id", user_id).execute()

    await _to_thread(_update)


async def update_balance(user_id: int, amount: int):
    current = await get_balance(user_id)
    await _set_balance(user_id, current + amount)


async def update_balance_usdt(user_id: int, amount: float):
    current = await get_balance_usdt(user_id)
    await _set_balance_usdt(user_id, current + amount)


# Product functions
async def get_products():
    def _rpc():
        return get_supabase_client().rpc("get_products_with_stock").execute()

    try:
        resp = await _to_thread(_rpc)
        rows = resp.data or []
        position_map = await _fetch_product_positions([row.get("id") for row in rows])
        products = []
        for row in rows:
            product_id = row.get("id")
            products.append({
                "id": product_id,
                "name": row.get("name"),
                "telegram_icon": row.get("telegram_icon") or "",
                "telegram_icon_custom_emoji_id": row.get("telegram_icon_custom_emoji_id") or "",
                "price": _safe_int(row.get("price")),
                "description": row.get("description"),
                "stock": _safe_int(row.get("stock")),
                "price_usdt": _safe_float(row.get("price_usdt")),
                "format_data": row.get("format_data"),
                "price_tiers": _safe_list(row.get("price_tiers")),
                "promo_buy_quantity": _safe_int(row.get("promo_buy_quantity")),
                "promo_bonus_quantity": _safe_int(row.get("promo_bonus_quantity")),
                "sort_position": position_map.get(str(product_id), _safe_optional_int(row.get("sort_position"))),
                "bot_folder_id": _safe_optional_int(row.get("bot_folder_id")),
            })
        return _sort_products_by_position(products)
    except Exception:
        # Fallback to per-product counting if RPC not available
        def _fetch():
            try:
                return _get_table("products").select(
                    "id, name, telegram_icon, telegram_icon_custom_emoji_id, price, description, price_usdt, format_data, price_tiers, promo_buy_quantity, promo_bonus_quantity, sort_position, bot_folder_id"
                ).eq("is_deleted", False).eq("is_hidden", False).order("id").execute()
            except Exception:
                try:
                    return _get_table("products").select(
                        "id, name, telegram_icon, price, description, price_usdt, format_data, price_tiers, promo_buy_quantity, promo_bonus_quantity, sort_position, bot_folder_id"
                    ).eq("is_deleted", False).eq("is_hidden", False).order("id").execute()
                except Exception:
                    return _get_table("products").select(
                        "id, name, price, description, price_usdt, format_data"
                    ).order("id").execute()

        resp = await _to_thread(_fetch)
        rows = resp.data or []
        products = []
        for row in rows:
            product_id = row.get("id")

            def _stock():
                return _get_table("stock").select("id").eq("product_id", product_id).eq("sold", False).execute()

            stock_resp = await _to_thread(_stock)
            stock_count = len(stock_resp.data or [])
            products.append({
                "id": product_id,
                "name": row.get("name"),
                "telegram_icon": row.get("telegram_icon") or "",
                "telegram_icon_custom_emoji_id": row.get("telegram_icon_custom_emoji_id") or "",
                "price": _safe_int(row.get("price")),
                "description": row.get("description"),
                "stock": stock_count,
                "price_usdt": _safe_float(row.get("price_usdt")),
                "format_data": row.get("format_data"),
                "price_tiers": _safe_list(row.get("price_tiers")),
                "promo_buy_quantity": _safe_int(row.get("promo_buy_quantity")),
                "promo_bonus_quantity": _safe_int(row.get("promo_bonus_quantity")),
                "sort_position": _safe_optional_int(row.get("sort_position")),
                "bot_folder_id": _safe_optional_int(row.get("bot_folder_id")),
            })
        return _sort_products_by_position(products)


async def search_products(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    terms = [term for term in str(query or "").strip().lower().split() if term]
    if not terms:
        return []

    products = await get_products()
    matches: List[Tuple[int, Dict[str, Any]]] = []
    for product in products:
        name = str(product.get("name") or "").lower()
        description = str(product.get("description") or "").lower()
        haystack = f"{name} {description}"
        if not all(term in haystack for term in terms):
            continue
        score = 0
        if name.startswith(terms[0]):
            score += 30
        score += sum(10 for term in terms if term in name)
        score += min(_safe_int(product.get("stock")), 20)
        matches.append((score, product))

    matches.sort(key=lambda item: (-item[0], _product_sort_key(item[1])))
    safe_limit = max(1, min(int(limit or 10), 20))
    return [product for _, product in matches[:safe_limit]]


async def get_low_stock_products(threshold: int = 5, limit: int = 10) -> List[Dict[str, Any]]:
    safe_threshold = max(0, int(threshold or 0))
    safe_limit = max(1, min(int(limit or 10), 50))
    products = await get_products()
    low_stock = [
        product for product in products
        if _safe_int(product.get("stock")) <= safe_threshold
    ]
    low_stock.sort(key=lambda product: (_safe_int(product.get("stock")), _product_sort_key(product)))
    return low_stock[:safe_limit]


async def get_delivery_outbox_stats() -> Dict[str, Any]:
    stats = {
        "available": False,
        "pending": 0,
        "sending": 0,
        "sent": 0,
        "failed": 0,
        "retry_due": 0,
    }

    def _fetch():
        return _get_table("bot_delivery_outbox").select("status, next_retry_at").limit(1000).execute()

    try:
        resp = await _to_thread(_fetch)
    except Exception as exc:
        if _is_missing_relation_error_message(str(exc)) and "bot_delivery_outbox" in str(exc):
            return stats
        return {**stats, "error": str(exc)[:160]}

    now_dt = datetime.now(timezone.utc)
    stats["available"] = True
    for row in resp.data or []:
        status = str(row.get("status") or "").strip().lower()
        if status in ("pending", "sending", "sent", "failed"):
            stats[status] = int(stats.get(status, 0)) + 1
        retry_at = _parse_created_at(row.get("next_retry_at"))
        if status == "pending" and retry_at:
            retry_at_utc = retry_at.astimezone(timezone.utc) if retry_at.tzinfo else retry_at.replace(tzinfo=timezone.utc)
            if retry_at_utc <= now_dt:
                stats["retry_due"] += 1
    return stats


async def get_admin_ops_health_snapshot(low_stock_threshold: int = 5) -> Dict[str, Any]:
    threshold = max(0, int(low_stock_threshold or 0))

    def _rpc():
        return get_supabase_client().rpc(
            "admin_ops_health_snapshot",
            {"p_low_stock_threshold": threshold},
        ).execute()

    try:
        resp = await _to_thread(_rpc)
        payload = resp.data
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass

    outbox = await get_delivery_outbox_stats()
    low_stock = await get_low_stock_products(threshold=threshold, limit=12)
    pending_deposits = await get_pending_deposits()
    pending_withdrawals = await get_pending_withdrawals()
    pending_usdt = await get_pending_usdt_withdrawals()
    pending_direct = await get_pending_direct_orders()
    pending_binance = await get_pending_binance_direct_orders()

    return {
        "checkedAt": _now_iso(),
        "queues": {
            "pendingDeposits": len(pending_deposits),
            "pendingWithdrawals": len(pending_withdrawals),
            "pendingUsdtWithdrawals": len(pending_usdt),
            "pendingDirectOrders": len(pending_direct) + len(pending_binance),
            "deliveryOutbox": outbox,
        },
        "stock": {
            "threshold": threshold,
            "count": len(low_stock),
            "items": [
                {
                    "id": product.get("id"),
                    "name": product.get("name"),
                    "availableStock": _safe_int(product.get("stock")),
                }
                for product in low_stock
            ],
        },
    }


async def get_bot_product_folders():
    def _fetch():
        return _get_table("bot_product_folders").select("id, name, sort_position").order("id").execute()

    try:
        resp = await _to_thread(_fetch)
    except Exception:
        return []

    folders = []
    for row in resp.data or []:
        folders.append({
            "id": row.get("id"),
            "name": row.get("name"),
            "sort_position": _safe_optional_int(row.get("sort_position")),
        })
    return _sort_folders_by_position(folders)


async def get_product(product_id: int):
    def _rpc():
        return get_supabase_client().rpc("get_product_with_stock", {"p_id": product_id}).execute()

    try:
        resp = await _to_thread(_rpc)
        data = resp.data or []
        if not data:
            return None
        row = data[0]
        return {
            "id": row.get("id"),
            "name": row.get("name"),
            "telegram_icon": row.get("telegram_icon") or "",
            "telegram_icon_custom_emoji_id": row.get("telegram_icon_custom_emoji_id") or "",
            "price": _safe_int(row.get("price")),
            "description": row.get("description"),
            "stock": _safe_int(row.get("stock")),
            "price_usdt": _safe_float(row.get("price_usdt")),
            "format_data": row.get("format_data"),
            "price_tiers": _safe_list(row.get("price_tiers")),
            "promo_buy_quantity": _safe_int(row.get("promo_buy_quantity")),
            "promo_bonus_quantity": _safe_int(row.get("promo_bonus_quantity")),
            "sort_position": _safe_optional_int(row.get("sort_position")),
            "bot_folder_id": _safe_optional_int(row.get("bot_folder_id")),
        }
    except Exception:
        def _fetch():
            try:
                return _get_table("products").select(
                    "id, name, telegram_icon, telegram_icon_custom_emoji_id, price, description, price_usdt, format_data, price_tiers, promo_buy_quantity, promo_bonus_quantity, sort_position, bot_folder_id"
                ).eq(
                    "id", product_id
                ).eq(
                    "is_deleted", False
                ).eq(
                    "is_hidden", False
                ).limit(1).execute()
            except Exception:
                try:
                    return _get_table("products").select(
                        "id, name, telegram_icon, price, description, price_usdt, format_data, price_tiers, promo_buy_quantity, promo_bonus_quantity, sort_position, bot_folder_id"
                    ).eq(
                        "id", product_id
                    ).eq(
                        "is_deleted", False
                    ).eq(
                        "is_hidden", False
                    ).limit(1).execute()
                except Exception:
                    return _get_table("products").select(
                        "id, name, price, description, price_usdt, format_data"
                    ).eq("id", product_id).limit(1).execute()

        resp = await _to_thread(_fetch)
        data = resp.data or []
        if not data:
            return None
        row = data[0]

        def _stock():
            return _get_table("stock").select("id").eq("product_id", product_id).eq("sold", False).execute()

        stock_resp = await _to_thread(_stock)
        stock_count = len(stock_resp.data or [])
        return {
            "id": row.get("id"),
            "name": row.get("name"),
            "telegram_icon": row.get("telegram_icon") or "",
            "telegram_icon_custom_emoji_id": row.get("telegram_icon_custom_emoji_id") or "",
            "price": _safe_int(row.get("price")),
            "description": row.get("description"),
            "stock": stock_count,
            "price_usdt": _safe_float(row.get("price_usdt")),
            "format_data": row.get("format_data"),
            "price_tiers": _safe_list(row.get("price_tiers")),
            "promo_buy_quantity": _safe_int(row.get("promo_buy_quantity")),
            "promo_bonus_quantity": _safe_int(row.get("promo_bonus_quantity")),
            "sort_position": _safe_optional_int(row.get("sort_position")),
            "bot_folder_id": _safe_optional_int(row.get("bot_folder_id")),
        }


async def get_active_sale_products() -> List[Dict[str, Any]]:
    def _rpc():
        return get_supabase_client().rpc("get_active_sale_products").execute()

    try:
        resp = await _to_thread(_rpc)
    except Exception as exc:
        if _is_missing_rpc_error_message(str(exc)):
            return []
        raise

    products = [
        _normalize_sale_product_row(row)
        for row in (resp.data or [])
        if isinstance(row, dict)
    ]
    products.sort(key=_sale_product_sort_key)
    return products


async def get_active_sale_product(sale_item_id: int) -> Optional[Dict[str, Any]]:
    safe_sale_item_id = int(sale_item_id or 0)
    if safe_sale_item_id <= 0:
        return None

    def _rpc():
        return get_supabase_client().rpc(
            "get_active_sale_product",
            {"p_sale_item_id": safe_sale_item_id},
        ).execute()

    try:
        resp = await _to_thread(_rpc)
        rows = resp.data or []
        row = rows[0] if isinstance(rows, list) and rows else rows
        if isinstance(row, dict) and row:
            return _normalize_sale_product_row(row)
    except Exception as exc:
        if not _is_missing_rpc_error_message(str(exc)):
            raise

    for product in await get_active_sale_products():
        if _safe_int(product.get("sale_item_id")) == safe_sale_item_id:
            return product
    return None


async def add_product(
    name: str,
    price: int,
    description: str = "",
    price_usdt: float = 0,
    format_data: str = "",
    price_tiers=None,
    promo_buy_quantity: int = 0,
    promo_bonus_quantity: int = 0,
    sort_position: Optional[int] = None,
    bot_folder_id: Optional[int] = None,
    telegram_icon: str = "",
    telegram_icon_custom_emoji_id: str = "",
):
    shifted_rows: list[tuple[int, int]] = []

    if sort_position is not None:
        def _load_rows_to_shift():
            return _get_table("products").select("id, sort_position").gte(
                "sort_position", sort_position
            ).order("sort_position", desc=True).order("id", desc=True).execute()

        try:
            shift_resp = await _to_thread(_load_rows_to_shift)
            shifted_rows = [
                (int(row.get("id")), int(row.get("sort_position")))
                for row in (shift_resp.data or [])
                if row.get("sort_position") is not None
            ]
            for product_id, current_position in shifted_rows:
                def _shift_one(pid=product_id, next_position=current_position + 1):
                    return _get_table("products").update({"sort_position": next_position}).eq("id", pid).execute()

                await _to_thread(_shift_one)
        except Exception:
            shifted_rows = []
            raise

    def _insert():
        payload = {
            "name": name,
            "telegram_icon": str(telegram_icon or "").strip()[:16] or None,
            "telegram_icon_custom_emoji_id": _safe_custom_emoji_id(telegram_icon_custom_emoji_id) or None,
            "price": price,
            "description": description,
            "price_usdt": price_usdt,
            "format_data": format_data,
            "price_tiers": price_tiers if price_tiers else None,
            "promo_buy_quantity": promo_buy_quantity,
            "promo_bonus_quantity": promo_bonus_quantity,
            "sort_position": sort_position,
            "bot_folder_id": bot_folder_id,
        }
        try:
            return _get_table("products").insert(payload).execute()
        except Exception:
            legacy_payload = {
                "name": name,
                "price": price,
                "description": description,
                "price_usdt": price_usdt,
                "format_data": format_data,
            }
            return _get_table("products").insert(legacy_payload).execute()

    try:
        resp = await _to_thread(_insert)
    except Exception:
        if shifted_rows:
            for product_id, original_position in shifted_rows:
                def _restore_one(pid=product_id, position=original_position):
                    return _get_table("products").update({"sort_position": position}).eq("id", pid).execute()

                await _to_thread(_restore_one)
        raise
    data = resp.data or []
    return data[0].get("id") if data else None


async def update_product_price_usdt(product_id: int, price_usdt: float):
    def _update():
        return _get_table("products").update({"price_usdt": price_usdt}).eq("id", product_id).execute()

    await _to_thread(_update)


async def delete_product(product_id: int):
    def _soft_delete():
        return _get_table("products").update({
            "is_hidden": True,
            "is_deleted": True,
            "deleted_at": _now_iso()
        }).eq("id", product_id).execute()

    await _to_thread(_soft_delete)


async def add_stock(product_id: int, content: str):
    def _insert():
        return _get_table("stock").insert({"product_id": product_id, "content": content}).execute()

    await _to_thread(_insert)


async def add_stock_bulk(product_id: int, contents: list):
    payload = [{"product_id": product_id, "content": content} for content in contents]

    def _insert():
        return _get_table("stock").insert(payload).execute()

    await _to_thread(_insert)


async def get_available_stock(product_id: int):
    def _fetch():
        return _get_table("stock").select("id, content").eq("product_id", product_id).eq(
            "sold", False
        ).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    return (row.get("id"), row.get("content"))


async def get_available_stock_batch(product_id: int, quantity: int):
    def _fetch():
        return _get_table("stock").select("id, content").eq("product_id", product_id).eq(
            "sold", False
        ).limit(quantity).execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [(row.get("id"), row.get("content")) for row in rows]


async def mark_stock_sold(stock_id: int):
    def _update():
        return _get_table("stock").update({"sold": True}).eq("id", stock_id).execute()

    await _to_thread(_update)


async def mark_stock_sold_batch(stock_ids: list):
    if not stock_ids:
        return

    def _update():
        return _get_table("stock").update({"sold": True}).in_("id", stock_ids).execute()

    await _to_thread(_update)


async def get_stock_by_product(product_id: int):
    def _fetch():
        return _get_table("stock").select("id, content, sold").eq("product_id", product_id).order(
            "sold", desc=False
        ).order("id", desc=True).execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [(row.get("id"), row.get("content"), row.get("sold")) for row in rows]


async def get_stock_detail(stock_id: int):
    def _fetch():
        return _get_table("stock").select("id, product_id, content, sold").eq(
            "id", stock_id
        ).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    return (row.get("id"), row.get("product_id"), row.get("content"), row.get("sold"))


async def update_stock_content(stock_id: int, new_content: str):
    def _update():
        return _get_table("stock").update({"content": new_content}).eq("id", stock_id).execute()

    await _to_thread(_update)


async def delete_stock(stock_id: int):
    def _delete():
        return _get_table("stock").delete().eq("id", stock_id).execute()

    await _to_thread(_delete)


async def delete_all_stock(product_id: int, only_unsold: bool = False):
    def _delete():
        query = _get_table("stock").delete().eq("product_id", product_id)
        if only_unsold:
            query = query.eq("sold", False)
        return query.execute()

    await _to_thread(_delete)


async def export_stock(product_id: int, only_unsold: bool = True):
    def _fetch():
        query = _get_table("stock").select("content").eq("product_id", product_id)
        if only_unsold:
            query = query.eq("sold", False)
        return query.order("id").execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [row.get("content") for row in rows]


# Order functions
async def create_order_bulk(
    user_id: int,
    product_id: int,
    contents: list,
    price_per_item: int,
    order_group: str,
    total_price: int = None,
    quantity: int = None,
):
    final_quantity = quantity if quantity is not None else len(contents)
    final_total = total_price if total_price is not None else price_per_item * len(contents)

    def _insert():
        return _get_table("orders").insert({
            "user_id": user_id,
            "product_id": product_id,
            "content": json.dumps(contents),
            "price": int(final_total),
            "quantity": int(final_quantity),
            "order_group": order_group,
            "created_at": _now_iso(),
        }).execute()

    await _to_thread(_insert)


async def create_order(user_id: int, product_id: int, content: str, price: int):
    def _insert():
        return _get_table("orders").insert({
            "user_id": user_id,
            "product_id": product_id,
            "content": content,
            "price": price,
            "quantity": 1,
            "created_at": _now_iso(),
        }).execute()

    await _to_thread(_insert)


async def fulfill_bot_balance_purchase(
    user_id: int,
    product_id: int,
    quantity: int,
    bonus_quantity: int,
    order_price_per_item: int,
    order_total_price: int,
    charge_balance: int = 0,
    charge_balance_usdt: float = 0.0,
    order_group: Optional[str] = None,
) -> Dict[str, Any]:
    def _rpc():
        return get_supabase_client().rpc(
            "fulfill_bot_balance_purchase",
            {
                "p_user_id": user_id,
                "p_product_id": product_id,
                "p_quantity": max(1, int(quantity or 0)),
                "p_bonus_quantity": max(0, int(bonus_quantity or 0)),
                "p_order_price_per_item": int(order_price_per_item or 0),
                "p_order_total_price": int(order_total_price or 0),
                "p_charge_balance": max(0, int(charge_balance or 0)),
                "p_charge_balance_usdt": max(0.0, float(charge_balance_usdt or 0.0)),
                "p_order_group": (order_group or "").strip() or None,
            },
        ).execute()

    try:
        resp = await _to_thread(_rpc)
        payload = _normalize_balance_purchase_payload(getattr(resp, "data", None))
        if payload:
            return payload
    except Exception as exc:
        message = str(exc)
        if not _is_missing_rpc_error_message(message):
            raise _map_fulfillment_error_from_message(message, 10) from exc

    required_stock = max(1, int(quantity) + max(0, int(bonus_quantity or 0)))
    stocks = await get_available_stock_batch(product_id, required_stock)
    if not stocks or len(stocks) < required_stock:
        raise DirectOrderFulfillmentError("not_enough_stock")

    if charge_balance:
        current_balance = await get_balance(user_id)
        if current_balance < int(charge_balance):
            raise DirectOrderFulfillmentError("insufficient_balance")
    else:
        current_balance = None

    if charge_balance_usdt:
        current_balance_usdt = await get_balance_usdt(user_id)
        if current_balance_usdt + 1e-9 < float(charge_balance_usdt):
            raise DirectOrderFulfillmentError("insufficient_usdt_balance")
    else:
        current_balance_usdt = None

    stock_ids = [stock[0] for stock in stocks]
    items = [stock[1] for stock in stocks]
    await mark_stock_sold_batch(stock_ids)

    next_order_group = (order_group or "").strip() or f"ORD{user_id}{datetime.now().strftime('%Y%m%d%H%M%S')}"
    await create_order_bulk(
        user_id,
        product_id,
        items,
        int(order_price_per_item),
        next_order_group,
        total_price=int(order_total_price),
        quantity=len(items),
    )

    new_balance = None
    if charge_balance:
        await update_balance(user_id, -int(charge_balance))
        new_balance = await get_balance(user_id)
    elif current_balance is not None:
        new_balance = current_balance

    new_balance_usdt = None
    if charge_balance_usdt:
        await update_balance_usdt(user_id, -float(charge_balance_usdt))
        new_balance_usdt = await get_balance_usdt(user_id)
    elif current_balance_usdt is not None:
        new_balance_usdt = current_balance_usdt

    product = await _get_direct_product_delivery_details(product_id)
    return {
        "user_id": user_id,
        "product_id": product_id,
        "product_name": str(product.get("name") or f"#{product_id}").strip(),
        "description": str(product.get("description") or ""),
        "format_data": str(product.get("format_data") or ""),
        "quantity": int(quantity),
        "bonus_quantity": int(bonus_quantity or 0),
        "delivered_quantity": len(items),
        "order_group": next_order_group,
        "items": items,
        "new_balance": new_balance,
        "new_balance_usdt": new_balance_usdt,
        "order_total_price": int(order_total_price),
        "charged_balance": int(charge_balance or 0),
        "charged_balance_usdt": float(charge_balance_usdt or 0.0),
    }


async def fulfill_bot_sale_balance_purchase(
    user_id: int,
    sale_item_id: int,
    quantity: int,
    charge_currency: str = "vnd",
    order_group: Optional[str] = None,
) -> Dict[str, Any]:
    def _rpc():
        return get_supabase_client().rpc(
            "fulfill_bot_sale_balance_purchase",
            {
                "p_user_id": user_id,
                "p_sale_item_id": int(sale_item_id),
                "p_quantity": max(1, int(quantity or 0)),
                "p_charge_currency": str(charge_currency or "vnd").strip().lower(),
                "p_order_group": (order_group or "").strip() or None,
            },
        ).execute()

    try:
        resp = await _to_thread(_rpc)
        payload = _normalize_balance_purchase_payload(getattr(resp, "data", None))
        if payload:
            return payload
    except Exception as exc:
        raise _map_fulfillment_error_from_message(str(exc), 10) from exc

    raise DirectOrderFulfillmentError("sale_item_not_active")


async def _get_product_names(product_ids: List[int]) -> Dict[int, str]:
    if not product_ids:
        return {}

    def _fetch():
        return _get_table("products").select("id, name").in_("id", list(set(product_ids))).execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return {row.get("id"): row.get("name") for row in rows}


async def get_user_orders(user_id: int):
    def _fetch():
        return _get_table("orders").select(
            "id, product_id, content, price, created_at, quantity, products(name)"
        ).eq("user_id", user_id).order("created_at", desc=True).limit(20).execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    results = []
    for row in rows:
        product = row.get("products") or {}
        results.append((
            row.get("id"),
            product.get("name"),
            row.get("content"),
            _safe_int(row.get("price")),
            row.get("created_at"),
            _safe_int(row.get("quantity"), 1),
        ))
    return results


async def get_order_detail(order_id: int):
    def _fetch():
        return _get_table("orders").select(
            "id, user_id, product_id, content, price, created_at, quantity, products(name, description, format_data)"
        ).eq("id", order_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    product = row.get("products") or {}
    return (
        row.get("id"),
        row.get("user_id"),
        row.get("product_id"),
        product.get("name"),
        row.get("content"),
        _safe_int(row.get("price")),
        row.get("created_at"),
        _safe_int(row.get("quantity"), 1),
        product.get("description"),
        product.get("format_data"),
    )


async def get_sold_codes_by_product(product_id: int, limit: int = 100):
    def _fetch():
        return _get_table("orders").select(
            "id, user_id, content, price, quantity, created_at"
        ).eq("product_id", product_id).order("created_at", desc=True).limit(limit).execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [
        (
            row.get("id"),
            row.get("user_id"),
            row.get("content"),
            _safe_int(row.get("price")),
            _safe_int(row.get("quantity"), 1),
            row.get("created_at"),
        )
        for row in rows
    ]


async def get_sold_codes_by_user(user_id: int, limit: int = 50):
    def _fetch():
        return _get_table("orders").select(
            "id, product_id, content, price, quantity, created_at, products(name)"
        ).eq("user_id", user_id).order("created_at", desc=True).limit(limit).execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [
        (
            row.get("id"),
            (row.get("products") or {}).get("name"),
            row.get("content"),
            _safe_int(row.get("price")),
            _safe_int(row.get("quantity"), 1),
            row.get("created_at"),
        )
        for row in rows
    ]


async def search_user_by_id(user_id: int):
    def _fetch():
        return _get_table("users").select("user_id, username, balance, created_at").eq(
            "user_id", user_id
        ).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    return (row.get("user_id"), row.get("username"), _safe_int(row.get("balance")), row.get("created_at"))


# Deposit functions
async def create_deposit_with_settings(user_id: int, amount: int, code: str):
    def _rpc():
        return get_supabase_client().rpc(
            "create_deposit_and_get_bank_settings",
            {"p_user_id": user_id, "p_amount": amount, "p_code": code},
        ).execute()

    try:
        resp = await _to_thread(_rpc)
        data = resp.data or []
        row = data[0] if isinstance(data, list) and data else data
        if row:
            return {
                "bank_name": row.get("bank_name") or "",
                "account_number": row.get("account_number") or "",
                "account_name": row.get("account_name") or "",
                "sepay_token": "",
            }
    except Exception:
        pass

    # Fallback to separate calls if RPC missing
    await create_deposit(user_id, amount, code)
    return await get_bank_settings()


async def create_deposit(user_id: int, amount: int, code: str):
    def _insert():
        return _get_table("deposits").insert({
            "user_id": user_id,
            "amount": amount,
            "code": code,
            "created_at": _now_iso(),
        }).execute()

    await _to_thread(_insert)


# Direct order functions
async def create_direct_order_with_settings(
    user_id: int,
    product_id: int,
    quantity: int,
    unit_price: int,
    amount: int,
    code: str,
    bonus_quantity: int = 0,
):
    def _rpc():
        return get_supabase_client().rpc(
            "create_direct_order_and_get_bank_settings",
            {
                "p_user_id": user_id,
                "p_product_id": product_id,
                "p_quantity": quantity,
                "p_bonus_quantity": bonus_quantity,
                "p_unit_price": unit_price,
                "p_amount": amount,
                "p_code": code,
            },
        ).execute()

    try:
        resp = await _to_thread(_rpc)
        data = resp.data or []
        row = data[0] if isinstance(data, list) and data else data
        if row:
            return {
                "bank_name": row.get("bank_name") or "",
                "account_number": row.get("account_number") or "",
                "account_name": row.get("account_name") or "",
                "sepay_token": "",
            }
    except Exception:
        pass

    await create_direct_order(
        user_id,
        product_id,
        quantity,
        unit_price,
        amount,
        code,
        bonus_quantity=bonus_quantity,
    )
    return await get_bank_settings()


async def create_sale_direct_order_with_settings(
    user_id: int,
    sale_item_id: int,
    quantity: int,
    code: str,
    hold_minutes: int = 10,
) -> Dict[str, Any]:
    def _rpc():
        return get_supabase_client().rpc(
            "create_sale_direct_order_and_get_bank_settings",
            {
                "p_user_id": user_id,
                "p_sale_item_id": int(sale_item_id),
                "p_quantity": max(1, int(quantity or 0)),
                "p_code": code,
                "p_hold_minutes": max(1, int(hold_minutes or 10)),
            },
        ).execute()

    try:
        resp = await _to_thread(_rpc)
        row = _normalize_rpc_payload(getattr(resp, "data", None))
        if row:
            return {
                "direct_order_id": _safe_int(row.get("direct_order_id")),
                "product_id": _safe_int(row.get("product_id")),
                "product_name": row.get("product_name") or "",
                "bank_name": row.get("bank_name") or "",
                "account_number": row.get("account_number") or "",
                "account_name": row.get("account_name") or "",
                "sepay_token": "",
                "quantity": _safe_int(row.get("quantity"), 1),
                "bonus_quantity": _safe_int(row.get("bonus_quantity"), 0),
                "unit_price": _safe_int(row.get("unit_price")),
                "amount": _safe_int(row.get("amount")),
                "code": row.get("code") or code,
                "sale_campaign_id": _safe_int(row.get("sale_campaign_id")),
                "sale_item_id": _safe_int(row.get("sale_item_id")),
                "held_until": row.get("held_until"),
            }
    except Exception as exc:
        raise _map_fulfillment_error_from_message(str(exc), hold_minutes) from exc

    raise DirectOrderFulfillmentError("sale_direct_order_failed")


async def create_direct_order(
    user_id: int,
    product_id: int,
    quantity: int,
    unit_price: int,
    amount: int,
    code: str,
    bonus_quantity: int = 0,
):
    def _insert():
        payload = {
            "user_id": user_id,
            "product_id": product_id,
            "quantity": quantity,
            "bonus_quantity": bonus_quantity,
            "unit_price": unit_price,
            "amount": amount,
            "code": code,
            "created_at": _now_iso(),
        }
        try:
            return _get_table("direct_orders").insert(payload).execute()
        except Exception:
            legacy_payload = {
                "user_id": user_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "amount": amount,
                "code": code,
                "created_at": _now_iso(),
            }
            return _get_table("direct_orders").insert(legacy_payload).execute()

    await _to_thread(_insert)


async def get_pending_direct_orders():
    def _fetch():
        try:
            return _get_table("direct_orders").select(
                "id, user_id, product_id, quantity, bonus_quantity, unit_price, amount, code, created_at, payment_channel"
            ).eq("status", "pending").neq("payment_channel", "binance_onchain").execute()
        except Exception:
            return _get_table("direct_orders").select(
                "id, user_id, product_id, quantity, unit_price, amount, code, created_at"
            ).eq("status", "pending").execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [
        (
            row.get("id"),
            row.get("user_id"),
            row.get("product_id"),
            _safe_int(row.get("quantity"), 1),
            _safe_int(row.get("bonus_quantity"), 0),
            _safe_int(row.get("unit_price")),
            _safe_int(row.get("amount")),
            row.get("code"),
            row.get("created_at"),
        )
        for row in rows
    ]

async def get_user_direct_order_by_code(user_id: int, code: str):
    def _fetch():
        try:
            return _get_table("direct_orders").select(
                "id, user_id, product_id, quantity, bonus_quantity, unit_price, amount, code, status, created_at, payment_channel, external_payment_id, external_paid_at"
            ).eq("user_id", user_id).eq("code", code).order("id", desc=True).limit(1).execute()
        except Exception:
            return _get_table("direct_orders").select(
                "id, user_id, product_id, quantity, unit_price, amount, code, status, created_at"
            ).eq("user_id", user_id).eq("code", code).order("id", desc=True).limit(1).execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    if not rows:
        return None
    row = rows[0]
    return {
        "id": _safe_int(row.get("id")),
        "user_id": _safe_int(row.get("user_id")),
        "product_id": _safe_int(row.get("product_id")),
        "quantity": _safe_int(row.get("quantity"), 1),
        "bonus_quantity": _safe_int(row.get("bonus_quantity"), 0),
        "unit_price": _safe_int(row.get("unit_price")),
        "amount": _safe_int(row.get("amount")),
        "code": row.get("code"),
        "status": str(row.get("status") or "pending"),
        "created_at": row.get("created_at"),
        "payment_channel": row.get("payment_channel") or "vietqr",
        "external_payment_id": row.get("external_payment_id"),
        "external_paid_at": row.get("external_paid_at"),
    }


async def set_direct_order_status(order_id: int, status: str):
    def _update():
        return _get_table("direct_orders").update({"status": status}).eq("id", order_id).execute()

    await _to_thread(_update)


def _normalize_bot_delivery_outbox_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "id": _safe_int(row.get("id")),
        "direct_order_id": _safe_int(row.get("direct_order_id")),
        "user_id": _safe_int(row.get("user_id")),
        "channel": str(row.get("channel") or "telegram_bot"),
        "payload": _safe_json_object(row.get("payload")),
        "status": str(row.get("status") or "pending"),
        "attempt_count": _safe_int(row.get("attempt_count")),
        "next_retry_at": row.get("next_retry_at"),
        "last_error": row.get("last_error"),
        "sent_at": row.get("sent_at"),
        "last_attempt_at": row.get("last_attempt_at"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


async def get_bot_delivery_outbox(direct_order_id: int) -> Optional[Dict[str, Any]]:
    def _fetch():
        return _get_table("bot_delivery_outbox").select("*").eq("direct_order_id", direct_order_id).limit(1).execute()

    try:
        resp = await _to_thread(_fetch)
    except Exception as exc:
        if _is_missing_relation_error_message(str(exc)) and "bot_delivery_outbox" in str(exc):
            return None
        raise

    rows = resp.data or []
    return _normalize_bot_delivery_outbox_row(dict(rows[0])) if rows else None


async def ensure_bot_delivery_outbox(
    direct_order_id: int,
    user_id: int,
    payload: Dict[str, Any],
    reset_status: bool = False,
) -> Optional[Dict[str, Any]]:
    existing = await get_bot_delivery_outbox(direct_order_id)
    now_iso = _now_iso()

    if existing:
        def _update():
            update_payload: Dict[str, Any] = {
                "user_id": user_id,
                "payload": payload,
                "channel": "telegram_bot",
            }
            if reset_status:
                update_payload.update(
                    {
                        "status": "pending",
                        "last_error": None,
                        "next_retry_at": now_iso,
                        "sent_at": None,
                        "last_attempt_at": None,
                    }
                )
            return _get_table("bot_delivery_outbox").update(update_payload).eq("id", existing["id"]).execute()

        try:
            await _to_thread(_update)
        except Exception as exc:
            if _is_missing_relation_error_message(str(exc)) and "bot_delivery_outbox" in str(exc):
                return None
            raise
        return await get_bot_delivery_outbox(direct_order_id)

    def _insert():
        return _get_table("bot_delivery_outbox").insert(
            {
                "direct_order_id": direct_order_id,
                "user_id": user_id,
                "channel": "telegram_bot",
                "payload": payload,
                "status": "pending",
                "attempt_count": 0,
                "next_retry_at": now_iso,
                "last_error": None,
                "sent_at": None,
                "last_attempt_at": None,
            }
        ).execute()

    try:
        await _to_thread(_insert)
    except Exception as exc:
        if _is_missing_relation_error_message(str(exc)) and "bot_delivery_outbox" in str(exc):
            return None
        raise
    return await get_bot_delivery_outbox(direct_order_id)


async def get_due_bot_delivery_outbox(limit: int = 20) -> List[Dict[str, Any]]:
    def _fetch():
        return _get_table("bot_delivery_outbox").select("*").eq("status", "pending").lte("next_retry_at", _now_iso()).order("next_retry_at").limit(max(1, int(limit or 20))).execute()

    try:
        resp = await _to_thread(_fetch)
    except Exception as exc:
        if _is_missing_relation_error_message(str(exc)) and "bot_delivery_outbox" in str(exc):
            return []
        raise

    rows = resp.data or []
    return [normalized for normalized in (_normalize_bot_delivery_outbox_row(dict(row)) for row in rows) if normalized]


async def mark_bot_delivery_outbox_sending(outbox_id: int, attempt_count: int):
    def _update():
        return _get_table("bot_delivery_outbox").update(
            {
                "status": "sending",
                "attempt_count": int(attempt_count),
                "last_attempt_at": _now_iso(),
            }
        ).eq("id", outbox_id).execute()

    await _to_thread(_update)


async def mark_bot_delivery_outbox_sent(outbox_id: int, attempt_count: int):
    def _update():
        return _get_table("bot_delivery_outbox").update(
            {
                "status": "sent",
                "attempt_count": int(attempt_count),
                "last_error": None,
                "next_retry_at": None,
                "sent_at": _now_iso(),
            }
        ).eq("id", outbox_id).execute()

    await _to_thread(_update)


async def schedule_bot_delivery_outbox_retry(
    outbox_id: int,
    attempt_count: int,
    last_error: str,
    next_retry_at: Optional[str],
):
    def _update():
        return _get_table("bot_delivery_outbox").update(
            {
                "status": "pending",
                "attempt_count": int(attempt_count),
                "last_error": str(last_error or "")[:2000],
                "next_retry_at": next_retry_at,
            }
        ).eq("id", outbox_id).execute()

    await _to_thread(_update)


async def mark_bot_delivery_outbox_failed(outbox_id: int, attempt_count: int, last_error: str):
    def _update():
        return _get_table("bot_delivery_outbox").update(
            {
                "status": "failed",
                "attempt_count": int(attempt_count),
                "last_error": str(last_error or "")[:2000],
                "next_retry_at": None,
            }
        ).eq("id", outbox_id).execute()

    await _to_thread(_update)


async def get_recent_confirmed_direct_orders_missing_delivery(limit: int = 50, hours: int = 48) -> List[Dict[str, Any]]:
    cutoff_iso = (datetime.now() - timedelta(hours=max(1, int(hours or 48)))).isoformat()

    def _fetch_orders():
        return _get_table("direct_orders").select(
            "id, user_id, product_id, quantity, bonus_quantity, amount, code, created_at, status"
        ).eq("status", "confirmed").gte("created_at", cutoff_iso).order("created_at", desc=True).limit(max(1, int(limit or 50))).execute()

    resp = await _to_thread(_fetch_orders)
    rows = [dict(row) for row in (resp.data or [])]
    if not rows:
        return []

    outbox_by_order_id: Dict[int, Dict[str, Any]] = {}
    direct_order_ids = [row.get("id") for row in rows if row.get("id") is not None]
    if direct_order_ids:
        def _fetch_outbox():
            return _get_table("bot_delivery_outbox").select("direct_order_id").in_("direct_order_id", direct_order_ids).execute()

        try:
            outbox_resp = await _to_thread(_fetch_outbox)
            outbox_by_order_id = {
                _safe_int(item.get("direct_order_id")): dict(item) for item in (outbox_resp.data or [])
            }
        except Exception as exc:
            if _is_missing_relation_error_message(str(exc)) and "bot_delivery_outbox" in str(exc):
                return rows
            raise

    return [row for row in rows if _safe_int(row.get("id")) not in outbox_by_order_id]


async def build_bot_delivery_payload_for_direct_order(direct_order_id: int) -> Optional[Dict[str, Any]]:
    def _fetch_direct_order():
        return _get_table("direct_orders").select(
            "id, user_id, product_id, quantity, bonus_quantity, amount, code, created_at, status"
        ).eq("id", direct_order_id).limit(1).execute()

    direct_resp = await _to_thread(_fetch_direct_order)
    direct_rows = direct_resp.data or []
    if not direct_rows:
        return None

    direct_order = dict(direct_rows[0])
    if str(direct_order.get("status") or "") != "confirmed":
        return None

    product = await _get_direct_product_delivery_details(_safe_int(direct_order.get("product_id")))
    expected_delivered_quantity = max(1, _safe_int(direct_order.get("quantity"), 1)) + max(0, _safe_int(direct_order.get("bonus_quantity"), 0))

    def _fetch_orders():
        return _get_table("orders").select(
            "id, content, price, quantity, order_group, created_at"
        ).eq("user_id", direct_order.get("user_id")).eq("product_id", direct_order.get("product_id")).gte("created_at", direct_order.get("created_at")).order("created_at").limit(20).execute()

    order_resp = await _to_thread(_fetch_orders)
    order_rows = [dict(row) for row in (order_resp.data or [])]
    if not order_rows:
        return None

    selected_row: Optional[Dict[str, Any]] = None
    selected_items: List[str] = []
    for row in order_rows:
        items = _safe_str_list(row.get("content"))
        delivered_quantity = max(len(items), _safe_int(row.get("quantity")))
        if delivered_quantity == expected_delivered_quantity and _safe_int(row.get("price")) == _safe_int(direct_order.get("amount")):
            selected_row = row
            selected_items = items
            break
    if selected_row is None:
        for row in order_rows:
            items = _safe_str_list(row.get("content"))
            if len(items) == expected_delivered_quantity:
                selected_row = row
                selected_items = items
                break
    if selected_row is None:
        for row in order_rows:
            items = _safe_str_list(row.get("content"))
            if items:
                selected_row = row
                selected_items = items
                break
    if selected_row is None or not selected_items:
        return None

    product_id = _safe_int(direct_order.get("product_id"))
    return {
        "directOrderId": _safe_int(direct_order.get("id")),
        "orderId": _safe_optional_int(selected_row.get("id")),
        "userId": _safe_int(direct_order.get("user_id")),
        "productId": product_id,
        "productName": str(product.get("name") or f"#{product_id}"),
        "description": str(product.get("description") or ""),
        "formatData": str(product.get("format_data") or ""),
        "quantity": max(1, _safe_int(direct_order.get("quantity"), 1)),
        "bonusQuantity": max(0, _safe_int(direct_order.get("bonus_quantity"), 0)),
        "deliveredQuantity": max(1, len(selected_items) or expected_delivered_quantity),
        "amount": max(0, _safe_int(direct_order.get("amount"))),
        "code": str(direct_order.get("code") or ""),
        "orderGroup": str(selected_row.get("order_group") or ""),
        "items": [str(item or "") for item in selected_items],
    }


async def get_pending_website_direct_orders():
    def _fetch():
        return _get_table("website_direct_orders").select(
            "id, auth_user_id, user_email, product_id, quantity, bonus_quantity, unit_price, amount, code, created_at"
        ).eq("status", "pending").execute()

    try:
        resp = await _to_thread(_fetch)
    except Exception:
        return []

    rows = resp.data or []
    return [
        (
            row.get("id"),
            row.get("auth_user_id"),
            row.get("user_email"),
            row.get("product_id"),
            _safe_int(row.get("quantity"), 1),
            _safe_int(row.get("bonus_quantity"), 0),
            _safe_int(row.get("unit_price")),
            _safe_int(row.get("amount")),
            row.get("code"),
            row.get("created_at"),
        )
        for row in rows
    ]


async def create_website_order_bulk(
    auth_user_id: Optional[str],
    user_email: Optional[str],
    product_id: int,
    contents: list,
    price_per_item: int,
    order_group: str,
    total_price: int = None,
    quantity: int = None,
    source_direct_code: Optional[str] = None,
) -> Optional[int]:
    final_quantity = quantity if quantity is not None else len(contents)
    final_total = total_price if total_price is not None else price_per_item * len(contents)

    def _insert():
        payload = {
            "auth_user_id": auth_user_id,
            "user_email": user_email,
            "product_id": product_id,
            "content": json.dumps(contents),
            "price": int(final_total),
            "quantity": int(final_quantity),
            "order_group": order_group,
            "source_direct_code": source_direct_code,
            "created_at": _now_iso(),
        }
        try:
            return _get_table("website_orders").insert(payload).execute()
        except Exception:
            legacy_payload = {
                "auth_user_id": auth_user_id,
                "user_email": user_email,
                "product_id": product_id,
                "content": json.dumps(contents),
                "price": int(final_total),
                "quantity": int(final_quantity),
                "order_group": order_group,
                "created_at": _now_iso(),
            }
            return _get_table("website_orders").insert(legacy_payload).execute()

    resp = await _to_thread(_insert)
    rows = resp.data or []
    if isinstance(rows, list) and rows:
        return _safe_int(rows[0].get("id"), 0) or None
    if isinstance(rows, dict):
        return _safe_int(rows.get("id"), 0) or None
    return None


async def set_website_direct_order_status(
    order_id: int,
    status: str,
    fulfilled_order_id: Optional[int] = None,
):
    def _update():
        payload: Dict[str, Any] = {
            "status": status,
            "updated_at": _now_iso(),
        }
        if status == "confirmed":
            payload["confirmed_at"] = _now_iso()
        if fulfilled_order_id is not None:
            payload["fulfilled_order_id"] = fulfilled_order_id
        return _get_table("website_direct_orders").update(payload).eq("id", order_id).execute()

    try:
        await _to_thread(_update)
    except Exception:
        # Website-specific tables may not exist yet on some environments.
        return


async def _get_direct_product_delivery_details(product_id: int) -> Dict[str, Any]:
    def _fetch():
        return _get_table("products").select(
            "id, name, website_name, description, format_data"
        ).eq("id", product_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return dict(rows[0]) if rows else {}


async def fulfill_bot_direct_order(
    order_id: int,
    order_group: Optional[str] = None,
    expire_minutes: int = 10,
) -> Dict[str, Any]:
    def _rpc():
        return get_supabase_client().rpc(
            "fulfill_bot_direct_order",
            {
                "p_direct_order_id": order_id,
                "p_order_group": (order_group or "").strip() or None,
                "p_expire_minutes": max(1, int(expire_minutes or 10)),
            },
        ).execute()

    try:
        resp = await _to_thread(_rpc)
        payload = _normalize_rpc_payload(getattr(resp, "data", None))
        if payload:
            payload["items"] = _safe_str_list(payload.get("items"))
            return payload
    except Exception as exc:
        message = str(exc)
        if not _is_missing_rpc_error_message(message):
            raise _map_fulfillment_error_from_message(message, expire_minutes) from exc

    def _fetch_direct_order():
        return _get_table("direct_orders").select(
            "id, user_id, product_id, quantity, bonus_quantity, unit_price, amount, code, status, created_at"
        ).eq("id", order_id).limit(1).execute()

    resp = await _to_thread(_fetch_direct_order)
    rows = resp.data or []
    if not rows:
        raise DirectOrderFulfillmentError("direct_order_not_found")
    row = rows[0]
    if str(row.get("status") or "") != "pending":
        raise DirectOrderFulfillmentError("direct_order_not_pending")
    if _is_direct_order_expired(row.get("created_at"), expire_minutes):
        await set_direct_order_status(order_id, "cancelled")
        raise DirectOrderFulfillmentError(
            "direct_order_expired",
            f"expired_after_{max(1, expire_minutes)}m",
        )

    product_id = _safe_int(row.get("product_id"))
    quantity = _safe_int(row.get("quantity"), 1)
    bonus_quantity = _safe_int(row.get("bonus_quantity"), 0)
    deliver_quantity = max(1, quantity + max(0, bonus_quantity))
    stocks = await get_available_stock_batch(product_id, deliver_quantity)
    if not stocks or len(stocks) < deliver_quantity:
        await set_direct_order_status(order_id, "failed")
        raise DirectOrderFulfillmentError("not_enough_stock")

    stock_ids = [stock[0] for stock in stocks]
    items = [stock[1] for stock in stocks]
    await mark_stock_sold_batch(stock_ids)

    payload_order_group = (order_group or "").strip() or f"PAY{row.get('user_id')}{datetime.now().strftime('%Y%m%d%H%M%S')}"
    total_price = _safe_int(row.get("amount")) or (_safe_int(row.get("unit_price")) * max(1, quantity))
    await create_order_bulk(
        _safe_int(row.get("user_id")),
        product_id,
        items,
        _safe_int(row.get("unit_price")),
        payload_order_group,
        total_price=total_price,
        quantity=len(items),
    )
    await set_direct_order_status(order_id, "confirmed")

    product = await _get_direct_product_delivery_details(product_id)
    return {
        "direct_order_id": _safe_int(row.get("id")),
        "order_id": None,
        "user_id": _safe_int(row.get("user_id")),
        "product_id": product_id,
        "product_name": str(product.get("name") or f"#{product_id}").strip(),
        "description": str(product.get("description") or ""),
        "format_data": str(product.get("format_data") or ""),
        "quantity": quantity,
        "bonus_quantity": bonus_quantity,
        "delivered_quantity": len(items),
        "unit_price": _safe_int(row.get("unit_price")),
        "amount": total_price,
        "code": str(row.get("code") or ""),
        "order_group": payload_order_group,
        "items": items,
    }


async def fulfill_website_direct_order(
    website_direct_order_id: int,
    order_group: Optional[str] = None,
    expire_minutes: int = 10,
) -> Dict[str, Any]:
    def _rpc():
        return get_supabase_client().rpc(
            "fulfill_website_direct_order",
            {
                "p_website_direct_order_id": website_direct_order_id,
                "p_order_group": (order_group or "").strip() or None,
                "p_expire_minutes": max(1, int(expire_minutes or 10)),
            },
        ).execute()

    try:
        resp = await _to_thread(_rpc)
        payload = _normalize_rpc_payload(getattr(resp, "data", None))
        if payload:
            payload["items"] = _safe_str_list(payload.get("items"))
            return payload
    except Exception as exc:
        message = str(exc)
        if not _is_missing_rpc_error_message(message):
            raise _map_fulfillment_error_from_message(message, expire_minutes) from exc

    def _fetch_website_direct_order():
        return _get_table("website_direct_orders").select(
            "id, auth_user_id, user_email, product_id, quantity, bonus_quantity, unit_price, amount, code, status, created_at"
        ).eq("id", website_direct_order_id).limit(1).execute()

    resp = await _to_thread(_fetch_website_direct_order)
    rows = resp.data or []
    if not rows:
        raise DirectOrderFulfillmentError("website_direct_order_not_found")
    row = rows[0]
    if str(row.get("status") or "") != "pending":
        raise DirectOrderFulfillmentError("website_direct_order_not_pending")

    mirror_code = str(row.get("code") or "").strip()

    def _fetch_mirror_direct_order():
        return _get_table("direct_orders").select(
            "id, status"
        ).eq("code", mirror_code).order("id", desc=True).limit(1).execute()

    mirror_resp = await _to_thread(_fetch_mirror_direct_order)
    mirror_rows = mirror_resp.data or []
    if not mirror_rows:
        raise DirectOrderFulfillmentError("mirror_direct_order_not_found")
    mirror_row = mirror_rows[0]
    mirror_id = _safe_int(mirror_row.get("id"))
    if str(mirror_row.get("status") or "") != "pending":
        raise DirectOrderFulfillmentError("mirror_direct_order_not_pending")

    if _is_direct_order_expired(row.get("created_at"), expire_minutes):
        await set_website_direct_order_status(website_direct_order_id, "cancelled")
        await set_direct_order_status(mirror_id, "cancelled")
        raise DirectOrderFulfillmentError(
            "website_direct_order_expired",
            f"expired_after_{max(1, expire_minutes)}m",
        )

    product_id = _safe_int(row.get("product_id"))
    quantity = _safe_int(row.get("quantity"), 1)
    bonus_quantity = _safe_int(row.get("bonus_quantity"), 0)
    deliver_quantity = max(1, quantity + max(0, bonus_quantity))
    stocks = await get_available_stock_batch(product_id, deliver_quantity)
    if not stocks or len(stocks) < deliver_quantity:
        await set_website_direct_order_status(website_direct_order_id, "failed")
        await set_direct_order_status(mirror_id, "failed")
        raise DirectOrderFulfillmentError("not_enough_stock")

    stock_ids = [stock[0] for stock in stocks]
    items = [stock[1] for stock in stocks]
    await mark_stock_sold_batch(stock_ids)

    payload_order_group = (order_group or "").strip() or f"WEB{datetime.now().strftime('%Y%m%d%H%M%S')}"
    total_price = _safe_int(row.get("amount")) or (_safe_int(row.get("unit_price")) * max(1, quantity))
    created_order_id = await create_website_order_bulk(
        row.get("auth_user_id"),
        row.get("user_email"),
        product_id,
        items,
        _safe_int(row.get("unit_price")),
        payload_order_group,
        total_price=total_price,
        quantity=len(items),
        source_direct_code=mirror_code,
    )
    await set_direct_order_status(mirror_id, "confirmed")
    await set_website_direct_order_status(
        website_direct_order_id,
        "confirmed",
        fulfilled_order_id=created_order_id,
    )

    product = await _get_direct_product_delivery_details(product_id)
    return {
        "website_direct_order_id": _safe_int(row.get("id")),
        "direct_order_id": mirror_id,
        "website_order_id": created_order_id,
        "auth_user_id": str(row.get("auth_user_id") or ""),
        "user_email": str(row.get("user_email") or ""),
        "product_id": product_id,
        "product_name": str(product.get("website_name") or product.get("name") or f"#{product_id}").strip(),
        "quantity": quantity,
        "bonus_quantity": bonus_quantity,
        "delivered_quantity": len(items),
        "unit_price": _safe_int(row.get("unit_price")),
        "amount": total_price,
        "code": mirror_code,
        "order_group": payload_order_group,
        "items": items,
    }


async def get_pending_deposits():
    def _fetch():
        return _get_table("deposits").select("id, user_id, amount, code, created_at").eq(
            "status", "pending"
        ).execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [
        (
            row.get("id"),
            row.get("user_id"),
            _safe_int(row.get("amount")),
            row.get("code"),
            row.get("created_at"),
        )
        for row in rows
    ]


async def confirm_deposit(deposit_id: int):
    def _fetch():
        return _get_table("deposits").select("user_id, amount").eq("id", deposit_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    user_id = row.get("user_id")
    amount = _safe_int(row.get("amount"))

    def _update_deposit():
        return _get_table("deposits").update({"status": "confirmed"}).eq("id", deposit_id).execute()

    await _to_thread(_update_deposit)
    await update_balance(user_id, amount)
    return (user_id, amount)


async def cancel_deposit(deposit_id: int):
    def _update():
        return _get_table("deposits").update({"status": "cancelled"}).eq("id", deposit_id).execute()

    await _to_thread(_update)


async def set_deposit_status(deposit_id: int, status: str):
    def _update():
        return _get_table("deposits").update({"status": status}).eq("id", deposit_id).execute()

    await _to_thread(_update)


# Stats
async def get_stats():
    def _rpc():
        return get_supabase_client().rpc("get_stats").execute()

    try:
        resp = await _to_thread(_rpc)
        data = resp.data or []
        row = data[0] if isinstance(data, list) and data else data
        if row:
            return {
                "users": _safe_int(row.get("users")),
                "orders": _safe_int(row.get("orders")),
                "revenue": _safe_int(row.get("revenue")),
            }
    except Exception:
        pass

    # Fallback to manual counts
    def _count_users():
        return _get_table("users").select("user_id").execute()

    def _count_orders():
        return _get_table("orders").select("id, price").execute()

    users_resp = await _to_thread(_count_users)
    orders_resp = await _to_thread(_count_orders)
    users = len(users_resp.data or [])
    orders = len(orders_resp.data or [])
    revenue = sum(_safe_int(row.get("price")) for row in (orders_resp.data or []))
    return {"users": users, "orders": orders, "revenue": revenue}


async def get_all_user_ids():
    def _fetch():
        return _get_table("users").select("user_id").execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [row.get("user_id") for row in rows if row.get("user_id") is not None]


# Withdrawal functions
async def create_withdrawal(user_id: int, amount: int, momo_phone: str):
    def _insert():
        return _get_table("withdrawals").insert({
            "user_id": user_id,
            "amount": amount,
            "momo_phone": momo_phone,
            "created_at": _now_iso(),
        }).execute()

    await _to_thread(_insert)


async def get_pending_withdrawals():
    def _fetch():
        return _get_table("withdrawals").select(
            "id, user_id, amount, momo_phone, created_at"
        ).eq("status", "pending").execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [
        (
            row.get("id"),
            row.get("user_id"),
            _safe_int(row.get("amount")),
            row.get("momo_phone"),
            row.get("created_at"),
        )
        for row in rows
    ]


async def get_withdrawal_detail(withdrawal_id: int):
    def _fetch():
        return _get_table("withdrawals").select(
            "id, user_id, amount, momo_phone, status, created_at"
        ).eq("id", withdrawal_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    return (
        row.get("id"),
        row.get("user_id"),
        _safe_int(row.get("amount")),
        row.get("momo_phone"),
        row.get("status"),
        row.get("created_at"),
    )


async def get_user_pending_withdrawal(user_id: int):
    def _fetch():
        return _get_table("withdrawals").select("amount").eq("user_id", user_id).eq(
            "status", "pending"
        ).execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return sum(_safe_int(row.get("amount")) for row in rows)


async def confirm_withdrawal(withdrawal_id: int):
    def _fetch():
        return _get_table("withdrawals").select("user_id, amount, momo_phone").eq(
            "id", withdrawal_id
        ).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    user_id = row.get("user_id")
    amount = _safe_int(row.get("amount"))
    momo_phone = row.get("momo_phone")

    balance = await get_balance(user_id)
    if balance < amount:
        return None

    await _set_balance(user_id, balance - amount)

    def _update():
        return _get_table("withdrawals").update({"status": "confirmed"}).eq("id", withdrawal_id).execute()

    await _to_thread(_update)
    return (user_id, amount, momo_phone)


async def cancel_withdrawal(withdrawal_id: int):
    def _fetch():
        return _get_table("withdrawals").select("user_id, amount").eq("id", withdrawal_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]

    def _update():
        return _get_table("withdrawals").update({"status": "cancelled"}).eq("id", withdrawal_id).execute()

    await _to_thread(_update)
    return (row.get("user_id"), _safe_int(row.get("amount")))


# Settings functions
async def get_setting(key: str, default: str = ""):
    now = time.time()
    cached = _settings_cache["values"].get(key)
    if cached is not None and (now - _settings_cache["ts"] <= _SETTINGS_TTL_SECONDS):
        return cached

    def _fetch():
        return _get_table("settings").select("value").eq("key", key).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    value = data[0].get("value") if data else default
    _settings_cache["values"][key] = value
    _settings_cache["ts"] = now
    return value


async def set_setting(key: str, value: str):
    def _upsert():
        return _get_table("settings").upsert({"key": key, "value": value}).execute()

    await _to_thread(_upsert)
    _settings_cache["values"][key] = value
    _settings_cache["ts"] = time.time()


async def get_ui_flags() -> Dict[str, bool]:
    return {
        "show_shop": _parse_bool(await get_setting("show_shop", "true")),
        "show_balance": _parse_bool(await get_setting("show_balance", "true")),
        "show_deposit": _parse_bool(await get_setting("show_deposit", "true")),
        "show_withdraw": _parse_bool(await get_setting("show_withdraw", "true")),
        "show_usdt": _parse_bool(await get_setting("show_usdt", "true")),
        "show_history": _parse_bool(await get_setting("show_history", "true")),
        "show_language": _parse_bool(await get_setting("show_language", "true")),
        "show_support": _parse_bool(await get_setting("show_support", "true")),
    }


async def get_bank_settings():
    return {
        "bank_name": await get_setting("bank_name", ""),
        "account_number": await get_setting("account_number", ""),
        "account_name": await get_setting("account_name", ""),
        "sepay_token": await get_setting("sepay_token", ""),
    }


# Binance on-chain direct-order helpers
async def create_binance_direct_order(
    user_id: int,
    product_id: int,
    quantity: int,
    unit_price: int,
    amount: int,
    code: str,
    *,
    bonus_quantity: int = 0,
    payment_asset: str,
    payment_network: str,
    payment_amount_asset: str,
    payment_rate_vnd: str,
    payment_address: str,
    payment_address_tag: str = "",
):
    def _rpc():
        return get_supabase_client().rpc(
            "create_binance_direct_order",
            {
                "p_user_id": user_id,
                "p_product_id": product_id,
                "p_quantity": quantity,
                "p_bonus_quantity": bonus_quantity,
                "p_unit_price": unit_price,
                "p_amount": amount,
                "p_code": code,
                "p_payment_asset": payment_asset,
                "p_payment_network": payment_network,
                "p_payment_amount_asset": payment_amount_asset,
                "p_payment_rate_vnd": payment_rate_vnd,
                "p_payment_address": payment_address,
                "p_payment_address_tag": payment_address_tag,
            },
        ).execute()

    try:
        resp = await _to_thread(_rpc)
        data = resp.data or []
        row = data[0] if isinstance(data, list) and data else data
        if row:
            return {
                "direct_order_id": _safe_int(row.get("direct_order_id")),
                "code": row.get("code") or code,
                "payment_asset": row.get("payment_asset") or payment_asset,
                "payment_network": row.get("payment_network") or payment_network,
                "payment_amount_asset": row.get("payment_amount_asset") or payment_amount_asset,
                "payment_address": row.get("payment_address") or payment_address,
                "payment_address_tag": row.get("payment_address_tag") or payment_address_tag,
                "created_at": row.get("created_at") or _now_iso(),
            }
    except Exception as exc:
        if not _is_missing_rpc_error_message(str(exc or "")):
            raise _map_binance_order_error_from_message(str(exc))

    def _insert():
        return _get_table("direct_orders").insert(
            {
                "user_id": user_id,
                "product_id": product_id,
                "quantity": quantity,
                "bonus_quantity": bonus_quantity,
                "unit_price": unit_price,
                "amount": amount,
                "code": code,
                "payment_channel": "binance_onchain",
                "payment_asset": payment_asset,
                "payment_network": payment_network,
                "payment_amount_asset": payment_amount_asset,
                "payment_rate_vnd": payment_rate_vnd,
                "payment_address": payment_address,
                "payment_address_tag": payment_address_tag or None,
                "created_at": _now_iso(),
            }
        ).execute()

    try:
        resp = await _to_thread(_insert)
    except Exception as exc:
        raise _map_binance_order_error_from_message(str(exc))

    rows = resp.data or []
    row = rows[0] if rows else {}
    return {
        "direct_order_id": _safe_int(row.get("id")),
        "code": row.get("code") or code,
        "payment_asset": row.get("payment_asset") or payment_asset,
        "payment_network": row.get("payment_network") or payment_network,
        "payment_amount_asset": row.get("payment_amount_asset") or payment_amount_asset,
        "payment_address": row.get("payment_address") or payment_address,
        "payment_address_tag": row.get("payment_address_tag") or payment_address_tag,
        "created_at": row.get("created_at") or _now_iso(),
    }


async def create_binance_sale_direct_order(
    user_id: int,
    sale_item_id: int,
    quantity: int,
    code: str,
    *,
    payment_asset: str,
    payment_network: str,
    payment_amount_asset: str,
    payment_rate_vnd: str,
    payment_address: str,
    payment_address_tag: str = "",
    hold_minutes: int = 10,
) -> Dict[str, Any]:
    def _rpc():
        return get_supabase_client().rpc(
            "create_binance_sale_direct_order",
            {
                "p_user_id": user_id,
                "p_sale_item_id": int(sale_item_id),
                "p_quantity": max(1, int(quantity or 0)),
                "p_code": code,
                "p_payment_asset": payment_asset,
                "p_payment_network": payment_network,
                "p_payment_amount_asset": payment_amount_asset,
                "p_payment_rate_vnd": payment_rate_vnd,
                "p_payment_address": payment_address,
                "p_payment_address_tag": payment_address_tag,
                "p_hold_minutes": max(1, int(hold_minutes or 10)),
            },
        ).execute()

    try:
        resp = await _to_thread(_rpc)
        row = _normalize_rpc_payload(getattr(resp, "data", None))
        if row:
            return {
                "direct_order_id": _safe_int(row.get("direct_order_id")),
                "code": row.get("code") or code,
                "payment_asset": row.get("payment_asset") or payment_asset,
                "payment_network": row.get("payment_network") or payment_network,
                "payment_amount_asset": row.get("payment_amount_asset") or payment_amount_asset,
                "payment_address": row.get("payment_address") or payment_address,
                "payment_address_tag": row.get("payment_address_tag") or payment_address_tag,
                "created_at": row.get("created_at") or _now_iso(),
                "product_id": _safe_int(row.get("product_id")),
                "product_name": row.get("product_name") or "",
                "quantity": _safe_int(row.get("quantity"), 1),
                "bonus_quantity": _safe_int(row.get("bonus_quantity"), 0),
                "unit_price": _safe_int(row.get("unit_price")),
                "amount": _safe_int(row.get("amount")),
                "sale_campaign_id": _safe_int(row.get("sale_campaign_id")),
                "sale_item_id": _safe_int(row.get("sale_item_id")),
                "held_until": row.get("held_until"),
            }
    except Exception as exc:
        message = str(exc)
        if "sale_" in message or "not_enough_stock" in message or "insufficient" in message:
            raise _map_fulfillment_error_from_message(message, hold_minutes) from exc
        raise _map_binance_order_error_from_message(message) from exc

    raise BinanceDirectOrderError("binance_direct_order_failed")


async def get_pending_binance_direct_orders():
    def _fetch():
        return _get_table("direct_orders").select(
            "id, user_id, product_id, quantity, bonus_quantity, unit_price, amount, code, created_at, payment_asset, payment_network, payment_amount_asset, payment_rate_vnd, payment_address, payment_address_tag, external_payment_id, external_tx_id, external_paid_at"
        ).eq("status", "pending").eq("payment_channel", "binance_onchain").order("created_at").execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [
        {
            "id": row.get("id"),
            "user_id": row.get("user_id"),
            "product_id": row.get("product_id"),
            "quantity": _safe_int(row.get("quantity"), 1),
            "bonus_quantity": _safe_int(row.get("bonus_quantity"), 0),
            "unit_price": _safe_int(row.get("unit_price")),
            "amount": _safe_int(row.get("amount")),
            "code": row.get("code") or "",
            "created_at": row.get("created_at"),
            "payment_asset": row.get("payment_asset") or "",
            "payment_network": row.get("payment_network") or "",
            "payment_amount_asset": row.get("payment_amount_asset"),
            "payment_rate_vnd": row.get("payment_rate_vnd"),
            "payment_address": row.get("payment_address") or "",
            "payment_address_tag": row.get("payment_address_tag") or "",
            "external_payment_id": row.get("external_payment_id") or "",
            "external_tx_id": row.get("external_tx_id") or "",
            "external_paid_at": row.get("external_paid_at"),
        }
        for row in rows
    ]


async def record_direct_order_external_payment(
    order_id: int,
    *,
    payment_id: str,
    tx_id: str,
    paid_at: str | None,
):
    def _update():
        return _get_table("direct_orders").update(
            {
                "external_payment_id": payment_id,
                "external_tx_id": tx_id,
                "external_paid_at": paid_at,
            }
        ).eq("id", order_id).execute()

    await _to_thread(_update)


async def is_processed_binance_deposit(payment_id: str):
    def _fetch():
        return _get_table("binance_processed_deposits").select("payment_id").eq(
            "payment_id", str(payment_id or "").strip()
        ).limit(1).execute()

    resp = await _to_thread(_fetch)
    return bool(resp.data)


async def mark_processed_binance_deposit(
    payment_id: str,
    *,
    tx_id: str,
    direct_order_id: int | None,
    amount_asset: str,
    payment_asset: str,
    payment_network: str,
):
    def _upsert():
        return _get_table("binance_processed_deposits").upsert(
            {
                "payment_id": str(payment_id or "").strip(),
                "tx_id": str(tx_id or "").strip(),
                "direct_order_id": direct_order_id,
                "amount_asset": amount_asset,
                "payment_asset": payment_asset,
                "payment_network": payment_network,
                "processed_at": _now_iso(),
            }
        ).execute()

    await _to_thread(_upsert)


# USDT Withdrawal functions
async def create_usdt_withdrawal(user_id: int, usdt_amount: float, wallet_address: str, network: str = "TRC20"):
    def _insert():
        return _get_table("usdt_withdrawals").insert({
            "user_id": user_id,
            "usdt_amount": usdt_amount,
            "wallet_address": wallet_address,
            "network": network,
            "created_at": _now_iso(),
        }).execute()

    await _to_thread(_insert)


async def get_pending_usdt_withdrawals():
    def _fetch():
        return _get_table("usdt_withdrawals").select(
            "id, user_id, usdt_amount, wallet_address, network, created_at"
        ).eq("status", "pending").execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [
        (
            row.get("id"),
            row.get("user_id"),
            _safe_float(row.get("usdt_amount")),
            row.get("wallet_address"),
            row.get("network"),
            row.get("created_at"),
        )
        for row in rows
    ]


async def get_usdt_withdrawal_detail(withdrawal_id: int):
    def _fetch():
        return _get_table("usdt_withdrawals").select(
            "id, user_id, usdt_amount, wallet_address, network, status, created_at"
        ).eq("id", withdrawal_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    return (
        row.get("id"),
        row.get("user_id"),
        _safe_float(row.get("usdt_amount")),
        row.get("wallet_address"),
        row.get("network"),
        row.get("status"),
        row.get("created_at"),
    )


async def get_user_pending_usdt_withdrawal(user_id: int):
    def _fetch():
        return _get_table("usdt_withdrawals").select("usdt_amount").eq("user_id", user_id).eq(
            "status", "pending"
        ).execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return sum(_safe_float(row.get("usdt_amount")) for row in rows)


async def confirm_usdt_withdrawal(withdrawal_id: int):
    def _fetch():
        return _get_table("usdt_withdrawals").select(
            "user_id, usdt_amount, wallet_address"
        ).eq("id", withdrawal_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    user_id = row.get("user_id")
    usdt_amount = _safe_float(row.get("usdt_amount"))
    wallet_address = row.get("wallet_address")

    balance = await get_balance_usdt(user_id)
    if balance < usdt_amount:
        return None

    await _set_balance_usdt(user_id, balance - usdt_amount)

    def _update():
        return _get_table("usdt_withdrawals").update({"status": "confirmed"}).eq("id", withdrawal_id).execute()

    await _to_thread(_update)
    return (user_id, usdt_amount, wallet_address)


async def cancel_usdt_withdrawal(withdrawal_id: int):
    def _fetch():
        return _get_table("usdt_withdrawals").select("user_id, usdt_amount").eq(
            "id", withdrawal_id
        ).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]

    def _update():
        return _get_table("usdt_withdrawals").update({"status": "cancelled"}).eq("id", withdrawal_id).execute()

    await _to_thread(_update)
    return (row.get("user_id"), _safe_float(row.get("usdt_amount")))


# SePay processed transactions
async def is_processed_transaction(tx_id: str) -> bool:
    def _fetch():
        return _get_table("processed_transactions").select("tx_id").eq("tx_id", tx_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    return bool(data)


async def mark_processed_transaction(tx_id: str):
    def _insert():
        return _get_table("processed_transactions").insert({"tx_id": tx_id}).execute()

    await _to_thread(_insert)
