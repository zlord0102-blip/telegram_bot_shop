import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .supabase_client import get_supabase_client


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


class DirectOrderFulfillmentError(RuntimeError):
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
    return payload


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
                "price": _safe_int(row.get("price")),
                "description": row.get("description"),
                "stock": _safe_int(row.get("stock")),
                "price_usdt": _safe_float(row.get("price_usdt")),
                "format_data": row.get("format_data"),
                "price_tiers": _safe_list(row.get("price_tiers")),
                "promo_buy_quantity": _safe_int(row.get("promo_buy_quantity")),
                "promo_bonus_quantity": _safe_int(row.get("promo_bonus_quantity")),
                "sort_position": position_map.get(str(product_id), _safe_optional_int(row.get("sort_position"))),
            })
        return _sort_products_by_position(products)
    except Exception:
        # Fallback to per-product counting if RPC not available
        def _fetch():
            try:
                return _get_table("products").select(
                    "id, name, price, description, price_usdt, format_data, price_tiers, promo_buy_quantity, promo_bonus_quantity, sort_position"
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
                "price": _safe_int(row.get("price")),
                "description": row.get("description"),
                "stock": stock_count,
                "price_usdt": _safe_float(row.get("price_usdt")),
                "format_data": row.get("format_data"),
                "price_tiers": _safe_list(row.get("price_tiers")),
                "promo_buy_quantity": _safe_int(row.get("promo_buy_quantity")),
                "promo_bonus_quantity": _safe_int(row.get("promo_bonus_quantity")),
                "sort_position": _safe_optional_int(row.get("sort_position")),
            })
        return _sort_products_by_position(products)


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
            "price": _safe_int(row.get("price")),
            "description": row.get("description"),
            "stock": _safe_int(row.get("stock")),
            "price_usdt": _safe_float(row.get("price_usdt")),
            "format_data": row.get("format_data"),
            "price_tiers": _safe_list(row.get("price_tiers")),
            "promo_buy_quantity": _safe_int(row.get("promo_buy_quantity")),
            "promo_bonus_quantity": _safe_int(row.get("promo_bonus_quantity")),
            "sort_position": _safe_optional_int(row.get("sort_position")),
        }
    except Exception:
        def _fetch():
            try:
                return _get_table("products").select(
                    "id, name, price, description, price_usdt, format_data, price_tiers, promo_buy_quantity, promo_bonus_quantity, sort_position"
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
            "price": _safe_int(row.get("price")),
            "description": row.get("description"),
            "stock": stock_count,
            "price_usdt": _safe_float(row.get("price_usdt")),
            "format_data": row.get("format_data"),
            "price_tiers": _safe_list(row.get("price_tiers")),
            "promo_buy_quantity": _safe_int(row.get("promo_buy_quantity")),
            "promo_bonus_quantity": _safe_int(row.get("promo_bonus_quantity")),
            "sort_position": _safe_optional_int(row.get("sort_position")),
        }


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
            "price": price,
            "description": description,
            "price_usdt": price_usdt,
            "format_data": format_data,
            "price_tiers": price_tiers if price_tiers else None,
            "promo_buy_quantity": promo_buy_quantity,
            "promo_bonus_quantity": promo_bonus_quantity,
            "sort_position": sort_position,
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
            "id, product_id, content, price, created_at, quantity, products(name, description, format_data)"
        ).eq("id", order_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    product = row.get("products") or {}
    return (
        row.get("id"),
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
                "id, user_id, product_id, quantity, bonus_quantity, unit_price, amount, code, created_at"
            ).eq("status", "pending").execute()
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


async def set_direct_order_status(order_id: int, status: str):
    def _update():
        return _get_table("direct_orders").update({"status": status}).eq("id", order_id).execute()

    await _to_thread(_update)


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


# Binance deposit functions
async def create_binance_deposit(user_id: int, usdt_amount: float, vnd_amount: int, code: str):
    def _insert():
        return _get_table("binance_deposits").insert({
            "user_id": user_id,
            "usdt_amount": usdt_amount,
            "vnd_amount": vnd_amount,
            "code": code,
            "created_at": _now_iso(),
        }).execute()

    await _to_thread(_insert)


async def update_binance_deposit_screenshot(user_id: int, code: str, file_id: str):
    def _update():
        return _get_table("binance_deposits").update(
            {"screenshot_file_id": file_id}
        ).eq("user_id", user_id).eq("code", code).eq("status", "pending").execute()

    await _to_thread(_update)


async def get_pending_binance_deposits():
    def _fetch():
        return _get_table("binance_deposits").select(
            "id, user_id, usdt_amount, vnd_amount, code, screenshot_file_id, created_at"
        ).eq("status", "pending").not_.is_("screenshot_file_id", "null").execute()

    resp = await _to_thread(_fetch)
    rows = resp.data or []
    return [
        (
            row.get("id"),
            row.get("user_id"),
            _safe_float(row.get("usdt_amount")),
            _safe_int(row.get("vnd_amount")),
            row.get("code"),
            row.get("screenshot_file_id"),
            row.get("created_at"),
        )
        for row in rows
    ]


async def get_binance_deposit_detail(deposit_id: int):
    def _fetch():
        return _get_table("binance_deposits").select(
            "id, user_id, usdt_amount, vnd_amount, code, screenshot_file_id, status, created_at"
        ).eq("id", deposit_id).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    return (
        row.get("id"),
        row.get("user_id"),
        _safe_float(row.get("usdt_amount")),
        _safe_int(row.get("vnd_amount")),
        row.get("code"),
        row.get("screenshot_file_id"),
        row.get("status"),
        row.get("created_at"),
    )


async def confirm_binance_deposit(deposit_id: int):
    def _fetch():
        return _get_table("binance_deposits").select("user_id, usdt_amount").eq(
            "id", deposit_id
        ).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    user_id = row.get("user_id")
    usdt_amount = _safe_float(row.get("usdt_amount"))

    def _update_deposit():
        return _get_table("binance_deposits").update({"status": "confirmed"}).eq(
            "id", deposit_id
        ).execute()

    await _to_thread(_update_deposit)
    await update_balance_usdt(user_id, usdt_amount)
    return (user_id, usdt_amount)


async def cancel_binance_deposit(deposit_id: int):
    def _update():
        return _get_table("binance_deposits").update({"status": "cancelled"}).eq("id", deposit_id).execute()

    await _to_thread(_update)


async def get_user_pending_binance_deposit(user_id: int):
    def _fetch():
        return _get_table("binance_deposits").select(
            "id, usdt_amount, vnd_amount, code"
        ).eq("user_id", user_id).eq("status", "pending").order("id", desc=True).limit(1).execute()

    resp = await _to_thread(_fetch)
    data = resp.data or []
    if not data:
        return None
    row = data[0]
    return (
        row.get("id"),
        _safe_float(row.get("usdt_amount")),
        _safe_int(row.get("vnd_amount")),
        row.get("code"),
    )


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
