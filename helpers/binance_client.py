import hashlib
import hmac
import logging
import os
import time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import aiohttp

from config import (
    BINANCE_API_KEY,
    BINANCE_API_SECRET,
    BINANCE_DIRECT_COIN,
    BINANCE_DIRECT_ENABLED,
    BINANCE_DIRECT_NETWORK,
    BINANCE_DIRECT_RATE,
)

logger = logging.getLogger(__name__)

_BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com").strip().rstrip("/")
_ADDRESS_CACHE_TTL_SECONDS = 120
_CAPITAL_CONFIG_CACHE_TTL_SECONDS = 120
_AMOUNT_QUANT = Decimal("0.000001")
_BASE_AMOUNT_QUANT = Decimal("0.001")
_ZERO = Decimal("0")
_capital_config_cache: dict[str, Any] = {"value": None, "ts": 0.0}
_address_cache: dict[str, dict[str, Any]] = {}
_NETWORK_ALIASES = {
    "TRC20": "TRX",
    "TRON": "TRX",
    "BEP20": "BSC",
    "ERC20": "ETH",
}
_NETWORK_LABELS = {
    "TRX": "TRX (TRC20)",
    "BSC": "BSC (BEP20)",
    "ETH": "ETH (ERC20)",
}


class BinanceConfigError(RuntimeError):
    pass


class BinanceApiError(RuntimeError):
    pass


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    lowered = str(value).strip().lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    return default


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_amount_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value.quantize(_AMOUNT_QUANT, rounding=ROUND_HALF_UP)
    text = _normalize_text(value)
    if not text:
        return _ZERO
    try:
        return Decimal(text).quantize(_AMOUNT_QUANT, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return _ZERO


def normalize_binance_network(value: Any) -> str:
    normalized = _normalize_text(value).upper()
    if not normalized:
        return ""
    return _NETWORK_ALIASES.get(normalized, normalized)


def format_binance_network_label(value: Any) -> str:
    normalized = normalize_binance_network(value)
    return _NETWORK_LABELS.get(normalized, normalized)


def format_binance_amount(value: Any) -> str:
    amount = _normalize_amount_decimal(value)
    return format(amount, "f")


def compute_binance_exact_amount(total_vnd: int, rate_vnd: Decimal, suffix: int) -> Decimal:
    if rate_vnd <= 0:
        raise BinanceConfigError("binance_direct_rate_invalid")
    safe_total = max(0, int(total_vnd or 0))
    safe_suffix = max(1, min(999, int(suffix or 1)))
    base_amount = (Decimal(safe_total) / rate_vnd).quantize(_BASE_AMOUNT_QUANT, rounding=ROUND_HALF_UP)
    return (base_amount + (Decimal(safe_suffix) / Decimal("1000000"))).quantize(
        _AMOUNT_QUANT,
        rounding=ROUND_HALF_UP,
    )


def compute_binance_exact_amount_from_asset(total_asset: Any, suffix: int) -> Decimal:
    base_amount = _normalize_amount_decimal(total_asset)
    if base_amount <= 0:
        raise BinanceConfigError("binance_direct_asset_amount_invalid")
    safe_suffix = max(1, min(999, int(suffix or 1)))
    return (base_amount + (Decimal(safe_suffix) / Decimal("1000000"))).quantize(
        _AMOUNT_QUANT,
        rounding=ROUND_HALF_UP,
    )


async def get_binance_direct_settings() -> Dict[str, Any]:
    from database import get_setting

    api_key = _normalize_text(await get_setting("binance_api_key", BINANCE_API_KEY))
    api_secret = _normalize_text(await get_setting("binance_api_secret", BINANCE_API_SECRET))
    enabled = _parse_bool(await get_setting("binance_direct_enabled", "true" if BINANCE_DIRECT_ENABLED else "false"))
    coin = _normalize_text(await get_setting("binance_direct_coin", BINANCE_DIRECT_COIN or "USDT")).upper() or "USDT"
    network = normalize_binance_network(await get_setting("binance_direct_network", BINANCE_DIRECT_NETWORK))
    raw_rate = _normalize_text(await get_setting("binance_direct_rate", BINANCE_DIRECT_RATE))

    try:
        rate_vnd = Decimal(raw_rate)
    except (InvalidOperation, ValueError):
        rate_vnd = _ZERO

    return {
        "enabled": enabled,
        "api_key": api_key,
        "api_secret": api_secret,
        "coin": coin,
        "network": network,
        "network_label": format_binance_network_label(network),
        "rate_vnd": rate_vnd,
        "valid": enabled and bool(api_key and api_secret and coin and network and rate_vnd > 0),
    }


class BinanceWalletClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str = _BINANCE_BASE_URL):
        self.api_key = _normalize_text(api_key)
        self.api_secret = _normalize_text(api_secret)
        self.base_url = _normalize_text(base_url) or _BINANCE_BASE_URL
        if not self.api_key or not self.api_secret:
            raise BinanceConfigError("binance_credentials_missing")

    async def _signed_get(self, path: str, params: Dict[str, Any]) -> Any:
        query_params = {
            key: value
            for key, value in params.items()
            if value is not None and _normalize_text(value) != ""
        }
        query_params["timestamp"] = int(time.time() * 1000)
        query_params.setdefault("recvWindow", 5000)
        query_string = urlencode(query_params, doseq=True)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        url = f"{self.base_url}{path}?{query_string}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}
        timeout = aiohttp.ClientTimeout(total=15)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 429:
                    retry_after = response.headers.get("Retry-After")
                    raise BinanceApiError(f"binance_rate_limited:{retry_after or ''}")
                if response.status >= 400:
                    body = await response.text()
                    raise BinanceApiError(f"binance_http_{response.status}:{body[:300]}")
                data = await response.json()
                if isinstance(data, dict) and data.get("code") not in (None, 0):
                    raise BinanceApiError(str(data.get("msg") or data.get("code")))
                return data

    async def get_capital_config(self, force_refresh: bool = False) -> list[dict[str, Any]]:
        now = time.time()
        if (
            not force_refresh
            and _capital_config_cache["value"] is not None
            and now - float(_capital_config_cache["ts"] or 0) <= _CAPITAL_CONFIG_CACHE_TTL_SECONDS
        ):
            return list(_capital_config_cache["value"])

        data = await self._signed_get("/sapi/v1/capital/config/getall", {})
        if not isinstance(data, list):
            raise BinanceApiError("binance_capital_config_invalid")

        _capital_config_cache["value"] = list(data)
        _capital_config_cache["ts"] = now
        return list(data)

    async def get_network_config(self, coin: str, network: str) -> dict[str, Any]:
        safe_coin = _normalize_text(coin).upper()
        safe_network = normalize_binance_network(network)
        config = await self.get_capital_config()
        for coin_row in config:
            if _normalize_text(coin_row.get("coin")).upper() != safe_coin:
                continue
            for network_row in coin_row.get("networkList") or []:
                if _normalize_text(network_row.get("network")).upper() == safe_network:
                    return dict(network_row)
        raise BinanceConfigError("binance_network_not_found")

    async def get_deposit_address(self, coin: str, network: str, force_refresh: bool = False) -> dict[str, str]:
        safe_coin = _normalize_text(coin).upper()
        safe_network = normalize_binance_network(network)
        cache_key = f"{safe_coin}:{safe_network}"
        now = time.time()
        cached = _address_cache.get(cache_key)
        if cached and not force_refresh and now - float(cached.get("ts") or 0) <= _ADDRESS_CACHE_TTL_SECONDS:
            return {
                "address": _normalize_text(cached.get("address")),
                "tag": _normalize_text(cached.get("tag")),
                "coin": safe_coin,
                "network": safe_network,
            }

        data = await self._signed_get(
            "/sapi/v1/capital/deposit/address",
            {"coin": safe_coin, "network": safe_network},
        )
        if not isinstance(data, dict):
            raise BinanceApiError("binance_deposit_address_invalid")

        address = _normalize_text(data.get("address"))
        if not address:
            raise BinanceConfigError("binance_address_missing")

        payload = {
            "address": address,
            "tag": _normalize_text(data.get("tag")),
            "coin": safe_coin,
            "network": safe_network,
        }
        _address_cache[cache_key] = {"ts": now, **payload}
        return payload

    async def get_deposit_history(
        self,
        *,
        coin: str,
        status: int = 1,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        params: Dict[str, Any] = {
            "coin": _normalize_text(coin).upper(),
            "status": int(status),
            "limit": max(1, min(1000, int(limit or 1000))),
        }
        if start_time:
            params["startTime"] = int(start_time)
        if end_time:
            params["endTime"] = int(end_time)
        data = await self._signed_get("/sapi/v1/capital/deposit/hisrec", params)
        if not isinstance(data, list):
            raise BinanceApiError("binance_deposit_history_invalid")
        return [dict(item) for item in data]


async def get_binance_direct_runtime(force_refresh: bool = False) -> Dict[str, Any]:
    settings = await get_binance_direct_settings()
    if not settings["enabled"]:
        return {**settings, "available": False, "reason": "disabled"}
    if not settings["valid"]:
        return {**settings, "available": False, "reason": "invalid_settings"}

    client = BinanceWalletClient(
        api_key=str(settings["api_key"]),
        api_secret=str(settings["api_secret"]),
    )
    network_config = await client.get_network_config(settings["coin"], settings["network"])
    if not _parse_bool(network_config.get("depositEnable"), default=False):
        raise BinanceConfigError("binance_network_deposit_disabled")

    address_info = await client.get_deposit_address(
        settings["coin"],
        settings["network"],
        force_refresh=force_refresh,
    )
    return {
        **settings,
        "available": True,
        "client": client,
        "network_config": network_config,
        "address": _normalize_text(address_info.get("address")),
        "address_tag": _normalize_text(address_info.get("tag")),
        "network_label": format_binance_network_label(settings["network"]),
        "requires_tag": bool(_normalize_text(network_config.get("memoRegex")) or _parse_bool(network_config.get("withdrawTag"), default=False)),
    }
