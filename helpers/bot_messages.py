import asyncio
from dataclasses import dataclass
import re
from typing import Any, Mapping

from telegram import MessageEntity

from database import get_bot_message_template
from helpers.telegram_resilience import edit_or_reply_callback_message
from helpers.telegram_ui import clean_single_line, fit_button_text, normalize_custom_emoji_id


CUSTOM_EMOJI_PLACEHOLDER = "✨"
INLINE_CUSTOM_EMOJI_RE = re.compile(r"\{(?:emoji|custom_emoji):([0-9]{5,64})\}")
INLINE_CUSTOM_EMOJI_TOKEN = "\uE000custom_emoji_{}\uE001"
SUPPORTED_BOT_LANGUAGES = ("vi", "en")


BOT_BUTTON_LABEL_DEFAULTS: dict[str, dict[str, str]] = {
    "reply.shop": {"vi": "🛒 Mua hàng", "en": "🛒 Shop"},
    "reply.balance": {"vi": "💰 Số dư", "en": "💰 Balance"},
    "reply.deposit": {"vi": "➕ Nạp tiền", "en": "➕ Deposit"},
    "reply.withdraw": {"vi": "💸 Rút tiền", "en": "💸 Withdraw"},
    "reply.history": {"vi": "📜 Lịch sử mua", "en": "📜 History"},
    "reply.support": {"vi": "💬 Hỗ trợ", "en": "💬 Support"},
    "reply.language": {"vi": "🌐 Ngôn ngữ", "en": "🌐 Language"},
    "reply.cancel": {"vi": "❌ Hủy", "en": "❌ Cancel"},
    "button.delete": {"vi": "🗑 Xóa", "en": "🗑 Delete"},
    "button.back": {"vi": "🔙 Quay lại", "en": "🔙 Back"},
    "button.back_shop": {"vi": "🔙 Shop", "en": "🔙 Shop"},
    "button.back_product": {"vi": "🔙 Quay lại sản phẩm", "en": "🔙 Back to product"},
    "button.refresh": {"vi": "🔄 Cập nhật", "en": "🔄 Refresh"},
    "button.prev": {"vi": "⬅️ Trước", "en": "⬅️ Prev"},
    "button.next": {"vi": "Sau ➡️", "en": "Next ➡️"},
    "button.check_status": {"vi": "🔄 Kiểm tra trạng thái", "en": "🔄 Check status"},
    "button.history": {"vi": "📜 Lịch sử", "en": "📜 History"},
    "button.support": {"vi": "💬 Hỗ trợ", "en": "💬 Support"},
    "button.account": {"vi": "👤 Tài khoản", "en": "👤 Account"},
    "button.open_shop": {"vi": "🛒 Mở danh mục", "en": "🛒 Open shop"},
    "button.main_shop": {"vi": "🛒 Mua hàng", "en": "🛒 Shop"},
    "button.main_deposit": {"vi": "💰 Nạp tiền", "en": "💰 Deposit"},
    "button.rebuy": {"vi": "🛒 Mua lại", "en": "🛒 Buy again"},
    "button.quick_quantity": {"vi": "⚡ Chọn nhanh", "en": "⚡ Quick pick"},
    "button.manual_quantity": {"vi": "✍️ Nhập tay", "en": "✍️ Enter manually"},
    "button.pay_vnd": {"vi": "💰 Ví VNĐ", "en": "💰 VND wallet"},
    "button.pay_usdt": {"vi": "💵 Ví USDT", "en": "💵 USDT wallet"},
    "button.vietqr": {"vi": "💳 VietQR", "en": "💳 VietQR"},
    "button.binance": {"vi": "🟡 Binance", "en": "🟡 Binance"},
}

_BOT_BUTTON_LABEL_CACHE: dict[tuple[str, str], str] = {}


class _SafeFormatDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _safe_custom_emoji_id(value: Any) -> str:
    text = str(value or "").strip()
    return "".join(char for char in text if char.isdigit())[:64]


def _normalize_lang(lang: str) -> str:
    clean_lang = str(lang or "vi").strip().lower()
    return clean_lang if clean_lang in SUPPORTED_BOT_LANGUAGES else "vi"


def _fallback_for_lang(fallbacks: Mapping[str, str], lang: str) -> str:
    clean_lang = _normalize_lang(lang)
    return str(fallbacks.get(clean_lang) or fallbacks.get("vi") or "")


def get_cached_bot_button_label(template_key: str, lang: str, fallback: str) -> str:
    cache_key = (_normalize_lang(lang), str(template_key or "").strip())
    cached = _BOT_BUTTON_LABEL_CACHE.get(cache_key)
    return cached if cached else clean_single_line(fallback, fallback)


def get_cached_common_button_label(template_key: str, lang: str) -> str:
    fallbacks = BOT_BUTTON_LABEL_DEFAULTS.get(template_key, {})
    return get_cached_bot_button_label(template_key, lang, _fallback_for_lang(fallbacks, lang))


def _utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _protect_inline_custom_emoji(text: str) -> tuple[str, dict[str, str]]:
    replacements: dict[str, str] = {}

    def replace(match: re.Match[str]) -> str:
        token = INLINE_CUSTOM_EMOJI_TOKEN.format(len(replacements))
        replacements[token] = match.group(0)
        return token

    return INLINE_CUSTOM_EMOJI_RE.sub(replace, text), replacements


def _restore_inline_custom_emoji(text: str, replacements: Mapping[str, str]) -> str:
    for token, placeholder in replacements.items():
        text = text.replace(token, placeholder)
    return text


def _extract_inline_custom_emoji_entities(text: str) -> tuple[str, list[MessageEntity]]:
    entities: list[MessageEntity] = []
    output_parts: list[str] = []
    output_offset = 0
    last_index = 0

    for match in INLINE_CUSTOM_EMOJI_RE.finditer(text):
        custom_emoji_id = _safe_custom_emoji_id(match.group(1))
        if not custom_emoji_id:
            continue

        before = text[last_index:match.start()]
        output_parts.append(before)
        output_offset += _utf16_len(before)

        output_parts.append(CUSTOM_EMOJI_PLACEHOLDER)
        entities.append(
            MessageEntity(
                type=MessageEntity.CUSTOM_EMOJI,
                offset=output_offset,
                length=_utf16_len(CUSTOM_EMOJI_PLACEHOLDER),
                custom_emoji_id=custom_emoji_id,
            )
        )
        output_offset += _utf16_len(CUSTOM_EMOJI_PLACEHOLDER)
        last_index = match.end()

    tail = text[last_index:]
    output_parts.append(tail)
    return "".join(output_parts), entities


def _render_variables(text: str, variables: Mapping[str, Any] | None) -> str:
    protected_text, inline_emoji_replacements = _protect_inline_custom_emoji(text)
    if not variables:
        return _restore_inline_custom_emoji(protected_text, inline_emoji_replacements)
    safe_variables = _SafeFormatDict({key: str(value) for key, value in variables.items()})
    try:
        rendered = protected_text.format_map(safe_variables)
    except Exception:
        return text
    return _restore_inline_custom_emoji(rendered, inline_emoji_replacements)


@dataclass(frozen=True)
class RenderedBotMessage:
    text: str
    custom_emoji_id: str = ""
    fallback_emoji: str = ""
    from_template: bool = False

    def to_telegram_kwargs(self) -> dict[str, Any]:
        custom_emoji_id = _safe_custom_emoji_id(self.custom_emoji_id)
        text = self.text.strip()
        prefix_entities: list[MessageEntity] = []
        if custom_emoji_id:
            text = f"{CUSTOM_EMOJI_PLACEHOLDER} {text}" if text else CUSTOM_EMOJI_PLACEHOLDER
            prefix_entities.append(
                MessageEntity(
                    type=MessageEntity.CUSTOM_EMOJI,
                    offset=0,
                    length=_utf16_len(CUSTOM_EMOJI_PLACEHOLDER),
                    custom_emoji_id=custom_emoji_id,
                )
            )
        else:
            fallback_emoji = str(self.fallback_emoji or "").strip()
            if fallback_emoji and text and not text.startswith(fallback_emoji):
                text = f"{fallback_emoji} {text}"
            elif fallback_emoji and not text:
                text = fallback_emoji

        rendered_text, inline_entities = _extract_inline_custom_emoji_entities(text)
        entities = prefix_entities + inline_entities
        if entities:
            return {"text": rendered_text, "entities": entities}
        return {"text": rendered_text}


async def render_bot_message(
    template_key: str,
    lang: str,
    fallback: str,
    *,
    variables: Mapping[str, Any] | None = None,
    fallback_emoji: str = "",
) -> RenderedBotMessage:
    fallback_text = _render_variables(str(fallback or ""), variables)
    try:
        template = await get_bot_message_template(template_key, lang)
    except Exception:
        template = None

    if not template or not template.get("enabled", True):
        return RenderedBotMessage(
            text=fallback_text,
            fallback_emoji=fallback_emoji,
            from_template=False,
        )

    body_text = _render_variables(str(template.get("body_text") or fallback_text), variables)
    return RenderedBotMessage(
        text=body_text,
        custom_emoji_id=str(template.get("custom_emoji_id") or ""),
        fallback_emoji=str(template.get("fallback_emoji") or ""),
        from_template=True,
    )


@dataclass(frozen=True)
class RenderedBotButton:
    text: str
    custom_emoji_id: str = ""

    def to_inline_button_kwargs(self) -> dict[str, Any]:
        custom_emoji_id = normalize_custom_emoji_id(self.custom_emoji_id)
        return {"icon_custom_emoji_id": custom_emoji_id} if custom_emoji_id else {}


async def render_bot_button(
    template_key: str,
    lang: str,
    fallback: str,
    *,
    variables: Mapping[str, Any] | None = None,
    fallback_emoji: str = "",
    fallback_custom_emoji_id: str = "",
) -> RenderedBotButton:
    rendered = await render_bot_message(
        template_key,
        lang,
        fallback,
        variables=variables,
        fallback_emoji=fallback_emoji,
    )
    custom_emoji_id = normalize_custom_emoji_id(rendered.custom_emoji_id)
    if not rendered.from_template:
        custom_emoji_id = custom_emoji_id or normalize_custom_emoji_id(fallback_custom_emoji_id)

    label = clean_single_line(rendered.text, fallback)
    if not custom_emoji_id:
        icon = clean_single_line(rendered.fallback_emoji or fallback_emoji)
        if icon and label and not label.startswith(icon):
            label = f"{icon} {label}"
        elif icon and not label:
            label = icon

    button = RenderedBotButton(
        text=fit_button_text(label),
        custom_emoji_id=custom_emoji_id,
    )
    cache_key = (_normalize_lang(lang), str(template_key or "").strip())
    if cache_key[1]:
        _BOT_BUTTON_LABEL_CACHE[cache_key] = button.text
    return button


async def warm_bot_button_labels(
    lang: str,
    defaults: Mapping[str, Mapping[str, str]] | None = None,
) -> None:
    clean_lang = _normalize_lang(lang)
    label_defaults = defaults or BOT_BUTTON_LABEL_DEFAULTS
    tasks = [
        render_bot_button(template_key, clean_lang, _fallback_for_lang(fallbacks, clean_lang))
        for template_key, fallbacks in label_defaults.items()
    ]
    if not tasks:
        return
    await asyncio.gather(*tasks, return_exceptions=True)


async def reply_bot_message(
    message,
    template_key: str,
    lang: str,
    fallback: str,
    *,
    variables: Mapping[str, Any] | None = None,
    fallback_emoji: str = "",
    **kwargs,
):
    rendered = await render_bot_message(
        template_key,
        lang,
        fallback,
        variables=variables,
        fallback_emoji=fallback_emoji,
    )
    payload = rendered.to_telegram_kwargs()
    if payload.get("entities"):
        kwargs.pop("parse_mode", None)
    return await message.reply_text(**payload, **kwargs)


async def send_bot_message(
    bot,
    chat_id: int,
    template_key: str,
    lang: str,
    fallback: str,
    *,
    variables: Mapping[str, Any] | None = None,
    fallback_emoji: str = "",
    **kwargs,
):
    rendered = await render_bot_message(
        template_key,
        lang,
        fallback,
        variables=variables,
        fallback_emoji=fallback_emoji,
    )
    payload = rendered.to_telegram_kwargs()
    if payload.get("entities"):
        kwargs.pop("parse_mode", None)
    return await bot.send_message(chat_id=chat_id, **payload, **kwargs)


async def edit_bot_message_text(
    query,
    template_key: str,
    lang: str,
    fallback: str,
    *,
    variables: Mapping[str, Any] | None = None,
    fallback_emoji: str = "",
    **kwargs,
):
    rendered = await render_bot_message(
        template_key,
        lang,
        fallback,
        variables=variables,
        fallback_emoji=fallback_emoji,
    )
    payload = rendered.to_telegram_kwargs()
    if payload.get("entities"):
        kwargs.pop("parse_mode", None)
    return await edit_or_reply_callback_message(
        query,
        action=f"bot_message.{template_key}",
        **payload,
        **kwargs,
    )
