from typing import Any, Dict, Optional, Tuple

from telegram import Update
from telegram.ext import ContextTypes

from database import get_or_create_user, log_telegram_message


def _extract_content(update: Update) -> Tuple[str, Optional[str], Optional[Dict[str, Any]]]:
    message = update.effective_message
    if not message:
        return ("unknown", None, None)

    text = message.text or message.caption
    payload: Optional[Dict[str, Any]] = None

    if message.document:
        doc = message.document
        payload = {
            "file_id": doc.file_id,
            "file_name": doc.file_name,
            "mime_type": doc.mime_type,
        }
        return ("document", text, payload)

    if message.photo:
        photo = message.photo[-1]
        payload = {"file_id": photo.file_id}
        return ("photo", text, payload)

    if message.sticker:
        payload = {"file_id": message.sticker.file_id, "emoji": message.sticker.emoji}
        return ("sticker", text, payload)

    if message.voice:
        payload = {"file_id": message.voice.file_id, "duration": message.voice.duration}
        return ("voice", text, payload)

    if message.video:
        payload = {"file_id": message.video.file_id, "duration": message.video.duration}
        return ("video", text, payload)

    if message.audio:
        payload = {"file_id": message.audio.file_id, "duration": message.audio.duration}
        return ("audio", text, payload)

    if message.text:
        return ("text", message.text, None)

    return ("unknown", text, None)


async def log_incoming_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Capture user<->bot chat history for the admin dashboard.
    Telegram Bot API doesn't allow fetching arbitrary history, so we store updates as they arrive.
    """
    message = update.effective_message
    user = update.effective_user
    if not message or not user or not message.chat:
        return

    # Only log private chats (user_id == chat_id).
    if getattr(message.chat, "type", None) != "private":
        return

    try:
        await get_or_create_user(
            user.id,
            getattr(user, "username", None),
            getattr(user, "first_name", None),
            getattr(user, "last_name", None)
        )
    except Exception:
        # User creation is best-effort; logging can still proceed.
        pass

    message_type, text, payload = _extract_content(update)

    await log_telegram_message(
        chat_id=message.chat.id,
        message_id=message.message_id,
        direction="in",
        message_type=message_type,
        text=text,
        payload=payload,
        sent_at=message.date,
    )
