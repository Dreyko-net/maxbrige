"""
Вспомогательные функции для отправки сообщений в Telegram.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Optional, TYPE_CHECKING

from aiogram import Bot
from aiogram.types import Message as TgMessage

from database import db, User, Chat, MediaCache
from bridge.queue import BridgeEvent

if TYPE_CHECKING:
    from bridge.max_client import MaxUserClient

log = logging.getLogger(__name__)


def format_history_message(
    sender_name: str,
    text: str,
    timestamp: int,
    has_media: bool = False,
    media_type: str | None = None,
) -> str:
    """Форматирует историческое сообщение с именем отправителя и временем."""
    dt = datetime.fromtimestamp(timestamp / 1000).strftime("%d.%m %H:%M")
    header = f"👤 <b>{sender_name}</b>  <i>{dt}</i>"

    if has_media and not text:
        icon = {
            "photo": "🖼", "video": "🎬", "document": "📄",
            "voice": "🎤", "audio": "🎵", "sticker": "😊",
        }.get(media_type or "", "📎")
        body = f"{icon} <i>[{media_type or 'медиафайл'}]</i>"
    else:
        body = text or ""

    return f"{header}\n{body}" if body else header


def format_live_message(sender_name: str, text: str, has_media: bool = False,
                        media_type: str | None = None) -> str:
    """Форматирует живое (только что полученное) сообщение."""
    if has_media and not text:
        icon = {
            "photo": "🖼", "video": "🎬", "document": "📄",
            "voice": "🎤", "audio": "🎵", "sticker": "😊",
        }.get(media_type or "", "📎")
        body = f"{icon} <i>[{media_type or 'медиафайл'}]</i>"
    else:
        body = text or ""

    return f"👤 <b>{sender_name}</b>\n{body}" if body else f"👤 <b>{sender_name}</b>"


async def send_to_telegram_topic(
    bot:      Bot,
    group_id: int,
    topic_id: int,
    text:     str,
    sender_name: Optional[str] = None,
) -> Optional[TgMessage]:
    """Отправляет текст в тему супергруппы с retry при flood control."""
    from aiogram.exceptions import TelegramRetryAfter
    import asyncio
    for attempt in range(5):
        try:
            if sender_name:
                text = f"{sender_name}: {text}"
            return await bot.send_message(
                chat_id           = group_id,
                message_thread_id = topic_id,
                text              = text[:4096],
                parse_mode        = "HTML",
            )
        except TelegramRetryAfter as e:
            wait = e.retry_after + 1
            log.warning("send_to_telegram_topic flood, waiting %ds (attempt %d)",
                        wait, attempt + 1)
            await asyncio.sleep(wait)
        except Exception as e:
            log.error("send_to_telegram_topic error: %s", e)
            return None
    return None


async def send_to_telegram(
    bot:        Bot,
    event:      BridgeEvent,
    user:       User,
    chat:       Chat,
    max_client: Optional["MaxUserClient"],
):
    """
    Отправляет живое сообщение из MAX в Telegram-тему.
    Если есть медиа — добавляет кнопку "📎 Загрузить".
    """
    from telegram.keyboards import media_download_kb
    # Имя отправителя пока берём из event (можно расширить)
    sender_name = await max_client.get_client(event.max_sender_id) if event.max_sender_id else ""

    text = format_live_message(
        sender_name = sender_name,
        text        = event.text,
        has_media   = event.has_media,
        media_type  = event.media_type,
    )

    sent = await send_to_telegram_topic(
        bot      = bot,
        group_id = user.tg_group_id,
        topic_id = chat.tg_topic_id,
        text     = text,
    )
    if not sent:
        return

    # Сохраняем маппинг
    msg_db_id = await db.save_message(
        user_id    = user.id,
        chat_id    = chat.id,
        direction  = "max_to_tg",
        timestamp  = event.timestamp,
        max_sender_id = event.max_sender_id,
        max_msg_id = event.max_msg_id,
        tg_msg_id  = sent.message_id,
        has_media  = event.has_media,
    )

    # Кнопка загрузки медиа
    if event.has_media and msg_db_id and event.max_msg_id:
        try:
            await bot.edit_message_reply_markup(
                chat_id      = user.tg_group_id,
                message_id   = sent.message_id,
                reply_markup = media_download_kb(msg_db_id, event.max_msg_id),
            )
        except Exception as e:
            log.error("edit media kb error: %s", e)