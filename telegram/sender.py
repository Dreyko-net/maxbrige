"""
Вспомогательные функции для отправки сообщений и медиа в Telegram.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Optional, TYPE_CHECKING

import aiohttp
from aiogram import Bot
from aiogram.types import BufferedInputFile, Message as TgMessage
from aiogram.exceptions import TelegramRetryAfter

from database import db, User, Chat
from bridge.queue import BridgeEvent

if TYPE_CHECKING:
    from bridge.max_client import MaxUserClient

log = logging.getLogger(__name__)


# ── Форматирование текста ──────────────────────────────────────────────────

def format_history_message(
    sender_name: str,
    text: str,
    timestamp: int,
    has_media: bool = False,
    media_type: str | None = None,
) -> str:
    dt = datetime.fromtimestamp(timestamp / 1000).strftime("%d.%m %H:%M")
    header = f"👤 {sender_name}  {dt}"

    if has_media and not text:
        icon = {
            "photo": "🖼", "video": "🎬", "document": "📄",
            "voice": "🎤", "audio": "🎵", "sticker": "😊",
        }.get(media_type or "", "📎")
        body = f"{icon} [{media_type or 'медиафайл'}]"
    else:
        body = text or ""

    return f"{header}\n{body}" if body else header


def format_live_message(sender_name: str, text: str, has_media: bool = False,
                        media_type: str | None = None) -> str:
    if has_media and not text:
        icon = {
            "photo": "🖼", "video": "🎬", "document": "📄",
            "voice": "🎤", "audio": "🎵", "sticker": "😊",
        }.get(media_type or "", "📎")
        body = f"{icon} [{media_type or 'медиафайл'}]"
    else:
        body = text or ""

    return f"👤 {sender_name}\n{body}" if body else f"👤 {sender_name}"


# ── Извлечение вложения из сообщения MAX ───────────────────────────────────

def extract_attachment(msg) -> dict | None:
    """Извлекает информацию о первом подходящем вложении из msg.attaches.
    
    Возвращает dict с ключами:
      - type: 'photo' | 'video' | 'document' | 'voice' | 'audio' | 'sticker' | 'share'
      - attach: объект вложения (PhotoAttachment и т.д.)
      - url: прямой URL если есть (photo, audio, sticker)
      - filename: имя файла если есть
      - duration: длительность если есть
    """
    attaches = getattr(msg, "attaches", None) or []
    if not attaches:
        return None

    from pymax.types.domain.attachments import (
        PhotoAttachment, VideoAttachment, FileAttachment,
        AudioAttachment, StickerAttachment, ShareAttachment,
    )

    for attach in attaches:
        if isinstance(attach, PhotoAttachment):
            return {
                "type": "photo",
                "attach": attach,
                "url": attach.base_url,
                "filename": f"photo_{attach.photo_id}.jpg",
                "width": attach.width,
                "height": attach.height,
            }

        if isinstance(attach, VideoAttachment):
            return {
                "type": "video",
                "attach": attach,
                "url": None,  # нужен get_video_by_id
                "filename": f"video_{attach.video_id}.mp4",
                "duration": attach.duration,
                "width": attach.width,
                "height": attach.height,
            }

        if isinstance(attach, FileAttachment):
            return {
                "type": "document",
                "attach": attach,
                "url": None,  # нужен get_file_by_id
                "filename": attach.name or f"file_{attach.file_id}",
                "size": attach.size,
            }

        if isinstance(attach, AudioAttachment):
            # Короткие аудио — голосовые, длинные — музыка
            duration = attach.duration or 0
            if duration > 0 and duration < 300:
                atype = "voice"
                filename = f"voice_{attach.audio_id or 0}.ogg"
            else:
                atype = "audio"
                filename = f"audio_{attach.audio_id or 0}.mp3"
            return {
                "type": atype,
                "attach": attach,
                "url": attach.url,
                "filename": filename,
                "duration": duration,
            }

        if isinstance(attach, StickerAttachment):
            return {
                "type": "sticker",
                "attach": attach,
                "url": getattr(attach, "image_url", None)
                       or getattr(attach, "image", None),
                "filename": "sticker.webp",
            }

        if isinstance(attach, ShareAttachment):
            return {
                "type": "share",
                "attach": attach,
                "url": None,
                "filename": None,
            }

    return None


# ── Скачивание медиа из MAX ───────────────────────────────────────────────

async def download_from_max(client: "MaxUserClient", msg, attach_info: dict) -> bytes | None:
    """Скачивает медиа из MAX. Возвращает байты или None."""
    atype = attach_info["type"]
    url = attach_info.get("url")

    # Прямой URL (photo, audio, sticker)
    if url:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    log.warning("Download from direct URL failed: status=%s", resp.status)
        except Exception as e:
            log.error("Download from direct URL error: %s", e)
        return None

    # Видео — нужен VideoRequest
    if atype == "video":
        try:
            req = await client._client.get_video_by_id(
                chat_id=getattr(msg, "chat_id", 0) or 0,
                message_id=getattr(msg, "id", 0),
                video_id=attach_info["attach"].video_id,
            )
            if req and req.url:
                async with aiohttp.ClientSession() as session:
                    async with session.get(req.url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                        if resp.status == 200:
                            return await resp.read()
        except Exception as e:
            log.error("Download video error: %s", e)
        return None

    # Файл — нужен FileRequest
    if atype == "document":
        try:
            req = await client._client.get_file_by_id(
                chat_id=getattr(msg, "chat_id", 0) or 0,
                message_id=getattr(msg, "id", 0),
                file_id=attach_info["attach"].file_id,
            )
            if req and req.url:
                async with aiohttp.ClientSession() as session:
                    async with session.get(req.url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                        if resp.status == 200:
                            return await resp.read()
        except Exception as e:
            log.error("Download file error: %s", e)
        return None

    return None


# ── Отправка медиа в Telegram ─────────────────────────────────────────────

async def send_media_to_telegram_topic(
    bot:      Bot,
    group_id: int,
    topic_id: int,
    text:     str,
    client:   "MaxUserClient",
    msg,
    caption: str = "",
) -> TgMessage | None:
    """Отправляет сообщение в тему с реальным медиа-контентом.
    
    Если у сообщения есть вложение — скачивает из MAX и отправляет
    как photo/video/voice/audio/document. Иначе — как текст.
    """
    attach_info = extract_attachment(msg)
    
    if not attach_info:
        # Нет вложений — отправляем как текст
        return await send_text_to_topic(bot, group_id, topic_id, text)

    atype = attach_info["type"]

    # Share (геолокация/ссылка) — отправляем текстом
    if atype == "share":
        share_text = _format_share(attach_info["attach"], caption)
        return await send_text_to_topic(bot, group_id, topic_id, share_text)

    # Скачиваем медиа
    data = await download_from_max(client, msg, attach_info)
    if not data:
        log.warning("Failed to download %s, sending as text fallback", atype)
        return await send_text_to_topic(bot, group_id, topic_id, text)

    filename = attach_info.get("filename", "file")
    buf = BufferedInputFile(data, filename=filename)
    duration = attach_info.get("duration")

    # Отправляем по типу
    try:
        if atype == "photo":
            return await _send_with_retry(
                bot.send_photo,
                chat_id=group_id,
                message_thread_id=topic_id,
                photo=buf,
                caption=caption[:1024] if caption else None,
            )

        elif atype == "video":
            return await _send_with_retry(
                bot.send_video,
                chat_id=group_id,
                message_thread_id=topic_id,
                video=buf,
                caption=caption[:1024] if caption else None,
                duration=duration,
            )

        elif atype == "voice":
            return await _send_with_retry(
                bot.send_voice,
                chat_id=group_id,
                message_thread_id=topic_id,
                voice=buf,
                caption=caption[:1024] if caption else None,
                duration=duration,
            )

        elif atype == "audio":
            return await _send_with_retry(
                bot.send_audio,
                chat_id=group_id,
                message_thread_id=topic_id,
                audio=buf,
                caption=caption[:1024] if caption else None,
                duration=duration,
                title=filename,
            )

        elif atype == "sticker":
            # Стикеры MAX несовместимы с TG — отправляем как фото
            return await _send_with_retry(
                bot.send_photo,
                chat_id=group_id,
                message_thread_id=topic_id,
                photo=buf,
                caption=caption[:1024] if caption else None,
            )

        elif atype == "document":
            return await _send_with_retry(
                bot.send_document,
                chat_id=group_id,
                message_thread_id=topic_id,
                document=buf,
                caption=caption[:1024] if caption else None,
            )

        else:
            return await send_text_to_topic(bot, group_id, topic_id, text)

    except Exception as e:
        log.error("send_media error (type=%s): %s", atype, e)
        # Фоллбэк — текст
        return await send_text_to_topic(bot, group_id, topic_id, text)


async def _send_with_retry(func, *, max_retries: int = 3, **kwargs) -> TgMessage | None:
    """Вызывает функцию отправки с retry при TelegramRetryAfter."""
    for attempt in range(max_retries):
        try:
            return await func(**kwargs)
        except TelegramRetryAfter as e:
            wait = e.retry_after + 1
            log.warning("send retry, waiting %ds (attempt %d)", wait, attempt + 1)
            await asyncio.sleep(wait)
        except Exception as e:
            log.error("send error: %s", e)
            return None
    return None


async def send_text_to_topic(
    bot: Bot, group_id: int, topic_id: int, text: str,
) -> TgMessage | None:
    """Отправляет текст в тему."""
    return await _send_with_retry(
        bot.send_message,
        chat_id=group_id,
        message_thread_id=topic_id,
        text=text[:4096],
        parse_mode="HTML",
    )


def _format_share(attach, text: str = "") -> str:
    """Форматирует ShareAttachment как текст."""
    parts = []
    geo = getattr(attach, "geo_point", None) or getattr(attach, "geoPoint", None)
    if geo:
        lat = getattr(geo, "lat", "?")
        lon = getattr(geo, "lon", "?")
        parts.append(f"📍 Геолокация: {lat}, {lon}")
    url = getattr(attach, "url", None) or getattr(attach, "link", None)
    if url:
        parts.append(f"🔗 {url}")
    if text:
        parts.append(text)
    return "\n".join(parts) if parts else (text or "📎 Поделился")


# ── Живые сообщения (из очереди) ──────────────────────────────────────────

async def send_to_telegram(
    bot:        Bot,
    event:      BridgeEvent,
    user:       User,
    chat:       Chat,
    max_client: Optional["MaxUserClient"],
):
    """Отправляет живое сообщение из MAX в Telegram-тему с медиа."""
    sender_name = await max_client.get_client(event.max_sender_id) if event.max_sender_id else ""

    caption = format_live_message(
        sender_name = sender_name,
        text        = event.text,
        has_media   = event.has_media,
        media_type  = event.media_type,
    )

    # Для живых сообщений у нас нет объекта msg, отправляем текст
    # Медиа для живых сообщений можно добавить позже через поиск в истории
    sent = await send_text_to_topic(
        bot      = bot,
        group_id = user.tg_group_id,
        topic_id = chat.tg_topic_id,
        text     = caption,
    )
    if not sent:
        return

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

