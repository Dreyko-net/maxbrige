"""
Вспомогательные функции для отправки сообщений и медиа в Telegram.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Optional, TYPE_CHECKING

import aiohttp
from aiogram import Bot
from aiogram.types import BufferedInputFile, Message as TgMessage
from aiogram.exceptions import TelegramRetryAfter, TelegramNetworkError

from database import db, User, Chat
from bridge.queue import BridgeEvent
from config import MAX_SEND_BYTES

if TYPE_CHECKING:
    from bridge.max_client import MaxUserClient

log = logging.getLogger(__name__)

# Внутренний кэш скачанных файлов: {(chat_id, file_id): bytes}
# Сбрасывается вручную через clear_download_cache()
_download_cache: dict[tuple, bytes] = {}


# ── Форматирование текста ──────────────────────────────────────────────────

def format_history_message(
    sender_name: str,
    text: str,
    timestamp: int,
    has_media: bool = False,
    media_type: str | None = None,
) -> str:
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
    if has_media and not text:
        icon = {
            "photo": "🖼", "video": "🎬", "document": "📄",
            "voice": "🎤", "audio": "🎵", "sticker": "😊",
        }.get(media_type or "", "📎")
        body = f"{icon} <i>[{media_type or 'медиафайл'}]</i>"
    else:
        body = text or ""

    return f"👤 <b>{sender_name}</b>\n{body}" if body else f"👤 <b>{sender_name}</b>"



def extract_single_attach(attach) -> dict | None:
    """Извлекает информацию об одном вложении (по объекту attach, не из msg).

    Возвращает тот же формат что и extract_attachment, но для произвольного attach.
    """
    cls_name = type(attach).__name__.lower()
    atype_str = getattr(attach, "type", None) or ""

    if cls_name in ("controlattachment", "unknownattachment", "inlinekeyboardattachment"):
        return None
    if atype_str in ("CONTROL", "INLINE_KEYBOARD", "UNKNOWN"):
        return None

    if "photo" in cls_name or atype_str == "PHOTO":
        photo_id = getattr(attach, "photo_id", 0)
        base_url = getattr(attach, "base_url", None)
        return {"type": "photo", "attach": attach, "url": base_url,
                "filename": f"photo_{photo_id}.jpg", "photo_id": photo_id}

    if "video" in cls_name or atype_str == "VIDEO":
        video_id = getattr(attach, "video_id", 0)
        return {"type": "video", "attach": attach, "url": None,
                "filename": f"video_{video_id}.mp4", "video_id": video_id,
                "duration": getattr(attach, "duration", None)}

    if "file" in cls_name or atype_str == "FILE":
        file_id = getattr(attach, "file_id", 0)
        name = getattr(attach, "name", None)
        return {"type": "document", "attach": attach, "url": None,
                "filename": name or f"file_{file_id}", "file_id": file_id,
                "size": getattr(attach, "size", 0)}

    if "audio" in cls_name or atype_str == "AUDIO":
        audio_id = getattr(attach, "audio_id", 0)
        duration = getattr(attach, "duration", 0) or 0
        url = getattr(attach, "url", None)
        if duration > 0 and duration <= 300:
            atype = "voice"
            filename = f"voice_{audio_id or 0}.ogg"
        else:
            atype = "audio"
            filename = f"audio_{audio_id or 0}.mp3"
        return {"type": atype, "attach": attach, "url": url,
                "filename": filename, "audio_id": audio_id, "duration": duration}

    if "sticker" in cls_name or atype_str == "STICKER":
        url = getattr(attach, "url", None)
        sticker_id = getattr(attach, "sticker_id", 0)
        return {"type": "sticker", "attach": attach, "url": url,
                "filename": f"sticker_{sticker_id}.webp"}

    if "share" in cls_name or atype_str == "SHARE":
        return {"type": "share", "attach": attach, "url": None, "filename": None}

    if "contact" in cls_name or atype_str == "CONTACT":
        return {"type": "contact", "attach": attach, "url": None, "filename": None}

    if "call" in cls_name or atype_str == "CALL":
        return {"type": "call", "attach": attach, "url": None, "filename": None}

    log.debug("Skipping unknown attachment: cls=%s api_type=%s", cls_name, atype_str)
    return None


def extract_all_attachments(msg) -> list[dict]:
    """Извлекает информацию обо всех вложениях из сообщения."""
    attaches = getattr(msg, "attaches", None) or []
    result = []
    for att in attaches:
        info = extract_single_attach(att)
        if info:
            result.append(info)
    return result


# ── Извлечение вложения из сообщения MAX ───────────────────────────────────

def extract_attachment(msg) -> dict | None:
    """Извлекает информацию о первом подходящем вложении из msg.attaches.

    Возвращает dict с ключами:
      - type: 'photo' | 'video' | 'document' | 'voice' | 'audio' | 'sticker' | 'share'
      - attach: объект вложения
      - url: прямой URL если есть (photo, audio, sticker)
      - filename: имя файла
      - duration: длительность если есть
    Возвращает None если:
      - вложений нет
      - тип системный (Control, InlineKeyboard, Unknown) — такие не отправляем
    """
    attaches = getattr(msg, "attaches", None) or []
    if not attaches:
        return None

    attach = attaches[0]
    cls_name = type(attach).__name__.lower()
    atype_str = getattr(attach, "type", None) or ""

    # ── Системные типы — пропускаем полностью ──
    if cls_name in ("controlattachment", "unknownattachment", "inlinekeyboardattachment"):
        return None
    if atype_str in ("CONTROL", "INLINE_KEYBOARD", "UNKNOWN"):
        return None

    # ── Photo ──
    if "photo" in cls_name or atype_str == "PHOTO":
        photo_id = getattr(attach, "photo_id", 0)
        base_url = getattr(attach, "base_url", None)
        return {
            "type": "photo",
            "attach": attach,
            "url": base_url,
            "filename": f"photo_{photo_id}.jpg",
            "photo_id": photo_id,
        }

    # ── Video ──
    if "video" in cls_name or atype_str == "VIDEO":
        video_id = getattr(attach, "video_id", 0)
        return {
            "type": "video",
            "attach": attach,
            "url": None,  # нужен get_video_by_id
            "filename": f"video_{video_id}.mp4",
            "video_id": video_id,
            "duration": getattr(attach, "duration", None),
        }

    # ── File (document) ──
    if "file" in cls_name or atype_str == "FILE":
        file_id = getattr(attach, "file_id", 0)
        name = getattr(attach, "name", None)
        return {
            "type": "document",
            "attach": attach,
            "url": None,  # нужен get_file_by_id
            "filename": name or f"file_{file_id}",
            "file_id": file_id,
            "size": getattr(attach, "size", 0),
        }

    # ── Audio / Voice ──
    if "audio" in cls_name or atype_str == "AUDIO":
        audio_id = getattr(attach, "audio_id", 0)
        duration = getattr(attach, "duration", 0) or 0
        url = getattr(attach, "url", None)
        if duration > 0 and duration <= 300:
            atype = "voice"
            filename = f"voice_{audio_id or 0}.ogg"
        else:
            atype = "audio"
            filename = f"audio_{audio_id or 0}.mp3"
        return {
            "type": atype,
            "attach": attach,
            "url": url,
            "filename": filename,
            "audio_id": audio_id,
            "duration": duration,
        }

    # ── Sticker ──
    if "sticker" in cls_name or atype_str == "STICKER":
        url = getattr(attach, "url", None)
        sticker_id = getattr(attach, "sticker_id", 0)
        return {
            "type": "sticker",
            "attach": attach,
            "url": url,
            "filename": f"sticker_{sticker_id}.webp",
        }

    # ── Share ──
    if "share" in cls_name or atype_str == "SHARE":
        return {
            "type": "share",
            "attach": attach,
            "url": None,
            "filename": None,
        }

    # ── Contact ──
    if "contact" in cls_name or atype_str == "CONTACT":
        return {
            "type": "contact",
            "attach": attach,
            "url": None,
            "filename": None,
        }

    # ── Call ──
    if "call" in cls_name or atype_str == "CALL":
        return {
            "type": "call",
            "attach": attach,
            "url": None,
            "filename": None,
        }

    # Неизвестный тип — пропускаем (не падаем)
    log.debug("Skipping unknown attachment: cls=%s api_type=%s", cls_name, atype_str)
    return None


# ── Скачивание медиа из MAX ───────────────────────────────────────────────

async def download_from_max(client: "MaxUserClient", msg, attach_info: dict,
                          max_chat_id: str | None = None) -> bytes | None:
    """Скачивает медиа из MAX. Возвращает байты или None.

    Args:
        max_chat_id: ID чата MAX (строка). Если не передан — берётся из msg.chat_id.
                      Обязателен для video и document, т.к. msg.chat_id может быть 0.
    """
    atype = attach_info["type"]
    url = attach_info.get("url")
    # Явно переданный chat_id приоритетнее (msg.chat_id может быть 0)
    chat_id = int(max_chat_id) if max_chat_id else (getattr(msg, "chat_id", 0) or 0)
    msg_id = getattr(msg, "id", 0)
    # Ключ для кэша: (chat_id, file_id/video_id/audio_id)
    cache_key = None
    for fk in ("video_id", "file_id", "audio_id", "photo_id", "sticker_id"):
        fid = attach_info.get(fk)
        if fid:
            cache_key = (chat_id, int(fid))
            break

    # Проверяем кэш
    if cache_key and cache_key in _download_cache:
        log.debug("download_from_max: cache hit for %s", cache_key)
        return _download_cache[cache_key]

    # Прямой URL (photo, audio, sticker)
    if url:
        data = await _download_url(url, timeout=60)
        if data and cache_key:
            _download_cache[cache_key] = data
        return data

    # Видео — нужен VideoRequest
    if atype == "video":
        video_id = attach_info.get("video_id", 0)
        if not video_id or not chat_id:
            log.warning("Video: missing video_id or chat_id (video=%s, chat=%s, msg=%s)",
                        video_id, chat_id, msg_id, max_chat_id)
            return None
        try:
            req = await client._client.get_video_by_id(
                chat_id=chat_id,
                message_id=int(msg_id) if msg_id else 0,
                video_id=int(video_id),
            )
            if req and getattr(req, "url", None):
                data = await _download_url(req.url, timeout=120)
                if data and cache_key:
                    _download_cache[cache_key] = data
                return data
            log.warning("get_video_by_id returned no URL (chat=%s, msg=%s, video=%s)",
                        chat_id, msg_id, video_id)
        except Exception as e:
            log.error("Download video error (chat=%s, msg=%s, video=%s): %s",
                      chat_id, msg_id, video_id, e)
        return None

    # Файл — нужен FileRequest
    if atype == "document":
        file_id = attach_info.get("file_id", 0)
        if not file_id or not chat_id:
            log.warning("File: missing file_id or chat_id (file=%s, chat=%s, msg=%s)",
                        file_id, chat_id, msg_id, max_chat_id)
            return None
        try:
            req = await client._client.get_file_by_id(
                chat_id=chat_id,
                message_id=int(msg_id) if msg_id else 0,
                file_id=int(file_id),
            )
            if req and getattr(req, "url", None):
                data = await _download_url(req.url, timeout=120)
                if data and cache_key:
                    _download_cache[cache_key] = data
                return data
            log.warning("get_file_by_id returned no URL (chat=%s, msg=%s, file=%s)",
                        chat_id, msg_id, file_id)
        except Exception as e:
            log.error("Download file error (chat=%s, msg=%s, file=%s): %s",
                      chat_id, msg_id, file_id, e)
        return None

    log.warning("No download method for attachment type=%s", atype)
    return None

def clear_download_cache():
    """Очищает кэш скачанных файлов. Вызывать после завершения синхронизации."""
    global _download_cache
    if _download_cache:
        log.info("Clearing download cache: %d entries", len(_download_cache))
        _download_cache.clear()



async def _download_url(url: str, timeout: int = 60) -> bytes | None:
    """Скачивает файл по URL с одним повтором при ошибке."""
    for attempt in range(2):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        if len(data) > 0:
                            return data
                    log.warning("Download failed: status=%s, url=%s", resp.status, url[:120])
        except Exception as e:
            if attempt == 0:
                log.warning("Download attempt 1 failed: %s, retrying...", e)
                await asyncio.sleep(1)
            else:
                log.error("Download failed after 2 attempts: %s", e)
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
    max_chat_id:  str | None = None,
) -> TgMessage | None:
    """Отправляет сообщение в тему с реальным медиа-контентом.

    Args:
        max_chat_id: ID чата MAX. Обязателен для видео и документов,
                     т.к. msg.chat_id может быть 0 в истории.
    """
    attach_info = extract_attachment(msg)

    if not attach_info:
        # Нет вложений (или системный тип типа Control) — отправляем как текст
        return await send_text_to_topic(bot, group_id, topic_id, text)

    atype = attach_info["type"]

    # Текстовые типы — отправляем как форматированный текст
    if atype == "share":
        return await send_text_to_topic(bot, group_id, topic_id,
                                        _format_share(attach_info["attach"], caption))
    if atype == "contact":
        return await send_text_to_topic(bot, group_id, topic_id,
                                        _format_contact(attach_info["attach"]))
    if atype == "call":
        return await send_text_to_topic(bot, group_id, topic_id,
                                        _format_call(attach_info["attach"]))

    # Скачиваем медиа
    data = await download_from_max(client, msg, attach_info, max_chat_id=max_chat_id)
    if not data:
        log.warning("Failed to download %s (msg=%s), sending as text fallback",
                    atype, getattr(msg, "id", "?"))
        icon = {"photo": "🖼", "video": "🎬", "document": "📄",
                "voice": "🎤", "audio": "🎵", "sticker": "😊"}.get(atype, "📎")
        fallback = f"{text}\n{icon} <i>[{atype} — не удалось скачать]</i>" if text else f"{icon} <i>[{atype} — не удалось скачать]</i>"
        return await send_text_to_topic(bot, group_id, topic_id, fallback)

    # Проверяем размер — если файл слишком большой для прокси/Telegram API
    if len(data) > MAX_SEND_BYTES:
        size_mb = len(data) / (1024 * 1024)
        log.warning("File too large (%.1f MB > %d MB limit), sending as text fallback: %s",
                    size_mb, MAX_SEND_BYTES // (1024 * 1024), attach_info.get("filename", "?"))
        icon = {"photo": "🖼", "video": "🎬", "document": "📄",
                "voice": "🎤", "audio": "🎵", "sticker": "😊"}.get(atype, "📎")
        fallback = f"{text}\n{icon} <i>[{atype} — файл слишком большой ({size_mb:.1f} МБ)]</i>" if text else f"{icon} <i>[{atype} — файл слишком большой ({size_mb:.1f} МБ)]</i>"
        return await send_text_to_topic(bot, group_id, topic_id, fallback)

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
                parse_mode="HTML",
            )

        elif atype == "video":
            kw = dict(chat_id=group_id, message_thread_id=topic_id,
                      video=buf,
                      caption=caption[:1024] if caption else None,
                      parse_mode="HTML")
            if duration:
                kw["duration"] = duration
            return await _send_with_retry(bot.send_video, **kw)

        elif atype == "voice":
            kw = dict(chat_id=group_id, message_thread_id=topic_id,
                      voice=buf,
                      caption=caption[:1024] if caption else None,
                      parse_mode="HTML")
            if duration:
                kw["duration"] = duration
            return await _send_with_retry(bot.send_voice, **kw)

        elif atype == "audio":
            kw = dict(chat_id=group_id, message_thread_id=topic_id,
                      audio=buf,
                      caption=caption[:1024] if caption else None,
                      parse_mode="HTML")
            if duration:
                kw["duration"] = duration
            return await _send_with_retry(bot.send_audio, **kw)

        elif atype == "sticker":
            # Стикеры MAX несовместимы с TG — отправляем как документ (webp)
            return await _send_with_retry(
                bot.send_document,
                chat_id=group_id,
                message_thread_id=topic_id,
                document=buf,
                caption=caption[:1024] if caption else None,
                parse_mode="HTML",
            )

        elif atype == "document":
            return await _send_with_retry(
                bot.send_document,
                chat_id=group_id,
                message_thread_id=topic_id,
                document=buf,
                caption=caption[:1024] if caption else None,
                parse_mode="HTML",
            )

        else:
            return await send_text_to_topic(bot, group_id, topic_id, text)

    except Exception as e:
        log.error("send_media error (type=%s): %s", atype, e)
        return await send_text_to_topic(bot, group_id, topic_id, text)


# ── Вспомогательные функции отправки ──────────────────────────────────────

async def _send_with_retry(func, *, max_retries: int = 3, **kwargs) -> TgMessage | None:
    """Вызывает функцию отправки с retry при flood control И сетевых ошибках."""
    for attempt in range(max_retries):
        try:
            return await func(**kwargs)
        except TelegramRetryAfter as e:
            wait = e.retry_after + 1
            log.warning("send retry (flood), waiting %ds (attempt %d)", wait, attempt + 1)
            await asyncio.sleep(wait)
        except TelegramNetworkError as e:
            # ServerDisconnectedError, ConnectionReset и прочее — пробуем ещё раз
            wait = 2 ** attempt + 1  # 2, 3, 5 секунд
            log.warning("send retry (network: %s), waiting %ds (attempt %d)",
                        type(e).__name__, wait, attempt + 1)
            await asyncio.sleep(wait)
        except Exception as e:
            log.error("send error: %s", e)
            return None
    log.error("send failed after %d retries", max_retries)
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
    url = getattr(attach, "url", None)
    title = getattr(attach, "title", None)
    description = getattr(attach, "description", None)
    if url:
        link_text = title or url
        parts.append(f"🔗 <a href=\"{url}\">{link_text}</a>")
    elif title:
        parts.append(f"🔗 {title}")
    if description:
        parts.append(f"📝 {description}")
    if text:
        parts.append(text)
    return "\n".join(parts) if parts else (text or "📎 Поделился")


def _format_contact(attach) -> str:
    name = getattr(attach, "name", None)
    first = getattr(attach, "first_name", None)
    last = getattr(attach, "last_name", None)
    contact_id = getattr(attach, "contact_id", None)
    display = name or f"{first or ''} {last or ''}".strip() or f"Контакт {contact_id}"
    return f"👤 <b>Контакт:</b> {display}"


def _format_call(attach) -> str:
    duration = getattr(attach, "duration", None)
    if duration is not None:
        mins, secs = divmod(int(duration), 60)
        return f"📞 <b>Звонок</b> ({mins}:{secs:02d})"
    return f"📞 <b>Звонок</b>"


# ── Живые сообщения (из очереди) ──────────────────────────────────────────

async def send_to_telegram(
    bot:        Bot,
    event:      BridgeEvent,
    user:       User,
    chat:       Chat,
    max_client: Optional["MaxUserClient"],
):
    """Отправляет живое сообщение из MAX в Telegram-тему.

    Для живых сообщений объект msg из PyMax недоступен,
    поэтому медиа отправляем текстом.
    """
    sender_name = ""
    if max_client and event.max_sender_id:
        try:
            sender_name = await max_client.get_client(event.max_sender_id) or ""
        except Exception:
            pass

    caption = format_live_message(
        sender_name = sender_name,
        text        = event.text,
        has_media   = event.has_media,
        media_type  = event.media_type,
    )

    sent = await send_text_to_topic(
        bot      = bot,
        group_id = user.tg_group_id,
        topic_id = chat.tg_topic_id,
        text     = caption,
    )
    if not sent:
        return

    await db.save_message(
        user_id    = user.id,
        chat_id    = chat.id,
        direction  = "max_to_tg",
        timestamp  = event.timestamp,
        max_sender_id = event.max_sender_id,
        max_msg_id = event.max_msg_id,
        tg_msg_id  = sent.message_id,
        has_media  = event.has_media,
    )


# ── Совместимость ─────────────────────────────────────────────────────────


# Подготовлен для удаления 2026/07/17 13:48
# async def send_to_telegram_topic(
#     bot:      Bot,
#     group_id: int,
#     topic_id: int,
#     text:     str,
#     sender_name: Optional[str] = None,
# ) -> Optional[TgMessage]:
#     """Отправляет текст в тему супергруппы с retry при flood control.

#     УСТАРЕВШАЯ — оставлена для совместимости.
#     Для медиа используй send_media_to_telegram_topic.
#     """
#     for attempt in range(5):
#         try:
#             if sender_name:
#                 text = f"{sender_name}: {text}"
#             return await bot.send_message(
#                 chat_id           = group_id,
#                 message_thread_id = topic_id,
#                 text              = text[:4096],
#                 parse_mode        = "HTML",
#             )
#         except TelegramRetryAfter as e:
#             wait = e.retry_after + 1
#             log.warning("send_to_telegram_topic flood, waiting %ds (attempt %d)",
#                         wait, attempt + 1)
#             await asyncio.sleep(wait)
#         except TelegramNetworkError as e:
#             wait = 2 ** attempt + 1
#             log.warning("send_to_telegram_topic network error, waiting %ds (attempt %d)",
#                         wait, attempt + 1)
#             await asyncio.sleep(wait)
#         except Exception as e:
#             log.error("send_to_telegram_topic error: %s", e)
#             return None
#     return None