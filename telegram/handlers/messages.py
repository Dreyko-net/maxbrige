"""
Хэндлеры входящих сообщений из Telegram → MAX.

Потоки:
1. Сообщения из тем супергруппы → MAX (текст и медиа)
2. Пересланные сообщения (с медиа) из любых чатов/каналов → MAX
"""

from __future__ import annotations

import asyncio
import logging
import time

from aiogram import Router, F, Bot
from aiogram.types import Message, ContentType

from bridge.manager import manager
from bridge.queue import BridgeEvent, tg_to_max_queue
from database import db

log = logging.getLogger(__name__)
router = Router()

# ── Буфер для альбомов (media_group_id → данные) ──────────────────────────

_media_group_buffer: dict[str, dict] = {}
_media_group_timers: dict[str, asyncio.Task] = {}

# Таймаут сброса буфера после последнего сообщения (секунды)
_MEDIA_GROUP_FLUSH_DELAY = 0.5


async def _flush_media_group(media_group_id: str):
    """Собирает буферизированные элементы альбома в одно событие и отправляет в очередь."""
    entry = _media_group_buffer.pop(media_group_id, None)
    _media_group_timers.pop(media_group_id, None)
    if not entry:
        return

    items = entry["items"]
    if not items:
        return

    # Формируем media_group список
    album_items = []
    other_items = []

    for item in items:
        if item["type"] in ("photo", "video"):
            album_items.append({
                "bytes": item["bytes"],
                "filename": item["filename"],
                "type": item["type"],
            })
        else:
            other_items.append(item)

    # Caption берём из первого элемента (в TG caption только на первом)
    caption = entry.get("caption", "")

    # Альбом: фото/видео → одно событие с media_group
    if album_items:
        event = BridgeEvent(
            direction   = "tg_to_max",
            tg_user_id  = entry["tg_user_id"],
            max_chat_id = entry["max_chat_id"],
            text        = caption,
            timestamp   = entry["timestamp"],
            tg_msg_id   = entry["tg_msg_ids"][0],
            media_group = album_items,
        )
        await tg_to_max_queue.put(event)

    # Остальные вложения — по одному
    for i, item in enumerate(other_items):
        txt = caption if not album_items and i == 0 else ""
        ev = BridgeEvent(
            direction   = "tg_to_max",
            tg_user_id  = entry["tg_user_id"],
            max_chat_id = entry["max_chat_id"],
            text        = txt,
            timestamp   = entry["timestamp"],
            tg_msg_id   = item.get("tg_msg_id"),
            has_media   = bool(item.get("bytes")),
            media_type  = item["type"],
            media_bytes = item.get("bytes"),
            media_name  = item.get("filename"),
        )
        await tg_to_max_queue.put(ev)

    # Если были только не-альбомные — отправляем текст отдельно если не прикреплён
    if not album_items and other_items and caption:
        # текст уже прикреплён к первому other_item
        pass
    elif not album_items and not other_items and caption:
        ev = BridgeEvent(
            direction   = "tg_to_max",
            tg_user_id  = entry["tg_user_id"],
            max_chat_id = entry["max_chat_id"],
            text        = caption,
            timestamp   = entry["timestamp"],
            tg_msg_id   = entry["tg_msg_ids"][0],
        )
        await tg_to_max_queue.put(ev)


def _schedule_flush(media_group_id: str):
    """Перезапускает таймер сброса буфера для данного media_group_id."""
    # Отменяем предыдущий таймер если есть
    old_task = _media_group_timers.get(media_group_id)
    if old_task and not old_task.done():
        old_task.cancel()

    loop = asyncio.get_running_loop()
    _media_group_timers[media_group_id] = loop.call_later(
        _MEDIA_GROUP_FLUSH_DELAY,
        lambda: asyncio.ensure_future(_flush_media_group(media_group_id)),
    )


# ── Пересланные сообщения (из любых чатов/каналов/групп) → MAX ───────────

@router.message(F.forward_origin, ~F.message_thread_id)
async def handle_forwarded_media(msg: Message, bot: Bot):
    """
    Пользователь переслал сообщение боту (личка).
    Скачиваем медиа и отправляем в MAX (в последний привязанный чат).
    """
    tg_user_id = msg.from_user.id
    user = await db.get_user(tg_user_id)
    if not user or user.status != "active":
        return

    # Берём последний привязанный чат пользователя
    chats = await db.get_user_chats(user.id)
    if not chats:
        await msg.answer(
            "⚠️ Нет привязанных чатов MAX.\n"
            "Сначала выполните /sync_chats или /sync."
        )
        return
    last_chat = chats[-1]
    max_chat_id = last_chat.max_chat_id

    media_type, file_id, filename = _extract_media(msg)

    # Если медиа нет — отправляем только текст
    if not file_id:
        text = msg.text or msg.caption or ""
        if not text:
            return
        event = BridgeEvent(
            direction   = "tg_to_max",
            tg_user_id  = tg_user_id,
            max_chat_id = max_chat_id,
            text        = text,
            timestamp   = int(time.time() * 1000),
            tg_msg_id   = msg.message_id,
        )
        await tg_to_max_queue.put(event)
        await msg.answer(f"✅ Текст отправлен в чат <b>{last_chat.max_chat_title}</b>", parse_mode="HTML")
        return

    # Скачиваем медиа
    media_bytes = await _download_media(bot, file_id, tag="[forwarded]")

    if not media_bytes:
        log.error("[forwarded] Download media failed")
        await msg.answer("❌ Не удалось скачать файл из Telegram.")
        return

    event = BridgeEvent(
        direction   = "tg_to_max",
        tg_user_id  = tg_user_id,
        max_chat_id = max_chat_id,
        text        = msg.caption or "",
        timestamp   = int(time.time() * 1000),
        tg_msg_id   = msg.message_id,
        has_media   = True,
        media_type  = media_type,
        media_bytes = media_bytes,
        media_name  = filename,
    )
    await tg_to_max_queue.put(event)
    await msg.answer(f"✅ Медиа отправлено в чат <b>{last_chat.max_chat_title}</b>", parse_mode="HTML")


# ── Текстовые сообщения из темы → MAX ────────────────────────────────────────

@router.message(F.chat.type.in_({"supergroup", "group"}), F.text)
async def handle_group_text(msg: Message):
    if not msg.message_thread_id:
        return  # сообщение не в теме — игнорируем

    tg_user_id = await _find_owner(msg.chat.id)
    if not tg_user_id:
        return

    user = await db.get_user(tg_user_id)
    if not user:
        return

    chat = await db.get_chat_by_topic(user.id, msg.message_thread_id)
    if not chat:
        log.warning("No MAX chat for topic %s", msg.message_thread_id)
        return

    event = BridgeEvent(
        direction   = "tg_to_max",
        tg_user_id  = tg_user_id,
        max_chat_id = chat.max_chat_id,
        text        = msg.text or "",
        timestamp   = int(time.time() * 1000),
        tg_msg_id   = msg.message_id,
    )
    await tg_to_max_queue.put(event)


# ── Медиафайлы из темы → MAX ──────────────────────────────────────────────────

@router.message(
    F.chat.type.in_({"supergroup", "group"}),
    F.content_type.in_({
        ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT,
        ContentType.VOICE, ContentType.AUDIO,
    }),
)
async def handle_group_media(msg: Message, bot: Bot):
    if not msg.message_thread_id:
        return

    tg_user_id = await _find_owner(msg.chat.id)
    if not tg_user_id:
        return

    user = await db.get_user(tg_user_id)
    if not user:
        return

    chat = await db.get_chat_by_topic(user.id, msg.message_thread_id)
    if not chat:
        return

    media_type, file_id, filename = _extract_media(msg)
    media_bytes = await _download_media(bot, file_id, tag="")

    if not media_bytes:
        log.error("Download TG media failed, putting text-only event")
        fallback = BridgeEvent(
            direction   = "tg_to_max",
            tg_user_id  = tg_user_id,
            max_chat_id = chat.max_chat_id,
            text        = msg.caption or msg.text or "",
            timestamp   = int(time.time() * 1000),
            tg_msg_id   = msg.message_id,
        )
        await tg_to_max_queue.put(fallback)
        return

    # ── Альбом: буферизируем по media_group_id ──
    mg_id = msg.media_group_id
    if mg_id:
        if mg_id not in _media_group_buffer:
            _media_group_buffer[mg_id] = {
                "tg_user_id": tg_user_id,
                "max_chat_id": chat.max_chat_id,
                "caption": msg.caption or "",
                "timestamp": int(time.time() * 1000),
                "tg_msg_ids": [],
                "items": [],
            }
        buf = _media_group_buffer[mg_id]
        buf["items"].append({
            "type": media_type,
            "bytes": media_bytes,
            "filename": filename,
            "tg_msg_id": msg.message_id,
        })
        buf["tg_msg_ids"].append(msg.message_id)
        # Перезапускаем таймер сброса
        _schedule_flush(mg_id)
        return

    # ── Обычное медиа (не альбом) ──
    event = BridgeEvent(
        direction   = "tg_to_max",
        tg_user_id  = tg_user_id,
        max_chat_id = chat.max_chat_id,
        text        = msg.caption or "",
        timestamp   = int(time.time() * 1000),
        tg_msg_id   = msg.message_id,
        has_media   = True,
        media_type  = media_type,
        media_bytes = media_bytes,
        media_name  = filename,
    )
    await tg_to_max_queue.put(event)


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _find_owner(tg_group_id: int) -> int | None:
    user = await db.get_user_by_group(tg_group_id)
    return user.tg_user_id if user else None



async def _download_media(bot: Bot, file_id: str, tag: str = "") -> bytes | None:
    """Скачивает файл из Telegram.
    Сначала пробует обычный способ (для маленьких файлов).
    При обрыве — докачивает чанками по 4МБ через HTTP Range.
    """
    import aiohttp

    file = await bot.get_file(file_id)
    if not file.file_path:
        return None

    # Строим URL файла (через прокси бота, если настроен)
    file_url = bot.session.api.file.format(
        token=bot.token, path=file.file_path,
    )

    # 1. Обычное скачивание (для файлов < 5МБ работает)
    try:
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(file_url) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    if data:
                        return data
    except Exception as e:
        log.warning("%s Normal download failed: %s", tag, e)

    # 2. Fallback: чанками через HTTP Range (по 4МБ)
    log.info("%s Trying Range-based chunked download...", tag)
    return await _download_chunks(file_url, tag)


async def _download_chunks(file_url: str, tag: str = "") -> bytes | None:
    """Скачивает файл чанками по 4МБ через HTTP Range-запросы."""
    import aiohttp

    CHUNK = 4 * 1024 * 1024  # 4МБ — ниже порога обрыва ~5МБ
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Узнаём размер файла и проверяем поддержку Range
        try:
            async with session.head(file_url) as resp:
                if resp.status != 200:
                    return None
                total = int(resp.headers.get("Content-Length", 0))
                accepts_range = resp.headers.get("Accept-Ranges", "") == "bytes"
        except Exception as e:
            log.warning("%s HEAD request failed: %s", tag, e)
            return None

        if not accepts_range or total <= CHUNK:
            log.warning("%s Range not supported or file too small (%d bytes)", tag, total)
            return None

        log.info("%s Chunked download: total=%d bytes, chunk=%d", tag, total, CHUNK)
        chunks = []
        offset = 0

        while offset < total:
            end = min(offset + CHUNK - 1, total - 1)
            try:
                async with session.get(
                    file_url,
                    headers={"Range": f"bytes={offset}-{end}"},
                ) as resp:
                    if resp.status not in (200, 206):
                        log.warning("%s Chunk %d-%d: status=%d", tag, offset, end, resp.status)
                        return None
                    chunk = await resp.read()
                    if not chunk:
                        log.warning("%s Chunk %d-%d: empty response", tag, offset, end)
                        return None
                    chunks.append(chunk)
                    log.info("%s Chunk %d-%d/%d OK (%d bytes)",
                                tag, offset, end, total, len(chunk))
            except Exception as e:
                log.warning("%s Chunk %d-%d failed: %s", tag, offset, end, e)
                return None

            offset = end + 1

    result = b"".join(chunks)
    log.info("%s Download complete: %d bytes", tag, len(result))
    return result

def _extract_media(msg: Message) -> tuple[str, str, str]:
    """Возвращает (media_type, file_id, filename)."""
    if msg.photo:
        return "photo", msg.photo[-1].file_id, "photo.jpg"
    if msg.video:
        return "video", msg.video.file_id, msg.video.file_name or "video.mp4"
    if msg.document:
        return "document", msg.document.file_id, msg.document.file_name or "file"
    if msg.voice:
        return "voice", msg.voice.file_id, "voice.ogg"
    if msg.audio:
        return "audio", msg.audio.file_id, msg.audio.file_name or "audio.mp3"
    return "", "", ""