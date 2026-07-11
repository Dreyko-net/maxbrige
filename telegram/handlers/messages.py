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

    # Скачиваем медиа с retry
    media_bytes = None
    for _dl_attempt in range(3):
        try:
            file = await bot.get_file(file_id)
            data = await bot.download_file(file.file_path)
            media_bytes = data.read() if hasattr(data, "read") else bytes(data)
            break
        except Exception as e:
            log.warning("[forwarded] Download media attempt %d failed: %s", _dl_attempt + 1, e)
            if _dl_attempt < 2:
                await asyncio.sleep(2 ** _dl_attempt)

    if not media_bytes:
        log.error("[forwarded] Download media failed after 3 attempts")
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

    # Скачиваем файл (с retry при сетевых ошибках)
    media_type, file_id, filename = _extract_media(msg)
    media_bytes = None
    for _dl_attempt in range(3):
        try:
            file = await bot.get_file(file_id)
            data = await bot.download_file(file.file_path)
            media_bytes = data.read() if hasattr(data, "read") else bytes(data)
            break
        except Exception as e:
            log.warning("Download TG media attempt %d failed: %s", _dl_attempt + 1, e)
            if _dl_attempt < 2:
                await asyncio.sleep(2 ** _dl_attempt)

    if not media_bytes:
        log.error("Download TG media failed after 3 attempts, putting text-only event")
        # Отправляем хотя бы текст в MAX через очередь
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