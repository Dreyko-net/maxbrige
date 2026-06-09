"""
Обработчик inline-кнопок.
Кнопка "📎 Загрузить файл" — скачивает медиа из MAX и отправляет в Telegram.
"""

from __future__ import annotations

import logging

from aiogram import Router, Bot
from aiogram.types import CallbackQuery

from bridge.manager import manager
from database import db

log = logging.getLogger(__name__)
router = Router()


@router.callback_query(lambda c: c.data and c.data.startswith("dl:"))
async def handle_download(callback: CallbackQuery, bot: Bot):
    """
    callback_data = "dl:{message_db_id}:{max_file_id}"
    """
    try:
        _, msg_db_id_str, max_file_id = callback.data.split(":", 2)
        msg_db_id = int(msg_db_id_str)
    except (ValueError, AttributeError):
        await callback.answer("❌ Неверный формат кнопки")
        return

    await callback.answer("⏳ Скачиваю файл…")

    # Найти сообщение и пользователя
    msg_row = await _get_message(msg_db_id)
    if not msg_row:
        await callback.message.reply("❌ Сообщение не найдено в БД")
        return

    user_id, tg_user_id = msg_row

    # Проверить есть ли уже в кэше (tg_file_id)
    media = await db.get_media(msg_db_id)
    if media and media.tg_file_id:
        # Уже скачано — переиспользуем Telegram file_id
        await _send_cached(bot, callback, media)
        return

    # Скачиваем из MAX
    client = manager.get_client(tg_user_id)
    if not client:
        await callback.message.reply("❌ MAX клиент не подключён")
        return

    data = await client.download_file(max_file_id)
    if not data:
        await callback.message.reply("❌ Не удалось скачать файл из MAX")
        return

    # Определяем тип из кэша или по умолчанию document
    file_type = media.file_type if media else "document"
    filename  = f"file_{max_file_id[:8]}"

    sent = await _send_bytes(bot, callback, data, file_type, filename)
    if not sent:
        return

    # Сохраняем tg_file_id для переиспользования
    tg_file_id = _extract_tg_file_id(sent, file_type)
    if media and tg_file_id:
        await db.update_tg_file_id(media.id, tg_file_id)
    elif tg_file_id:
        await db.save_media(
            message_id  = msg_db_id,
            file_type   = file_type,
            file_size   = len(data),
            max_file_id = max_file_id,
            tg_file_id  = tg_file_id,
        )


async def _send_cached(bot: Bot, callback: CallbackQuery, media):
    try:
        chat_id = callback.message.chat.id
        thread  = callback.message.message_thread_id
        if media.file_type == "photo":
            await bot.send_photo(chat_id, media.tg_file_id,
                                 message_thread_id=thread)
        elif media.file_type == "video":
            await bot.send_video(chat_id, media.tg_file_id,
                                 message_thread_id=thread)
        elif media.file_type == "voice":
            await bot.send_voice(chat_id, media.tg_file_id,
                                 message_thread_id=thread)
        else:
            await bot.send_document(chat_id, media.tg_file_id,
                                    message_thread_id=thread)
    except Exception as e:
        log.error("send_cached error: %s", e)


async def _send_bytes(bot: Bot, callback: CallbackQuery, data: bytes,
                      file_type: str, filename: str):
    from aiogram.types import BufferedInputFile
    chat_id = callback.message.chat.id
    thread  = callback.message.message_thread_id
    buf = BufferedInputFile(data, filename=filename)
    try:
        if file_type == "photo":
            return await bot.send_photo(chat_id, buf, message_thread_id=thread)
        elif file_type == "video":
            return await bot.send_video(chat_id, buf, message_thread_id=thread)
        elif file_type == "voice":
            return await bot.send_voice(chat_id, buf, message_thread_id=thread)
        else:
            return await bot.send_document(chat_id, buf, message_thread_id=thread)
    except Exception as e:
        log.error("send_bytes error: %s", e)
        return None


def _extract_tg_file_id(msg, file_type: str) -> str | None:
    if file_type == "photo" and msg.photo:
        return msg.photo[-1].file_id
    if file_type == "video" and msg.video:
        return msg.video.file_id
    if file_type == "voice" and msg.voice:
        return msg.voice.file_id
    if msg.document:
        return msg.document.file_id
    return None


async def _get_message(msg_db_id: int):
    import aiosqlite
    from config import DB_PATH
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            """SELECT m.user_id, u.tg_user_id
               FROM messages m
               JOIN users u ON u.id = m.user_id
               WHERE m.id = ?""",
            (msg_db_id,),
        ) as cur:
            return await cur.fetchone()
