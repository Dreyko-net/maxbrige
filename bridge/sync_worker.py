"""
SyncWorker — загрузка истории чатов MAX в темы Telegram.

Алгоритм:
1. Получить список чатов MAX
2. Для каждого чата создать тему в супергруппе (если нет)
3. Загрузить сообщения за сегодня → отправить в тему
4. Фоново загрузить остальные сообщения за N дней
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from database import db, User, Chat
from config import HISTORY_DAYS, FLOOD_SLEEP, CONTROL_TOPIC_NAME

if TYPE_CHECKING:
    from aiogram import Bot
    from bridge.manager import BridgeManager
    from bridge.max_client import MaxUserClient

log = logging.getLogger(__name__)

# Начало сегодняшнего дня в Unix ms
def _today_start_ms() -> int:
    d = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return int(d.timestamp() * 1000)

def _days_ago_ms(days: int) -> int:
    d = datetime.now() - timedelta(days=days)
    return int(d.timestamp() * 1000)

def _now_ms() -> int:
    return int(time.time() * 1000)


class SyncWorker:
    def __init__(self, bot: "Bot", manager: "BridgeManager"):
        self.bot     = bot
        self.manager = manager

    async def full_sync(self, user: User, client: "MaxUserClient"):
        """
        Полная синхронизация при первом подключении пользователя.
        Вызывается из auth handler после создания супергруппы.
        """
        tg_group_id = user.tg_group_id
        if not tg_group_id:
            log.error("No tg_group_id for user %s", user.tg_user_id)
            return

        await self._notify(tg_group_id, "📋 Получаю список чатов MAX…")

        # 1. Получить все чаты
        chats = await client.get_chats()
        if not chats:
            await self._notify(tg_group_id, "⚠️ Чаты не найдены.")
            return

        await self._notify(tg_group_id,
                           f"✅ Найдено чатов: {len(chats)}\n"
                           f"🗂 Создаю темы…")

        # 2. Создать темы для всех чатов
        db_chats: list[Chat] = []
        for max_chat in chats:
            chat_id    = str(getattr(max_chat, "id",    "") or "")
            chat_title = str(getattr(max_chat, "title", "") or
                             getattr(max_chat, "name",  "") or "Без названия")
            if not chat_id:
                continue

            db_chat = await db.upsert_chat(user.id, chat_id, chat_title)

            if not db_chat.tg_topic_id:
                topic_id = await self._create_topic(tg_group_id, chat_title)
                if topic_id:
                    await db.set_topic_id(db_chat.id, topic_id)
                    db_chat.tg_topic_id = topic_id
                await asyncio.sleep(0.5)  # не спамим Telegram API

            db_chats.append(db_chat)

        await self._notify(tg_group_id,
                           f"✅ Темы созданы.\n"
                           f"📥 Загружаю сообщения за сегодня…")

        # 3. Сначала — сообщения за сегодня
        today_start = _today_start_ms()
        now         = _now_ms()
        for db_chat in db_chats:
            await self._sync_chat(
                user, client, db_chat,
                from_ts=today_start, to_ts=now,
            )

        await self._notify(tg_group_id,
                           f"✅ Сообщения за сегодня загружены.\n"
                           f"📦 Загружаю историю за {HISTORY_DAYS} дней в фоне…")

        # 4. Остальная история — в фоне
        asyncio.create_task(
            self._sync_history_background(user, client, db_chats, today_start)
        )

    async def _sync_history_background(
        self,
        user: User,
        client: "MaxUserClient",
        db_chats: list[Chat],
        before_ts: int,
    ):
        """Загружает историю старше today_start в фоновом режиме."""
        from_ts = _days_ago_ms(HISTORY_DAYS)
        try:
            for db_chat in db_chats:
                await self._sync_chat(
                    user, client, db_chat,
                    from_ts=from_ts, to_ts=before_ts,
                )
                await asyncio.sleep(1)

            await self._notify(
                user.tg_group_id,
                f"✅ История за {HISTORY_DAYS} дней загружена полностью.",
            )
        except Exception as e:
            log.error("Background sync error for user %s: %s", user.tg_user_id, e)

    async def _sync_chat(
        self,
        user: User,
        client: "MaxUserClient",
        db_chat: Chat,
        from_ts: int,
        to_ts: int,
    ):
        if not db_chat.tg_topic_id:
            return

        messages = await client.get_history(
            max_chat_id = db_chat.max_chat_id,
            from_ts     = from_ts,
            to_ts       = to_ts,
            limit       = 200,
        )
        if not messages:
            return

        log.info("Syncing %d messages for chat %s (user %s)",
                 len(messages), db_chat.max_chat_id, user.tg_user_id)

        for msg in messages:
            try:
                await self._forward_msg_to_tg(user, client, db_chat, msg)
                await asyncio.sleep(FLOOD_SLEEP)
            except Exception as e:
                log.error("Sync message error: %s", e)

        await db.set_chat_synced(db_chat.id)

    async def _forward_msg_to_tg(
        self,
        user: User,
        client: "MaxUserClient",
        db_chat: Chat,
        msg,
    ):
        from telegram.sender import format_history_message, send_to_telegram_topic
        from bridge.queue import BridgeEvent
        from bridge.max_client import _detect_media

        text      = getattr(msg, "text",      "") or ""
        msg_id    = str(getattr(msg, "id",    "") or "")
        timestamp = getattr(msg, "timestamp", _now_ms())
        sender    = getattr(msg, "sender",    None)
        sender_name = ""
        if sender:
            sender_name = (getattr(sender, "name", "") or
                           getattr(sender, "username", "") or "")

        has_media, media_type = _detect_media(msg)

        formatted = format_history_message(
            sender_name = sender_name,
            text        = text,
            timestamp   = timestamp,
            has_media   = has_media,
            media_type  = media_type,
        )

        sent_msg = await send_to_telegram_topic(
            bot         = self.bot,
            group_id    = user.tg_group_id,
            topic_id    = db_chat.tg_topic_id,
            text        = formatted,
        )

        if sent_msg:
            msg_db_id = await db.save_message(
                user_id    = user.id,
                chat_id    = db_chat.id,
                direction  = "max_to_tg",
                timestamp  = timestamp,
                max_msg_id = msg_id,
                tg_msg_id  = sent_msg.message_id,
                has_media  = has_media,
            )
            # Кнопка "📎 Загрузить" для медиа
            if has_media and msg_db_id:
                from telegram.keyboards import media_download_kb
                await self.bot.edit_message_reply_markup(
                    chat_id      = user.tg_group_id,
                    message_id   = sent_msg.message_id,
                    reply_markup = media_download_kb(msg_db_id, msg_id),
                )

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _create_topic(self, group_id: int, name: str) -> int | None:
        try:
            result = await self.bot.create_forum_topic(
                chat_id    = group_id,
                name       = name[:128],  # Telegram limit
            )
            return result.message_thread_id
        except Exception as e:
            log.error("create_forum_topic error: %s", e)
            return None

    async def _notify(self, group_id: int, text: str):
        """Отправляет сообщение в General тему супергруппы."""
        try:
            await self.bot.send_message(chat_id=group_id, text=text)
        except Exception as e:
            log.error("notify error: %s", e)
