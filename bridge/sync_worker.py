"""
SyncWorker — загрузка истории чатов MAX в темы Telegram.

Алгоритм:
1. Получить все чаты через fetch_chats()
2. Создать тему в супергруппе для каждого чата
3. Загрузить сообщения за сегодня → отправить в тему
4. Фоново загрузить остальные за N дней
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from aiogram.exceptions import TelegramRetryAfter,TelegramBadRequest
from database import db, User, Chat
from config import HISTORY_DAYS, FLOOD_SLEEP, CONTROL_TOPIC_NAME

if TYPE_CHECKING:
    from aiogram import Bot
    from bridge.manager import BridgeManager, manager
    from bridge.max_client import MaxUserClient

log = logging.getLogger(__name__)


def _today_start_ms() -> int:
    d = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return int(d.timestamp() * 1000)

def _days_ago_ms(days: int) -> int:
    return int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

def _now_ms() -> int:
    return int(time.time() * 1000)


class SyncWorker:
    def __init__(self, bot: "Bot", manager: "BridgeManager"):
        self.bot     = bot
        self.manager = manager

    async def full_sync(self, user: User, client: "MaxUserClient"):
        tg_group_id = user.tg_group_id
        if not tg_group_id:
            log.error("No tg_group_id for user %s", user.tg_user_id)
            return

        await self._notify(tg_group_id, "📋 Получаю список чатов MAX…")

        # 1. Получаем чаты
        chats = await client.get_chats()
        if not chats:
            await self._notify(tg_group_id, "⚠️ Чаты не найдены или список пуст.")
            return

        await self._notify(tg_group_id, f"✅ Найдено чатов: {len(chats)}\n🗂 Создаю темы…")

        # 2. Создаём темы
        db_chats: list[Chat] = []
        for max_chat in chats:
            chat_id    = _chat_id(max_chat)
            chat_title = await _chat_title(max_chat, client)
            if not chat_id:
                continue

            db_chat = await db.upsert_chat(user.id, chat_id, chat_title)
            #Проверка существует ли топик/тема. Если нет, то надо создать снова. Если есть, но другое имя, то переименовываем.
            test_topic = None
            if db_chat.tg_topic_id:
                test_topic = await self.test_topic(tg_group_id, db_chat.tg_topic_id, chat_title)
                #Если False то значить не существует, None скорее всего сетевая ошибка
                if test_topic == None:
                    log.warning("test_exist_forum_topic Проверка топика выдала ошибку. chat_title: %s, user.id: %s,db_chat.tg_topic_id: %s", chat_title, user.id, db_chat.tg_topic_id)
                    test_topic = True
            if not test_topic:
                db_delete = await db.delete_topic_id(user.id, chat_id, db_chat.tg_topic_id)
                db_chat = await db.upsert_chat(user.id, chat_id, chat_title)

            if not db_chat.tg_topic_id:
                topic_id = await self._create_topic(tg_group_id, chat_title)
                if topic_id:
                    await db.set_topic_id(db_chat.id, topic_id)
                    db_chat.tg_topic_id = topic_id
                await asyncio.sleep(2)

            db_chats.append(db_chat)

        await self._notify(tg_group_id,
                           f"✅ Темы созданы.\n📥 Загружаю сообщения за сегодня…")

        # 3. Сначала — сегодня
        today_start = _today_start_ms()
        now         = _now_ms()
        for db_chat in db_chats:
            await self._sync_chat(user, client, db_chat, from_ts=today_start, to_ts=now)

        await self._notify(tg_group_id,
                           f"✅ Сообщения за сегодня загружены.\n"
                           f"📦 Загружаю историю за {HISTORY_DAYS} дней в фоне…")

        # 4. Остальная история — в фоне
        asyncio.create_task(
            self._sync_history_background(user, client, db_chats, today_start)
        )

    async def _sync_history_background(self, user, client, db_chats, before_ts):
        from_ts = _days_ago_ms(HISTORY_DAYS)
        try:
            for db_chat in db_chats:
                await self._sync_chat(user, client, db_chat,
                                      from_ts=from_ts, to_ts=before_ts)
                await asyncio.sleep(1)
            await self._notify(user.tg_group_id,
                               f"✅ История за {HISTORY_DAYS} дней загружена.")
        except Exception as e:
            log.error("Background sync error for user %s: %s", user.tg_user_id, e)

    async def _sync_chat(self, user, client, db_chat: Chat, from_ts, to_ts):
        if not db_chat.tg_topic_id:
            return

        messages = await client.get_history(
            max_chat_id = db_chat.max_chat_id,
            from_ts     = from_ts,
            to_ts       = to_ts,
            limit       = 100,
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

    async def _forward_msg_to_tg(self, user, client, db_chat: Chat, msg):
        from telegram.sender import format_history_message, send_to_telegram_topic
        from bridge.max_client import _detect_media

        text        = getattr(msg, "text",      "") or ""
        msg_id      = str(getattr(msg, "id",    "") or "")
        timestamp   = getattr(msg, "time", _now_ms())
        sender      = getattr(msg, "sender",    None)
        sender_name = await client.get_client(sender) if sender else ""
        has_media, media_type = _detect_media(msg)

        formatted = format_history_message(
            sender_name = sender_name,
            text        = text,
            timestamp   = timestamp,
            has_media   = has_media,
            media_type  = media_type,
        )

        sent_msg = await send_to_telegram_topic(
            bot      = self.bot,
            group_id = user.tg_group_id,
            topic_id = db_chat.tg_topic_id,
            text     = formatted,
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
            if has_media and msg_db_id:
                from telegram.keyboards import media_download_kb
                try:
                    await self.bot.edit_message_reply_markup(
                        chat_id      = user.tg_group_id,
                        message_id   = sent_msg.message_id,
                        reply_markup = media_download_kb(msg_db_id, msg_id),
                    )
                except Exception:
                    pass

    async def _create_topic(self, group_id: int, name: str) -> int | None:
        for attempt in range(5):
            try:
                result = await self.bot.create_forum_topic(
                    chat_id = group_id,
                    name    = name[:128],
                )
                return result.message_thread_id
            except TelegramRetryAfter as e:
                wait = e.retry_after + 1
                log.warning("create_forum_topic flood, waiting %ds (attempt %d)",
                            wait, attempt + 1)
                await asyncio.sleep(wait)
            except Exception as e:
                log.error("create_forum_topic error: %s", e)
                return None
        log.error("create_forum_topic: too many retries for '%s'", name)
        return None


    async def test_topic(self, group_id: int, tg_topic_id: int, name: str) -> int | None:
        for attempt in range(5):
            try:
                result = await self.bot.edit_forum_topic(
                            chat_id = group_id,
                            message_thread_id = tg_topic_id,
                            name    = name[:128],
                            )#icon_custom_emoji_id 
                return True
            except TelegramRetryAfter as e:
                wait = e.retry_after + 1
                log.warning("test_exist_forum_topic flood, waiting %ds (attempt %d)",
                            wait, attempt + 1)
                await asyncio.sleep(wait)
            except TelegramBadRequest as e:
                # Тут можно логировать e.description для точной диагностики
                description = getattr(e, "description",    None) or getattr(e, "message", "?")
                if 'TOPIC_ID_INVALID' in description:
                    log.warning("test_exist_forum_topic Топик не найден, возможно удалён: %s", e)
                    await asyncio.sleep(0,5)
                    return False
                if 'TOPIC_NOT_MODIFIED' in description:
                    log.warning("test_exist_forum_topic Топик найден, изменения не требуются: %s", e)
                    await asyncio.sleep(0,5)
                    return True
                log.error("test_exist_forum_topic error: %s", e)
                return False
            except Exception as e:
                log.error("test_exist_forum_topic error: %s", e)
                return None
        log.error("test_exist_forum_topic: too many retries for '%s'", group_id)
        return None

    async def _notify(self, group_id: int, text: str):
        for attempt in range(3):
            try:
                await self.bot.send_message(chat_id=group_id, text=text)
                return
            except TelegramRetryAfter as e:
                wait = e.retry_after + 1
                log.warning("notify flood, waiting %ds", wait)
                await asyncio.sleep(wait)
            except Exception as e:
                log.error("notify error: %s", e)
                return


# ── Helpers ───────────────────────────────────────────────────────────────────

def _chat_id(chat) -> str:
    """Извлекает ID чата из объекта pymax Chat."""
    for attr in ("id", "chat_id", "peer_id"):
        val = getattr(chat, attr, None)
        if val is not None:
            return str(val)
    return ""

async def _chat_title(chat, client) -> str:
    """Извлекает название чата."""
    #Находим сервисные чаты и ботов
    if getattr(chat, 'has_bots', False):
        if (getattr(chat, 'options', None) or {}).get('SERVICE_CHAT', False):
            ctitle = 'MAX service Chat'
            return ctitle
        else:
            max_bot_id = next(user_id for user_id in getattr(chat, 'participants',    '?') if user_id != getattr(chat, 'owner',    '?'))
            # ctitle = f"MAX Bot: {next(user_id for user_id in getattr(chat, 'participants',    '?') if user_id != getattr(chat, 'owner',    '?'))}" 
            max_bot_name = await client._client.get_user(max_bot_id)
            max_bot_firstname = getattr(max_bot_name.names[0], 'first_name',    '?') if max_bot_name else 'max_bot_id'
            ctitle = f"MAX Bot: {max_bot_firstname}"
            return " ".join(str(ctitle).split())
    if getattr(chat, "type", "?") == 'DIALOG' and not getattr(chat, 'has_bots', False) and getattr(chat, "id",    None) == 0:
        ctitle = 'MAX Избранное'
        return ctitle

    
    # Групповые чаты — title или name
    for attr in ("title", "name"):
        val = getattr(chat, attr, None)
        if val:
            return str(val)

    # Личные диалоги — participants   
    if getattr(chat, "type", "?") == 'DIALOG' and not getattr(chat, 'has_bots', False) and getattr(chat, "id",    None) != 0:
        # Пропускаем себя если есть me_id
        participant = next(user_id for user_id in getattr(chat, 'participants',    None) if user_id != getattr(chat, 'owner',    '?'))
        if participant:
            try: 
                #user = await client._client.fetch_users([participant])
                user = await client._client.get_user(participant)
                names = getattr(user, "names", None)
                if names:
                    try:
                        name = f"{getattr(names[0], 'first_name', '')} {getattr(names[0], 'last_name', '')}"
                        if name:
                            return " ".join(str(name).split())
                    except (IndexError, StopIteration, KeyError):
                        pass
                for attr in ("name", "first_name", "username"):
                    val = getattr(participant, attr, None)
                    if val:
                        return str(val)
            except Exception:
                pass

    # Последний вариант — ID чата
    cid = getattr(chat, "id", None) or getattr(chat, "chat_id", "")
    return f"Диалог {cid}" if cid else "Без названия"
