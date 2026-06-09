"""
BridgeManager — центральный координатор.

- Хранит пул MaxUserClient (один на пользователя)
- При старте восстанавливает сессии из БД
- Запускает воркеры очередей max→tg и tg→max
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, TYPE_CHECKING

from database import db, User
from bridge.max_client import MaxUserClient, session_path_for
from bridge.queue import BridgeEvent, max_to_tg_queue, tg_to_max_queue
from bridge.sync_worker import SyncWorker

if TYPE_CHECKING:
    from aiogram import Bot

log = logging.getLogger(__name__)


class BridgeManager:
    def __init__(self):
        # tg_user_id → MaxUserClient
        self._clients:  dict[int, MaxUserClient] = {}
        self._tasks:    list[asyncio.Task]        = []
        self._bot:      Optional["Bot"]           = None
        self._sync:     Optional[SyncWorker]      = None

    def set_bot(self, bot: "Bot"):
        self._bot = bot
        if self._sync:
            self._sync.bot = bot

    # ── Жизненный цикл ───────────────────────────────────────────────────────

    async def start(self, bot: "Bot"):
        self.set_bot(bot)
        self._sync = SyncWorker(bot=bot, manager=self)

        # Восстанавливаем сессии всех активных пользователей
        users = await db.get_active_users()
        log.info("Restoring %d user sessions…", len(users))
        for user in users:
            await self._restore_client(user)

        # Воркеры очередей
        self._tasks.append(asyncio.create_task(self._worker_max_to_tg()))
        self._tasks.append(asyncio.create_task(self._worker_tg_to_max()))
        self._tasks.append(asyncio.create_task(self._purge_media_loop()))
        log.info("BridgeManager started.")

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        for client in self._clients.values():
            await client.stop()

    # ── Подключение нового пользователя ──────────────────────────────────────

    async def connect_user(
        self,
        tg_user_id: int,
        max_phone: str,
        sms_code_provider,
    ) -> MaxUserClient:
        """
        Создаёт и запускает клиент для нового пользователя.
        sms_code_provider — объект, реализующий SmsCodeProvider.
        Возвращает клиент после успешной авторизации.
        """
        path = session_path_for(tg_user_id)

        client = MaxUserClient(
            tg_user_id       = tg_user_id,
            max_phone        = max_phone,
            session_path     = path,
            sms_code_provider = sms_code_provider,
        )
        await client.start()
        self._clients[tg_user_id] = client

        # Запускаем run_forever в фоне
        task = asyncio.create_task(client.run())
        self._tasks.append(task)

        return client

    async def _restore_client(self, user: User):
        """Восстанавливает сессию существующего пользователя (без SMS)."""
        try:
            client = MaxUserClient(
                tg_user_id   = user.tg_user_id,
                max_phone    = user.max_phone,
                session_path = user.session_path,
            )
            await client.start()
            self._clients[user.tg_user_id] = client
            task = asyncio.create_task(client.run())
            self._tasks.append(task)
            log.info("Session restored for user %s", user.tg_user_id)
        except Exception as e:
            log.error("Failed to restore session for user %s: %s",
                      user.tg_user_id, e)

    def get_client(self, tg_user_id: int) -> Optional[MaxUserClient]:
        return self._clients.get(tg_user_id)

    # ── Воркер MAX → Telegram ─────────────────────────────────────────────────

    async def _worker_max_to_tg(self):
        log.info("Worker max→tg started")
        while True:
            try:
                event = await max_to_tg_queue.get()
                await self._handle_max_to_tg(event)
                max_to_tg_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("max→tg worker error: %s", e)

    async def _handle_max_to_tg(self, event: BridgeEvent):
        from telegram.sender import send_to_telegram
        user = await db.get_user(event.tg_user_id)
        if not user or not user.tg_group_id:
            return

        chat = await db.get_chat_by_max(user.id, event.max_chat_id)
        if not chat or not chat.tg_topic_id:
            log.warning("No topic for max_chat_id=%s user=%s",
                        event.max_chat_id, event.tg_user_id)
            return

        await send_to_telegram(
            bot        = self._bot,
            event      = event,
            user       = user,
            chat       = chat,
            max_client = self.get_client(event.tg_user_id),
        )

    # ── Воркер Telegram → MAX ─────────────────────────────────────────────────

    async def _worker_tg_to_max(self):
        log.info("Worker tg→max started")
        while True:
            try:
                event = await tg_to_max_queue.get()
                await self._handle_tg_to_max(event)
                tg_to_max_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("tg→max worker error: %s", e)

    async def _handle_tg_to_max(self, event: BridgeEvent):
        client = self.get_client(event.tg_user_id)
        if not client:
            log.warning("No MAX client for user %s", event.tg_user_id)
            return

        if event.has_media and event.media_bytes:
            await client.send_file(
                max_chat_id = event.max_chat_id,
                data        = event.media_bytes,
                filename    = event.media_name or "file",
                caption     = event.text,
            )
        elif event.text:
            await client.send_message(
                max_chat_id = event.max_chat_id,
                text        = event.text,
            )

    # ── Очистка медиакэша ─────────────────────────────────────────────────────

    async def _purge_media_loop(self):
        """Каждые 30 минут удаляет просроченные записи медиакэша."""
        while True:
            try:
                await asyncio.sleep(1800)
                deleted = await db.purge_expired_media()
                if deleted:
                    log.info("Purged %d expired media cache entries", deleted)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("Media purge error: %s", e)


# Глобальный экземпляр
manager = BridgeManager()
