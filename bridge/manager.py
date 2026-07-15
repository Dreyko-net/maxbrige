"""
BridgeManager — центральный координатор.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, TYPE_CHECKING

from database import db, User
from bridge.max_client import MaxUserClient, session_path_for
from config import SESSIONS_DIR
from bridge.queue import BridgeEvent, max_to_tg_queue, tg_to_max_queue
from bridge.sync_worker import SyncWorker

if TYPE_CHECKING:
    from aiogram import Bot

log = logging.getLogger(__name__)


class BridgeManager:
    def __init__(self):
        self._clients:  dict[int, MaxUserClient] = {}
        self._tasks:    list[asyncio.Task]        = []
        self._bot:      Optional["Bot"]           = None
        self._sync:     Optional[SyncWorker]      = None

    def set_bot(self, bot: "Bot"):
        self._bot = bot
        if self._sync:
            self._sync.bot = bot

    async def start(self, bot: "Bot"):
        self.set_bot(bot)
        self._sync = SyncWorker(bot=bot, manager=self)

        users = await db.get_active_users()
        log.info("Restoring %d user sessions…", len(users))
        for user in users:
            await self._restore_client(user)

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
        password_provider,
    ) -> MaxUserClient:
        path = session_path_for(tg_user_id)
        log.info("[user=%s] connect_user started, path=%s", tg_user_id, path)

        client = MaxUserClient(
            tg_user_id        = tg_user_id,
            max_phone         = max_phone,
            session_path      = path,
            sms_code_provider = sms_code_provider,
            password_provider = password_provider,
        )

        client._on_session_revoked = self._on_session_revoked
        log.info("[user=%s] calling client.start()", tg_user_id)
        await client.start()
        log.info("[user=%s] client.start() done, me=%s", tg_user_id, client.me)

        self._clients[tg_user_id] = client
        return client

    async def _run_client(self, client: MaxUserClient):
        """Запускает run_forever с логированием ошибок."""
        try:
            await client.run()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("[user=%s] MAX client crashed: %s", client.tg_user_id, e,
                      exc_info=True)

    async def _restore_client(self, user: User):
        try:
            client = MaxUserClient(
                tg_user_id   = user.tg_user_id,
                max_phone    = user.max_phone,
                session_path = user.session_path,
            )
            client._on_session_revoked = self._on_session_revoked
            await client.start()
            self._clients[user.tg_user_id] = client
            log.info("Session restored for user %s", user.tg_user_id)
        except (TimeoutError, asyncio.TimeoutError, ConnectionError) as e:
            log.error("Session restore failed for user %s: %s — treating as revoked",
                      user.tg_user_id, e)
            asyncio.create_task(self._on_session_revoked(user.tg_user_id))
        except Exception as e:
            log.error("Failed to restore session for user %s: %s",
                      user.tg_user_id, e, exc_info=True)

    def get_client(self, tg_user_id: int) -> Optional[MaxUserClient]:
        return self._clients.get(tg_user_id)

    async def _on_session_revoked(self, tg_user_id: int):
        """Сессия MAX сброшена — чистим всё и просим повторную авторизацию."""
        log.warning("[user=%s] session revoked, cleaning up", tg_user_id)

        # Останавливаем и удаляем клиент из пула
        client = self._clients.pop(tg_user_id, None)
        if client:
            await client.stop()

        # Удаляем файл сессии pymax чтобы не пытался войти по старому токену
        import os, glob
        session_pattern = str(SESSIONS_DIR / f"user_{tg_user_id}" / "session.db")
        for f in glob.glob(session_pattern):
            try:
                os.remove(f)
                log.info("[user=%s] removed session file: %s", tg_user_id, f)
            except Exception as e:
                log.error("[user=%s] failed to remove session file: %s", tg_user_id, e)

        # Сбрасываем статус в БД — пользователь должен пройти авторизацию заново
        import aiosqlite
        from config import DB_PATH
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "UPDATE users SET status='pending' WHERE tg_user_id=?",
                (tg_user_id,)
            )
            await conn.commit()
        log.info("[user=%s] user status reset to pending", tg_user_id)

        # Уведомляем пользователя
        if self._bot:
            try:
                msg = (
                    "<b>Сессия MAX сброшена.</b> "
                    "MAX разлогинил аккаунт. "
                    "Пройдите авторизацию заново: /start"
                )
                await self._bot.send_message(tg_user_id, msg, parse_mode="HTML")
            except Exception as e:
                log.error("[user=%s] notify failed: %s", tg_user_id, e)

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
                log.error("max→tg worker error: %s", e, exc_info=True)

    async def _handle_max_to_tg(self, event: BridgeEvent):
        from telegram.sender import (
            send_to_telegram,
            format_live_message,
            send_text_to_topic,
        )
        from aiogram.types import BufferedInputFile
        from telegram.sender import _send_with_retry

        user = await db.get_user(event.tg_user_id)
        if not user or not user.tg_group_id:
            return

        chat = await db.get_chat_by_max(user.id, event.max_chat_id)
        if not chat or not chat.tg_topic_id:
            log.warning("No topic for max_chat_id=%s user=%s",
                        event.max_chat_id, event.tg_user_id)
            return

        # Если медиа скачано — отправляем реальным медиа-методом
        if event.has_media and event.media_bytes:
            max_client = self.get_client(event.tg_user_id)
            sender_name = ""
            if max_client and event.max_sender_id:
                try:
                    sender_name = await max_client.get_client(event.max_sender_id) or ""
                except Exception:
                    pass

            caption = format_live_message(
                sender_name = sender_name,
                text        = event.text,
                has_media   = False,  # медиа реальное, плейсхолдер не нужен
                media_type  = event.media_type,
            )

            filename = event.media_name or "file"
            buf = BufferedInputFile(event.media_bytes, filename=filename)
            atype = event.media_type or "document"

            sent = None
            try:
                if atype == "photo":
                    sent = await _send_with_retry(
                        self._bot.send_photo,
                        chat_id=user.tg_group_id,
                        message_thread_id=chat.tg_topic_id,
                        photo=buf,
                        caption=caption[:1024] if caption else None,
                        parse_mode="HTML",
                    )
                elif atype == "video":
                    sent = await _send_with_retry(
                        self._bot.send_video,
                        chat_id=user.tg_group_id,
                        message_thread_id=chat.tg_topic_id,
                        video=buf,
                        caption=caption[:1024] if caption else None,
                        parse_mode="HTML",
                    )
                elif atype == "voice":
                    sent = await _send_with_retry(
                        self._bot.send_voice,
                        chat_id=user.tg_group_id,
                        message_thread_id=chat.tg_topic_id,
                        voice=buf,
                        caption=caption[:1024] if caption else None,
                        parse_mode="HTML",
                    )
                elif atype == "audio":
                    sent = await _send_with_retry(
                        self._bot.send_audio,
                        chat_id=user.tg_group_id,
                        message_thread_id=chat.tg_topic_id,
                        audio=buf,
                        caption=caption[:1024] if caption else None,
                        parse_mode="HTML",
                    )
                elif atype == "sticker":
                    sent = await _send_with_retry(
                        self._bot.send_document,
                        chat_id=user.tg_group_id,
                        message_thread_id=chat.tg_topic_id,
                        document=buf,
                        caption=caption[:1024] if caption else None,
                        parse_mode="HTML",
                    )
                else:
                    sent = await _send_with_retry(
                        self._bot.send_document,
                        chat_id=user.tg_group_id,
                        message_thread_id=chat.tg_topic_id,
                        document=buf,
                        caption=caption[:1024] if caption else None,
                        parse_mode="HTML",
                    )
            except Exception as e:
                log.error("Live media send error (type=%s): %s", atype, e)
                # Фоллбэк — текст
                await send_to_telegram(
                    bot=self._bot, event=event, user=user, chat=chat,
                    max_client=self.get_client(event.tg_user_id),
                )
                return

            if sent:
                await db.save_message(
                    user_id=user.id, chat_id=chat.id,
                    direction="max_to_tg", timestamp=event.timestamp,
                    max_sender_id=event.max_sender_id,
                    max_msg_id=event.max_msg_id,
                    tg_msg_id=sent.message_id,
                    has_media=event.has_media,
                )
        else:
            # Без медиа (или не удалось скачать) — текстовый fallback
            await send_to_telegram(
                bot        = self._bot,
                event      = event,
                user       = user,
                chat       = chat,
                max_client = self.get_client(event.tg_user_id)
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
                log.error("tg→max worker error: %s", e, exc_info=True)

    async def _handle_tg_to_max(self, event: BridgeEvent):
        client = self.get_client(event.tg_user_id)
        if not client:
            log.warning("No MAX client for user %s", event.tg_user_id)
            return

        if event.has_media and event.media_bytes:
            if event.media_type == "photo":
                await client.send_photo(
                    max_chat_id = event.max_chat_id,
                    data        = event.media_bytes,
                    caption     = event.text,
                )
            elif event.media_type == "video":
                await client.send_video(
                    max_chat_id = event.max_chat_id,
                    data        = event.media_bytes,
                    filename    = event.media_name or "video.mp4",
                    caption     = event.text,
                )
            else:
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