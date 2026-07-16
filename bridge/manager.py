"""
BridgeManager — центральный координатор.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional, TYPE_CHECKING

from database import db, User
from bridge.max_client import MaxUserClient, session_path_for
from config import (
    SESSIONS_DIR,
    TG_MAX_FILE_SIZE,
    FILES_DIR,
    FILES_URL_BASE,
    FILES_MAX_AGE_DAYS,
)
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
        self._tasks.append(asyncio.create_task(self._cleanup_old_files()))
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
        import glob
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
            _send_with_retry,
        )
        from aiogram.types import BufferedInputFile

        user = await db.get_user(event.tg_user_id)
        if not user or not user.tg_group_id:
            return

        chat = await db.get_chat_by_max(user.id, event.max_chat_id)
        if not chat or not chat.tg_topic_id:
            log.warning("No topic for max_chat_id=%s user=%s",
                        event.max_chat_id, event.tg_user_id)
            return

        # ── Альбом: несколько фото/видео в одном сообщении ──
        if event.media_group:
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
                has_media   = False,
                media_type  = None,
            )

            await self._send_media_group_to_tg(
                bot=self._bot,
                group_id=user.tg_group_id,
                topic_id=chat.tg_topic_id,
                caption=caption,
                user=user,
                chat=chat,
                event=event,
            )
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
            atype = event.media_type or "document"
            data = event.media_bytes
            data_size = len(data)

            # Файл превышает лимит Telegram — сохраняем на диск и отправляем ссылку
            if data_size > TG_MAX_FILE_SIZE:
                log.info("File too large (%.1f MB > %d MB), saving to disk",
                         data_size / (1024*1024), TG_MAX_FILE_SIZE // (1024*1024))
                await self._send_large_file_as_link(
                    bot=self._bot,
                    group_id=user.tg_group_id,
                    topic_id=chat.tg_topic_id,
                    data=data,
                    filename=filename,
                    caption=caption,
                    atype=atype,
                    user=user,
                    chat=chat,
                    event=event,
                )
                return

            buf = BufferedInputFile(data, filename=filename)

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

    async def _send_media_group_to_tg(self, bot, group_id: int, topic_id: int,
                                       caption: str, user, chat, event: BridgeEvent):
        """Отправляет группу фото/видео как альбом (send_media_group).

        Файлы > TG_MAX_FILE_SIZE извлекаются и отправляются отдельными ссылками.
        При ошибке send_media_group — фоллбэк на поодиночную отправку.
        """
        from aiogram.types import BufferedInputFile, InputMediaPhoto, InputMediaVideo
        from aiogram.exceptions import TelegramRetryAfter, TelegramNetworkError, TelegramBadRequest
        from telegram.sender import send_text_to_topic, _send_with_retry

        group_items = []   # Для send_media_group
        group_sources = [] # Параллельный список исходных item dicts
        large_items = []   # Для отдельных ссылок

        is_first = True
        for item in event.media_group:
            data = item["bytes"]
            filename = item["filename"]
            mtype = item["type"]

            if len(data) > TG_MAX_FILE_SIZE:
                large_items.append(item)
                continue

            buf = BufferedInputFile(data, filename=filename)
            cap = caption[:1024] if (is_first and caption) else None
            pm = "HTML" if cap else None
            if mtype == "photo":
                group_items.append(InputMediaPhoto(media=buf, caption=cap, parse_mode=pm))
            else:
                group_items.append(InputMediaVideo(media=buf, caption=cap, parse_mode=pm))
            group_sources.append(item)
            is_first = False

        first_tg_msg_id = None
        album_failed = False

        # Отправляем альбом
        if group_items:
            for attempt in range(3):
                try:
                    messages = await bot.send_media_group(
                        chat_id=group_id,
                        message_thread_id=topic_id,
                        media=group_items,
                    )
                    if messages:
                        first_tg_msg_id = messages[0].message_id
                    break
                except TelegramRetryAfter as e:
                    wait = e.retry_after + 1
                    log.warning("send_media_group retry (flood), waiting %ds (attempt %d)",
                                wait, attempt + 1)
                    await asyncio.sleep(wait)
                except TelegramNetworkError as e:
                    wait = 2 ** attempt + 1
                    log.warning("send_media_group retry (network: %s), waiting %ds (attempt %d)",
                                type(e).__name__, wait, attempt + 1)
                    await asyncio.sleep(wait)
                except TelegramBadRequest as e:
                    log.warning("send_media_group bad request: %s — falling back to individual send", e)
                    album_failed = True
                    break
                except Exception as e:
                    log.error("send_media_group error: %s — falling back to individual send", e)
                    album_failed = True
                    break

        # Фоллбэк: если альбом не удался — отправляем каждое медиа по отдельности
        if album_failed and group_items:
            log.info("Sending %d media items individually (album fallback)", len(group_items))
            for i, (gm_item, src_item) in enumerate(zip(group_items, group_sources)):
                data = src_item["bytes"]
                filename = src_item["filename"]
                mtype = src_item["type"]
                buf = BufferedInputFile(data, filename=filename)
                # Caption — только к первому
                cap = caption[:1024] if (i == 0 and caption) else None
                sent = None
                try:
                    if mtype == "photo":
                        sent = await _send_with_retry(
                            bot.send_photo,
                            chat_id=group_id, message_thread_id=topic_id,
                            photo=buf, caption=cap, parse_mode="HTML" if cap else None,
                        )
                    else:
                        sent = await _send_with_retry(
                            bot.send_video,
                            chat_id=group_id, message_thread_id=topic_id,
                            video=buf, caption=cap, parse_mode="HTML" if cap else None,
                        )
                except Exception as e:
                    log.warning("individual send failed for %s: %s — skipping", filename, e)
                if sent and not first_tg_msg_id:
                    first_tg_msg_id = sent.message_id

        # Отправляем большие файлы как ссылки
        link_caption = "" if first_tg_msg_id else caption
        for item in large_items:
            await self._send_large_file_as_link(
                bot=bot,
                group_id=group_id,
                topic_id=topic_id,
                data=item["bytes"],
                filename=item["filename"],
                caption=link_caption,
                atype=item["type"],
                user=user,
                chat=chat,
                event=event,
            )
            link_caption = ""  # только первый получает caption

        # Сохраняем в БД
        if first_tg_msg_id:
            await db.save_message(
                user_id=user.id, chat_id=chat.id,
                direction="max_to_tg", timestamp=event.timestamp,
                max_sender_id=event.max_sender_id,
                max_msg_id=event.max_msg_id,
                tg_msg_id=first_tg_msg_id,
                has_media=True,
            )
        elif not group_items and not large_items:
            # Ничего не отправилось — хотя бы текст
            await send_text_to_topic(bot, group_id, topic_id, caption)

    async def _send_large_file_as_link(self, bot, group_id: int, topic_id: int,
                                        data: bytes, filename: str, caption: str,
                                        atype: str, user, chat, event: BridgeEvent):
        """Сохраняет большой файл на диск и отправляет в чат ссылку для скачивания."""
        import time as _time

        # Формируем имя файла: <timestamp>.<extension>
        _, ext = os.path.splitext(filename)
        if not ext:
            ext = ".mp4" if atype == "video" else ".bin"
        saved_name = f"{int(_time.time())}{ext}"
        saved_path = FILES_DIR / saved_name

        # Сохраняем на диск
        try:
            saved_path.write_bytes(data)
            size_mb = len(data) / (1024 * 1024)
            log.info("Large file saved: %s (%.1f MB)", saved_path, size_mb)
        except Exception as e:
            log.error("Failed to save large file: %s", e)
            # Фоллбэк — текстовое сообщение
            from telegram.sender import send_to_telegram
            await send_to_telegram(
                bot=self._bot, event=event, user=user, chat=chat,
                max_client=self.get_client(event.tg_user_id),
            )
            return

        # Формируем ссылку
        if FILES_URL_BASE:
            download_url = f"{FILES_URL_BASE}/{saved_name}"
        else:
            log.warning("FILES_URL_BASE not set — cannot send download link")
            download_url = None

        # Иконка типа медиа
        icon = {
            "photo": "🖼", "video": "🎬", "document": "📄",
            "voice": "🎤", "audio": "🎵",
        }.get(atype, "📎")

        size_mb = len(data) / (1024 * 1024)

        if download_url:
            msg_text = (
                f"{caption}\n\n"
                f"{icon} <b>{filename}</b> ({size_mb:.1f} МБ)\n"
                f"Файл слишком большой для Telegram. Скачать: {download_url}"
            ) if caption else (
                f"{icon} <b>{filename}</b> ({size_mb:.1f} МБ)\n"
                f"Файл слишком большой для Telegram. Скачать: {download_url}"
            )
        else:
            msg_text = (
                f"{caption}\n\n"
                f"{icon} <b>{filename}</b> ({size_mb:.1f} МБ)\n"
                f"<i>Файл слишком большой для отправки. FILES_URL_BASE не настроен.</i>"
            ) if caption else (
                f"{icon} <b>{filename}</b> ({size_mb:.1f} МБ)\n"
                f"<i>Файл слишком большой для отправки. FILES_URL_BASE не настроен.</i>"
            )

        # Отправляем текстовое сообщение со ссылкой
        from telegram.sender import _send_with_retry
        sent = await _send_with_retry(
            bot.send_message,
            chat_id=group_id,
            message_thread_id=topic_id,
            text=msg_text[:4096],
            parse_mode="HTML",
            disable_web_page_preview=False,
        )

        if sent:
            await db.save_message(
                user_id=user.id, chat_id=chat.id,
                direction="max_to_tg", timestamp=event.timestamp,
                max_sender_id=event.max_sender_id,
                max_msg_id=event.max_msg_id,
                tg_msg_id=sent.message_id,
                has_media=event.has_media,
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

    # ── Очистка старых файлов с диска ────────────────────────────────────────

    async def _cleanup_old_files(self):
        """Периодически удаляет файлы из FILES_DIR старше FILES_MAX_AGE_DAYS."""
        import time as _time

        while True:
            try:
                await asyncio.sleep(86400)  # проверяем раз в сутки
                cutoff = _time.time() - (FILES_MAX_AGE_DAYS * 86400)
                removed = 0
                for f in FILES_DIR.iterdir():
                    if f.is_file() and f.stat().st_mtime < cutoff:
                        try:
                            f.unlink()
                            removed += 1
                        except Exception as e:
                            log.error("Failed to delete old file %s: %s", f, e)
                if removed:
                    log.info("Cleaned up %d old files from %s (max age: %d days)",
                             removed, FILES_DIR, FILES_MAX_AGE_DAYS)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("File cleanup error: %s", e)


# Глобальный экземпляр
manager = BridgeManager()