"""
Обёртка над pymax.Client для одного пользователя.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Callable, Optional

from pymax import Client, Message

from bridge.queue import BridgeEvent, max_to_tg_queue
from config import SESSIONS_DIR

log = logging.getLogger(__name__)


class MaxUserClient:
    def __init__(
        self,
        tg_user_id: int,
        max_phone: str,
        session_path: str,
        on_ready: Optional[Callable] = None,
        sms_code_provider=None,
    ):
        self.tg_user_id    = tg_user_id
        self.max_phone     = max_phone
        self.session_path  = session_path
        self.on_ready      = on_ready
        self._sms_provider = sms_code_provider
        self._client: Optional[Client] = None
        self._task:   Optional[asyncio.Task] = None
        self.me = None
        self._ready = asyncio.Event()  # выставляется когда app.start() завершён

    def _build_client(self) -> Client:
        Path(self.session_path).mkdir(parents=True, exist_ok=True)
        return Client(
            phone             = self.max_phone,
            work_dir          = self.session_path,
            session_name      = "session.db",
            sms_code_provider = self._sms_provider,
        )

    async def start(self) -> None:
        """
        Запускает клиент в фоновом Task.
        Ждёт пока _app.start() завершится (авторизация + login).
        """
        self._client = self._build_client()
        self._register_handlers()

        # Патчим on_start чтобы поймать момент готовности
        original_start = self._client._app.start

        async def patched_start():
            await original_start()
            # _app.start() вернулся — авторизация и login завершены
            self.me = getattr(self._client, "me", None) or \
                      getattr(self._client._app, "profile", None)
            log.info("[user=%s] MAX ready, me=%s", self.tg_user_id, self.me)
            self._ready.set()
            if self.on_ready:
                await self.on_ready(self)

        self._client._app.start = patched_start

        # Запускаем бесконечный Client.start() как Task
        self._task = asyncio.create_task(
            self._run_forever(),
            name=f"max_client_{self.tg_user_id}",
        )

        # Ждём готовности (таймаут 30 сек)
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=30)
        except asyncio.TimeoutError:
            log.error("[user=%s] MAX client ready timeout", self.tg_user_id)
            self._task.cancel()
            raise

    async def _run_forever(self):
        """Бесконечный цикл pymax — работает пока не отменят."""
        try:
            await self._client.start()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("[user=%s] MAX client error: %s", self.tg_user_id, e,
                      exc_info=True)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def _register_handlers(self):
        client = self._client

        @client.on_message()
        async def handle_message(msg: Message, _client: Client) -> None:
            try:
                text      = getattr(msg, "text",      "") or ""
                msg_id    = str(getattr(msg, "id",    "") or "")
                chat_id   = str(getattr(msg, "chat_id", "") or "")
                timestamp = getattr(msg, "timestamp", None) or int(time.time() * 1000)

                has_media, media_type = _detect_media(msg)

                event = BridgeEvent(
                    direction   = "max_to_tg",
                    tg_user_id  = self.tg_user_id,
                    max_chat_id = chat_id,
                    text        = text,
                    timestamp   = timestamp,
                    max_msg_id  = msg_id,
                    has_media   = has_media,
                    media_type  = media_type,
                )
                await max_to_tg_queue.put(event)

            except Exception as e:
                log.error("[user=%s] handle_message error: %s", self.tg_user_id, e)

    # ── Отправка в MAX ────────────────────────────────────────────────────────

    async def send_message(self, max_chat_id: str, text: str) -> Optional[str]:
        try:
            result = await self._client.send_message(chat_id=max_chat_id, text=text)
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_message error: %s", self.tg_user_id, e)
            return None

    async def send_file(self, max_chat_id: str, data: bytes,
                        filename: str, caption: str = "") -> Optional[str]:
        try:
            result = await self._client.send_file(
                chat_id=max_chat_id, data=data, filename=filename, caption=caption)
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_file error: %s", self.tg_user_id, e)
            return None

    async def get_chats(self) -> list:
        log.info("[user=%s] get_chats: %s", self.tg_user_id,self._client.get_chats())
        try:
            return await self._client.get_chats() or []
        except Exception as e:
            log.error("[user=%s] get_chats error: %s", self.tg_user_id, e)
            return []

    async def get_history(self, max_chat_id: str, from_ts: int,
                          to_ts: int, limit: int = 100) -> list:
        try:
            return await self._client.get_messages(
                chat_id=max_chat_id, from_ts=from_ts, to_ts=to_ts, limit=limit) or []
        except Exception as e:
            log.error("[user=%s] get_history error: %s", self.tg_user_id, e)
            return []

    async def download_file(self, file_id: str) -> Optional[bytes]:
        try:
            return await self._client.download_file(file_id)
        except Exception as e:
            log.error("[user=%s] download_file error: %s", self.tg_user_id, e)
            return None


def _detect_media(msg) -> tuple[bool, Optional[str]]:
    for attr, kind in [("photo","photo"),("video","video"),("document","document"),
                       ("voice","voice"),("audio","audio"),("sticker","sticker")]:
        if getattr(msg, attr, None):
            return True, kind
    return False, None


def session_path_for(tg_user_id: int) -> str:
    return str(SESSIONS_DIR / f"user_{tg_user_id}")