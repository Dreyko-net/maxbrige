"""
Обёртка над pymax.Client для одного пользователя.
Регистрирует обработчики и кладёт события в очередь max_to_tg_queue.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Callable, Optional

from pymax import Client, Message, ConsoleSmsCodeProvider
from pymax.auth.sms import SmsAuthFlow

from bridge.queue import BridgeEvent, max_to_tg_queue
from config import SESSIONS_DIR

log = logging.getLogger(__name__)


class MaxUserClient:
    """
    Клиент MAX для одного пользователя моста.

    tg_user_id    — Telegram ID владельца
    max_phone     — номер телефона в MAX
    session_path  — work_dir для pymax (берётся из БД)
    on_ready      — колбэк, вызывается после успешного start()
    """

    def __init__(
        self,
        tg_user_id: int,
        max_phone: str,
        session_path: str,
        on_ready: Optional[Callable] = None,
        sms_code_provider=None,
    ):
        self.tg_user_id   = tg_user_id
        self.max_phone    = max_phone
        self.session_path = session_path
        self.on_ready     = on_ready
        self._sms_provider = sms_code_provider
        self._client: Optional[Client] = None
        self._task: Optional[asyncio.Task] = None
        self.me = None

    def _build_client(self) -> Client:
        Path(self.session_path).mkdir(parents=True, exist_ok=True)
        return Client(
            phone=self.max_phone,
            work_dir=self.session_path,
            session_name="session.db",
            sms_code_provider=self._sms_provider,
        )

    async def start(self) -> None:
        """Запускает клиент. При наличии сессии — без SMS."""
        self._client = self._build_client()
        self._register_handlers()
        try:
            self.me = await self._client.start()
        except asyncio.CancelledError:
            # SMS-провайдер отменил авторизацию (таймаут)
            # Останавливаем клиент чтобы pymax не ушёл в reconnect
            try:
                await self._client.stop()
            except Exception:
                pass
            raise
        log.info("[user=%s] MAX connected as %s", self.tg_user_id,
                 getattr(self.me, "name", self.max_phone))
        if self.on_ready:
            await self.on_ready(self)

    async def run(self) -> None:
        """Держит соединение живым. Запускать как asyncio.Task."""
        try:
            await self._client.run_forever()
        except Exception as e:
            log.error("[user=%s] MAX connection lost: %s", self.tg_user_id, e)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()

    def _register_handlers(self):
        client = self._client

        @client.on_message()
        async def handle_message(msg: Message, _client: Client) -> None:
            try:
                text       = getattr(msg, "text",      "") or ""
                msg_id     = str(getattr(msg, "id",    "") or "")
                chat_id    = str(getattr(msg, "chat_id", "") or "")
                timestamp  = getattr(msg, "timestamp", None) or int(time.time() * 1000)

                # Определяем медиа
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
        """Отправляет текстовое сообщение в MAX. Возвращает max_msg_id."""
        try:
            result = await self._client.send_message(
                chat_id=max_chat_id,
                text=text,
            )
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_message error: %s", self.tg_user_id, e)
            return None

    async def send_file(
        self,
        max_chat_id: str,
        data: bytes,
        filename: str,
        caption: str = "",
    ) -> Optional[str]:
        """Отправляет файл в MAX."""
        try:
            result = await self._client.send_file(
                chat_id  = max_chat_id,
                data     = data,
                filename = filename,
                caption  = caption,
            )
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_file error: %s", self.tg_user_id, e)
            return None

    async def get_chats(self) -> list:
        """Возвращает список чатов пользователя."""
        try:
            return await self._client.get_chats() or []
        except Exception as e:
            log.error("[user=%s] get_chats error: %s", self.tg_user_id, e)
            return []

    async def get_history(
        self,
        max_chat_id: str,
        from_ts: int,
        to_ts: int,
        limit: int = 100,
    ) -> list:
        """Возвращает историю сообщений чата за период."""
        try:
            return await self._client.get_messages(
                chat_id = max_chat_id,
                from_ts = from_ts,
                to_ts   = to_ts,
                limit   = limit,
            ) or []
        except Exception as e:
            log.error("[user=%s] get_history error: %s", self.tg_user_id, e)
            return []

    async def download_file(self, file_id: str) -> Optional[bytes]:
        """Скачивает файл из MAX по file_id."""
        try:
            return await self._client.download_file(file_id)
        except Exception as e:
            log.error("[user=%s] download_file error: %s", self.tg_user_id, e)
            return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _detect_media(msg: Message) -> tuple[bool, Optional[str]]:
    """Определяет есть ли медиа в сообщении и какого типа."""
    for attr, kind in [
        ("photo",    "photo"),
        ("video",    "video"),
        ("document", "document"),
        ("voice",    "voice"),
        ("audio",    "audio"),
        ("sticker",  "sticker"),
    ]:
        if getattr(msg, attr, None):
            return True, kind
    return False, None


def session_path_for(tg_user_id: int) -> str:
    return str(SESSIONS_DIR / f"user_{tg_user_id}")