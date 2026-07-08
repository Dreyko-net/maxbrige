"""
Абстракция очереди сообщений.

Сейчас: asyncio.Queue
Переход на Redis: заменить реализацию в этом файле,
интерфейс (put/get) остаётся прежним во всём проекте.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Literal


Direction = Literal["max_to_tg", "tg_to_max"]


@dataclass
class BridgeEvent:
    """Единица работы в очереди."""
    direction:   Direction
    tg_user_id:  int
    max_chat_id: str          # ID чата в MAX
    text:        str
    timestamp:   int
    max_sender_id: int | None = None
    max_msg_id:  str  | None = None
    tg_msg_id:   int  | None = None
    # медиа
    has_media:   bool         = False
    media_type:  str  | None = None   # photo/video/document/voice
    media_bytes: bytes| None = None   # данные файла (если уже скачан)
    media_name:  str  | None = None   # имя файла для document


class MessageQueue:
    """
    Обёртка над asyncio.Queue.

    Чтобы перейти на Redis:
      1. Добавить зависимость aioredis
      2. Заменить self._q на redis stream
      3. Методы put/get оставить с тем же интерфейсом
    """

    def __init__(self):
        # Redis-ready: здесь будет aioredis.client.Redis
        self._q: asyncio.Queue[BridgeEvent] = asyncio.Queue()

    async def put(self, event: BridgeEvent) -> None:
        # Redis-ready: XADD stream_name * field value ...
        await self._q.put(event)

    async def get(self) -> BridgeEvent:
        # Redis-ready: XREAD COUNT 1 BLOCK 0 STREAMS stream_name >
        return await self._q.get()

    def task_done(self) -> None:
        self._q.task_done()

    def qsize(self) -> int:
        return self._q.qsize()


# Два канала: MAX→TG и TG→MAX
max_to_tg_queue = MessageQueue()
tg_to_max_queue = MessageQueue()
