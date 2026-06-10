"""
SmsCodeProvider который запрашивает SMS-код через Telegram.
"""

from __future__ import annotations

import asyncio
import logging

log = logging.getLogger(__name__)


class TelegramSmsCodeProvider:
    def __init__(self, tg_user_id: int, bot, chat_id: int):
        self.tg_user_id = tg_user_id
        self.bot        = bot
        self.chat_id    = chat_id
        self._event     = asyncio.Event()
        self._code: str = ""
        self.cancelled  = False   # флаг — провайдер мёртв, не перезапускать

    async def get_code(self, phone: str) -> str:
        """Вызывается pymax. Ждёт пока пользователь введёт код."""

        # Если провайдер уже отменён (таймаут был раньше) — сразу стоп
        if self.cancelled:
            raise asyncio.CancelledError("Provider already cancelled")

        await self.bot.send_message(
            chat_id    = self.chat_id,
            text       = f"📱 На номер <code>{phone}</code> отправлен SMS-код.\n"
                         f"Введите его в этот чат:",
            parse_mode = "HTML",
        )

        try:
            await asyncio.wait_for(self._event.wait(), timeout=300)
        except asyncio.TimeoutError:
            self.cancelled = True
            await self.bot.send_message(
                chat_id = self.chat_id,
                text    = "⏱ Время ожидания кода истекло. Начните заново: /start",
            )
            # Бросаем CancelledError — pymax остановит клиент
            raise asyncio.CancelledError("SMS code timeout")

        return self._code

    def set_code(self, code: str):
        """Вызывается из Telegram-хэндлера когда пришёл код."""
        self._code = code.strip()
        self._event.set()