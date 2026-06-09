"""
SmsCodeProvider который запрашивает SMS-код через Telegram.

pymax вызывает get_sms_code() когда нужен код подтверждения.
Мы блокируемся на asyncio.Event и ждём пока пользователь
введёт код прямо в чат с ботом.
"""

from __future__ import annotations

import asyncio
import logging

log = logging.getLogger(__name__)


class TelegramSmsCodeProvider:
    """
    Передаётся в MaxUserClient при авторизации.
    Бот зовёт .set_code(code) когда пользователь присылает SMS-код.
    """

    def __init__(self, tg_user_id: int, bot, chat_id: int):
        self.tg_user_id = tg_user_id
        self.bot        = bot
        self.chat_id    = chat_id
        self._event     = asyncio.Event()
        self._code: str = ""

    async def get_sms_code(self) -> str:
        """Вызывается pymax. Ждёт пока пользователь введёт код."""
        await self.bot.send_message(
            chat_id = self.chat_id,
            text    = "📱 На ваш номер отправлен SMS-код.\n"
                      "Введите его в этот чат:",
        )
        # Ждём код от пользователя (таймаут 5 минут)
        try:
            await asyncio.wait_for(self._event.wait(), timeout=300)
        except asyncio.TimeoutError:
            await self.bot.send_message(
                chat_id = self.chat_id,
                text    = "⏱ Время ожидания кода истекло. Начните заново: /start",
            )
            raise TimeoutError("SMS code timeout")
        return self._code

    def set_code(self, code: str):
        """Вызывается из Telegram-хэндлера когда пришёл код."""
        self._code = code.strip()
        self._event.set()
