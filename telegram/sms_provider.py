"""
SmsCodeProvider который запрашивает SMS-код через Telegram.
При отмене или таймауте бросает CancelledError — это останавливает
внутренний reconnect-цикл pymax.
"""

from __future__ import annotations

import asyncio
import logging

log = logging.getLogger(__name__)

# Максимальное число попыток запроса кода за одну сессию авторизации.
# Если pymax пытается получить код больше раз — значит что-то пошло не так
# (например MAX вернул "слишком много попыток") и мы прерываем цикл.
MAX_CODE_ATTEMPTS = 2


class TelegramSmsCodeProvider:
    def __init__(self, tg_user_id: int, bot, chat_id: int):
        self.tg_user_id   = tg_user_id
        self.bot          = bot
        self.chat_id      = chat_id
        self._event       = asyncio.Event()
        self._code: str   = ""
        self.cancelled    = False
        self._attempts    = 0

    async def get_code(self, phone: str) -> str:
        """Вызывается pymax при каждой попытке получить SMS-код."""

        # Защита от бесконечного цикла
        self._attempts += 1
        if self.cancelled or self._attempts > MAX_CODE_ATTEMPTS:
            log.warning("[sms_provider] too many attempts (%d) or cancelled, stopping",
                        self._attempts)
            raise asyncio.CancelledError("Auth stopped: too many attempts or cancelled")

        log.info("[sms_provider] get_code attempt=%d user=%s", self._attempts, self.tg_user_id)

        # Сбрасываем Event для повторного запроса (если это не первый раз)
        self._event.clear()
        self._code = ""

        await self.bot.send_message(
            chat_id    = self.chat_id,
            text       = f"📱 На номер <code>{phone}</code> отправлен SMS-код. (Если есть авторизаванный аккаунт MAX, то код может отправиться в него через Бота \"Коды подтверждения\" \n"
                         f"Введите его в этот чат:",
            parse_mode = "HTML",
        )

        try:
            await asyncio.wait_for(self._event.wait(), timeout=300)
        except asyncio.TimeoutError:
            self.cancelled = True
            await self.bot.send_message(
                chat_id = self.chat_id,
                text    = "⏱ Время ожидания SMS-кода истекло (5 минут).\n"
                          "Попробуйте снова: /start",
            )
            raise asyncio.CancelledError("SMS code timeout")

        if self.cancelled:
            raise asyncio.CancelledError("Auth cancelled by user")

        return self._code

    def set_code(self, code: str):
        """Вызывается из Telegram-хэндлера когда пользователь ввёл код."""
        self._code = code.strip()
        self._event.set()