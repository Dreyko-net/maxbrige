"""
PasswordProvider который запрашивает пароль 2FA через Telegram.
Вызывается pymax только если у пользователя включена двухфакторная защита.
"""

from __future__ import annotations

import asyncio
import logging

log = logging.getLogger(__name__)


class TelegramPasswordProvider:
    """
    Передаётся в MaxUserClient как password_provider.
    Бот зовёт .set_password(pwd) когда пользователь вводит пароль 2FA.
    """

    def __init__(self, tg_user_id: int, bot, chat_id: int):
        self.tg_user_id = tg_user_id
        self.bot        = bot
        self.chat_id    = chat_id
        self._event     = asyncio.Event()
        self._password: str = ""
        self.cancelled  = False

    async def get_password(self, hint: str | None) -> str:
        """Вызывается pymax когда требуется пароль 2FA."""
        if self.cancelled:
            raise asyncio.CancelledError("Auth cancelled")

        hint_text = f"\n\nПодсказка: <i>{hint}</i>" if hint else ""
        await self.bot.send_message(
            chat_id    = self.chat_id,
            text       = f"🔐 На вашем аккаунте MAX включена "
                         f"двухфакторная защита.\n\n"
                         f"Введите пароль для входа:{hint_text}",
            parse_mode = "HTML",
        )

        self._event.clear()
        try:
            await asyncio.wait_for(self._event.wait(), timeout=300)
        except asyncio.TimeoutError:
            self.cancelled = True
            await self.bot.send_message(
                chat_id = self.chat_id,
                text    = "⏱ Время ожидания пароля истекло.\nНачните заново: /start",
            )
            raise asyncio.CancelledError("2FA password timeout")

        if self.cancelled:
            raise asyncio.CancelledError("Auth cancelled by user")

        return self._password

    def set_password(self, password: str):
        """Вызывается из Telegram-хэндлера когда пользователь ввёл пароль."""
        self._password = password.strip()
        self._event.set()
