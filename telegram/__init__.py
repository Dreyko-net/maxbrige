import logging
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.client.telegram import TelegramAPIServer
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import TelegramObject

from config import TG_BOT_TOKEN, TG_PROXY
from telegram.handlers import auth, messages, callbacks

log = logging.getLogger(__name__)


class LogAllUpdatesMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        log.info("[MIDDLEWARE] update type=%s data_keys=%s",
                 type(event).__name__, list(data.keys()))
        result = await handler(event, data)
        return result


def create_bot(token: str) -> tuple[Bot, Dispatcher]:
    if TG_PROXY:
        # TG_PROXY — базовый URL вашего nginx прокси
        # Например: https://venture.my.to
        # nginx проксирует /bot/ → https://api.telegram.org/bot
        log.info("Using Telegram API proxy: %s", TG_PROXY)
        server = TelegramAPIServer.from_base(TG_PROXY)
        bot = Bot(token=token, server=server)
    else:
        bot = Bot(token=token)

    dp = Dispatcher(storage=MemoryStorage())
    dp.update.middleware(LogAllUpdatesMiddleware())

    dp.include_router(auth.router)
    dp.include_router(callbacks.router)
    dp.include_router(messages.router)

    return bot, dp