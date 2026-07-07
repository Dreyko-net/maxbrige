import logging
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import TelegramObject

from config import TG_PROXY
from telegram.handlers import auth, messages, callbacks

log = logging.getLogger(__name__)


class LogAllUpdatesMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        log.info("[MIDDLEWARE] update type=%s data_keys=%s",
                 type(event).__name__, list(data.keys()))
        result = await handler(event, data)
        return result


def create_bot(token: str) -> tuple[Bot, Dispatcher]:
    proxy = TG_PROXY.rstrip("/") if TG_PROXY else ""

    if proxy:
        # log.info("Using Telegram API proxy: %s", proxy)
        server = TelegramAPIServer.from_base(proxy)
        # log.info("API URL template: %s", server.base)
        # Проверяем что URL корректный
        # test_url = server.base.format(token=token, method="getMe")
        # log.info("Test getMe URL: %s", test_url)
        session = AiohttpSession(api=server)
        bot = Bot(token=token, session=session)
    else:
        log.info("Using direct Telegram connection")
        bot = Bot(token=token)

    dp = Dispatcher(storage=MemoryStorage())
    dp.update.middleware(LogAllUpdatesMiddleware())

    dp.include_router(auth.router)
    dp.include_router(callbacks.router)
    dp.include_router(messages.router)

    return bot, dp