import logging
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import TelegramObject, BotCommand

from config import TG_PROXY, DEBUG
from telegram.handlers import auth, messages, callbacks, commands

log = logging.getLogger(__name__)


class LogAllUpdatesMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        log.info("[MIDDLEWARE] update type=%s data_keys=%s",
                 type(event).__name__, list(data.keys()))
        result = await handler(event, data)
        return result


async def _set_bot_commands(bot: Bot):
    """Устанавливает меню команд бота."""
    commands = [
        BotCommand(command="start", description="Подключить MAX"),
        BotCommand(command="status", description="Статус подключения"),
        BotCommand(command="sync_chats", description="Синхронизировать чаты"),
        BotCommand(command="sync", description="Полная синхронизация"),
        BotCommand(command="history", description="Скачать историю (в топике)"),
    ]
    try:
        await bot.set_my_commands(commands)
        log.info("Bot commands menu set")
    except Exception as e:
        log.error("Failed to set bot commands: %s", e)


def create_bot(token: str) -> tuple[Bot, Dispatcher]:
    proxy = TG_PROXY.rstrip("/") if TG_PROXY != '' else ""

    if proxy:
        server = TelegramAPIServer.from_base(proxy)
        if DEBUG:
            log.info("Using Telegram API proxy: %s", proxy)
            log.info("API URL template: %s", server.base)
            test_url = server.base.format(token=token, method="getMe")
            log.info("Test getMe URL: %s", test_url)
        session = AiohttpSession(api=server)
        bot = Bot(token=token, session=session)
    else:
        log.info("Using direct Telegram connection")
        bot = Bot(token=token)

    dp = Dispatcher(storage=MemoryStorage())
    dp.update.middleware(LogAllUpdatesMiddleware())

    dp.include_router(auth.router)
    dp.include_router(commands.router)
    dp.include_router(callbacks.router)
    dp.include_router(messages.router)

    # Устанавливаем меню после включения роутеров
    dp.startup.register(_set_bot_commands)

    return bot, dp