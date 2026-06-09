from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from telegram.handlers import auth, messages, callbacks


def create_bot(token: str) -> tuple[Bot, Dispatcher]:
    bot = Bot(token=token)
    dp  = Dispatcher(storage=MemoryStorage())

    dp.include_router(auth.router)
    dp.include_router(callbacks.router)
    dp.include_router(messages.router)

    return bot, dp
