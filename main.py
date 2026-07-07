#!/usr/bin/env python3
"""
MAX ↔ Telegram Bridge
Точка входа: запускает Telegram бота и BridgeManager в одном event loop.
"""

import asyncio
import logging
import sys
import os

from aiogram import Bot
from aiogram.exceptions import TelegramNetworkError, TelegramAPIError

from config import TG_BOT_TOKEN, TG_PROXY, HANDLE_SIGNALS
from database import db
from bridge.manager import manager
from telegram import create_bot

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bridge.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# Отключаем лишние логи pymax
logging.getLogger("pymax").setLevel(logging.WARNING)
logging.getLogger("aiogram").setLevel(logging.INFO)
if TG_PROXY != '':
    logging.getLogger("aiogram.dispatcher").setLevel(logging.CRITICAL)

# ── Настройки переподключения ─────────────────────────────────────────────
RECONNECT_BASE_DELAY = 2    # начальная задержка (сек)
RECONNECT_MAX_DELAY  = 60   # максимальная задержка (сек)
RECONNECT_BACKOFF    = 2    # множитель экспоненциальной задержки


def calc_delay(attempt: int) -> float:
    """Экспоненциальная задержка с ограничением сверху."""
    return min(
        RECONNECT_BASE_DELAY * (RECONNECT_BACKOFF ** min(attempt - 1, 5)),
        RECONNECT_MAX_DELAY,
    )


async def polling_loop(bot: Bot, dp) -> None:
    """
    Запускает long-polling с автоматическим переподключением.

    При сетевых ошибках (ServerDisconnected, timeout и т.д.) —
    ждёт с экспоненциальной задержкой и перезапускает polling,
    НЕ останавливая BridgeManager и БД.

    Выходит только при:
    - asyncio.CancelledError (Ctrl+C / системный сигнал)
    - штатном завершении polling без исключения
    """
    attempt = 0

    while True:
        attempt += 1
        try:
            await dp.start_polling(
                bot,
                allowed_updates=["message", "callback_query", "my_chat_member"],
                handle_signals=False,       # мы сами управляем остановкой
                close_bot_session=False,    # не убиваем сессию при переподключении
            )
            # start_polling вернулся без исключения — штатная остановка
            log.info("Polling stopped gracefully.")
            return

        except (TelegramNetworkError,) as e:
            delay = calc_delay(attempt)
            log.warning(
                "[attempt %d] Telegram network error: %s — reconnecting in %ds",
                attempt, e, delay,
            )
            await asyncio.sleep(delay)

        except (TelegramAPIError,) as e:
            delay = calc_delay(attempt)
            log.warning(
                "[attempt %d] Telegram API error: %s — reconnecting in %ds",
                attempt, e, delay,
            )
            await asyncio.sleep(delay)

        except asyncio.CancelledError:
            log.info("Polling cancelled — shutting down.")
            return

        except Exception as e:
            delay = calc_delay(attempt)
            log.error(
                "[attempt %d] Unexpected error in polling: %s — restarting in %ds",
                attempt, e, delay,
                exc_info=True,
            )
            await asyncio.sleep(delay)


async def main():
    if not TG_BOT_TOKEN:
        print("❌ Укажите TG_BOT_TOKEN в файле .env или переменной окружения")
        sys.exit(1)

    print("\n" + "=" * 55)
    print("  MAX <-> Telegram Bridge")
    print("=" * 55 + "\n")

    # Инициализация БД
    await db.connect()
    log.info("Database connected")

    # Создание бота и диспетчера
    bot, dp = create_bot(TG_BOT_TOKEN)

    # Запуск BridgeManager (восстанавливает сессии из БД)
    await manager.start(bot)

    log.info("Starting Telegram bot polling...")
    print("Бот запущен. Нажмите Ctrl+C для остановки.\n")

    try:
        await polling_loop(bot, dp)
    finally:
        # Cleanup — только при реальной остановке (CancelledError или
        # штатное завершение polling), НЕ при каждом сетовом сбое
        await manager.stop()
        await db.close()
        await bot.session.close()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановка...")