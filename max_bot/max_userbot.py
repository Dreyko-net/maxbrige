#!/usr/bin/env python3
"""
MAX Messenger Userbot — авторизация под пользователем через номер телефона.
Получает все входящие сообщения и сохраняет в SQLite.

Использует: maxapi-python (pip install maxapi-python)
Документация: https://github.com/MaxApiTeam/PyMax
"""

import asyncio
import logging
import sys
from datetime import datetime

from pymax import SocketMaxClient, Message
from pymax.payloads import UserAgentPayload

from database import Database
from config import Config

# ── Логирование ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("max_userbot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ── Глобальные объекты ───────────────────────────────────────────────────────
cfg = Config()
db  = Database(cfg.db_path)


# ── Клиент ───────────────────────────────────────────────────────────────────
def make_client(phone: str) -> SocketMaxClient:
    """Создаёт клиент с авторизацией по номеру телефона (DESKTOP-режим)."""
    ua = UserAgentPayload(
        device_type="DESKTOP",
        app_version="25.12.13",
    )
    return SocketMaxClient(
        phone=phone,
        work_dir="cache",   # здесь хранятся файлы сессии
        headers=ua,
    )


# ── Обработчики событий ──────────────────────────────────────────────────────
def register_handlers(client: SocketMaxClient):

    @client.on_message()
    async def handle_message(msg: Message) -> None:
        """Вызывается на каждое входящее сообщение."""
        try:
            sender_id   = str(getattr(msg.sender, "id",   "") or "")
            sender_name = str(getattr(msg.sender, "name", "") or getattr(msg.sender, "username", "") or "")
            chat_id     = str(getattr(msg, "chat_id", "") or "")
            text        = getattr(msg, "text", "") or ""
            msg_id      = str(getattr(msg, "id", "") or "")
            timestamp   = getattr(msg, "timestamp", None)

            if timestamp is None:
                timestamp = int(datetime.now().timestamp() * 1000)

            db.save_message(
                message_id  = msg_id,
                sender_id   = sender_id,
                sender_name = sender_name,
                chat_id     = chat_id,
                text        = text,
                timestamp   = timestamp,
                raw         = repr(msg),
            )

            dt = datetime.fromtimestamp(timestamp / 1000).strftime("%H:%M:%S")
            log.info("[%s] %s → chat:%s | %s", dt, sender_name or sender_id, chat_id,
                     text[:100] if text else "(без текста)")

        except Exception as e:
            log.error("Ошибка обработки сообщения: %s | %s", e, msg)

    @client.on_start
    async def on_start() -> None:
        me = client.me
        me_id   = getattr(me, "id",   "?")
        me_name = getattr(me, "name", "?") or getattr(me, "username", "?")

        log.info("✅  Подключено! Пользователь: %s (id=%s)", me_name, me_id)
        print(f"\n✅  Авторизован как: {me_name} (id={me_id})")
        print(f"💾  База данных: {cfg.db_path}")
        print("🚀  Ожидание сообщений. Нажмите Ctrl+C для остановки.\n")

        # Сохраняем информацию о себе
        db.set_meta("me_id",   str(me_id))
        db.set_meta("me_name", me_name)


# ── Точка входа ──────────────────────────────────────────────────────────────
async def main():
    print("\n" + "═" * 55)
    print("  MAX Messenger Userbot — сборщик сообщений")
    print("═" * 55 + "\n")

    # Номер телефона
    phone = cfg.phone
    if not phone:
        phone = input("Введите номер телефона (например, +79001234567): ").strip()
    if not phone:
        print("Номер не введён. Выход.")
        sys.exit(1)

    # Инициализация БД
    db.init()

    # Создание и настройка клиента
    client = make_client(phone)
    register_handlers(client)

    print(f"\nПодключение для номера {phone}…")
    print("(Программа запросит SMS-код при первом запуске)\n")

    try:
        await client.start()
    except KeyboardInterrupt:
        print("\n\nОстановка…")
    except Exception as e:
        log.error("Критическая ошибка: %s", e)
        raise
    finally:
        db.close()
        print("До свидания!")


if __name__ == "__main__":
    asyncio.run(main())
