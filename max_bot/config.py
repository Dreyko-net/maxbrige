"""
Конфигурация userbot'а.
Можно задавать через переменные окружения или напрямую ниже.
"""

import os


class Config:
    # Номер телефона (+79001234567).
    # Если пусто — программа спросит при запуске.
    # export MAX_PHONE="+79001234567"
    phone: str = os.getenv("MAX_PHONE", "")

    # Путь к файлу SQLite
    # export MAX_DB_PATH="messages.db"
    db_path: str = os.getenv("MAX_DB_PATH", "messages.db")

    # Уровень логирования: DEBUG / INFO / WARNING
    log_level: str = os.getenv("MAX_LOG_LEVEL", "INFO")
