"""
Конфигурация моста MAX ↔ Telegram.
Все секреты берутся из переменных окружения или файла .env
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────────────
# Токен Telegram-бота (от @BotFather)
TG_BOT_TOKEN: str = os.getenv("TG_BOT_TOKEN", "")
TG_PROXY: str = os.getenv("TG_PROXY", "")

# ── Пути ─────────────────────────────────────────────────────────────────────
DEFAULT_PATH  = Path(__file__).parent
BASE_DIR      = Path(os.getenv("BASE_DIR", DEFAULT_PATH))
if not BASE_DIR.exists():
    BASE_DIR.mkdir()

SESSIONS_DIR  = BASE_DIR / "sessions"   # папки сессий pymax по пользователям
DB_PATH       = BASE_DIR / "bridge.db"  # основная SQLite база

SESSIONS_DIR.mkdir(exist_ok=True)

# ── Синхронизация ─────────────────────────────────────────────────────────────
# Сколько дней истории загружать при первом подключении
HISTORY_DAYS: int = int(os.getenv("HISTORY_DAYS", "7"))

# Сколько часов хранить медиакэш
MEDIA_CACHE_HOURS: int = int(os.getenv("MEDIA_CACHE_HOURS", "24"))

# Пауза между сообщениями при заливке истории (сек) — чтобы не словить flood
FLOOD_SLEEP: float = float(os.getenv("FLOOD_SLEEP", "0.05"))

# ── Лимиты ─────────────────────────────────────────────────────────────────────
# Максимальный размер файла (байт) для отправки через Telegram API.
# Если прокси nginx имеет меньший client_max_body_size — файлы больше этого
# значения будут отправляться как текстовый фоллбэк без попытки отправки.
MAX_SEND_BYTES: int = int(os.getenv("MAX_SEND_BYTES", str(10 * 1024 * 1024)))  # 10 МБ по умолчанию

# Максимальный размер одного файла (байт) для отправки через Telegram Bot API.
# Cloud API: 50 МБ, Local Bot API: до 2000 МБ.
# Файлы больше этого значения будут разбиты на части и отправлены как документы.
TG_CHUNK_SIZE: int = int(os.getenv("TG_CHUNK_SIZE", str(40 * 1024 * 1024)))  # 50 МБ по умолчанию

# ── Служебная тема ────────────────────────────────────────────────────────────
CONTROL_TOPIC_NAME: str = "⚙️ Управление"

# ── Настройки тестирования и разработки ───────────────────────────────────────
if os.getenv("DEBUG", False):
    # Получать ли сиглалы от ОС для остановки (Ctrl+C например)
    HANDLE_SIGNALS = True
    DEBUG = os.getenv("DEBUG", False)
else:
    HANDLE_SIGNALS = False
    DEBUG = os.getenv("DEBUG", False)