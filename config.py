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

# Максимальный размер файла (байт) для прямой отправки через Telegram Bot API.
# Файлы больше этого размера сохраняются на диск и отправляются ссылкой.
TG_MAX_FILE_SIZE: int = int(os.getenv("TG_MAX_FILE_SIZE", str(49 * 1024 * 1024)))  # 50 МБ

# ── Файловый сервер (для больших файлов) ─────────────────────────────────────
# Директория для сохранения файлов, не помещающихся в лимит Telegram.
FILES_DIR: Path = Path(os.getenv("FILES_DIR", str(BASE_DIR / "files")))
FILES_DIR.mkdir(parents=True, exist_ok=True)

# Базовый URL для скачивания файлов (без слэша на конце).
# Пример: "https://example.com/files" или "http://192.168.1.100:8090/files"
FILES_URL_BASE: str = os.getenv("FILES_URL_BASE", "")

# Срок хранения файлов на диске (дней).
FILES_MAX_AGE_DAYS: int = int(os.getenv("FILES_MAX_AGE_DAYS", "7"))

# ── Служебная тема ────────────────────────────────────────────────────────────
CONTROL_TOPIC_NAME: str = "⚙️ Управление"

# ── Настройки тестирования и разработки ───────────────────────────────────────
if os.getenv("DEBUG", False):
    # Получать ли сиглалы от ОС для остановки (Ctrl+C например)
    HANDLE_SIGNALS = True
    DEBUG = os.getenv("DEBUG", False)
else:
    HANDLE_SIGNALS = False
    DEBUG = False