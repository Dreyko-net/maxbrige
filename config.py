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
BASE_DIR      = Path(os.getenv("BASE_DIR", __file__)).parent #Path(__file__).parent
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

# ── Служебная тема ────────────────────────────────────────────────────────────
CONTROL_TOPIC_NAME: str = "⚙️ Управление"
