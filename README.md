# MAX ↔ Telegram Bridge

Мост между мессенджером MAX и Telegram.  
Пользователь авторизуется в боте, бот создаёт супергруппу с темами (topics),
каждый чат MAX = отдельная тема. Сообщения пересылаются в обе стороны.

---

## Быстрый старт

### 1. Создайте Telegram-бота

У @BotFather: `/newbot` → получите `TG_BOT_TOKEN`.

### 2. Установите зависимости

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Создайте .env

```bash
cp .env.example .env
nano .env   # вставьте TG_BOT_TOKEN
```

### 4. Запустите

```bash
python main.py
```

---

## Флоу подключения нового пользователя

```
/start
  → введите номер MAX (+79001234567)
  → придёт SMS, введите код в бот
  → создайте супергруппу в Telegram:
      • включите Topics (Темы) в настройках группы
      • добавьте бота администратором
        (права: управление темами + сообщения)
      • перешлите любое сообщение из группы боту
  → бот создаст темы для каждого чата MAX
  → загрузит сообщения за сегодня, потом за 7 дней
```

---

## Структура проекта

```
max_bridge/
├── main.py                    # точка входа
├── config.py                  # настройки из .env
├── .env.example               # шаблон конфига
├── database/
│   ├── __init__.py
│   └── db.py                  # схема БД, все запросы
├── bridge/
│   ├── __init__.py
│   ├── manager.py             # BridgeManager, пул клиентов
│   ├── max_client.py          # обёртка над pymax.Client
│   ├── queue.py               # очереди (Redis-ready)
│   └── sync_worker.py         # синхронизация истории
├── telegram/
│   ├── __init__.py            # create_bot()
│   ├── sender.py              # отправка в Telegram
│   ├── keyboards.py           # inline-клавиатуры
│   ├── sms_provider.py        # SMS-код через Telegram
│   └── handlers/
│       ├── auth.py            # /start, FSM авторизации
│       ├── messages.py        # TG → MAX
│       └── callbacks.py       # кнопка 📎 Загрузить
└── sessions/
    └── user_{tg_id}/
        └── session.db         # сессия pymax (авто)
```

---

## Переход на Redis (когда понадобится)

Замените в `bridge/queue.py`:

```python
# Было:
self._q = asyncio.Queue()
await self._q.put(event)
return await self._q.get()

# Стало:
import aioredis
self._redis = aioredis.from_url("redis://localhost")
await self._redis.xadd("bridge_events", {"data": json.dumps(event)})
result = await self._redis.xread({"bridge_events": ">"}, block=0)
```

Интерфейс `put/get` остаётся одинаковым — остальной код не меняется.

---

## Запуск как systemd-сервис

`/etc/systemd/system/max-bridge.service`:

```ini
[Unit]
Description=MAX Telegram Bridge
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/max_bridge
EnvironmentFile=/path/to/max_bridge/.env
ExecStart=/path/to/venv/bin/python main.py
Restart=on-failure
RestartSec=15

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable max-bridge
sudo systemctl start max-bridge
sudo journalctl -u max-bridge -f
```
