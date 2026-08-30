# MAX ↔ Telegram Bridge

Двусторонний мост между мессенджером MAX и Telegram. 
Каждый пользователь авторизует свой аккаунт MAX через бота, создаёт 
супергруппу-форум в Telegram, и бот автоматически пересылает сообщения 
в обе стороны — каждый чат MAX = отдельная тема (topic).

## Возможности

- **Двусторонняя пересылка** — текст, фото, видео, голосовые, аудио, стикеры, документы
- **Альбомы** — группы фото/видео пересылаются единым сообщением (media group)
- **Пересылка из Telegram в MAX** — текст и медиа из тем, а также пересланные сообщения из любых чатов
- **Полная синхронизация истории** — загрузка всех сообщений с реальными медиа-вложениями
- **Выборочная загрузка истории** — команда `/history` внутри топика с выбором периода (сутки / неделя / месяц / всё)
- **Большие файлы** — файлы > 50 МБ сохраняются на диск, в Telegram отправляется ссылка для скачивания
- **Чанковое скачивание** — файлы 5–20 МБ из Telegram качаются чанками по 4 МБ через HTTP Range
- **Автовосстановление топиков** — если тема удалена в Telegram, бот автоматически создаёт новую при следующем сообщении
- **Обработка.revoked-сессий** — при разлогине MAX бот уведомляет пользователя и сбрасывает статус
- **Двухфакторная авторизация (2FA)** — поддержка аккаунтов MAX с включённой 2FA
- **Прокси** — поддержка подключения к Telegram API через прокси (DPI/TLS-облокировки)
- **Многопользовательский режим** — каждый пользователь работает со своим аккаунтом MAX
- **Auto-reconnect** — автоматическое переподключение при обрывах связи с экспоненциальной задержкой (и для Telegram polling, и для MAX-клиента)
- **Диагностика подключения** — при запуске проверяется DNS, TCP и TLS до api.telegram.org
- **Медиакэш с TTL** — кэширование `tg_file_id` для повторной отправки без повторной загрузки
- **Flood control** — retry с задержкой при лимитах Telegram API, настраиваемая пауза между сообщениями

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

### 3. Создайте `.env`

```bash
cp .env.example .env
nano .env   # вставьте TG_BOT_TOKEN
```

### 4. Запустите

```bash
python main.py
```

---

## Подключение пользователя

### Шаг 1. Авторизация в MAX

Напишите боту в личные сообщения `/start` → нажмите кнопку «Начать подключение к Max» → введите номер телефона в формате `+79001234567` → введите SMS-код (приходит в SMS или в бота «Коды подтверждения» MAX) → при наличии 2FA введите пароль.

### Шаг 2. Создание группы-форума

1. Создайте в Telegram новую супергруппу
2. Включите **Темы** (Topics): Настройки группы → Темы → Включить
3. Добавьте бота в группу **администратором** с правами: управление темами + отправка сообщений

Бот определяет группу автоматически через событие `my_chat_member` или по команде `/start` внутри группы.

### Шаг 3. Синхронизация

После подключения группы доступны команды:

| Команда | Где | Описание |
|---|---|---|
| `/sync_chats` | Личные сообщения | Создаёт/обновляет темы для всех чатов MAX (без истории) |
| `/sync` | Личные сообщения | Полная синхронизация: темы + загрузка всей истории с медиа |
| `/history` | Внутри топика | Загрузка истории только этого чата (выбор периода: сутки / неделя / месяц / всё) |
| `/status` | Личные сообщения | Статус подключения к MAX |

---

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `TG_BOT_TOKEN` | — | Токен Telegram-бота (от @BotFather) **(обязательно)** |
| `TG_PROXY` | `""` | URL Telegram API через прокси (для обхода блокировок) |
| `BASE_DIR` | директория проекта | Рабочая директория (сессии, БД, файлы) |
| `HISTORY_DAYS` | `7` | Дней истории при полной синхронизации |
| `MEDIA_CACHE_HOURS` | `24` | Срок жизни записей медиакэша (часы) |
| `FLOOD_SLEEP` | `0.05` | Пауза между сообщениями при заливке истории (сек) |
| `MAX_SEND_BYTES` | `10 МБ` | Макс. размер файла для отправки через Telegram API |
| `TG_MAX_FILE_SIZE` | `50 МБ` | Файлы больше сохраняются на диск, отправляется ссылка |
| `FILES_DIR` | `BASE_DIR/files` | Директория для больших файлов |
| `FILES_URL_BASE` | `""` | Базовый URL для скачивания файлов (без слэша на конце) |
| `FILES_MAX_AGE_DAYS` | `7` | Срок хранения файлов на диске (дней) |
| `DEBUG` | `False` | Режим отладки (обработка сигналов ОС, подробные логи) |

---

## Структура проекта

```
maxbrige/
├── main.py                     # Точка входа, polling с auto-reconnect
├── config.py                   # Настройки из .env
├── requirements.txt            # Зависимости
├── database/
│   ├── __init__.py
│   └── db.py                 # SQLite (aiosqlite), схема, все запросы
├── bridge/
│   ├── __init__.py             # Экспорт manager, очередей, BridgeEvent
│   ├── manager.py             # BridgeManager — центральный координатор
│   ├── max_client.py          # Обёртка над pymax.Client (на пользователя)
│   ├── queue.py               # Две очереди BridgeEvent (asyncio.Queue, Redis-ready)
│   └── sync_worker.py         # Загрузка истории чатов MAX в темы Telegram
├── telegram/
│   ├── __init__.py             # create_bot(), прокси, DNS/TLS-диагностика
│   ├── sender.py              # Отправка в Telegram (текст, медиа, альбомы, большие файлы)
│   ├── sms_provider.py        # Запрос SMS-кода через Telegram
│   ├── password_provider.py   # Запрос 2FA-пароля через Telegram
│   └── handlers/
│       ├── auth.py            # /start, FSM авторизации (телефон → SMS → 2FA → группа)
│       ├── commands.py        # /status, /sync_chats, /sync, /history
│       ├── messages.py        # TG → MAX: текст, медиа, альбомы, пересланные сообщения
│       └── callbacks.py       # Кнопка «Загрузить файл» из истории
└── sessions/                   # Сессии pymax (автосоздание)
    └── user_{tg_id}/
        └── session.db
```

---

## Архитектура

### Очереди сообщений

Два асинхронных канала `BridgeEvent` связывают MAX и Telegram:

```
MAX → max_to_tg_queue → _handle_max_to_tg() → Telegram
Telegram → tg_to_max_queue → _handle_tg_to_max() → MAX
```

Очереди реализованы через `asyncio.Queue`. Интерфейс `put/get` 
абстрагирован — переход на Redis (streams) требует замены 
только `bridge/queue.py`.

### Обработка медиа (MAX → Telegram)

1. `MaxUserClient` получает сообщение от pymax и определяет тип вложений
2. Медиа скачивается из MAX (прямые URL для фото, API-запросы для видео/файлов)
3. Фото и видео группируются в альбомы (`media_group`)
4. `BridgeEvent` с готовыми байтами попадает в `max_to_tg_queue`
5. `_handle_max_to_tg` отправляет в Telegram-тему:
   - Альбомы через `send_media_group` (с фоллбэком на поодиночную отправку)
   - Файлы > `TG_MAX_FILE_SIZE` сохраняются на диск, отправляется ссылка
   - Остальные медиа по типу (photo/video/voice/audio/document)

### Обработка сообщений (Telegram → MAX)

1. Текстовые сообщения из тем супергруппы отправляются напрямую
2. Медиафайлы скачиваются из Telegram (обычно + чанковый fallback через HTTP Range)
3. Альбомы буферизуются по `media_group_id` и отправляются единым сообщением
4. Пересланные сообщения из любых чатов/каналов пересылаются в последний привязанный чат MAX

### Автовосстановление топиков

Если топик удалён в Telegram но числится в БД:
1. При отправке Telegram может вернуть ошибку `message thread not found` или молча отправить в General
2. Бот детектирует оба случая (по ошибке или по несовпадению `message_thread_id` в ответе)
3. Вызывается `_ensure_chat_and_topic` — создаётся новый топик, `topic_id` обновляется в БД
4. Отправка повторяется с новым `topic_id`

---

## Переход на Redis (когда понадобится)

Замените реализацию в `bridge/queue.py`:

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
WorkingDirectory=/path/to/maxbrige
EnvironmentFile=/path/to/maxbrige/.env
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
