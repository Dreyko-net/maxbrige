# MAX Messenger Userbot

Userbot на Python: авторизуется в MAX под реальным пользователем
по номеру телефона и SMS-коду, получает все входящие сообщения,
сохраняет в SQLite.

> ⚠️ **Дисклеймер**
> Используется неофициальный внутренний API MAX (библиотека `maxapi-python`).
> Может нарушать условия сервиса. Используйте на свой риск.

---

## Требования

- Python 3.10+
- Linux / macOS / Windows

---

## Установка

```bash
pip install -r requirements.txt
```

---

## Запуск

```bash
python max_userbot.py
```

При первом запуске программа спросит номер телефона,
затем MAX пришлёт SMS с кодом подтверждения.

**Через переменную окружения (чтобы не вводить каждый раз):**

```bash
export MAX_PHONE="+79001234567"
python max_userbot.py
```

Сессия сохраняется в папке `cache/` — при следующем запуске
авторизация не потребуется.

---

## Просмотр сообщений

```bash
python view.py               # последние 20 сообщений
python view.py -n 100        # последние 100
python view.py -c <chat_id>  # сообщения из конкретного чата
python view.py --stats       # статистика: топ чатов и отправителей
python view.py --search привет  # поиск по тексту
```

---

## Схема таблицы `messages` (SQLite)

| Поле        | Описание                           |
|-------------|------------------------------------|
| message_id  | ID сообщения в MAX                 |
| sender_id   | ID отправителя                     |
| sender_name | Имя / username отправителя         |
| chat_id     | ID чата или диалога                |
| text        | Текст сообщения                    |
| timestamp   | Время в миллисекундах (Unix)       |
| received_at | Время записи в БД                  |
| raw         | Полное строковое представление     |

---

## Файлы проекта

```
max_bot/
├── max_userbot.py   # главный скрипт
├── database.py      # работа с SQLite
├── config.py        # настройки
├── view.py          # просмотр сообщений
├── requirements.txt
├── cache/           # файлы сессии (создаются автоматически)
└── messages.db      # база данных (создаётся при запуске)
```

---

## Запуск как systemd-сервис

`/etc/systemd/system/max-userbot.service`:

```ini
[Unit]
Description=MAX Messenger Userbot
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/max_bot
Environment=MAX_PHONE=+79001234567
ExecStart=/usr/bin/python3 max_userbot.py
Restart=on-failure
RestartSec=15

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable max-userbot
sudo systemctl start max-userbot
sudo journalctl -u max-userbot -f   # логи
```
