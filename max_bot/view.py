#!/usr/bin/env python3
"""
Просмотр сохранённых сообщений из базы данных.

Использование:
    python view.py                   # последние 20
    python view.py -n 100            # последние 100
    python view.py -c <chat_id>      # фильтр по чату
    python view.py -s <sender_id>    # фильтр по отправителю
    python view.py --stats           # статистика
    python view.py --search слово    # поиск по тексту
"""

import argparse
from datetime import datetime
from database import Database
from config import Config


def ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime("%d.%m.%Y %H:%M:%S")


def main():
    ap = argparse.ArgumentParser(description="Просмотр сообщений MAX userbot")
    ap.add_argument("-n", "--limit",  type=int, default=20)
    ap.add_argument("-c", "--chat",   default=None, help="Фильтр по chat_id")
    ap.add_argument("-s", "--sender", default=None, help="Фильтр по sender_id")
    ap.add_argument("--search",       default=None, help="Поиск по тексту")
    ap.add_argument("--stats",        action="store_true")
    ap.add_argument("--db",           default=None)
    args = ap.parse_args()

    cfg = Config()
    db  = Database(args.db or cfg.db_path)
    db.init()

    if args.stats:
        total = db.count()
        rows  = db.conn.execute(
            """SELECT sender_name, sender_id, COUNT(*) as cnt
               FROM messages GROUP BY sender_id
               ORDER BY cnt DESC LIMIT 15"""
        ).fetchall()
        chats = db.conn.execute(
            """SELECT chat_id, COUNT(*) as cnt
               FROM messages GROUP BY chat_id
               ORDER BY cnt DESC LIMIT 10"""
        ).fetchall()
        print(f"\n📊  Всего сообщений: {total}\n")
        print("Топ отправителей:")
        for r in rows:
            print(f"  {r['sender_name'] or '?':20s} (id {r['sender_id']}): {r['cnt']}")
        print("\nТоп чатов:")
        for r in chats:
            print(f"  chat {r['chat_id']}: {r['cnt']}")
        db.close(); return

    # Поиск по тексту
    if args.search:
        rows = db.conn.execute(
            "SELECT * FROM messages WHERE text LIKE ? ORDER BY timestamp DESC LIMIT ?",
            (f"%{args.search}%", args.limit),
        ).fetchall()
    else:
        rows = db.get_messages(
            chat_id=args.chat,
            sender_id=args.sender,
            limit=args.limit,
        )

    if not rows:
        print("Сообщений не найдено."); db.close(); return

    SEP = "─" * 62
    print(f"\n{SEP}")
    for m in reversed(rows):
        who  = m["sender_name"] or m["sender_id"] or "?"
        text = m["text"] or "(без текста)"
        print(f"[{ts(m['timestamp'])}]  {who}  →  chat:{m['chat_id']}")
        print(f"  {text[:250]}")
        print(SEP)

    print(f"\nПоказано {len(rows)} сообщений | БД: {db.path}\n")
    db.close()


if __name__ == "__main__":
    main()
