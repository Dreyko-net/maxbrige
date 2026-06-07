"""
SQLite база данных для хранения сообщений MAX.
"""

import sqlite3
import logging
from datetime import datetime

log = logging.getLogger(__name__)


class Database:
    def __init__(self, path: str = "messages.db"):
        self.path = path
        self.conn: sqlite3.Connection | None = None

    def init(self):
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        log.info("БД инициализирована: %s", self.path)

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id  TEXT,
                sender_id   TEXT    NOT NULL DEFAULT '',
                sender_name TEXT    DEFAULT '',
                chat_id     TEXT    NOT NULL DEFAULT '',
                text        TEXT    DEFAULT '',
                timestamp   INTEGER NOT NULL,
                received_at TEXT    NOT NULL,
                raw         TEXT    DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_ts   ON messages(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_chat ON messages(chat_id);
            CREATE INDEX IF NOT EXISTS idx_sndr ON messages(sender_id);

            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        self.conn.commit()

    def save_message(
        self,
        message_id:  str,
        sender_id:   str,
        sender_name: str,
        chat_id:     str,
        text:        str,
        timestamp:   int,
        raw:         str = "",
    ) -> int | None:
        received_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            cur = self.conn.execute(
                """
                INSERT INTO messages
                    (message_id, sender_id, sender_name, chat_id, text,
                     timestamp, received_at, raw)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (message_id, sender_id, sender_name, chat_id, text,
                 timestamp, received_at, raw),
            )
            self.conn.commit()
            return cur.lastrowid
        except Exception as e:
            log.error("Ошибка записи в БД: %s", e)
            return None

    def set_meta(self, key: str, value: str):
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                (key, value),
            )
            self.conn.commit()
        except Exception as e:
            log.error("Ошибка записи meta: %s", e)

    def get_messages(
        self,
        chat_id:   str | None = None,
        sender_id: str | None = None,
        limit:     int = 50,
        offset:    int = 0,
    ) -> list[sqlite3.Row]:
        q = "SELECT * FROM messages WHERE 1=1"
        p: list = []
        if chat_id:
            q += " AND chat_id = ?"; p.append(chat_id)
        if sender_id:
            q += " AND sender_id = ?"; p.append(sender_id)
        q += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
        p += [limit, offset]
        return self.conn.execute(q, p).fetchall()

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]

    def close(self):
        if self.conn:
            self.conn.close()
