"""
Слой базы данных.  Все запросы к SQLite через aiosqlite.
"""

from __future__ import annotations

import time
import aiosqlite
from dataclasses import dataclass
from typing import Optional

from config import DB_PATH, MEDIA_CACHE_HOURS

# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class User:
    id:           int
    tg_user_id:   int
    tg_username:  Optional[str]
    max_phone:    str
    session_path: str          # work_dir для pymax
    tg_group_id:  Optional[int]
    status:       str
    created_at:   int


@dataclass
class Chat:
    id:             int
    user_id:        int
    max_chat_id:    str
    max_chat_title: Optional[str]
    tg_topic_id:    Optional[int]
    last_synced_at: Optional[int]


@dataclass
class Message:
    id:         int
    user_id:    int
    chat_id:    int
    max_msg_id: Optional[str]
    tg_msg_id:  Optional[int]
    direction:  str            # 'max_to_tg' | 'tg_to_max'
    has_media:  bool
    timestamp:  int


@dataclass
class MediaCache:
    id:          int
    message_id:  int
    max_file_id: Optional[str]
    tg_file_id:  Optional[str]
    file_type:   str
    file_size:   int
    cached_at:   int
    expires_at:  int


# ── DDL ───────────────────────────────────────────────────────────────────────

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS users (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_user_id      INTEGER UNIQUE NOT NULL,
    tg_username     TEXT,
    max_phone       TEXT NOT NULL,
    session_path    TEXT NOT NULL,
    tg_group_id     INTEGER,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS chats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    max_chat_id     TEXT NOT NULL,
    max_chat_title  TEXT,
    tg_topic_id     INTEGER,
    last_synced_at  INTEGER,
    UNIQUE(user_id, max_chat_id)
);

CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    chat_id     INTEGER NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    max_msg_id  TEXT,
    tg_msg_id   INTEGER,
    direction   TEXT NOT NULL,
    has_media   INTEGER NOT NULL DEFAULT 0,
    timestamp   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_msg_tg  ON messages(tg_msg_id);
CREATE INDEX IF NOT EXISTS idx_msg_max ON messages(max_msg_id);
CREATE INDEX IF NOT EXISTS idx_msg_ts  ON messages(timestamp DESC);

CREATE TABLE IF NOT EXISTS media_cache (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id  INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    max_file_id TEXT,
    tg_file_id  TEXT,
    file_type   TEXT NOT NULL,
    file_size   INTEGER NOT NULL DEFAULT 0,
    cached_at   INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_media_exp ON media_cache(expires_at);
"""


# ── Database class ────────────────────────────────────────────────────────────

class Database:
    def __init__(self):
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._db = await aiosqlite.connect(DB_PATH)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA)
        await self._db.commit()

    async def close(self):
        if self._db:
            await self._db.close()

    # ── Users ─────────────────────────────────────────────────────────────────

    async def get_user(self, tg_user_id: int) -> Optional[User]:
        async with self._db.execute(
            "SELECT * FROM users WHERE tg_user_id = ?", (tg_user_id,)
        ) as cur:
            row = await cur.fetchone()
            return _user(row) if row else None

    async def get_active_users(self) -> list[User]:
        async with self._db.execute(
            "SELECT * FROM users WHERE status = 'active'"
        ) as cur:
            return [_user(r) for r in await cur.fetchall()]

    async def create_user(
        self,
        tg_user_id: int,
        tg_username: Optional[str],
        max_phone: str,
        session_path: str,
    ) -> User:
        now = int(time.time())
        await self._db.execute(
            """INSERT INTO users
               (tg_user_id, tg_username, max_phone, session_path, status, created_at)
               VALUES (?, ?, ?, ?, 'pending', ?)
               ON CONFLICT(tg_user_id) DO UPDATE SET
                 max_phone=excluded.max_phone,
                 session_path=excluded.session_path,
                 status='pending'
            """,
            (tg_user_id, tg_username, max_phone, session_path, now),
        )
        await self._db.commit()
        return await self.get_user(tg_user_id)

    async def set_user_active(self, tg_user_id: int):
        await self._db.execute(
            "UPDATE users SET status='active' WHERE tg_user_id=?", (tg_user_id,)
        )
        await self._db.commit()

    async def set_user_group(self, tg_user_id: int, tg_group_id: int):
        await self._db.execute(
            "UPDATE users SET tg_group_id=? WHERE tg_user_id=?",
            (tg_group_id, tg_user_id),
        )
        await self._db.commit()

    # ── Chats ─────────────────────────────────────────────────────────────────

    async def upsert_chat(
        self,
        user_id: int,
        max_chat_id: str,
        max_chat_title: str,
        tg_topic_id: Optional[int] = None,
    ) -> Chat:
        await self._db.execute(
            """INSERT INTO chats (user_id, max_chat_id, max_chat_title, tg_topic_id)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, max_chat_id) DO UPDATE SET
                 max_chat_title=excluded.max_chat_title,
                 tg_topic_id=COALESCE(excluded.tg_topic_id, tg_topic_id)
            """,
            (user_id, max_chat_id, max_chat_title, tg_topic_id),
        )
        await self._db.commit()
        return await self.get_chat_by_max(user_id, max_chat_id)

    async def get_chat_by_max(self, user_id: int, max_chat_id: str) -> Optional[Chat]:
        async with self._db.execute(
            "SELECT * FROM chats WHERE user_id=? AND max_chat_id=?",
            (user_id, max_chat_id),
        ) as cur:
            row = await cur.fetchone()
            return _chat(row) if row else None

    async def get_chat_by_topic(self, user_id: int, tg_topic_id: int) -> Optional[Chat]:
        async with self._db.execute(
            "SELECT * FROM chats WHERE user_id=? AND tg_topic_id=?",
            (user_id, tg_topic_id),
        ) as cur:
            row = await cur.fetchone()
            return _chat(row) if row else None

    async def get_user_chats(self, user_id: int) -> list[Chat]:
        async with self._db.execute(
            "SELECT * FROM chats WHERE user_id=?", (user_id,)
        ) as cur:
            return [_chat(r) for r in await cur.fetchall()]

    async def set_chat_synced(self, chat_id: int):
        await self._db.execute(
            "UPDATE chats SET last_synced_at=? WHERE id=?",
            (int(time.time()), chat_id),
        )
        await self._db.commit()

    async def set_topic_id(self, chat_id: int, tg_topic_id: int):
        await self._db.execute(
            "UPDATE chats SET tg_topic_id=? WHERE id=?",
            (tg_topic_id, chat_id),
        )
        await self._db.commit()

    async def delete_topic_id(self, user_id: int, chat_id: int, tg_topic_id: int):
        await self._db.execute(
            "DELETE FROM chats WHERE user_id=? AND max_chat_id=? AND tg_topic_id=?",
            (user_id, chat_id, tg_topic_id),
        )
        await self._db.commit()
    # ── Messages ──────────────────────────────────────────────────────────────

    async def save_message(
        self,
        user_id: int,
        chat_id: int,
        direction: str,
        timestamp: int,
        max_msg_id: Optional[str] = None,
        tg_msg_id: Optional[int] = None,
        has_media: bool = False,
    ) -> int:
        cur = await self._db.execute(
            """INSERT INTO messages
               (user_id, chat_id, max_msg_id, tg_msg_id, direction, has_media, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (user_id, chat_id, max_msg_id, tg_msg_id, direction,
             int(has_media), timestamp),
        )
        await self._db.commit()
        return cur.lastrowid

    async def get_message_by_tg(self, tg_msg_id: int) -> Optional[Message]:
        async with self._db.execute(
            "SELECT * FROM messages WHERE tg_msg_id=?", (tg_msg_id,)
        ) as cur:
            row = await cur.fetchone()
            return _message(row) if row else None

    async def get_message_by_max(self, max_msg_id: str) -> Optional[Message]:
        async with self._db.execute(
            "SELECT * FROM messages WHERE max_msg_id=?", (max_msg_id,)
        ) as cur:
            row = await cur.fetchone()
            return _message(row) if row else None

    async def update_tg_msg_id(self, message_id: int, tg_msg_id: int):
        await self._db.execute(
            "UPDATE messages SET tg_msg_id=? WHERE id=?", (tg_msg_id, message_id)
        )
        await self._db.commit()

    # ── Media cache ───────────────────────────────────────────────────────────

    async def save_media(
        self,
        message_id: int,
        file_type: str,
        file_size: int = 0,
        max_file_id: Optional[str] = None,
        tg_file_id: Optional[str] = None,
    ) -> int:
        now = int(time.time())
        expires = now + MEDIA_CACHE_HOURS * 3600
        cur = await self._db.execute(
            """INSERT INTO media_cache
               (message_id, max_file_id, tg_file_id, file_type, file_size,
                cached_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (message_id, max_file_id, tg_file_id, file_type, file_size, now, expires),
        )
        await self._db.commit()
        return cur.lastrowid

    async def get_media(self, message_id: int) -> Optional[MediaCache]:
        async with self._db.execute(
            "SELECT * FROM media_cache WHERE message_id=?", (message_id,)
        ) as cur:
            row = await cur.fetchone()
            return _media(row) if row else None

    async def update_tg_file_id(self, media_id: int, tg_file_id: str):
        await self._db.execute(
            "UPDATE media_cache SET tg_file_id=? WHERE id=?", (tg_file_id, media_id)
        )
        await self._db.commit()

    async def purge_expired_media(self) -> int:
        """Удаляет просроченные записи медиакэша. Возвращает количество удалённых."""
        now = int(time.time())
        cur = await self._db.execute(
            "DELETE FROM media_cache WHERE expires_at < ?", (now,)
        )
        await self._db.commit()
        return cur.rowcount


# ── Row converters ────────────────────────────────────────────────────────────

def _user(r) -> User:
    return User(
        id=r["id"], tg_user_id=r["tg_user_id"], tg_username=r["tg_username"],
        max_phone=r["max_phone"], session_path=r["session_path"],
        tg_group_id=r["tg_group_id"], status=r["status"], created_at=r["created_at"],
    )

def _chat(r) -> Chat:
    return Chat(
        id=r["id"], user_id=r["user_id"], max_chat_id=r["max_chat_id"],
        max_chat_title=r["max_chat_title"], tg_topic_id=r["tg_topic_id"],
        last_synced_at=r["last_synced_at"],
    )

def _message(r) -> Message:
    return Message(
        id=r["id"], user_id=r["user_id"], chat_id=r["chat_id"],
        max_msg_id=r["max_msg_id"], tg_msg_id=r["tg_msg_id"],
        direction=r["direction"], has_media=bool(r["has_media"]),
        timestamp=r["timestamp"],
    )

def _media(r) -> MediaCache:
    return MediaCache(
        id=r["id"], message_id=r["message_id"], max_file_id=r["max_file_id"],
        tg_file_id=r["tg_file_id"], file_type=r["file_type"],
        file_size=r["file_size"], cached_at=r["cached_at"], expires_at=r["expires_at"],
    )


# Глобальный экземпляр
db = Database()
