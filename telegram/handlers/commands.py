"""
Хэндлеры команд бота: /status, /sync_chats, /history, и callback'и кнопок.
Вынесены из auth.py чтобы отделить команды от процесса авторизации.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from aiogram import Router, Bot, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery,
)

from bridge.manager import manager
from bridge.max_client import _detect_media
from bridge.sync_worker import SyncWorker, FLOOD_SLEEP, MEDIA_SLEEP
from database import db

if TYPE_CHECKING:
    from bridge.max_client import MaxUserClient

log = logging.getLogger(__name__)
router = Router()


# ── Клавиатуры ────────────────────────────────────────────────────────────────

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Статус", callback_data="cmd:status"),
            InlineKeyboardButton(text="🔄 Синхронизировать чаты", callback_data="cmd:sync_chats"),
        ],
        [
            InlineKeyboardButton(text="📥 Скачать историю", callback_data="cmd:history"),
        ],
    ])


def history_period_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Сутки", callback_data="hist:1"),
            InlineKeyboardButton(text="📆 Неделя", callback_data="hist:7"),
        ],
        [
            InlineKeyboardButton(text="🗓 Месяц", callback_data="hist:30"),
            InlineKeyboardButton(text="📋 Вся история", callback_data="hist:0"),
        ],
        [
            InlineKeyboardButton(text="❌ Отмена", callback_data="hist:cancel"),
        ],
    ])


# ── /status ───────────────────────────────────────────────────────────────────

async def _do_status(bot: Bot, chat_id: int, tg_user_id: int):
    """Показывает статус подключения к MAX."""
    user = await db.get_user(tg_user_id)
    if not user:
        await bot.send_message(
            chat_id,
            "❌ Вы ещё не авторизованы.\n\nДля подключения напишите /start",
            parse_mode="HTML",
        )
        return

    client = manager.get_client(tg_user_id)
    if client and client.me:
        # Получаем имя из MAX
        max_name = "?"
        try:
            me_contact = getattr(client.me, "contact", None)
            if me_contact:
                names = getattr(me_contact, "names", None)
                if names:
                    first = getattr(names[0], "first_name", "")
                    last = getattr(names[0], "last_name", "")
                    max_name = f"{first} {last}".strip() or str(getattr(me_contact, "id", "?"))
        except Exception:
            pass

        group_info = ""
        if user.tg_group_id:
            group_info = f"\n📁 Группа: <code>{user.tg_group_id}</code>"
        else:
            group_info = "\n📁 Группа: <b>не подключена</b>"

        await bot.send_message(
            chat_id,
            f"✅ <b>Подключено к MAX</b>\n\n"
            f"👤 Имя: <b>{max_name}</b>\n"
            f"📱 Телефон: <code>{user.max_phone}</code>\n"
            f"📁 MAX ID: <code>{getattr(client.me.contact, 'id', '?')}</code>"
            f"{group_info}\n"
            f"📌 Статус: <b>{user.status}</b>",
            parse_mode="HTML",
            reply_markup=main_menu_kb(),
        )
    else:
        await bot.send_message(
            chat_id,
            f"⚠️ Аккаунт MAX <b>не подключён</b>\n\n"
            f"📱 Телефон: <code>{user.max_phone}</code>\n"
            f"📌 Статус: <b>{user.status}</b>\n\n"
            f"Для переподключения напишите /start",
            parse_mode="HTML",
            reply_markup=main_menu_kb(),
        )


@router.message(Command("status"))
async def cmd_status(msg: Message, bot: Bot):
    await _do_status(bot, msg.chat.id, msg.from_user.id)


# ── /sync_chats — только список чатов, без сообщений ───────────────────────────

async def _do_sync_chats(bot: Bot, chat_id: int, tg_user_id: int):
    """Синхронизирует только список чатов (создаёт/обновляет темы), без сообщений."""
    user = await db.get_user(tg_user_id)
    if not user or user.status != "active":
        await bot.send_message(chat_id, "❌ Сначала авторизуйтесь: /start", parse_mode="HTML")
        return

    client = manager.get_client(tg_user_id)
    if not client:
        await bot.send_message(chat_id, "❌ Клиент MAX не найден. Напишите /start", parse_mode="HTML")
        return

    if not user.tg_group_id:
        await bot.send_message(chat_id, "❌ Группа не подключена. Пройдите /start", parse_mode="HTML")
        return

    await bot.send_message(
        user.tg_group_id,
        "🔄 Синхронизирую список чатов…",
        parse_mode="HTML",
    )

    # Импортируем здесь чтобы избежать циклического импорта
    from bridge.sync_worker import SyncWorker as SW
    from bridge.sync_worker import _chat_id, _chat_title

    try:
        chats = await client.get_chats()
        if not chats:
            await bot.send_message(user.tg_group_id, "⚠️ Чаты не найдены.", parse_mode="HTML")
            return

        created = 0
        updated = 0
        for max_chat in chats:
            cid = _chat_id(max_chat)
            ctitle = await _chat_title(max_chat, client)
            if not cid:
                continue

            db_chat = await db.upsert_chat(user.id, cid, ctitle)
            if db_chat.tg_topic_id:
                # Тема существует — проверяем и обновляем если нужно
                from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramNetworkError
                for attempt in range(5):
                    try:
                        result = await bot.edit_forum_topic(
                            chat_id = user.tg_group_id,
                            message_thread_id = db_chat.tg_topic_id,
                            name    = ctitle[:128],
                            )
                    except TelegramRetryAfter as e:
                        await asyncio.sleep(e.retry_after + 1)
                    except TelegramBadRequest as e:
                        description = getattr(e, "description", None) or getattr(e, "message", "?")
                        if 'TOPIC_ID_INVALID' in description:
                            log.warning("test_forum_topic: Topic not found: %s", e)
                            await asyncio.sleep(2)
                            updated += 1
                            break
                        if 'TOPIC_NOT_MODIFIED' in description:
                            log.info("test_forum_topic: Topic found: %s", e)
                            await asyncio.sleep(2)
                            updated += 1
                            break
                    except TelegramNetworkError as e:
                        # ServerDisconnectedError, ConnectionReset и прочее — пробуем ещё раз
                        wait = 2 ** attempt + 1  # 2, 3, 5 секунд
                        log.warning("send retry (network: %s), waiting %ds (attempt %d)",
                                    type(e).__name__, wait, attempt + 1)
                        await asyncio.sleep(wait)
                    except Exception as e:
                        log.error("create_forum_topic error: %s", e)
                        break
                await asyncio.sleep(2)
            else:
                # Создаём тему
                from aiogram.exceptions import TelegramRetryAfter
                for attempt in range(5):
                    try:
                        result = await bot.create_forum_topic(
                            chat_id=user.tg_group_id,
                            name=ctitle[:128],
                        )
                        topic_id = result.message_thread_id
                        await db.set_topic_id(db_chat.id, topic_id)
                        created += 1
                        break
                    except TelegramRetryAfter as e:
                        await asyncio.sleep(e.retry_after + 1)
                    except Exception as e:
                        log.error("create_forum_topic error: %s", e)
                        break
                await asyncio.sleep(2)

        await bot.send_message(
            user.tg_group_id,
            f"✅ Список чатов синхронизирован:\n"
            f"• Создано тем: <b>{created}</b>\n"
            f"• Обновлено тем: <b>{updated}</b>\n\n"
            f"Для скачивания истории используйте кнопку <b>Скачать историю</b> в теме чата.",
            parse_mode="HTML",
        )
    except Exception as e:
        log.error("sync_chats error: %s", e)
        await bot.send_message(user.tg_group_id, f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML")


@router.message(Command("sync_chats"))
async def cmd_sync_chats(msg: Message, bot: Bot):
    await _do_sync_chats(bot, msg.chat.id, msg.from_user.id)


# ── /sync — полная синхронизация (оставлено) ─────────────────────────────────

@router.message(Command("sync"))
async def cmd_sync(msg: Message, bot: Bot):
    """Принудительно запускает полную синхронизацию (чаты + история)."""
    tg_user_id = msg.from_user.id
    user = await db.get_user(tg_user_id)
    if not user or user.status != "active":
        await msg.answer("❌ Сначала авторизуйтесь: /start")
        return
    if not user.tg_group_id:
        await msg.answer("❌ Группа не подключена. Пройдите /start")
        return
    client = manager.get_client(tg_user_id)
    if not client:
        await msg.answer("❌ Клиент MAX не найден. Перезапустите: /start")
        return

    await msg.answer("🔄 Запускаю полную синхронизацию…")
    sync = SyncWorker(bot=bot, manager=manager)
    asyncio.create_task(sync.full_sync(user=user, client=client))


# ── Кнопка "Скачать историю" в топике ────────────────────────────────────────

@router.message(F.chat.type.in_({"supergroup", "group"}), Command("history"))
async def cmd_history_in_topic(msg: Message, bot: Bot):
    """Команда /history вызвана внутри темы — показываем выбор периода."""
    if not msg.message_thread_id:
        return

    tg_user_id = await _find_owner(msg.chat.id)
    if not tg_user_id:
        return

    user = await db.get_user(tg_user_id)
    if not user or user.status != "active":
        return

    chat = await db.get_chat_by_topic(user.id, msg.message_thread_id)
    if not chat:
        await msg.answer("❌ Этот топик не привязан к чату MAX.")
        return

    await msg.answer(
        f"📥 <b>Скачать историю чата:</b> {chat.max_chat_title}\n\n"
        f"Выберите период:",
        parse_mode="HTML",
        reply_markup=history_period_kb(),
    )


# ── Callback'и кнопок меню ────────────────────────────────────────────────────

@router.callback_query(F.data == "cmd:status")
async def cb_status(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    # Отвечаем в личные сообщения
    await _do_status(bot, callback.from_user.id, callback.from_user.id)


@router.callback_query(F.data == "cmd:sync_chats")
async def cb_sync_chats(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    await _do_sync_chats(bot, callback.from_user.id, callback.from_user.id)


@router.callback_query(F.data == "cmd:history")
async def cb_history_menu(callback: CallbackQuery, bot: Bot):
    """Кнопка 'Скачать историю' из главного меню — подсказка."""
    await callback.answer()
    await bot.send_message(
        callback.from_user.id,
        "📥 Чтобы скачать историю конкретного чата:\n\n"
        "1. Откройте <b>группу-зеркало</b> в Telegram\n"
        "2. Зайдите в нужный <b>топик (тему)</b>\n"
        "3. Напишите команду <code>/history</code>\n\n"
        "Или воспользуйтесь полной синхронизацией: /sync",
        parse_mode="HTML",
    )


# ── Callback'и выбора периода истории ────────────────────────────────────────

@router.callback_query(F.data.startswith("hist:"))
async def cb_history_period(callback: CallbackQuery, bot: Bot):
    """Обработка выбора периода скачивания истории."""
    await callback.answer()

    if not callback.message.message_thread_id:
        await callback.answer("❌ Команда работает только внутри топика", show_alert=True)
        return

    tg_user_id = await _find_owner(callback.message.chat.id)
    if not tg_user_id:
        return

    user = await db.get_user(tg_user_id)
    if not user or user.status != "active":
        await callback.message.reply("❌ Пользователь не авторизован.")
        return

    chat = await db.get_chat_by_topic(user.id, callback.message.message_thread_id)
    if not chat:
        await callback.message.reply("❌ Этот топик не привязан к чату MAX.")
        return

    # Разбираем период
    parts = callback.data.split(":")
    if len(parts) < 2:
        return
    period = parts[1]

    if period == "cancel":
        await callback.message.edit_reply_markup(reply_markup=None)
        return

    days = int(period)  # 0 = вся история, 1/7/30 = сутки/неделя/месяц
    client = manager.get_client(tg_user_id)
    if not client:
        await callback.message.reply("❌ Клиент MAX не найден.")
        return

    if days == 0:
        from_ts = client.me.contact.registration_time
        label = "всю историю"
    else:
        from_ts = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        labels = {1: "сутки", 7: "неделю", 30: "месяц"}
        label = labels.get(days, f"{days} дней")

    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.reply(
        f"📥 Скачиваю историю за <b>{label}</b>…\n"
        f"Это может занять некоторое время.",
        parse_mode="HTML",
    )

    # Запускаем в фоне
    asyncio.create_task(
        _sync_single_chat(bot, user, client, chat, from_ts)
    )


# ── Фоновая синхронизация одного чата ────────────────────────────────────────

async def _sync_single_chat(
    bot: Bot,
    user,
    client: "MaxUserClient",
    db_chat,
    from_ts: int,
):
    """Скачивает историю одного чата за указанный период."""
    from telegram.sender import (
        format_history_message,
        send_media_to_telegram_topic,
        send_text_to_topic,
    )
    to_ts = int(time.time() * 1000)
    sent_count = 0
    error_count = 0

    try:
        messages = await client.get_history(
            max_chat_id = db_chat.max_chat_id,
            from_ts     = from_ts,
            to_ts       = to_ts,
            limit       = 100,
        )
        if not messages:
            await bot.send_message(
                user.tg_group_id,
                f"✅ Нет новых сообщений в чате <b>{db_chat.max_chat_title}</b> за выбранный период.",
                parse_mode="HTML",
                message_thread_id=db_chat.tg_topic_id,
            )
            return

        log.info("Syncing single chat %s: %d messages", db_chat.max_chat_id, len(messages))

        for msg in messages:
            try:
                msg_id    = str(getattr(msg, "id", "") or "")
                timestamp = getattr(msg, "time", 0) or getattr(msg, "timestamp", 0)
                sender    = getattr(msg, "sender", None)
                text      = getattr(msg, "text", "") or ""
                sender_name = await client.get_client(sender) if sender else ""
                has_media, media_type = _detect_media(msg)

                # Проверяем дедупликацию
                existing = await db.get_message_by_max_for_user(
                    user.id, db_chat.id, msg_id,
                )
                if existing and existing.tg_msg_id:
                    continue

                formatted = format_history_message(
                    sender_name = sender_name,
                    text        = text,
                    timestamp   = timestamp,
                    has_media   = False,
                    media_type  = media_type,
                )

                if has_media:
                    sent_msg = await send_media_to_telegram_topic(
                        bot          = bot,
                        group_id     = user.tg_group_id,
                        topic_id     = db_chat.tg_topic_id,
                        text         = formatted,
                        client       = client,
                        msg          = msg,
                        caption      = text[:1024] if text else "",
                        max_chat_id = db_chat.max_chat_id,
                    )
                else:
                    sent_msg = await send_text_to_topic(
                        bot      = bot,
                        group_id = user.tg_group_id,
                        topic_id = db_chat.tg_topic_id,
                        text     = formatted,
                    )

                if sent_msg:
                    # Сохраняем маппинг (если записи ещё нет в БД)
                    if not existing:
                        await db.save_message(
                            user_id=user.id, chat_id=db_chat.id,
                            direction="max_to_tg", timestamp=timestamp,
                            max_sender_id=sender, max_msg_id=msg_id,
                            tg_msg_id=sent_msg.message_id, has_media=has_media,
                        )
                    else:
                        await db.update_tg_msg_id_by_max(
                            user.id, db_chat.id, msg_id, sent_msg.message_id,
                        )
                    sent_count += 1

                await asyncio.sleep(MEDIA_SLEEP if has_media else FLOOD_SLEEP)
            except Exception as e:
                log.error("Sync single chat msg error: %s", e)
                error_count += 1

        await bot.send_message(
            user.tg_group_id,
            f"✅ История чата <b>{db_chat.max_chat_title}</b> загружена.\n"
            f"Загружено: {sent_count}, ошибок: {error_count}",
            parse_mode="HTML",
            message_thread_id=db_chat.tg_topic_id,
        )

    except Exception as e:
        log.error("Sync single chat error: %s", e)
        await bot.send_message(
            user.tg_group_id,
            f"❌ Ошибка синхронизации: <code>{e}</code>",
            parse_mode="HTML",
            message_thread_id=db_chat.tg_topic_id,
        )


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _find_owner(tg_group_id: int) -> int | None:
    """Находит tg_user_id владельца по group_id."""
    import aiosqlite
    from config import DB_PATH
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            "SELECT tg_user_id FROM users WHERE tg_group_id=?", (tg_group_id,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else None