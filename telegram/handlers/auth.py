"""
Хэндлеры авторизации пользователя.

FSM:
  /start → WAIT_PHONE → WAIT_SMS → (авторизация) → CONNECTED
"""

from __future__ import annotations

import asyncio
import logging

from aiogram import Router, F, Bot
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message

from bridge.manager import manager
from bridge.max_client import session_path_for
from database import db
from telegram.sms_provider import TelegramSmsCodeProvider
from bridge.sync_worker import SyncWorker
from config import CONTROL_TOPIC_NAME

log = logging.getLogger(__name__)
router = Router()

# Активные SMS-провайдеры: tg_user_id → TelegramSmsCodeProvider
_pending_auth: dict[int, TelegramSmsCodeProvider] = {}


class AuthStates(StatesGroup):
    WAIT_PHONE = State()
    WAIT_SMS   = State()
    CONNECTED  = State()


# ── /start ────────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext, bot: Bot):
    user_id = msg.from_user.id
    user = await db.get_user(user_id)

    if user and user.status == "active":
        client = manager.get_client(user_id)
        if client:
            await msg.answer(
                "✅ Вы уже подключены к MAX.\n"
                "Все сообщения зеркалируются в вашу группу."
            )
            return

    await msg.answer(
        "👋 Добро пожаловать в <b>MAX Bridge</b>!\n\n"
        "Этот бот создаст зеркало ваших чатов MAX прямо в Telegram.\n\n"
        "Введите ваш номер телефона, зарегистрированный в MAX\n"
        "(формат: <code>+79001234567</code>):",
        parse_mode="HTML",
    )
    await state.set_state(AuthStates.WAIT_PHONE)


# ── Ввод номера телефона ──────────────────────────────────────────────────────

@router.message(AuthStates.WAIT_PHONE)
async def handle_phone(msg: Message, state: FSMContext, bot: Bot):
    phone = msg.text.strip() if msg.text else ""
    if not phone.startswith("+") or len(phone) < 10:
        await msg.answer("❌ Неверный формат. Введите номер в формате +79001234567:")
        return

    tg_user_id = msg.from_user.id
    await state.update_data(phone=phone)

    # Создаём провайдер кода — он будет ждать SMS
    provider = TelegramSmsCodeProvider(
        tg_user_id = tg_user_id,
        bot        = bot,
        chat_id    = msg.chat.id,
    )
    _pending_auth[tg_user_id] = provider

    await msg.answer(f"📲 Подключаюсь к MAX с номером <code>{phone}</code>…",
                     parse_mode="HTML")

    # Создаём пользователя в БД
    await db.create_user(
        tg_user_id   = tg_user_id,
        tg_username  = msg.from_user.username,
        max_phone    = phone,
        session_path = session_path_for(tg_user_id),
    )

    await state.set_state(AuthStates.WAIT_SMS)

    # Запускаем авторизацию в фоне — она остановится и подождёт SMS-код
    asyncio.create_task(
        _run_auth(tg_user_id=tg_user_id, phone=phone, provider=provider,
                  bot=bot, chat_id=msg.chat.id, state=state)
    )


# ── Ввод SMS-кода ─────────────────────────────────────────────────────────────

@router.message(AuthStates.WAIT_SMS)
async def handle_sms_code(msg: Message, state: FSMContext):
    code = msg.text.strip() if msg.text else ""
    if not code.isdigit():
        await msg.answer("❌ Код должен состоять из цифр. Попробуйте ещё раз:")
        return

    tg_user_id = msg.from_user.id
    provider = _pending_auth.get(tg_user_id)
    if not provider:
        await msg.answer("⚠️ Сессия устарела. Начните заново: /start")
        await state.clear()
        return

    # Передаём код провайдеру — он разбудит ждущий asyncio.Event
    provider.set_code(code)
    await msg.answer("🔐 Проверяю код…")


# ── Фоновая авторизация ───────────────────────────────────────────────────────

async def _run_auth(
    tg_user_id: int,
    phone:      str,
    provider:   TelegramSmsCodeProvider,
    bot:        Bot,
    chat_id:    int,
    state:      FSMContext,
):
    try:
        client = await manager.connect_user(
            tg_user_id        = tg_user_id,
            max_phone         = phone,
            sms_code_provider = provider,
        )

        # Авторизация прошла
        _pending_auth.pop(tg_user_id, None)
        await db.set_user_active(tg_user_id)
        await state.set_state(AuthStates.CONNECTED)

        me_name = getattr(client.me, "name", phone) or phone
        await bot.send_message(
            chat_id,
            f"✅ Авторизован в MAX как <b>{me_name}</b>\n\n"
            f"🏗 Создаю вашу группу-зеркало в Telegram…",
            parse_mode="HTML",
        )

        # Создаём супергруппу
        user = await db.get_user(tg_user_id)
        group_id = await _create_mirror_group(bot, me_name, tg_user_id)
        if not group_id:
            await bot.send_message(chat_id,
                "❌ Не удалось создать группу. Создайте вручную и добавьте бота "
                "администратором с правом управления темами.")
            return

        await db.set_user_group(tg_user_id, group_id)
        user = await db.get_user(tg_user_id)

        await bot.send_message(
            chat_id,
            f"✅ Группа создана!\n"
            f"🔄 Начинаю синхронизацию чатов…",
        )

        # Запускаем синхронизацию
        sync = SyncWorker(bot=bot, manager=manager)
        await sync.full_sync(user=user, client=client)

    except TimeoutError:
        _pending_auth.pop(tg_user_id, None)
        await state.clear()
    except Exception as e:
        log.error("Auth error for user %s: %s", tg_user_id, e)
        _pending_auth.pop(tg_user_id, None)
        await state.clear()
        await bot.send_message(
            chat_id,
            f"❌ Ошибка авторизации: <code>{e}</code>\n"
            f"Попробуйте снова: /start",
            parse_mode="HTML",
        )


async def _create_mirror_group(bot: Bot, me_name: str, tg_user_id: int) -> int | None:
    """
    Создаёт супергруппу-зеркало.

    ВАЖНО: Telegram Bot API не позволяет ботам самостоятельно создавать группы.
    Пользователь должен создать группу вручную, добавить бота администратором
    и переслать любое сообщение из группы боту.

    Здесь мы просим пользователя сделать это и ждём group_id.
    """
    # Инструкция пользователю
    await bot.send_message(
        tg_user_id,
        "📌 <b>Необходимо создать группу вручную:</b>\n\n"
        "1. Создайте новую супергруппу в Telegram\n"
        "2. Включите <b>Темы (Topics)</b> в настройках группы\n"
        "3. Добавьте этого бота администратором\n"
        "   (нужны права: управление темами + отправка сообщений)\n"
        "4. Перешлите любое сообщение из этой группы сюда\n\n"
        "Жду пересланное сообщение…",
        parse_mode="HTML",
    )
    return None  # group_id придёт через handle_forwarded_group
