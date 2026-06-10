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

log = logging.getLogger(__name__)
router = Router()

# Активные SMS-провайдеры: tg_user_id → TelegramSmsCodeProvider
_pending_auth: dict[int, TelegramSmsCodeProvider] = {}


class AuthStates(StatesGroup):
    WAIT_PHONE = State()
    WAIT_SMS   = State()
    CONNECTED  = State()


GROUP_INSTRUCTIONS = (
    "📌 <b>Создайте группу-зеркало:</b>\n\n"
    "1. Создайте новую супергруппу в Telegram\n"
    "2. Включите <b>Темы (Topics / Форум)</b> в настройках группы\n"
    "3. Добавьте этого бота администратором\n"
    "   (права: управление темами + отправка сообщений)\n"
    "4. Перешлите любое сообщение из этой группы в этот чат\n\n"
    "Жду пересланное сообщение…"
)


# ── /start ────────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext, bot: Bot):
    user_id = msg.from_user.id
    log.info("[/start] user_id=%s", user_id)

    user = await db.get_user(user_id)
    log.info("[/start] db_user=%s status=%s tg_group_id=%s",
             user.id if user else None,
             user.status if user else None,
             user.tg_group_id if user else None)

    if user and user.status == "active":
        client = manager.get_client(user_id)
        log.info("[/start] client_in_pool=%s", client is not None)

        if client and user.tg_group_id:
            await msg.answer("✅ Вы уже подключены к MAX.\nВсе сообщения зеркалируются в вашу группу.")
            return

        if client and not user.tg_group_id:
            # MAX подключён, но группа ещё не создана
            log.info("[/start] client connected but no group yet, showing instructions")
            await msg.answer(GROUP_INSTRUCTIONS, parse_mode="HTML")
            await state.set_state(AuthStates.CONNECTED)
            return

        # Клиента нет в пуле (перезапуск не восстановил) — переподключаем
        log.info("[/start] user is active but client not in pool, reconnecting")
        await msg.answer("🔄 Переподключаюсь к MAX…")
        asyncio.create_task(
            _reconnect(user_id, user.max_phone, bot, msg.chat.id, state)
        )
        return

    log.info("[/start] no active user, starting fresh auth flow")
    await msg.answer(
        "👋 Добро пожаловать в <b>MAX Bridge</b>!\n\n"
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
    log.info("[auth] phone entered user=%s phone=%s", tg_user_id, phone)
    await state.update_data(phone=phone)

    provider = TelegramSmsCodeProvider(
        tg_user_id = tg_user_id,
        bot        = bot,
        chat_id    = msg.chat.id,
    )
    _pending_auth[tg_user_id] = provider

    await msg.answer(f"📲 Подключаюсь к MAX с номером <code>{phone}</code>…",
                     parse_mode="HTML")

    await db.create_user(
        tg_user_id   = tg_user_id,
        tg_username  = msg.from_user.username,
        max_phone    = phone,
        session_path = session_path_for(tg_user_id),
    )

    await state.set_state(AuthStates.WAIT_SMS)

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
    log.info("[auth] sms code entered user=%s", tg_user_id)
    provider = _pending_auth.get(tg_user_id)
    if not provider:
        await msg.answer("⚠️ Сессия устарела. Начните заново: /start")
        await state.clear()
        return

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
    log.info("[auth] _run_auth started user=%s", tg_user_id)
    try:
        client = await manager.connect_user(
            tg_user_id        = tg_user_id,
            max_phone         = phone,
            sms_code_provider = provider,
        )
        log.info("[auth] connect_user done user=%s me=%s", tg_user_id, client.me)

        _pending_auth.pop(tg_user_id, None)
        await db.set_user_active(tg_user_id)
        log.info("[auth] user set active user=%s", tg_user_id)

        await state.set_state(AuthStates.CONNECTED)

        me_name = getattr(client.me, "name", phone) or phone
        log.info("[auth] sending success message user=%s me_name=%s", tg_user_id, me_name)

        await bot.send_message(
            chat_id,
            f"✅ Авторизован в MAX как <b>{me_name}</b>",
            parse_mode="HTML",
        )
        await bot.send_message(chat_id, GROUP_INSTRUCTIONS, parse_mode="HTML")
        log.info("[auth] instructions sent user=%s", tg_user_id)

    except (asyncio.CancelledError,):
        log.info("[auth] cancelled (sms timeout) user=%s", tg_user_id)
        _pending_auth.pop(tg_user_id, None)
        await state.clear()
    except Exception as e:
        log.error("[auth] error user=%s: %s", tg_user_id, e, exc_info=True)
        _pending_auth.pop(tg_user_id, None)
        await state.clear()
        await bot.send_message(
            chat_id,
            f"❌ Ошибка авторизации: <code>{e}</code>\nПопробуйте снова: /start",
            parse_mode="HTML",
        )


async def _reconnect(tg_user_id: int, phone: str, bot: Bot, chat_id: int, state: FSMContext):
    """Переподключает существующего пользователя без SMS (сессия есть на диске)."""
    log.info("[reconnect] starting user=%s", tg_user_id)
    try:
        client = await manager.connect_user(
            tg_user_id        = tg_user_id,
            max_phone         = phone,
            sms_code_provider = None,
        )
        log.info("[reconnect] done user=%s me=%s", tg_user_id, client.me)
        user = await db.get_user(tg_user_id)
        if user and not user.tg_group_id:
            await bot.send_message(chat_id, GROUP_INSTRUCTIONS, parse_mode="HTML")
            await state.set_state(AuthStates.CONNECTED)
        else:
            await bot.send_message(chat_id, "✅ Переподключено к MAX.")
    except Exception as e:
        log.error("[reconnect] error user=%s: %s", tg_user_id, e, exc_info=True)
        await bot.send_message(chat_id, f"❌ Ошибка переподключения: <code>{e}</code>", parse_mode="HTML")