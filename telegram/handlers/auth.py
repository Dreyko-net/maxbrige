"""
Хэндлеры авторизации пользователя.
FSM: /start → WAIT_PHONE → WAIT_SMS → CONNECTED
"""

from __future__ import annotations

import asyncio
import logging

from aiogram import Router, F, Bot
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)

from bridge.manager import manager
from bridge.max_client import session_path_for
from database import db
from telegram.sms_provider import TelegramSmsCodeProvider
from bridge.sync_worker import SyncWorker

log = logging.getLogger(__name__)
router = Router()

_pending_auth: dict[int, TelegramSmsCodeProvider] = {}


class AuthStates(StatesGroup):
    WAIT_PHONE    = State()
    WAIT_SMS      = State()
    WAIT_GROUP    = State()   # ждём пересланное сообщение из группы
    CONNECTED     = State()


def _step1_kb() -> InlineKeyboardMarkup:
    """Шаг 1 — группа создана."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Группа создана", callback_data="group_created"),
    ]])


def _step2_kb(bot_username: str) -> InlineKeyboardMarkup:
    """Шаг 2 — добавить бота администратором."""
    add_url = (
        f"https://t.me/{bot_username}?startgroup=setup"
        f"&admin=manage_topics+post_messages+delete_messages"
    )
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить бота в группу", url=add_url)],
        [InlineKeyboardButton(text="✅ Бот добавлен", callback_data="group_added")],
    ])


def _forward_hint_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❓ Как переслать сообщение?", callback_data="forward_help"),
    ]])


async def _send_group_instructions(bot: Bot, chat_id: int, tg_user_id: int):
    await bot.send_message(
        chat_id,
        "🏗 <b>Шаг 1 из 2 — создайте группу-зеркало</b>\n\n"
        "1. Откройте Telegram\n"
        "2. Создайте новую супергруппу (Новая группа → добавьте любого участника)\n"
        "3. Зайдите в <b>Настройки группы → Темы</b> и включите их\n\n"
        "Нажмите кнопку когда группа готова.",
        parse_mode="HTML",
        reply_markup=_step1_kb(),
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
            await msg.answer("✅ Вы уже подключены к MAX.\nСообщения зеркалируются в вашу группу.")
            return

        if client and not user.tg_group_id:
            log.info("[/start] no group yet, showing setup")
            await _send_group_instructions(bot, msg.chat.id, user_id)
            await state.set_state(AuthStates.WAIT_GROUP)
            return

        log.info("[/start] client not in pool, reconnecting")
        await msg.answer("🔄 Переподключаюсь к MAX…")
        asyncio.create_task(_reconnect(user_id, user.max_phone, bot, msg.chat.id, state))
        return

    log.info("[/start] fresh auth flow")
    await msg.answer(
        "👋 Добро пожаловать в <b>MAX Bridge</b>!\n\n"
        "Этот бот создаст зеркало ваших чатов MAX прямо в Telegram.\n\n"
        "Введите ваш номер телефона, зарегистрированный в MAX\n"
        "(формат: <code>+79001234567</code>):",
        parse_mode="HTML",
    )
    await state.set_state(AuthStates.WAIT_PHONE)


# ── Телефон ───────────────────────────────────────────────────────────────────

@router.message(AuthStates.WAIT_PHONE)
async def handle_phone(msg: Message, state: FSMContext, bot: Bot):
    phone = msg.text.strip() if msg.text else ""
    if not phone.startswith("+") or len(phone) < 10:
        await msg.answer("❌ Неверный формат. Введите номер в формате +79001234567:")
        return

    tg_user_id = msg.from_user.id
    log.info("[auth] phone entered user=%s", tg_user_id)
    await state.update_data(phone=phone)

    provider = TelegramSmsCodeProvider(tg_user_id=tg_user_id, bot=bot, chat_id=msg.chat.id)
    _pending_auth[tg_user_id] = provider

    await msg.answer(f"📲 Подключаюсь к MAX с номером <code>{phone}</code>…", parse_mode="HTML")

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


# ── SMS-код ───────────────────────────────────────────────────────────────────

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


# ── Callback: группа создана (шаг 1) ─────────────────────────────────────────

@router.callback_query(F.data == "group_created")
async def cb_group_created(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    me = await bot.get_me()
    await callback.message.answer(
        "✅ Отлично!"
        "🏗 <b>Шаг 2 из 2 — добавьте бота в группу</b>"
        "Нажмите кнопку ниже — Telegram предложит выбрать вашу группу "
        "и автоматически назначит бота администратором с нужными правами.",
        parse_mode="HTML",
        reply_markup=_step2_kb(me.username),
    )


# ── Callback: бот добавлен в группу (шаг 2) ──────────────────────────────────

@router.callback_query(F.data == "group_added")
async def cb_group_added(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(
        "✅ Бот добавлен!"
        "🔗 <b>Последний шаг — свяжите группу с ботом:</b>"
        "1. Зайдите в созданную группу"
        "2. Нажмите на любое сообщение → <b>Переслать</b>"
        "3. Выберите получателем <b>этого бота</b>"
        "Бот определит группу и начнёт синхронизацию.",
        parse_mode="HTML",
        reply_markup=_forward_hint_kb(),
    )
    await state.set_state(AuthStates.WAIT_GROUP)


@router.callback_query(F.data == "forward_help")
async def cb_forward_help(callback: CallbackQuery):
    await callback.answer(
        "Откройте группу → долгое нажатие на сообщение → "
        "Переслать → найдите этого бота в списке",
        show_alert=True,
    )


# ── Пересланное сообщение из группы ──────────────────────────────────────────

@router.message(AuthStates.WAIT_GROUP, F.forward_from_chat)
@router.message(AuthStates.CONNECTED,  F.forward_from_chat)
async def handle_forwarded_group(msg: Message, state: FSMContext, bot: Bot):
    tg_user_id = msg.from_user.id
    user = await db.get_user(tg_user_id)
    if not user:
        return

    group = msg.forward_from_chat
    if not group or group.type not in ("supergroup", "group"):
        await msg.answer("❌ Перешлите сообщение из <b>супергруппы</b>.", parse_mode="HTML")
        return

    group_id = group.id

    # Проверяем права бота
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(group_id, me.id)
        if member.status not in ("administrator", "creator"):
            await msg.answer(
                "❌ Бот не является администратором этой группы.\n"
                "Добавьте его через кнопку выше и попробуйте снова."
            )
            return
    except Exception as e:
        await msg.answer(f"❌ Не могу получить доступ к группе: <code>{e}</code>",
                         parse_mode="HTML")
        return

    # Проверяем включены ли Topics
    try:
        chat_info = await bot.get_chat(group_id)
        if not getattr(chat_info, "is_forum", False):
            await msg.answer(
                "⚠️ В группе не включены <b>Темы (Topics)</b>.\n\n"
                "Включите их: Настройки группы → Темы → Включить\n"
                "Затем снова перешлите сообщение сюда.",
                parse_mode="HTML",
            )
            return
    except Exception:
        pass  # Если не можем проверить — продолжаем

    await db.set_user_group(tg_user_id, group_id)
    user = await db.get_user(tg_user_id)

    await msg.answer(
        "✅ Группа подключена! Начинаю синхронизацию чатов MAX…\n"
        "Прогресс буду отправлять в группу."
    )
    await state.set_state(AuthStates.CONNECTED)

    client = manager.get_client(tg_user_id)
    if client:
        sync = SyncWorker(bot=bot, manager=manager)
        asyncio.create_task(sync.full_sync(user=user, client=client))
    else:
        await msg.answer("⚠️ Клиент MAX не найден. Попробуйте /start")


# ── Фоновая авторизация ───────────────────────────────────────────────────────

async def _run_auth(tg_user_id, phone, provider, bot, chat_id, state):
    log.info("[auth] _run_auth started user=%s", tg_user_id)
    try:
        client = await manager.connect_user(
            tg_user_id=tg_user_id, max_phone=phone, sms_code_provider=provider)
        log.info("[auth] connect_user done user=%s me=%s", tg_user_id, client.me)

        _pending_auth.pop(tg_user_id, None)
        await db.set_user_active(tg_user_id)
        await state.set_state(AuthStates.WAIT_GROUP)

        me_name = getattr(client.me, "name", phone) or phone
        await bot.send_message(
            chat_id,
            f"✅ Авторизован в MAX как <b>{me_name}</b>",
            parse_mode="HTML",
        )
        await _send_group_instructions(bot, chat_id, tg_user_id)

    except (asyncio.CancelledError,):
        log.info("[auth] cancelled user=%s", tg_user_id)
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


async def _reconnect(tg_user_id, phone, bot, chat_id, state):
    log.info("[reconnect] starting user=%s", tg_user_id)
    try:
        client = await manager.connect_user(
            tg_user_id=tg_user_id, max_phone=phone, sms_code_provider=None)
        log.info("[reconnect] done user=%s", tg_user_id)
        user = await db.get_user(tg_user_id)
        if user and not user.tg_group_id:
            await _send_group_instructions(bot, chat_id, tg_user_id)
            await state.set_state(AuthStates.WAIT_GROUP)
        else:
            await bot.send_message(chat_id, "✅ Переподключено к MAX.")
    except Exception as e:
        log.error("[reconnect] error user=%s: %s", tg_user_id, e, exc_info=True)
        await bot.send_message(chat_id,
            f"❌ Ошибка переподключения: <code>{e}</code>", parse_mode="HTML")