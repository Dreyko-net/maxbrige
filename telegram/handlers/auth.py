"""
Хэндлеры авторизации пользователя.
FSM: /start → WAIT_PHONE → WAIT_SMS → WAIT_GROUP → CONNECTED
"""

from __future__ import annotations

import asyncio
import logging

from aiogram import Router, F, Bot
from aiogram.filters import CommandStart, Command
from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter, IS_NOT_MEMBER, IS_MEMBER
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, ChatMemberUpdated,
)

from bridge.manager import manager
from bridge.max_client import session_path_for
from database import db
from telegram.sender import _send_with_retry
from telegram.sms_provider import TelegramSmsCodeProvider
from telegram.password_provider import TelegramPasswordProvider

log = logging.getLogger(__name__)
router = Router()

# tg_user_id → TelegramSmsCodeProvider
pending_auth: dict[int, TelegramSmsCodeProvider] = {}
pending_2fa_auth: dict[int, TelegramPasswordProvider] = {}

# tg_user_id → asyncio.Task авторизации (чтобы можно было отменить)
auth_tasks: dict[int, asyncio.Task] = {}


class AuthStates(StatesGroup):
    WAIT_INIT  = State()
    WAIT_PHONE = State()
    WAIT_SMS   = State()
    WAIT_2FA   = State()
    WAIT_GROUP = State()
    CONNECTED  = State()


# ── Клавиатуры ────────────────────────────────────────────────────────────────

def _connected_menu_kb() -> InlineKeyboardMarkup:
    """Клавиатура после успешного подключения."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Синхронизировать чаты", callback_data="cmd:sync_chats"),
        ],
        [
            InlineKeyboardButton(text="📥 Полная синхронизация", callback_data="cmd:full_sync"),
        ],
    ])


def init_connect_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Начать подключение к Max", callback_data="max_connect"),
    ]])

def bot_add_group_kb(bot_username: str) -> InlineKeyboardMarkup:
    add_url = (
        f"https://t.me/{bot_username}?startgroup=setup"
        f"&admin=manage_topics+post_messages+delete_messages"
    )
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="➕ Добавить бота в группу", url=add_url),
    ]])

def bot_setting_group_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Разрешаю перенастроить группу для корректной работы.", callback_data="group_setting"),
    ]])

def step1_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Группа создана", callback_data="group_created"),
    ]])


def step2_kb(bot_username: str) -> InlineKeyboardMarkup:
    add_url = (
        f"https://t.me/{bot_username}?startgroup=setup"
        f"&admin=manage_topics+post_messages+delete_messages"
    )
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="➕ Добавить бота в группу", url=add_url),
    ],
    [
        InlineKeyboardButton(text="Бот уже добавлен в группу", callback_data="bot_added_group"),
    ]])


def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отменить авторизацию", callback_data="cancel_auth"),
    ]])


# ── Тексты ────────────────────────────────────────────────────────────────────

STEP1_TEXT = (
    "🏗 <b>Шаг 1 из 2 — создайте группу-зеркало</b>\n\n"
    "1. Откройте Telegram\n"
    "2. Создайте новую супергруппу\n"
    "3. Зайдите в <b>Настройки группы → Темы</b> и включите их\n\n"
    "Нажмите кнопку когда группа готова."
)

AFTER_GROUP_TEXT = (
    "✅ <b>Подключение завершено!</b>\n\n"
    "Доступные команды (в личных сообщениях боту):\n"
    "• <code>/status</code> — статус подключения к MAX\n"
    "• <code>/sync_chats</code> — синхронизировать список чатов (без истории)\n"
    "• <code>/sync</code> — полная синхронизация (чаты + история)\n\n"
    "Команды внутри топика (темы) в группе:\n"
    "• <code>/history</code> — скачать историю только этого чата\n\n"
    "Выберите действие:"
)


async def send_step1(bot: Bot, chat_id: int):
    await bot.send_message(chat_id, STEP1_TEXT, parse_mode="HTML",
                           reply_markup=step1_kb())


# ── /start ────────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext, bot: Bot):
    if msg.chat.type in ("supergroup", "group"):
        log.info("[/start] вызван в группе. Проверяем настройки группы")
        me = await _send_with_retry(bot.get_me())
        member = await _send_with_retry(msg.chat.id, bot.id)
        if member.status == 'member':
            await msg.answer(
            "Группа найдена. Необходимо Бота добавить в Администраторы группы с правом изменять группу (для перенастройки в Форум).\n",
            parse_mode="HTML",
            reply_markup=bot_add_group_kb(me.username),
            )
            await state.set_state(AuthStates.WAIT_GROUP)
        if member.status == 'administrator':
            if msg.chat.is_forum is None:
                log.info("[/start] вызван в группе. Права Администратора есть, но группа не Форум")
                await msg.answer(
                f"⚠️ Бот добавлен в группу <b>{msg.chat.title}</b> и является Администраторм, "
                f"но <b>Темы (Topics)</b> не включены.\n\n"
                f"Включите: Настройки группы → Темы → Включить\n"
                f"После этого заново сделайте: /start\n",
                parse_mode="HTML",
                )
                await state.set_state(AuthStates.WAIT_GROUP)
            else:
                log.info("[/start] вызван в группе. Настройки корректны, группа подключена.")
                await msg.answer(
                    "✅ Группа настроена корректно.\n\n"
                    "Команды управления (в личных сообщениях боту):\n"
                    "• <code>/sync_chats</code> — синхронизировать список чатов\n"
                    "• <code>/sync</code> — полная синхронизация\n\n"
                    "Внутри топика:\n"
                    "• <code>/history</code> — скачать историю этого чата",
                    parse_mode="HTML",
                )
    if msg.chat.type == 'private':
        user_id = msg.from_user.id
        log.info("[/start] fresh auth flow")
        await msg.answer(
            "👋 Добро пожаловать в <b>MAX Bridge</b>!\n\n"
            "Бот обеспечивает пересылку сообщений из MAX в Вашу группу Телеграмм\n"
            "Для корректной работы необходимо:\n"
            "1. Иметь зарегистрированный в Max номер телефона (формат: <code>+79001234567</code>)\n"
            "2. Необходимо будет создать Группу в Телеграм, куда Бот будет направлять все сообщения из Max\n"
            "3. Добавить Бота, Администратором группы в Телеграм\n"
            "4. Группа в Телеграм будет сформированна в виде форума, для деления между чатами Max\n",
            parse_mode="HTML",
            reply_markup=init_connect_kb(),
        )
        await state.set_state(AuthStates.WAIT_INIT)

# ── Callback max_connect ────────────────────────────────────────────────────────────────────
@router.callback_query(F.data == "max_connect")
@router.message(AuthStates.WAIT_INIT)
async def cb_max_connect(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    user_id = callback.from_user.id
    log.info("[max_connect] user_id=%s", user_id)

    # Если уже идёт авторизация — предлагаем отменить
    if user_id in auth_tasks and not auth_tasks[user_id].done():
        await callback.answer(
            "⏳ Авторизация уже выполняется.\n"
            "Если хотите начать заново — отмените текущую.",
            reply_markup=cancel_kb(),
        )
        return

    user = await db.get_user(user_id)
    log.info("[max_connect] db_user=%s status=%s tg_group_id=%s",
             user.id if user else None,
             user.status if user else None,
             user.tg_group_id if user else None)

    if user and user.status == "active":
        client = manager.get_client(user_id)
        log.info("[max_connect] client_in_pool=%s", client is not None)

        if client and user.tg_group_id:
            await callback.answer("✅ Вы уже подключены к MAX.\nСообщения зеркалируются в вашу группу.")
            return

        if client and not user.tg_group_id:
            await send_step1(bot, callback.message.chat.id)
            await state.set_state(AuthStates.WAIT_GROUP)
            return

        log.info("[max_connect] client not in pool, reconnecting")
        await callback.answer("🔄 Переподключаюсь к MAX…")
        asyncio.create_task(_reconnect(user_id, user.max_phone, bot, callback.message.chat.id, state))
        return

    await callback.message.answer(
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
    pending_auth[tg_user_id] = provider
    provider_2fa = TelegramPasswordProvider(tg_user_id=tg_user_id, bot=bot, chat_id=msg.chat.id)
    pending_2fa_auth[tg_user_id] = provider_2fa
    await msg.answer(
        f"📲 Подключаюсь к MAX с номером <code>{phone}</code>…",
        parse_mode="HTML",
        reply_markup=cancel_kb(),
    )
    await db.create_user(
        tg_user_id   = tg_user_id,
        tg_username  = msg.from_user.username,
        max_phone    = phone,
        session_path = session_path_for(tg_user_id),
    )
    await state.set_state(AuthStates.WAIT_SMS)

    task = asyncio.create_task(
        run_auth(tg_user_id=tg_user_id, phone=phone, provider=provider, provider_2fa=provider_2fa,
                  bot=bot, chat_id=msg.chat.id, state=state)
    )
    auth_tasks[tg_user_id] = task


# ── SMS-код ───────────────────────────────────────────────────────────────────

@router.message(AuthStates.WAIT_SMS)
async def handle_sms_code(msg: Message, state: FSMContext):
    code = msg.text.strip() if msg.text else ""
    if not code.isdigit():
        await msg.answer("❌ Код должен состоять из цифр. Попробуйте ещё раз:")
        return

    tg_user_id = msg.from_user.id
    log.info("[auth] sms code entered user=%s", tg_user_id)
    provider = pending_auth.get(tg_user_id)
    if not provider:
        await msg.answer("⚠️ Сессия устарела. Начните заново: /start")
        await state.clear()
        return

    provider.set_code(code)
    await msg.answer("🔐 Проверяю код…")
    await state.set_state(AuthStates.WAIT_2FA)


# ── 2FA-код ───────────────────────────────────────────────────────────────────

@router.message(AuthStates.WAIT_2FA)
async def handle_2FA_code(msg: Message, state: FSMContext):
    code = msg.text.strip() if msg.text else ""

    tg_user_id = msg.from_user.id
    log.info("[auth] 2fa code entered user=%s", tg_user_id)
    provider_2fa = pending_2fa_auth.get(tg_user_id)
    if not provider_2fa:
        await msg.answer("⚠️ Сессия устарела. Начните заново: /start")
        await state.clear()
        return

    provider_2fa.set_password(code)
    await msg.answer("🔐 Проверяю 2FA код…")


# ── Отмена авторизации ────────────────────────────────────────────────────────

@router.callback_query(F.data == "cancel_auth")
async def cb_cancel_auth(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    tg_user_id = callback.from_user.id

    task = auth_tasks.pop(tg_user_id, None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    provider = pending_auth.pop(tg_user_id, None)
    if provider:
        provider.cancelled = True
        provider._event.set()

    await state.clear()
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(
        "❌ Авторизация отменена.\n\n"
        "Для повторной попытки напишите /start\n"
        "⚠️ Если MAX заблокировал запросы — подождите несколько минут перед повторной попыткой."
    )
    log.info("[auth] cancelled by user=%s", tg_user_id)


# ── Callbacks шагов ───────────────────────────────────────────────────────────

@router.callback_query(F.data == "group_created")
async def cb_group_created(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    me = await bot.get_me()
    await callback.message.answer(
        "✅ Отлично!\n\n"
        "🏗 <b>Шаг 2 из 2 — добавьте бота в группу</b>\n\n"
        "Нажмите кнопку ниже — Telegram предложит выбрать вашу группу "
        "и автоматически назначит бота администратором с нужными правами.\n\n"
        "После добавления бот <b>НЕ запускает</b> синхронизацию автоматически.\n"
        "Вы сможете запустить её вручную командой /sync или /sync_chats",
        parse_mode="HTML",
        reply_markup=step2_kb(me.username),
    )
    await state.set_state(AuthStates.WAIT_GROUP)


# ── Регистрация группы (из /start в группе или callback) ─────────────────────

async def register_group(bot: Bot, group_id: int, tg_user_id: int):
    """Общая логика регистрации группы — без запуска синхронизации."""
    user = await db.get_user(tg_user_id)
    if not user or user.status != "active":
        log.info("[group] user not found or not active, skipping")
        return

    if user.tg_group_id and user.tg_group_id != group_id:
        log.info("[group] user already has different group, skipping")
        return

    await db.set_user_group(tg_user_id, group_id)

    # Подсказка — что делать дальше
    await bot.send_message(
        tg_user_id,
        AFTER_GROUP_TEXT,
        parse_mode="HTML",
        reply_markup=_connected_menu_kb(),
    )

    await bot.send_message(
        group_id,
        f"✅ Группа подключена! {AFTER_GROUP_TEXT}",
        parse_mode="HTML",
    )


# ── /start в группе (админ) ──────────────────────────────────────────────────

# Заменяем старую start_sync_msg на register_group
@router.callback_query(F.data == "bot_added_group")
async def bot_added_as_admin(callback: CallbackQuery, bot: Bot):
    """Пользователь нажал 'Бот уже добавлен в группу'."""
    await callback.answer()
    tg_user_id = callback.from_user.id

    # Пытаемся определить group_id из состояния или сообщения
    # Для этого случая пользователь нажал кнопку в личке — group_id ещё не известен
    # Направляем к пересылке сообщения из группы
    await bot.send_message(
        tg_user_id,
        "📎 Перешлите <b>любое сообщение</b> из вашей группы-зеркала сюда,\n"
        "чтобы я мог определить ID группы.",
        parse_mode="HTML",
    )


# ── my_chat_member события ─────────────────────────────────────────────────

@router.my_chat_member(
    ChatMemberUpdatedFilter(member_status_changed=IS_NOT_MEMBER >> IS_MEMBER)
)
async def bot_added_as_member(event: ChatMemberUpdated, bot: Bot):
    """Бот добавлен как обычный участник (без прав админа)."""
    group = event.chat
    if group.type not in ("supergroup", "group"):
        return
    tg_user_id = event.from_user.id
    await bot.send_message(
        tg_user_id,
        f"⚠️ Бот добавлен в группу <b>{group.title}</b> без прав администратора.\n\n"
        f"Назначьте бота администратором с правами:\n"
        f"• Управление темами\n"
        f"• Отправка сообщений",
        parse_mode="HTML",
    )


# ── Фоновые функции ───────────────────────────────────────────────────────────

async def run_auth(tg_user_id, phone, provider, provider_2fa, bot, chat_id, state):
    log.info("[auth] run_auth started user=%s", tg_user_id)
    try:
        client = await manager.connect_user(
            tg_user_id=tg_user_id, max_phone=phone,
            sms_code_provider=provider, password_provider=provider_2fa)
        log.info("[auth] connect_user done user=%s me=%s", tg_user_id, client.me)

        pending_auth.pop(tg_user_id, None)
        auth_tasks.pop(tg_user_id, None)
        await db.set_user_active(tg_user_id)
        await state.set_state(AuthStates.WAIT_GROUP)

        me_name = getattr(client.me, "name", phone) or phone
        await bot.send_message(
            chat_id,
            f"✅ Авторизован в MAX как <b>{me_name}</b>",
            parse_mode="HTML",
        )
        await send_step1(bot, chat_id)

    except asyncio.CancelledError:
        log.info("[auth] task cancelled user=%s", tg_user_id)
        pending_auth.pop(tg_user_id, None)
        auth_tasks.pop(tg_user_id, None)
        await state.clear()
    except Exception as e:
        log.error("[auth] error user=%s: %s", tg_user_id, e, exc_info=True)
        pending_auth.pop(tg_user_id, None)
        auth_tasks.pop(tg_user_id, None)
        await state.clear()
        await bot.send_message(
            chat_id,
            f"❌ Ошибка авторизации: <code>{e}</code>\n\n"
            f"Попробуйте снова: /start\n"
            f"⚠️ Если ошибка повторяется — подождите несколько минут.",
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
            await send_step1(bot, chat_id)
            await state.set_state(AuthStates.WAIT_GROUP)
        else:
            await bot.send_message(chat_id, "✅ Переподключено к MAX.")
    except Exception as e:
        log.error("[reconnect] error user=%s: %s", tg_user_id, e, exc_info=True)
        await bot.send_message(
            chat_id,
            f"❌ Ошибка переподключения: <code>{e}</code>",
            parse_mode="HTML",
        )