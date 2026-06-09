"""
Inline-клавиатуры для Telegram-бота.
"""

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def media_download_kb(message_db_id: int, max_file_id: str) -> InlineKeyboardMarkup:
    """Кнопка под сообщением с медиафайлом."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text          = "📎 Загрузить файл",
            callback_data = f"dl:{message_db_id}:{max_file_id}",
        )
    ]])


def confirm_kb(action: str, payload: str) -> InlineKeyboardMarkup:
    """Кнопки подтверждения/отмены."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да",    callback_data=f"{action}:yes:{payload}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"{action}:no:{payload}"),
    ]])
