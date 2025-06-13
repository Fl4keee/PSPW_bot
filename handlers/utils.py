import re
import logging
import asyncio
from datetime import datetime
import pytz
from typing import List, Dict, Any, Optional, Callable, Tuple
from aiogram import Bot
from aiogram.types import Message, ReactionTypeEmoji, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto, InputMediaVideo, InputMediaDocument
from Levenshtein import distance
from config import CONSTANTS, RESPONSE_TEMPLATES, KEYBOARDS, ADMIN_IDS, ALLOWED_USERS
from database import Database
from api import PayphoriaAPI

logger = logging.getLogger(__name__)

MONTHS = {
    "01": "января", "02": "февраля", "03": "марта", "04": "апреля",
    "05": "мая", "06": "июня", "07": "июля", "08": "августа",
    "09": "сентября", "10": "октября", "11": "ноября", "12": "декабря"
}



async def set_reaction_on_chain(bot: Bot, message: Message, reactions: List[str]) -> None:
    """Установить реакцию только в чате мерчанта."""
    db = Database()
    if message.chat.id in [m["chat_id"] for m in db.get_merchants()]:
        try:
            logger.debug('reaction set from list')
            await bot.set_message_reaction(chat_id=message.chat.id, message_id=message.message_id, reaction=[ReactionTypeEmoji(emoji=reactions[0])])
        except:
            logger.debug('reaction set eyes')
            await bot.set_message_reaction(chat_id=message.chat.id, message_id=message.message_id, reaction=[ReactionTypeEmoji(emoji="👀")])

async def find_deal_id_in_chain(message: Message) -> Optional[str]:
    """Найти deal_id в сообщении или цепочке ответов."""
    text = message.text or message.caption or ""
    deal_ids = re.findall(CONSTANTS["DEAL_ID_PATTERN"], text)
    if deal_ids:
        return deal_ids[0]
    if message.reply_to_message:
        return await find_deal_id_in_chain(message.reply_to_message)
    return None

async def get_deal_ids(message: Message, api: PayphoriaAPI) -> Optional[str]:
    """Получить первый валидный deal_id."""
    text = message.text or message.caption or ""
    deal_ids = re.findall(CONSTANTS["DEAL_ID_PATTERN"], text)

    for deal_id in deal_ids:
        order = await api.get_order(deal_id, message.from_user.id)

        if order:  # Проверяем, вернул ли метод валидные данные
            logger.debug(f'deal id найден {deal_id}')
            return deal_id
        else:
            logger.debug('данные не возвращены')
    return await find_deal_id_in_chain(message)

async def get_media(message: Message) -> List[Dict[str, Any]]:
    """Получить медиа из сообщения."""
    media = []
    if message.photo:
        media.append({"type": "photo", "file_id": message.photo[-1].file_id, "caption": message.caption})
    if message.video:
        media.append({"type": "video", "file_id": message.video.file_id, "caption": message.caption})
    if message.document:
        media.append({"type": "document", "file_id": message.document.file_id, "caption": message.caption})
    return media[:10]

async def create_media_group(media: List[Dict[str, Any]]) -> List[Any]:
    """Создать медиагруппу."""
    media_group = []
    for item in media:
        if item["type"] == "photo":
            media_group.append(InputMediaPhoto(media=item["file_id"], caption=item.get("caption")))
        elif item["type"] == "video":
            media_group.append(InputMediaVideo(media=item["file_id"], caption=item.get("caption")))
        elif item["type"] == "document":
            media_group.append(InputMediaDocument(media=item["file_id"], caption=item.get("caption")))
    return media_group

async def send_message_with_media(
    bot: Bot,
    chat_id: str,
    text: str,
    media: List[Dict[str, Any]],
    reply_markup: Optional[InlineKeyboardMarkup] = None
) -> Message:
    """Отправить сообщение с медиа."""

    if media:
        media_group = await create_media_group(media)

        messages = await bot.send_media_group(chat_id=chat_id, media=media_group)
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
        return messages[0]

    return await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)

def create_keyboard(keyboard_type: str, msg_data: Optional[dict] = None) -> InlineKeyboardMarkup:
    """Создать клавиатуру."""

    current_keyboard = KEYBOARDS.get(keyboard_type, [])



    buttons = []
    for text, data in current_keyboard:
        # Создаем callback_data с ID сообщения и действием
        if msg_data:
            callback_data = f"{data}:{msg_data['deal_id']}:{msg_data['chat_id']}"
        else:
            callback_data = data

        buttons.append([InlineKeyboardButton(text=text, callback_data=callback_data)])


    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def find_integrator_chat(deal_id: str, api: PayphoriaAPI, db: Database) -> Optional[Dict[str, Any]]:
    """Найти чат интегратора."""
    deal_data = await api.get_order(deal_id, list(ADMIN_IDS)[0])
    if not deal_data:
        return None
    integrator_name = deal_data["integrator_name"]
    cascades = db.get_cascades()
    for cascade in cascades:
        if distance(integrator_name.lower(), cascade["name"].lower()) <= 2:
            return cascade
    return None



async def log_errors(error: Exception, bot: Bot) -> None:
    """Логировать ошибки и уведомлять админов."""
    logger.error(f"Ошибка: {error}")
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"⚠️ Ошибка: {str(error)}")
        except:
            logger.error(f"Не удалось уведомить админа {admin_id}")

def require_auth(func: Callable) -> Callable:
    """Проверка авторизации."""
    async def wrapper(message: Message, *args, **kwargs):

        if message.from_user.id not in ALLOWED_USERS:
            await message.reply(CONSTANTS["ACCESS_DENIED"])
            return
        return await func(message, *args, **kwargs)
    return wrapper

def require_admin(func: Callable) -> Callable:
    """Проверка админ-доступа."""

    async def wrapper(message: Message, *args, **kwargs):

        if message.from_user.id not in ADMIN_IDS:
            await message.reply(CONSTANTS["ACCESS_DENIED"])
            return

        return await func(message, *args, **kwargs)
    return wrapper