from aiogram import Router
from aiogram.types import Message
from datetime import datetime
import logging
from database import Database
from api import PayphoriaAPI
from .messages import handle_message

router = Router()
logger = logging.getLogger(__name__)

@router.edited_message()
async def handle_edited_message(message: Message, db: Database, api: PayphoriaAPI) -> None:
    """Обработка отредактированных сообщений."""
    if not message.edit_date or (message.edit_date - message.date.timestamp()) > 30:
        logger.debug(f"Игнорируем редактирование сообщения {message.message_id} после 30 секунд")
        return
    await handle_message(message, db, api)