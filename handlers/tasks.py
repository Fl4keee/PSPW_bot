from aiogram import Router, Bot
from aiogram.types import Message, Chat
import asyncio
from datetime import datetime
import pytz
import logging
from database import Database
from api import PayphoriaAPI
from config import RESPONSE_TEMPLATES, SLA_DAY_SECONDS, SLA_NIGHT_SECONDS, DAY_START, DAY_END, CONSTANTS
from handlers.utils import send_message_with_media, set_reaction_on_chain, log_errors
from config import HELP_TEXT, ADMIN_COMMANDS, ADMIN_IDS, RESPONSE_TEMPLATES, CONSTANTS

router = Router()
logger = logging.getLogger(__name__)

def is_day_time() -> bool:
    """Проверить, дневное ли время (MSK)."""
    now = datetime.now(pytz.timezone("Europe/Moscow"))
    day_start = datetime.strptime(DAY_START, "%H:%M").time()
    day_end = datetime.strptime(DAY_END, "%H:%M").time()
    return day_start <= now.time() <= day_end

async def get_sla_timeout(sent_time: float) -> float:
    """Получить SLA-таймаут."""
    timeout = SLA_DAY_SECONDS if is_day_time() else SLA_NIGHT_SECONDS
    return sent_time + timeout

async def check_deals(bot: Bot, db: Database, api: PayphoriaAPI) -> None:
    """Периодическая проверка сделок."""
    try:
        for deal in db.get_deals():
            if deal["status"] == "awaiting":
                timeout = await get_sla_timeout(deal["sent_time"])
                if datetime.now(pytz.timezone("Europe/Moscow")).timestamp() > timeout:
                    merchant = db.get_merchant(chat_id=deal["merchant_chat_id"])
                    if merchant:
                        await send_message_with_media(
                            bot,
                            deal["handler_id"],
                            RESPONSE_TEMPLATES["sla_expired"].format(
                                deal_id=deal["deal_id"],
                                merchant_name=merchant["display_name"]
                            ),
                            []
                        )
                        db.add_sla_notification(deal["deal_id"], 0, True)


            elif deal["status"] == "awaiting_integrator":
                deal_data = await api.get_order(deal["deal_id"], ADMIN_IDS[0])

                logger.debug(deal_data)

                if deal_data and deal_data.get("status") == "success":
                    messages = db.get_messages(deal_id=deal["deal_id"])

                    if messages:

                        chat_id = deal["merchant_chat_id"]
                        chat = Chat(id=chat_id, type="group")

                        msg = Message(
                            message_id=deal['message_id'],
                            chat=chat,
                            bot=bot,
                            date=deal['sent_time']

                        )

                        await send_message_with_media(
                            bot,
                            deal["merchant_chat_id"],
                            RESPONSE_TEMPLATES["deal_completed"].format(deal_id=deal["deal_id"]),
                            []
                        )
                        await set_reaction_on_chain(bot, msg, ["👍"])
                    db.update_deal_status(deal["deal_id"], "completed")
                    db.add_stat(deal["handler_id"], "completed", deal_data["merchant_name"])
    except Exception as e:
        await log_errors(e, bot)

async def start_tasks(bot: Bot, db: Database, api: PayphoriaAPI) -> None:
    """Запуск периодических задач."""
    async def run_check_deals():
        while True:
            await check_deals(bot, db, api)
            await asyncio.sleep(20)
    asyncio.create_task(run_check_deals())