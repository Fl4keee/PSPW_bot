from aiogram import Router, F
from aiogram.types import Message
import asyncio
import logging
from datetime import datetime
import pytz
from database import Database
from api import PayphoriaAPI
from config import CONSTANTS, RESPONSE_TEMPLATES, IGNORED_USERS
from handlers.utils import get_deal_ids, get_media, send_message_with_media, set_reaction_on_chain, create_keyboard, find_integrator_chat

router = Router()
logger = logging.getLogger(__name__)




async def process_deal(message: Message, deal_id: str, db: Database, api: PayphoriaAPI, merchant: dict | None) -> None:
    """Обработка сделки."""
    deal_data = await api.get_order(deal_id, message.from_user.id)
    if not deal_data:
        logger.warning(f"Невалидный deal_id: {deal_id}")
        return
    handler_id = merchant["handler_id"] if merchant else list(CONSTANTS["ADMIN_IDS"])[0]
    media = await get_media(message)


    msg = await send_message_with_media(
        message.bot,
        str(handler_id),
        RESPONSE_TEMPLATES["deal_info"].format(**deal_data),
        media,
        reply_markup=create_keyboard("action",{'deal_id':deal_data['deal_id'], 'chat_id':message.chat.id})
    )


    db.add_deal(
        deal_id=deal_id,
        merchant_chat_id=message.chat.id,
        message_id=message.message_id,
        status="awaiting",
        sent_time=message.date.timestamp(),
        merchant_id=merchant["merchant_id"] if merchant else "",
        handler_id=handler_id
    )

    db.add_message(deal_id, handler_id, message.message_id, handler_id, msg.date.timestamp())
    db.add_stat(handler_id, "taken", deal_data["merchant_name"])

    if merchant:
        await message.reply(RESPONSE_TEMPLATES["deal_accepted"].format(deal_id=deal_id), parse_mode="HTML")
        await set_reaction_on_chain(message.bot, message, ["👀"])






async def handle_message(message: Message, db: Database, api: PayphoriaAPI) -> None:
    """Обработка сообщений: сделки, кб внешний, медиа, апелляции."""
    if message.from_user.id in IGNORED_USERS and message.chat.type != "private":
        logger.debug(f"Игнорируем сообщение от {message.from_user.id}")
        return
    if message.sticker:
        return

    text = message.text or message.caption or ""
    deal_id = await get_deal_ids(message, api)

    merchant = db.get_merchant(chat_id=message.chat.id)
    cascade = next((c for c in db.get_cascades() if c["chat_id"] == message.chat.id), None)

    # Обработка "кб внешний"
    if "кб внешний" in text.lower() and deal_id and (merchant or message.chat.type == "private"):

        logger.debug('начата обработка кб внешний')

        deal_data = await api.get_order(deal_id, message.from_user.id)
        if deal_data:
            await send_message_with_media(
                message.bot,
                str(message.chat.id),
                RESPONSE_TEMPLATES["kb_request"].format(deal_id=deal_id),
                []
            )
            integrator = await find_integrator_chat(deal_id, api, db)

            if integrator:
                await send_message_with_media(
                    message.bot,
                    integrator["chat_id"],
                    RESPONSE_TEMPLATES["integrator_kb_request"].format(deal_id=deal_id),
                    []
                )
            if merchant:
                await set_reaction_on_chain(message.bot, message, ["⚡️"])
        return

    # Медиа от интегратора для rejected сделок

    if cascade and (message.photo or message.video or message.document):
        if deal_id:
            deal = next((d for d in db.get_deals() if d["deal_id"] == deal_id and d["status"] == "rejected"), None)
            if deal:
                media = await get_media(message)
                await send_message_with_media(
                    message.bot,
                    deal["merchant_chat_id"],
                    RESPONSE_TEMPLATES["integrator_proof_accepted"].format(deal_id=deal_id),
                    []
                )
                await set_reaction_on_chain(message.bot, message, ["👀"])
                msg = await send_message_with_media(
                    message.bot,
                    deal["handler_id"],
                    RESPONSE_TEMPLATES["integrator_proof"].format(deal_id=deal_id),
                    media,
                    reply_markup=create_keyboard("integrator_proof")
                )
                db.add_message(deal_id, deal["handler_id"], msg.message_id, message.from_user.id, msg.date.timestamp())
                db.add_proof_message(deal_id, msg.message_id)
        return


    # Медиа без deal_id от мерчанта
    if merchant and (message.photo or message.video or message.document) and not deal_id:

        logger.debug('добавлено медиа без deal id ')

        await asyncio.sleep(30)  # Ждём 30 секунд на редактирование
        deal_id = await get_deal_ids(message, api)  # Проверяем цепочку ответов
        if deal_id:
            await process_deal(message, deal_id, db, api, merchant)

            logger.debug('пользователь отредактировал и добавил deal id ')

        elif any(a["deal_id"] == deal_id for a in db.get_appeals()):
            media = await get_media(message)
            msg = await send_message_with_media(
                message.bot,
                merchant["handler_id"],
                RESPONSE_TEMPLATES["proofs_added"].format(deal_id=deal_id),
                media
            )
            db.add_message(deal_id, merchant["handler_id"], msg.message_id, message.from_user.id, msg.date.timestamp())
            db.add_proof_message(deal_id, msg.message_id)
        return

    # Сделки или ручные апелляции
    if deal_id and (merchant or message.chat.type == "private"):
        if message.chat.type == "private" and any(a["deal_id"] == deal_id for a in db.get_appeals()):
            media = await get_media(message)
            handler_id = next((a["user_id"] for a in db.get_appeals() if a["deal_id"] == deal_id), list(CONSTANTS["ADMIN_IDS"])[0])
            msg = await send_message_with_media(
                message.bot,
                handler_id,
                RESPONSE_TEMPLATES["proofs_added"].format(deal_id=deal_id),
                media
            )
            db.add_message(deal_id, handler_id, msg.message_id, message.from_user.id, msg.date.timestamp())
            db.add_proof_message(deal_id, msg.message_id)
        else:
            await process_deal(message, deal_id, db, api, merchant)

@router.message(F.text | F.caption | F.photo | F.video | F.document)
async def message_handler(message: Message, db: Database, api: PayphoriaAPI) -> None:
    await handle_message(message, db, api)