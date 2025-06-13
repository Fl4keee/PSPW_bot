from datetime import datetime

import pytz
from aiogram import Router
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from database import Database
from api import PayphoriaAPI
from config import RESPONSE_TEMPLATES, CONSTANTS, KEYBOARDS, ADMIN_IDS
from handlers.utils import send_message_with_media, set_reaction_on_chain, create_keyboard, log_errors, find_integrator_chat, \
    get_media

import logging
logger = logging.getLogger(__name__)
router = Router()

@router.callback_query(lambda c: c.data.split(":")[0] in ["approve", "reject", "view"])
async def handle_action(callback: CallbackQuery, db: Database, api: PayphoriaAPI) -> None:
    """Обработка действий по сделке."""

    callback_action = callback.data.split(":")[0]

    callback_origin_deal_id = callback.data.split(":")[1]

    messages = db.get_messages(chat_id=callback.message.chat.id, deal_id=callback_origin_deal_id)

    if not messages:
        await callback.message.delete()
        return

    deal_id = callback_origin_deal_id


    deal = next((d for d in db.get_deals() if d["deal_id"] == deal_id), None)
    if not deal:
        await callback.message.delete()
        return

    if callback_action == "approve":
        integrator = await find_integrator_chat(deal_id, api, db)
        # integrator = None
        if integrator:
            deal_data = await api.get_order(deal_id, callback.from_user.id)
            media = await get_media(callback.message)


            # отправка в чат интегратора успеха по сделке

            await send_message_with_media(
                callback.message.bot,
                str(integrator['chat_id']),
                RESPONSE_TEMPLATES["deal_info"].format(**deal_data),
                media,
                reply_markup=create_keyboard("integrator_approve", {'deal_id': deal_id, 'chat_id': 0}),
            )

            print(callback.answer().text)

            db.update_deal_status(deal_id, "awaiting_integrator")
            db.add_stat(callback.from_user.id, "approved", deal_data["merchant_name"])

        else:

            await send_message_with_media(
                callback.message.bot,
                str(callback.message.chat.id),
                f"❌ Сделка `{deal_id}` не отправлена интегратору. Обработайте вручную.",
                [],
                reply_markup=create_keyboard("integrator_proof",{'deal_id':deal_id, 'chat_id':0})

            )

        await callback.message.delete()

    elif callback_action == "reject":

        message = next((d for d in db.get_deals() if d["deal_id"] == deal_id), None)

        merch_chat_id = message['merchant_chat_id']

        await callback.message.edit_reply_markup(reply_markup=create_keyboard("reject"))


        # await send_message_with_media(
        #     callback.message.bot,
        #     str(merch_chat_id),
        #     f"❌ Сделка `{deal_id}` отменена  - необходимо повторное подтверждение",
        #     [],
        #     reply_markup=create_keyboard("integrator_action")
        # )

    elif callback_action == "view":
        await callback.message.edit_text(
            callback.message.text + "\n" + CONSTANTS["VIEWED"],
            reply_markup=None
        )
        db.add_stat(callback.from_user.id, "viewed", "N/A")
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("reason_"))
async def handle_reject_reason(callback: CallbackQuery, db: Database, api: PayphoriaAPI) -> None:
    """Обработка причины отклонения."""
    reason = callback.data

    logger.debug('обработка отклонения')



    messages = db.get_messages(chat_id=callback.message.chat.id, message_id=callback.message.message_id - 1)
    if not messages:
        await callback.message.delete()
        return


    deal_id = messages[-1]["deal_id"]
    deal = next((d for d in db.get_deals() if d["deal_id"] == deal_id), None)

    print(213123123,messages[-1])

    if not deal:
        await callback.message.delete()
        return



    reason_text = next((text for text, data in KEYBOARDS["reject"] if data == reason), "Неизвестно")


    await callback.message.bot.send_message(
        deal["merchant_chat_id"],
        RESPONSE_TEMPLATES["deal_rejected"].format(deal_id=deal_id, reason_text=reason_text)
    )



    await set_reaction_on_chain(callback.message.bot, callback.message, ["👎"])


    db.update_deal_status(deal_id, "rejected")
    db.add_stat(callback.from_user.id, "rejected", "completed")
    for admin_id in ADMIN_IDS:
        await callback.message.bot.send_message(
            admin_id,
            RESPONSE_TEMPLATES["integrator_reject_notify"].format(deal_id=deal_id, reason_text=reason_text)
        )
    await callback.message.delete()
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("integrator_approve"))
async def handle_integrator_approve(callback: CallbackQuery, db: Database, api: PayphoriaAPI) -> None:
    """Обработка одобрения интегратором."""

    print('await approve')

    messages = db.get_messages(chat_id=callback.message.chat.id, message_id=callback.message.message_id)
    if not messages:
        return
    deal_id = messages[0]["deal_id"]
    deal = next((d for d in db.get_deals() if d["deal_id"] == deal_id), None)
    if not deal:
        await callback.message.delete()
        return





    deal_data = await api.get_order(deal_id, callback.from_user.id)
    if deal_data and deal_data.get("status") == "success":
        await callback.message.reply(
            deal["merchant_chat_id"],
            RESPONSE_TEMPLATES["deal_completed"].format(deal_id=deal_id)
        )
        await set_reaction_on_chain(callback.message.bot,callback.message, ["👍"])
        db.update_deal_status(deal_id, "completed")
        db.add_stat(callback.from_user.id, "completed", deal_data["merchant_name"])
        await callback.message.delete()

    else:
        await callback.message.reply(
            RESPONSE_TEMPLATES["integrator_approve_error"].format(deal_id=deal_id)
        )
        await callback.message.delete()
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("integrator_reject"))
async def handle_integrator_reject(callback: CallbackQuery, db: Database) -> None:
    """Обработка отклонения интегратором."""
    messages = db.get_messages(chat_id=callback.message.chat.id, message_id=callback.message.message_id)
    if not messages:
        return
    deal_id = messages[0]["deal_id"]
    deal = next((d for d in db.get_deals() if d["deal_id"] == deal_id), None)
    if not deal:
        await callback.message.delete()
        return

    # Проверка SLA (упрощённая, реализация зависит от логики)
    if False:  # SLA истекло
        await callback.message.bot.send_message(
            deal["handler_id"],
            RESPONSE_TEMPLATES["integrator_reject_sla"].format(deal_id=deal_id)
        )
        await callback.message.delete()
    else:
        await callback.message.edit_reply_markup(reply_markup=create_keyboard("reject"))
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("integrator_proof"))
async def handle_integrator_proof(callback: CallbackQuery, db: Database) -> None:
    """Обработка доказательств от интегратора."""

    callback_action = callback.data.split(":")[0]

    callback_origin_deal_id = callback.data.split(":")[1]


    messages = db.get_messages(chat_id=callback.message.chat.id, deal_id=callback_origin_deal_id)


    if not messages:
        await callback.message.delete()
        return

    message_to_react = messages[-1]

    print(message_to_react)

    deal = next((d for d in db.get_deals() if d["deal_id"] == callback_origin_deal_id), None)
    if not deal:
        await callback.message.delete()
        return

    if callback.data == "integrator_proof_approve":

        media = await get_media(callback.message)
        print('approve')
        await send_message_with_media(
            callback.message.bot,
            deal["merchant_chat_id"],
            RESPONSE_TEMPLATES["integrator_proof_sent"].format(deal_id=deal_id),
            media
        )
        await set_reaction_on_chain(callback.message.bot, callback.message, ["👀"])
        await callback.message.delete()

    elif callback.data == "integrator_proof_reject":
        await callback.message.edit_text(
            callback.message.text + RESPONSE_TEMPLATES["integrator_proof_rejected"],
            reply_markup=None
        )
    await callback.answer()

@router.callback_query(lambda c: c.data == "YES")
async def handle_shift_stop_confirm(callback: CallbackQuery, db: Database, message:Message) -> None:
    """Подтверждение завершения смены."""
    date = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%Y-%m-%d")
    stats = db.get_stats(callback.from_user.id, date)
    count = len([d for d in db.get_deals() if d["status"] != "awaiting_integrator"])
    # Удаление сделок (реализация в Database)
    db.delete_deals_except(status="awaiting_integrator")
    await callback.message.bot.send_message(
        callback.message.chat.id,
        RESPONSE_TEMPLATES["shift_stop_report"].format(
            stats=RESPONSE_TEMPLATES["stats"].format(
                date=date,
                username=callback.from_user.username,
                taken=stats.get("taken", 0),
                approved=stats.get("approved", 0),
                completed=stats.get("completed", 0),
                rejected=stats.get("rejected", 0),
                viewed=stats.get("viewed", 0),
                errors=stats.get("errors", 0),
                iterations=stats.get("completed", 0) + stats.get("rejected", 0),
                merchant_messages=stats.get("merchant_messages", 0),
                merchants=", ".join(set(stats.get("merchants", []))),
                pending_deals="Нет"
            ),
            count=count,
            time=datetime.now(pytz.timezone("Europe/Moscow")).strftime("%H:%M:%S")
        )
    )
    await callback.answer()

@router.callback_query(lambda c: c.data == "NO")
async def handle_shift_stop_cancel(callback: CallbackQuery) -> None:
    """Отмена завершения смены."""
    await callback.message.reply("❌ Отменено")
    await callback.message.delete()
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("merchant_"))
async def _handle_merchant_select(callback: CallbackQuery, db: Database) -> None:
    """Выбор мерчанта."""

    merchant_name = callback.data[len("merchant_"):]  # Получаем имя мерчанта
    merchant = db.get_merchant(name=merchant_name)  # Получаем информацию о мерчанте

    buttons = []


    if merchant:
        if merchant["handler_id"] is None:
            # Если handler_id пустой, устанавливаем его на текущего пользователя
            db.update_merchant_handler(merchant["name"], callback.from_user.id)
            await callback.message.edit_text(f"Мерчант {merchant['display_name']} выбран.")
        else:
            # Если handler_id уже установлен, убираем его (устанавливаем на null)
            db.update_merchant_handler(merchant["name"], None)
            await callback.message.edit_text(f"Мерчант {merchant['display_name']} отменен.")
    merchants = db.get_merchants()
    # Обновляем клавиатуру после изменения состояния
    for m in merchants:
        is_selected = m['handler_id'] == callback.from_user.id  # Проверяем, выбран ли мерчант
        buttons.append([
            InlineKeyboardButton(text=f"{m['display_name']} {'✅' if is_selected else '❌'}",
                                 callback_data=f"merchant_{m['name']}")
        ])

    # Обновляем клавиатуру
    await callback.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()
