from datetime import datetime

from aiogram import Router, F, Dispatcher
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton,BotCommand
from aiogram.filters import Command, CommandStart, Filter, or_f
from config import HELP_TEXT, ADMIN_COMMANDS, ADMIN_IDS, RESPONSE_TEMPLATES, CONSTANTS
from database import Database
from handlers.utils import require_auth, require_admin, create_keyboard,send_message_with_media
import logging
logger = logging.getLogger(__name__)

import pytz



router = Router()

@router.message(CommandStart())
@require_auth
async def cmd_start(message: Message, **kwargs) -> None:
    """Обработка команды /start."""
    await message.reply("Добро пожаловать в PSPWare! Используйте /help.")

@router.message(Command("help"))
@require_auth
async def cmd_help(message: Message, **kwargs) -> None:
    """Обработка команды /help."""
    await message.reply(HELP_TEXT["help"].format(admin_ids=", ".join(str(id) for id in ADMIN_IDS)))

@router.message(Command("merchant_list"))
@require_auth
async def cmd_merchant_list(message: Message, db: Database, **kwargs) -> None:
    """Обработка команды /merchant_list."""
    merchants = db.get_merchants()
    if not merchants:
        await message.reply("Нет доступных мерчантов.")
        return
    buttons = [
        [InlineKeyboardButton(text=f"{m['display_name']} {'✅' if m['handler_id'] == message.from_user.id else '❌'}",
                              callback_data=f"merchant_{m['name']}")]
        for m in merchants
    ]


    await message.reply("Выберите мерчанта:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.message(Command("shift_start"))
@require_auth
async def cmd_shift_start(message: Message, db: Database, **kwargs) -> None:
    """Обработка команды /shift_start."""
    db.add_shift(message.from_user.id, datetime.now().timestamp())
    db.add_stat(message.from_user.id, "taken", "N/A")
    await message.reply(RESPONSE_TEMPLATES["shift_start"].format(time=datetime.now(pytz.timezone("Europe/Moscow")).strftime("%H:%M:%S")))

@router.message(Command("shift_stop"))
@require_auth
async def cmd_shift_stop(message: Message, db: Database, **kwargs) -> None:
    """Обработка команды /shift_stop."""
    await message.reply(RESPONSE_TEMPLATES["shift_stop_confirm"], reply_markup=create_keyboard("yes_no"))

@router.message(Command("stats"))
@require_auth
async def cmd_stats(message: Message, db: Database, **kwargs) -> None:
    """Обработка команды /stats."""
    date = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%Y-%m-%d")
    stats = db.get_stats(message.from_user.id, date)
    pending_deals = "\n".join([f"<code>{d['deal_id']}</code>" for d in db.get_deals(status="awaiting_integrator")])
    await message.reply(
        RESPONSE_TEMPLATES["stats"].format(
            date=date,
            username=message.from_user.username or "N/A",
            taken=stats.get("taken", 0),
            approved=stats.get("approved", 0),
            completed=stats.get("completed", 0),
            rejected=stats.get("rejected", 0),
            viewed=stats.get("viewed", 0),
            errors=stats.get("errors", 0),
            iterations=stats.get("completed", 0) + stats.get("rejected", 0),
            merchant_messages=stats.get("merchant_messages", 0),
            merchants=", ".join(set(stats.get("merchants", []))),
            pending_deals=pending_deals or "Нет"
        ),
        parse_mode="HTML"
    )

@router.message(Command("get_chats"))
@require_auth
async def cmd_get_chats(message: Message, db: Database, **kwargs) -> None:
    """Обработка команды /get_chats."""
    merchants = db.get_merchants()
    cascades = db.get_cascades()
    chats = [f"Мерчант: {m['display_name']} ({m['chat_id']})" for m in merchants] + \
            [f"Интегратор: {c['display_name']} ({c['chat_id']})" for c in cascades]
    await message.reply("\n".join(chats) or CONSTANTS["NO_CHAT"])

@router.message(Command("list_cascades"))
@require_auth
async def cmd_get_cascades(message: Message, db: Database, **kwargs) -> None:
    """Обработка команды /get_cascades."""
    cascades = db.get_cascades()

    if not cascades:
        await send_message_with_media(
            message.bot,
            str(message.chat.id),
            "📭 Интеграторы не найдены.",
            []
        )
        return

    cascades_text = "---->\n" + "\n---->\n".join(
        f"{'Имя'}: {c.get('name', '')} | Chat_id: {c.get('chat_id', 'N/A')} | external_id: {c.get('needs_external_id', 0)}"
        for c in cascades
    )





    await send_message_with_media(message.bot,
                                  str(message.chat.id),
                                  f"🤝 Интеграторы:\n{cascades_text}",
                                  media=[])

    logger.info(f"Список интеграторов отправлен: chat_id={message.chat.id}, кол-во={len(cascades)}")

@router.message(Command("link"))
@require_admin
async def cmd_link(message: Message, db: Database, **kwargs) -> None:
    """Обработка команды /link."""
    args = message.text.split()[1:]
    if len(args) < 2 or args[0].lower() not in ["m", "i"]:
        await message.reply("Формат: /link [m|i] <name> [chat_id]")
        return
    chat_type, name = args[0], args[1]
    chat_id = int(args[2]) if len(args) > 2 else message.chat.id
    if chat_type == "m":
        db.add_merchant(name, name, chat_id=chat_id)
    else:
        db.merge_cascade(name, name, chat_id=chat_id)
    await message.reply(f"✅ Чат привязан к {name}")



# Преобразуем команды в объекты BotCommand

@router.message(Command("/add_merchant"))
@require_admin
async def cmd_admin(message: Message, db: Database, **kwargs) -> None:


    """Обработка админ-команд."""
    cmd = message.text.split()[0][1:].lower()

    args = message.text.split()[1:]


    if cmd in ADMIN_COMMANDS:
        print(args)
        try:
            required_args = ADMIN_COMMANDS[cmd]["args"]
            if len(args) < required_args:
                await message.reply(f"Формат: /{cmd} {' '.join(['<arg>' for _ in range(required_args)])}")
                return
            result = ADMIN_COMMANDS[cmd]["action"](db, args)
            if result:
                await message.reply(ADMIN_COMMANDS[cmd]["success"].format(*args))
            else:
                await message.reply(CONSTANTS["DATA_ERROR"])
        except Exception as e:
            logger.error(f"Ошибка админ-команды {cmd}: {e}")
            await message.reply(f"Ошибка: {e}")
    else:
        await message.reply("Неизвестная команда")