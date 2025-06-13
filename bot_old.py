import telebot
import re
import asyncio
import aiohttp
import logging
import time as time_module
import traceback
import sys
import json
from datetime import datetime, time
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReactionTypeEmoji
from telebot.async_telebot import AsyncTeleBot
from config import BOT_TOKEN, API_USERNAME, API_PASSWORD, API_BASE_URL, API_AUTH_URL, ADMIN_ID, ALLOWED_USERS, \
    IGNORED_USERS, HELP_TEXT, RESPONSE_TEMPLATES, CONSTANTS, KEYBOARDS, ADMIN_COMMANDS, SLA_DAY_SECONDS, SLA_NIGHT_SECONDS
from database import Database
from api import PayphoriaAPI

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.db.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

bot = AsyncTeleBot(BOT_TOKEN)
db = Database("data")
api = PayphoriaAPI(db=db)

def log_errors():
    def decorator(func):
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Ошибка в {func.__name__}: {e}\n{traceback.format_exc()}")
                await bot.send_message(ADMIN_ID, f"🚨 Ошибка в {func.__name__}: {str(e)}")
                raise
        return wrapper
    return decorator

def retry(attempts=3, delay=2):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(attempts):
                try:
                    return await func(*args, **kwargs)
                except aiohttp.ClientConnectorError as e:
                    logger.error(f"Ошибка подключения: {e}, попытка {attempt + 1}/{attempts}")
                    if attempt == attempts - 1:
                        raise
                    await asyncio.sleep(delay)
                except Exception as e:
                    logger.error(f"Ошибка: {e}, попытка {attempt + 1}/{attempts}")
                    raise
        return wrapper
    return decorator

def require_auth(func):
    async def wrapper(*args, **kwargs):
        obj = args[0]
        if isinstance(obj, telebot.types.Message):
            user_id = obj.from_user.id
            chat_id = obj.chat.id
            source = "message"
        elif isinstance(obj, telebot.types.CallbackQuery):
            user_id = obj.from_user.id
            chat_id = obj.message.chat.id if obj.message else None
            source = "callback_query"
        else:
            logger.error(f"Неизвестный тип объекта в require_auth: {type(obj)}")
            return

        logger.info(f"🔐 Проверка доступа: source={source}, user_id={user_id}, type={type(user_id)}, "
                    f"ALLOWED_USERS={ALLOWED_USERS}, type={type(list(ALLOWED_USERS)[0])}, "
                    f"ADMIN_ID={ADMIN_ID}, type={type(ADMIN_ID)}, chat_id={chat_id}")

        if user_id not in ALLOWED_USERS:
            logger.info(f"Доступ запрещён: user_id={user_id} не в ALLOWED_USERS")
            if source == "message":
                await bot.reply_to(obj, CONSTANTS["ACCESS_DENIED"])
            else:
                await bot.answer_callback_query(obj.id, text=CONSTANTS["ACCESS_DENIED"])
            return
        if user_id == ADMIN_ID:
            logger.debug(f"Доступ разрешён для админа: user_id={user_id}")
            return await func(*args, **kwargs)
        if chat_id and chat_id < 0:
            logger.info(f"Команда игнорируется в групповом чате: chat_id={chat_id}, user_id={user_id}")
            if source == "message":
                await bot.reply_to(obj, CONSTANTS["ACCESS_DENIED"])
            else:
                await bot.answer_callback_query(obj.id, text="Команды не доступны в групповом чате")
            return
        logger.debug(f"Доступ разрешён: user_id={user_id}")
        return await func(*args, **kwargs)
    return wrapper

def require_admin(func):
    async def wrapper(*args):
        obj = args[0]
        user_id = obj.from_user.id if isinstance(obj, telebot.types.Message) else obj.from_user.id
        logger.debug(f"🛡️ Проверка админ-доступа: user_id={user_id}, admin_id={ADMIN_ID}")
        if user_id != ADMIN_ID:
            logger.info(f"Админ-доступ запрещён: user_id={user_id}")
            if isinstance(obj, telebot.types.Message):
                await bot.reply_to(obj, CONSTANTS["ACCESS_DENIED"])
            else:
                await bot.answer_callback_query(obj.id, text="Недостаточно прав")
            return
        logger.debug(f"Админ-доступ разрешён: user_id={user_id}")
        return await func(*args)
    return wrapper

def with_deal_data():
    def decorator(func):
        async def wrapper(call, *args, **kwargs):
            deal = db.get_deal_by_message_id(str(call.message.chat.id), str(call.message.message_id))
            deal_id = deal.get('deal_id') if deal else None
            logger.debug(f"🔍 Проверка deal_id: chat_id={call.message.chat.id}, message_id={call.message.message_id}, deal_id={deal_id}")
            if not deal_id:
                logger.info(f"Сделка не найдена: chat_id={call.message.chat.id}, message_id={call.message.message_id}")
                await bot.answer_callback_query(call.id, CONSTANTS["DEAL_NOT_FOUND"])
                return
            data = await api.get_order(deal_id, call.from_user.id)
            logger.debug(f"Данные заказа: deal_id={deal_id}, data={data}")
            if not data:
                logger.info(f"Ошибка данных заказа: deal_id={deal_id}")
                await bot.answer_callback_query(call.id, CONSTANTS["DATA_ERROR"])
                return
            return await func(call, data, deal_id, *args, **kwargs)
        return wrapper
    return decorator

def get_sla_timeout():
    now = datetime.now().time()
    return SLA_DAY_SECONDS if time(10, 0) <= now <= time(22, 0) else SLA_NIGHT_SECONDS

def create_keyboard(button_type="action", row_width=2, buttons_data=None):
    keyboard = InlineKeyboardMarkup(row_width=row_width)
    buttons = KEYBOARDS.get(button_type, KEYBOARDS['action'])
    if button_type == "merchant":
        buttons = [(f"{m['name']} {'✅' if m.get('chat_id') else '❌'}", f"merchant_{m['name']}") for m in db.get_merchants().values()]
    elif button_type == "action" and buttons_data and not buttons_data.get("is_integrator"):
        buttons = buttons + [("👀", "view")]
    for text, callback in buttons:
        keyboard.add(InlineKeyboardButton(text=text, callback_data=callback))
    return keyboard

async def get_deal_id(message):
    logger.debug(f"🔎 Извлечение deal_id: chat_id={message.chat.id}, user_id={message.from_user.id}, content_type={message.content_type}")
    text = message.text or message.caption or ""
    if message.reply_to_message:
        reply_text = message.reply_to_message.text or message.reply_to_message.caption or ""
        text += reply_text
        logger.debug(f"Текст ответа включён: {reply_text}")
    match = re.search(CONSTANTS["DEAL_ID_PATTERN"], text, re.I)
    deal_id = match.group(0) if match else None
    logger.debug(f"Результат deal_id: deal_id={deal_id}, текст_поиска={text}")
    return deal_id

async def get_media(message):
    logger.debug(f"📷 Извлечение медиа: chat_id={message.chat.id}, message_id={message.message_id}")
    media = {'photos': [], 'videos': [], 'documents': [], 'animations': []}
    for attr, key in [(message.photo, 'photos'), (message.video, 'videos'), (message.document, 'documents'), (message.animation, 'animations')]:
        if attr:
            media[key].append(attr[-1].file_id if key == 'photos' else attr.file_id)
    logger.debug(f"Медиа извлечены: {media}")
    return media

def create_media_group(media):
    logger.debug(f"📁 Создание группы медиа: {media}")
    media_group = []
    for photo in media['photos']:
        media_group.append(telebot.types.InputMediaPhoto(media=photo))
    for video in media['videos']:
        media_group.append(telebot.types.InputMediaVideo(media=video))
    for document in media['documents']:
        media_group.append(telebot.types.InputMediaDocument(media=document))
    for animation in media['animations']:
        media_group.append(telebot.types.InputMediaAnimation(media=animation))
    logger.debug(f"Группа медиа создана: {len(media_group)} элементов")
    return media_group

async def send_message_with_media(chat_id, text, media=None, reply_markup=None, reply_to_id=None, reaction=None, stats=None):
    logger.debug(f"📨 Отправка сообщения: chat_id={chat_id}, текст={text}, медиа={media}, reply_to_id={reply_to_id}, статистика={stats}")
    if media and any(media.values()):
        try:
            media_group = create_media_group(media)
            if media_group:
                first_media = media_group[0]
                first_media.caption = text
                first_media.parse_mode = 'Markdown'
                sent = await bot.send_media_group(chat_id, media_group)
                logger.debug(f"Группа медиа отправлена: chat_id={chat_id}, message_ids={[m.message_id for m in sent]}")
                if reply_markup:
                    await bot.send_message(chat_id, "Действия:", reply_markup=reply_markup)
                if stats:
                    db.update_stats(stats['user_id'], stats['stat_type'], stats['merchant_name'])
                    logger.debug(f"Статистика обновлена: {stats}")
                return sent
        except Exception as e:
            logger.error(f"Ошибка отправки медиа: chat_id={chat_id}, ошибка={e}")
            return None
    sent = await bot.send_message(
        chat_id, text, reply_markup=reply_markup, reply_to_message_id=reply_to_id, parse_mode='Markdown'
    )
    logger.debug(f"Сообщение отправлено: chat_id={chat_id}, message_id={sent.message_id}")
    if reaction:
        await bot.set_message_reaction(chat_id, sent.message_id, reaction)
        logger.debug(f"Реакция установлена: chat_id={chat_id}, message_id={sent.message_id}, реакция={reaction}")
    if stats:
        db.update_stats(stats['user_id'], stats['stat_type'], stats['merchant_name'])
        logger.debug(f"Статистика обновлена: {stats}")
    return sent

async def format_deal_msg(data, merchant_name, integrator=None, is_integrator=False):
    logger.debug(f"📝 Форматирование сообщения сделки: deal_id={data.get('id')}, имя_мерчанта={merchant_name}, is_integrator={is_integrator}")
    valid_statuses = {"success", "canceled", "processing"}
    status = data.get("status", "processing") if data.get("status") in valid_statuses else "processing"
    created_at = data.get("created_at", "Н/Д")
    if created_at and isinstance(created_at, str):
        try:
            dt = datetime.strptime(created_at[:26], "%Y-%m-%dT%H:%M:%S.%f")
            created_at = dt.strftime("%H:%M %d-%m-%Y")
        except ValueError:
            try:
                dt = datetime.strptime(created_at[:19], "%Y-%m-%dT%H:%M:%S")
                created_at = dt.strftime("%H:%M %d-%m-%Y")
            except ValueError:
                created_at = "Н/Д"
    integrator_order_id = (f"\n🆔 Внешний ID: `{data.get('integratorOrderId', 'Н/Д')}`"
                          if integrator and integrator.get("needs_external_id") and data.get('integratorOrderId')
                          else "")
    result = RESPONSE_TEMPLATES["deal_info"].format(
        deal_id=data.get('id', 'Н/Д'),
        merchant_name=merchant_name,
        integrator=integrator.get('name', 'Неизвестно') if integrator else 'Неизвестно',
        recipient=data.get('recipient', 'Н/Д'),
        card=data.get('card', 'Н/Д'),
        bank=data.get('bank', 'Н/Д'),
        sbp_type='СБП' if data.get('is_sbp', False) else 'Карта',
        sum=data.get('sum', 0),
        currency=data.get('currency', 'RUB'),
        status=status,
        created_at=created_at,
        integrator_order_id=integrator_order_id
    )
    logger.debug(f"Отформатированное сообщение: {result}")
    return result

async def set_reaction_on_chain(chat_id, deal_id, reaction_emoji, ignore_users):
    logger.debug(f"🔗 Установка реакции на цепочку: chat_id={chat_id}, deal_id={deal_id}, реакция={reaction_emoji}")
    messages = db.get_messages_by_deal(deal_id)
    for msg in messages:
        if msg['chat_id'] == str(chat_id) and msg.get('user_id', str((await bot.get_me()).id)) not in ignore_users:
            try:
                await bot.set_message_reaction(chat_id, msg['message_id'], [ReactionTypeEmoji(emoji=reaction_emoji)])
                logger.debug(f"Реакция установлена: chat_id={chat_id}, message_id={msg['message_id']}, реакция={reaction_emoji}")
            except Exception as e:
                logger.error(f"Ошибка установки реакции: chat_id={chat_id}, message_id={msg['message_id']}, ошибка={e}")

@log_errors()
async def check_deals():
    try:
        while True:
            logger.debug("🔄 Начало цикла проверки сделок")
            for deal in db.get_non_success_deals():
                deal_id = deal["deal_id"]
                merchant_chat_id = deal["merchant_chat_id"]
                handler_id = deal["handler_id"]
                merchants = db.get_merchants()
                merchant = next((m for m in merchants.values() if m["chat_id"] == merchant_chat_id), None)
                m_name = merchant["name"] if merchant else "Неизвестно"
                logger.debug(f"Проверка сделки: deal_id={deal_id}, статус={deal['status']}")
                if deal["status"] == "awaiting_integrator":
                    data = await api.get_order(deal_id, None)
                    logger.debug(f"Ответ API для deal_id={deal_id}: {data}")
                    if data and isinstance(data, dict) and data.get("status") == "success":
                        reaction = [ReactionTypeEmoji(emoji="👍")]
                        await send_message_with_media(
                            merchant_chat_id,
                            f"✅ Сделка `{deal_id}` успешно завершена",
                            reply_to_id=db.get_merchant_message_id(deal_id),
                            stats={"user_id": handler_id, "stat_type": "success", "merchant_name": m_name}
                        )
                        await set_reaction_on_chain(merchant_chat_id, deal_id, "👍", IGNORED_USERS)
                        integrator_msg = db.get_integrator_message(deal_id)
                        if integrator_msg:
                            await bot.delete_message(
                                integrator_msg['chat_id'],
                                integrator_msg['message_id']
                            )
                            logger.debug(f"Удалено сообщение: chat_id={integrator_msg['chat_id']}, message_id={integrator_msg['message_id']}")
                        db.delete_deal(deal_id)
                        logger.info(f"Сделка завершена: deal_id={deal_id}")
                        continue
                    elif data and isinstance(data, dict) and data.get("status") == "canceled":
                        reaction = [ReactionTypeEmoji(emoji="👎")]
                        await send_message_with_media(
                            merchant_chat_id,
                            f"❌ Сделка `{deal_id}` отменена",
                            reply_to_id=db.get_merchant_message_id(deal_id),
                            stats={"user_id": handler_id, "stat_type": "rejected", "merchant_name": m_name}
                        )
                        await set_reaction_on_chain(merchant_chat_id, deal_id, "👎", IGNORED_USERS)
                        db.delete_deal(deal_id)
                        logger.info(f"Сделка отменена: deal_id={deal_id}")
                        continue
                if deal["status"] != "awaiting" or not deal["deal_id"]:
                    continue
                if time_module.time() - deal["sent_time"] < get_sla_timeout():
                    continue
                try:
                    # Проверка handler_id на валидность
                    if not handler_id or not str(handler_id).isdigit() or int(handler_id) < 0:
                        logger.warning(f"Некорректный handler_id: {handler_id}, deal_id={deal_id}")
                        await bot.send_message(
                            ADMIN_ID,
                            f"⚠️ Некорректный handler_id `{handler_id}` для сделки `{deal_id}`. Проверьте базу данных."
                        )
                        continue
                    # Проверка доступности чата
                    await bot.get_chat(handler_id)
                    sent = await bot.send_message(
                        handler_id,
                        RESPONSE_TEMPLATES["sla_expired"].format(deal_id=deal_id, merchant_name=m_name),
                        reply_markup=create_keyboard(button_type="yes_no")
                    )
                    db.save_sla_notification(deal_id, sent.message_id)
                    logger.debug(f"SLA-уведомление: deal_id={deal_id}, handler_id={handler_id}, message_id={sent.message_id}")
                except telebot.asyncio_helper.ApiTelegramException as e:
                    if e.error_code == 403:
                        logger.warning(f"Не удалось отправить SLA-уведомление: handler_id={handler_id}, deal_id={deal_id}, ошибка=Forbidden")
                        await bot.send_message(
                            ADMIN_ID,
                            f"⚠️ Не удалось отправить SLA-уведомление для сделки `{deal_id}` пользователю `{handler_id}`: бот не имеет доступа к чату."
                        )
                    else:
                        logger.error(f"Ошибка отправки SLA-уведомления: handler_id={handler_id}, deal_id={deal_id}, ошибка={e}")
                        await bot.send_message(
                            ADMIN_ID,
                            f"🚨 Ошибка SLA-уведомления для сделки `{deal_id}`: {e}"
                        )
                except Exception as e:
                    logger.error(f"Неизвестная ошибка в SLA-уведомлении: handler_id={handler_id}, deal_id={deal_id}, ошибка={e}")
                    await bot.send_message(
                        ADMIN_ID,
                        f"🚨 Неизвестная ошибка SLA-уведомления для сделки `{deal_id}`: {e}"
                    )
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        logger.info("Задача check_deals завершена")
        raise
    except Exception as e:
        logger.error(f"Ошибка в check_deals: {e}\n{traceback.format_exc()}")
        await asyncio.sleep(60)

@bot.message_handler(commands=['start', 'help'])
@require_auth
@log_errors()
async def start_help_command(message):
    logger.debug(f"📜 Команда: {message.text}, chat_id={message.chat.id}, user_id={message.from_user.id}")
    text = HELP_TEXT["help"].format(ADMIN_ID=ADMIN_ID) if message.text.startswith("/help") else "👋 Добро пожаловать в PSPWare! Используйте /help для справки."
    await send_message_with_media(message.chat.id, text, reply_to_id=message.message_id)
    logger.info(f"Команда обработана: {message.text}, chat_id={message.chat.id}")

@bot.message_handler(commands=['merchant_list'])
@require_auth
@log_errors()
async def merchant_list_command(obj):
    if isinstance(obj, telebot.types.CallbackQuery):
        chat_id = obj.message.chat.id
        message_id = obj.message.message_id
        user_id = obj.from_user.id
        reply_to_id = None
    else:
        chat_id = obj.chat.id
        message_id = None
        user_id = obj.from_user.id
        reply_to_id = obj.message_id
    logger.debug(f"🏬 Команда /merchant_list: chat_id={chat_id}, user_id={user_id}")
    db.clear_cache()
    merchants = db.get_merchants()
    if not merchants:
        await send_message_with_media(chat_id, CONSTANTS["MERCHANT_NOT_FOUND"], reply_to_id=reply_to_id)
        logger.info(f"Мерчанты не найдены: chat_id={chat_id}")
        return
    keyboard = InlineKeyboardMarkup(row_width=2)
    for m in merchants.values():
        status = "✅" if m.get('chat_id') else "❌"
        keyboard.add(InlineKeyboardButton(text=f"{m['name']} {status}", callback_data=f"merchant_{m['name']}"))
    if message_id:
        await bot.edit_message_text(
            "🏬 Мерчанты:",
            chat_id,
            message_id,
            reply_markup=keyboard
        )
    else:
        await send_message_with_media(chat_id, "🏬 Мерчанты:", reply_to_id=reply_to_id, reply_markup=keyboard)
    logger.info(f"Список мерчантов отправлен: chat_id={chat_id}, кол-во={len(merchants)}")

@bot.message_handler(commands=['shift_start'])
@require_auth
@log_errors()
async def shift_start_command(message):
    logger.debug(f"🚖 Команда /shift_start: chat_id={message.chat.id}, user_id={message.from_user.id}")
    db.start_shift(str(message.from_user.id), time_module.time())
    db.update_stats(str(message.from_user.id), "taken", None)
    start_time = datetime.now().strftime("%H:%M:%S")
    await send_message_with_media(message.chat.id, RESPONSE_TEMPLATES["shift_start"].format(time=start_time), reply_to_id=message.message_id)
    logger.info(f"Смена начата: user_id={message.from_user.id}, chat_id={message.chat.id}")

@bot.message_handler(commands=['shift_stop'])
@require_auth
@log_errors()
async def shift_stop_command(message):
    logger.debug(f"🛑 Команда /shift_stop: chat_id={message.chat.id}, user_id={message.from_user.id}")
    await send_message_with_media(
        message.chat.id,
        RESPONSE_TEMPLATES["shift_stop_confirm"],
        reply_to_id=message.message_id,
        reply_markup=create_keyboard(button_type="yes_no")
    )
    logger.info(f"Запрос на завершение смены: chat_id={message.chat.id}")

@bot.message_handler(commands=['stats'])
@require_auth
@log_errors()
async def stats_command(message):
    logger.debug(f"📊 Команда /stats: chat_id={message.chat.id}, user_id={message.from_user.id}")
    stats = db.get_user_stats(str(message.from_user.id))
    merchants_text = "\n".join(f"{n}: {c}" for n, c in stats.get('merchants', {}).items()) if stats.get('merchants') else "Нет данных"
    stats_text = (
        f"📊 Статистика за {datetime.now().strftime('%Y-%m-%d')}:\n"
        f"Принято: {stats.get('taken', 0)}\n"
        f"Передано: {stats.get('forwarded', 0)}\n"
        f"Успешно: {stats.get('success', 0)}\n"
        f"Отклонено: {stats.get('rejected', 0)}\n"
        f"Просмотрено: {stats.get('viewed', 0)}\n"
        f"Ошибки: {stats.get('errors', 0)}\n"
        f"Итерации: {stats.get('iterations', 0)}\n"
        f"Сообщений: {stats.get('merchant_messages', 0)}\n"
        f"\nМерчанты:\n{merchants_text}"
    )
    await send_message_with_media(message.chat.id, stats_text, reply_to_id=message.message_id)
    logger.info(f"Статистика отправлена: chat_id={message.chat.id}, user_id={message.from_user.id}")

@bot.message_handler(commands=['get_chats'])
@require_admin
@log_errors()
async def get_chats_command(message):
    logger.debug(f"💬 Команда /get_chats: chat_id={message.chat.id}, user_id={message.from_user.id}")
    chats = set()
    updates = await bot.get_updates(offset=-1, limit=100, timeout=30)
    for u in updates:
        if u.message and u.message.chat.id:
            chats.add(str(u.message.chat.id))
    merchants = db.get_merchants()
    cascades = db.get_cascades()
    for m in merchants.values():
        if m.get('chat_id'):
            chats.add(str(m.get('chat_id')))
    for c in cascades.values():
        if c.get('chat_id'):
            chats.add(str(c.get('chat_id')))
    chats_text = []
    for chat_id in chats:
        try:
            chat = await bot.get_chat(chat_id)
            chat_name = chat.title or chat.first_name or chat.username or f"Chat {chat_id}"
        except Exception as e:
            logger.error(f"Ошибка получения чата {chat_id}: {e}")
            chat_name = f"Chat {chat_id}"
        merchants_text = ", ".join(m.get('name', 'Н/Д') for m in merchants.values() if str(m.get('chat_id')) == chat_id)
        cascades_text = ", ".join(c.get('name', 'Н/Д') for c in cascades.values() if str(c.get('chat_id')) == chat_id)
        if merchants_text or cascades_text:
            chats_text.append(f"{chat_id}: {chat_name} ({merchants_text}{', ' if merchants_text and cascades_text else ''}{cascades_text})")
        else:
            chats_text.append(f"{chat_id}: {chat_name}")
    if not chats_text:
        await send_message_with_media(message.chat.id, CONSTANTS["NO_CHAT"], reply_to_id=message.message_id)
        logger.info(f"Чаты не найдены: chat_id={message.chat.id}")
        return
    chats_text = '\n'.join(chats_text)
    await send_message_with_media(
        message.chat.id,
        f"💬 Чаты:\n{chats_text}",
        reply_to_id=message.message_id
    )
    logger.info(f"Список чатов отправлен: chat_id={message.chat.id}, кол-во={len(chats_text.splitlines())}")

@bot.message_handler(commands=['link'])
@require_admin
@log_errors()
async def link_command(message):
    logger.debug(f"🔗 Команда /link: {message.text}, chat_id={message.chat.id}, user_id={message.from_user.id}")
    parts = message.text.split()
    if len(parts) < 3 or parts[0] != '/link':
        await send_message_with_media(message.chat.id, "Использование: /link m|i <name> [chat_id]", reply_to_id=message.message_id)
        return
    link_type = parts[1].lower()
    name = parts[2]
    chat_id = parts[3] if len(parts) > 3 else str(message.chat.id)
    logger.debug(f"Привязка: type={link_type}, name={name}, chat_id={chat_id}")
    if link_type not in ['m', 'i']:
        await send_message_with_media(message.chat.id, "❌ Тип должен быть 'm' или 'i'", reply_to_id=message.message_id)
        return
    try:
        int(chat_id)
    except ValueError:
        await send_message_with_media(message.chat.id, f"❌ Неверный chat_id: {chat_id}", reply_to_id=message.message_id)
        return
    if link_type == 'm':
        merchants = db.get_merchants()
        m = next((m for m in merchants.values() if m.get("name") == name), None)
        if not m:
            await send_message_with_media(message.chat.id, CONSTANTS["MERCHANT_NOT_FOUND"], reply_to_id=message.message_id)
            return
        db.add_merchant(name, name, chat_id=chat_id, handler_id=str(message.from_user.id))
        await send_message_with_media(message.chat.id, f"🔗 Мерчант {name} привязан к чату {chat_id}", reply_to_id=message.message_id)
        logger.info(f"Мерчант привязан: name={name}, chat_id={chat_id}")
    elif link_type == 'i':
        i = db.get_cascade_by_name(name)
        if not i:
            await send_message_with_media(message.chat.id, f"❌ Интегратор {name} не найден", reply_to_id=message.message_id)
            return
        db.merge_cascade(name, name, chat_id=chat_id)
        await send_message_with_media(message.chat.id, f"🔗 Интегратор {name} привязан к чату {chat_id}", reply_to_id=message.message_id)
        logger.info(f"Интегратор привязан: name={name}, chat_id={chat_id}")

@bot.message_handler(commands=['add_merchant', 'delete_merchant', 'add_cascade', 'delete_cascade', 'bind_merchant', 'candles', 'candle', 'add_user', 'remove_user'])
@require_admin
@log_errors()
async def handle_admin_command(message):
    logger.debug(f"🔧 Админ-команда: {message.text}, chat_id={message.chat.id}, user_id={message.from_user.id}")
    parts = message.text.split()
    cmd = parts[0][1:].lower()
    spec = ADMIN_COMMANDS.get(cmd)
    if not spec:
        await send_message_with_media(message.chat.id, f"❌ Неизвестная команда: {cmd}", reply_to_id=message.message_id)
        return
    if len(parts) < spec['args'] + 1:
        usage = f"Использование: /{cmd} {' '.join(['<arg>' for _ in range(spec['args'])])}"
        await send_message_with_media(message.chat.id, usage, reply_to_id=message.message_id)
        return
    try:
        args = parts[1:spec['args'] + 1] + [str(message.from_user.id)]
        spec['action'](db, args)
        db.clear_cache()
        success_msg = spec['success'].format(
            *args[:spec['args']],
            'включён' if cmd in ['candles', 'candle'] and args[1].lower() in ['true', '1', 'вкл'] else 'выключен'
        )
        await send_message_with_media(message.chat.id, success_msg, reply_to_id=message.message_id)
        logger.info(f"Админ-команда выполнена: {cmd}, аргументы={args[:spec['args']]}")
    except Exception as e:
        logger.error(f"Ошибка в админ-команде {cmd}: {e}\n{traceback.format_exc()}")
        await send_message_with_media(message.chat.id, f"❌ Ошибка: {str(e)}", reply_to_id=message.message_id)

@bot.message_handler(commands=['list_cascades'])
@require_auth
@log_errors()
async def list_cascades_command(message):
    logger.debug(f"🤝 Команда /list_cascades: chat_id={message.chat.id}, user_id={message.from_user.id}")
    cascades = db.get_cascades()
    if not cascades:
        await send_message_with_media(message.chat.id, "📭 Интеграторы не найдены.", reply_to_id=message.message_id)
        return
    cascades_text = "\n".join(
        f"{name}: {c.get('name', '')} (chat_id: {c.get('chat_id', 'N/A')}, external_id: {c.get('needs_external_id', 0)})"
        for name, c in sorted(cascades.items())
    )
    await send_message_with_media(message.chat.id, f"🤝 Интеграторы:\n{cascades_text}", reply_to_id=message.message_id)
    logger.info(f"Список интеграторов отправлен: chat_id={message.chat.id}, кол-во={len(cascades)}")

@bot.message_handler(commands=['manage_users'])
@require_admin
@log_errors()
async def manage_users_command(message):
    logger.debug(f"👥 Команда /manage_users: chat_id={message.chat.id}, user_id={message.from_user.id}")
    users_text = "\n".join(str(u) for u in ALLOWED_USERS if db.get_user_token(u)) or "📭 Пользователи не найдены."
    await send_message_with_media(message.chat.id, f"👥 Пользователи:\n{users_text}", reply_to_id=message.message_id)
    logger.info(f"Список пользователей отправлен: chat_id={message.chat.id}, кол-во={len(users_text.splitlines())}")

@bot.message_handler(content_types=['text', 'photo', 'video', 'document', 'animation'])
@log_errors()
async def handle_message(message):
    logger.debug(f"📬 Обработка сообщения: chat_id={message.chat.id}, user_id={message.from_user.id}, type={message.content_type}")
    if message.from_user.id in IGNORED_USERS:
        logger.info(f"Игнорируем: user_id={message.from_user.id}")
        return
    deal_id = await get_deal_id(message)
    if not deal_id:
        appeal = db.get('pending_appeal', str(message.from_user.id))
        if appeal and message.content_type != 'text':
            media = await get_media(message)
            merchants = db.get_merchants()
            m = next((m for m in merchants.values() if str(m.get('chat_id', '')) == str(message.chat.id)), None)
            stats = {
                "user_id": str(message.from_user.id),
                "stat_type": "confirmed",
                "merchant_name": m.get('name') if m else None
            } if m else None
            sent = await send_message_with_media(
                message.chat.id,
                f"🔍 Доказательства для апелляции: `{appeal['deal_id']}`",
                media=media,
                stats=stats
            )
            if stats and sent:
                db.save_proof_message(
                    appeal['deal_id'],
                    str(sent[0].message_id) if isinstance(sent, list) else str(sent.message_id)
                )
                db.delete_message('pending_appeal', str(message.from_user.id))
                logger.info(f"Доказательства: deal_id={appeal['deal_id']}, chat_id={message.chat.id}")
            return
        logger.info(f"deal_id не найден: chat_id={message.chat.id}, message_id={message.message_id}")
        return

    data = await api.get_order(deal_id, message.from_user.id)
    if not data or data.get('status') == 'not_found':
        await send_message_with_media(message.from_user.id, f"❌ Сделка не найдена: `{deal_id}`")
        return

    merchants = db.get_merchants()
    m = next((m for m in merchants.values() if str(m.get('chat_id', '')) == str(message.chat.id)), None)
    if not m:
        logger.info(f"Чат не привязан к мерчанту: chat_id={message.chat.id}")
        return

    media = await get_media(message)
    db.save_media(deal_id, str(media))

    try:
        await bot.set_message_reaction(
            message.chat.id,
            message.message_id,
            [ReactionTypeEmoji(emoji="🔍")]
        )
        await send_message_with_media(
            message.chat.id,
            f"✅ Сделка `{deal_id}` принята для обработки",
            reply_to_id=message.message_id
        )
    except Exception as e:
        logger.error(f"Ошибка реакции/подтверждения: chat_id={message.chat.id}, error={e}")

    integrator_id = data.get('integrator', {}).get('id')
    integrator = db.get_cascade_by_id(str(integrator_id)) if integrator_id else None
    text = await format_deal_msg(data, m['name'], integrator=integrator)
    stats = {
        "user_id": str(message.from_user.id),
        "stat_type": "pending",
        "merchant_name": m['name']
    }
    handler_id = m.get('handler_id') or str(ADMIN_ID)
    target_chat_id = handler_id
    sent = await send_message_with_media(
        target_chat_id,
        text,
        media=media,
        reply_markup=create_keyboard(button_type="action", buttons_data={"is_integrator": False}),
        stats=stats
    )

    if sent:
        db.save_deal(
            deal_id,
            target_chat_id,
            str(sent[0].message_id) if isinstance(sent, list) else str(sent.message_id),
            "awaiting",
            time_module.time(),
            m.get('merchant_id'),
            str(message.from_user.id)
        )
        db.save_message(deal_id, str(message.chat.id), str(message.message_id))
        logger.info(f"Уведомление отправлено: deal_id={deal_id}, target_chat_id={target_chat_id}")

@bot.callback_query_handler(func=lambda c: c.data in ["approve", "reject", "view"])
@with_deal_data()
async def handle_action_buttons(call, data, deal_id):
    logger.debug(f"🔍 Action: callback_data={call.data}, deal_id={deal_id}, chat_id={call.message.chat.id}")
    merchants = db.get_merchants()
    m = next((m for m in merchants.values() if m.get('merchant_id') == str(db.get_deal_by_id(deal_id).get('merchant_id', ''))), None)
    m_name = m.get('name') if m else None
    m_chat_id = m.get('chat_id') if m else None
    cascades = db.get_cascades()
    i_name = data.get('integrator', {}).get('id')
    i = cascades.get(i_name) if i_name else None
    if not i and i_name:
        for cascade_name, c in cascades.items():
            if str(i_name).startswith(cascade_name):
                i = c
                logger.debug(f"Частичное совпадение интегратора: API_id={i_name}, cascade_name={cascade_name}")
                break
    i_chat_id = i.get('chat_id') if i else None

    stats = {
        "user_id": str(call.from_user.id),
        "stat_type": "success" if call.data == "approve" else "rejected" if call.data == "reject" else "view",
        "merchant_name": m_name
    }

    media_data = db.get_media(deal_id)
    media_data = json.loads(media_data) if media_data else None

    if call.data == "approve":
        if not i_chat_id:
            await send_message_with_media(
                m_chat_id,
                f"❌ Сделка `{deal_id}` не отправлена интегратору. Обработайте вручную.",
                reply_to_id=db.get_merchant_message_id(deal_id)
            )
            db.save_deal_status(deal_id, "pending_manual")
            await bot.answer_callback_query(call.id, "❌ Нет интегратора")
            return
        reaction = [ReactionTypeEmoji(emoji="✅")]
        await send_message_with_media(
            m_chat_id,
            f"✅ Сделка `{deal_id}` одобрена ({i.get('name', '')})",
            reply_to_id=db.get_merchant_message_id(deal_id),
            reaction=reaction,
            stats=stats
        )
        text = await format_deal_msg(data, m_name, integrator=i, is_integrator=True)
        sent = await send_message_with_media(
            i_chat_id,
            text,
            media=media_data,
            reply_markup=create_keyboard("action", buttons_data={"is_integrator": True}),
            stats=None
        )
        if sent:
            db.save_deal(
                deal_id,
                i_chat_id,
                str(sent[0].message_id) if isinstance(sent, list) else str(sent.message_id),
                "awaiting_integrator",
                time_module.time(),
                m.get('merchant_id'),
                str(call.from_user.id)
            )
            db.save_message(deal_id, str(i_chat_id), str(sent[0].message_id) if isinstance(sent, list) else str(sent.message_id))
            await bot.edit_message_text(
                f"✅ Спор `{deal_id}` отправлен интегратору",
                call.message.chat.id,
                call.message.message_id
            )
            await bot.answer_callback_query(call.id, CONSTANTS["DEAL_OK"])
    elif call.data == "reject":
        await bot.edit_message_reply_markup(
            call.message.chat.id,
            call.message.message_id,
            reply_markup=create_keyboard("reject", row_width=2)
        )
        await bot.answer_callback_query(call.id, "Выберите причину отказа")
    elif call.data == "view":
        await bot.answer_callback_query(call.id, CONSTANTS["VIEWED"])
        db.update_stats(str(call.from_user.id), stats["stat_type"], m_name)

@bot.callback_query_handler(func=lambda c: c.data.startswith("reason_"))
@with_deal_data()
async def handle_reject_reason(call, data, deal_id):
    logger.debug(f"🚫 Reject Reason: deal_id={deal_id}, reason={call.data}, chat_id={call.message.chat.id}")
    merchants = db.get_merchants()
    m = next((m for m in merchants.values() if m.get('merchant_id') == str(db.get_deal_by_id(deal_id).get('merchant_id', ''))), None)
    reason = call.data.replace("reason_", "")
    reason_text = {
        "fake": "Фейк-чек",
        "rec": "Неверные реквизиты",
        "request_external_id": "Запросите доп.",
        "no_payment": "Не было поступления",
        "other": "Другое"
    }.get(reason, "Неизвестная причина")
    reaction = [ReactionTypeEmoji(emoji="👎")]
    error_text = f"❌ Сделка `{deal_id}` отклонена: {reason_text}"
    await send_message_with_media(
        m.get('chat_id'),
        error_text,
        reply_to_id=db.get_merchant_message_id(deal_id),
        reaction=reaction,
        stats={
            "user_id": str(call.from_user.id),
            "stat_type": "rejected",
            "merchant_name": m.get('name')
        }
    )
    await set_reaction_on_chain(m.get('chat_id'), deal_id, "👎", IGNORED_USERS)
    if reason == "other":
        for merchant in merchants.values():
            if merchant.get('chat_id') != m.get('chat_id'):
                await send_message_with_media(
                    merchant.get('chat_id'),
                    f"⚖️ Интегратор отклонил `{deal_id}` по причине 'Другое'. Обработайте вручную."
                )
    db.save_deal_status(deal_id, "rejected")
    await bot.edit_message_reply_markup(
        call.message.chat.id,
        call.message.message_id,
        reply_markup=None
    )
    await bot.answer_callback_query(call.id, CONSTANTS["DEAL_REJECTED"])

@bot.callback_query_handler(func=lambda c: c.data in ["integrator_approve", "integrator_reject"])
@with_deal_data()
async def handle_integrator_buttons(call, data, deal_id):
    logger.debug(f"🤖 Integrator: action={call.data}, deal_id={deal_id}, chat_id={call.message.chat.id}")
    merchants = db.get_merchants()
    cascades = db.get_cascades()
    i_name = data.get('integrator', {}).get('id')
    i = cascades.get(str(i_name)) if i_name else None
    if not i and i_name:
        for cascade_name, c in cascades.items():
            if str(i_name).startswith(cascade_name):
                i = c
                logger.info(f"Частичное совпадение: API_id={i_name}, cascade_name={cascade_name}")
                break
    i_chat_id = i.get('chat_id') if i else None
    if not i or str(call.message.chat.id) != str(i_chat_id):
        await bot.answer_callback_query(call.id, CONSTANTS["INTEGRATOR_CHAT_NOT_SET"])
        return
    m = next((m for m in merchants.values() if m.get('merchant_id') == str(db.get_deal_by_id(deal_id).get('merchant_id', ''))), None)
    m_name = m.get('name') if m else None
    deal = db.get_deal_by_id(deal_id)
    sent_time = deal.get('sent_time', time_module.time()) if deal else time_module.time()
    is_sla_exp = time_module.time() - sent_time >= get_sla_timeout()

    if call.data == "integrator_approve":
        if data.get('status') != "success":
            await send_message_with_media(
                i_chat_id,
                f"❌ Сделка `{deal_id}` не в статусе 'success'. Проверьте callback."
            )
            await bot.answer_callback_query(call.id, "❌ Callback требуется")
            return
        reaction = [ReactionTypeEmoji(emoji="👍")]
        await send_message_with_media(
            m.get('chat_id'),
            f"✅ Сделка завершена: `{deal_id}`",
            reply_to_id=db.get_merchant_message_id(deal_id),
            reaction=reaction,
            stats={"user_id": str(call.from_user.id), "stat_type": "success", "merchant_name": m_name}
        )
        await set_reaction_on_chain(m.get('chat_id'), deal_id, "👍", IGNORED_USERS)
        db.delete_message(i_chat_id, str(call.message.message_id))
        db.save_deal_status(deal_id, "completed")
        await bot.answer_callback_query(call.id, CONSTANTS["DEAL_OK"])
        logger.info(f"Интегратор одобрил: deal_id={deal_id}")
    elif call.data == "integrator_reject":
        if is_sla_exp:
            for merchant in merchants.values():
                if merchant.get('chat_id') != m.get('chat_id'):
                    await send_message_with_media(
                        merchant.get('chat_id'),
                        f"⚖️ Интегратор попытался отменить `{deal_id}` после SLA."
                    )
            await bot.answer_callback_query(call.id, "❌ SLA истёк")
            logger.info(f"Отказ отклонен из-за истекшего SLA: deal_id={deal_id}")
            return
        await bot.edit_message_reply_markup(
            call.message.chat.id,
            call.message.message_id,
            reply_markup=create_keyboard("reject", row_width=2)
        )
        await bot.answer_callback_query(call.id, "Выберите причину отказа")

@bot.callback_query_handler(func=lambda c: c.data in ["YES", "NO"])
@log_errors()
async def handle_shift_stop(call):
    logger.debug(f"🛑 Shift Stop: {call.data}, chat_id={call.message.chat.id}")
    if call.data == "YES":
        db.stop_shift(str(call.from_user.id), time_module.time())
        db.update_stats(str(call.from_user.id), "stopped", None)
        end_time = datetime.now().strftime("%H:%M")
        await bot.edit_message_text(
            f"✅ Смена завершена: {end_time}",
            call.message.chat.id,
            call.message.message_id
        )
        logger.info(f"Смена завершена: user_id={call.from_user.id}, time={end_time}")
    else:
        await bot.edit_message_text(
            f"❌ Завершение отменено",
            call.message.chat.id,
            call.message.message_id
        )
        logger.info(f"Завершение отменено: user_id={call.from_user.id}")
    await bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("merchant_"))
@log_errors()
async def handle_merchant_callback(call):
    logger.debug(f"🏬 Merchant: {call.data}, chat_id={call.message.chat.id}, user_id={call.from_user.id}")
    db.clear_cache()
    mid = call.data.replace("merchant_", "")
    merchants = db.get_merchants()
    m = next((m for m in merchants.values() if m.get('name') == mid), None)
    if not m:
        await bot.answer_callback_query(call.id, CONSTANTS["MERCHANT_NOT_FOUND"])
        logger.warning(f"Мерчант не найден: {mid}")
        return
    text = f"🏬 Мерчант: {m.get('name')}\nЧат: {m.get('chat_id') or 'Не привязан'}\nОбработчик: {m.get('handler_id') or 'Не задан'}"
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_list"))
    await bot.edit_message_text(
        text,
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        reply_markup=keyboard
    )
    await bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data == "back_to_list")
@require_auth
@log_errors()
async def handle_back_to_list(call):
    logger.debug(f"🔙 Back to list: chat_id={call.message.chat.id}, user_id={call.from_user.id}")
    await merchant_list_command(call)
    await bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data in ["send_reminder", "skip_reminder"])
@with_deal_data()
async def handle_sla_reminder(call, data, deal_id):
    logger.debug(f"⏰ SLA Reminder: {call.data}, deal_id={deal_id}, chat_id={call.message.chat.id}")
    merchants = db.get_merchants()
    m = next((m for m in merchants.values() if m.get('merchant_id') == str(db.get_deal_by_id(deal_id).get('merchant_id', ''))), None)
    m_name = m.get('name') if m else None
    if call.data == "send_reminder":
        cascades = db.get_cascades()
        i_name = data.get('integrator', {}).get('id')
        i = cascades.get(str(i_name)) if i_name else None
        if not i and i_name:
            for cascade_name, c in cascades.items():
                if str(i_name).startswith(cascade_name):
                    i = c
                    logger.info(f"Частичное совпадение: API_id={i_name}, cascade_name={cascade_name}")
                    break
        i_chat_id = i.get('chat_id') if i else None
        if not i or not i_chat_id:
            await bot.edit_message_text(
                f"❌ Напоминание не отправлено: интегратор не найден для `{deal_id}`",
                call.message.chat.id,
                call.message.message_id
            )
            await bot.answer_callback_query(call.id, CONSTANTS["INTEGRATOR_NOT_SET"])
            return
        text = f"⏰ Напоминание: обработайте сделку `{deal_id}` ({m_name})"
        await send_message_with_media(
            i_chat_id,
            text
        )
        await bot.edit_message_text(
            f"✅ Напоминание отправлено: `{deal_id}`",
            call.message.chat.id,
            call.message.message_id
        )
        db.update_stats(str(call.from_user.id), "reminders_stats", m_name)
        logger.info(f"Напоминание отправлено: deal_id={deal_id}, i_chat_id={i_chat_id}")
    else:
        await bot.edit_message_text(
            f"✅ Напоминание пропущено: `{deal_id}`",
            call.message.chat.id,
            call.message.message_id
        )
        logger.info(f"Напоминание пропущено: {deal_id}")
    await bot.answer_callback_query(call.id)

async def shutdown():
    logger.info("Завершение работы бота...")
    tasks = asyncio.all_tasks()
    for task in tasks:
        task.cancel()
        try:
            await task
        except:
            pass
    await bot.delete_webhook()
    if api.session and not api.session.closed:
        await api.session.close()
        logger.info("Сессия aiohttp закрыта")
    logger.info("Bot shutdown complete")

async def main():
    logger.info("Запуск бота...")
    try:
        tasks = [asyncio.create_task(check_deals())]
        await bot.delete_webhook()
        await bot.polling()
    except KeyboardInterrupt:
        logger.info("Получен Ctrl+C")
        await shutdown()
    except Exception as e:
        logger.error(f"Ошибка при запуске: {e}\n{traceback.format_exc()}")
        await bot.send_message(ADMIN_ID, f"🚨 Ошибка: {e}")
        await asyncio.sleep(5)
        await shutdown()

asyncio.run(main())
