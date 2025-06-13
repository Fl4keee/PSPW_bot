import asyncio
import logging
import os
from logging.handlers import RotatingFileHandler
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config import BOT_TOKEN
from database import Database
from api import PayphoriaAPI
from handlers import commands, callbacks, messages, edited_messages, tasks

if not os.path.exists('logs'):
        os.makedirs('logs')

logging.basicConfig(
    level=logging.DEBUG,

    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler("logs/bot.log", maxBytes=10*1024*1024, backupCount=5, encoding='UTF-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    db = Database()


    api = PayphoriaAPI()

    dp.include_router(commands.router)
    dp.include_router(callbacks.router)
    dp.include_router(messages.router)
    dp.include_router(edited_messages.router)
    dp.include_router(tasks.router)

    await api.start()

    try:
        await tasks.start_tasks(bot, db, api)

        await dp.start_polling(bot, db=db, api=api)

    finally:
        await api.close()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())