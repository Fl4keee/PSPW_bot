import aiohttp
from typing import Dict, Any, Optional
import logging
from config import API_USERNAME, API_PASSWORD, API_BASE_URL

from typing import List, Dict, Any, Optional, Callable, Tuple
import asyncio
from datetime import datetime
import pytz
from tenacity import retry, stop_after_attempt
MONTHS = {
    "01": "января", "02": "февраля", "03": "марта", "04": "апреля",
    "05": "мая", "06": "июня", "07": "июля", "08": "августа",
    "09": "сентября", "10": "октября", "11": "ноября", "12": "декабря"
}


logger = logging.getLogger(__name__)


def format_created_at(created_at: str) -> str:
    """Форматировать дату в ДД ММММ ГГГГ, ЧЧ:ММ:СС (MSK)."""
    try:
        dt = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        dt = dt.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone("Europe/Moscow"))
        return f"{dt.day:02d} {MONTHS[f'{dt.month:02d}']} {dt.year}, {dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}"
    except ValueError:
        return "Не указано"



class PayphoriaAPI:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.token_cache: Dict[int, str] = {}

    async def start(self):
        """Инициализация сессии."""
        self.session = aiohttp.ClientSession()

    async def close(self):
        """Закрытие сессии."""
        if self.session:
            await self.session.close()

    @retry(stop=stop_after_attempt(3))
    async def get_token(self, user_id: int, order_id: Optional[str] = None) -> Optional[str]:
        """Получить токен."""
        if user_id in self.token_cache:
            return self.token_cache[user_id]

        async with self.session.post(
            API_BASE_URL + "users/login",
            json={"username": API_USERNAME, "password": API_PASSWORD}
        ) as response:

            if response.status == 200:
                data = await response.json()

                token = data.get("accessToken")
                self.token_cache[user_id] = token

                return token
            logger.error(f"Ошибка авторизации: {response.status}")
            return None

    @retry(stop=stop_after_attempt(3))
    async def get_order(self, order_id: str, user_id: int) -> Optional[Dict[str, Any]]:
        """Получить данные сделки."""
        token = await self.get_token(user_id)

        if not token:
            return None

        async with self.session.get(
            API_BASE_URL + f"orders/{order_id}",
            headers={"Authorization": f"Bearer {token}"}
        ) as response:

            if response.status == 200:
                data = await response.json()
                logger.debug(f"получена сделка {order_id}: {response.status}")
                return {
                    "deal_id": data["id"],
                    "merchant_name": data.get("merchant_name", "Unknown"),
                    "integrator_name": data.get("integrator", {}).get("name", "Unknown"),
                    "recipient": data.get("recipient", "N/A"),
                    "card": data.get("card", "N/A"),
                    "bank_name": data.get("bankName", "N/A"),
                    "sbp_type": "СБП" if data.get("is_sbp") else "Карта",
                    "sum": data.get("sum", 0.0),
                    "currency": data.get("currency", "RUB"),
                    "status": data.get("status", "unknown"),
                    "created_at": format_created_at(data.get("createdAt", "")),
                    "integrator_order_id": f"ID интегратора: {data.get('integratorOrderId', 'N/A')}" if data.get("integratorOrderId") else ""
                }
            logger.error(f"Ошибка получения сделки {order_id}: {response.status}")
            return None

    async def validate_token(self, user_id: int, token: str) -> bool:
        """Проверить токен."""
        async with self.session.get(
            API_BASE_URL + "orders",
            headers={"Authorization": f"Bearer {token}"}
        ) as response:
            return response.status == 200