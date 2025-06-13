import json
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
import uuid
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

class Database:
    """Класс для работы с базой данных бота PSPWare на основе JSON Lines."""

    def __init__(self):
        """Инициализация директории и файлов базы данных."""
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        self.files = {
            "users": self.data_dir / "users.jsonl",
            "merchants": self.data_dir / "merchants.jsonl",
            "cascades": self.data_dir / "cascades.jsonl",
            "deals": self.data_dir / "deals.jsonl",
            "messages": self.data_dir / "messages.jsonl",
            "sla_notifications": self.data_dir / "sla_notifications.jsonl",
            "stats": self.data_dir / "stats.jsonl",
            "shifts": self.data_dir / "shifts.jsonl",
            "appeals": self.data_dir / "appeals.jsonl",
            "proof_messages": self.data_dir / "proof_messages.jsonl"
        }
        for file in self.files.values():
            if not file.exists():
                file.touch()

    def _read_jsonl(self, file_path: Path) -> List[Dict[str, Any]]:
        """Чтение JSON Lines файла."""
        try:
            with file_path.open("r", encoding="utf-8") as f:
                return [json.loads(line) for line in f if line.strip()]
        except Exception as e:
            logger.error(f"Ошибка чтения {file_path}: {e}")
            return []

    def _write_jsonl(self, file_path: Path, data: List[Dict[str, Any]]) -> None:
        """Запись данных в JSON Lines файл."""
        try:
            with file_path.open("w", encoding="utf-8") as f:
                for item in data:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.error(f"Ошибка записи {file_path}: {e}")

    def add_merchant(self, name: str, display_name: str, chat_id: Optional[int] = None, handler_id: Optional[int] = None) -> bool:
        """Добавить мерчанта."""
        try:
            merchants = self._read_jsonl(self.files["merchants"])
            if any(m["name"] == name for m in merchants):
                logger.warning(f"Мерчант {name} уже существует")
                return False
            merchants.append({
                "name": name,
                "display_name": display_name,
                "chat_id": chat_id,
                "merchant_id": str(uuid.uuid4()),
                "handler_id": handler_id
            })
            self._write_jsonl(self.files["merchants"], merchants)
            logger.info(f"Добавлен мерчант {name}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления мерчанта {name}: {e}")
            return False

    def delete_merchant(self, name: str) -> bool:
        """Удалить мерчанта."""
        try:
            merchants = self._read_jsonl(self.files["merchants"])
            new_merchants = [m for m in merchants if m["name"] != name]
            if len(new_merchants) == len(merchants):
                logger.warning(f"Мерчант {name} не найден")
                return False
            self._write_jsonl(self.files["merchants"], new_merchants)
            logger.info(f"Удалён мерчант {name}")
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления мерчанта {name}: {e}")
            return False

    def get_merchant(self, chat_id: Optional[int] = None, name: Optional[str] = None) -> Dict[str, Any]:
        """Получить мерчанта по chat_id или name."""
        merchants = self._read_jsonl(self.files["merchants"])
        for merchant in merchants:
            if (chat_id and merchant.get("chat_id") == chat_id) or (name and merchant["name"] == name):
                return merchant
        return {}

    def get_merchants(self) -> List[Dict[str, Any]]:
        """Получить всех мерчантов."""
        return self._read_jsonl(self.files["merchants"])

    def merge_cascade(self, name: str, display_name: str, chat_id: Optional[int] = None, needs_external_id: Optional[bool] = None) -> bool:
        """Добавить или обновить интегратора."""
        try:
            cascades = self._read_jsonl(self.files["cascades"])
            existing = next((c for c in cascades if c["name"] == name), None)
            if existing:
                existing["display_name"] = display_name
                if chat_id is not None:
                    existing["chat_id"] = chat_id
                if needs_external_id is not None:
                    existing["needs_external_id"] = needs_external_id
            else:
                cascades.append({
                    "name": name,
                    "display_name": display_name,
                    "chat_id": chat_id,
                    "needs_external_id": needs_external_id if needs_external_id is not None else False
                })
            self._write_jsonl(self.files["cascades"], cascades)
            logger.info(f"Обновлён/добавлен интегратор {name}")
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления интегратора {name}: {e}")
            return False

    def delete_cascade(self, name: str) -> bool:
        """Удалить интегратора."""
        try:
            cascades = self._read_jsonl(self.files["cascades"])
            new_cascades = [c for c in cascades if c["name"] != name]
            if len(new_cascades) == len(cascades):
                logger.warning(f"Интегратор {name} не найден")
                return False
            self._write_jsonl(self.files["cascades"], new_cascades)
            logger.info(f"Удалён интегратор {name}")
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления интегратора {name}: {e}")
            return False

    def get_cascades(self) -> List[Dict[str, Any]]:
        """Получить всех интеграторов."""
        return self._read_jsonl(self.files["cascades"])

    def add_deal(self, deal_id: str, merchant_chat_id: int, message_id: int, status: str, sent_time: float, merchant_id: str, handler_id: int) -> bool:
        """Добавить сделку."""
        try:
            deals = self._read_jsonl(self.files["deals"])
            if any(d["deal_id"] == deal_id for d in deals):
                logger.warning(f"Сделка {deal_id} уже существует")
                return False
            deals.append({
                "deal_id": deal_id,
                "merchant_chat_id": merchant_chat_id,
                "message_id": message_id,
                "status": status,
                "sent_time": sent_time,
                "merchant_id": merchant_id,
                "handler_id": handler_id
            })
            self._write_jsonl(self.files["deals"], deals)
            logger.info(f"Добавлена сделка {deal_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления сделки {deal_id}: {e}")
            return False

    def get_deals(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получить сделки по статусу."""
        deals = self._read_jsonl(self.files["deals"])
        if status:
            return [d for d in deals if d["status"] == status]
        return deals

    def update_deal_status(self, deal_id: str, status: str) -> bool:
        """Обновить статус сделки."""
        try:
            deals = self._read_jsonl(self.files["deals"])
            for deal in deals:
                if deal["deal_id"] == deal_id:
                    deal["status"] = status
                    self._write_jsonl(self.files["deals"], deals)
                    logger.info(f"Обновлён статус сделки {deal_id} на {status}")
                    return True
            logger.warning(f"Сделка {deal_id} не найдена")
            return False
        except Exception as e:
            logger.error(f"Ошибка обновления статуса сделки {deal_id}: {e}")
            return False

    def add_message(self, deal_id: str, chat_id: int, message_id: int, user_id: int, sent_time: float) -> bool:
        """Добавить сообщение."""
        try:
            messages = self._read_jsonl(self.files["messages"])
            messages.append({
                "deal_id": deal_id,
                "chat_id": chat_id,
                "message_id": message_id,
                "user_id": user_id,
                "sent_time": sent_time
            })
            self._write_jsonl(self.files["messages"], messages)
            logger.info(f"Добавлено сообщение для сделки {deal_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления сообщения для {deal_id}: {e}")
            return False

    def get_messages(self, deal_id: Optional[str] = None, chat_id: Optional[int] = None, message_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Получить сообщения по deal_id, chat_id или message_id."""
        messages = self._read_jsonl(self.files["messages"])
        if deal_id:
            messages = [m for m in messages if m["deal_id"] == deal_id]
        if chat_id:
            messages = [m for m in messages if m["chat_id"] == chat_id]
        if message_id:
            messages = [m for m in messages if m["message_id"] == message_id]
        return messages

    def add_stat(self, user_id: int, stat_type: str, merchant_name: str, count: int = 1) -> bool:
        """Добавить статистику."""
        try:
            stats = self._read_jsonl(self.files["stats"])
            date = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%Y-%m-%d")
            existing = next((s for s in stats if s["user_id"] == user_id and s["date"] == date), None)
            if existing:
                existing[stat_type] = existing.get(stat_type, 0) + count
                if merchant_name not in existing.get("merchants", []):
                    existing["merchants"] = existing.get("merchants", []) + [merchant_name]
            else:
                stats.append({
                    "user_id": user_id,
                    "date": date,
                    stat_type: count,
                    "merchants": [merchant_name] if merchant_name else []
                })
            self._write_jsonl(self.files["stats"], stats)
            logger.info(f"Добавлена статистика {stat_type} для {user_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления статистики для {user_id}: {e}")
            return False

    def get_stats(self, user_id: int, date: str) -> Dict[str, Any]:
        """Получить статистику за день."""
        stats = self._read_jsonl(self.files["stats"])
        for stat in stats:
            if stat["user_id"] == user_id and stat["date"] == date:
                return stat
        return {}

    def save_user_token(self, user_id: int, token: Optional[str]) -> bool:
        """Сохранить или удалить токен пользователя."""
        try:
            users = self._read_jsonl(self.files["users"])
            existing = next((u for u in users if u["user_id"] == user_id), None)
            if existing:
                if token is None:
                    users = [u for u in users if u["user_id"] != user_id]
                else:
                    existing["token"] = token
            elif token:
                users.append({"user_id": user_id, "token": token})
            self._write_jsonl(self.files["users"], users)
            logger.info(f"Обновлён токен для пользователя {user_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления токена для {user_id}: {e}")
            return False

    def get_user(self, user_id: int) -> Dict[str, Any]:
        """Получить пользователя."""
        users = self._read_jsonl(self.files["users"])
        for user in users:
            if user["user_id"] == user_id:
                return user
        return {}

    def get_users(self) -> List[Dict[str, Any]]:
        """Получить всех пользователей."""
        return self._read_jsonl(self.files["users"])

    def add_appeal(self, deal_id: str, user_id: int, is_manual: bool) -> bool:
        """Добавить апелляцию."""
        try:
            appeals = self._read_jsonl(self.files["appeals"])
            if any(a["deal_id"] == deal_id for a in appeals):
                logger.warning(f"Апелляция для {deal_id} уже существует")
                return False
            appeals.append({
                "deal_id": deal_id,
                "user_id": user_id,
                "is_manual": is_manual,
                "created_at": datetime.now(pytz.timezone("Europe/Moscow")).timestamp()
            })
            self._write_jsonl(self.files["appeals"], appeals)
            logger.info(f"Добавлена апелляция для {deal_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления апелляции для {deal_id}: {e}")
            return False

    def get_appeals(self, deal_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получить апелляции."""
        appeals = self._read_jsonl(self.files["appeals"])
        if deal_id:
            return [a for a in appeals if a["deal_id"] == deal_id]
        return appeals

    def add_sla_notification(self, deal_id: str, message_id: int, sent: bool) -> bool:
        """Добавить SLA-уведомление."""
        try:
            sla_notifications = self._read_jsonl(self.files["sla_notifications"])
            sla_notifications.append({
                "deal_id": deal_id,
                "message_id": message_id,
                "sent": sent,
                "sent_time": datetime.now(pytz.timezone("Europe/Moscow")).timestamp()
            })
            self._write_jsonl(self.files["sla_notifications"], sla_notifications)
            logger.info(f"Добавлено SLA-уведомление для {deal_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления SLA-уведомления для {deal_id}: {e}")
            return False

    def get_sla_notifications(self, deal_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получить SLA-уведомления."""
        sla_notifications = self._read_jsonl(self.files["sla_notifications"])
        if deal_id:
            return [n for n in sla_notifications if n["deal_id"] == deal_id]
        return sla_notifications

    def add_shift(self, user_id: int, start_time: float, end_time: Optional[float] = None) -> bool:
        """Добавить смену."""
        try:
            shifts = self._read_jsonl(self.files["shifts"])
            shifts.append({
                "user_id": user_id,
                "start_time": start_time,
                "end_time": end_time
            })
            self._write_jsonl(self.files["shifts"], shifts)
            logger.info(f"Добавлена смена для {user_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления смены для {user_id}: {e}")
            return False

    def get_shifts(self, user_id: int) -> List[Dict[str, Any]]:
        """Получить смены пользователя."""
        shifts = self._read_jsonl(self.files["shifts"])
        return [s for s in shifts if s["user_id"] == user_id]

    def add_proof_message(self, deal_id: str, message_id: int) -> bool:
        """Добавить сообщение с доказательствами."""
        try:
            proof_messages = self._read_jsonl(self.files["proof_messages"])
            proof_messages.append({
                "deal_id": deal_id,
                "message_id": message_id,
                "created_at": datetime.now(pytz.timezone("Europe/Moscow")).timestamp()
            })
            self._write_jsonl(self.files["proof_messages"], proof_messages)
            logger.info(f"Добавлено доказательство для {deal_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления доказательства для {deal_id}: {e}")
            return False

    def get_proof_messages(self, deal_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получить сообщения с доказательствами."""
        proof_messages = self._read_jsonl(self.files["proof_messages"])
        if deal_id:
            return [p for p in proof_messages if p["deal_id"] == deal_id]
        return proof_messages

    def delete_deals_except(self, status: str) -> bool:
        """Удалить все сделки, кроме указанного статуса."""
        try:
            deals = self._read_jsonl(self.files["deals"])
            new_deals = [d for d in deals if d["status"] == status]
            self._write_jsonl(self.files["deals"], new_deals)
            logger.info(f"Удалены сделки, кроме статуса {status}")
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления сделок: {e}")
            return False

    def update_merchant_handler(self, merchant_name: str, handler_id: int) -> bool:
        """Обновить handler_id для мерчанта."""
        try:
            merchants = self._read_jsonl(self.files["merchants"])
            for merchant in merchants:
                if merchant["name"] == merchant_name:
                    merchant["handler_id"] = handler_id
                    self._write_jsonl(self.files["merchants"], merchants)
                    logger.info(f"Обновлён handler_id для мерчанта {merchant_name}")
                    return True
            logger.warning(f"Мерчант {merchant_name} не найден")
            return False
        except Exception as e:
            logger.error(f"Ошибка обновления handler_id для {merchant_name}: {e}")
            return False