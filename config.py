from typing import Dict, List, Tuple

ADMIN_IDS: list[int] = [6726178723, 6787231702]
ALLOWED_USERS: set[int] = {6726178723, 6787231702}
IGNORED_USERS: set[int] = {}  # Автоматически все ALLOWED_USERS
BOT_TOKEN: str = "7864952387:AAGORlzuTL-Vw3XrJuJOOCn8OyVIzpUWDGg"
API_USERNAME: str = "SupportBublik"
API_PASSWORD: str = "SOSAL7777"
API_BASE_URL: str = "https://api.payphoria.space/payphoria/api/v1/"
API_AUTH_URL: str = API_BASE_URL + "users/login"
SLA_DAY_SECONDS: int = 2400
SLA_NIGHT_SECONDS: int = 3600
DAY_START: str = "10:00"  # MSK
DAY_END: str = "22:00"  # MSK
EDIT_TIMEOUT_SECONDS: int = 30

HELP_TEXT: Dict[str, str] = {
    "help": """
📖 Команды PSPWare
Бот для сделок. Доступ для сотрудников.

**Для сотрудников**
👋 /start — Запуск
📘 /help — Команды
📋 /merchant_list — Мерчанты
🚗 /shift_start — Начать смену
🛑 /shift_stop — Завершить смену
📈 /stats — Статистика
📩 /get_chats — Чаты
🔗 /link m <name> [chat_id] — Привязать мерчанта (админ)
🔗 /link i <name> [chat_id] — Привязать интегратора (админ)

**Для админов**
➕ /add_merchant <name> — Добавить мерчанта
➖ /delete_merchant <name> — Удалить
➕ /add_cascade <name> — Добавить интегратора
✨ /candles <name> <вкл/выкл> — Внешний ID
➖ /delete_cascade <name> — Удалить интегратора
📋 /list_cascades — Интеграторы
➕ /add_user <user_id> — Добавить сотрудника
➖ /remove_user <user_id> — Удалить
👥 /manage_users — Сотрудники
🔗 /bind_merchant <name> — Привязать

Примеры:
- /candles Payphoria вкл
- /link m Shop123

Админы: {admin_ids}
"""
}

CONSTANTS: Dict[str, str] = {
    "ACCESS_DENIED": "🚫 Доступ запрещён!",
    "MERCHANT_NOT_FOUND": "🚫 Мерчант не найден!",
    "INTEGRATOR_CHAT_NOT_FOUND": "⚠️ Чат интегратора не установлен!",
    "NO_CHAT": "🏠 Нет чатов!",
    "DEAL_NOT_FOUND": "🗑️ Сделка не найдена!",
    "DATA_ERROR": "⚠️ Ошибка данных!",
    "DEAL_OK": "✅ Сделка подтверждена!",
    "DEAL_REJECTED": "❌ Сделка отклонена!",
    "VIEWED": "👁️ Просмотрено!\n———————\nВзята в ручную обработку.",
    "NO_APPEAL": "🗓️ Нет апелляций!",
    "DEAL_ID_PATTERN": r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
    "PARTIAL_INTEGRATOR_MATCH": "⚠️ Частичное совпадение интегратора: {integrator_name} (API) ~ {cascade_name} (база)."
}

RESPONSE_TEMPLATES: Dict[str, str] = {
    "deal_info": (
        "🆔 Сделка: <code>{deal_id}</code>\n"
        "🏪 Мерчант: {merchant_name}\n"
        "🤝 Интегратор: {integrator_name}\n"
        "👤 Получатель: {recipient}\n"
        "💳 Реквизиты: {card}\n"
        "🏦 Банк: {bank_name}\n"
        "💸 Тип: {sbp_type}\n"
        "💰 Сумма: {sum} {currency}\n"
        "📊 Статус: {status}\n"
        "📅 Создано: {created_at}\n"
        "{integrator_order_id}"
    ),
    "deal_accepted": "✅ Сделка <code>{deal_id}</code> принята для обработки! 🛠️",
    "deal_completed": "✔️ Сделка <code>{deal_id}</code> завершена! 🎉",
    "deal_rejected": "❌ Сделка <code>{deal_id}</code> отклонена: {reason_text} 🚫",
    "shift_start": "🚗 Смена началась в {time}! 🕒",
    "shift_stop_confirm": "🛑 Подтвердите завершение смены:",
    "shift_stop_report": (
        "{stats}\n"
        "🗑️ Удалено {count} сделок! ✅ Смена завершена в {time}"
    ),
    "stats": (
        "📈 Статистика за {date} 📊\n"
        "👤 Пользователь: {username}\n"
        "🆔 Принято: {taken}\n"
        "✅ Подтверждено: {approved}\n"
        "✔️ Успешно: {completed}\n"
        "❌ Отклонено: {rejected}\n"
        "👁️ Просмотрено: {viewed}\n"
        "⚠️ Ошибки: {errors}\n"
        "🔄 Итерации: {iterations}\n"
        "💬 Сообщения мерчантов: {merchant_messages}\n"
        "🏪 Мерчанты: {merchants}\n\n"
        "Ожидают интегратора:\n{pending_deals}"
    ),
    "sla_expired": "⏰ SLA истёк для сделки <code>{deal_id}</code> у мерчанта {merchant_name}! ⏳",
    "integrator_approve_error": "⚠️ Сначала отправьте КБ по сделке <code>{deal_id}</code>, затем попробуйте снова! 📋",
    "integrator_reject_sla": "⚖️ Интегратор попытался отменить <code>{deal_id}</code> после SLA! ⏳",
    "integrator_reject_notify": "⚠️ Сделка <code>{deal_id}</code> отклонена интегратором: {reason_text}",
    "partial_integrator_match": "⚠️ Частичное совпадение интегратора: {integrator_name} (API) ~ {cascade_name} (база).",
    "proofs_added": "🔍 Доказательства для <code>{deal_id}</code>",
    "kb_request": "⚡️ Запросили повторно перевод в успех по сделке <code>{deal_id}</code>",
    "integrator_kb_request": "⚠️ Коллеги, прошу направить несколько раз повторный callback по заявке: <code>{deal_id}</code>",
    "integrator_proof": "Доказательства от интегратора по сделке <code>{deal_id}</code>",
    "integrator_proof_accepted": "Доказательства по сделке <code>{deal_id}</code> приняты в обработку.",
    "integrator_proof_sent": "Прикреплены доказательства по сделке <code>{deal_id}</code>",
    "integrator_proof_rejected": "——————————\n🔴 Вы отклонили данное подтверждение"
}

KEYBOARDS: Dict[str, List[Tuple[str, str]]] = {
    "reject": [
        ("Фейк-чек", "reason_fake"), ("Неверные реквизиты", "reason_rec"),
        ("Запросите доп.", "reason_request_external_id"), ("Не было поступления", "reason_no_payment"),
        ("Другое", "reason_other"),
    ],
    "yes_no": [("✅ Да", "YES"), ("❌ Нет", "NO")],
    "action": [("✅ OK", "approve"), ("❌ Отклонить", "reject"), ("👁️ Просмотр", "view")],
    "integrator_proof": [("✅ Принять", "integrator_proof_approve"), ("❌ Отклонить", "integrator_proof_reject")],
    "integrator_approve": [("✅ Принять", "integrator_proof_approve"), ("❌ Отклонить", "integrator_proof_reject")]
}

ADMIN_COMMANDS: Dict[str, Dict[str, any]] = {
    "add_merchant": {
        "args": 1,
        "action": lambda db, args: db.add_merchant(args[0], args[0], handler_id=args[1] if len(args) > 1 else None),
        "success": "✅ Мерчант {0} добавлен 🏪"
    },
    "delete_merchant": {
        "args": 1,
        "action": lambda db, args: db.delete_merchant(args[0]),
        "success": "🗑️ Мерчант {0} удалён 🗑️"
    },
    "add_cascade": {
        "args": 1,
        "action": lambda db, args: db.merge_cascade(args[0], args[0], None, False),
        "success": "✅ Интегратор {0} добавлен 🤝"
    },
    "delete_cascade": {
        "args": 1,
        "action": lambda db, args: db.delete_cascade(args[0]),
        "success": "🗑️ Интегратор {0} удалён 🗂️"
    },
    "bind_merchant": {
        "args": 1,
        "action": lambda db, args: db.add_merchant(args[0], args[0], handler_id=args[1] if len(args) > 1 else None),
        "success": "🔗 Мерчант {0} привязан 🏪"
    },
    "candles": {
        "args": 2,
        "action": lambda db, args: db.merge_cascade(args[0], args[0], None, int(args[1].lower() in ['true', '1', 'вкл'])),
        "success": "✅ Интегратор {0} {1} ✨"
    },
    "add_user": {
        "args": 1,
        "action": lambda db, args: db.save_user_token(int(args[0]), "user_token"),
        "success": "✅ Пользователь {0} добавлен 👤"
    },
    "remove_user": {
        "args": 1,
        "action": lambda db, args: db.save_user_token(int(args[0]), None),
        "success": "🗑️ Пользователь {0} удалён 👤"
    }
}