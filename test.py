import asyncio
import logging
import mysql.connector
import sqlite3
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import html
import re 
from bs4 import BeautifulSoup
import json
from aiogram.types import InputMediaPhoto

# ========== НАСТРОЙКИ ==========
MYSQL_CONFIG = {
    'host': '155.212.223.45',
    'user': 'myuser',
    'password': '!qwert12345',
    'database': 'my_database',
    'charset': 'utf8mb4'
}
BOT_TOKEN = '8639319444:AAEU9aUaTq3rxuW6xf2nlfXCRiCN37qrD7c' #bot['bot_token']
#BOT_TOKEN = '6849348700:AAHpEKe3x4eTc_t19l7WTR_y-W1b_o0klmc'
ADMIN_ID = 5374683743#2109578014  

# ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ==========
nodes = {}               # {node_key: {'node_id': id, 'text': ..., 'image': ..., 'is_root': ...}}
buttons_by_node = {}     # {node_id: [{'text': ..., 'type': ..., 'target': ...}]}
reply_buttons = {}       # {normalized_text: {'response': ..., 'node_key': ..., 'message_key': ...}}
reply_button_texts = []  # список текстов для главной клавиатуры
delayed_messages = []    # список отложенных сообщений (с полем is_active)

# Системные кнопки (тексты)
CONTACT_BUTTON_TEXT = '✏️Связаться с менеджером'
MAILING_BUTTON_TEXT = '📨Рассылка'
CALC_BUTTON_TEXT = '💵Рассчитать стоимость'

# Состояния для FSM
class ContactState(StatesGroup):
    waiting_for_message = State()

class MailingStates(StatesGroup):
    waiting_for_content = State()
    waiting_for_schedule = State()

class CalcStates(StatesGroup):
    choosing_type = State()
    waiting_for_dimensions = State()

class ManagerReplyStates(StatesGroup):
    waiting_for_reply = State()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def normalize(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split()).strip().lower()

def clean_html_for_telegram(text, name=None):
    if not text:
        return text

    if name:
        text = text.replace('{name}', name)

    # Преобразуем __текст__ в <u>текст</u> (подчёркивание)
    text = re.sub(r'__(.*?)__', r'<u>\1</u>', text, flags=re.DOTALL)

    text = html.unescape(text)
    soup = BeautifulSoup(text, "html.parser")

    allowed_tags = {"b", "strong", "i", "em", "u", "a"}

    # <br> заменяем на перенос строки
    for br in soup.find_all("br"):
        br.replace_with("\n")

    for tag in soup.find_all(True):
        if tag.name not in allowed_tags and tag.name != "p":
            tag.unwrap()
        else:
            if tag.name == "a":
                href = tag.get("href")
                tag.attrs = {}
                if href:
                    tag["href"] = href
            else:
                tag.attrs = {}

    blocks = []

    for p in soup.find_all("p"):
        inner = "".join(str(x) for x in p.contents).strip()

        check_text = BeautifulSoup(inner, "html.parser").get_text().strip()
        if not check_text or check_text.replace("\xa0", "").strip() == "":
            continue

        blocks.append(inner)

    if blocks:
        cleaned = "\n\n".join(blocks)
    else:
        cleaned = str(soup)

    cleaned = cleaned.replace("\xa0", " ")
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)

    return cleaned.strip()
# ========== ЗАГРУЗКА ДАННЫХ ==========
def load_all_data():
    global nodes, buttons_by_node, reply_buttons, reply_button_texts, delayed_messages
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # 1. Узлы воронки
        cursor.execute("SELECT node_id, node_key, message_text, image_url, is_root FROM funnel_nodes WHERE is_active = 1")
        nodes.clear()
        for row in cursor.fetchall():
            nodes[row['node_key']] = {
                'node_id': row['node_id'],
                'text': row['message_text'],
                'image': row['image_url'],
                'is_root': row['is_root']
            }

        # 2. Кнопки узлов
        cursor.execute("SELECT node_id, button_text, button_type, target FROM funnel_buttons WHERE is_active = 1 ORDER BY menu_order")
        buttons_by_node.clear()
        for row in cursor.fetchall():
            node_id = row['node_id']
            if node_id not in buttons_by_node:
                buttons_by_node[node_id] = []
            buttons_by_node[node_id].append({
                'text': row['button_text'],
                'type': row['button_type'],
                'target': row['target']
            })

        # 3. Reply-кнопки
        cursor.execute("SELECT button_text, response_text, node_key, message_key FROM bot_buttons WHERE bot_id = 1 AND is_active = 1 ORDER BY menu_order")
        reply_buttons.clear()
        reply_button_texts.clear()
        for row in cursor.fetchall():
            text = row['button_text']
            norm = normalize(text)
            reply_buttons[norm] = {
                'response': row['response_text'],
                'node_key': row['node_key'],
                'message_key': row['message_key']
            }
            reply_button_texts.append(text)
        for sys_btn in [CONTACT_BUTTON_TEXT, MAILING_BUTTON_TEXT, CALC_BUTTON_TEXT]:
            if sys_btn not in reply_button_texts:
                reply_button_texts.append(sys_btn)
                norm = normalize(sys_btn)
                # Если таких кнопок нет в словаре reply_buttons, добавим пустую запись, чтобы не ломать поиск
                if norm not in reply_buttons:
                    reply_buttons[norm] = {
                        'response': '',
                        'node_key': None,
                        'message_key': None
                    }
        # 4. Отложенные сообщения (загружаем все, is_active учитываем при отправке)
        cursor.execute("""
            SELECT id, node_key, message_text, image_url, delay_hours, delay_unit, is_active
            FROM delayed_messages
            WHERE bot_id = 1
        """)
        delayed_messages.clear()
        for row in cursor.fetchall():
            delayed_messages.append({
                'id': row['id'],
                'node_key': row['node_key'],
                'text': row['message_text'],
                'image': row['image_url'],
                'delay_hours': row['delay_hours'],
                'delay_unit': row['delay_unit'],
                'is_active': row['is_active']
            })

        cursor.close()
        conn.close()
        logging.info(f"✅ Загружено узлов: {len(nodes)}, кнопок узлов: {sum(len(v) for v in buttons_by_node.values())}, reply-кнопок: {len(reply_buttons)}, отложенных: {len(delayed_messages)}")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки данных: {e}")

# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard(user_id=None):
    builder = ReplyKeyboardBuilder()
    for text in reply_button_texts:
        if text == MAILING_BUTTON_TEXT and user_id != ADMIN_ID:
            continue
        builder.add(types.KeyboardButton(text=text))
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def get_node_keyboard(node_key):
    node = nodes.get(node_key)
    if not node:
        return None
    node_id = node['node_id']
    builder = InlineKeyboardBuilder()
    if node_id in buttons_by_node:
        for btn in buttons_by_node[node_id]:
            if btn['type'] == 'url':
                builder.button(text=btn['text'], url=btn['target'])
            else:
                builder.button(text=btn['text'], callback_data=f"node:{btn['target']}")
    builder.adjust(1)
    return builder.as_markup() if builder.buttons else None

def get_calc_type_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🏕 Беседка", callback_data="calc_type_gazebo")
    builder.button(text="🛁 Баня", callback_data="calc_type_bath")
    builder.button(text="🏡 Летний дом", callback_data="calc_type_summer")
    builder.button(text="🏠 Дом для жизни", callback_data="calc_type_house")
    builder.adjust(1)
    return builder.as_markup()

def get_calc_new_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="👷 Написать прорабу", url="https://t.me/drev_house")
    builder.button(text="🔄 Новый расчет", callback_data="calc_new")
    builder.button(text="🏠 В меню", callback_data="calc_to_menu")
    builder.adjust(1)
    return builder.as_markup()

# ========== ОТПРАВКА УЗЛА (С РЕДАКТИРОВАНИЕМ) ==========
async def send_node(chat_id, node_key, bot, user_name=None, edit_message_id=None):
    if node_key not in nodes:
        await bot.send_message(chat_id, "Узел не найден")
        return
    node = nodes[node_key]
    text = clean_html_for_telegram(node['text'] or "Пустое сообщение", name=user_name)
    
    # Парсим изображения из JSON
    image_urls = []
    image_data = node.get('image')
    if image_data:
        try:
            parsed = json.loads(image_data)
            if isinstance(parsed, list):
                # Извлекаем все URL из списка объектов (ожидаем, что каждый элемент имеет поле 'url')
                image_urls = [item['url'] for item in parsed if item.get('url')]
            elif isinstance(parsed, str):
                image_urls = [parsed]
        except json.JSONDecodeError:
            # Если не JSON, считаем одиночной строкой
            image_urls = [image_data] if image_data else []
    
    keyboard = get_node_keyboard(node_key)

    if edit_message_id:
        # При редактировании нельзя сменить тип сообщения (фото на текст и т.д.)
        # Будем редактировать только текст/подпись, игнорируя фото
        if image_urls:
            # Если есть фото, предполагаем, что исходное сообщение было с фото, редактируем подпись
            await bot.edit_message_caption(
                chat_id=chat_id,
                message_id=edit_message_id,
                caption=text,
                parse_mode="HTML",
                reply_markup=keyboard
            )
        else:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=edit_message_id,
                text=text,
                parse_mode="HTML",
                reply_markup=keyboard
            )
    else:
        if len(image_urls) > 1:
            media_group = []
            for i, url in enumerate(image_urls):
                if i == 0:
                    media_group.append(InputMediaPhoto(media=url, caption=text, parse_mode="HTML"))
                else:
                    media_group.append(InputMediaPhoto(media=url))
            await bot.send_media_group(chat_id, media_group[:10])
            # После альбома отправляем клавиатуру отдельным сообщением (у медиагруппы нет reply_markup)
            if keyboard:
                await bot.send_message(chat_id, "Выберите действие:", reply_markup=keyboard)
        elif len(image_urls) == 1:
            await bot.send_photo(chat_id, image_urls[0], caption=text, parse_mode="HTML", reply_markup=keyboard)
        else:
            await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=keyboard)
# ========== SQLite ДЛЯ ПОЛЬЗОВАТЕЛЕЙ И СТАТУСОВ ==========
def init_db():
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT,
            join_date TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_delayed_status (
            user_id INTEGER,
            delayed_id INTEGER,
            sent_at TEXT,
            PRIMARY KEY (user_id, delayed_id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS preview_sent (
            message_id INTEGER PRIMARY KEY,
            sent_at TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logging.info("✅ SQLite инициализирована")

def add_user(user_id, first_name):
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO users (user_id, first_name, join_date) VALUES (?, ?, ?)',
                   (user_id, first_name, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def get_send_time(join_date, delay_hours, delay_unit):
    if delay_unit == 'minutes':
        return join_date + timedelta(minutes=delay_hours)
    elif delay_unit == 'days':
        return join_date + timedelta(days=delay_hours)
    else:  # hours
        return join_date + timedelta(hours=delay_hours)

def get_active_managers():
    # Возвращаем список менеджеров – пока только ADMIN_ID
    return [ADMIN_ID]

# ========== ФОНОВЫЕ ЗАДАЧИ ==========
async def check_delayed_messages(bot: Bot):
    while True:
        try:
            conn_sqlite = sqlite3.connect('users.db')
            cursor_sqlite = conn_sqlite.cursor()
            # Получаем также first_name для подстановки
            cursor_sqlite.execute("SELECT user_id, first_name, join_date FROM users")
            users = cursor_sqlite.fetchall()
            now = datetime.now()

            for user_id, user_name, join_str in users:
                try:
                    join_date = datetime.fromisoformat(join_str)
                except:
                    continue

                for msg in delayed_messages:
                    if msg['is_active'] != 0:  # отправляем только подтверждённые (is_active=0)
                        continue
                    send_time = get_send_time(join_date, msg['delay_hours'], msg['delay_unit'])
                    if now >= send_time:
                        cursor_sqlite.execute(
                            "SELECT 1 FROM user_delayed_status WHERE user_id = ? AND delayed_id = ?",
                            (user_id, msg['id'])
                        )
                        if cursor_sqlite.fetchone():
                            continue

                        # Отправка
                        try:
                            if msg['node_key']:
                                await send_node(user_id, msg['node_key'], bot, user_name=user_name)
                            else:
                                text = clean_html_for_telegram(msg['text'] or "", name=user_name)
                                
                                # Парсим JSON с изображениями
                                image_urls = []
                                image_data = msg.get('image')
                                if image_data:
                                    try:
                                        parsed = json.loads(image_data)
                                        if isinstance(parsed, list):
                                            # Извлекаем все URL из списка объектов
                                            image_urls = [item['url'] for item in parsed if item.get('url')]
                                        elif isinstance(parsed, str):
                                            # Если это просто строка (старый формат)
                                            image_urls = [parsed]
                                    except json.JSONDecodeError:
                                        # Не JSON – значит одиночная строка
                                        image_urls = [image_data] if image_data else []
                                
                                # Отправка в зависимости от количества фото
                                if len(image_urls) > 1:
                                    logging.info(f"Отправка альбома пользователю {user_id}, URL: {image_urls}")
                                    media_group = []
                                    for i, url in enumerate(image_urls):
                                        if i == 0:
                                            media_group.append(InputMediaPhoto(media=url, caption=text, parse_mode="HTML"))
                                        else:
                                            media_group.append(InputMediaPhoto(media=url))
                                    # Ограничение Telegram – до 10 фото
                                    await bot.send_media_group(user_id, media_group[:10])
                                elif len(image_urls) == 1:
                                    await bot.send_photo(user_id, image_urls[0], caption=text, parse_mode="HTML")
                                else:
                                    await bot.send_message(user_id, text, parse_mode="HTML")

                            cursor_sqlite.execute(
                                "INSERT INTO user_delayed_status (user_id, delayed_id, sent_at) VALUES (?, ?, ?)",
                                (user_id, msg['id'], now.isoformat())
                            )
                            conn_sqlite.commit()
                            logging.info(f"✅ Отложенное #{msg['id']} отправлено пользователю {user_id}")
                        except Exception as e:
                            logging.error(f"Ошибка отправки отложенного #{msg['id']} пользователю {user_id}: {e}")

            conn_sqlite.close()
        except Exception as e:
            logging.error(f"Ошибка в check_delayed_messages: {e}")
        await asyncio.sleep(30)

async def send_previews_to_admin(bot: Bot):
    while True:
        try:
            pending = [msg for msg in delayed_messages if msg['is_active'] == 1]
            if not pending:
                await asyncio.sleep(30)
                continue

            sqlite_conn = sqlite3.connect('users.db')
            sqlite_cur = sqlite_conn.cursor()

            for msg in pending:
                sqlite_cur.execute("SELECT 1 FROM preview_sent WHERE message_id = ?", (msg['id'],))
                if sqlite_cur.fetchone():
                    continue

                # Определяем текст для предпросмотра
                if msg['node_key'] and msg['node_key'] in nodes:
                    # Берём текст из узла
                    preview_text_content = clean_html_for_telegram(nodes[msg['node_key']]['text'] or "")
                else:
                    preview_text_content = clean_html_for_telegram(msg['text'] or "")

                preview_text = (
                    f"📨 Новое отложенное сообщение\n\n"
                    f" {preview_text_content[:200]}...\n"
                    
                )
                kb = InlineKeyboardBuilder()
                kb.button(text="✅ Подтвердить", callback_data=f"confirm:{msg['id']}")
                kb.button(text="❌ Отменить", callback_data=f"cancel:{msg['id']}")
                kb.adjust(2)

                try:
                    if msg['image']:
                        await bot.send_photo(ADMIN_ID, msg['image'], caption=preview_text,
                                             parse_mode="HTML", reply_markup=kb.as_markup())
                    else:
                        await bot.send_message(ADMIN_ID, preview_text,
                                               parse_mode="HTML", reply_markup=kb.as_markup())
                except Exception as e:
                    logging.error(f"Не удалось отправить предпросмотр: {e}")
                    continue

                sqlite_cur.execute(
                    "INSERT INTO preview_sent (message_id, sent_at) VALUES (?, ?)",
                    (msg['id'], datetime.now().isoformat())
                )
                sqlite_conn.commit()
                logging.info(f"✅ Предпросмотр для сообщения #{msg['id']} отправлен админу")

            sqlite_cur.close()
            sqlite_conn.close()
        except Exception as e:
            logging.error(f"Ошибка в send_previews_to_admin: {e}")
        await asyncio.sleep(30)

# ========== ОБРАБОТЧИКИ КОМАНД И СООБЩЕНИЙ ==========
dp = Dispatcher(storage=MemoryStorage())


# ---------- Старт ----------
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    add_user(message.from_user.id, message.from_user.first_name)

    root_key = next((k for k, v in nodes.items() if v.get('is_root')), None)
    if not root_key:
        await message.answer("❌ Корневой узел не настроен")
        return

    await send_node(message.chat.id, root_key, message.bot, user_name=message.from_user.first_name)
    await message.answer(
        "Для удобства смотрите меню 👇",
        reply_markup=get_main_keyboard(message.from_user.id)
    )

# ---------- Обработка reply-кнопок ----------

# ---------- Переход по узлам ----------
@dp.callback_query(F.data.startswith("node:"))
async def node_callback(callback: types.CallbackQuery):
    node_key = callback.data.split(":")[1]
    await send_node(callback.message.chat.id, node_key, callback.bot,
                user_name=callback.from_user.first_name,
                edit_message_id=callback.message.message_id)
    await callback.answer()

# ---------- Подтверждение/отмена отложенных ----------
@dp.callback_query(F.data.startswith("confirm:"))
async def confirm_message(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    msg_id = int(callback.data.split(":")[1])
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("UPDATE delayed_messages SET is_active = 0 WHERE id = %s", (msg_id,))
        conn.commit()
        cursor.close()
        conn.close()

        # Удаляем из preview_sent
        sqlite_conn = sqlite3.connect('users.db')
        sqlite_cur = sqlite_conn.cursor()
        sqlite_cur.execute("DELETE FROM preview_sent WHERE message_id = ?", (msg_id,))
        sqlite_conn.commit()
        sqlite_cur.close()
        sqlite_conn.close()

        load_all_data()  # обновляем кэш
        await callback.message.edit_text(f"✅ Сообщение #{msg_id} подтверждено и будет отправлено подписчикам.")
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка подтверждения: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("cancel:"))
async def cancel_message(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    msg_id = int(callback.data.split(":")[1])
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM delayed_messages WHERE id = %s", (msg_id,))
        conn.commit()
        cursor.close()
        conn.close()

        sqlite_conn = sqlite3.connect('users.db')
        sqlite_cur = sqlite_conn.cursor()
        sqlite_cur.execute("DELETE FROM preview_sent WHERE message_id = ?", (msg_id,))
        sqlite_conn.commit()
        sqlite_cur.close()
        sqlite_conn.close()

        load_all_data()
        await callback.message.edit_text(f"❌ Сообщение #{msg_id} отменено и удалено.")
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка отмены: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)

# ---------- Калькулятор ----------
@dp.callback_query(F.data.startswith("calc_type_"))
async def calc_type_chosen(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(CalcStates.waiting_for_dimensions)
    type_map = {
        "calc_type_gazebo": "Беседка",
        "calc_type_bath": "Баня",
        "calc_type_summer": "Летний дом",
        "calc_type_house": "Дом для жизни"
    }
    chosen = type_map.get(callback.data, "Неизвестно")
    await state.update_data(building_type=callback.data)

    cancel_kb = InlineKeyboardBuilder()
    cancel_kb.button(text="❌ Отмена", callback_data="calc_cancel_input")
    await callback.message.edit_text(
        f"Выбрано: {chosen}\n\nВведите размеры: два числа (ширина длина) или одно (площадь).",
        reply_markup=cancel_kb.as_markup()
    )
@dp.callback_query(F.data == "calc_cancel_input", CalcStates.waiting_for_dimensions)
async def calc_cancel_input(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.edit_text("❌ Расчёт отменён.")
    await callback.message.answer("Главное меню:", reply_markup=get_main_keyboard(callback.from_user.id))

@dp.message(CalcStates.waiting_for_dimensions)
async def calc_dimensions_received(message: types.Message, state: FSMContext):
    text = message.text.strip()
    parts = text.split()
    area = None
    try:
        if len(parts) == 2:
            w, h = map(float, parts)
            area = w * h
        elif len(parts) == 1:
            area = float(parts[0])
        else:
            raise ValueError
    except ValueError:
        await message.answer("❌ Неверный формат. Введите два числа через пробел или одно число.")
        return

    if area <= 0:
        await message.answer("❌ Площадь должна быть положительной.")
        return

    data = await state.get_data()
    btype = data.get('building_type')

    # Базовая информация о типе и диаметре
    if btype == "calc_type_gazebo":
        price_per_m2 = 750
        diameter = "16-20"
        type_name = "Беседка"
        details = (
            "✅ Цена указана <b>только за комплект</b> (стеновой комплект + стропильная система).\n"
            "🚧 Сборка оплачивается отдельно."
        )
    elif btype == "calc_type_bath":
        # Цена зависит от площади
        price_per_m2 = 600  # базовая, но по тексту от 600, но у вас было 950/800
        # Уточним у клиента логику: вероятно, он хочет фиксированную цену 600?
        # Пока оставим как было, но можно сделать фикс. Для примера сделаем фикс 600.
        price_per_m2 = 600  # или оставить старую логику
        diameter = "22-24"
        type_name = "Баня"
        details = (
            "✅ Цена указана <b>только за комплект</b> (стеновой комплект + стропильная система).\n"
            "🚧 Сборка оплачивается отдельно."
        )
    elif btype == "calc_type_summer":
        price_per_m2 = 980
        diameter = "20-22"
        type_name = "Летний дом"
        details = (
            "✅ Цена указана <b>только за комплект</b> (стеновой комплект + стропильная система).\n"
            "🚧 Сборка оплачивается отдельно."
        )
    elif btype == "calc_type_house":
        price_per_m2 = 1150 if area < 50 else 980
        diameter = "28"
        type_name = "Дом ПП"
        details = (
            "✅ Цена указана <b>только за комплект</b> (стеновой комплект + стропильная система).\n"
            "🚧 Сборка оплачивается отдельно."
        )
    else:
        await message.answer("Ошибка типа. Начните заново.")
        await state.clear()
        return

    total = area * price_per_m2
    result_text = (
        f"✅ <b>Результат расчёта</b>\n\n"
        f"<b>{type_name}</b>\n"
        f"Диаметр бревна: {diameter}\n"
        f"Площадь: {area:.2f} м²\n"
        f"Цена за м²: <b>от {price_per_m2} руб</b>\n"
        f"<b>Итого: от {total:,.2f} руб.</b>\n\n"
        f"{details}\n\n"
        f"<i>Точная стоимость зависит от конфигурации и этажности.</i>"
    ).replace(',', ' ')

    await message.answer(result_text, parse_mode="HTML", reply_markup=get_calc_new_keyboard())
    await state.clear()

@dp.callback_query(F.data == "calc_new")
async def calc_new(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.delete()
    await callback.message.answer("Выберите тип постройки:", reply_markup=get_calc_type_keyboard())
    # состояние не устанавливаем – пользователь снова выбирает тип

@dp.callback_query(F.data == "calc_to_menu")
async def calc_to_menu(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.delete()
    await callback.message.answer("Главное меню:", reply_markup=get_main_keyboard(callback.from_user.id))
    await state.clear()

# ---------- Связь с менеджером ----------
@dp.message(ContactState.waiting_for_message)
async def process_client_message(message: types.Message, state: FSMContext, bot: Bot):
    if not message.text:
        await message.answer("Пока поддерживается только текст.")
        return

    user = message.from_user
    user_id = user.id
    username = f"@{user.username}" if user.username else "нет"
    user_link = f"<a href='tg://user?id={user_id}'>{user.full_name or user.first_name}</a>"

    await message.answer("✅ Отправлено менеджеру. Ожидайте.", reply_markup=get_main_keyboard(user_id))
    await state.clear()

    notification = (
        f"📩 <b>Новое сообщение!</b>\n\n"
        f"👤 Имя: {user_link}\n"
        f"🔗 Username: {username}\n"
        f"🆔 ID: <code>{user_id}</code>\n\n"
        f"💬 Сообщение:\n{message.text}"
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="💬 Ответить", callback_data=f"reply_to_{user_id}")
    markup = builder.as_markup()

    for manager_id in get_active_managers():
        try:
            await bot.send_message(manager_id, notification, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)
        except Exception as e:
            logging.warning(f"Не удалось отправить менеджеру {manager_id}: {e}")

@dp.callback_query(F.data.startswith("reply_to_"))
async def manager_reply_init(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_active_managers():
        await callback.answer("⛔ Доступ запрещён")
        return
    user_id = int(callback.data.split("_")[2])
    await state.update_data(reply_to_user_id=user_id)
    await state.set_state(ManagerReplyStates.waiting_for_reply)
    await callback.message.answer("✏️ Напишите текст ответа клиенту:")
    await callback.answer()

@dp.message(ManagerReplyStates.waiting_for_reply)
async def manager_send_reply(message: types.Message, state: FSMContext, bot: Bot):
    if message.from_user.id not in get_active_managers():
        await message.answer("⛔ Доступ запрещён")
        await state.clear()
        return
    data = await state.get_data()
    target_user_id = data.get('reply_to_user_id')
    if not target_user_id:
        await message.answer("Ошибка: ID пользователя утерян.")
        await state.clear()
        return
    try:
        await bot.send_message(target_user_id, f"👤 <b>Ответ от менеджера:</b>\n\n{message.text}", parse_mode="HTML")
        await message.answer("✅ Ответ отправлен клиенту.")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить: {e}")
    finally:
        await state.clear()

# ---------- Рассылка (для админа) ----------
async def process_mailing(bot: Bot, mailing_id: int, msg_data: dict):
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()
    conn.close()

    success_count = 0
    fail_count = 0

    for (user_id,) in users:
        try:
            content_type = msg_data['content_type']
            if content_type == 'text':
                await bot.send_message(user_id, msg_data['text'], parse_mode="HTML")
            elif content_type == 'photo':
                await bot.send_photo(user_id, msg_data['file_id'], caption=msg_data.get('caption', ''), parse_mode="HTML")
            elif content_type == 'video':
                await bot.send_video(user_id, msg_data['file_id'], caption=msg_data.get('caption', ''), parse_mode="HTML")
            elif content_type == 'document':
                await bot.send_document(user_id, msg_data['file_id'], caption=msg_data.get('caption', ''), parse_mode="HTML")
            elif content_type == 'audio':
                await bot.send_audio(user_id, msg_data['file_id'], caption=msg_data.get('caption', ''), parse_mode="HTML")
            elif content_type == 'voice':
                await bot.send_voice(user_id, msg_data['file_id'], caption=msg_data.get('caption', ''), parse_mode="HTML")
            elif content_type == 'video_note':
                await bot.send_video_note(user_id, msg_data['file_id'])
            else:
                logging.warning(f"Неизвестный тип {content_type}")
                continue
            success_count += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logging.warning(f"Ошибка отправки {user_id}: {e}")
            fail_count += 1

    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("UPDATE bot_mailings SET status = 'sent', sent_at = NOW() WHERE id = %s", (mailing_id,))
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Рассылка #{mailing_id} завершена: {success_count} ок, {fail_count} ошибок")
    except Exception as e:
        logging.error(f"Ошибка обновления статуса рассылки: {e}")

@dp.message(Command("mailing"), F.from_user.id == ADMIN_ID)
async def cmd_mailing(message: types.Message, state: FSMContext):
    await message.answer("✏️ Пришлите сообщение для рассылки.")
    await state.set_state(MailingStates.waiting_for_content)

@dp.message(MailingStates.waiting_for_content, F.from_user.id == ADMIN_ID)
async def mailing_content_received(message: types.Message, state: FSMContext):
    data = {}
    if message.text:
        data['content_type'] = 'text'
        data['text'] = message.text
    elif message.photo:
        data['content_type'] = 'photo'
        data['file_id'] = message.photo[-1].file_id
        data['caption'] = message.caption or ''
    elif message.video:
        data['content_type'] = 'video'
        data['file_id'] = message.video.file_id
        data['caption'] = message.caption or ''
    elif message.document:
        data['content_type'] = 'document'
        data['file_id'] = message.document.file_id
        data['caption'] = message.caption or ''
    elif message.audio:
        data['content_type'] = 'audio'
        data['file_id'] = message.audio.file_id
        data['caption'] = message.caption or ''
    elif message.voice:
        data['content_type'] = 'voice'
        data['file_id'] = message.voice.file_id
        data['caption'] = message.caption or ''
    elif message.video_note:
        data['content_type'] = 'video_note'
        data['file_id'] = message.video_note.file_id
        data['caption'] = ''
    else:
        await message.answer("❌ Неподдерживаемый тип. Отправьте текст, фото, видео, аудио, документ или кружок.")
        return
    await state.update_data(**data)
    builder = InlineKeyboardBuilder()
    builder.button(text="🚀 Отправить сейчас", callback_data="mailing_now")
    builder.button(text="⏳ Запланировать", callback_data="mailing_schedule")
    builder.button(text="❌ Отмена", callback_data="mailing_cancel")
    builder.adjust(1)
    await message.answer("Выберите действие:", reply_markup=builder.as_markup())

@dp.callback_query(F.data == "mailing_now", F.from_user.id == ADMIN_ID)
async def mailing_now_callback(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    content_type = data.get('content_type')
    if not content_type:
        await callback.message.edit_text("Ошибка: данные не найдены. Начните заново.")
        await state.clear()
        return
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        if content_type == 'text':
            sql = "INSERT INTO bot_mailings (message_text, content_type, send_at, status) VALUES (%s, %s, NOW(), 'processing')"
            params = (data['text'], content_type)
        else:
            sql = "INSERT INTO bot_mailings (message_text, content_type, file_id, caption, send_at, status) VALUES (%s, %s, %s, %s, NOW(), 'processing')"
            params = ('', content_type, data['file_id'], data.get('caption', ''))
        cursor.execute(sql, params)
        conn.commit()
        mailing_id = cursor.lastrowid
        cursor.close()
        conn.close()
        asyncio.create_task(process_mailing(callback.bot, mailing_id, data))
        await callback.message.edit_text("✅ Рассылка запущена.")
    except Exception as e:
        logging.error(f"Ошибка рассылки: {e}")
        await callback.message.edit_text("❌ Ошибка запуска рассылки.")
    finally:
        await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "mailing_schedule", F.from_user.id == ADMIN_ID)
async def mailing_schedule_callback(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("📅 Введите дату и время `ДД.ММ.ГГГГ ЧЧ:ММ` (пример: 25.12.2025 15:30)")
    await state.set_state(MailingStates.waiting_for_schedule)
    await callback.answer()

@dp.message(MailingStates.waiting_for_schedule, F.from_user.id == ADMIN_ID)
async def mailing_schedule_time(message: types.Message, state: FSMContext):
    try:
        send_at = datetime.strptime(message.text.strip(), "%d.%m.%Y %H:%M")
    except ValueError:
        await message.answer("❌ Неверный формат. Повторите.")
        return
    if send_at <= datetime.now():
        await message.answer("❌ Время должно быть в будущем.")
        return
    data = await state.get_data()
    content_type = data.get('content_type')
    if not content_type:
        await message.answer("Ошибка: данные не найдены.")
        await state.clear()
        return
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        if content_type == 'text':
            sql = "INSERT INTO bot_mailings (message_text, content_type, send_at, status) VALUES (%s, %s, %s, 'pending')"
            params = (data['text'], content_type, send_at)
        else:
            sql = "INSERT INTO bot_mailings (message_text, content_type, file_id, caption, send_at, status) VALUES (%s, %s, %s, %s, %s, 'pending')"
            params = ('', content_type, data['file_id'], data.get('caption', ''), send_at)
        cursor.execute(sql, params)
        conn.commit()
        cursor.close()
        conn.close()
        await message.answer(f"✅ Запланировано на {send_at.strftime('%d.%m.%Y %H:%M')}.")
    except Exception as e:
        logging.error(f"Ошибка планирования: {e}")
        await message.answer("❌ Не удалось запланировать.")
    finally:
        await state.clear()

@dp.callback_query(F.data == "mailing_cancel", F.from_user.id == ADMIN_ID)
async def mailing_cancel_callback(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Отменено.")
    await state.clear()
    await callback.answer()

# ---------- Фоновое обновление данных ----------
async def refresh_loop():
    while True:
        load_all_data()
        await asyncio.sleep(30)

@dp.message(F.text)
async def handle_reply_buttons(message: types.Message, state: FSMContext):
    # Если есть активное состояние – не обрабатываем (пусть обрабатывают хендлеры состояний)
    current = await state.get_state()
    if current is not None:
        return

    norm = normalize(message.text)
    uid = message.from_user.id

    # Системные кнопки
    if message.text == CONTACT_BUTTON_TEXT:
        await state.set_state(ContactState.waiting_for_message)
        await message.answer("Напишите сообщение менеджеру:", reply_markup=get_main_keyboard(uid))
        return

    if message.text == MAILING_BUTTON_TEXT and uid == ADMIN_ID:
        await state.set_state(MailingStates.waiting_for_content)
        await message.answer("✏️ Пришлите сообщение для рассылки.")
        return

    if message.text == CALC_BUTTON_TEXT:
        await message.answer("Выберите тип постройки:", reply_markup=get_calc_type_keyboard())
        return

    # Обычные reply-кнопки
    if norm not in reply_buttons:
        await message.answer("Неизвестная команда. Используйте кнопки ниже.", reply_markup=get_main_keyboard(uid))
        return

    btn = reply_buttons[norm]
    if btn['node_key']:
        await send_node(message.chat.id, btn['node_key'], message.bot, user_name=message.from_user.first_name)
    elif btn['message_key']:
        response = btn['response'] or "Раздел в разработке."
        await message.answer(
            clean_html_for_telegram(response, name=message.from_user.first_name),
            parse_mode="HTML",
            reply_markup=get_main_keyboard(uid)
        )
    else:
        response = btn['response'] or "Пустой ответ"
        await message.answer(
            clean_html_for_telegram(response, name=message.from_user.first_name),
            parse_mode="HTML",
            reply_markup=get_main_keyboard(uid)
        )# ---------- Запуск ----------
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    init_db()
    load_all_data()
    bot = Bot(token=BOT_TOKEN)

    asyncio.create_task(refresh_loop())
    asyncio.create_task(check_delayed_messages(bot))
    asyncio.create_task(send_previews_to_admin(bot))

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())


