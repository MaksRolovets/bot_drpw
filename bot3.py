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
#BOT_TOKEN = '6609879243:AAGRnV1wY7bdW-05uD4eWUwWr20TW1tck5c'
#BOT_TOKEN = '8639319444:AAEU9aUaTq3rxuW6xf2nlfXCRiCN37qrD7c' #bot['bot_token']
BOT_TOKEN = '8344442339:AAG_heJ9emoN5NAItfWXGhHDpiF-mEwzVQk'
ADMIN_ID = 5374683743#2109578014#


# ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ==========
BASE_IMAGE_URL = "https://horrifyingly-enchanted-bandicoot.cloudpub.ru/"
nodes = {}               # {node_key: {'node_id': id, 'text': ..., 'image': ..., 'is_root': ...}}
buttons_by_node = {}     # {node_id: [{'text': ..., 'type': ..., 'target': ...}]}
reply_buttons = {}       # {normalized_text: {'response': ..., 'node_key': ..., 'message_key': ...}}
reply_button_texts = []  # список текстов для главной клавиатуры
delayed_messages = []    # список отложенных сообщений (с полем is_active)
START_NODE_KEYS = set()  # множество ключей узлов, которые должны открываться новым сообщением
# Системные кнопки (тексты)
CONTACT_BUTTON_TEXT = '✏️Связаться с менеджером'
MAILING_BUTTON_TEXT = '📨Рассылка'
CALC_BUTTON_TEXT = '💵Рассчитать стоимость'
STATS_BUTTON_TEXT = '📊 Статистика'
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

import hashlib

def get_absolute_image_url(relative_url):
    """Преобразует относительный путь в полный URL с BASE_IMAGE_URL."""
    if not relative_url:
        return None
    if relative_url.startswith(('http://', 'https://')):
        return relative_url
    # Убираем префикс 'image/' если он есть
    if relative_url.startswith('image/'):
        relative_url = relative_url[6:]  # удаляем первые 6 символов
    # Убираем ведущий слеш, если есть
    relative_url = relative_url.lstrip('/')
    return BASE_IMAGE_URL + relative_url

def get_message_hash(msg):
    """Возвращает MD5-хеш содержимого отложенного сообщения."""
    # Учитываем node_key (если есть), текст, задержку, единицу, изображение
    if msg.get('node_key'):
        node = nodes.get(msg['node_key'], {})
        node_text = node.get('text', '')
        content = f"{msg['node_key']}_{node_text}_{msg['delay_hours']}_{msg.get('delay_unit', 'hours')}_{msg.get('image', '')}"
    else:
        content = f"{msg.get('text', '')}_{msg['delay_hours']}_{msg.get('delay_unit', 'hours')}_{msg.get('image', '')}"
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def normalize(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split()).strip().lower()

def clean_html_for_telegram(text, name=None):
    if not text:
        return text

    if name:
        text = text.replace('{name}', name)

    # Декодируем HTML-сущности
    text = html.unescape(text)
    
    # Функция для обработки подчёркиваний с HTML внутри
    def replace_underscores(match):
        content = match.group(1).strip()
        if content:
            return f'<u>{content}</u>'
        return ''
    
    # Сначала заменяем все __...__ на <u>...</u>
    parts = []
    last_end = 0
    pattern = re.compile(r'__(.*?)__', re.DOTALL)
    
    for match in pattern.finditer(text):
        start, end = match.span()
        parts.append(text[last_end:start])
        content = match.group(1)
        if content.strip():
            parts.append(f'<u>{content}</u>')
        last_end = end
    parts.append(text[last_end:])
    text = ''.join(parts)
    
    # Парсим HTML
    soup = BeautifulSoup(text, "html.parser")

    allowed_tags = {"b", "strong", "i", "em", "u", "a"}

    # Заменяем <br> на перенос строки
    for br in soup.find_all("br"):
        br.replace_with("\n")

    # Удаляем неразрешённые теги, сохраняя содержимое
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

    # Убираем пустые теги <u>
    for u_tag in soup.find_all("u"):
        if not u_tag.get_text(strip=True):
            u_tag.decompose()

    # Нормализуем пробелы: заменяем множественные пробелы на один
    # (но не трогаем переносы строк)
    for text_node in soup.find_all(string=True):
        if text_node.parent.name not in ['script', 'style']:
            normalized = re.sub(r'[ \t]+', ' ', text_node)
            if normalized != text_node:
                text_node.replace_with(normalized)

    # Собираем содержимое параграфов
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

    # Окончательная чистка
    cleaned = cleaned.replace("\xa0", " ")
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    # Убираем лишние пробелы перед знаками препинания (опционально)
    cleaned = re.sub(r'\s+([.,!?;:])', r'\1', cleaned)
    # Убираем пробелы в начале и конце строк
    cleaned = '\n'.join(line.strip() for line in cleaned.split('\n'))
    return cleaned.strip()
# ========== ЗАГРУЗКА ДАННЫХ ==========
def load_all_data():
    global nodes, buttons_by_node, reply_buttons, reply_button_texts, delayed_messages
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # 1. Узлы воронки
        cursor.execute("SELECT node_id, node_key, message_text, image_url, is_root FROM funnel_nodes WHERE is_active = 1 AND bot_id = 4")
        nodes.clear()
        for row in cursor.fetchall():
            nodes[row['node_key']] = {
                'node_id': row['node_id'],
                'text': row['message_text'],
                'image': row['image_url'],
                'is_root': row['is_root']
            }

        # 2. Кнопки узлов (теперь загружаем раньше)
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
        cursor.execute("SELECT button_text, response_text, node_key, message_key FROM bot_buttons WHERE bot_id = 4 AND is_active = 1 ORDER BY menu_order")
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

        # Добавляем системные кнопки (контакт, рассылка, калькулятор, статистика)
        for sys_btn in [CONTACT_BUTTON_TEXT, MAILING_BUTTON_TEXT,  STATS_BUTTON_TEXT]:
            if sys_btn not in reply_button_texts:
                reply_button_texts.append(sys_btn)
                norm = normalize(sys_btn)
                if norm not in reply_buttons:
                    reply_buttons[norm] = {
                        'response': '',
                        'node_key': None,
                        'message_key': None
                    }

        # 4. Отложенные сообщения
        cursor.execute("""
            SELECT id, node_key, message_text, image_url, delay_hours, delay_unit, is_active, family_id
            FROM delayed_messages
            WHERE bot_id = 4
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
                'is_active': row['is_active'],
                'family_id': row['family_id']
            })

        cursor.close()
        conn.close()

        # Формируем START_NODE_KEYS (после загрузки узлов и кнопок)
        global START_NODE_KEYS
        START_NODE_KEYS = set()
        start_node_id = None
        for key, node in nodes.items():
            if key == 'start':
                start_node_id = node['node_id']
                break
        if start_node_id:
            START_NODE_KEYS.add('start')
            for btn in buttons_by_node.get(start_node_id, []):
                if btn['type'] != 'url':
                    START_NODE_KEYS.add(btn['target'])

        logging.info(f"✅ Загружено узлов: {len(nodes)}, кнопок узлов: {sum(len(v) for v in buttons_by_node.values())}, reply-кнопок: {len(reply_buttons)}, отложенных: {len(delayed_messages)}")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки данных: {e}")
# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard(user_id=None):
    builder = ReplyKeyboardBuilder()
    for text in reply_button_texts:
        if text == MAILING_BUTTON_TEXT and user_id != ADMIN_ID:
            continue
        if text == STATS_BUTTON_TEXT and user_id != ADMIN_ID:
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
    original_text = node['text'] or "Пустое сообщение"
    logging.info(f"📥 [{node_key}] ДО clean_html: {original_text}")
    text = clean_html_for_telegram(node['text'] or "Пустое сообщение", name=user_name)
    logging.info(f"📤 [{node_key}] ПОСЛЕ clean_html: {text}")

    # Парсим изображения из JSON
    image_urls = []
    image_data = node.get('image')
    if image_data:
        try:
            parsed = json.loads(image_data)
            if isinstance(parsed, list):
                # Применяем get_absolute_image_url к каждому URL
                image_urls = [get_absolute_image_url(item['url']) for item in parsed if item.get('url')]
            elif isinstance(parsed, str):
                image_urls = [get_absolute_image_url(parsed)]
        except json.JSONDecodeError:
            # Если не JSON, считаем одиночной строкой
            image_urls = [get_absolute_image_url(image_data)] if image_data else []
    
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
            await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=keyboard)# ========== SQLite ДЛЯ ПОЛЬЗОВАТЕЛЕЙ И СТАТУСОВ ==========

def init_db():
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT,
            username TEXT,
            join_date TEXT
        )
    ''')
    # Проверяем, есть ли колонка username (на случай, если таблица уже существовала)
    cursor.execute("PRAGMA table_info(users)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'username' not in columns:
        cursor.execute("ALTER TABLE users ADD COLUMN username TEXT")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_delayed_status (
            user_id INTEGER,
            family_id TEXT,
            sent_at TEXT,
            PRIMARY KEY (user_id, family_id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS preview_sent (
            message_id INTEGER PRIMARY KEY,
            sent_at TEXT
        )
    ''')
    # НОВАЯ ТАБЛИЦА ДЛЯ ХРАНЕНИЯ ВРЕМЕНИ ПОСЛЕДНЕЙ ОТПРАВКИ
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_last_sent (
            user_id INTEGER PRIMARY KEY,
            last_sent_at TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logging.info("✅ SQLite инициализирована")

def add_user(user_id, first_name, username=None):
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO users (user_id, first_name, username, join_date) VALUES (?, ?, ?, ?)',
                   (user_id, first_name, username, datetime.now().isoformat()))
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

async def check_delayed_messages(bot: Bot):
    MAX_OVERDUE = timedelta(hours=1)   # порог просроченности
    INTERVAL = timedelta(days=2)        # интервал между просроченными

    while True:
        try:
            conn_sqlite = sqlite3.connect('users.db')
            cursor_sqlite = conn_sqlite.cursor()
            cursor_sqlite.execute("SELECT user_id, first_name, join_date FROM users")
            users = cursor_sqlite.fetchall()
            now = datetime.now()

            for user_id, user_name, join_str in users:
                try:
                    join_date = datetime.fromisoformat(join_str)
                except:
                    continue

                # Собираем все готовые сообщения для этого пользователя
                fresh_msgs = []    # не просрочены (придут сразу)
                overdue_msgs = []  # просрочены (будут с интервалом)

                for msg in delayed_messages:
                    if msg['is_active'] != 0:
                        continue
                    send_time = get_send_time(join_date, msg['delay_hours'], msg['delay_unit'])
                    if now < send_time:
                        continue  # ещё не время

                    # Определяем status_id
                    if msg.get('family_id'):
                        status_id = msg['family_id']
                    else:
                        status_id = get_message_hash(msg)

                    # Проверяем, не отправляли ли уже это семейство
                    cursor_sqlite.execute(
                        "SELECT 1 FROM user_delayed_status WHERE user_id = ? AND family_id = ?",
                        (user_id, status_id)
                    )
                    if cursor_sqlite.fetchone():
                        continue

                    # Разделяем на свежие и просроченные
                    if now <= send_time + MAX_OVERDUE:
                        fresh_msgs.append((send_time, msg, status_id))
                    else:
                        overdue_msgs.append((send_time, msg, status_id))

                # Сначала отправляем свежие сообщения (без ограничений)
                fresh_msgs.sort(key=lambda x: x[0])  # по времени отправки
                for send_time, msg, status_id in fresh_msgs:
                    try:
                        if msg['node_key']:
                            await send_node(user_id, msg['node_key'], bot, user_name=user_name)
                        else:
                            text = clean_html_for_telegram(msg['text'] or "", name=user_name)
                            image_urls = []
                            image_data = msg.get('image')
                            if image_data:
                                try:
                                    parsed = json.loads(image_data)
                                    if isinstance(parsed, list):
                                        image_urls = [get_absolute_image_url(item['url']) for item in parsed if item.get('url')]
                                    elif isinstance(parsed, str):
                                        image_urls = [get_absolute_image_url(parsed)]
                                except json.JSONDecodeError:
                                    image_urls = [get_absolute_image_url(image_data)] if image_data else []
                            
                            if len(image_urls) > 1:
                                media_group = []
                                for i, url in enumerate(image_urls):
                                    if i == 0:
                                        media_group.append(InputMediaPhoto(media=url, caption=text, parse_mode="HTML"))
                                    else:
                                        media_group.append(InputMediaPhoto(media=url))
                                await bot.send_media_group(user_id, media_group[:10])
                            elif len(image_urls) == 1:
                                await bot.send_photo(user_id, image_urls[0], caption=text, parse_mode="HTML")
                            else:
                                await bot.send_message(user_id, text, parse_mode="HTML")

                        # Помечаем семейство как отправленное
                        cursor_sqlite.execute(
                            "INSERT OR IGNORE INTO user_delayed_status (user_id, family_id, sent_at) VALUES (?, ?, ?)",
                            (user_id, status_id, now.isoformat())
                        )
                        # Обновляем время последней отправки (даже для свежих, чтобы интервал для просроченных считался)
                        cursor_sqlite.execute(
                            "REPLACE INTO user_last_sent (user_id, last_sent_at) VALUES (?, ?)",
                            (user_id, now.isoformat())
                        )
                        conn_sqlite.commit()
                        logging.info(f"✅ Свежее #{msg['id']} отправлено пользователю {user_id} (семейство {status_id})")
                    except Exception as e:
                        logging.error(f"Ошибка отправки свежего #{msg['id']}: {e}", exc_info=True)
                        if "bot was blocked" in str(e).lower() or "chat not found" in str(e).lower():
                            cursor_sqlite.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
                            conn_sqlite.commit()
                        else:
                            cursor_sqlite.execute(
                                "INSERT OR IGNORE INTO user_delayed_status (user_id, family_id, sent_at) VALUES (?, ?, ?)",
                                (user_id, status_id, now.isoformat())
                            )
                            conn_sqlite.commit()

                # Теперь обрабатываем просроченные сообщения с интервалом 2 дня
                if overdue_msgs:
                    # Получаем время последней отправки (учитываем, что могли отправить свежие)
                    cursor_sqlite.execute("SELECT last_sent_at FROM user_last_sent WHERE user_id = ?", (user_id,))
                    row = cursor_sqlite.fetchone()
                    last_sent = datetime.fromisoformat(row[0]) if row and row[0] else None

                    # Если прошло достаточно времени – отправляем одно самое старое просроченное
                    if last_sent is None or (now - last_sent) >= INTERVAL:
                        overdue_msgs.sort(key=lambda x: x[0])  # самое старое первым
                        send_time, msg, status_id = overdue_msgs[0]
                        try:
                            if msg['node_key']:
                                await send_node(user_id, msg['node_key'], bot, user_name=user_name)
                            else:
                                text = clean_html_for_telegram(msg['text'] or "", name=user_name)
                                image_urls = []
                                image_data = msg.get('image')
                                if image_data:
                                    try:
                                        parsed = json.loads(image_data)
                                        if isinstance(parsed, list):
                                            image_urls = [get_absolute_image_url(item['url']) for item in parsed if item.get('url')]
                                        elif isinstance(parsed, str):
                                            image_urls = [get_absolute_image_url(parsed)]
                                    except json.JSONDecodeError:
                                        image_urls = [get_absolute_image_url(image_data)] if image_data else []
                                
                                if len(image_urls) > 1:
                                    media_group = []
                                    for i, url in enumerate(image_urls):
                                        if i == 0:
                                            media_group.append(InputMediaPhoto(media=url, caption=text, parse_mode="HTML"))
                                        else:
                                            media_group.append(InputMediaPhoto(media=url))
                                    await bot.send_media_group(user_id, media_group[:10])
                                elif len(image_urls) == 1:
                                    await bot.send_photo(user_id, image_urls[0], caption=text, parse_mode="HTML")
                                else:
                                    await bot.send_message(user_id, text, parse_mode="HTML")

                            cursor_sqlite.execute(
                                "INSERT OR IGNORE INTO user_delayed_status (user_id, family_id, sent_at) VALUES (?, ?, ?)",
                                (user_id, status_id, now.isoformat())
                            )
                            cursor_sqlite.execute(
                                "REPLACE INTO user_last_sent (user_id, last_sent_at) VALUES (?, ?)",
                                (user_id, now.isoformat())
                            )
                            conn_sqlite.commit()
                            logging.info(f"✅ Просроченное #{msg['id']} отправлено пользователю {user_id} (семейство {status_id})")
                        except Exception as e:
                            logging.error(f"Ошибка отправки просроченного #{msg['id']}: {e}", exc_info=True)
                            if "bot was blocked" in str(e).lower() or "chat not found" in str(e).lower():
                                cursor_sqlite.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
                                conn_sqlite.commit()
                            else:
                                cursor_sqlite.execute(
                                    "INSERT OR IGNORE INTO user_delayed_status (user_id, family_id, sent_at) VALUES (?, ?, ?)",
                                    (user_id, status_id, now.isoformat())
                                )
                                conn_sqlite.commit()
                    else:
                        # Не прошло 2 дня – ничего не отправляем
                        logging.debug(f"⏳ Для пользователя {user_id} ещё не прошло 2 дня, просроченные ждут")

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

                # Если сообщение ссылается на узел – отправляем узел целиком
                if msg['node_key'] and msg['node_key'] in nodes:
                    # Отправляем узел админу (со всеми inline‑кнопками и изображениями)
                    await send_node(ADMIN_ID, msg['node_key'], bot,user_name="Drev.house👉 Строительство 🪓" )
                    # Отправляем кнопки подтверждения/отмены отдельным сообщением
                    kb = InlineKeyboardBuilder()
                    kb.button(text="✅ Подтвердить", callback_data=f"confirm:{msg['id']}")
                    kb.button(text="❌ Отменить", callback_data=f"cancel:{msg['id']}")
                    kb.adjust(2)
                    await bot.send_message(ADMIN_ID, "Выберите действие для этого сообщения:", reply_markup=kb.as_markup())
                else:
                    # Обычное отложенное сообщение – отправляем текст и фото
                    preview_text_content = clean_html_for_telegram(msg['text'] or "", name= "Drev.house👉 Строительство 🪓")
                    preview_text = (
                        f"📨 Новое отложенное сообщение\n\n"
                        f"{preview_text_content}\n"
                        f"📎 Всего изображений: {len(json.loads(msg['image'])) if msg['image'] else 0}"
                    )

                    # Парсим изображения
                    image_urls = []
                    if msg['image']:
                        try:
                            parsed = json.loads(msg['image'])
                            if isinstance(parsed, list):
                                image_urls = [get_absolute_image_url(item['url']) for item in parsed if item.get('url')]
                            elif isinstance(parsed, str):
                                image_urls = [get_absolute_image_url(parsed)]
                        except:
                            image_urls = [get_absolute_image_url(msg['image'])] if msg['image'] else []

                    # Кнопки подтверждения/отмены
                    kb = InlineKeyboardBuilder()
                    kb.button(text="✅ Подтвердить", callback_data=f"confirm:{msg['id']}")
                    kb.button(text="❌ Отменить", callback_data=f"cancel:{msg['id']}")
                    kb.adjust(2)

                    try:
                        if image_urls:
                            # Альбом (первое фото с подписью)
                            media_group = []
                            for i, url in enumerate(image_urls):
                                if i == 0:
                                    media_group.append(InputMediaPhoto(media=url, caption=preview_text, parse_mode="HTML"))
                                else:
                                    media_group.append(InputMediaPhoto(media=url))
                            await bot.send_media_group(ADMIN_ID, media_group[:10])
                            # После альбома – кнопки
                            await bot.send_message(ADMIN_ID, "Выберите действие:", reply_markup=kb.as_markup())
                        else:
                            # Текст без фото
                            await bot.send_message(ADMIN_ID, preview_text, parse_mode="HTML", reply_markup=kb.as_markup())
                    except Exception as e:
                        logging.error(f"Не удалось отправить предпросмотр #{msg['id']}: {e}")
                        continue

                # Помечаем, что предпросмотр отправлен (без удаления старых сообщений)
                sqlite_cur.execute(
                    "INSERT OR IGNORE INTO preview_sent (message_id, sent_at) VALUES (?, ?)",
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
    add_user(message.from_user.id, message.from_user.first_name, message.from_user.username)

    root_key = next((k for k, v in nodes.items() if v.get('is_root')), None)
    if not root_key:
        await message.answer("❌ Корневой узел не настроен")
        return

    await send_node(message.chat.id, root_key, message.bot, user_name=message.from_user.first_name)
    await message.answer(
        "Для удобства смотрите меню 👇",
        reply_markup=get_main_keyboard(message.from_user.id)
    )

@dp.callback_query(F.data == "cancel_contact")
async def cancel_contact(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Диалог с менеджером отменён.")
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_keyboard(callback.from_user.id)
    )
    await callback.answer()
# ---------- Переход по узлам ----------
@dp.callback_query(F.data.startswith("node:"))
async def node_callback(callback: types.CallbackQuery, state: FSMContext):
    node_key = callback.data.split(":")[1]

    # Если это узел связи с менеджером
    if node_key == 'manager':
        await state.set_state(ContactState.waiting_for_message)
        # Создаём клавиатуру с кнопкой "Отмена"
        cancel_kb = InlineKeyboardBuilder()
        cancel_kb.button(text="❌ Отмена", callback_data="cancel_contact")
        await callback.message.answer(
            "Напишите сообщение менеджеру:",
            reply_markup=cancel_kb.as_markup()
        )
        await callback.answer()
        return

    # Обычная логика для остальных узлов
    if node_key in START_NODE_KEYS:
        await send_node(callback.message.chat.id, node_key, callback.bot,
                        user_name=callback.from_user.first_name)
    else:
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
        price_per_m2 = 750 if area < 20 else 590
        diameter = "16-20"
        type_name = "Беседка"
        details = (
            "🪙 Стоимость указана+/- локоть что бы вы могли от чего-то отталкиваться 🌿\n\n"
            "📎 Точная стоимость только после предоставления строительного проекта, который можно заказать у нашего проектировщика📑\n\n"
            "💳 Цены беседок за м² в среднем \n"
            "➡️ ДО 20 м² от 750р за м²\n"
            "➡️ Больше 20 м² от 590р за м²\n\n"
            "💟 Подробнее о стоимости <u>✏️ пишите прорабу по кнопке ниже ⤵️</u>"
        )
    elif btype == "calc_type_bath":
        # Цена зависит от площади
       
        price_per_m2 = 650 if area > 50 else 800  
        diameter = "22-24"
        type_name = "Баня"
        details = (
             "🪙 Стоимость указана+/- локоть что бы вы могли от чего-то отталкиваться 🌿\n\n"
            "📎 Точная стоимость только после предоставления строительного проекта, который можно заказать у нашего проектировщика📑\n\n"
            "💳 Цены бани за м² в среднем \n"
            "➡️ ДО 50 м² от 800р за м²\n"
            "➡️ Больше 50 м² от 650р за м²\n\n"
            "💟 Подробнее о стоимости <u>✏️ пишите прорабу по кнопке ниже ⤵️</u>\n"
        )
    elif btype == "calc_type_summer":
        price_per_m2 = 752
        diameter = "20-22"
        type_name = "Летний дом"
        details = (
             "🪙 Стоимость указана+/- локоть что бы вы могли от чего-то отталкиваться 🌿\n\n"
            "📎 Точная стоимость только после предоставления строительного проекта, который можно заказать у нашего проектировщика📑\n\n"
            "💳 Цены летних домов за м² в среднем от 752р-963 за м² м \n"
            "➡️ Если под ключ от 1150р-1290р за м²\n\n"
            "💟 Подробнее о стоимости <u>✏️ пишите прорабу по кнопке ниже ⤵️</u>\n"
        )
    elif btype == "calc_type_house":
        price_per_m2 = 1150 
        diameter = "28"
        type_name = "Дом для проживания"
        details = (
            "🪙 Стоимость указана+/- локоть что бы вы могли от чего-то отталкиваться 🌿\n\n"
            "📎 Точная стоимость только после предоставления строительного проекта, который можно заказать у нашего проектировщика📑\n\n"
            "💳 Цены домов для постоянного проживания за м² в среднем от 1150р за м² до 1790р за м² \n\n"
            "💟 Подробнее о стоимости <u>✏️ пишите прорабу по кнопке ниже ⤵️</u>"
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

# @dp.message(Command("export_users"), F.from_user.id == ADMIN_ID)
# async def cmd_export_users(message: types.Message):
#     try:
#         conn = sqlite3.connect('users.db')
#         cursor = conn.cursor()
#         cursor.execute("SELECT user_id, first_name, username, join_date FROM users ORDER BY join_date DESC")
#         rows = cursor.fetchall()
#         conn.close()

#         if not rows:
#             await message.answer("📭 Нет пользователей в базе.")
#             return

#         import io
#         import csv
#         import codecs

#         # Используем разделитель ';' для лучшей совместимости с Excel (русская версия)
#         output = io.BytesIO()
#         # codecs.getwriter('utf-8-sig') добавляет BOM
#         writer = csv.writer(codecs.getwriter('utf-8-sig')(output), delimiter=';')
#         writer.writerow(["ID", "Имя", "Username", "Дата регистрации"])
#         for row in rows:
#             # Можно отформатировать дату, если нужно
#             # join_date хранится в ISO формате, оставим как есть
#             writer.writerow(row)
#         output.seek(0)

#         await message.answer_document(
#             types.BufferedInputFile(output.getvalue(), filename="users_export.csv"),
#             caption=f"📊 Экспортировано пользователей: {len(rows)}"
#         )
#     except Exception as e:
#         logging.error(f"Ошибка экспорта пользователей: {e}")
#         await message.answer("❌ Не удалось выполнить экспорт.")

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
    current = await state.get_state()
    if current is not None:
        return

    norm = normalize(message.text)
    uid = message.from_user.id

    # Системные кнопки
    if message.text == CONTACT_BUTTON_TEXT:
        await state.set_state(ContactState.waiting_for_message)
        cancel_kb = InlineKeyboardBuilder()
        cancel_kb.button(text="❌ Отмена", callback_data="cancel_contact")
        await message.answer(
            "Напишите сообщение менеджеру:",
            reply_markup=cancel_kb.as_markup()
        )
        return

    if message.text == MAILING_BUTTON_TEXT and uid == ADMIN_ID:
        await state.set_state(MailingStates.waiting_for_content)
        await message.answer("✏️ Пришлите сообщение для рассылки.")
        return

    if message.text == CALC_BUTTON_TEXT:
        await message.answer("Выберите тип постройки:", reply_markup=get_calc_type_keyboard())
        return

    # Кнопка статистики (только для админа)
    if message.text == STATS_BUTTON_TEXT and uid == ADMIN_ID:
        try:
            conn = sqlite3.connect('users.db')
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, first_name, username, join_date FROM users ORDER BY join_date DESC")
            rows = cursor.fetchall()
            conn.close()

            if not rows:
                await message.answer("📭 Нет пользователей в базе.")
                return

            # Создаём CSV-файл с BOM и разделителем ;
            import io
            import csv
            import codecs

            output = io.BytesIO()
            # codecs.getwriter('utf-8-sig') добавляет BOM для корректного отображения в Excel
            writer = csv.writer(codecs.getwriter('utf-8-sig')(output), delimiter=';')
            writer.writerow(["ID", "Имя", "Username", "Дата регистрации"])
            for row in rows:
                writer.writerow(row)
            output.seek(0)

            await message.answer_document(
                types.BufferedInputFile(output.getvalue(), filename="users_export.csv"),
                caption=f"📊 Экспортировано пользователей: {len(rows)}"
            )
        except Exception as e:
            logging.error(f"Ошибка экспорта пользователей: {e}")
            await message.answer("❌ Не удалось выполнить экспорт.")
        return

    # Обычные reply-кнопки
        # Обычные reply-кнопки
    if norm not in reply_buttons:
        # Пересылаем сообщение администратору (как в process_client_message)
        user = message.from_user
        user_id = user.id
        username = f"@{user.username}" if user.username else "нет"
        user_link = f"<a href='tg://user?id={user_id}'>{user.full_name or user.first_name}</a>"

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
                await message.bot.send_message(ADMIN_ID, notification, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)
            except Exception as e:
                logging.warning(f"Не удалось отправить менеджеру {ADMIN_ID}: {e}")

        await message.answer("✅ Отправлено менеджеру. Ожидайте.", reply_markup=get_main_keyboard(user_id))
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

async def get_admin_name(bot):
    try:
        chat = await bot.get_chat(ADMIN_ID)
        return chat.first_name or "Drev.house👉 Строительство 🪓"
    except:
        return "Drev.house👉 Строительство 🪓"


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

