import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import hashlib
# =========================
# MYSQL CONFIG
# =========================
MYSQL_CONFIG = {
    'host': '62.217.180.138',
    'user': 'myuser',
    'password': 'MyStrongPass!123',
    'database': 'my_database',
}

# Папка с медиафайлами для delayed_messages (не используется, т.к. нет file_path)
MEDIA_ROOT = '/path/to/media/files'   # Можно оставить, но не используется

# =========================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# =========================
BOT_TOKEN = None
ADMIN_ID = None
MESSAGES = {}
LAST_MESSAGES_UPDATE = None
LAST_BUTTONS_UPDATE = None

DYNAMIC_BUTTONS = {}                  # {normalized_text: response_text}
DYNAMIC_LIST = []                     # порядок из menu_order
CONTACT_BUTTON_TEXT = 'Связаться с менеджером'  # Текст кнопки для контакта
MAILING_BUTTON_TEXT = 'Рассылка'                # Текст кнопки для рассылки
CALC_BUTTON_TEXT = 'Рассчитать стоимость'     # Текст кнопки для калькулятора

BUTTON_KEY_TO_MESSAGE_KEY = {
    "catalog": "catalog",
    "about": "about",
    "prices": "prices",
    "contacts": "contacts"
}  # Адаптировал без key, используй если нужно fallback по button_text.lower()

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

# =========================
# ВСПОМОГАТЕЛЬНЫЕ
# =========================
import re

import re
import html

from bs4 import BeautifulSoup

def clean_html_for_telegram(text, name=None):
    if not text:
        return text
    if name:
        text = text.replace('{name}', name)
    
    # Декодируем HTML-сущности
    import html
    text = html.unescape(text)
    
    # Парсим HTML
    soup = BeautifulSoup(text, 'html.parser')
    
    # Разрешённые теги
    allowed_tags = {'b', 'strong', 'i', 'em', 'u', 'ins', 's', 'strike', 'del', 'a', 'code', 'pre'}
    
    # Проходим по всем тегам, удаляем неподдерживаемые
    for tag in soup.find_all():
        if tag.name not in allowed_tags:
            tag.unwrap()  # удаляем тег, сохраняя содержимое
        else:
            # Для ссылок оставляем только href
            if tag.name == 'a':
                href = tag.get('href')
                tag.attrs = {}
                if href:
                    tag['href'] = href
            else:
                tag.attrs = {}  # удаляем все атрибуты у остальных
    
    return str(soup)

def normalize(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split()).strip().lower()

def get_message_hash(msg):
    """Возвращает MD5-хеш содержимого отложенного сообщения."""
    content = f"{msg['message_text']}_{msg['delay_hours']}_{msg.get('image_url', '')}"
    return hashlib.md5(content.encode('utf-8')).hexdigest()

# =========================
# ЗАГРУЗКА ДАННЫХ
# =========================
def load_active_bot():
    global BOT_TOKEN, ADMIN_ID
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT bot_token, admin_id FROM bot_accounts WHERE status = 1 LIMIT 1")
        bot = cursor.fetchone()
        cursor.close()
        conn.close()
        if not bot:
            raise Exception("Нет активного бота")
        BOT_TOKEN = '8639319444:AAEU9aUaTq3rxuW6xf2nlfXCRiCN37qrD7c' #bot['bot_token']
        ADMIN_ID = '5374683743'#bot['admin_id']
        logging.info("✅ Бот загружен")
    except Error as e:
        logging.error(f"❌ Ошибка загрузки бота: {e}")
        raise


def load_messages():
    global MESSAGES, LAST_MESSAGES_UPDATE
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT message_key, message_text, updated_at FROM bot_messages")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        MESSAGES = {r['message_key']: r['message_text'] for r in rows}
        LAST_MESSAGES_UPDATE = max((r['updated_at'] for r in rows if r['updated_at']), default=None)
        logging.info(f"✅ Сообщений: {len(MESSAGES)}")
    except Error as e:
        logging.error(f"❌ Ошибка загрузки сообщений: {e}")


def load_buttons():
    global DYNAMIC_BUTTONS, DYNAMIC_LIST, LAST_BUTTONS_UPDATE
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT button_text, response_text, updated_at
            FROM bot_buttons
            WHERE bot_id = 1 AND button_type = 'reply' AND is_active = 1
            ORDER BY menu_order
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        new_dynamic = {}
        new_list = []
        max_upd = None

        for row in rows:
            text = row['button_text']
            resp = row.get('response_text', '')
            norm = normalize(text)

            if row['updated_at'] and (max_upd is None or row['updated_at'] > max_upd):
                max_upd = row['updated_at']

            # Специальные кнопки – добавляем в список, но не в DYNAMIC_BUTTONS
            if text in (CONTACT_BUTTON_TEXT, MAILING_BUTTON_TEXT, CALC_BUTTON_TEXT):
                new_list.append(text)
                continue

            # Обычные кнопки
            if resp and resp.strip():
                new_dynamic[norm] = resp
                new_list.append(text)
            else:
                logging.warning(f"Кнопка '{text}' пропущена: пустой ответ")

        # Добавляем системные кнопки, если их ещё нет в списке
        for sys_button in (CONTACT_BUTTON_TEXT, CALC_BUTTON_TEXT, MAILING_BUTTON_TEXT):
            if sys_button not in new_list:
                new_list.append(sys_button)

        DYNAMIC_BUTTONS = new_dynamic
        DYNAMIC_LIST = new_list
        LAST_BUTTONS_UPDATE = max_upd
        logging.info(f"✅ Загружено кнопок: {len(DYNAMIC_LIST)} (динамических: {len(DYNAMIC_BUTTONS)})")
    except Error as e:
        logging.error(f"❌ Ошибка кнопок: {e}")

def check_messages_updated():
    global LAST_MESSAGES_UPDATE
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(updated_at) FROM bot_messages")
        res = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        if res and res != LAST_MESSAGES_UPDATE:
            load_messages()
    except Error as e:
        logging.error(f"check_messages_updated: {e}")


def check_buttons_updated():
    global LAST_BUTTONS_UPDATE
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(updated_at) FROM bot_buttons WHERE bot_id = 1 AND button_type = 'reply' AND is_active = 1")
        res = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        if res and res != LAST_BUTTONS_UPDATE:
            load_buttons()
    except Error as e:
        logging.error(f"check_buttons_updated: {e}")


# =========================
# КЛАВИАТУРА
# =========================
def get_main_keyboard(user_id=None):
    builder = ReplyKeyboardBuilder()
    for text in DYNAMIC_LIST:
        if text == MAILING_BUTTON_TEXT and user_id != ADMIN_ID:
            continue
        builder.add(types.KeyboardButton(text=text))
    if builder.buttons:  # если есть кнопки
        builder.adjust(2)
        return builder.as_markup(resize_keyboard=True)
    else:
        # если нет кнопок – вернуть пустую клавиатуру (или None)
        return types.ReplyKeyboardRemove()

def get_calc_type_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🏕 Беседка", callback_data="calc_type_gazebo")
    builder.button(text="🛁 Баня / Летний домик", callback_data="calc_type_bath")
    builder.button(text="🏠 Дом ПП (28 диаметр)", callback_data="calc_type_house")
    builder.adjust(1)
    return builder.as_markup()


def get_calc_new_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Новый расчет", callback_data="calc_new")
    builder.button(text="🏠 В меню", callback_data="calc_to_menu")
    builder.adjust(1)
    return builder.as_markup()


# =========================
# АВТОМАТИЧЕСКАЯ ОТПРАВКА ОТЛОЖЕННЫХ СООБЩЕНИЙ (с задержкой + image_url)
# =========================
import os
from pathlib import Path
from aiogram.types import FSInputFile

# Базовая директория скрипта
async def check_delayed_media_messages(bot: Bot):
    while True:
        try:
            # Загружаем активные сообщения из MySQL
            mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
            mysql_cur = mysql_conn.cursor(dictionary=True)
            mysql_cur.execute("""
                SELECT id, message_text, delay_hours, image_url
                FROM delayed_messages
                WHERE bot_id = 1 AND is_active = 1
            """)
            delayed_list = mysql_cur.fetchall()
            mysql_cur.close()
            mysql_conn.close()

            # Работа с SQLite
            sqlite_conn = sqlite3.connect('drev_house.db')
            sqlite_cur = sqlite_conn.cursor()
            sqlite_cur.execute("SELECT user_id, join_date FROM users")
            users = sqlite_cur.fetchall()
            now = datetime.now()

            for user_id, join_str in users:
                try:
                    join_date = datetime.strptime(join_str, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    continue

                for msg in delayed_list:
                    send_time = join_date + timedelta(minutes=msg['delay_hours'])

                    if now >= send_time:
                        # Вычисляем хеш содержимого
                        msg_hash = get_message_hash(msg)
                        # Проверяем по хешу (не по ID)
                        sqlite_cur.execute(
                            "SELECT 1 FROM user_delayed_status WHERE user_id = ? AND content_hash = ?",
                            (user_id, msg_hash)
                        )
                        if sqlite_cur.fetchone():
                            # Уже отправляли такое сообщение
                            continue

                        caption = msg.get('message_text') or ""
                        sent_success = False
                        image = msg.get('image_url')

                        try:
                            if image and isinstance(image, str) and image.strip():
                                image = image.strip()
                                if image.startswith(('http://', 'https://')):
                                    await bot.send_photo(user_id, image, caption=caption, parse_mode="HTML")
                                    sent_success = True
                                else:
                                    # Локальный файл (если нужно)
                                    # ... ваш код для локальных файлов
                                    pass
                            else:
                                await bot.send_message(user_id, caption, parse_mode="HTML")
                                sent_success = True
                        except Exception as e:
                            logging.error(f"Ошибка отправки delayed #{msg['id']} пользователю {user_id}: {e}")
                            if "chat not found" in str(e).lower() or "bot was blocked" in str(e).lower():
                                sqlite_cur.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
                                sqlite_conn.commit()
                                logging.info(f"Пользователь {user_id} удалён (бот заблокирован)")
                            continue

                        if sent_success:
                            sqlite_cur.execute(
                                "INSERT INTO user_delayed_status (user_id, delayed_id, content_hash, sent_at) VALUES (?, ?, ?, ?)",
                                (user_id, msg['id'], msg_hash, now)
                            )
                            sqlite_conn.commit()
                            logging.info(f"✅ Delayed #{msg['id']} отправлено пользователю {user_id}")

            sqlite_cur.close()
            sqlite_conn.close()

        except Exception as e:
            logging.error(f"check_delayed_media_messages error: {e}")

        await asyncio.sleep(30)
async def forward_any_to_admin(message: types.Message, bot: Bot):
    user = message.from_user
    username = f"@{user.username}" if user.username else "скрыт"
    user_link = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
    caption_prefix = f"📩 <b>Сообщение от {user_link} (username: {username})</b>\n\n"

    if message.caption:
        full_caption = caption_prefix + message.caption
    else:
        full_caption = caption_prefix

    try:
        if message.text:
            await bot.send_message(ADMIN_ID, caption_prefix + message.text, parse_mode="HTML")
        elif message.photo:
            photo = message.photo[-1].file_id
            await bot.send_photo(ADMIN_ID, photo, caption=full_caption, parse_mode="HTML")
        elif message.video:
            await bot.send_video(ADMIN_ID, message.video.file_id, caption=full_caption, parse_mode="HTML")
        elif message.document:
            await bot.send_document(ADMIN_ID, message.document.file_id, caption=full_caption, parse_mode="HTML")
        elif message.audio:
            await bot.send_audio(ADMIN_ID, message.audio.file_id, caption=full_caption, parse_mode="HTML")
        elif message.voice:
            await bot.send_voice(ADMIN_ID, message.voice.file_id, caption=full_caption, parse_mode="HTML")
        elif message.sticker:
            await bot.send_sticker(ADMIN_ID, message.sticker.file_id)
            await bot.send_message(ADMIN_ID, f"📩 Стикер от {user_link}", parse_mode="HTML")
        else:
            await bot.send_message(ADMIN_ID, f"📩 {user_link} отправил неподдерживаемый тип.", parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка пересылки: {e}")

async def forward_to_admin(message: types.Message, bot: Bot):
    user = message.from_user
    username = f"@{user.username}" if user.username else "скрыт"
    user_link = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
    admin_text = (
        f"📩 <b>Новый вопрос!</b>\n\n"
        f"👤 <b>Клиент:</b> {user_link}\n"
        f"🔗 <b>Username:</b> {username}\n"
        f"📝 <b>Текст:</b> {message.text}\n\n"
        f"👆 <i>Нажмите на имя клиента выше, чтобы начать чат</i>"
    )
    await bot.send_message(ADMIN_ID, admin_text, parse_mode="HTML")
    reply = MESSAGES.get("user_reply", "Сообщение отправлено менеджеру.")
    await message.answer(reply)

async def process_mailing(bot: Bot, mailing_id: int, msg_data: dict):
    conn = sqlite3.connect('drev_house.db')
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
    except Error as e:
        logging.error(f"Ошибка статуса рассылки: {e}")

async def send_previews_to_admin(bot: Bot):
    """Проверяет сообщения с is_active=0 и отправляет предпросмотр админу, если ещё не отправляли."""
    while True:
        try:
            # Загружаем из MySQL неподтверждённые сообщения
            mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
            mysql_cur = mysql_conn.cursor(dictionary=True)
            mysql_cur.execute("""
                SELECT id, message_text, delay_hours, image_url
                FROM delayed_messages
                WHERE bot_id = 1 AND is_active = 0
            """)
            unconfirmed = mysql_cur.fetchall()
            mysql_cur.close()
            mysql_conn.close()

            if not unconfirmed:
                await asyncio.sleep(30)
                continue

            # Подключаемся к SQLite
            sqlite_conn = sqlite3.connect('drev_house.db')
            sqlite_cur = sqlite_conn.cursor()

            for msg in unconfirmed:
                # Проверяем, отправляли ли уже предпросмотр
                sqlite_cur.execute("SELECT 1 FROM preview_sent WHERE message_id = ?", (msg['id'],))
                if sqlite_cur.fetchone():
                    continue  # уже отправляли

                # Формируем текст предпросмотра
                preview_text = (
                    f"📨 **Новое отложенное сообщение**\n\n"
                    f"**Текст:** {msg['message_text']}\n"
                    f"**Задержка:** {msg['delay_hours']} ч\n"
                    f"**Изображение:** {msg['image_url'] or 'нет'}"
                )
                # Кнопки
                kb = InlineKeyboardBuilder()
                kb.button(text="✅ Подтвердить", callback_data=f"confirm_{msg['id']}")
                kb.button(text="❌ Отменить", callback_data=f"cancel_{msg['id']}")
                kb.adjust(2)

                try:
                    if msg['image_url']:
                        await bot.send_photo(ADMIN_ID, msg['image_url'], caption=preview_text,
                                             parse_mode="HTML", reply_markup=kb.as_markup())
                    else:
                        await bot.send_message(ADMIN_ID, preview_text,
                                               parse_mode="HTML", reply_markup=kb.as_markup())
                except Exception as e:
                    logging.error(f"Не удалось отправить предпросмотр: {e}")
                    continue

                # Помечаем в SQLite, что предпросмотр отправлен
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

async def check_pending_mailings(bot: Bot):
    while True:
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT id, message_text, content_type, file_id, caption
                FROM bot_mailings
                WHERE send_at <= NOW() AND status = 'pending'
                ORDER BY send_at
            """)
            mailings = cursor.fetchall()
            cursor.close()
            conn.close()

            for mailing in mailings:
                conn = mysql.connector.connect(**MYSQL_CONFIG)
                cursor = conn.cursor()
                cursor.execute("UPDATE bot_mailings SET status = 'processing' WHERE id = %s AND status = 'pending'", (mailing['id'],))
                conn.commit()
                affected = cursor.rowcount
                cursor.close()
                conn.close()

                if affected > 0:
                    msg_data = {'content_type': mailing['content_type']}
                    if mailing['content_type'] == 'text':
                        msg_data['text'] = mailing['message_text']
                    else:
                        msg_data['file_id'] = mailing['file_id']
                        msg_data['caption'] = mailing['caption'] or ''
                    asyncio.create_task(process_mailing(bot, mailing['id'], msg_data))
        except Exception as e:
            logging.error(f"Ошибка проверки рассылок: {e}")

        await asyncio.sleep(60)

def init_db():
    conn = sqlite3.connect('drev_house.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, name TEXT, join_date TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS user_autosend_status (user_id INTEGER, autosend_id INTEGER, sent_at TEXT, PRIMARY KEY (user_id, autosend_id))''')
    # Создаём таблицу для статусов отложенных сообщений с хешем
    cursor.execute('''CREATE TABLE IF NOT EXISTS user_delayed_status (
        user_id INTEGER,
        delayed_id INTEGER,
        content_hash TEXT,
        sent_at TEXT,
        PRIMARY KEY (user_id, content_hash)
    )''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS preview_sent (
            message_id INTEGER PRIMARY KEY,
            sent_at TEXT
        )
    ''')
    conn.commit()
    conn.close()

def add_user(user_id, name):
    conn = sqlite3.connect('drev_house.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO users (user_id, name, join_date) VALUES (?, ?, ?)',
                   (user_id, name, datetime.now()))
    conn.commit()
    conn.close()

def get_active_managers():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM bot_managers WHERE is_active = 1")
        managers = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return managers
    except Error as e:
        logging.error(f"❌ Ошибка менеджеров: {e}")
        return []

# =========================
# РЕГИСТРАЦИЯ ОБРАБОТЧИКОВ
# =========================
# =========================
# РЕГИСТРАЦИЯ ОБРАБОТЧИКОВ
# =========================
def register_handlers(dp: Dispatcher, bot: Bot):
    
    # --- 1. КОМАНДЫ (COMMANDS) ---
    @dp.message(Command("start"))
    async def cmd_start(message: types.Message, state: FSMContext):
        await state.clear()
        add_user(message.from_user.id, message.from_user.first_name)
        name = message.from_user.first_name
        welcome = MESSAGES.get("welcome_static", f"<b>{name}, приветствуем! ✌️</b>").replace("{name}", name)
        welcome = clean_html_for_telegram(welcome)

        btn_prod = InlineKeyboardBuilder()
        btn_prod.button(text="🎬 фото-видео с производства", callback_data="prod_info_static")
        await message.answer(welcome, reply_markup=btn_prod.as_markup(), parse_mode="HTML")
        await message.answer(
            f"<b>{name}!</b>\n\nЗдесь вы можете выбрать нужную категорию или написать сообщение менеджеру.",
            reply_markup=get_main_keyboard(message.from_user.id),
            parse_mode="HTML"
        )

    @dp.message(Command("mailing"), F.from_user.id == ADMIN_ID)
    async def cmd_mailing(message: types.Message, state: FSMContext):
        await message.answer("✏️ Пришлите сообщение для рассылки.")
        await state.set_state(MailingStates.waiting_for_content)


    # --- 2. ОБРАБОТЧИКИ СОСТОЯНИЙ (FSM STATES) ---
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

    @dp.message(MailingStates.waiting_for_schedule, F.from_user.id == ADMIN_ID)
    async def mailing_schedule_time(message: types.Message, state: FSMContext):
        try:
            send_at = datetime.strptime(message.text.strip(), "%d.%m.%Y %H:%M")
        except ValueError:
            await message.answer("❌ Неверный формат. Повторите.")
            return

        if send_at <= datetime.now():
            await message.answer("❌ Время в будущем. Повторите.")
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

    @dp.message(ContactState.waiting_for_message)
    async def process_client_message(message: types.Message, state: FSMContext, bot: Bot):
        if not message.text:
            await message.answer("Пока только текст.")
            return

        user = message.from_user
        user_id = user.id
        username = f"@{user.username}" if user.username else "нет"
        user_link = f"<a href='tg://user?id={user_id}'>{user.full_name or user.first_name}</a>"

        await message.answer("✅ Отправлено менеджеру. Ожидайте.", reply_markup=get_main_keyboard(message.from_user.id))
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
                logging.warning(f"Ошибка менеджеру {manager_id}: {e}")

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
            await message.answer("❌ Неверный формат.")
            return

        if area <= 0:
            await message.answer("❌ Площадь >0.")
            return

        data = await state.get_data()
        btype = data.get('building_type')

        if btype == "calc_type_gazebo":
            price_per_m2 = 750
        elif btype == "calc_type_bath":
            price_per_m2 = 950 if area < 50 else 800
        elif btype == "calc_type_house":
            price_per_m2 = 1150 if area < 50 else 980
        else:
            await message.answer("Ошибка типа. Начните заново.")
            await state.clear()
            return

        total = area * price_per_m2
        result_text = (
            f"✅ Результат:\n\n"
            f"Тип: {btype.replace('calc_type_', '').capitalize()}\n"
            f"Площадь: {area:.2f} м²\n"
            f"Цена м²: от {price_per_m2} руб\n"
            f"<b>Итого: от {total:,.2f} руб.</b>"
        ).replace(',', ' ')

        await message.answer(result_text, parse_mode="HTML", reply_markup=get_calc_new_keyboard())
        await state.clear()

    @dp.message(ManagerReplyStates.waiting_for_reply)
    async def process_manager_reply(message: types.Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        target_user_id = data.get("reply_to_user_id")
        
        if not target_user_id:
            await message.answer("Ошибка: ID пользователя утерян.")
            await state.clear()
            return
            
        try:
            await bot.send_message(target_user_id, f"👤 <b>Ответ от менеджера:</b>\n\n{message.text}", parse_mode="HTML")
            await message.answer("✅ Ответ успешно отправлен клиенту.")
        except Exception as e:
            await message.answer(f"❌ Не удалось отправить ответ: {e}")
        finally:
            await state.clear()


    # --- 3. ИНЛАЙН КНОПКИ (CALLBACKS) ---
    @dp.callback_query(F.data == "prod_info_static")
    async def prod_info_callback(callback: types.CallbackQuery):
        name = callback.from_user.first_name
        text = MESSAGES.get("prod_info", f"<b>{name}</b>, информация о производстве").replace("{name}", name)
        text = clean_html_for_telegram(text)
        builder = InlineKeyboardBuilder()
        builder.button(text="⬅️ Вернуться к описанию", callback_data="back_to_welcome")
        await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
        await callback.answer()

    @dp.callback_query(F.data.startswith("confirm_"))
    async def confirm_message(callback: types.CallbackQuery):
        logging.info(f"🔹 Получен confirm: {callback.data}")
        try:
            msg_id = int(callback.data.split("_")[1])
            logging.info(f"🔹 ID сообщения: {msg_id}")

            # Подключение к MySQL
            mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
            mysql_cur = mysql_conn.cursor()
            update_sql = "UPDATE delayed_messages SET is_active = 1 WHERE id = %s"
            mysql_cur.execute(update_sql, (msg_id,))
            mysql_conn.commit()
            rows_affected = mysql_cur.rowcount
            logging.info(f"🔹 MySQL update: затронуто строк {rows_affected}")
            mysql_cur.close()
            mysql_conn.close()

            # Подключение к SQLite
            sqlite_conn = sqlite3.connect('drev_house.db')
            sqlite_cur = sqlite_conn.cursor()
            delete_sql = "DELETE FROM preview_sent WHERE message_id = ?"
            sqlite_cur.execute(delete_sql, (msg_id,))
            sqlite_conn.commit()
            rows_deleted = sqlite_cur.rowcount
            logging.info(f"🔹 SQLite delete: удалено строк {rows_deleted}")
            sqlite_cur.close()
            sqlite_conn.close()

            await callback.message.edit_text(f"✅ Сообщение #{msg_id} подтверждено.")
            await callback.answer()
        except Exception as e:
            logging.error(f"❌ Ошибка в confirm_: {e}", exc_info=True)
            await callback.answer("❌ Ошибка", show_alert=True)

    @dp.callback_query(F.data.startswith("cancel_"))
    async def cancel_message(callback: types.CallbackQuery):
        msg_id = int(callback.data.split("_")[1])
        try:
            # Удаляем сообщение из MySQL (или можно деактивировать)
            mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
            mysql_cur = mysql_conn.cursor()
            mysql_cur.execute("DELETE FROM delayed_messages WHERE id = %s", (msg_id,))
            mysql_conn.commit()
            mysql_cur.close()
            mysql_conn.close()

            # Удаляем из preview_sent
            sqlite_conn = sqlite3.connect('drev_house.db')
            sqlite_cur = sqlite_conn.cursor()
            sqlite_cur.execute("DELETE FROM preview_sent WHERE message_id = ?", (msg_id,))
            sqlite_conn.commit()
            sqlite_cur.close()
            sqlite_conn.close()

            await callback.message.edit_text(f"❌ Сообщение #{msg_id} отменено и удалено.")
            await callback.answer()
        except Exception as e:
            logging.error(f"Ошибка отмены: {e}")
            await callback.answer("❌ Ошибка", show_alert=True)

    @dp.callback_query(F.data == "back_to_welcome")
    async def back_to_welcome_callback(callback: types.CallbackQuery):
        name = callback.from_user.first_name
        welcome = MESSAGES.get("welcome_static", f"<b>{name}, приветствуем! ✌️</b>").replace("{name}", name)
        welcome = clean_html_for_telegram(welcome)

        btn_prod = InlineKeyboardBuilder()
        btn_prod.button(text="🎬 фото-видео с производства", callback_data="prod_info_static")
        await callback.message.edit_text(welcome, reply_markup=btn_prod.as_markup(), parse_mode="HTML")
        await callback.answer()

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

    @dp.callback_query(F.data == "mailing_cancel", F.from_user.id == ADMIN_ID)
    async def mailing_cancel_callback(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("❌ Отменено.")
        await state.clear()
        await callback.answer()

    @dp.callback_query(CalcStates.choosing_type, F.data.startswith("calc_type_"))
    async def calc_type_chosen(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        type_map = {
            "calc_type_gazebo": "Беседка",
            "calc_type_bath": "Баня / Летний домик (24 диаметр)",
            "calc_type_house": "Дом ПП (28 диаметр)"
        }
        chosen = type_map.get(callback.data, "Неизвестно")
        await state.update_data(building_type=callback.data)

        await callback.message.edit_text(
            f"Выбрано: {chosen}\n\nВведите размеры: два числа (ширина длина) или одно (площадь).",
            reply_markup=None
        )
        await state.set_state(CalcStates.waiting_for_dimensions)

    @dp.callback_query(CalcStates.choosing_type, F.data == "calc_cancel")
    async def calc_cancel(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await callback.message.edit_text("Отменено.")
        await state.clear()

    @dp.callback_query(F.data == "calc_new")
    async def calc_new(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await callback.message.delete()
        await callback.message.answer("Выберите тип:", reply_markup=get_calc_type_keyboard())
        await state.set_state(CalcStates.choosing_type)

    @dp.callback_query(F.data == "calc_to_menu")
    async def calc_to_menu(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await callback.message.delete()
        await callback.message.answer("Меню:", reply_markup=get_main_keyboard(callback.from_user.id))
        await state.clear()

    @dp.callback_query(F.data.startswith("reply_to_"))
    async def manager_reply_init(callback: types.CallbackQuery, state: FSMContext):
        user_id = callback.data.split("_")[2]
        await state.update_data(reply_to_user_id=user_id)
        await state.set_state(ManagerReplyStates.waiting_for_reply)
        await callback.message.answer("✏️ Напишите текст ответа клиенту:")
        await callback.answer()


    # --- 4. УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК ТЕКСТА (ВСЕГДА В САМОМ НИЗУ) ---
    @dp.message(F.text)
    async def handle_message(message: types.Message, state: FSMContext):
        current_state = await state.get_state()
        if current_state is not None:
            # Если бот ждет специфический текст состояния, но пользователь ввел что-то странное (что не поймали хэндлеры выше)
            return

        if not message.text:
            return
        
        norm = normalize(message.text)
        uid = message.from_user.id

        if message.text == CONTACT_BUTTON_TEXT:
            await state.set_state(ContactState.waiting_for_message)
            await message.answer(MESSAGES.get("contact_prompt", "Напишите сообщение менеджеру:"), reply_markup=get_main_keyboard(uid))
            return

        if message.text == MAILING_BUTTON_TEXT and uid == ADMIN_ID:
            await state.set_state(MailingStates.waiting_for_content)
            await message.answer("✏️ Пришлите сообщение для рассылки.")
            return

        if message.text == CALC_BUTTON_TEXT:
            await state.set_state(CalcStates.choosing_type)
            await message.answer("Выберите тип постройки:", reply_markup=get_calc_type_keyboard())
            return

        if norm in DYNAMIC_BUTTONS:
            await message.answer(DYNAMIC_BUTTONS[norm], parse_mode="HTML", reply_markup=get_main_keyboard(uid))
            return

        await message.answer("Неизвестная команда. Используйте кнопки ниже.", reply_markup=get_main_keyboard(uid))

# =========================
# ЗАПУСК
# =========================
async def refresh_loop():
    while True:
        check_messages_updated()
        check_buttons_updated()
        await asyncio.sleep(30)

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    load_active_bot()
    load_messages()
    load_buttons()
    init_db()

    storage = MemoryStorage()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=storage)

    register_handlers(dp, bot)

    asyncio.create_task(refresh_loop())
    asyncio.create_task(check_pending_mailings(bot))
    asyncio.create_task(check_delayed_media_messages(bot))
    asyncio.create_task(send_previews_to_admin(bot))    

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())