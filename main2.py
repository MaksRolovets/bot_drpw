# calc_bot.py – отдельный файл, запускается как самостоятельный бот
import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

BOT_TOKEN = '8507123559:AAGufCpotTM5Y9svjQuMEjJ53u-2h5NmICI'
ADMIN_ID = 2109578014  # можно оставить того же админа или другого

CALC_BUTTON_TEXT = '💵 Рассчитать стоимость'

class CalcStates(StatesGroup):
    waiting_for_dimensions = State()

dp = Dispatcher(storage=MemoryStorage())

# --- Reply-клавиатура с кнопкой расчёта ---
def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.add(types.KeyboardButton(text=CALC_BUTTON_TEXT))
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)

# --- Inline-клавиатуры ---
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
    builder.button(text="🔄 Новый расчет", callback_data="calc_new")
    builder.button(text="🏠 В меню", callback_data="calc_to_menu")
    builder.adjust(1)
    return builder.as_markup()

# --- Старт с deep linking ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    args = message.text.split()
    if len(args) > 1 and args[1].startswith("calc_"):
        # Если пришли с параметром, сразу показываем клавиатуру выбора типа
        await message.answer("Выберите тип постройки:", reply_markup=get_calc_type_keyboard())
    else:
        # Обычный старт – показываем reply-клавиатуру
        await message.answer(
            "Привет! Я бот для расчёта стоимости построек.\n"
            "Нажмите кнопку ниже, чтобы начать.",
            reply_markup=get_main_keyboard()
        )

# --- Обработка reply-кнопки "Рассчитать стоимость" ---
@dp.message(F.text == CALC_BUTTON_TEXT)
async def handle_calc_button(message: types.Message, state: FSMContext):
    await message.answer("Выберите тип постройки:", reply_markup=get_calc_type_keyboard())

# --- Обработчики inline-кнопок ---
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
    await callback.message.answer("Главное меню:", reply_markup=get_main_keyboard())

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

@dp.callback_query(F.data == "calc_to_menu")
async def calc_to_menu(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.delete()
    await callback.message.answer("Главное меню:", reply_markup=get_main_keyboard())

async def main():
    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=BOT_TOKEN)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())