import asyncio
import logging
import os
import uuid
import json
from typing import Optional, Dict, List, Tuple
from tariffs import TARIFFS
from instructions import INSTRUCTIONS
from locales import LOCALES
import aiofiles
from yookassa import Configuration, Payment
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Конфигурация
logging.basicConfig(level=logging.INFO)

class Config:
    TOKEN = os.environ.get("BOT_TOKEN")
    ADMINS = ["725739479", "693411987"]  # ID администраторов
    SUBSCRIBERS_FILE = "subscribers.txt"
    YOOKASSA_SHOP_ID = int(os.environ.get("YOOKASSA_SHOP_ID", 0))
    YOOKASSA_SECRET_KEY = os.environ.get("YOOKASSA_SECRET_KEY")
    IMAGES_DIR = "images"
    KEYS_DIR = "keys"
    CODES_FILE = "codes.txt"
    STATISTICS_FILE = "statistics.json"  # Файл для статистики

# Сервисы
class CodeManager:
    """Менеджер для работы с файлами кодов активации."""
    def __init__(self, key_dir: str, bot: Bot):
        self.key_dir = key_dir
        self.bot = bot

    async def get_code(self, key_file: str) -> Optional[str]:
        """Извлекает код из файла и уведомляет администратора, если кодов остаётся мало."""
        path = os.path.join(self.key_dir, key_file)
        try:
            async with aiofiles.open(path, mode="r+") as f:
                codes = await f.readlines()
                if not codes:
                    return None
                code = codes[0].strip()
                # Уведомляем администратора, если кодов осталось мало
                if len(codes) <= 10:
                    await self._notify_admin(key_file, len(codes))
                # Перезаписываем файл, удаляя использованный код
                await f.seek(0)
                await f.truncate()
                await f.writelines(codes[1:])
                return code
        except FileNotFoundError:
            logging.error(f"Файл не найден: {path}")
            return None
        except Exception as e:
            logging.error(f"Ошибка: {e}")
            return None

    async def check_code_exists(self, key_file: str) -> bool:
        """Проверяет, есть ли коды в файле."""
        path = os.path.join(self.key_dir, key_file)
        try:
            async with aiofiles.open(path, mode="r") as f:
                content = await f.readlines()
                return bool(content)
        except FileNotFoundError:
            return False
        except Exception as e:
            logging.error(f"Ошибка проверки кодов: {e}")
            return False

    async def _notify_admin(self, key_file: str, remaining_codes: int):
        """Уведомляет администратора о заканчивающихся кодах."""
        message = f"⚠️ Внимание! В файле {key_file} осталось {remaining_codes} кодов."
        for admin_id in Config.ADMINS:
            try:
                await self.bot.send_message(admin_id, message)
            except Exception as e:
                logging.error(f"Ошибка при отправке уведомления администратору {admin_id}: {e}")

class PaymentManager:
    """Менеджер для работы с платежами через Yookassa."""
    @staticmethod
    async def create_payment(amount: float, description: str, bot_username: str, user_id: int) -> dict:
        """Создаёт платеж через Yookassa."""
        Configuration.configure(
            account_id=Config.YOOKASSA_SHOP_ID,
            secret_key=Config.YOOKASSA_SECRET_KEY
        )
        payment = Payment.create({
            "amount": {
                "value": f"{amount:.2f}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": f"https://t.me/{bot_username}"
            },
            "capture": True,
            "description": description,
            "metadata": {
                "user_id": user_id
            }
        }, uuid.uuid4())
        return payment

# Состояния
class Form(StatesGroup):
    language = State()
    welcome = State()
    faq = State()
    citizenship = State()
    operator = State()
    tariff = State()
    instruction = State()
    payment = State()
    payment_confirmed = State()  # Новое состояние для отслеживания подтверждения оплаты

# Функции для работы с подписчиками
def add_subscriber(chat_id: int):
    """Добавляет chat_id в файл подписчиков, если его там ещё нет."""
    subscribers = set()
    if os.path.exists(Config.SUBSCRIBERS_FILE):
        with open(Config.SUBSCRIBERS_FILE, "r") as f:
            subscribers = set(line.strip() for line in f if line.strip())
    if str(chat_id) not in subscribers:
        with open(Config.SUBSCRIBERS_FILE, "a") as f:
            f.write(f"{chat_id}\n")
        logging.info(f"Добавлен новый подписчик: {chat_id}")

def get_subscribers() -> List[int]:
    """Возвращает список chat_id подписчиков из файла."""
    if not os.path.exists(Config.SUBSCRIBERS_FILE):
        return []
    with open(Config.SUBSCRIBERS_FILE, "r") as f:
        return [int(line.strip()) for line in f if line.strip()]

# Обработчики
class TariffBot:
    def __init__(self):
        self.bot = Bot(token=Config.TOKEN)
        self.dp = Dispatcher()
        self.code_manager = CodeManager(Config.KEYS_DIR, self.bot)
        self._register_handlers()
        self.statistics_file = Config.STATISTICS_FILE

    def _register_handlers(self):
        """Регистрирует обработчики команд и callback-запросов."""
        self.dp.message.register(self.start, Command("start"))
        self.dp.message.register(self.broadcast, Command("send"))
        self.dp.message.register(self.show_stats, Command("stats"))
        self.dp.callback_query.register(self.process_language, F.data.in_(["ru", "uz", "tj"]))
        self.dp.callback_query.register(self.process_back, F.data.startswith("back:"))
        self.dp.callback_query.register(self.select_tariff, F.data == "select_tariff")
        self.dp.callback_query.register(self.process_citizenship, F.data.startswith("citizen_"))
        self.dp.callback_query.register(self.process_operator, F.data.startswith("operator_"))
        self.dp.callback_query.register(self.process_tariff, F.data.startswith("tariff_"))
        self.dp.callback_query.register(self.process_instruction, F.data == "show_instructions")
        self.dp.callback_query.register(self.process_payment, F.data == "confirm_payment")
        self.dp.callback_query.register(self.process_proceed_payment, F.data == "proceed_to_payment")
        self.dp.callback_query.register(self.show_faq, F.data == "faq")
        self.dp.message.register(self.handle)

    async def start(self, message: types.Message, state: FSMContext):
        """Обработчик команды /start."""
        await state.set_state(Form.language)
        add_subscriber(message.chat.id)
        logging.info(f"Start chat id: {message.chat.id}")
        if str(message.chat.id) in Config.ADMINS:
            await message.answer("👑 Аккаунт администратора!")
        await message.answer(
            LOCALES["ru"]["choose_language"],
            reply_markup=self._language_keyboard()
        )

    async def handle(self, message: types.Message, state: FSMContext):
        """Обработчик неизвестных сообщений."""
        await message.answer("Не понимаю вас. Используйте /start")

    def _language_keyboard(self) -> InlineKeyboardMarkup:
        """Клавиатура для выбора языка."""
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="🇷🇺 Русский", callback_data="ru"),
            InlineKeyboardButton(text="🇺🇿 O'zbek", callback_data="uz"),
            InlineKeyboardButton(text="🇹🇯 Тоҷикӣ", callback_data="tj")
        )
        return builder.as_markup()

    async def broadcast(self, message: types.Message, state: FSMContext):
        """Обработчик команды /send для рассылки сообщений."""
        if str(message.chat.id) not in Config.ADMINS:
            await message.answer("У вас нет доступа к этой команде.")
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer("Используйте: /send <сообщение>")
            return
        broadcast_message = parts[1].strip()
        subscribers = get_subscribers()
        sent = 0
        for chat_id in subscribers:
            try:
                await self.bot.send_message(chat_id, broadcast_message)
                sent += 1
            except Exception as e:
                logging.error(f"Ошибка отправки пользователю {chat_id}: {e}")
        await message.answer(f"Рассылка отправлена {sent} пользователям.")

    async def process_language(self, callback: CallbackQuery, state: FSMContext):
        """Обработчик выбора языка."""
        lang = callback.data
        await state.update_data(lang=lang)
        await state.set_state(Form.welcome)
        await callback.message.edit_text(
            LOCALES[lang]["welcome"],
            reply_markup=self._welcome_keyboard(lang)
        )

    async def show_faq(self, callback: CallbackQuery, state: FSMContext):
        """Обработчик для показа FAQ."""
        builder = InlineKeyboardBuilder()
        data = await state.get_data()
        lang = data.get("lang", 'ru')
        await state.set_state(Form.faq)
        builder.row(
            InlineKeyboardButton(
                text=LOCALES[lang]["back"],
                callback_data="back:welcome"
            )
        )
        await callback.message.edit_text(
            LOCALES[lang]["questions"],
            reply_markup=builder.as_markup()
        )

    def _welcome_keyboard(self, lang: str) -> InlineKeyboardMarkup:
        """Клавиатура для приветственного сообщения."""
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(
                text=LOCALES[lang].get("select_tariff", "✅ Выбрать тариф"),
                callback_data="select_tariff"
            ),
            InlineKeyboardButton(
                text=LOCALES[lang].get("faq", "❓ Частые вопросы"),
                callback_data="faq"
            )
        )
        builder.row(
            InlineKeyboardButton(
                text=LOCALES[lang]["back"],
                callback_data="back:language"
            )
        )
        return builder.as_markup()

    def _citizenship_keyboard(self, lang: str) -> InlineKeyboardMarkup:
        """Клавиатура для выбора гражданства."""
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(
                text="🇷🇺 Гражданин РФ" if lang == "ru" 
                     else "🇷🇺 Rossiya fuqarosi" if lang == "uz" 
                     else "🇷🇺 Русия шаҳрвонди",
                callback_data="citizen_ru"
            )
        )
        builder.row(
            InlineKeyboardButton(
                text="🌍 Иностранец" if lang == "ru" 
                     else "🌍 Chet el fuqarosi" if lang == "uz" 
                     else "🌍 Хориҷӣ",
                callback_data="citizen_foreign"
            )
        )
        return builder.as_markup()

    async def process_citizenship(self, callback: CallbackQuery, state: FSMContext):
        """Обработчик выбора гражданства."""
        await state.update_data(citizenship=callback.data)
        await state.set_state(Form.operator)
        data = await state.get_data()
        lang = data.get("lang", "ru")
        data = await state.get_data()
        await callback.message.edit_text(
            LOCALES[lang]["choose_operator"],
            reply_markup=self._operator_keyboard(lang, callback.data)
        )

    def _operator_keyboard(self, lang: str, citizen: str) -> InlineKeyboardMarkup:
        """Клавиатура для выбора оператора."""
        builder = InlineKeyboardBuilder()
        operators = ["megafon", "tele2", "mts", "biline", "yota", "sbermobile"] if citizen == "citizen_ru" else ["sbermobile"]
        for op in operators:
            if op in TARIFFS:
                if op == "sbermobile":
                    builder.button(
                        text=op.capitalize() + " | Бесплатное подключение",
                        callback_data=f"operator_{op}"
                    )
                else:
                    builder.button(
                        text=op.capitalize(),
                        callback_data=f"operator_{op}"
                    )
        builder.button(
            text=LOCALES[lang]["back"],
            callback_data="back:citizenship"
        )
        builder.adjust(2)
        return builder.as_markup()

    async def process_operator(self, callback: CallbackQuery, state: FSMContext):
        """Обработчик выбора оператора."""
        operator = callback.data.split("_")[1]
        await state.update_data(operator=operator)
        await state.set_state(Form.tariff)
        data = await state.get_data()
        lang = data.get("lang", "ru")
        reply_markup = await self._tariff_keyboard(operator, lang)
        
        # Формируем текстовое сообщение
        text = LOCALES[lang]["choose_tariff"].format(oper=operator.capitalize())
        try:
            # Получаем список всех картинок оператора
            operator_images = [
                img for img in os.listdir(Config.IMAGES_DIR)
                if img.startswith(f"operator_{operator}") and img.endswith((".jpg", ".jpeg", ".png"))
            ]
            
            if operator_images:
                # Создаём медиагруппу
                media_group = []
                for img in operator_images:
                    image_path = os.path.join(Config.IMAGES_DIR, img)
                    media_group.append(types.InputMediaPhoto(
                        media=FSInputFile(image_path)
                    ))
                
                await callback.message.delete()
                
                # Отправляем медиагруппу
                await callback.message.answer_media_group(media_group)
                
                # Отправляем текстовое сообщение с клавиатурой
                await callback.message.answer(text, reply_markup=reply_markup)
            else:
                # Если картинок нет, отправляем только текстовое сообщение
                logging.warning(f"Картинки оператора не найдены: {operator}")
                await callback.message.answer(text, reply_markup=reply_markup)
        except Exception as e:
            logging.error(f"Ошибка при отправке картинок оператора: {e}")
            await callback.message.answer(text, reply_markup=reply_markup)

    async def _tariff_keyboard(self, operator: str, lang: str) -> InlineKeyboardMarkup:
        """Клавиатура для выбора тарифа."""
        builder = InlineKeyboardBuilder()
        for tariff in TARIFFS.get(operator, []):
            if not await self.code_manager.check_code_exists(tariff["activation_key_path"]):
                continue
            builder.button(
                text=f"{tariff['price']}₽ | {tariff['name']}",
                callback_data=f"tariff_{tariff['id']}"
            )
        builder.button(
            text=LOCALES[lang]["back"],
            callback_data="back:operator"
        )
        builder.adjust(1)
        return builder.as_markup()

    async def process_back(self, callback: CallbackQuery, state: FSMContext):
        """Обработчик возврата к предыдущему шагу."""
        data = await state.get_data()
        lang = data.get("lang", "ru")
        try:
            target_state = callback.data.split(":")[1]
            new_state = None
            text = ""
            keyboard = None

            if target_state == "language":
                new_state = Form.language
                text = LOCALES[lang]["choose_language"]
                keyboard = self._language_keyboard()
            elif target_state == "welcome":
                new_state = Form.welcome
                text = LOCALES[lang]["welcome"]
                keyboard = self._welcome_keyboard(lang)
            elif target_state == "citizenship":
                new_state = Form.citizenship
                text = LOCALES[lang]["choose_citizenship"]
                keyboard = self._citizenship_keyboard(lang)
            elif target_state == "operator":
                new_state = Form.operator
                text = LOCALES[lang]["choose_operator"]
                keyboard = self._operator_keyboard(lang, data.get('citizenship', 'citizen_ru'))
            elif target_state == "tariff":
                new_state = Form.tariff
                text = LOCALES[lang]["choose_tariff"].format(oper=data.get("operator", "mts").capitalize())
                keyboard = await self._tariff_keyboard(data.get("operator", "mts"), lang)
            elif target_state == "instruction":
                new_state = Form.instruction
                if data.get("operator", "sbermobile") == 'sbermobile':
                    text = INSTRUCTIONS[data.get('citizenship', 'citizen_ru')][lang]['sbermobile']
                else:
                    text = INSTRUCTIONS[data.get('citizenship', 'citizen_ru')][lang]['text']
                keyboard = self._instruction_keyboard(lang)
            elif target_state == "payment":
                # Проверяем, была ли уже подтверждена оплата
                if await state.get_state() == Form.payment_confirmed:
                    await callback.answer("Оплата уже подтверждена, код уже был выдан.", show_alert=True)
                    return
                new_state = Form.payment
                text = LOCALES[lang]["payment"]
                keyboard = self._payment_keyboard("", lang, "")
            else:
                await callback.answer("Невозможно вернуться")
                return

            await state.set_state(new_state)
            await callback.message.delete()
            await callback.message.answer(text, reply_markup=keyboard)
        except Exception as e:
            logging.error(f"Error in process_back: {e}")
            await callback.answer("Произошла ошибка при возврате")

    async def select_tariff(self, callback: CallbackQuery, state: FSMContext):
        """Обработчик выбора тарифа."""
        await state.set_state(Form.citizenship)
        data = await state.get_data()
        lang = data.get("lang", "ru")
        await callback.message.answer(
            LOCALES[lang]["choose_citizenship"],
            reply_markup=self._citizenship_keyboard(lang)
        )

    async def process_tariff(self, callback: CallbackQuery, state: FSMContext):
        """Обработчик выбора тарифа."""
        tariff_id = callback.data.split("_")[1]
        data = await state.get_data()
        operator = data.get('operator', 'mts')
        lang = data.get('lang', 'ru')
        tariff = next(t for t in TARIFFS[operator] if t["id"] == tariff_id)
        await state.update_data(tariff=tariff)
        
        # Формируем текстовое описание тарифа
        description_text = LOCALES[lang]["tariff_disc"].format(
            tariff_name=tariff.get('name'),
            tariff_price=tariff.get('price'),
            tariff_discription=tariff.get('description')
        )
        callback.message.delete()
        try:
            # Проверяем, есть ли картинка тарифа
            if 'pic' in tariff:
                image_path = os.path.join(Config.IMAGES_DIR, tariff['pic'])
                if os.path.exists(image_path):
                    await callback.message.delete()
                    await callback.message.answer_photo(
                        FSInputFile(image_path),
                        caption=description_text,
                        reply_markup=self._sber_tariff_details_keyboard(lang) if operator == "sbermobile" else self._tariff_details_keyboard(lang)
                    )
                else:
                    # Если картинка не найдена, отправляем только текст
                    logging.warning(f"Картинка тарифа не найдена: {image_path}")
                    await callback.message.edit_text(
                        description_text,
                        reply_markup=self._sber_tariff_details_keyboard(lang) if operator == "sbermobile" else self._tariff_details_keyboard(lang)
                    )
            else:
                # Если картинки нет, отправляем только текст
                await callback.message.edit_text(
                    description_text,
                    reply_markup=self._sber_tariff_details_keyboard(lang) if operator == "sbermobile" else self._tariff_details_keyboard(lang)
                )
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения: {e}")
            await callback.answer("Произошла ошибка", show_alert=True)
        
        await state.set_state(Form.instruction)

    def _tariff_details_keyboard(self, lang: str) -> InlineKeyboardMarkup:
        """Клавиатура для деталей тарифа."""
        builder = InlineKeyboardBuilder()
        builder.button(
            text=LOCALES[lang].get("accept_terms", "✅ 199р"),
            callback_data="show_instructions"
        )
        builder.button(
            text=LOCALES[lang].get("free", "🤑 Хочу бесплатно"),
            callback_data="operator_sbermobile"
        )
        builder.button(
            text=LOCALES[lang]["back"],
            callback_data="back:tariff"
        )
        builder.adjust(1)
        return builder.as_markup()
    
    def _sber_tariff_details_keyboard(self, lang: str) -> InlineKeyboardMarkup:
        """Клавиатура для деталей тарифа Sbermobile."""
        builder = InlineKeyboardBuilder()
        builder.button(
            text=LOCALES[lang].get("promo", "✅ Получить бесплатно"),
            callback_data="show_instructions"
        )
        builder.button(
            text=LOCALES[lang]["back"],
            callback_data="back:tariff"
        )
        builder.adjust(1)
        return builder.as_markup()

    def _instruction_keyboard(self, lang: str) -> InlineKeyboardMarkup:
        """Клавиатура для инструкций."""
        builder = InlineKeyboardBuilder()
        builder.button(
            text=LOCALES[lang].get("accept_terms", "✅ 199р"),
            callback_data="show_instructions"
        )
        builder.button(
            text=LOCALES[lang]["back"],
            callback_data="back:tariff"
        )
        builder.adjust(1)
        return builder.as_markup()

    async def process_instruction(self, callback: CallbackQuery, state: FSMContext):
        """Обработчик показа инструкций."""
        data = await state.get_data()
        tariff = data.get("tariff")
        oper = data.get("operator", 'mts')
        lang = data.get('lang', 'ru')
        image_path = os.path.join(Config.IMAGES_DIR, tariff.get("image"))
        link_app = ""
        offices = ""
        if oper == "megafon":
            link_app = "https://moscow.megafon.ru/help/lk/"
            offices = " https://megafon.ru/help/offices/ в фильтрах выберите \"Замена SIM другого региона\""
        elif oper == "tele2":
            link_app = "https://msk.t2.ru/promo/mytele2"
            offices = "https://t2.ru/offices в фильтрах выберите \"Обслуживание корпоративных клиентов\""
        elif oper == "mts":
            link_app = "https://mymts.ru/"
            offices = "https://mts.ru/personal/podderzhka/zoni-obsluzhivaniya/offices/ в фильтрах выберите \"Подключиться к МТС и управлять своим тарифом\""
        if oper == "sbermobile":
            text = "sbermobile"
        else:
            text = "text"
        try:
            if os.path.exists(image_path):
                await callback.message.delete()
                await callback.message.answer_photo(
                    FSInputFile(image_path),
                    caption=INSTRUCTIONS[data.get('citizenship', 'citizen_ru')][lang][text] + "\n\nСкачайте приложение оператора\n" +  link_app + "\nНайдите ближайший офис\n" + offices,
                    reply_markup=self._payment_instruction_keyboard(lang, oper)
                )
            else:
                await callback.message.edit_text(INSTRUCTIONS[data.get('citizenship', 'citizen_ru')][lang][text] + "\n\nСкачайте приложение оператора\n" +  link_app + "\nНайдите ближайший офис\n" + offices, reply_markup=self._payment_instruction_keyboard(lang, oper))
            
        except Exception as e:
            logging.error(f"Ошибка отправки фото: {e}")
            await callback.answer("Произошла ошибка", show_alert=True)
        await state.set_state(Form.payment)

    def _payment_instruction_keyboard(self, lang: str, oper: str) -> InlineKeyboardMarkup:
        """Клавиатура для инструкций по оплате."""
        builder = InlineKeyboardBuilder()
        if oper == "sbermobile":
            builder.button(
                text=LOCALES[lang]['promo'],
                callback_data="confirm_payment"
            )
        else:
            builder.button(
                text=LOCALES[lang]['pay_button'],
                callback_data="proceed_to_payment"
            )
        builder.button(
            text=LOCALES[lang]["back"],
            callback_data="back:tariff"
        )
        builder.adjust(1)
        return builder.as_markup()

    async def process_proceed_payment(self, callback: CallbackQuery, state: FSMContext):
        """Обработчик перехода к оплате."""
        data = await state.get_data()
        tariff = data.get("tariff")
        user = callback.from_user
        try:
            me = await self.bot.get_me()
            payment = await PaymentManager.create_payment(
                amount=199,
                description=f"Оплата тарифа {tariff['name']}",
                bot_username=me.username,
                user_id=user.id
            )
            await state.update_data(
                payment_id=payment.id,
                tariff_id=tariff["id"],
                user_id=user.id
            )
            await callback.message.edit_reply_markup(
                reply_markup=self._payment_keyboard(
                    payment.confirmation.confirmation_url,
                    data.get('lang', 'ru'),
                    tariff["id"]
                )
            )
            await state.set_state(Form.payment)
        except Exception as e:
            logging.error(f"Payment creation error: {str(e)}")
            await callback.answer("Ошибка создания платежа", show_alert=True)
    
    def _payment_keyboard(self, payment_link: str, lang: str, tariff_id: str) -> InlineKeyboardMarkup:
        """Клавиатура для оплаты."""
        builder = InlineKeyboardBuilder()
        builder.button(
            text="💳 Оплатить" if lang == "ru" else "💳 To'lov" if lang == "uz" else "💳 Пардохт",
            url=payment_link
        )
        builder.button(
            text="✅ Проверить оплату" if lang == "ru" else "✅ To'lovni tekshirish" if lang == "uz" else "✅ Пардохтро тафтиш кунед",
            callback_data="confirm_payment"
        )
        builder.button(
            text=LOCALES[lang]["back"],
            callback_data="back:tariff"
        )
        builder.adjust(1)
        return builder.as_markup()
    
    async def process_payment(self, callback: CallbackQuery, state: FSMContext):
        """Обработчик подтверждения оплаты."""
        data = await state.get_data()
        lang = data.get("lang", "ru")
        tariff = data.get("tariff")

        # Проверяем, была ли уже подтверждена оплата
        if await state.get_state() == Form.payment_confirmed:
            await callback.answer("Оплата уже подтверждена, код уже был выдан.", show_alert=True)
            return

        try:
            if data.get("operator", "megafon") != "sbermobile":
                payment = Payment.find_one(data["payment_id"])
                if payment.status != "succeeded":
                    await callback.answer("Платеж не подтвержден", show_alert=True)
                    return
                if not tariff:
                    raise ValueError("Tariff data not found")
                codeM = await self.code_manager.get_code(tariff["activation_key_path"])
                code = codeM.split(":")[0]
                number = codeM.split(":")[1]
            else:
                if data.get("citizenship", "citizen_ru") == "citizen_ru":
                    code = "good51"
                else:
                    code = "good52"
                number = "❌"
            if code:
                await callback.message.edit_reply_markup(
                    reply_markup=None
                )
                await self._update_statistics(tariff["id"], data.get("operator", "mts"), callback.from_user.id)
                full_text = LOCALES[lang]["payment_success"].format(
                    code=code,
                    number=number,
                    tariff_name=tariff['name'],
                    tariff_price=tariff['price']
                )
                await callback.message.answer(
                    full_text,
                    parse_mode="HTML",
                    reply_markup=self._restart_keyboard(lang)
                )
                if data.get("operator", "megafon") == "tele2":
                    if lang == 'tj':
                        suptext = "Барои фаъолсозии пурраи рақам ба дастгирӣ нависед (@stan359)"
                    elif lang == "uz":
                        suptext = "Raqamni to'liq faollashtirish uchun (@stan359) qo'llab-quvvatlang"
                    else:
                        suptext = "Напишите в поддержку (@stan359) для полной активации номера"
                    await callback.message.answer(suptext)
                    await self.bot.send_message(chat_id="693411987", text=f"Куплен номер теле2.\nКод: {code}\nНомер: {number}")
            else:
                error_text = LOCALES[lang].get("no_codes_error", "❌ Коды закончились")
                await callback.answer(error_text, show_alert=True)
                await self._show_tariff_list(callback, data)
        except Exception as e:
            logging.error(f"Payment processing error: {e}")
            error_text = LOCALES[lang].get("payment_error", "⛔ Ошибка обработки платежа")
            await callback.answer(error_text, show_alert=True)

        # Устанавливаем состояние, что оплата подтверждена
        await state.set_state(Form.payment_confirmed)

    def _restart_keyboard(self, lang: str) -> InlineKeyboardMarkup:
        """Клавиатура для перезапуска."""
        builder = InlineKeyboardBuilder()
        builder.button(
            text=LOCALES[lang]['restart'],
            callback_data="back:tariff"
        )
        builder.adjust(1)
        return builder.as_markup()

    async def _show_tariff_list(self, callback: CallbackQuery, data: dict):
        """Показывает список тарифов."""
        await callback.message.edit_text(
            LOCALES[data.get("lang", "ru")]["choose_tariff"].format(oper=data.get('operator', 'mts').capitalize()),
            reply_markup=await self._tariff_keyboard(
                data.get('operator', 'mts'), 
                data.get('lang', 'ru')
            )
        )

    async def _update_statistics(self, tariff_id: str, operator: str, user_id: int):
        """Обновляет статистику покупок."""
        try:
            if not os.path.exists(self.statistics_file):
                stats = {}
            else:
                async with aiofiles.open(self.statistics_file, "r") as f:
                    content = await f.read()
                    stats = json.loads(content) if content else {}

            if tariff_id not in stats:
                stats[tariff_id] = {
                    "operator": operator,
                    "purchase_count": 0,
                    "users": []
                }

            stats[tariff_id]["purchase_count"] += 1
            if user_id not in stats[tariff_id]["users"]:
                stats[tariff_id]["users"].append(user_id)

            async with aiofiles.open(self.statistics_file, "w") as f:
                await f.write(json.dumps(stats, indent=2))

        except Exception as e:
            logging.error(f"Ошибка обновления статистики: {e}")
    
    async def show_stats(self, message: types.Message):
        """Показывает статистику покупок администратору."""
        if str(message.chat.id) not in Config.ADMINS:
            await message.answer("У вас нет доступа к этой команде.")
            return

        statistics = await self.get_statistics()
        if not statistics:
            await message.answer("Статистика покупок отсутствует.")
            return

        # Формируем текстовое сообщение со статистикой
        stats_text = "📊 Статистика покупок:\n\n"
        for tariff_id, data in statistics.items():
            stats_text += (
                f"Тариф: {tariff_id}\n"
                f"Оператор: {data['operator']}\n"
                f"Количество покупок: {data['purchase_count']}\n"
                f"Пользователи: {len(data['users'])}\n\n"
            )

        # Получаем самый популярный тариф
        most_popular = await self.get_most_popular_tariff()
        if most_popular:
            stats_text += f"Самый популярный тариф: {most_popular[0]} (покупок: {most_popular[1]['purchase_count']})"

        await message.answer(stats_text)

    async def get_statistics(self) -> Dict:
        """Возвращает статистику покупок."""
        try:
            if os.path.exists(self.statistics_file):
                with open(self.statistics_file, "r", encoding="utf-8") as f:
                    statistics = json.load(f)
                return statistics
            else:
                return {}
        except Exception as e:
            logging.error(f"Ошибка при загрузке статистики: {e}")
            return {}

    async def get_most_popular_tariff(self) -> Optional[Tuple[str, Dict]]:
        """Возвращает самый популярный тариф."""
        statistics = await self.get_statistics()
        if not statistics:
            return None

        most_popular_tariff_id = max(
            statistics.keys(),
            key=lambda x: statistics[x]["purchase_count"]
        )
        return most_popular_tariff_id, statistics[most_popular_tariff_id]

# Запуск бота
async def main():
    bot_instance = TariffBot()
    await bot_instance.dp.start_polling(bot_instance.bot)

if __name__ == "__main__":
    asyncio.run(main())