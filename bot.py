Да, это отличная идея. Создание отдельного чата для уведомлений — это стандартная практика («Чат мониторинга» или «Чат администрации»).

Я подготовил для вас обновленный код, в котором:

Добавлены новые админы (Альбина и Элиночка). Теперь они тоже могут скачивать табель и очищать базу.

Настроена отправка уведомлений в общий чат.

Добавлена команда /chatid, чтобы вы легко узнали ID вашей группы.

⚙️ Инструкция по установке (3 шага)
Шаг 1. Создайте группу в Telegram

Создайте новую группу (назовите её, например, «Термы: Контроль»).

Добавьте туда:

Себя.

Альбину.

Элиночку.

Вашего бота (найдите его через поиск и добавьте как участника).

Сделайте бота Администратором группы (это нужно, чтобы он мог писать сообщения без ограничений).

Шаг 2. Обновите код bot.py

Скопируйте код ниже целиком и замените им файл bot.py на GitHub.

В этом коде я уже прописал ID Альбины и Элиночки в список админов. Вам останется только вписать ID группы (см. Шаг 3).

code
Python
download
content_copy
expand_less
import logging
import os
import re
import pandas as pd
import psycopg2
import pytz
from datetime import datetime, time, timedelta
from dotenv import load_dotenv

# Telegram и Календарь
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    CallbackQueryHandler,
    filters,
)
from telegram_bot_calendar import DetailedTelegramCalendar, LSTEP

# Для Excel
from openpyxl.styles import Font, Alignment, Border, Side

# Загрузка настроек
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# --- НАСТРОЙКИ АДМИНОВ И ЧАТОВ ---

# 1. Список ID всех админов (Кому можно делать /export и /clear)
ADMIN_LIST = [
    617492397,  # Азат (Ваш ID)
    802271920,  # Альбина
    806725469   # Элиночка
]

# 2. ID ГРУППЫ для уведомлений об опозданиях
# Сначала запустите бота, напишите в группу команду /chatid, узнайте номер и впишите сюда!
# Номер должен быть с минусом, например: -100123456789
ALARM_CHAT_ID = None  # <--- ВПИСАТЬ СЮДА ID ГРУППЫ (позже)

# 3. Часовой пояс и Граница опоздания
TIMEZONE = pytz.timezone('Asia/Yekaterinburg') 
LATE_LIMIT = time(9, 16) # Опоздание после 09:16

# Логи
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Справочник отделов
DEPT_MAP = {
    "rescue": "🆘 Спасатели",
    "lockers": "🔐 Локеры",
    "admin": "👨‍💻 Админ.",
    "tech": "🔧 Тех. отдел"
}
DEPT_REVERSE_MAP = {v: k for k, v in DEPT_MAP.items()}

# Клавиатура
buttons_list = list(DEPT_MAP.values())
dept_rows = [buttons_list[i:i + 2] for i in range(0, len(buttons_list), 2)]
DEPT_KEYBOARD = ReplyKeyboardMarkup(dept_rows, resize_keyboard=True, one_time_keyboard=True)

MAIN_MENU_KEYBOARD = ReplyKeyboardMarkup(
    [["👋 Приход", "🏁 Уход"], ["👤 Мое ФИО"]], 
    resize_keyboard=True, one_time_keyboard=False
)

# Состояния
REGISTER_NAME, SELECT_DATE, DEPARTMENT, TIME_INPUT = range(4)

# --- БАЗА ДАННЫХ ---
def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS records (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            full_name TEXT,
            date TEXT,
            department TEXT,
            check_in TEXT,
            check_out TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            real_name TEXT
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()

def get_user_name(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT real_name FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else None

def register_user_db(user_id, real_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO users (user_id, real_name) 
        VALUES (%s, %s)
        ON CONFLICT (user_id) 
        DO UPDATE SET real_name = EXCLUDED.real_name;
    """, (user_id, real_name))
    conn.commit()
    cursor.close()
    conn.close()

def clear_all_records():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM records") 
    conn.commit()
    cursor.close()
    conn.close()

def save_check_in(user_id, date_str, dept_code, time_str):
    real_name = get_user_name(user_id) or "Неизвестный"
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM records WHERE user_id = %s AND date = %s", (user_id, date_str))
    existing = cursor.fetchone()
    if existing:
        cursor.execute('UPDATE records SET check_in = %s, department = %s, full_name = %s WHERE id = %s', 
                       (time_str, dept_code, real_name, existing[0]))
        status = "updated"
    else:
        cursor.execute('INSERT INTO records (user_id, full_name, date, department, check_in) VALUES (%s, %s, %s, %s, %s)', 
                       (user_id, real_name, date_str, dept_code, time_str))
        status = "created"
    conn.commit(); cursor.close(); conn.close()
    return status

def save_check_out(user_id, selected_date_str, time_str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE records SET check_out = %s WHERE user_id = %s AND date = %s', (time_str, user_id, selected_date_str))
    if cursor.rowcount > 0:
        conn.commit(); cursor.close(); conn.close()
        return True, selected_date_str
    try:
        dt = datetime.strptime(selected_date_str, "%Y-%m-%d")
        prev_date_str = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
        cursor.execute('UPDATE records SET check_out = %s WHERE user_id = %s AND date = %s AND check_out IS NULL', (time_str, user_id, prev_date_str))
        if cursor.rowcount > 0:
            conn.commit(); cursor.close(); conn.close()
            return True, prev_date_str
    except Exception: pass
    cursor.close(); conn.close()
    return False, None

def validate_time_format(time_text):
    return re.match(r"^([01]\d|2[0-3]):([0-5]\d)$", time_text) is not None

def generate_timesheet():
    conn = get_db_connection()
    try:
        df = pd.read_sql_query("SELECT * FROM records", conn)
    except Exception: return None
    finally: conn.close()
    if df.empty: return None

    df['department'] = df['department'].map(DEPT_MAP).fillna(df['department'])
    def calc_hours(row):
        try:
            if not row['check_in'] or not row['check_out']: return 0
            t1 = datetime.strptime(row['check_in'], "%H:%M")
            t2 = datetime.strptime(row['check_out'], "%H:%M")
            if t2 < t1: t2 += timedelta(days=1)
            raw_hours = (t2 - t1).total_seconds() / 3600
            return round(max(0, raw_hours - 1.0), 2)
        except: return 0

    df['worked_hours'] = df.apply(calc_hours, axis=1)
    df['dt_obj'] = pd.to_datetime(df['date'], dayfirst=False, errors='coerce')
    mask = df['dt_obj'].isna()
    if mask.any(): df.loc[mask, 'dt_obj'] = pd.to_datetime(df.loc[mask, 'date'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['dt_obj'])
    df['day'] = df['dt_obj'].dt.day
    
    pivot = df.pivot_table(index=['department', 'full_name'], columns='day', values='worked_hours', aggfunc='sum').fillna(0)
    pivot['ИТОГО'] = pivot.sum(axis=1)

    filename = f"Tabel_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        pivot.to_excel(writer, sheet_name='Табель')
        worksheet = writer.sheets['Табель']
        thin = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        for row in worksheet.iter_rows():
            for cell in row:
                cell.border = thin
                cell.alignment = Alignment(horizontal='center', vertical='center')
        worksheet.column_dimensions['A'].width = 20
        worksheet.column_dimensions['B'].width = 30
    return filename

# --- ЛОГИКА БОТА ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if get_user_name(update.effective_user.id):
        await update.message.reply_text("👋 Меню:", reply_markup=MAIN_MENU_KEYBOARD)
        return ConversationHandler.END
    welcome = (
        "👋 <b>Добро пожаловать в электронный табель «Термы»!</b>\n\n"
        "ℹ️ <b>Важные правила:</b>\n"
        "1. Время указывайте <b>строго по графику</b> (например, 09:00).\n"
        "2. Программа автоматически вычитает <b>1 час</b> на обед.\n\n"
        "🚀 <b>Для начала регистрации напишите вашу Фамилию и Имя:</b>"
    )
    await update.message.reply_text(welcome, parse_mode='HTML', reply_markup=ReplyKeyboardRemove())
    return REGISTER_NAME

async def receive_registration_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if len(name) < 3:
        await update.message.reply_text("Введите ФИО полностью!")
        return REGISTER_NAME
    register_user_db(update.effective_user.id, name)
    await update.message.reply_text(f"✅ Сохранено: {name}", reply_markup=MAIN_MENU_KEYBOARD)
    return ConversationHandler.END

async def my_name_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = get_user_name(update.effective_user.id)
    msg = f"👤 Вы: <b>{name}</b>" if name else "⚠️ Вы не зарегистрированы. Нажмите /start"
    await update.message.reply_text(msg, parse_mode='HTML')

# --- КОМАНДА ДЛЯ ПОЛУЧЕНИЯ ID ГРУППЫ ---
async def get_chat_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(f"🆔 ID этого чата: <code>{chat_id}</code>", parse_mode='HTML')

async def clear_db_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_LIST:
        await update.message.reply_text("⛔️ У вас нет прав админа.")
        return
    clear_all_records()
    await update.message.reply_text("🗑 <b>База (PostgreSQL) очищена!</b>", parse_mode='HTML')

async def start_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not get_user_name(update.effective_user.id):
        await update.message.reply_text("⚠️ Сначала /start")
        return ConversationHandler.END
    calendar, step = DetailedTelegramCalendar(calendar_id=1, locale='ru').build()
    await update.message.reply_text("📅 Дата прихода:", reply_markup=calendar)
    context.user_data['action'] = 'in'
    return SELECT_DATE

async def start_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not get_user_name(update.effective_user.id):
        await update.message.reply_text("⚠️ Сначала /start")
        return ConversationHandler.END
    calendar, step = DetailedTelegramCalendar(calendar_id=2, locale='ru').build()
    await update.message.reply_text("📅 Дата ухода:", reply_markup=calendar)
    context.user_data['action'] = 'out'
    return SELECT_DATE

async def calendar_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cal_id = 1 if context.user_data.get('action') == 'in' else 2
    result, key, step = DetailedTelegramCalendar(calendar_id=cal_id, locale='ru').process(query.data)
    
    if not result and key:
        await query.edit_message_text(f"Выберите {LSTEP[step]}", reply_markup=key)
        return SELECT_DATE
    elif result:
        date_str = result.strftime("%Y-%m-%d")
        context.user_data['date'] = date_str
        await query.edit_message_text(f"🗓 Дата: {result.strftime('%d.%m.%Y')}")
        if context.user_data['action'] == 'in':
            await context.bot.send_message(query.message.chat_id, "🏢 Выберите отдел:", reply_markup=DEPT_KEYBOARD)
            return DEPARTMENT
        else:
            await context.bot.send_message(query.message.chat_id, "🕒 Время ухода (чч:мм):", reply_markup=ReplyKeyboardRemove())
            return TIME_INPUT

async def receive_department(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dept_code = DEPT_REVERSE_MAP.get(update.message.text)
    if not dept_code:
        await update.message.reply_text("⚠️ Выберите отдел кнопкой!", reply_markup=DEPT_KEYBOARD)
        return DEPARTMENT
    context.user_data['dept'] = dept_code
    await update.message.reply_text("🕒 Время прихода (чч:мм):", reply_markup=ReplyKeyboardRemove())
    return TIME_INPUT

async def receive_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    time_str = update.message.text.strip()
    if not validate_time_format(time_str):
        await update.message.reply_text("⚠️ Введите время в формате <b>чч:мм</b> (09:00).", parse_mode='HTML')
        return TIME_INPUT
    
    data = context.user_data
    user_id = update.effective_user.id
    
    if data['action'] == 'in':
        # --- ПРОВЕРКА ОПОЗДАНИЙ ---
        try:
            now_local = datetime.now(TIMEZONE)
            if data['date'] == now_local.strftime("%Y-%m-%d"):
                if LATE_LIMIT < now_local.time() < time(12, 0):
                    user_name = get_user_name(user_id)
                    alert_text = (
                        f"⚠️ <b>ОПОЗДАНИЕ!</b>\n👤 {user_name}\n"
                        f"📅 {data['date']}\n⏰ Факт: <b>{now_local.strftime('%H:%M')}</b>\n✍️ Вписал: {time_str}"
                    )
                    # Отправляем в ОБЩИЙ ЧАТ, если он настроен
                    if ALARM_CHAT_ID:
                        await context.bot.send_message(chat_id=ALARM_CHAT_ID, text=alert_text, parse_mode='HTML')
                    # Иначе отправляем всем админам в личку
                    else:
                        for admin_id in ADMIN_LIST:
                            try:
                                await context.bot.send_message(chat_id=admin_id, text=alert_text, parse_mode='HTML')
                            except: pass
        except Exception as e: logger.error(f"Alert Error: {e}")

        status = save_check_in(user_id, data['date'], data['dept'], time_str)
        dept_name = DEPT_MAP.get(data['dept'], data['dept'])
        msg = f"✅ <b>Приход:</b> {data['date']}\n🏢 {dept_name}\n🕘 {time_str}" if status == "created" else f"🔄 <b>Обновлено:</b> {data['date']}\n🏢 {dept_name}\n🕘 {time_str}"
        await update.message.reply_text(msg, parse_mode='HTML', reply_markup=MAIN_MENU_KEYBOARD)
    else:
        success, date_closed = save_check_out(user_id, data['date'], time_str)
        if success:
            msg = f"🏁 <b>Уход:</b> {date_closed} | {time_str}" + (f"\n(Закрыта смена за {date_closed})" if date_closed != data['date'] else "")
            await update.message.reply_text(msg, parse_mode='HTML', reply_markup=MAIN_MENU_KEYBOARD)
        else:
            await update.message.reply_text("⚠️ Не найдено открытых смен.", reply_markup=MAIN_MENU_KEYBOARD)
            
    context.user_data.clear()
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Отмена", reply_markup=MAIN_MENU_KEYBOARD)
    context.user_data.clear()
    return ConversationHandler.END

async def send_report_job(context: ContextTypes.DEFAULT_TYPE):
    filename = generate_timesheet()
    if not filename: return
    # Отправляем отчет в ГРУППУ, если она есть
    if ALARM_CHAT_ID:
        try:
            await context.bot.send_message(ALARM_CHAT_ID, "📊 Ежедневный табель")
            await context.bot.send_document(ALARM_CHAT_ID, open(filename, 'rb'))
        except: pass
    else:
        # Иначе отправляем всем админам
        for admin_id in ADMIN_LIST:
            try:
                await context.bot.send_message(admin_id, "📊 Табель")
                await context.bot.send_document(admin_id, open(filename, 'rb'))
            except: pass

async def manual_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id in ADMIN_LIST:
        await send_report_job(context)

if __name__ == '__main__':
    if not DATABASE_URL: print("ОШИБКА: Не задан DATABASE_URL в .env")
    else:
        init_db()
        application = ApplicationBuilder().token(TOKEN).build()
        conv_handler_kwargs = {'allow_reentry': True}
        
        conv_reg = ConversationHandler(
            entry_points=[CommandHandler('start', start_command)],
            states={REGISTER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_registration_name)]},
            fallbacks=[], **conv_handler_kwargs
        )
        conv_in = ConversationHandler(
            entry_points=[CommandHandler('checkin', start_checkin), MessageHandler(filters.Regex("^👋 Приход$"), start_checkin)],
            states={
                SELECT_DATE: [CallbackQueryHandler(calendar_handler, pattern="^cbcal_")],
                DEPARTMENT: [MessageHandler(filters.TEXT, receive_department)],
                TIME_INPUT: [MessageHandler(filters.TEXT, receive_time)]
            },
            fallbacks=[CommandHandler('cancel', cancel)], **conv_handler_kwargs
        )
        conv_out = ConversationHandler(
            entry_points=[CommandHandler('checkout', start_checkout), MessageHandler(filters.Regex("^🏁 Уход$"), start_checkout)],
            states={
                SELECT_DATE: [CallbackQueryHandler(calendar_handler, pattern="^cbcal_")],
                TIME_INPUT: [MessageHandler(filters.TEXT, receive_time)]
            },
            fallbacks=[CommandHandler('cancel', cancel)], **conv_handler_kwargs
        )
        
        application.add_handler(conv_reg)
        application.add_handler(conv_in)
        application.add_handler(conv_out)
        application.add_handler(CommandHandler('export', manual_export))
        application.add_handler(CommandHandler('clear', clear_db_command))
        # НОВАЯ КОМАНДА ДЛЯ УЗНАВАНИЯ ID ЧАТА
        application.add_handler(CommandHandler('chatid', get_chat_id_command))
        application.add_handler(MessageHandler(filters.Regex("^👤 Мое ФИО$"), my_name_command))
        
        application.job_queue.run_daily(send_report_job, time=time(hour=23, minute=0), days=(6,))
        print("Бот (PRO: Multi-Admin + Chat Alarm) запущен!")
        application.run_polling()
Шаг 3. Узнаем ID группы и вписываем его (Финальный штрих)

Залейте код на GitHub и дождитесь, пока Koyeb обновится.

Зайдите в вашу новую группу «Термы: Контроль».

Напишите туда команду: /chatid

Бот ответит: 🆔 ID этого чата: -10012345678 (цифры будут другие).

Скопируйте этот номер (вместе с минусом!).

Идите на GitHub -> bot.py -> ✏️ Edit.

Вставьте этот номер в строку 43:

code
Python
download
content_copy
expand_less
ALARM_CHAT_ID = -10012345678  # <--- Вставьте сюда ваш номер с минусом!

Нажмите Commit changes.

Все!
Теперь опоздания и вечерний отчет будут приходить в эту группу, где сидят Альбина и Элиночка. И они тоже смогут нажать /export в личке с ботом, если захотят.
