import asyncio
import random
import logging
import threading
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv
import os
import json
import re
import atexit
from flask import Flask

load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')

logging.basicConfig(filename='bot.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

app = Flask(__name__)

@app.route('/')
def dashboard():
    global balances, stats, paused, pending_duels
    user_count = len(balances)
    active_duels = len([d for d in pending_duels.values() if isinstance(d, dict) and 'scores' in d])
    rtp = (stats['total_wins'] / stats['total_bets'] * 100) if stats['total_bets'] > 0 else 0
    return f"""
    <html>
    <head><title>Dep-Kazino Dashboard</title></head>
    <body>
        <h1>🚀 Bot Status</h1>
        <p>👥 Active Users: {user_count}</p>
        <p>💰 Total Bets: ${stats['total_bets']:,}</p>
        <p>🏆 Total Wins: ${stats['total_wins']:,}</p>
        <p>📈 RTP: {rtp:.1f}%</p>
        <p>⚔️ Active Duels: {active_duels}</p>
        <p>⏸️ Paused: {'Yes' if paused else 'No'}</p>
        <hr>
        <h2>Recent Logs</h2>
        <pre>{get_recent_logs()}</pre>
        <p><small>Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</small></p>
    </body>
    </html>
    """

def get_recent_logs():
    try:
        with open('bot.log', 'r') as f:
            lines = f.readlines()
            return ''.join(lines[-20:])  # Last 20 lines
    except:
        return "No logs available."

@app.route('/logs')
def full_logs():
    try:
        with open('bot.log', 'r') as f:
            return f'<pre>{f.read()}</pre>'
    except:
        return 'Logs not found.'

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

balances = {}
user_info = {}
banned_users = set()
pending_duels = {}
random_queue = []
last_daily = {}
stats = {'total_bets': 0, 'total_wins': 0}
feedbacks = []
paused = False

def load_data():
    global balances, user_info, banned_users, pending_duels, random_queue, last_daily, stats
    backup_created = False
    try:
        with open('data.json', 'r') as f:
            raw_data = f.read()
        data = json.loads(raw_data)
        # Clean and merge balances: take max for duplicates
        temp_bal = data.get('balances', {})
        balances = {}
        for k, v in temp_bal.items():
            try:
                uid = int(k)
                val = int(v) if isinstance(v, (int, float)) else 10000
                balances[uid] = max(balances.get(uid, 0), val)
            except:
                pass
        # Load and clean user_info safely
        user_info_raw = data.get('user_info', {})
        user_info = {}
        for uid_str, info in user_info_raw.items():
            try:
                uid = int(uid_str)
                if uid in user_info:
                    # Merge duplicates
                    user_info[uid]['name'] = info.get('name', user_info[uid].get('name', ''))
                    user_info[uid]['balance'] = info.get('balance', user_info[uid].get('balance', 10000))
                    user_info[uid]['registered'] = info.get('registered', True)
                else:
                    info_copy = info.copy()
                    info_copy['registered'] = info_copy.get('registered', True)
                    info_copy['balance'] = info_copy.get('balance', 10000)
                    user_info[uid] = info_copy
            except ValueError:
                pass
        # Sync balances from user_info if missing
        for uid, info in user_info.items():
            if uid not in balances:
                balances[uid] = info.get('balance', 10000)
                logging.info(f'Synced balance {balances[uid]} from user_info for {uid}')
        banned_users = set()
        for u_str in data.get('banned_users', []):
            try:
                banned_users.add(int(u_str))
            except ValueError:
                pass
        pending_duels_raw = data.get('pending_duels', {})
        pending_duels = {}
        for k_str, v in pending_duels_raw.items():
            try:
                k = int(k_str)
                # Ensure chat_id if not present
                if isinstance(v, dict) and 'chat_id' not in v:
                    v['chat_id'] = None
                pending_duels[k] = v
            except ValueError:
                pass
        random_queue = data.get('random_queue', [])
        stats = data.get('stats', {'total_bets': 0, 'total_wins': 0})
        feedbacks = data.get('feedbacks', [])
        last_daily_dict = data.get('last_daily', {})
        last_daily = {}
        for k_str, v in last_daily_dict.items():
            try:
                k = int(k_str)
                last_daily[k] = datetime.fromisoformat(str(v).replace('Z', '+00:00'))  # Handle timezone
            except:
                pass
        paused = data.get('paused', False)
        logging.info(f'Data loaded successfully: {len(balances)} users, total balance {sum(balances.values())}')
        logging.info(f'Loaded balances sample: {dict(list(balances.items())[:3])}')
        logging.info(f'Loaded user_info: {len(user_info)} users')
        logging.info(f'Loaded feedbacks: {len(feedbacks)}')
    except json.JSONDecodeError as e:
        logging.error(f'JSON decode error: {e}. Raw data preview: {raw_data[:200] if "raw_data" in locals() else "N/A"}')
        # Backup corrupt file
        try:
            with open('data_corrupt_backup.json', 'w') as f:
                f.write(raw_data)
            backup_created = True
        except:
            pass
        # Reset to defaults
        balances = {}
        user_info = {}
        banned_users = set()
        pending_duels = {}
        random_queue = []
        stats = {'total_bets': 0, 'total_wins': 0}
        last_daily = {}
        logging.warning('Reset to defaults due to corrupt JSON. Backup created if possible.')
    except Exception as e:
        logging.error(f'Unexpected load error: {e}')
        balances = {}
        user_info = {}
        banned_users = set()
        pending_duels = {}
        random_queue = []
        stats = {'total_bets': 0, 'total_wins': 0}
        last_daily = {}

def save_data():
    last_daily_dict = {str(k): v.isoformat() for k, v in last_daily.items()}
    data = {
        'balances': balances,
        'user_info': user_info,
        'banned_users': list(banned_users),
        'pending_duels': pending_duels,
        'random_queue': random_queue,
        'last_daily': last_daily_dict,
        'stats': stats,
        'feedbacks': feedbacks,
        'paused': paused
    }
    try:
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=2)  # Pretty print for readability
        logging.info(f'Data saved: {len(balances)} users, balances total {sum(balances.values())}')
    except Exception as e:
        logging.error(f'Failed to save data: {e}')

atexit.register(save_data)

load_data()


def get_balance(user_id):
    if user_id in banned_users:
        return 0
    # Ensure user in balances and user_info
    if user_id not in user_info:
        user_info[user_id] = {'name': '', 'registered': True, 'balance': 10000}
    if user_id not in balances:
        balances[user_id] = user_info[user_id].get('balance', 10000)
        logging.info(f'Restored balance {balances[user_id]} for existing user {user_id}')
        save_data()
    else:
        # Sync balance to user_info if missing
        if 'balance' not in user_info[user_id]:
            user_info[user_id]['balance'] = balances[user_id]
    return balances[user_id]

def update_balance(user_id, amount):
    if user_id not in banned_users:
        balances[user_id] = max(0, balances[user_id] + amount)
        if user_id not in user_info:
            user_info[user_id] = {'name': '', 'registered': True}
        user_info[user_id]['balance'] = balances[user_id]
        logging.info(f'Balance update for {user_id}: +{amount}')
        save_data()

def is_admin(user_id):
    try:
        with open('admins.txt', 'r') as f:
            admins = [int(line.strip()) for line in f if line.strip().isdigit()]
        return user_id in admins
    except:
        return False

def log_action(action, user_id, details=''):
    logging.info(f'{action} by {user_id}: {details}')



class GameStates(StatesGroup):
    waiting_bet = State()
    waiting_roulette_number = State()
    waiting_blackjack_bet = State()
    blackjack_playing = State()
    waiting_duel_opponent = State()
    waiting_duel_bet = State()
    duel_playing = State()
    waiting_number = State()
    waiting_team = State()
    admin_ban = State()
    admin_add_money = State()
    waiting_feedback = State()
    admin_wait_feedback_id = State()
    admin_wait_feedback_reply = State()
    waiting_poker_bet = State()
    waiting_sport_bet = State()
    waiting_pm_recipient = State()
    waiting_pm_message = State()
    waiting_broadcast = State()
    waiting_reset_id = State()

main_keyboard = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text='💰 Баланс'), KeyboardButton(text='🏠 Главное меню')],
    [KeyboardButton(text='📖 Правила'), KeyboardButton(text='🎰 Слоты')],
    [KeyboardButton(text='🎡 Рулетка'), KeyboardButton(text='♠️ Блэкджек')],
    [KeyboardButton(text='♦️ Покер'), KeyboardButton(text='⚽ Спорт')],
    [KeyboardButton(text='⚔️ Дуэль'), KeyboardButton(text='🎲 Рандом дуэль')],
    [KeyboardButton(text='🎁 Бонус'), KeyboardButton(text='ℹ️ Помощь')]
], resize_keyboard=True)

@dp.message(Command('start'))
async def start_handler(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id in banned_users:
        try:
            await message.answer('🚫 Вы заблокированы!', reply_markup=main_keyboard)
        except Exception as e:
            logging.error(f'Error sending ban message to {user_id}: {e}')
        return
    get_balance(user_id)  # Ensures user in balances if new
    if user_id not in user_info or not user_info[user_id].get('name'):
        user_info[user_id] = {'name': message.from_user.username or 'User', 'registered': True}
    balance = get_balance(user_id)
    text = f'🎉 Добро пожаловать в Деп-Казино! 🎰\n💵 Баланс: ${balance}\n👤 @{user_info[user_id]["name"]}\nВыберите игру:'
    try:
        await message.answer(text, reply_markup=main_keyboard)
        save_data()
    except Exception as e:
        logging.error(f'Error in start_handler for {user_id}: {e}')
    await state.clear()
    log_action('start', user_id)

@dp.message(F.text == '💰 Баланс')
async def balance_handler(message: Message):
    user_id = message.from_user.id
    get_balance(user_id)
    if user_id not in user_info or not user_info[user_id]['name']:
        user_info[user_id] = user_info.get(user_id, {})
        user_info[user_id]['name'] = message.from_user.username or 'User'
    balance = get_balance(user_id)
    text = f'💰 Ваш баланс: ${balance}\n👤 @{user_info[user_id]["name"]}'
    try:
        await message.answer(text, reply_markup=main_keyboard)
    except Exception as e:
        logging.error(f'Error in balance_handler for {user_id}: {e}')


@dp.message(Command('balance'))
async def balance_command(message: Message):
    user_id = message.from_user.id
    get_balance(user_id)
    if user_id not in user_info or not user_info[user_id]['name']:
        user_info[user_id] = user_info.get(user_id, {})
        user_info[user_id]['name'] = message.from_user.username or 'User'
    balance = get_balance(user_id)
    text = f'💰 Ваш баланс: ${balance}\n👤 @{user_info[user_id]["name"]}'
    try:
        await message.answer(text, reply_markup=main_keyboard)
    except Exception as e:
        logging.error(f'Error in balance_command for {user_id}: {e}')

@dp.message(F.text == '🏠 Главное меню')
async def main_menu(message: Message, state: FSMContext):
    try:
        await start_handler(message, state)
    except Exception as e:
        logging.error(f'Error in main_menu for {message.from_user.id}: {e}')
        # Fallback: send menu directly
        user_id = message.from_user.id
        get_balance(user_id)
        if user_id not in user_info or not user_info[user_id].get('name'):
            user_info[user_id]['name'] = message.from_user.username or 'User'
            if user_id not in user_info:
                user_info[user_id] = {'name': message.from_user.username or 'User', 'registered': True}
        balance = get_balance(user_id)
        text = f'🏠 Главное меню\n💵 Баланс: ${balance}\n👤 @{user_info[user_id]["name"]}\nВыберите игру:'
        await message.answer(text, reply_markup=main_keyboard)
        await state.clear()
        log_action('main_menu', user_id)


@dp.message(Command('menu'))
async def menu_command(message: Message, state: FSMContext):
    await start_handler(message, state)

@dp.message(F.text == 'ℹ️ Помощь')
async def help_handler(message: Message):
    help_text = """
🎰 **Добро пожаловать в Деп-Казино!** ✨

🔹 **Основные команды:**
   • `/start` или `/menu` - Главное меню с балансом
   • `/balance` - Ваш текущий баланс
   • `/bonus` - Ежедневный бонус +200$ (1 раз в сутки)

🔹 **Игры и азарт:**
   • 🎰 **Слоты** - Крутите барабаны! 50% шанс выигрыша до x20 ставки. Выберите сумму и удачи!
   • 🎡 **Рулетка** - Ставки на красное/чёрное (x2), чёт/нечёт (x2) или число (x18). Классика!
   • ♠️ **Блэкджек** - Собирите 21! Hit/Stand против дилера. Blackjack даёт x1.5!
   • ♦️ **Покер** - 5 карт, выигрыш по комбинациям (пара x2, фулл-хаус x5, стрейт-флэш x100+).
   • ⚽ **Спорт** - Ставка на команду A или B (x2 при победе). 50/50!
   • ⚔️ **Дуэль** - 1v1 с другом или рандомом! Слоты по очереди, выше счёт - выигрыш. Укажите @username или ID.
   • 🎲 **Рандом дуэль** - Очередь на случайного оппонента (ставка 100$).

🔹 **Дуэли:**
   - Вызовите друга: Ответьте на сообщение или введите @username/ID.
   - Рандом: Нажмите кнопку, ждите матча.
   - Видите имя оппонента, анонимность снята для честной игры!

🔹 **Админы & Поддержка:**
   • `/feedback` - Отправьте отзыв, админы ответят!
   • `/help` - Эта справка.
   • Боты виртуальные, играйте responsibly! 😊

**RTP ~96% во всех играх. Удачи и больших выигрышей!** 🍀
    """
    await message.answer(help_text, reply_markup=main_keyboard)


@dp.message(Command('help'))
async def help_command(message: Message):
    await help_handler(message)

@dp.message(F.text == '📖 Правила')
async def rules_handler(message: Message):
    rules_text = """
📖 **Правила игр в Деп-Казино** 🎰

🔹 **🎰 Слоты:**
   - Выберите ставку (10$+), крутите 3 барабана.
   - Выигрыш по комбинациям: Три одинаковых — множитель (7s x20, звёзды x15, BAR x10, фрукты x8), две x3. Без комбо: 50% шанс small win (1-5x), 50% полный проигрыш (0x, потеря ставки). RTP ~96%.

🔹 **🎡 Рулетка:**
   - Ставки: Красное/Чёрное/Чёт/Нечёт (x2), Число 0-36 (x18).
   - Выпадает число 0-36. 0 - зелёное, выигрывает только точное число.

🔹 **♠️ Блэкджек:**
   - Цель: набрать 21 или ближе к 21, чем дилер, без перебора.
   - Карты: 2-10 по номиналу, J/Q/K=10, A=1 или 11. Раздача по 2 карты.
   - Hit: взять карту, Stand: остановиться. Дилер берёт до 17.
   - Blackjack (A + 10/J/Q/K): x1.5 ставки. Перебор >21: проигрыш. Ничья: возврат ставки.
   - Дилер hit на soft 17, без split/double.

🔹 **♦️ Покер (5-карточный):**
   - Раздача 5 карт. Выигрыш по стандартным комбинациям.
   - Royal Flush: x50, Straight Flush: x25, Four of a Kind: x15, Full House: x10, Flush: x6, Straight: x5, Three of a Kind: x4, Two Pair: x3, Pair: x2, High Card: x1.
   - Нет обмена карт.

🔹 **⚽ Спорт:**
   - Ставки на команду A или B (x2), или Over/Under 2.5 голов (x1.8).
   - Для команд: исходы - победа A, B или ничья (возврат).
   - Для голов: симулируется общий счёт 0-6 голов, over если >2.

🔹 **⚔️ Дуэль / 🎲 Рандом дуэль:**
   - 1v1 на слотах: по очереди крутите, счёт по комбинациям (три 7s: 15, три одинаковые: 10, две: 4-7, случайные: 1-5).
   - Выше счёт выигрывает, ничья - возврат. Ставка минимум 10$, равная для обоих.
   - Рандом: очередь, автоматический матч (ставка 100$).
   - Отмена: /cancel или кнопка.

🔹 **Общие правила:**
   - Начальный баланс 10000$, ежедневный бонус +200$.
   - Блокировка и админ-функции доступны только администраторам.
   - Все игры виртуальные.
   - Отправить отзыв: /feedback.

Удачи!
    """
    await message.answer(rules_text, reply_markup=main_keyboard)

@dp.message(Command('rules'))
async def rules_command(message: Message):
    await rules_handler(message)

# Slots
@dp.message(F.text == '🎰 Слоты')
async def slots_menu(message: Message, state: FSMContext):
    global paused
    if paused:
        await message.answer('⏸️ Игры приостановлены администратором. Попробуйте позже.', reply_markup=main_keyboard)
        return
    if message.chat.type != 'private':
        await message.answer('❌ Азартные игры доступны только в личных сообщениях с ботом. Дуэли работают в группах!', reply_markup=main_keyboard)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💎 Low 50$', callback_data='slots_50')],
        [InlineKeyboardButton(text='💎 Med 200$', callback_data='slots_200')],
        [InlineKeyboardButton(text='💎 High 500$', callback_data='slots_500')],
        [InlineKeyboardButton(text='🎯 Custom', callback_data='slots_custom')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    await message.answer('🎰 Выберите ставку для слотов:', reply_markup=keyboard)
    await state.set_state(GameStates.waiting_bet)


@dp.message(Command('slots'))
async def slots_command(message: Message, state: FSMContext):
    await slots_menu(message, state)

@dp.message(F.text == '⚽ Спорт')
async def sport_menu(message: Message, state: FSMContext):
    global paused
    if paused:
        await message.answer('⏸️ Игры приостановлены администратором. Попробуйте позже.', reply_markup=main_keyboard)
        return
    if message.chat.type != 'private':
        await message.answer('❌ Азартные игры доступны только в личных сообщениях с ботом. Дуэли работают в группах!', reply_markup=main_keyboard)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🏆 Команда A (x2)', callback_data='sport_a')],
        [InlineKeyboardButton(text='🏆 Команда B (x2)', callback_data='sport_b')],
        [InlineKeyboardButton(text='📊 Over 2.5 голов (x1.8)', callback_data='sport_over')],
        [InlineKeyboardButton(text='📊 Under 2.5 голов (x1.8)', callback_data='sport_under')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    await message.answer('⚽ Спорт: Выберите тип ставки', reply_markup=keyboard)


@dp.message(Command('sport'))
async def sport_command(message: Message, state: FSMContext):
    await sport_menu(message, state)


@dp.callback_query(F.data == 'sport_a')
async def sport_a(callback: CallbackQuery, state: FSMContext):
    await state.update_data(sport_type='team', sport_choice='a')
    await callback.message.edit_text('Выбрано: Команда A (x2)\nВведите ставку (мин 10$):')
    await state.set_state(GameStates.waiting_sport_bet)
    await callback.answer()


@dp.callback_query(F.data == 'sport_b')
async def sport_b(callback: CallbackQuery, state: FSMContext):
    await state.update_data(sport_type='team', sport_choice='b')
    await callback.message.edit_text('Выбрано: Команда B (x2)\nВведите ставку (мин 10$):')
    await state.set_state(GameStates.waiting_sport_bet)
    await callback.answer()

@dp.callback_query(F.data == 'sport_over')
async def sport_over(callback: CallbackQuery, state: FSMContext):
    await state.update_data(sport_type='overunder', sport_choice='over')
    await callback.message.edit_text('Выбрано: Over 2.5 голов (x1.8)\nВведите ставку (мин 10$):')
    await state.set_state(GameStates.waiting_sport_bet)
    await callback.answer()

@dp.callback_query(F.data == 'sport_under')
async def sport_under(callback: CallbackQuery, state: FSMContext):
    await state.update_data(sport_type='overunder', sport_choice='under')
    await callback.message.edit_text('Выбрано: Under 2.5 голов (x1.8)\nВведите ставку (мин 10$):')
    await state.set_state(GameStates.waiting_sport_bet)
    await callback.answer()

@dp.message(F.text == '🎲 Рандом дуэль')
async def random_duel(message: Message):
    global paused
    if paused:
        await message.reply('⏸️ Игры приостановлены администратором. Попробуйте позже.', reply_markup=main_keyboard)
        return
    user_id = message.from_user.id
    try:
        if any(u[0] == user_id for u in random_queue):
            await message.reply('Вы уже в очереди!')
            return
        bet = 100  # Fixed bet
        if get_balance(user_id) < bet:
            await message.reply('Недостаточно баланса! Минимум $100.')
            return
        random_queue.append((user_id, bet))
        await message.reply('Вы добавлены в очередь. Ждите оппонента...')
        save_data()
        # Check for match
        if len(random_queue) >= 2:
            # Find matching bet
            queue_copy = random_queue[:]
            for i in range(len(queue_copy)):
                for j in range(i+1, len(queue_copy)):
                    id1, bet1 = queue_copy[i]
                    id2, bet2 = queue_copy[j]
                    if abs(bet1 - bet2) <= 10:  # Allow small bet difference for flexibility
                        bet_avg = (bet1 + bet2) // 2
                        # Match found
                        random_queue[:] = [q for q in random_queue if q[0] not in (id1, id2)]
                        # Deduct bet
                        update_balance(id1, -bet1)
                        update_balance(id2, -bet2)
                        stats['total_bets'] += bet1 + bet2
                        # Unified duel structure: player1 (initiator), player2, current_turn, scores, bet
                        duel_id = f"{min(id1, id2)}_{max(id1, id2)}"
                        chat_id = message.chat.id if message.chat.type != 'private' else None
                        pending_duels[duel_id] = {
                            'player1': id1, 'player2': id2, 'bet': bet_avg, 'chat_id': chat_id,
                            'scores': {id1: 0, id2: 0}, 'current_turn': id1
                        }
                        save_data()
                        # Get names
                        name1 = get_opponent_name(id2)
                        name2 = get_opponent_name(id1)
                        # Message to player1 (turn)
                        text1 = f"⚔️ Оппонент найден ({name1})! Ставка: ${bet_avg}\nСчёт: Вы 0 - Оппонент 0\nВаша очередь крутить слоты."
                        keyboard1 = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text='Крутить', callback_data=f'duel_turn_{duel_id}')]
                        ])
                        await bot.send_message(id1, text1, reply_markup=keyboard1)
                        # Message to player2 (wait)
                        text2 = f"⚔️ Оппонент найден ({name1})! Ставка: ${bet_avg}\nСчёт: Вы 0 - Оппонент 0\nЖдите своей очереди."
                        await bot.send_message(id2, text2)
                        return
        save_data()
    except Exception as e:
        logging.error(f'Error in random_duel for {user_id}: {e}')
        try:
            await message.reply('Ошибка при добавлении в очередь. Попробуйте позже.')
        except:
            pass

@dp.message(F.text == '⚔️ Дуэль')
async def duel_menu(message: Message, state: FSMContext):
    global paused
    if paused:
        await message.answer('⏸️ Игры приостановлены администратором. Попробуйте позже.', reply_markup=main_keyboard)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🎰 Слоты (по умолчанию)', callback_data='duel_mode_slots')],
        [InlineKeyboardButton(text='🎡 Рулетка', callback_data='duel_mode_roulette')],
        [InlineKeyboardButton(text='🪙 Монетка', callback_data='duel_mode_coin')],
        [InlineKeyboardButton(text='❌ Отмена', callback_data='cancel_duel_input')]
    ])
    await message.answer('⚔️ Выберите режим дуэли:', reply_markup=keyboard)
    await state.set_state(GameStates.waiting_duel_mode)

def get_opponent_name(opp_id):
    """Get opponent name"""
    return user_info.get(opp_id, {}).get('name', f'Пользователь ID: {opp_id}')


@dp.message(Command('duel'))
async def duel_command(message: Message, state: FSMContext):
    await duel_menu(message, state)






class DuelModes(StatesGroup):
    waiting_duel_mode = State()

@dp.callback_query(F.data.startswith('duel_mode_'))
async def duel_mode_select(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.split('_')[-1]
    await state.update_data(duel_mode=mode)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='❌ Отмена', callback_data='cancel_duel_input')]
    ])
    await callback.message.edit_text(f'⚔️ Режим: {mode.capitalize()}\nОтветьте на сообщение оппонента или введите ID/ @username:', reply_markup=keyboard)
    await state.set_state(GameStates.waiting_duel_opponent)
    await callback.answer()

@dp.message(GameStates.waiting_duel_opponent)
async def duel_opponent_input(message: Message, state: FSMContext):
    data = await state.get_data()
    duel_mode = data.get('duel_mode', 'slots')  # Default slots
    user_id = message.from_user.id
    chat_id = message.chat.id if message.chat.type != 'private' else None
    try:
        opp_id = None
        if message.reply_to_message:
            opp_id = message.reply_to_message.from_user.id
        elif message.entities:
            # Parse mentions from entities
            for entity in message.entities:
                if entity.type == 'mention':
                    username = message.text[entity.offset:entity.offset + entity.length][1:]  # Remove @
                    for uid, info in user_info.items():
                        if info.get('name', '').lower() == username.lower():
                            opp_id = uid
                            break
                    if opp_id is None:
                        try:
                            bot_chat = await bot.get_chat(f'@{username}')
                            opp_id = bot_chat.id
                            if opp_id not in user_info:
                                user_info[opp_id] = {'name': username, 'registered': True}
                                get_balance(opp_id)
                                save_data()
                        except Exception as e:
                            logging.error(f'Error getting mention chat for @{username}: {e}')
                            continue
                    break
        if opp_id is None:
            text = message.text.strip()
            if text.startswith('@'):
                username = text[1:]
                for uid, info in user_info.items():
                    if info.get('name', '').lower() == username.lower():
                        opp_id = uid
                        break
                if opp_id is None:
                    try:
                        bot_chat = await bot.get_chat(f'@{username}')
                        opp_id = bot_chat.id
                        if opp_id not in user_info:
                            user_info[opp_id] = {'name': username, 'registered': True}
                            get_balance(opp_id)
                            save_data()
                    except Exception as e:
                        logging.error(f'Error getting chat for @{username}: {e}')
                        await message.reply(f'Пользователь @{username} не найден или недоступен.')
                        return
                if opp_id is None:
                    await message.reply(f'Пользователь @{username} не найден в базе.')
                    return
            else:
                try:
                    opp_id = int(text)
                except ValueError:
                    await message.reply('Неверный ввод! Ответьте на сообщение, упомяните @username или введите ID.')
                    return
        if opp_id == user_id:
            await message.reply('Нельзя дуэлировать с собой!')
            return
        if opp_id not in balances:
            await message.reply('Пользователь не зарегистрирован.')
            return
        if opp_id in banned_users:
            await message.reply('Оппонент заблокирован!')
            return
        await state.update_data(opp_id=opp_id, chat_id=chat_id)
        await message.reply('Введите ставку (мин 10$):')
        await state.set_state(GameStates.waiting_duel_bet)
    except Exception as e:
        logging.error(f'Error in duel_opponent_input for {user_id}: {e}')
        await message.reply('Ошибка ввода. Попробуйте снова.')

@dp.message(F.text == '♠️ Блэкджек')
async def blackjack_menu(message: Message, state: FSMContext):
    global paused
    if paused:
        await message.answer('⏸️ Игры приостановлены администратором. Попробуйте позже.', reply_markup=main_keyboard)
        return
    if message.chat.type != 'private':
        await message.answer('❌ Азартные игры доступны только в личных сообщениях с ботом. Дуэли работают в группах!', reply_markup=main_keyboard)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💎 Low 50$', callback_data='blackjack_50')],
        [InlineKeyboardButton(text='💎 Med 200$', callback_data='blackjack_200')],
        [InlineKeyboardButton(text='💎 High 500$', callback_data='blackjack_500')],
        [InlineKeyboardButton(text='🎯 Custom', callback_data='blackjack_custom')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    await message.answer('♠️ Блэкджек: Выберите ставку', reply_markup=keyboard)
    await state.set_state(GameStates.waiting_blackjack_bet)

@dp.message(F.text == '♦️ Покер')
async def poker_menu(message: Message, state: FSMContext):
    global paused
    if paused:
        await message.answer('⏸️ Игры приостановлены администратором. Попробуйте позже.', reply_markup=main_keyboard)
        return
    if message.chat.type != 'private':
        await message.answer('❌ Азартные игры доступны только в личных сообщениях с ботом. Дуэли работают в группах!', reply_markup=main_keyboard)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💎 Low 50$', callback_data='poker_50')],
        [InlineKeyboardButton(text='💎 Med 200$', callback_data='poker_200')],
        [InlineKeyboardButton(text='💎 High 500$', callback_data='poker_500')],
        [InlineKeyboardButton(text='🎯 Custom', callback_data='poker_custom')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    await message.answer('♦️ Покер: Выберите ставку для 5-карточного покера', reply_markup=keyboard)
    await state.set_state(GameStates.waiting_poker_bet)

@dp.message(Command('poker'))
async def poker_command(message: Message, state: FSMContext):
    await poker_menu(message, state)


@dp.message(Command('blackjack'))
async def blackjack_command(message: Message, state: FSMContext):
    await blackjack_menu(message, state)

@dp.message(F.text == '🎡 Рулетка')
async def roulette_menu(message: Message, state: FSMContext):
    global paused
    if paused:
        await message.answer('⏸️ Игры приостановлены администратором. Попробуйте позже.', reply_markup=main_keyboard)
        return
    if message.chat.type != 'private':
        await message.answer('❌ Азартные игры доступны только в личных сообщениях с ботом. Дуэли работают в группах!', reply_markup=main_keyboard)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔴 Красное (x2)', callback_data='roulette_red')],
        [InlineKeyboardButton(text='⚫ Черное (x2)', callback_data='roulette_black')],
        [InlineKeyboardButton(text='📊 Четное (x2)', callback_data='roulette_even')],
        [InlineKeyboardButton(text='📊 Нечетное (x2)', callback_data='roulette_odd')],
        [InlineKeyboardButton(text='🎯 Число (x18)', callback_data='roulette_number')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    await message.answer('🎡 Рулетка: Выберите тип ставки', reply_markup=keyboard)


@dp.message(Command('roulette'))
async def roulette_command(message: Message, state: FSMContext):
    await roulette_menu(message, state)

@dp.callback_query(F.data.startswith('roulette_'))
async def roulette_type(callback: CallbackQuery, state: FSMContext):
    data = callback.data.split('_', 1)[1]
    if data == 'number':
        await callback.message.edit_text('Введите число для ставки (0-36):')
        await state.set_state(GameStates.waiting_roulette_number)
    else:
        multiplier = 2
        await state.update_data(roulette_type=data, multiplier=multiplier)
        await callback.message.edit_text(f'Выбрано: {data.capitalize()} (x{multiplier})\nВведите ставку (мин 10$):')
        await state.set_state(GameStates.waiting_bet)
    await callback.answer()

@dp.callback_query(F.data.regexp(r'^slots_(\d+)$'))
async def slots_fixed_bet(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    bet_match = re.match(r'^slots_(\d+)$', callback.data)
    bet = int(bet_match.group(1))
    try:
        await play_slots(callback, bet, state)
    except Exception as e:
        logging.error(f'Error in slots fixed bet for {user_id}: {e}')
        await callback.answer('Ошибка ставки!')
    else:
        await callback.answer()

@dp.callback_query(F.data == 'slots_custom')
async def slots_custom(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try:
        await callback.message.edit_text('🎰 Введите кастомную ставку для слотов (мин 10$):')
        await state.set_state(GameStates.waiting_bet)
    except Exception as e:
        logging.error(f'Error editing custom slots prompt for {user_id}: {e}')
        try:
            await callback.message.reply('🎰 Введите кастомную ставку для слотов (мин 10$):')
            await state.set_state(GameStates.waiting_bet)
        except Exception as e2:
            logging.error(f'Error replying custom slots prompt for {user_id}: {e2}')
    await callback.answer()

async def play_slots(obj, bet: int, state: FSMContext):
    """Unified slots player: obj is Message or CallbackQuery"""
    if isinstance(obj, CallbackQuery):
        user_id = obj.from_user.id
        message = obj.message
        answer = obj.answer
    else:
        user_id = obj.from_user.id
        message = obj
        answer = lambda *args, **kwargs: None  # No answer for message

    balance = get_balance(user_id)
    if balance < bet:
        try:
            if isinstance(obj, CallbackQuery):
                await answer('💸 Недостаточно!')
            else:
                await message.reply('💸 Недостаточно!')
        except Exception as e:
            logging.error(f'Error in play_slots insufficient balance for {user_id}: {e}')
        return

    update_balance(user_id, -bet)
    stats['total_bets'] += bet
    symbols = ['🍒', '🍋', '🍊', '🔔', '⭐', '7️⃣']
    try:
        if isinstance(obj, CallbackQuery):
            await message.edit_text('🎰 Крутим слоты... ✨')
        else:
            await message.reply('🎰 Крутим слоты... ✨')
    except Exception as e:
        logging.error(f'Error starting slots for {user_id}: {e}')

    try:
        if isinstance(obj, CallbackQuery):
            msg = await message.reply('⏳')
        else:
            msg = await message.reply('⏳')
    except Exception as e:
        logging.error(f'Error sending slots loading for {user_id}: {e}')
        return

    slot1, slot2, slot3 = [random.choice(symbols) for _ in range(3)]
    try:
        for _ in range(15):
            temp_slots = [random.choice(symbols) for _ in range(3)]
            anim_text = f"🎰 {' | '.join(temp_slots)} 🎰"
            await msg.edit_text(anim_text)
            await asyncio.sleep(0.2)
    except Exception as e:
        logging.error(f'Error in slots animation for {user_id}: {e}')

    # Payout calculation
    payout = 0
    if slot1 == slot2 == slot3:
        if slot1 == '7️⃣':
            payout = 20
        elif slot1 in ['⭐']:  # Fixed: no 💎 in symbols
            payout = 15
        else:
            payout = 8  # Fruits/BAR etc.
    elif slot1 == slot2 or slot2 == slot3 or slot1 == slot3:
        payout = 3
    else:
        if random.random() < 0.6:  # 60% chance to lose in no-combo for RTP ~94%
            payout = 0
        else:
            payout = random.choice([1, 2, 3])

    result_text = f"🎰 {' | '.join([slot1, slot2, slot3])} 🎰"
    if payout > 0:
        win = bet * payout
        update_balance(user_id, win)
        stats['total_wins'] += win
        result = f'🎉 Вы выиграли ${win}! (x{payout})'
    else:
        result = f'😔 Не повезло, потеряли ${bet}.'

    new_balance = get_balance(user_id)
    full_text = f'{result_text}\n{result}\n💵 Баланс: ${new_balance}'
    try:
        await msg.edit_text(full_text)
    except Exception as e:
        logging.error(f'Error editing slots result for {user_id}: {e}')

    # Menu
    slots_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💎 Low 50$', callback_data='slots_50')],
        [InlineKeyboardButton(text='💎 Med 200$', callback_data='slots_200')],
        [InlineKeyboardButton(text='💎 High 500$', callback_data='slots_500')],
        [InlineKeyboardButton(text='🎯 Custom', callback_data='slots_custom')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    try:
        await bot.send_message(user_id, 'Хотите сыграть ещё?', reply_markup=slots_keyboard)
        await state.set_state(GameStates.waiting_bet)
    except Exception as e:
        logging.error(f'Error sending slots menu for {user_id}: {e}')

    await state.clear()
    try:
        await answer()
    except:
        pass  # No answer for message

@dp.message(GameStates.waiting_bet)
async def slots_bet_input(message: Message, state: FSMContext):
    data = await state.get_data()
    if 'roulette_type' in data:
        roulette_type = data['roulette_type']
        multiplier = data.get('multiplier', 18 if roulette_type == 'number' else 2)
        bet_number = data.get('bet_number')
        bet_text = message.text
        try:
            bet = int(bet_text)
            user_id = message.from_user.id
            balance = get_balance(user_id)
            if bet < 10 or bet > balance:
                try:
                    await message.reply('Неверная ставка! Мин 10$, макс баланс.')
                except Exception as e:
                    logging.error(f'Error in roulette bet invalid for {user_id}: {e}')
                return
            try:
                await play_roulette(message, bet, roulette_type, multiplier, bet_number)
            except Exception as e:
                logging.error(f'Error playing roulette for {user_id}: {e}')
        except ValueError:
            try:
                await message.reply('Введите число!')
            except Exception as e:
                logging.error(f'Error in roulette bet value for {user_id}: {e}')
        await state.clear()
        return
    # Slots bet
    try:
        bet = int(message.text)
        await play_slots(message, bet, state)
    except ValueError:
        try:
            await message.reply('Введите число!')
        except Exception as e:
            logging.error(f'Error in slots bet value error for {message.from_user.id}: {e}')
    except Exception as e:
        logging.error(f'Unexpected error in slots_bet_input: {e}')
        await message.reply('Ошибка обработки ставки. Попробуйте снова.')
    # State cleared in play_slots

@dp.message(GameStates.waiting_sport_bet)
async def sport_bet_input(message: Message, state: FSMContext):
    data = await state.get_data()
    sport_type = data.get('sport_type', 'team')
    sport_choice = data.get('sport_choice')
    if not sport_choice:
        await message.reply('Ошибка выбора ставки. Вернитесь в меню Спорт.')
        await state.clear()
        return
    bet_text = message.text
    try:
        bet = int(bet_text)
        user_id = message.from_user.id
        balance = get_balance(user_id)
        if bet < 10 or bet > balance:
            await message.reply('Неверная ставка! Мин 10$, макс баланс.')
            return
        await play_sport(message, bet, sport_type, sport_choice)
    except ValueError:
        await message.reply('Введите число!')
    except Exception as e:
        logging.error(f'Error in sport bet input: {e}')
        await message.reply('Ошибка обработки ставки. Попробуйте снова.')
    await state.clear()

@dp.message(GameStates.waiting_duel_bet)
async def duel_bet_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        bet = int(message.text)
        balance = get_balance(user_id)
        if bet < 10 or bet > balance:
            await message.reply('Неверная ставка! Мин 10$, макс баланс.')
            return
        data = await state.get_data()
        opp_id = data['opp_id']
        chat_id = data.get('chat_id')
        opp_balance = get_balance(opp_id)
        if opp_balance < bet:
            await message.reply(f'У оппонента недостаточно баланса! ({opp_balance}$)')
            return
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='Принять дуэль', callback_data=f'accept_duel_{user_id}_{opp_id}_{bet}_{chat_id or ""}')]
        ])
        opp_name = get_opponent_name(user_id)
        try:
            await bot.send_message(opp_id, f'⚔️ {opp_name} вызывает вас на дуэль! Ставка: ${bet}', reply_markup=keyboard)
        except Exception as e:
            logging.error(f'Error sending duel invite to {opp_id}: {e}')
            await message.reply('Ошибка отправки приглашения оппоненту.')
            return
        my_name = get_opponent_name(user_id)
        await message.reply(f'Приглашение отправлено {my_name}!')
        # Store pending invite
        pending_duels[user_id] = {'opp': opp_id, 'bet': bet, 'chat_id': chat_id}
        save_data()
        await state.clear()
    except ValueError:
        await message.reply('Введите число!')
    except Exception as e:
        logging.error(f'Error in duel_bet_input for {user_id}: {e}')
        await message.reply('Ошибка! Попробуйте снова.')

@dp.message(GameStates.waiting_roulette_number)
async def roulette_number_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        number = int(message.text)
        if not 0 <= number <= 36:
            try:
                await message.reply('Число должно быть от 0 до 36!')
            except Exception as e:
                logging.error(f'Error in roulette number invalid for {user_id}: {e}')
            return
        await state.update_data(bet_number=number, roulette_type='number', multiplier=18)
        try:
            await message.reply('Число выбрано! Введите ставку (мин 10$):')
        except Exception as e:
            logging.error(f'Error in roulette number reply for {user_id}: {e}')
        await state.set_state(GameStates.waiting_bet)
    except ValueError:
        try:
            await message.reply('Введите целое число!')
        except Exception as e:
            logging.error(f'Error in roulette number value for {user_id}: {e}')

@dp.callback_query(F.data == 'slots_menu')
async def slots_menu_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💎 Low 50$', callback_data='slots_50')],
        [InlineKeyboardButton(text='💎 Med 200$', callback_data='slots_200')],
        [InlineKeyboardButton(text='💎 High 500$', callback_data='slots_500')],
        [InlineKeyboardButton(text='🎯 Custom', callback_data='slots_custom')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    try:
        await callback.message.delete()
    except Exception as e:
        logging.error(f'Error deleting slots menu callback message for {user_id}: {e}')
    try:
        await bot.send_message(user_id, '🎰 Выберите ставку для слотов (50% шанс выигрыша):', reply_markup=keyboard)
        await state.set_state(GameStates.waiting_bet)
    except Exception as e:
        logging.error(f'Error sending slots menu for {user_id}: {e}')
    try:
        await callback.answer()
    except Exception as e:
        logging.error(f'Error answering slots menu callback for {user_id}: {e}')

@dp.callback_query(F.data.regexp(r'^blackjack_(\d+)$'))
async def blackjack_fixed_bet(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    bet_match = re.match(r'^blackjack_(\d+)$', callback.data)
    bet = int(bet_match.group(1))
    try:
        await start_blackjack(callback, bet, state)
    except Exception as e:
        logging.error(f'Error in blackjack fixed bet for {user_id}: {e}')
        await callback.answer('Ошибка ставки!')
    else:
        await callback.answer()

@dp.callback_query(F.data == 'blackjack_custom')
async def blackjack_custom(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try:
        await callback.message.edit_text('♠️ Введите кастомную ставку для блэкджека (мин 10$):')
        await state.set_state(GameStates.waiting_blackjack_bet)
    except Exception as e:
        logging.error(f'Error editing custom blackjack prompt for {user_id}: {e}')
        try:
            await callback.message.reply('♠️ Введите кастомную ставку для блэкджека (мин 10$):')
            await state.set_state(GameStates.waiting_blackjack_bet)
        except Exception as e2:
            logging.error(f'Error replying custom blackjack prompt for {user_id}: {e2}')
    await callback.answer()

@dp.callback_query(F.data.regexp(r'^poker_(\d+)$'))
async def poker_fixed_bet(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    bet_match = re.match(r'^poker_(\d+)$', callback.data)
    bet = int(bet_match.group(1))
    try:
        await play_poker(callback, bet, state)
    except Exception as e:
        logging.error(f'Error in poker fixed bet for {user_id}: {e}')
        await callback.answer('Ошибка ставки!')
    else:
        await callback.answer()

@dp.callback_query(F.data == 'poker_custom')
async def poker_custom(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try:
        await callback.message.edit_text('♦️ Введите кастомную ставку для покера (мин 10$):')
        await state.set_state(GameStates.waiting_poker_bet)
    except Exception as e:
        logging.error(f'Error editing custom poker prompt for {user_id}: {e}')
        try:
            await callback.message.reply('♦️ Введите кастомную ставку для покера (мин 10$):')
            await state.set_state(GameStates.waiting_poker_bet)
        except Exception as e2:
            logging.error(f'Error replying custom poker prompt for {user_id}: {e2}')
    await callback.answer()

@dp.message(GameStates.waiting_blackjack_bet)
async def blackjack_bet_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        bet = int(message.text)
        balance = get_balance(user_id)
        if bet < 10 or bet > balance:
            try:
                await message.reply('Неверная ставка! Мин 10$, макс баланс.')
            except Exception as e:
                logging.error(f'Error in blackjack bet invalid for {user_id}: {e}')
            return
        try:
            await start_blackjack(message, bet, state)
        except Exception as e:
            logging.error(f'Error starting blackjack for {user_id}: {e}')
    except ValueError:
        try:
            await message.reply('Введите число!')
        except Exception as e:
            logging.error(f'Error in blackjack bet value for {user_id}: {e}')
    await state.clear()

@dp.message(GameStates.waiting_poker_bet)
async def poker_bet_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        bet = int(message.text)
        balance = get_balance(user_id)
        if bet < 10 or bet > balance:
            try:
                await message.reply('Неверная ставка! Мин 10$, макс баланс.')
            except Exception as e:
                logging.error(f'Error in poker bet invalid for {user_id}: {e}')
            return
        try:
            await play_poker(message, bet, state)
        except Exception as e:
            logging.error(f'Error playing poker for {user_id}: {e}')
    except ValueError:
        try:
            await message.reply('Введите число!')
        except Exception as e:
            logging.error(f'Error in poker bet value for {user_id}: {e}')
    await state.clear()

# Admin
@dp.message(Command('admin'))
async def admin_handler(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer('🚫 Нет доступа!', reply_markup=main_keyboard)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💰 Управление балансом', callback_data='admin_balance')],
        [InlineKeyboardButton(text='👥 Список пользователей', callback_data='admin_users')],
        [InlineKeyboardButton(text='📊 Статистика', callback_data='admin_stats')],
        [InlineKeyboardButton(text='🏆 Топ пользователей', callback_data='admin_top')],
        [InlineKeyboardButton(text='🚫 Бан/разбан', callback_data='admin_ban')],
        [InlineKeyboardButton(text='🔄 Сброс баланса', callback_data='admin_reset')],
        [InlineKeyboardButton(text='⏸️ Пауза игр', callback_data='admin_pause')],
        [InlineKeyboardButton(text='🗑 Очистить очередь', callback_data='admin_queue')],
        [InlineKeyboardButton(text='📝 Логи', callback_data='admin_logs')],
        [InlineKeyboardButton(text='💬 Обр. Связь', callback_data='admin_feedback')],
        [InlineKeyboardButton(text='📢 Broadcast', callback_data='admin_broadcast')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    await message.answer('🛡️ Админ-панель:', reply_markup=keyboard)

@dp.callback_query(F.data == 'admin_balance')
async def admin_balance(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text('Введите ID и сумму (напр. 12345 +100):')
    await state.set_state(GameStates.admin_add_money)

@dp.message(GameStates.admin_add_money)
async def admin_add_money_input(message: Message, state: FSMContext):
    try:
        match = re.match(r'^(\d+)\s*([+-])\s*(\d+)$', message.text.strip())
        if not match:
            raise ValueError("Неверный формат. Используйте: ID +amount или ID -amount")
        user_id_str, op, amount_str = match.groups()
        user_id = int(user_id_str)
        if op not in ['+', '-']:
            raise ValueError("Операция должна быть '+' или '-'")
        amount = int(amount_str)
        change = amount if op == '+' else -amount
        old_balance = get_balance(user_id)
        update_balance(user_id, change)
        new_balance = get_balance(user_id)
        name = user_info.get(user_id, {}).get('name', 'User')
        await message.reply(f'Баланс @{name} (ID {user_id}): {old_balance} -> {new_balance} (изменение: {change})$')
    except ValueError as e:
        await message.reply(f'Ошибка ввода: {str(e)}\nПример: 12345 +100 или 12345+100')
    except Exception as e:
        await message.reply(f'Неожиданная ошибка: {str(e)}')
    await state.clear()

@dp.callback_query(F.data == 'admin_users')
async def admin_users(callback: CallbackQuery):
    if not balances:
        text = '👥 Нет пользователей.'
    else:
        text = '👥 Пользователи (sorted by balance):\n'
        for uid, bal in sorted(balances.items(), key=lambda x: x[1], reverse=True):
            name = user_info.get(uid, {}).get('name', 'User')
            text += f'ID: {uid} | @{name} | 💵 ${bal}\n'
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🔙 Админ', callback_data='admin_menu')]])
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == 'admin_stats')
async def admin_stats(callback: CallbackQuery):
    total_rtp = (stats['total_wins'] / stats['total_bets'] * 100) if stats['total_bets'] > 0 else 0
    top_balance = max(balances.values()) if balances else 0
    text = f'📊 Статистика:\n💰 Общие ставки: ${stats["total_bets"]}\n🏆 Общие выигрыши: ${stats["total_wins"]}\n📈 RTP: {total_rtp:.1f}%\n👥 Активных пользователей: {len(balances)}\n👑 Топ баланс: ${top_balance}\n🎯 Выигрышей: {len([b for b in balances.values() if b > 10000])}'
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🔙 Админ', callback_data='admin_menu')]])
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.message(Command('broadcast'))
async def broadcast_handler(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer('🚫 Нет доступа!', reply_markup=main_keyboard)
        return
    await message.answer('📢 Введите сообщение для рассылки всем пользователям:')
    await state.set_state(GameStates.waiting_broadcast)

@dp.message(GameStates.waiting_broadcast)
async def broadcast_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    broadcast_msg = message.text
    sent_count = 0
    failed_count = 0
    for user_id in list(user_info.keys()):
        try:
            await bot.send_message(user_id, broadcast_msg)
            sent_count += 1
        except Exception as e:
            logging.error(f'Failed to send broadcast to {user_id}: {e}')
            failed_count += 1
    await message.answer(f'📢 Рассылка завершена: {sent_count} отправлено, {failed_count} ошибок.', reply_markup=main_keyboard)
    await state.clear()
    logging.info(f'Broadcast sent by {message.from_user.id}: {sent_count} success, {failed_count} fail')

@dp.callback_query(F.data == 'admin_top')
async def admin_top_users(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer('🚫 Нет доступа!')
        return
    if not balances:
        text = '🏆 Нет пользователей.'
    else:
        top_users = sorted(balances.items(), key=lambda x: x[1], reverse=True)[:10]
        text = '🏆 Топ 10 пользователей по балансу:\n\n'
        for i, (uid, bal) in enumerate(top_users, 1):
            name = user_info.get(uid, {}).get('name', f'User{uid}')
            text += f'{i}. @{name} (ID: {uid}) - ${bal}\n'
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🔙 Админ', callback_data='admin_menu')]])
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == 'admin_reset')
async def admin_reset_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer('🚫 Нет доступа!')
        return
    await callback.message.edit_text('🔄 Введите ID пользователя для сброса баланса (к 10000$):')
    await state.set_state(GameStates.waiting_reset_id)

@dp.message(GameStates.waiting_reset_id)
async def admin_reset_balance(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        user_id = int(message.text.strip())
        old_balance = get_balance(user_id)
        change = 10000 - old_balance
        update_balance(user_id, change)
        name = user_info.get(user_id, {}).get('name', 'User')
        await message.reply(f'🔄 Баланс @{name} (ID {user_id}) сброшен: {old_balance} -> 10000$', reply_markup=main_keyboard)
        logging.info(f'Balance reset for {user_id} by {message.from_user.id} to 10000')
    except ValueError:
        await message.reply('❌ Неверный ID! Введите число.', reply_markup=main_keyboard)
    except Exception as e:
        await message.reply(f'❌ Ошибка: {e}', reply_markup=main_keyboard)
    await state.clear()

@dp.callback_query(F.data == 'admin_pause')
async def admin_pause_games(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer('🚫 Нет доступа!')
        return
    global paused
    paused = not paused
    save_data()
    status = '⏸️ Игры приостановлены' if paused else '▶️ Игры возобновлены'
    await callback.message.edit_text(f'{status} для всех пользователей.')
    logging.info(f'Games paused toggled to {paused} by {callback.from_user.id}')
    await callback.answer()

@dp.callback_query(F.data == 'admin_menu')
async def admin_menu(callback: CallbackQuery):
    await callback.message.edit_text('🛡️ Админ-панель:', reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💰 Управление балансом', callback_data='admin_balance')],
        [InlineKeyboardButton(text='👥 Список пользователей', callback_data='admin_users')],
        [InlineKeyboardButton(text='📊 Статистика', callback_data='admin_stats')],
        [InlineKeyboardButton(text='🏆 Топ пользователей', callback_data='admin_top')],
        [InlineKeyboardButton(text='🚫 Бан/разбан', callback_data='admin_ban')],
        [InlineKeyboardButton(text='🔄 Сброс баланса', callback_data='admin_reset')],
        [InlineKeyboardButton(text='⏸️ Пауза игр', callback_data='admin_pause')],
        [InlineKeyboardButton(text='🗑 Очистить очередь', callback_data='admin_queue')],
        [InlineKeyboardButton(text='📝 Логи', callback_data='admin_logs')],
        [InlineKeyboardButton(text='💬 Обр. Связь', callback_data='admin_feedback')],
        [InlineKeyboardButton(text='📢 Broadcast', callback_data='admin_broadcast')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ]))
    await callback.answer()

@dp.callback_query(F.data == 'admin_ban')
async def admin_ban(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text('Введите ID для бана/разбана:')
    await state.set_state(GameStates.admin_ban)

@dp.message(GameStates.admin_ban)
async def admin_ban_input(message: Message, state: FSMContext):
    try:
        user_id = int(message.text)
        if user_id in banned_users:
            banned_users.remove(user_id)
            await message.reply(f'{user_id} разбанен.')
            save_data()
        else:
            banned_users.add(user_id)
            await message.reply(f'{user_id} забанен.')
            save_data()
            save_data()
    except:
        await message.reply('Неверный ID!')
    await state.clear()

@dp.callback_query(F.data == 'admin_queue')
async def admin_queue(callback: CallbackQuery):
    random_queue.clear()
    save_data()
    await callback.answer('🗑 Очередь очищена!')

@dp.callback_query(F.data == 'admin_logs')
async def admin_logs(callback: CallbackQuery):
    try:
        with open('bot.log', 'r') as f:
            logs = f.read()[-1000:]
        await callback.message.edit_text(f'📝 Логи:\n{logs or "Нет логов"}')
    except:
        await callback.message.edit_text('Ошибка чтения логов.')
    await callback.answer()

@dp.callback_query(F.data == 'admin_feedback')
async def admin_feedback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer('🚫 Нет доступа!')
        return
    if not feedbacks:
        text = '💬 Нет отзывов.'
    else:
        text = '💬 Отзывы:\n\n'
        for i, fb in enumerate(feedbacks):
            status = '✅ (Ответлено)' if fb['replied'] else '⏳ (Ожидает)'
            text += f"{i+1}. От @{fb['username']} (ID: {fb['user_id']})\n"
            text += f"Сообщение: {fb['message']}\n"
            text += f"Дата: {fb['timestamp'][:16]}\n"
            text += f"Статус: {status}\n\n"
            if not fb['replied']:
                text += f"Ответить: /feedback_reply {fb['user_id']}\n\n"
            if fb['replied'] and fb['reply']:
                text += f"Ответ: {fb['reply']}\n\n"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔙 Админ', callback_data='admin_menu')]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == 'back_main')
async def back_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    if user_id in banned_users:
        try:
            await callback.message.delete()
        except:
            pass
        await bot.send_message(user_id, '🚫 Вы заблокированы!', reply_markup=main_keyboard)
        await callback.answer()
        return
    get_balance(user_id)
    if user_id not in user_info or not user_info[user_id]['name']:
        user_info[user_id] = user_info.get(user_id, {})
        user_info[user_id]['name'] = callback.from_user.username or 'User'
    balance = get_balance(user_id)
    text = f'🎉 Добро пожаловать в Деп-Казино! 🎰\n💵 Баланс: ${balance}\n👤 @{user_info[user_id]["name"]}\nВыберите игру:'
    try:
        await callback.message.delete()
    except:
        pass
    await bot.send_message(user_id, text, reply_markup=main_keyboard)
    log_action('back_main', user_id)
    await callback.answer()

# Add basic implementations for other games to make it work
@dp.message(F.text == '🎁 Бонус')
async def daily_bonus(message: Message):
    user_id = message.from_user.id
    now = datetime.now()
    last = last_daily.get(user_id, now - timedelta(days=2))
    if now - last > timedelta(days=1):
        old_balance = get_balance(user_id)
        update_balance(user_id, 200)
        last_daily[user_id] = now
        save_data()
        new_balance = get_balance(user_id)
        name = user_info.get(user_id, {}).get('name', 'User')
        try:
            await message.answer(f'🎁 +200$ ежедневно! 💵\nБаланс @{name}: {old_balance} -> {new_balance}', reply_markup=main_keyboard)
        except Exception as e:
            logging.error(f'Error in daily bonus success for {user_id}: {e}')
    else:
        remaining = timedelta(days=1) - (now - last)
        hours = remaining.seconds // 3600
        mins = (remaining.seconds % 3600) // 60
        try:
            await message.answer(f'⏳ Бонус доступен через {hours}ч {mins}м!', reply_markup=main_keyboard)
        except Exception as e:
            logging.error(f'Error in daily bonus cooldown for {user_id}: {e}')


@dp.message(Command('bonus'))
async def bonus_command(message: Message):
    await daily_bonus(message)

@dp.message(Command('cancel'))
async def cancel_handler(message: Message, state: FSMContext):
    user_id = message.from_user.id
    current_state = await state.get_state()
    await state.clear()
    # Remove from random queue
    global random_queue
    random_queue = [q for q in random_queue if q[0] != user_id]
    # Cancel pending duel invite
    if user_id in pending_duels and isinstance(pending_duels[user_id], dict) and 'opp' in pending_duels[user_id]:
        del pending_duels[user_id]
    # Cancel PM if in PM states
    if current_state in [State(GameStates.waiting_pm_recipient), State(GameStates.waiting_pm_message)]:
        pm_sessions.pop(user_id, None)
    # If in ongoing duel, just notify
    if current_state in [State(GameStates.waiting_duel_opponent), State(GameStates.waiting_duel_bet)]:
        await message.reply('❌ Дуэль отменена.\nВернулись в главное меню.', reply_markup=main_keyboard)
    else:
        await message.reply('❌ Действие отменено.\nВернулись в главное меню.', reply_markup=main_keyboard)
    save_data()

@dp.callback_query(F.data == 'cancel_duel_input')
async def cancel_duel_input(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await state.clear()
    # Remove from queue/pending as in cancel_handler
    global random_queue
    random_queue = [q for q in random_queue if q[0] != user_id]
    if user_id in pending_duels and isinstance(pending_duels[user_id], dict) and 'opp' in pending_duels[user_id]:
        del pending_duels[user_id]
    save_data()
    await callback.message.edit_text('❌ Дуэль отменена.')
    balance = get_balance(user_id)
    text = f'🎉 Добро пожаловать в Деп-Казино! 🎰\n💵 Баланс: ${balance}\n👤 @{user_info[user_id]["name"]}\nВыберите игру:'
    await bot.send_message(user_id, text, reply_markup=main_keyboard)
    await callback.answer()

# Private messaging
pm_sessions = {}  # {user_id: {'recipient': opp_id, 'active': True}}

@dp.message(Command('pm'))
async def pm_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id in pm_sessions:
        await message.reply('Вы уже в приватном чате. Используйте /cancel для выхода.')
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='❌ Отмена', callback_data='cancel_pm')]
    ])
    await message.answer('Напишите @username для приватного чата:', reply_markup=keyboard)
    await state.set_state(GameStates.waiting_pm_recipient)

@dp.message(GameStates.waiting_pm_recipient)
async def pm_recipient_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    text = message.text.strip()
    if text.startswith('@'):
        username = text[1:]
        opp_id = None
        # Search in user_info
        for uid, info in user_info.items():
            if info.get('name', '').lower() == username.lower():
                opp_id = uid
                break
        if opp_id is None:
            try:
                chat = await bot.get_chat(f'@{username}')
                opp_id = chat.id
                if opp_id not in user_info:
                    user_info[opp_id] = {'name': username, 'registered': True}
                    get_balance(opp_id)
                    save_data()
            except Exception as e:
                logging.error(f'Error getting PM chat for @{username}: {e}')
                await message.reply(f'Пользователь @{username} не найден.')
                return
        if opp_id == user_id:
            await message.reply('Нельзя писать себе!')
            return
        if opp_id in banned_users:
            await message.reply('Пользователь заблокирован.')
            return
        # Start PM session
        pm_sessions[user_id] = {'recipient': opp_id, 'active': True}
        pm_sessions[opp_id] = {'recipient': user_id, 'active': True}
        await state.update_data(pm_recipient=opp_id)
        await message.reply(f'Приватный чат с @{username} открыт. Напишите сообщение:')
        await state.set_state(GameStates.waiting_pm_message)
        # Notify opponent
        opp_name = get_opponent_name(user_id)
        await bot.send_message(opp_id, f'🗨️ @{opp_name} начал приватный чат с вами. Ответьте в боте.')
    else:
        await message.reply('Введите @username.')

@dp.message(GameStates.waiting_pm_message)
async def pm_message_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    opp_id = data.get('pm_recipient')
    if opp_id not in pm_sessions or not pm_sessions[user_id]['active']:
        await message.reply('Приватный чат закрыт.')
        await state.clear()
        return
    text = message.text
    my_name = get_opponent_name(user_id)
    await bot.send_message(opp_id, f'🗨️ От @{my_name}: {text}')
    await message.reply(f'Сообщение отправлено @{get_opponent_name(opp_id)}. Продолжите чат или /cancel.')

@dp.callback_query(F.data == 'cancel_pm')
async def cancel_pm(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if user_id in pm_sessions:
        opp_id = pm_sessions[user_id]['recipient']
        pm_sessions.pop(user_id, None)
        pm_sessions.pop(opp_id, None)
    await state.clear()
    await callback.message.edit_text('❌ Приватный чат отменён.')
    await callback.answer()

# Feedback system
@dp.message(Command('feedback'))
async def feedback_start(message: Message, state: FSMContext):
    await message.answer('📝 Введите ваше сообщение для обратной связи (администраторы рассмотрят):')
    await state.set_state(GameStates.waiting_feedback)

@dp.message(GameStates.waiting_feedback)
async def feedback_submit(message: Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name or 'Пользователь'
    text = message.text
    now = datetime.now().isoformat()
    feedbacks.append({
        'user_id': user_id,
        'username': username,
        'message': text,
        'timestamp': now,
        'replied': False,
        'reply': ''
    })
    save_data()
    # Notify all admins
    try:
        with open('admins.txt', 'r') as f:
            admins = [int(line.strip()) for line in f if line.strip().isdigit()]
        for admin_id in admins:
            try:
                await bot.send_message(admin_id, f'🆕 Новый отзыв от @{username} (ID: {user_id}):\n\n{text}\n\nОтветить: /feedback_reply {user_id}')
            except Exception as e:
                logging.error(f'Failed to notify admin {admin_id}: {e}')
    except Exception as e:
        logging.error(f'Error loading admins for notification: {e}')
    balance = get_balance(user_id)
    await message.answer(f'✅ Спасибо за отзыв! Ваш баланс: ${balance}', reply_markup=main_keyboard)
    await state.clear()

# Admin reply to feedback
@dp.message(Command('feedback_reply'))
async def feedback_reply_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer('🚫 У вас нет прав администратора!')
        return
    await message.answer('📝 Введите ID пользователя, чей отзыв вы хотите обработать:')
    await state.set_state(GameStates.admin_wait_feedback_id)

@dp.message(GameStates.admin_wait_feedback_id)
async def admin_wait_feedback_id(message: Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
        # Check if has unreplied feedback
        unreplied = [f for f in feedbacks if f['user_id'] == user_id and not f['replied']]
        if not unreplied:
            await message.answer(f'❌ Для пользователя {user_id} нет необработанных отзывов.')
            await state.clear()
            return
        await state.update_data(feedback_user_id=user_id)
        await message.answer(f'📝 Найден отзыв от ID {user_id}. Введите ваш ответ:')
        await state.set_state(GameStates.admin_wait_feedback_reply)
    except ValueError:
        await message.answer('❌ Введите корректный числовой ID!')
    except Exception as e:
        logging.error(f'Error in admin_wait_feedback_id: {e}')
        await message.answer('❌ Произошла ошибка.')

@dp.message(GameStates.admin_wait_feedback_reply)
async def admin_wait_feedback_reply(message: Message, state: FSMContext):
    data = await state.get_data()
    user_id = data['feedback_user_id']
    reply_text = message.text
    fb = next((f for f in feedbacks if f['user_id'] == user_id and not f['replied']), None)
    if fb:
        fb['reply'] = reply_text
        fb['replied'] = True
        save_data()
        try:
            await bot.send_message(user_id, f'📩 Ответ от администратора на ваш отзыв:\n\n{reply_text}\n\nСпасибо за обратную связь!')
            await message.answer(f'✅ Ответ отправлен пользователю {user_id}.')
        except Exception as e:
            logging.error(f'Failed to send reply to {user_id}: {e}')
            await message.answer(f'✅ Ответ сохранен, но не удалось отправить пользователю {user_id} (возможно, заблокировал бота).')
    else:
        await message.answer('❌ Отзыв не найден или уже обработан.')
    await state.clear()

async def play_roulette(message: Message, bet: int, roulette_type: str, multiplier: int, bet_number=None):
    user_id = message.from_user.id
    balance = get_balance(user_id)
    if balance < bet:
        try:
            await message.reply('💸 Недостаточно средств!')
        except Exception as e:
            logging.error(f'Error in roulette insufficient for {user_id}: {e}')
        return
    update_balance(user_id, -bet)
    stats['total_bets'] += bet
    try:
        await message.reply('🎡 Крутим рулетку...')
    except Exception as e:
        logging.error(f'Error starting roulette for {user_id}: {e}')
    try:
        msg = await message.reply('⏳')
    except Exception as e:
        logging.error(f'Error sending roulette loading for {user_id}: {e}')
        return
    # Spin animation
    try:
        for _ in range(10):
            temp_num = random.randint(0, 36)
            await msg.edit_text(f'🎡 {temp_num}')
            await asyncio.sleep(0.3)
    except Exception as e:
        logging.error(f'Error in roulette animation for {user_id}: {e}')
    # Final number
    final_num = random.randint(0, 36)
    win = False
    win_amount = 0
    if roulette_type == 'number':
        if final_num == bet_number:
            win = True
            win_amount = bet * multiplier
        result_text = f'Выпало: {final_num}\nВаше число: {bet_number}'
    else:
        # Color/even: 50% chance
        if random.random() < 0.5:
            win = True
            win_amount = bet * multiplier
        color = 'Красное' if (final_num % 2 == 0 and final_num != 0) else 'Черное' if final_num % 2 == 1 else 'Зеленое'
        even_odd = 'Четное' if final_num % 2 == 0 else 'Нечетное'
        result_text = f'Выпало: {final_num} ({color}, {even_odd})'
        if win:
            result_text += f'\n🎉 Вы выиграли на {roulette_type}! ${win_amount}'
        else:
            result_text += f'\n😔 Проигрыш на {roulette_type}.'
    if win:
        update_balance(user_id, win_amount)
        stats['total_wins'] += win_amount
        result = f'🎉 Вы выиграли ${win_amount}!'
    else:
        result = f'😔 Потеряли ${bet}.'
    new_balance = get_balance(user_id)
    full_text = f'{result_text}\n{result}\n💵 Баланс: ${new_balance}'
    try:
        await msg.edit_text(full_text)
    except Exception as e:
        logging.error(f'Error editing roulette result for {user_id}: {e}')
    # Send new menu
    roulette_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔴 Красное (x2)', callback_data='roulette_red')],
        [InlineKeyboardButton(text='⚫ Черное (x2)', callback_data='roulette_black')],
        [InlineKeyboardButton(text='📊 Четное (x2)', callback_data='roulette_even')],
        [InlineKeyboardButton(text='📊 Нечетное (x2)', callback_data='roulette_odd')],
        [InlineKeyboardButton(text='🎯 Число (x18)', callback_data='roulette_number')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    try:
        await bot.send_message(user_id, 'Хотите сыграть ещё?', reply_markup=roulette_keyboard)
    except Exception as e:
        logging.error(f'Error sending roulette menu for {user_id}: {e}')

@dp.callback_query(F.data == 'roulette_menu')
async def roulette_menu_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔴 Красное (x2)', callback_data='roulette_red')],
        [InlineKeyboardButton(text='⚫ Черное (x2)', callback_data='roulette_black')],
        [InlineKeyboardButton(text='📊 Четное (x2)', callback_data='roulette_even')],
        [InlineKeyboardButton(text='📊 Нечетное (x2)', callback_data='roulette_odd')],
        [InlineKeyboardButton(text='🎯 Число (x18)', callback_data='roulette_number')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    try:
        await callback.message.delete()
    except Exception as e:
        logging.error(f'Error deleting roulette menu callback for {user_id}: {e}')
    try:
        await bot.send_message(user_id, '🎡 Рулетка: Выберите тип ставки', reply_markup=keyboard)
    except Exception as e:
        logging.error(f'Error sending roulette menu for {user_id}: {e}')
    try:
        await callback.answer()
    except Exception as e:
        logging.error(f'Error answering roulette menu callback for {user_id}: {e}')

async def get_card():
    r = random.randint(1, 13)
    if r > 10:
        return 10
    if r == 1:
        return 11  # Ace as 11 initially
    return r

def hand_value(hand):
    value = sum(hand)
    aces = hand.count(11)
    while value > 21 and aces:
        value -= 10
        aces -= 1
    return value

async def start_blackjack(message_or_callback, bet: int, state: FSMContext):
    if isinstance(message_or_callback, CallbackQuery):
        user_id = message_or_callback.from_user.id
    else:
        user_id = message_or_callback.from_user.id
    balance = get_balance(user_id)
    if balance < bet:
        if hasattr(message_or_callback, 'reply'):
            await message_or_callback.reply('💸 Недостаточно!')
        else:
            await message_or_callback.message.reply('💸 Недостаточно!')
        return
    update_balance(user_id, -bet)
    stats['total_bets'] += bet
    player_hand = [await get_card(), await get_card()]
    dealer_hand = [await get_card(), await get_card()]  # Dealer second card hidden
    player_value = hand_value(player_hand)
    dealer_visible = dealer_hand[0]
    await state.update_data(bet=bet, player_hand=player_hand, dealer_hand=dealer_hand, player_value=player_value, user_id=user_id)
    text = f'♠️ Блэкджек\nВаша ставка: ${bet}\nВаша рука: {player_hand} (сумма: {player_value})\nДилер: {dealer_visible} + ?'
    if player_value == 21:
        # Blackjack
        dealer_value = hand_value(dealer_hand)
        if dealer_value == 21:
            # Push
            update_balance(user_id, bet)
            text += '\nНичья! Возврат ставки.'
        else:
            win_amount = int(bet * 1.5)
            update_balance(user_id, bet + win_amount)
            stats['total_wins'] += bet + win_amount
            text += f'\nБлэкджек! Вы выиграли ${bet + win_amount}!'
        new_balance = get_balance(user_id)
        text += f'\n💵 Баланс: ${new_balance}'
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='♠️ Снова', callback_data='blackjack_menu')],
            [InlineKeyboardButton(text='🔙 Главное', callback_data='back_main')]
        ])
        await bot.send_message(user_id, text, reply_markup=keyboard)
        await state.clear()
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Хит (взять карту)', callback_data='blackjack_hit')],
        [InlineKeyboardButton(text='Стенд (остановиться)', callback_data='blackjack_stand')],
        [InlineKeyboardButton(text='🔙 Главное', callback_data='back_main')]
    ])
    await bot.send_message(user_id, text, reply_markup=keyboard)
    await state.set_state(GameStates.blackjack_playing)

@dp.callback_query(F.data == 'blackjack_hit')
async def blackjack_hit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    player_hand = data['player_hand']
    player_hand.append(await get_card())
    player_value = hand_value(player_hand)
    data['player_hand'] = player_hand
    data['player_value'] = player_value
    await state.set_data(data)
    text = f'♠️ Ваша рука: {player_hand} (сумма: {player_value})\nДилер: {data["dealer_hand"][0]} + ?'
    if player_value > 21:
        text += '\n💥 Баст! Вы проиграли.'
        dealer_value = hand_value(data['dealer_hand'])
        text += f'\nДилер: {data["dealer_hand"]} (сумма: {dealer_value})'
        new_balance = get_balance(callback.from_user.id)
        text += f'\n💵 Баланс: ${new_balance}'
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='♠️ Снова', callback_data='blackjack_menu')],
            [InlineKeyboardButton(text='🔙 Главное', callback_data='back_main')]
        ])
        await callback.message.edit_text(text, reply_markup=keyboard)
        await state.clear()
        await callback.answer()
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Хит', callback_data='blackjack_hit')],
        [InlineKeyboardButton(text='Стенд', callback_data='blackjack_stand')],
        [InlineKeyboardButton(text='🔙 Главное', callback_data='back_main')]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == 'blackjack_stand')
async def blackjack_stand(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    dealer_hand = data['dealer_hand']
    player_value = data['player_value']
    bet = data['bet']
    user_id = data['user_id']
    while hand_value(dealer_hand) < 17:
        dealer_hand.append(await get_card())
    dealer_value = hand_value(dealer_hand)
    text = f'♠️ Ваша рука: {data["player_hand"]} (сумма: {player_value})\nДилер: {dealer_hand} (сумма: {dealer_value})'
    if dealer_value > 21 or player_value > dealer_value:
        win_amount = bet
        update_balance(user_id, win_amount * 2)  # Return bet + win
        stats['total_wins'] += win_amount
        result = f'Вы выиграли ${win_amount}!'
    elif player_value == dealer_value:
        update_balance(user_id, bet)  # Push
        result = 'Ничья! Возврат ставки.'
    else:
        result = 'Дилер выиграл.'
    new_balance = get_balance(user_id)
    text += f'\n{result}\n💵 Баланс: ${new_balance}'
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='♠️ Снова', callback_data='blackjack_menu')],
        [InlineKeyboardButton(text='🔙 Главное', callback_data='back_main')]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard)
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == 'blackjack_menu')
async def blackjack_menu_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💎 Low 50$', callback_data='blackjack_50')],
        [InlineKeyboardButton(text='💎 Med 200$', callback_data='blackjack_200')],
        [InlineKeyboardButton(text='💎 High 500$', callback_data='blackjack_500')],
        [InlineKeyboardButton(text='🎯 Custom', callback_data='blackjack_custom')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    try:
        await callback.message.delete()
    except Exception as e:
        logging.error(f'Error deleting blackjack menu callback for {user_id}: {e}')
    try:
        await bot.send_message(user_id, '♠️ Блэкджек: Выберите ставку', reply_markup=keyboard)
        await state.set_state(GameStates.waiting_blackjack_bet)
    except Exception as e:
        logging.error(f'Error sending blackjack menu for {user_id}: {e}')
    try:
        await callback.answer()
    except Exception as e:
        logging.error(f'Error answering blackjack menu callback for {user_id}: {e}')

async def play_sport(message: Message, bet: int, sport_type: str, choice: str):
    user_id = message.from_user.id
    balance = get_balance(user_id)
    if balance < bet:
        try:
            await message.reply('💸 Недостаточно!')
        except Exception as e:
            logging.error(f'Error in sport insufficient for {user_id}: {e}')
        return
    update_balance(user_id, -bet)
    stats['total_bets'] += bet
    try:
        await message.reply('⚽ Матч начинается...')
    except Exception as e:
        logging.error(f'Error starting sport for {user_id}: {e}')
    try:
        msg = await message.reply('⏳')
    except Exception as e:
        logging.error(f'Error sending sport loading for {user_id}: {e}')
        return
    # Animation
    try:
        for _ in range(5):
            await msg.edit_text('⚽ Игра...')
            await asyncio.sleep(0.5)
    except Exception as e:
        logging.error(f'Error in sport animation for {user_id}: {e}')

    if sport_type == 'team':
        # Simulate with draw: ~33% each
        outcomes = ['a', 'b', 'draw']
        weights = [0.33, 0.33, 0.34]
        outcome = random.choices(outcomes, weights=weights)[0]
        multiplier = 2
        if outcome == 'draw':
            update_balance(user_id, bet)  # Refund
            result = f'🤝 Ничья! Ставка возвращена (${bet}).'
            display_outcome = 'Ничья'
        elif outcome == choice:
            win_amount = bet * multiplier
            update_balance(user_id, win_amount)
            stats['total_wins'] += win_amount
            result = f'🎉 Команда {choice.upper()} победила! Вы выиграли ${win_amount}!'
            display_outcome = f'Команда {choice.upper()}'
        else:
            result = f'😔 Команда {outcome.upper()} победила. Потеряли ${bet}.'
            display_outcome = f'Команда {outcome.upper()}'
        full_text = f'Исход: {display_outcome}\n{result}\n💵 Баланс: ${get_balance(user_id)}'
        sport_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='🏆 Команда A (x2)', callback_data='sport_a')],
            [InlineKeyboardButton(text='🏆 Команда B (x2)', callback_data='sport_b')],
            [InlineKeyboardButton(text='📊 Over 2.5 голов (x1.8)', callback_data='sport_over')],
            [InlineKeyboardButton(text='📊 Under 2.5 голов (x1.8)', callback_data='sport_under')],
            [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
        ])
    else:  # overunder
        # Simulate goals for two teams: randint(0,6) each
        goals_a = random.randint(0, 6)
        goals_b = random.randint(0, 6)
        total_goals = goals_a + goals_b
        is_over = total_goals > 2
        multiplier = 1.8
        win_amount = int(bet * multiplier)
        if (choice == 'over' and is_over) or (choice == 'under' and not is_over):
            update_balance(user_id, win_amount)
            stats['total_wins'] += win_amount
            result = f'🎉 {choice.upper()} 2.5! Вы выиграли ${win_amount}!'
            display_outcome = f'{total_goals} голов ({goals_a}:{goals_b})'
        else:
            result = f'😔 { "Over" if choice == "under" else "Under" } 2.5. Потеряли ${bet}.'
            display_outcome = f'{total_goals} голов ({goals_a}:{goals_b})'
        full_text = f'Счёт: {display_outcome}\n{result}\n💵 Баланс: ${get_balance(user_id)}'
        sport_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='🏆 Команда A (x2)', callback_data='sport_a')],
            [InlineKeyboardButton(text='🏆 Команда B (x2)', callback_data='sport_b')],
            [InlineKeyboardButton(text='📊 Over 2.5 голов (x1.8)', callback_data='sport_over')],
            [InlineKeyboardButton(text='📊 Under 2.5 голов (x1.8)', callback_data='sport_under')],
            [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
        ])

    try:
        await msg.edit_text(full_text)
    except Exception as e:
        logging.error(f'Error editing sport result for {user_id}: {e}')
    try:
        await bot.send_message(user_id, 'Хотите сыграть ещё?', reply_markup=sport_keyboard)
    except Exception as e:
        logging.error(f'Error sending sport menu for {user_id}: {e}')

def get_card_poker():
    rank = random.randint(2, 14)
    suit = random.choice(['♥', '♦', '♣', '♠'])
    return (rank, suit)

def evaluate_poker_hand(hand):
    ranks = sorted([card[0] for card in hand])  # int 2-14
    suits = [card[1] for card in hand]
    rank_counts = {}
    for r in ranks:
        rank_counts[r] = rank_counts.get(r, 0) + 1
    suits_set = set(suits)
    is_flush = len(suits_set) == 1
    is_straight = (len(set(ranks)) == 5 and ranks[-1] - ranks[0] == 4) or (set(ranks) == {2,3,4,5,14})
    counts = sorted(rank_counts.values(), reverse=True)
    # Royal flush
    if is_straight and is_flush and set(ranks) == {10,11,12,13,14}:
        return 250
    # Straight flush
    if is_straight and is_flush:
        return 100
    # Four of a kind
    if counts[0] == 4:
        return 50
    # Full house
    if counts == [3, 2]:
        return 25
    # Flush
    if is_flush:
        return 15
    # Straight
    if is_straight:
        return 10
    # Three of a kind
    if counts[0] == 3:
        return 6
    # Two pair
    if counts == [2, 2, 1]:
        return 4
    # Pair
    if counts[0] == 2:
        return 2
    return 1  # High card

async def play_poker(message_or_callback, bet: int, state: FSMContext):
    if isinstance(message_or_callback, CallbackQuery):
        user_id = message_or_callback.from_user.id
        msg = message_or_callback.message
        answer = message_or_callback.answer
    else:
        user_id = message_or_callback.from_user.id
        msg = message_or_callback
        answer = lambda *args, **kwargs: None
    try:
        balance = get_balance(user_id)
        if balance < bet:
            if isinstance(message_or_callback, CallbackQuery):
                await answer('💸 Недостаточно!')
            else:
                await msg.reply('💸 Недостаточно!')
            return
        update_balance(user_id, -bet)
        stats['total_bets'] += bet
        hand = [get_card_poker() for _ in range(5)]
        hand_value = evaluate_poker_hand(hand)
        rank_str = lambda r: str(r) if r <= 10 else ('J' if r==11 else ('Q' if r==12 else ('K' if r==13 else 'A')))
        cards_str = ' '.join([f'{rank_str(r)}{s}' for r, s in hand])
        multipliers = {1: 1, 2: 2, 4: 3, 6: 4, 10: 5, 15: 6, 25: 10, 50: 15, 100: 25, 250: 50}
        multiplier = multipliers.get(hand_value, 1)
        base_chance = max(0.1, 0.5 - (multiplier * 0.005))
        if random.random() < base_chance:
            win_amount = bet * multiplier
            update_balance(user_id, win_amount)
            stats['total_wins'] += win_amount
            result = f'🎉 Рука: {hand_value} (x{multiplier})! +${win_amount}'
        else:
            result = f'😔 Рука: {hand_value} (x{multiplier}). -${bet}'
        new_balance = get_balance(user_id)
        text = f'♦️ Покер\nСтавка: ${bet}\nКарты: {cards_str}\n{result}\n💵 ${new_balance}'
        if isinstance(message_or_callback, CallbackQuery):
            await msg.reply(text)
        else:
            await msg.reply(text)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='♦️ Снова', callback_data='poker_menu')],
            [InlineKeyboardButton(text='🔙 Главное', callback_data='back_main')]
        ])
        await bot.send_message(user_id, 'Ещё?', reply_markup=keyboard)
        await state.clear()
    except Exception as e:
        logging.error(f'Error in play_poker for {user_id}: {e}')
        if isinstance(message_or_callback, CallbackQuery):
            await answer('Ошибка ставки!')
        else:
            await msg.reply('Ошибка ставки! Попробуйте снова.')
        await state.clear()

@dp.callback_query(F.data == 'sport_menu')
async def sport_menu_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🏆 Команда A (x2)', callback_data='sport_a')],
        [InlineKeyboardButton(text='🏆 Команда B (x2)', callback_data='sport_b')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    try:
        await callback.message.delete()
    except Exception as e:
        logging.error(f'Error deleting sport menu callback for {user_id}: {e}')
    try:
        await bot.send_message(user_id, '⚽ Спорт: Выберите команду для ставки (50/50 шанс)', reply_markup=keyboard)
    except Exception as e:
        logging.error(f'Error sending sport menu for {user_id}: {e}')
    try:
        await callback.answer()
    except Exception as e:
        logging.error(f'Error answering sport menu callback for {user_id}: {e}')

@dp.callback_query(F.data.startswith('accept_duel_'))
async def accept_duel(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split('_')
    if len(parts) < 5:
        await callback.answer('Неверные данные!')
        return
    initiator_id = int(parts[2])
    opp_id = int(parts[3])
    bet = int(parts[4])
    chat_id_str = '_'.join(parts[5:]) if len(parts) > 5 else None
    chat_id = int(chat_id_str) if chat_id_str and chat_id_str.isdigit() else None
    if initiator_id in pending_duels:
        duel_data = pending_duels[initiator_id]
        if duel_data['opp'] == opp_id and duel_data['bet'] == bet:
            del pending_duels[initiator_id]
            update_balance(initiator_id, -bet)
            update_balance(opp_id, -bet)
            stats['total_bets'] += bet * 2
            duel_id = f"{min(initiator_id, opp_id)}_{max(initiator_id, opp_id)}"
            chat_id_final = duel_data.get('chat_id') or chat_id
            pending_duels[duel_id] = {
                'player1': initiator_id, 'player2': opp_id, 'bet': bet, 'mode': duel_data.get('mode', 'slots'), 'chat_id': chat_id_final,
                'scores': {initiator_id: 0, opp_id: 0}, 'current_turn': initiator_id
            }
            save_data()
            mode = duel_data.get('mode', 'slots')
            mode_text = {'slots': 'крутить слоты', 'roulette': 'выбрать в рулетке', 'coin': 'бросить монетку'}.get(mode, 'играть')
            await callback.message.edit_text(f'⚔️ Дуэль ({mode}) принята! Ставка: ${bet}')
            init_name = get_opponent_name(opp_id)
            text_init = f"⚔️ Дуэль ({mode}) принята {init_name}! Ставка: ${bet}\nСчёт: 0-0\nВаша очередь {mode_text}."
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text='Играть', callback_data=f'duel_turn_{duel_id}')]
            ])
            if chat_id_final:
                await bot.send_message(chat_id_final, f"⚔️ Дуэль между @{init_name} и @{get_opponent_name(initiator_id)} начата! Ставка: ${bet}\nСчёт: 0-0\nОчередь: {init_name}", reply_markup=keyboard)
                await bot.send_message(initiator_id, text_init, reply_markup=keyboard)
            else:
                await bot.send_message(initiator_id, text_init, reply_markup=keyboard)
            opp_name = get_opponent_name(initiator_id)
            text_opp = f"⚔️ Вы приняли дуэль ({mode}) с {opp_name}! Ставка: ${bet}\nСчёт: 0-0\nЖдите."
            if chat_id_final:
                await bot.send_message(chat_id_final, text_opp)
            else:
                await bot.send_message(opp_id, text_opp)
            await callback.answer('Дуэль начата!')
        else:
            await callback.answer('Приглашение не найдено!')
    else:
        await callback.answer('Устарело!')

# Unified duel turn handler (replaces random1/2, initiator/opp)
@dp.callback_query(F.data.startswith('duel_turn_'))
async def duel_turn_handler(callback: CallbackQuery):
    duel_id = callback.data.split('_', 2)[2]
    if duel_id not in pending_duels:
        await callback.answer('Дуэль не найдена!')
        return
    duel_data = pending_duels[duel_id]
    user_id = callback.from_user.id
    if duel_data['current_turn'] != user_id:
        await callback.answer('Не ваша очередь!')
        return
    opp_id = duel_data['player1'] if user_id == duel_data['player2'] else duel_data['player2']
    bet = duel_data['bet']
    mode = duel_data['mode']
    chat_id = duel_data.get('chat_id')
    score = 0
    result_text = ''
    if mode == 'slots':
        symbols = ['🍒', '🍋', '🍊', '🔔', '⭐', '7️⃣']
        slot1, slot2, slot3 = [random.choice(symbols) for _ in range(3)]
        if slot1 == slot2 == slot3:
            score = 15 if slot1 == '7️⃣' else 10
        elif slot1 == slot2 or slot2 == slot3 or slot1 == slot3:
            score = random.randint(4, 7)
        else:
            score = random.randint(1, 5)
        result_text = f"Комбо: {slot1} | {slot2} | {slot3} (счёт {score})"
    elif mode == 'roulette':
        choice = random.choice(['red', 'black'])
        outcome = random.choice(['red', 'black'])
        score = 2 if choice == outcome else 0
        result_text = f"Выбор: {choice}, Выпало: {outcome} (счёт {score})"
    elif mode == 'coin':
        choice = random.choice(['heads', 'tails'])
        outcome = random.choice(['heads', 'tails'])
        score = 1 if choice == outcome else 0
        result_text = f"Монетка: {choice}, Выпало: {outcome} (счёт {score})"
    duel_data['scores'][user_id] = score
    save_data()
    my_score = score
    opp_score = duel_data['scores'][opp_id]
    opp_name = get_opponent_name(opp_id)
    my_name = get_opponent_name(user_id)
    text = f"⚔️ Ход @{my_name}: {result_text}\nСтавка: ${bet}\nСчёт: @{my_name} {my_score} - @{opp_name} {opp_score}"
    if chat_id:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[])
        await bot.send_message(chat_id, text, reply_markup=keyboard)
    else:
        await callback.message.edit_text(text)
    if opp_score > 0:
        await end_duel_unified(duel_id, duel_data, user_id, opp_id, bet)
        del pending_duels[duel_id]
        save_data()
        return
    duel_data['current_turn'] = opp_id
    save_data()
    mode_text = {'slots': 'крутить', 'roulette': 'выбрать', 'coin': 'бросить'}.get(mode, 'играть')
    text_opp = f"⚔️ Ваша очередь ({mode_text}), @{my_name}! Ставка: ${bet}\nСчёт: Вы {opp_score} - Оппонент {my_score}"
    keyboard_opp = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f'{mode_text.capitalize()}', callback_data=f'duel_turn_{duel_id}')]
    ])
    if chat_id:
        await bot.send_message(chat_id, f"Очередь: @{get_opponent_name(opp_id)}", reply_markup=keyboard_opp)
        await bot.send_message(opp_id, text_opp, reply_markup=keyboard_opp)
    else:
        await bot.send_message(opp_id, text_opp, reply_markup=keyboard_opp)
        await bot.send_message(user_id, f"Ход {opp_name}. Ждите.")
    await callback.answer()

async def end_duel_unified(duel_id, duel_data, last_player, opp_id, bet):
    scores = duel_data['scores']
    player1_score = scores[duel_data['player1']]
    player2_score = scores[duel_data['player2']]
    mode = duel_data['mode']
    chat_id = duel_data.get('chat_id')
    if player1_score > player2_score:
        winner = duel_data['player1']
        loser = duel_data['player2']
    elif player2_score > player1_score:
        winner = duel_data['player2']
        loser = duel_data['player1']
    else:
        update_balance(duel_data['player1'], bet)
        update_balance(duel_data['player2'], bet)
        winner_name = get_opponent_name(duel_data['player1'])
        loser_name = get_opponent_name(duel_data['player2'])
        result_text = f"⚔️ Ничья в дуэли ({mode}) между @{winner_name} и @{loser_name}! Ставка возвращена."
        if chat_id:
            await bot.send_message(chat_id, result_text)
        else:
            await bot.send_message(duel_data['player1'], f"⚔️ Ничья ({mode})! +${bet}")
            await bot.send_message(duel_data['player2'], f"⚔️ Ничья ({mode})! +${bet}")
        return
    win_amount = bet * 2
    update_balance(winner, win_amount)
    stats['total_wins'] += win_amount
    save_data()
    winner_name = get_opponent_name(winner)
    loser_name = get_opponent_name(loser)
    result_text = f"⚔️ @{winner_name} выиграл дуэль ({mode}) против @{loser_name}! +${win_amount}"
    if chat_id:
        await bot.send_message(chat_id, result_text)
        await bot.send_message(loser, f"⚔️ Проиграли ({mode}) {winner_name}. -${bet}")
    else:
        await bot.send_message(winner, f"⚔️ Вы выиграли ({mode}) против {loser_name}! +${win_amount}")
        await bot.send_message(loser, f"⚔️ Проиграли ({mode}) {winner_name}. -${bet}")

# Remove old random handlers
# @dp.callback_query(F.data == 'duel_turn_random1') ... (remove entire block)
# @dp.callback_query(F.data == 'duel_turn_random2') ... (remove)
# async def end_duel ... (replace with unified above)

# Placeholder for other games
@dp.callback_query(F.data == 'poker_menu')
async def poker_menu_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💎 Low 50$', callback_data='poker_50')],
        [InlineKeyboardButton(text='💎 Med 200$', callback_data='poker_200')],
        [InlineKeyboardButton(text='💎 High 500$', callback_data='poker_500')],
        [InlineKeyboardButton(text='🎯 Custom', callback_data='poker_custom')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back_main')]
    ])
    try:
        await callback.message.delete()
    except Exception as e:
        logging.error(f'Error deleting poker menu callback for {user_id}: {e}')
    try:
        await bot.send_message(user_id, '♦️ Покер: Выберите ставку', reply_markup=keyboard)
        await state.set_state(GameStates.waiting_poker_bet)
    except Exception as e:
        logging.error(f'Error sending poker menu for {user_id}: {e}')
    try:
        await callback.answer()
    except Exception as e:
        logging.error(f'Error answering poker menu callback for {user_id}: {e}')


async def main():
    def run_dashboard():
        app.run(host='0.0.0.0', port=5000, debug=False)
    
    dashboard_thread = threading.Thread(target=run_dashboard)
    dashboard_thread.daemon = True
    dashboard_thread.start()
    logging.info('Dashboard started at http://localhost:5000')
    
    try:
        # Periodic save every 30s
        async def periodic_save():
            while True:
                await asyncio.sleep(30)
                save_data()
        asyncio.create_task(periodic_save())
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logging.info('Bot stopped by user')
    except Exception as e:
        logging.error(f'Error in main polling: {e}')
    finally:
        save_data()
        logging.info('Bot shutdown, final save completed')

if __name__ == '__main__':
    asyncio.run(main())