import os
import logging
import asyncio
import sqlite3
import random
import threading
import time
from datetime import datetime
from typing import Optional
from collections import deque
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# SQLite database setup
def init_db():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    
    # Create users table
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER PRIMARY KEY,
                  is_active INTEGER DEFAULT 0,
                  current_chat INTEGER,
                  last_activity TEXT,
                  country TEXT,
                  language TEXT,
                  gender TEXT,
                  karma INTEGER DEFAULT 0,
                  total_chats INTEGER DEFAULT 0,
                  positive_ratings INTEGER DEFAULT 0,
                  current_streak INTEGER DEFAULT 0,
                  best_streak INTEGER DEFAULT 0,
                  achievements TEXT DEFAULT '',
                  reveal_requested INTEGER DEFAULT 0,
                  username TEXT)''')
    
    # Add new columns if they don't exist (for existing databases)
    try:
        c.execute('ALTER TABLE users ADD COLUMN karma INTEGER DEFAULT 0')
    except: pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN total_chats INTEGER DEFAULT 0')
    except: pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN positive_ratings INTEGER DEFAULT 0')
    except: pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN current_streak INTEGER DEFAULT 0')
    except: pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN best_streak INTEGER DEFAULT 0')
    except: pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN achievements TEXT DEFAULT ""')
    except: pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN reveal_requested INTEGER DEFAULT 0')
    except: pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN username TEXT')
    except: pass
    
    conn.commit()
    conn.close()

# Initialize database
init_db()

# Constants
INACTIVITY_TIMEOUT = 3600  # 1 hour in seconds
MATCH_TIMEOUT = 120  # 2 minutes in seconds
MESSAGE_RATE_LIMIT = 30  # messages per second
MESSAGE_QUEUE = deque()
MESSAGE_QUEUE_LOCK = threading.Lock()
MESSAGE_QUEUE_THREAD = None
RUNNING = True
BOT_INSTANCE = None
QUEUE_LOOP = None

# Achievement definitions
ACHIEVEMENTS = {
    'first_chat': {'name': '🎉 First Chat', 'desc': 'Complete your first chat', 'requirement': lambda u: u.total_chats >= 1},
    'social_butterfly': {'name': '🦋 Social Butterfly', 'desc': 'Complete 10 chats', 'requirement': lambda u: u.total_chats >= 10},
    'chat_master': {'name': '👑 Chat Master', 'desc': 'Complete 50 chats', 'requirement': lambda u: u.total_chats >= 50},
    'legend': {'name': '🏆 Legend', 'desc': 'Complete 100 chats', 'requirement': lambda u: u.total_chats >= 100},
    'streak_starter': {'name': '🔥 Streak Starter', 'desc': 'Get a 3-chat streak', 'requirement': lambda u: u.best_streak >= 3},
    'on_fire': {'name': '💥 On Fire', 'desc': 'Get a 10-chat streak', 'requirement': lambda u: u.best_streak >= 10},
    'unstoppable': {'name': '⚡ Unstoppable', 'desc': 'Get a 25-chat streak', 'requirement': lambda u: u.best_streak >= 25},
    'loved': {'name': '❤️ Loved', 'desc': 'Receive 10 positive ratings', 'requirement': lambda u: u.positive_ratings >= 10},
    'superstar': {'name': '⭐ Superstar', 'desc': 'Receive 50 positive ratings', 'requirement': lambda u: u.positive_ratings >= 50},
    'karma_king': {'name': '👼 Karma King', 'desc': 'Reach 100 karma points', 'requirement': lambda u: u.karma >= 100},
}

def check_and_award_achievements(user_state) -> list:
    """Check and award new achievements. Returns list of newly earned achievements."""
    new_achievements = []
    current_achievements = user_state.achievements.split(',') if user_state.achievements else []
    
    for achievement_id, achievement in ACHIEVEMENTS.items():
        if achievement_id not in current_achievements:
            if achievement['requirement'](user_state):
                current_achievements.append(achievement_id)
                new_achievements.append(achievement)
    
    user_state.achievements = ','.join([a for a in current_achievements if a])
    return new_achievements

def get_karma_title(karma: int) -> str:
    """Get a title based on karma level."""
    if karma < 0:
        return "😈 Troublemaker"
    elif karma < 10:
        return "🌱 Newcomer"
    elif karma < 50:
        return "😊 Friendly"
    elif karma < 100:
        return "🌟 Popular"
    elif karma < 250:
        return "💎 Trusted"
    else:
        return "👑 Elite"

class UserState:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.is_active = False
        self.current_chat = None
        self.last_activity = datetime.now()
        self.settings = {
            'country': None,
            'language': None,
            'gender': None
        }
        self.match_start_time = None
        # New fields for karma, streaks, achievements
        self.karma = 0
        self.total_chats = 0
        self.positive_ratings = 0
        self.current_streak = 0
        self.best_streak = 0
        self.achievements = ''
        self.reveal_requested = False
        self.username = None

    def to_dict(self) -> dict:
        return {
            'is_active': self.is_active,
            'current_chat': self.current_chat,
            'last_activity': self.last_activity.isoformat(),
            'country': self.settings['country'],
            'language': self.settings['language'],
            'gender': self.settings['gender'],
            'karma': self.karma,
            'total_chats': self.total_chats,
            'positive_ratings': self.positive_ratings,
            'current_streak': self.current_streak,
            'best_streak': self.best_streak,
            'achievements': self.achievements,
            'reveal_requested': self.reveal_requested,
            'username': self.username
        }

    @classmethod
    def from_dict(cls, user_id: int, data: dict) -> 'UserState':
        state = cls(user_id)
        state.is_active = bool(data.get('is_active', 0))
        state.current_chat = data.get('current_chat')
        state.last_activity = datetime.fromisoformat(data.get('last_activity', datetime.now().isoformat()))
        state.settings = {
            'country': data.get('country'),
            'language': data.get('language'),
            'gender': data.get('gender')
        }
        state.match_start_time = data.get('match_start_time')
        state.karma = data.get('karma', 0) or 0
        state.total_chats = data.get('total_chats', 0) or 0
        state.positive_ratings = data.get('positive_ratings', 0) or 0
        state.current_streak = data.get('current_streak', 0) or 0
        state.best_streak = data.get('best_streak', 0) or 0
        state.achievements = data.get('achievements', '') or ''
        state.reveal_requested = bool(data.get('reveal_requested', 0))
        state.username = data.get('username')
        return state

def get_user_state(user_id: int) -> UserState:
    """Get user state from SQLite or create new one."""
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    
    c.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    data = c.fetchone()
    conn.close()
    
    if data:
        # Convert tuple to dict
        columns = ['user_id', 'is_active', 'current_chat', 'last_activity', 'country', 'language', 'gender',
                   'karma', 'total_chats', 'positive_ratings', 'current_streak', 'best_streak', 
                   'achievements', 'reveal_requested', 'username']
        data_dict = dict(zip(columns, data))
        return UserState.from_dict(user_id, data_dict)
    return UserState(user_id)

def save_user_state(user_state: UserState) -> None:
    """Save user state to SQLite."""
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    
    data = user_state.to_dict()
    c.execute('''INSERT OR REPLACE INTO users 
                 (user_id, is_active, current_chat, last_activity, country, language, gender,
                  karma, total_chats, positive_ratings, current_streak, best_streak, 
                  achievements, reveal_requested, username)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (user_state.user_id,
               int(data['is_active']),
               data['current_chat'],
               data['last_activity'],
               data['country'],
               data['language'],
               data['gender'],
               data['karma'],
               data['total_chats'],
               data['positive_ratings'],
               data['current_streak'],
               data['best_streak'],
               data['achievements'],
               int(data['reveal_requested']),
               data['username']))
    
    conn.commit()
    conn.close()

def check_compatibility(user1_state: UserState, user2_state: UserState) -> bool:
    """Check if two users are compatible based on their settings."""
    # If either user has no preferences set, consider them compatible
    if not user1_state.settings or not user2_state.settings:
        return True
    
    # Check gender preference if set
    if user1_state.settings.get('gender') and user2_state.settings.get('gender'):
        if user1_state.settings['gender'] != user2_state.settings['gender']:
            return False
    
    # Check language preference if set
    if user1_state.settings.get('language') and user2_state.settings.get('language'):
        if user1_state.settings['language'] != user2_state.settings['language']:
            return False
    
    # Check country preference if set
    if user1_state.settings.get('country') and user2_state.settings.get('country'):
        if user1_state.settings['country'] != user2_state.settings['country']:
            return False
    
    return True

def find_random_match(user_id: int) -> Optional[int]:
    """Find a random active user who is not the current user and matches preferences."""
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    
    # Get current user's state
    current_user_state = get_user_state(user_id)
    
    # Check if user has been waiting too long for a match
    if current_user_state.match_start_time:
        wait_time = (datetime.now() - current_user_state.match_start_time).total_seconds()
        if wait_time > MATCH_TIMEOUT:
            # Reset match start time
            current_user_state.match_start_time = None
            save_user_state(current_user_state)
            conn.close()
            return None
    
    # Get all active users except the current user who are not in a chat
    c.execute('''SELECT user_id FROM users 
                 WHERE is_active = 1 
                 AND current_chat IS NULL 
                 AND user_id != ?''', (user_id,))
    active_users = c.fetchall()
    
    # Shuffle the list to randomize
    random.shuffle(active_users)
    
    # Try to find a compatible match
    for (potential_match_id,) in active_users:
        potential_match_state = get_user_state(potential_match_id)
        
        # Check if users are compatible
        if check_compatibility(current_user_state, potential_match_state):
            # Reset match start time for both users
            current_user_state.match_start_time = None
            potential_match_state.match_start_time = None
            save_user_state(current_user_state)
            save_user_state(potential_match_state)
            conn.close()
            return potential_match_id
    
    # If no match found and user wasn't already waiting, set match start time
    if not current_user_state.match_start_time:
        current_user_state.match_start_time = datetime.now()
        save_user_state(current_user_state)
    
    conn.close()
    return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    
    # Save username for potential reveal later
    user_state = get_user_state(user.id)
    if user.username:
        user_state.username = user.username
        save_user_state(user_state)
    
    description = (
        "🎭 **Welcome to Anonymous P2P Chat Bot!**\n\n"
        "**How it works:**\n"
        "• Chat anonymously with random users\n"
        "• Set preferences to match with similar users\n"
        "• Earn karma, streaks, and achievements!\n"
        "• Only text and photos allowed\n\n"
        "**Commands:**\n"
        "/start - Show this menu\n"
        "/end - End current chat (& rate partner)\n"
        "/reveal - Request mutual profile reveal\n"
        "/typing - Show typing indicator\n\n"
        "**New Features:**\n"
        "⭐ Karma System - Get rated by chat partners\n"
        "🔥 Streaks - Build consecutive positive chats\n"
        "🏆 Achievements - Unlock badges as you chat\n"
        "🤝 Profile Reveal - Share profiles if both agree\n\n"
        "Use the menu buttons below. Enjoy chatting!"
    )
    await update.message.reply_text(description, parse_mode='Markdown')
    await show_main_menu(update, context)

def get_main_menu_markup(is_active: bool) -> InlineKeyboardMarkup:
    """Get the main menu markup."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Show Active Users", callback_data='show_active'),
            InlineKeyboardButton(f"Status: {'🟢 Online' if is_active else '🔴 Offline'}", 
                               callback_data='toggle_active')
        ],
        [
            InlineKeyboardButton("Settings", callback_data='settings'),
            InlineKeyboardButton("Find Match", callback_data='find_match')
        ],
        [
            InlineKeyboardButton("📊 My Profile", callback_data='my_profile'),
            InlineKeyboardButton("🏆 Achievements", callback_data='achievements')
        ]
    ])

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the main menu with current status."""
    user_id = update.effective_user.id
    user_state = get_user_state(user_id)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "Main Menu - Use the buttons below to interact with the bot.",
            reply_markup=get_main_menu_markup(user_state.is_active)
        )
    else:
        await update.message.reply_text(
            "Main Menu - Use the buttons below to interact with the bot.",
            reply_markup=get_main_menu_markup(user_state.is_active)
        )

async def show_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the settings menu."""
    user_id = update.effective_user.id
    user_state = get_user_state(user_id)
    
    keyboard = [
        [
            InlineKeyboardButton(f"Country: {user_state.settings['country'] or 'Not set'}", 
                               callback_data='set_country'),
            InlineKeyboardButton("Clear", callback_data='clear_country')
        ],
        [
            InlineKeyboardButton(f"Language: {user_state.settings['language'] or 'Not set'}", 
                               callback_data='set_language'),
            InlineKeyboardButton("Clear", callback_data='clear_language')
        ],
        [
            InlineKeyboardButton(f"Gender: {user_state.settings['gender'] or 'Not set'}", 
                               callback_data='set_gender'),
            InlineKeyboardButton("Clear", callback_data='clear_gender')
        ],
        [
            InlineKeyboardButton("Back to Main Menu", callback_data='back_to_main')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "Settings Menu - Select an option to change or clear:",
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            "Settings Menu - Select an option to change or clear:",
            reply_markup=reply_markup
        )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button presses."""
    query = update.callback_query
    await query.answer()

    if query.data == 'show_active':
        conn = sqlite3.connect('database.db')
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM users WHERE is_active = 1')
        count = c.fetchone()[0]
        conn.close()
        await query.edit_message_text(f"Active users: {count}")
        # Show main menu again after 2 seconds
        time.sleep(2)
        await show_main_menu(update, context)
    
    elif query.data == 'toggle_active':
        user_id = query.from_user.id
        user_state = get_user_state(user_id)
        user_state.is_active = not user_state.is_active
        save_user_state(user_state)
        await show_main_menu(update, context)

    elif query.data == 'settings':
        await show_settings_menu(update, context)

    elif query.data == 'back_to_main':
        await show_main_menu(update, context)

    elif query.data == 'clear_country':
        user_id = query.from_user.id
        user_state = get_user_state(user_id)
        user_state.settings['country'] = None
        save_user_state(user_state)
        await show_settings_menu(update, context)

    elif query.data == 'clear_language':
        user_id = query.from_user.id
        user_state = get_user_state(user_id)
        user_state.settings['language'] = None
        save_user_state(user_state)
        await show_settings_menu(update, context)

    elif query.data == 'clear_gender':
        user_id = query.from_user.id
        user_state = get_user_state(user_id)
        user_state.settings['gender'] = None
        save_user_state(user_state)
        await show_settings_menu(update, context)

    elif query.data == 'set_country':
        await query.edit_message_text(
            "Please enter your country (e.g., USA, UK, etc.):",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Back to Settings", callback_data='settings')
            ]])
        )
        context.user_data['awaiting_input'] = 'country'

    elif query.data == 'set_language':
        # Create keyboard with top 15 languages
        keyboard = [
            [
                InlineKeyboardButton("English", callback_data='lang_english'),
                InlineKeyboardButton("Mandarin", callback_data='lang_mandarin')
            ],
            [
                InlineKeyboardButton("Hindi", callback_data='lang_hindi'),
                InlineKeyboardButton("Spanish", callback_data='lang_spanish')
            ],
            [
                InlineKeyboardButton("French", callback_data='lang_french'),
                InlineKeyboardButton("Arabic", callback_data='lang_arabic')
            ],
            [
                InlineKeyboardButton("Bengali", callback_data='lang_bengali'),
                InlineKeyboardButton("Portuguese", callback_data='lang_portuguese')
            ],
            [
                InlineKeyboardButton("Russian", callback_data='lang_russian'),
                InlineKeyboardButton("Japanese", callback_data='lang_japanese')
            ],
            [
                InlineKeyboardButton("German", callback_data='lang_german'),
                InlineKeyboardButton("Korean", callback_data='lang_korean')
            ],
            [
                InlineKeyboardButton("Italian", callback_data='lang_italian'),
                InlineKeyboardButton("Turkish", callback_data='lang_turkish')
            ],
            [
                InlineKeyboardButton("Vietnamese", callback_data='lang_vietnamese'),
                InlineKeyboardButton("Back to Settings", callback_data='settings')
            ]
        ]
        await query.edit_message_text(
            "Select your language:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif query.data.startswith('lang_'):
        user_id = query.from_user.id
        user_state = get_user_state(user_id)
        language = query.data.split('_')[1].capitalize()
        user_state.settings['language'] = language
        save_user_state(user_state)
        await show_settings_menu(update, context)

    elif query.data == 'set_gender':
        keyboard = [
            [
                InlineKeyboardButton("Male", callback_data='gender_male'),
                InlineKeyboardButton("Female", callback_data='gender_female')
            ],
            [
                InlineKeyboardButton("Other", callback_data='gender_other'),
                InlineKeyboardButton("Back to Settings", callback_data='settings')
            ]
        ]
        await query.edit_message_text(
            "Select your gender:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif query.data.startswith('gender_'):
        user_id = query.from_user.id
        user_state = get_user_state(user_id)
        gender = query.data.split('_')[1].capitalize()
        user_state.settings['gender'] = gender
        save_user_state(user_state)
        await show_settings_menu(update, context)

    elif query.data == 'my_profile':
        user_id = query.from_user.id
        user_state = get_user_state(user_id)
        
        # Count achievements
        achievement_count = len([a for a in user_state.achievements.split(',') if a])
        total_achievements = len(ACHIEVEMENTS)
        
        profile_text = (
            f"📊 **Your Profile**\n\n"
            f"🎭 Title: {get_karma_title(user_state.karma)}\n"
            f"⭐ Karma: {user_state.karma}\n"
            f"💬 Total Chats: {user_state.total_chats}\n"
            f"👍 Positive Ratings: {user_state.positive_ratings}\n"
            f"🔥 Current Streak: {user_state.current_streak}\n"
            f"🏆 Best Streak: {user_state.best_streak}\n"
            f"🎖️ Achievements: {achievement_count}/{total_achievements}\n"
        )
        
        keyboard = [[InlineKeyboardButton("Back to Main Menu", callback_data='back_to_main')]]
        await query.edit_message_text(profile_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    elif query.data == 'achievements':
        user_id = query.from_user.id
        user_state = get_user_state(user_id)
        
        earned = user_state.achievements.split(',') if user_state.achievements else []
        
        text = "🏆 **Achievements**\n\n"
        for achievement_id, achievement in ACHIEVEMENTS.items():
            if achievement_id in earned:
                text += f"✅ {achievement['name']}\n   _{achievement['desc']}_\n\n"
            else:
                text += f"🔒 {achievement['name']}\n   _{achievement['desc']}_\n\n"
        
        keyboard = [[InlineKeyboardButton("Back to Main Menu", callback_data='back_to_main')]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    elif query.data == 'request_reveal':
        user_id = query.from_user.id
        user_state = get_user_state(user_id)
        
        if not user_state.current_chat:
            await query.edit_message_text("You're not in a chat!")
            return
        
        partner_state = get_user_state(user_state.current_chat)
        
        # Mark that this user wants to reveal
        user_state.reveal_requested = True
        save_user_state(user_state)
        
        # Check if partner also requested reveal
        if partner_state.reveal_requested:
            # Both agreed! Reveal profiles to each other
            user_username = user_state.username or "No username set"
            partner_username = partner_state.username or "No username set"
            
            queue_message(user_id, f"🎉 **Mutual Reveal!**\n\nYour partner's profile:\n👤 Username: @{partner_username}\n⭐ Karma: {partner_state.karma}\n🎭 Title: {get_karma_title(partner_state.karma)}")
            queue_message(user_state.current_chat, f"🎉 **Mutual Reveal!**\n\nYour partner's profile:\n👤 Username: @{user_username}\n⭐ Karma: {user_state.karma}\n🎭 Title: {get_karma_title(user_state.karma)}")
            
            # Reset reveal flags
            user_state.reveal_requested = False
            partner_state.reveal_requested = False
            save_user_state(user_state)
            save_user_state(partner_state)
        else:
            queue_message(user_id, "✨ You've requested to reveal profiles. Waiting for your partner to agree...")
            queue_message(user_state.current_chat, "💫 Your chat partner wants to reveal profiles! Use /reveal if you agree.")

    elif query.data.startswith('rate_'):
        user_id = query.from_user.id
        rating_type = query.data.split('_')[1]  # 'positive' or 'negative'
        
        # Get the partner ID from context
        partner_id = context.user_data.get('rate_partner_id')
        if not partner_id:
            await query.edit_message_text("Rating session expired.")
            await show_main_menu(update, context)
            return
        
        partner_state = get_user_state(partner_id)
        user_state = get_user_state(user_id)
        
        if rating_type == 'positive':
            partner_state.karma += 5
            partner_state.positive_ratings += 1
            user_state.current_streak += 1
            if user_state.current_streak > user_state.best_streak:
                user_state.best_streak = user_state.current_streak
            await query.edit_message_text("👍 Thanks for the positive rating!")
        else:
            partner_state.karma -= 3
            user_state.current_streak = 0  # Reset streak on negative rating
            await query.edit_message_text("👎 Thanks for your feedback.")
        
        # Check for new achievements
        new_achievements = check_and_award_achievements(user_state)
        save_user_state(partner_state)
        save_user_state(user_state)
        
        # Notify about new achievements
        for achievement in new_achievements:
            queue_message(user_id, f"🎉 Achievement Unlocked: {achievement['name']}\n_{achievement['desc']}_")
        
        # Clear rating context
        del context.user_data['rate_partner_id']
        
        time.sleep(1)
        await show_main_menu(update, context)

    elif query.data == 'skip_rating':
        await query.edit_message_text("Rating skipped.")
        if 'rate_partner_id' in context.user_data:
            del context.user_data['rate_partner_id']
        await show_main_menu(update, context)

    elif query.data == 'find_match':
        user_id = query.from_user.id
        user_state = get_user_state(user_id)
        
        # Save username for potential reveal later
        if update.effective_user.username:
            user_state.username = update.effective_user.username
            save_user_state(user_state)
        
        if not user_state.is_active:
            await query.edit_message_text("You need to be active to find a match!")
            time.sleep(2)
            await show_main_menu(update, context)
            return
        
        if user_state.current_chat:
            await query.edit_message_text("You are already in a chat!")
            time.sleep(2)
            await show_main_menu(update, context)
            return
        
        # Find a random active user
        match = find_random_match(user_id)
        if match:
            await query.edit_message_text("Match found! Starting chat...")
            await start_chat(user_id, match, context.bot)
        else:
            # Check if user has been waiting too long
            if user_state.match_start_time:
                wait_time = (datetime.now() - user_state.match_start_time).total_seconds()
                if wait_time > MATCH_TIMEOUT:
                    await query.edit_message_text("No match found after waiting too long. Please try again later!")
                    user_state.match_start_time = None
                    save_user_state(user_state)
                    time.sleep(2)
                    await show_main_menu(update, context)
                    return
            
            await query.edit_message_text("No matches found at the moment. We'll keep looking!")
            time.sleep(2)
            await show_main_menu(update, context)

def process_message_queue():
    """Process the message queue respecting Telegram's rate limits."""
    global BOT_INSTANCE, QUEUE_LOOP
    while RUNNING:
        try:
            # Check for inactive chats
            current_time = datetime.now()
            conn = sqlite3.connect('database.db')
            c = conn.cursor()
            
            # Get all users in chats
            c.execute('SELECT user_id, current_chat, last_activity FROM users WHERE current_chat IS NOT NULL')
            active_chats = c.fetchall()
            
            for user_id, partner_id, last_activity in active_chats:
                last_activity = datetime.fromisoformat(last_activity)
                if (current_time - last_activity).total_seconds() > INACTIVITY_TIMEOUT:
                    # End the chat due to inactivity
                    user_state = get_user_state(user_id)
                    partner_state = get_user_state(partner_id)
                    
                    # Clear chat states
                    user_state.current_chat = None
                    partner_state.current_chat = None
                    
                    save_user_state(user_state)
                    save_user_state(partner_state)
                    
                    # Queue notifications
                    queue_message(user_id, "Chat ended due to inactivity!")
                    queue_message(partner_id, "Chat ended due to inactivity!")
            
            conn.close()

            with MESSAGE_QUEUE_LOCK:
                if not MESSAGE_QUEUE:
                    time.sleep(0.1)  # Small delay when queue is empty
                    continue
                
                # Get the next message to send
                chat_id, content = MESSAGE_QUEUE.popleft()
            
            # Check the type of content and handle accordingly
            if isinstance(content, tuple):
                # Handle photo messages (chat_id, photo, caption)
                chat_id, photo, caption = content
                asyncio.run_coroutine_threadsafe(
                    BOT_INSTANCE.send_photo(
                        chat_id=chat_id,
                        photo=photo,
                        caption=caption
                    ),
                    QUEUE_LOOP
                ).result(timeout=10)
            elif isinstance(content, str):
                # Handle text messages
                asyncio.run_coroutine_threadsafe(
                    BOT_INSTANCE.send_message(chat_id=chat_id, text=content),
                    QUEUE_LOOP
                ).result(timeout=10)
            
            # Wait to respect rate limit
            time.sleep(1/MESSAGE_RATE_LIMIT)
            
        except Exception as e:
            logger.error(f"Error processing message queue: {e}")
            time.sleep(1)  # Wait before retrying

def queue_message(chat_id: int, content):
    """Add a message to the queue to be sent."""
    with MESSAGE_QUEUE_LOCK:
        MESSAGE_QUEUE.append((chat_id, content))

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle incoming messages."""
    # Check if we're waiting for user input for settings
    if 'awaiting_input' in context.user_data:
        setting_type = context.user_data['awaiting_input']
        user_id = update.effective_user.id
        user_state = get_user_state(user_id)
        
        if setting_type in ['country', 'language']:
            user_state.settings[setting_type] = update.message.text
            save_user_state(user_state)
            del context.user_data['awaiting_input']
            await show_settings_menu(update, context)
            return
    
    # Handle chat messages
    user_id = update.effective_user.id
    user_state = get_user_state(user_id)
    
    if user_state.current_chat:
        # Check for unauthorized file types
        if (update.message.video or update.message.document or update.message.audio or 
            update.message.voice or update.message.sticker or update.message.video_note):
            queue_message(
                user_id,
                "❌ Unauthorized file type. Only text messages and photos are allowed."
            )
            return
            
        if update.message.photo:
            # Handle photo messages
            photo = update.message.photo[-1]  # Get the highest quality photo
            caption = f"Anonymous: {update.message.caption}" if update.message.caption else "Anonymous sent a photo"
            queue_message(
                user_state.current_chat,
                (user_state.current_chat, photo.file_id, caption)
            )
        else:
            # Handle text messages
            queue_message(
                user_state.current_chat,
                f"Anonymous: {update.message.text}"
            )
        user_state.last_activity = datetime.now()
        save_user_state(user_state)
    else:
        # If not in a chat, show main menu
        await show_main_menu(update, context)

async def start_chat(user1_id: int, user2_id: int, bot) -> None:
    """Start a chat between two users."""
    user1_state = get_user_state(user1_id)
    user2_state = get_user_state(user2_id)
    
    user1_state.current_chat = user2_id
    user2_state.current_chat = user1_id
    
    # Reset reveal flags
    user1_state.reveal_requested = False
    user2_state.reveal_requested = False
    
    # Increment total chats
    user1_state.total_chats += 1
    user2_state.total_chats += 1
    
    # Set last_activity to now for both users
    user1_state.last_activity = datetime.now()
    user2_state.last_activity = datetime.now()
    
    save_user_state(user1_state)
    save_user_state(user2_state)
    
    # Create partner info messages
    user1_partner_info = f"🎉 **Chat Started!**\n\n👤 Your partner: {get_karma_title(user2_state.karma)}\n⭐ Karma: {user2_state.karma}\n\n💡 Commands:\n/end - End the chat\n/reveal - Request to share profiles\n/typing - Show typing indicator\n\nEnjoy your conversation!"
    user2_partner_info = f"🎉 **Chat Started!**\n\n👤 Your partner: {get_karma_title(user1_state.karma)}\n⭐ Karma: {user1_state.karma}\n\n💡 Commands:\n/end - End the chat\n/reveal - Request to share profiles\n/typing - Show typing indicator\n\nEnjoy your conversation!"
    
    # Queue notifications with partner info
    queue_message(user1_id, user1_partner_info)
    queue_message(user2_id, user2_partner_info)
    
    # Check for achievements
    for user_state, user_id in [(user1_state, user1_id), (user2_state, user2_id)]:
        new_achievements = check_and_award_achievements(user_state)
        save_user_state(user_state)
        for achievement in new_achievements:
            queue_message(user_id, f"🎉 Achievement Unlocked: {achievement['name']}\n_{achievement['desc']}_")

async def end_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """End the current chat."""
    user_id = update.effective_user.id
    user_state = get_user_state(user_id)
    
    if user_state.current_chat:
        partner_id = user_state.current_chat
        partner_state = get_user_state(partner_id)
        
        # Clear chat states and reveal flags
        user_state.current_chat = None
        partner_state.current_chat = None
        user_state.reveal_requested = False
        partner_state.reveal_requested = False
        
        save_user_state(user_state)
        save_user_state(partner_state)
        
        # Store partner ID for rating
        context.user_data['rate_partner_id'] = partner_id
        
        # Show rating prompt to the user who ended the chat
        rating_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("👍 Positive", callback_data='rate_positive'),
                InlineKeyboardButton("👎 Negative", callback_data='rate_negative')
            ],
            [InlineKeyboardButton("⏭️ Skip Rating", callback_data='skip_rating')]
        ])
        
        await update.message.reply_text(
            "💬 Chat ended!\n\nHow was your conversation? Rate your partner:",
            reply_markup=rating_keyboard
        )
        
        # Notify partner
        queue_message(partner_id, "👋 Your chat partner has ended the conversation.")
    else:
        queue_message(user_id, "You are not in a chat!")
        await show_main_menu(update, context)

async def reveal_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /reveal command to request profile sharing."""
    user_id = update.effective_user.id
    user_state = get_user_state(user_id)
    
    # Save username for reveal
    if update.effective_user.username:
        user_state.username = update.effective_user.username
        save_user_state(user_state)
    
    if not user_state.current_chat:
        await update.message.reply_text("You're not in a chat!")
        return
    
    partner_state = get_user_state(user_state.current_chat)
    
    # Mark that this user wants to reveal
    user_state.reveal_requested = True
    save_user_state(user_state)
    
    # Check if partner also requested reveal
    if partner_state.reveal_requested:
        # Both agreed! Reveal profiles to each other
        user_username = user_state.username or "No username set"
        partner_username = partner_state.username or "No username set"
        
        queue_message(user_id, f"🎉 **Mutual Reveal!**\n\nYour partner's profile:\n👤 Username: @{partner_username}\n⭐ Karma: {partner_state.karma}\n🎭 Title: {get_karma_title(partner_state.karma)}")
        queue_message(user_state.current_chat, f"🎉 **Mutual Reveal!**\n\nYour partner's profile:\n👤 Username: @{user_username}\n⭐ Karma: {user_state.karma}\n🎭 Title: {get_karma_title(user_state.karma)}")
        
        # Reset reveal flags
        user_state.reveal_requested = False
        partner_state.reveal_requested = False
        save_user_state(user_state)
        save_user_state(partner_state)
    else:
        await update.message.reply_text("✨ You've requested to reveal profiles. Waiting for your partner to agree...")
        queue_message(user_state.current_chat, "💫 Your chat partner wants to reveal profiles!\nUse /reveal if you agree to share your username.")

async def typing_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /typing command to show typing indicator."""
    user_id = update.effective_user.id
    user_state = get_user_state(user_id)
    
    if not user_state.current_chat:
        await update.message.reply_text("You're not in a chat!")
        return
    
    # Send typing indicator to partner
    queue_message(user_state.current_chat, "✍️ _Your partner is typing..._")

def main() -> None:
    """Start the bot."""
    global RUNNING, MESSAGE_QUEUE_THREAD, BOT_INSTANCE, QUEUE_LOOP
    
    # Create the Application and pass it your bot's token
    application = Application.builder().token(os.getenv('BOT_TOKEN')).build()
    BOT_INSTANCE = application.bot

    # Create a dedicated event loop for the message queue
    QUEUE_LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(QUEUE_LOOP)

    # Start the message queue processor in a separate thread
    MESSAGE_QUEUE_THREAD = threading.Thread(target=process_message_queue, daemon=True)
    MESSAGE_QUEUE_THREAD.start()

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("end", end_chat))
    application.add_handler(CommandHandler("reveal", reveal_handler))
    application.add_handler(CommandHandler("typing", typing_handler))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    application.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, message_handler))
    application.add_handler(MessageHandler(filters.VIDEO & ~filters.COMMAND, message_handler))
    application.add_handler(MessageHandler(filters.Document.ALL & ~filters.COMMAND, message_handler))
    application.add_handler(MessageHandler(filters.AUDIO & ~filters.COMMAND, message_handler))
    application.add_handler(MessageHandler(filters.VOICE & ~filters.COMMAND, message_handler))
    application.add_handler(MessageHandler(filters.VIDEO_NOTE & ~filters.COMMAND, message_handler))
    logger.info("Starting bot...")
    
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        # Stop the message queue thread
        RUNNING = False
        if MESSAGE_QUEUE_THREAD:
            MESSAGE_QUEUE_THREAD.join()
        if QUEUE_LOOP:
            QUEUE_LOOP.close()
    
if __name__ == '__main__':
    main()