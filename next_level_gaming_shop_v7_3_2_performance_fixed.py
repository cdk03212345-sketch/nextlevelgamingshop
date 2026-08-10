import os
import asyncio
import csv
import threading
import json
import html
import gzip
import hmac
import shutil
import tempfile
from urllib.parse import urlparse, parse_qs
from pathlib import Path
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import load_dotenv
from psycopg import connect, errors
from psycopg.rows import dict_row

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, BotCommand, BotCommandScopeDefault, BotCommandScopeChat, ReplyKeyboardRemove
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
CURRENCY = os.getenv("CURRENCY", "BDT")
PAYMENT_INFO = os.getenv("PAYMENT_INSTRUCTIONS", "bKash/Nagad: YOUR NUMBER")
SUPPORT = os.getenv("SUPPORT_USERNAME", "@YourSupport")
ADMIN_WEB_TOKEN = os.getenv("ADMIN_WEB_TOKEN", "").strip()
FEATURE_EFOOTBALL_COINS = True
APP_VERSION = "V7.3.2 PERFORMANCE FIX • INLINE-ONLY A-Z CUSTOM"
AUTO_DB_BACKUP_HOURS = max(0, int(os.getenv("AUTO_DB_BACKUP_HOURS", "24") or "24"))
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", "/tmp/next_level_backups"))

if not TOKEN:
    raise RuntimeError("BOT_TOKEN missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing. Add a PostgreSQL connection string in Render Environment Variables.")
if not ADMIN_IDS:
    raise RuntimeError("ADMIN_IDS missing")

DB_LOCK = threading.RLock()


def db_conn():
    return connect(DATABASE_URL, row_factory=dict_row, connect_timeout=10)


def db_execute(sql, params=(), fetch=None):
    """Short-lived PostgreSQL connection helper."""
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
                return cur.rowcount


def db_insert_returning(sql, params=()):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()


def now_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
    id BIGSERIAL PRIMARY KEY,
    tg_id BIGINT UNIQUE NOT NULL,
    username TEXT,
    name TEXT,
    balance NUMERIC(14,2) NOT NULL DEFAULT 0,
    blocked INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products(
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'Gaming',
    quantity INTEGER NOT NULL DEFAULT 0,
    price NUMERIC(14,2) NOT NULL,
    stock INTEGER NOT NULL DEFAULT 0,
    delivery_type TEXT NOT NULL DEFAULT 'code',
    active INTEGER NOT NULL DEFAULT 1,
    description TEXT NOT NULL DEFAULT '',
    image_file_id TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS product_codes(
    id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    code TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL DEFAULT 'available',
    sold_to BIGINT,
    order_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sold_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS orders(
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    product_id BIGINT NOT NULL REFERENCES products(id),
    game_uid TEXT,
    total NUMERIC(14,2) NOT NULL,
    delivered_code TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    admin_note TEXT DEFAULT '',
    refund_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    processed_at TIMESTAMPTZ,
    rewards_awarded BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payments(
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    amount NUMERIC(14,2) NOT NULL,
    method TEXT NOT NULL,
    trx_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    admin_note TEXT DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(method, trx_id)
);

CREATE TABLE IF NOT EXISTS payment_receipts(
    payment_id BIGINT PRIMARY KEY REFERENCES payments(id) ON DELETE CASCADE,
    file_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS balance_logs(
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    amount NUMERIC(14,2) NOT NULL,
    action TEXT NOT NULL,
    note TEXT DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS admin_logs(
    id BIGSERIAL PRIMARY KEY,
    admin_tg_id BIGINT NOT NULL,
    action TEXT NOT NULL,
    details TEXT DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS settings(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS favorites(
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(user_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_codes_product_status ON product_codes(product_id,status);
CREATE INDEX IF NOT EXISTS idx_codes_order ON product_codes(order_id);
CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_user_status ON orders(user_id,status);
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);
CREATE INDEX IF NOT EXISTS idx_payments_created ON payments(created_at);
"""


def init_db():
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA)
                # Safe migrations for existing V6.x databases.
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_code TEXT UNIQUE")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by BIGINT")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS loyalty_points INTEGER NOT NULL DEFAULT 0")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS lifetime_spend NUMERIC(14,2) NOT NULL DEFAULT 0")
                cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS rewards_awarded BOOLEAN NOT NULL DEFAULT FALSE")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)")
                defaults = {
                    "shop_name": "Next Level Gaming Shop",
                    "support": SUPPORT,
                    "payment_info": PAYMENT_INFO,
                    "maintenance": "0",
                    "low_stock_threshold": "3",
            "announcement": "🔥 <b>Welcome to Next Level Gaming Shop!</b> ⚡ Fast delivery • 🛡️ Secure payments • ⭐ Premium rewards",
            "currency": CURRENCY,
            "welcome_message": "👋 <b>Welcome to {shop_name}!</b>\n\nChoose an option below to continue. 🛍️",
            "maintenance_message": "🔧 <b>Shop is temporarily under maintenance.</b>\nPlease try again later.",
            "fallback_message": "Use the menu below or /shop to continue.",
            "search_prompt": "🔎 <b>Smart Product Search</b>\n\nSend a product name, game, category, or keyword.",
            "deposit_prompt": "💰 <b>Add Balance</b>\n\nSend the amount you want to add.",
            "signup_bonus": "0", "referral_reward": "0",
            "deposit_min": "1", "deposit_max": "0",
            "order_timeout_minutes": "30", "payment_timeout_minutes": "30",
            "button_shop": "🛍️ Shop", "button_search": "🔍 Search",
            "button_orders": "📦 My Orders", "button_favorites": "❤️ Favorites",
            "button_profile": "👤 Profile", "button_deposit": "💰 Deposit",
            "button_rewards": "⭐ Rewards", "button_refer": "🤝 Refer & Earn",
            "button_support": "🆘 Support",
            "footer_text": "⚡ Instant Delivery • 🛡️ Secure & Safe • 🎁 Rewards & VIP",
            "button_buy": "🛒 Buy Now",
            "button_purchase": "🛒 Purchase",
            "button_confirm": "✅ Confirm Purchase",
            "button_back": "⬅️ Back",
            "button_back_listings": "⬅️ Back to Listings",
            "button_main_menu": "🏠 Main Menu",
            "button_favorite_add": "⭐ Add to Favorites",
            "button_favorite_remove": "💔 Remove Favorite",
            "button_sold_out": "⛔ Sold Out",
            "payment_bkash_label": "bKash",
            "payment_nagad_label": "Nagad",
            "payment_method_prompt": "💳 Choose payment method:",
            "shop_title": "💎 <b>Premium Gaming Store</b>\n\n🎮 Choose a game category to continue:",
            "category_title": "📂 <b>Product Categories</b>\n\nChoose a category to browse products.",
            "buy_prompt": "Confirm your purchase:",
                }
                for key, value in defaults.items():
                    cur.execute(
                        "INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO NOTHING",
                        (key, str(value)),
                    )


init_db()
router = Router()

_SETTINGS_CACHE = {}
_SETTINGS_CACHE_LOCK = threading.RLock()

def _load_settings_cache():
    rows = db_execute("SELECT key,value FROM settings", fetch="all") or []
    with _SETTINGS_CACHE_LOCK:
        _SETTINGS_CACHE.clear()
        _SETTINGS_CACHE.update({r["key"]: r["value"] for r in rows})

_load_settings_cache()

def setting(key, fallback=""):
    with _SETTINGS_CACHE_LOCK:
        return _SETTINGS_CACHE.get(key, fallback)

def set_setting(key, value):
    value = str(value)
    db_execute("INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value", (key, value))
    with _SETTINGS_CACHE_LOCK:
        _SETTINGS_CACHE[key] = value

def shop_name():
    return setting("shop_name", "Next Level Gaming Shop")

def currency():
    return setting("currency", CURRENCY)

def custom_text(key, fallback=""):
    value = setting(key, fallback)
    try:
        return value.format(shop_name=shop_name(), currency=currency())
    except Exception:
        return value


def low_stock_threshold():
    try:
        return max(0, int(setting("low_stock_threshold", "3")))
    except ValueError:
        return 3


def is_admin(tg_id):
    return tg_id in ADMIN_IDS


def maintenance_active():
    return setting("maintenance", "0") == "1"


def user_access_denied(tg_id):
    if is_admin(tg_id):
        return False
    if user_blocked(tg_id):
        return True
    return maintenance_active()


def user_blocked(tg_id):
    row = db_execute("SELECT blocked FROM users WHERE tg_id=%s", (tg_id,), "one")
    return bool(row and row["blocked"])


def get_user(tg):
    username = getattr(tg, "username", None)
    name = getattr(tg, "full_name", None) or str(tg.id)
    return db_insert_returning(
        """INSERT INTO users(tg_id,username,name,referral_code) VALUES(%s,%s,%s,%s)
           ON CONFLICT(tg_id) DO UPDATE SET username=EXCLUDED.username, name=EXCLUDED.name,
             updated_at=NOW(), referral_code=COALESCE(users.referral_code, EXCLUDED.referral_code)
           RETURNING *""",
        (tg.id, username, name, f"NL{tg.id}"),
    )

def dmin_log(admin_id, action, details=""):
    db_execute(
        "INSERT INTO admin_logs(admin_tg_id,action,details) VALUES(%s,%s,%s)",
        (admin_id, action, details),
    )


BACKUP_TABLES = (
    "users", "products", "product_codes", "orders", "payments",
    "payment_receipts", "balance_logs", "admin_logs", "settings", "favorites"
)


def create_database_backup():
    """Create a compressed JSON snapshot of all application tables."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = BACKUP_DIR / f"next_level_db_backup_{stamp}.json.gz"
    payload = {"app_version": APP_VERSION, "created_at": now_text(), "tables": {}}
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                for table in BACKUP_TABLES:
                    cur.execute(f"SELECT * FROM {table} ORDER BY 1")
                    payload["tables"][table] = cur.fetchall()
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, default=str)
    return path


def cleanup_old_backups(keep=5):
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(BACKUP_DIR.glob("next_level_db_backup_*.json.gz"), key=lambda x: x.stat().st_mtime, reverse=True)
    for old in files[keep:]:
        try:
            old.unlink()
        except OSError:
            pass


def database_integrity_check():
    """Fail fast if the PostgreSQL database is unavailable or core tables are missing."""
    row = db_execute("SELECT current_database() AS db, current_schema() AS schema", fetch="one")
    missing = db_execute(
        """SELECT required.table_name FROM (VALUES
            ('users'),('products'),('product_codes'),('orders'),('payments'),
            ('payment_receipts'),('balance_logs'),('admin_logs'),('settings'),('favorites')
        ) AS required(table_name)
        LEFT JOIN information_schema.tables t
          ON t.table_schema=current_schema() AND t.table_name=required.table_name
        WHERE t.table_name IS NULL ORDER BY required.table_name""",
        fetch="all",
    )
    if missing:
        raise RuntimeError("Database integrity check failed; missing tables: " + ", ".join(r["table_name"] for r in missing))
    return row


def available_code_count(product_id):
    row = db_execute(
        "SELECT COUNT(*) AS c FROM product_codes WHERE product_id=%s AND status='available'",
        (product_id,), "one")
    return int(row["c"])


def is_auto_code_product(product):
    return product["delivery_type"] == "code"


def effective_stock(product):
    code_count = available_code_count(product["id"])
    if product["delivery_type"] == "code":
        return code_count
    return max(0, int(product["stock"]))


def sync_code_product_stock(product_id, conn=None):
    own = conn is None
    if own:
        conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM product_codes WHERE product_id=%s AND status='available'", (product_id,))
            count = int(cur.fetchone()["c"])
            cur.execute("UPDATE products SET stock=%s,updated_at=NOW() WHERE id=%s AND delivery_type='code'", (count, product_id))
        if own:
            conn.commit()
        return count
    finally:
        if own:
            conn.close()


def reconcile_all_code_stock():
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE products p
                    SET stock = COALESCE(pc.available, 0), updated_at = NOW()
                    FROM (SELECT product_id, COUNT(*) AS available FROM product_codes WHERE status='available' GROUP BY product_id) pc
                    WHERE p.id = pc.product_id AND p.delivery_type='code'
                """)
                cur.execute("""
                    UPDATE products p SET stock=0, updated_at=NOW()
                    WHERE p.delivery_type='code' AND NOT EXISTS (SELECT 1 FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available')
                """)


async def notify_low_stock(bot):
    rows=db_execute("""
        SELECT p.id,p.name,p.delivery_type,
               CASE WHEN p.delivery_type='code'
                    THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available')
                    ELSE p.stock END AS effective_stock
        FROM products p
        WHERE p.active=1
          AND (CASE WHEN p.delivery_type='code'
                    THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available')
                    ELSE p.stock END) <= %s
        ORDER BY effective_stock ASC LIMIT 20
    """,(low_stock_threshold(),),"all")
    if not rows:
        return
    text_msg="⚠️ <b>Low Stock Alert</b>\n\n" + "\n".join(
        f"🎮 {html.escape(r['name'])} — <b>{r['effective_stock']}</b> left" for r in rows
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id,text_msg)
        except Exception:
            pass

def fmt_money(value):
    return f"{float(value):.2f} {currency()}"


def status_emoji(status):
    return {"pending":"⏳","processing":"🔄","completed":"✅","rejected":"❌","refunded":"↩️","credited":"✅"}.get(status,"•")


def vip_progress(points):
    points=int(points or 0)
    tiers=[(0,"🥉 Starter",200),(200,"🥈 Silver",500),(500,"🥇 Gold",1000),(1000,"💎 Diamond",1000)]
    if points>=1000:
        return "💎 Diamond", 100, 1000
    for current,name,next_points in tiers:
        if points < next_points:
            pct=max(0,min(100,round((points-current)/(next_points-current)*100)))
            return name,pct,next_points
    return "🥉 Starter",0,200


def premium_home_text(u):
    points=int(u.get("loyalty_points") or 0)
    tier,pct,next_points=vip_progress(points)
    spend=float(u.get("lifetime_spend") or 0)
    balance=fmt_money(u["balance"])
    bar="█"*max(0,pct//10)+"░"*(10-max(0,pct//10))
    announcement=setting("announcement","").strip()
    lines=[
        f"👋 <b>Welcome Back!</b>  ✨",
        f"<i>Your Trusted Digital Store</i>",
        "",
        f"👤 <b>{html.escape(u.get('name') or 'Gamer')}</b>  •  🆔 <code>{u['tg_id']}</code>",
        f"🏅 VIP: <b>{tier}</b>",
        "",
        f"💰 Balance: <b>{balance}</b>    📦 Orders: <b>{int(u.get('order_count') or 0)}</b>",
        f"⭐ Points: <b>{points}</b>       📈 VIP: <b>{pct}%</b>",
        f"{bar}",
        f"💸 Lifetime Spend: <b>{fmt_money(spend)}</b>",
    ]
    if tier != "💎 Diamond":
        lines.append(f"🎯 Next VIP milestone: <b>{next_points} points</b>")
    if announcement:
        lines.extend(["", f"📢 <b>Announcement</b>\n{announcement}"])
    lines.extend(["", "⚡ <b>Instant Delivery</b>  •  🛡️ <b>Secure & Safe</b>  •  🎁 <b>Rewards & VIP</b>"])
    return "\n".join(lines)

def premium_home_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=setting("home_shop", "🛍️ Shop"), callback_data="home:shop"), InlineKeyboardButton(text=setting("home_orders", "📦 My Orders"), callback_data="home:orders")],
        [InlineKeyboardButton(text=setting("home_deposit", "💰 Deposit"), callback_data="home:deposit"), InlineKeyboardButton(text=setting("home_profile", "👤 Profile"), callback_data="home:profile")],
        [InlineKeyboardButton(text=setting("home_rewards", "⭐ Rewards"), callback_data="home:rewards"), InlineKeyboardButton(text=setting("home_refer", "🤝 Referral"), callback_data="home:refer")],
        [InlineKeyboardButton(text=setting("home_favorites", "❤️ Favorites"), callback_data="home:favorites"), InlineKeyboardButton(text=setting("home_support", "🆘 Support"), callback_data="home:support")],
        [InlineKeyboardButton(text=setting("home_search", "🔍 Search Products"), callback_data="home:search")],
    ])

def user_menu():
    # Inline-only UI: no Telegram ReplyKeyboard / persistent keyboard.
    return None

def inline_home_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=setting("button_shop","🛍️ Shop"), callback_data="home:shop"), InlineKeyboardButton(text=setting("button_orders","📦 My Orders"), callback_data="home:orders")],
        [InlineKeyboardButton(text=setting("button_deposit","💰 Deposit"), callback_data="home:deposit"), InlineKeyboardButton(text=setting("button_profile","👤 Profile"), callback_data="home:profile")],
        [InlineKeyboardButton(text=setting("button_rewards","⭐ Rewards"), callback_data="home:rewards"), InlineKeyboardButton(text=setting("button_refer","🤝 Referral"), callback_data="home:refer")],
        [InlineKeyboardButton(text=setting("button_favorites","❤️ Favorites"), callback_data="home:favorites"), InlineKeyboardButton(text=setting("button_support","🆘 Support"), callback_data="home:support")],
        [InlineKeyboardButton(text=setting("button_search","🔍 Search"), callback_data="home:search")],
    ])

def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=setting("admin_dashboard", "📊 Dashboard"), callback_data="admin:dashboard"), InlineKeyboardButton(text=setting("admin_reports", "📈 Reports"), callback_data="admin:reports")],
        [InlineKeyboardButton(text=setting("admin_premium", "💎 Premium Analytics"), callback_data="admin:premium")],
        [InlineKeyboardButton(text=setting("admin_orders", "🧾 Orders"), callback_data="admin:orders"), InlineKeyboardButton(text=setting("admin_payments", "💳 Payments"), callback_data="admin:payments")],
        [InlineKeyboardButton(text=setting("admin_users", "👥 Users"), callback_data="admin:users"), InlineKeyboardButton(text=setting("admin_products", "🛍 Products"), callback_data="admin:products")],
        [InlineKeyboardButton(text=setting("admin_codes", "🎫 Codes"), callback_data="admin:codes"), InlineKeyboardButton(text=setting("admin_balance", "💰 Balance"), callback_data="admin:balance")],
        [InlineKeyboardButton(text=setting("admin_broadcast", "📢 Broadcast"), callback_data="admin:broadcast"), InlineKeyboardButton(text=setting("admin_settings", "⚙️ Settings"), callback_data="admin:settings")],
        [InlineKeyboardButton(text=setting("admin_database", "📊 Database"), callback_data="admin:dbinfo"), InlineKeyboardButton(text=setting("admin_logs", "📝 Logs"), callback_data="admin:logs")],
    ])


def _split_category(category):
    category = (category or "Other").strip()
    if ">" in category:
        game, pack = category.split(">", 1)
        return game.strip(), pack.strip()
    return category, None


def categories_kb():
    rows = db_execute(
        "SELECT category,COUNT(*) AS c FROM products "
        "WHERE active=1 GROUP BY category ORDER BY category",
        fetch="all"
    )
    games = {}
    for r in rows:
        game, pack = _split_category(r["category"])
        games.setdefault(game, 0)
        games[game] += int(r["c"])

    buttons = [
        [InlineKeyboardButton(text=f"🎮 {game}  •  {count}",
                              callback_data=f"game:{game}")]
        for game, count in sorted(games.items())
    ]
    buttons.append([InlineKeyboardButton(text=setting("inline_all_products", "✨ All Products"), callback_data="cat:*")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def game_packs_kb(game):
    rows = db_execute(
        "SELECT category,COUNT(*) AS c FROM products "
        "WHERE active=1 AND (category=%s OR category LIKE %s) "
        "GROUP BY category ORDER BY category",
        (game, game + " > %"),
        "all"
    )
    buttons = []
    for r in rows:
        category = (r["category"] or "").strip()
        _, pack = _split_category(category)
        if pack:
            label = f"💎 {pack}  •  {int(r['c'])}"
        else:
            label = f"🛍 Products  •  {int(r['c'])}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"cat:{category}")])

    buttons.append([InlineKeyboardButton(text=setting("inline_games_back", "⬅️ Games"), callback_data="shop")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def products_kb(category="*", page=0, per_page=4):
    """Premium product list; stock is computed in SQL to avoid N+1 queries."""
    offset = page * per_page
    stock_join = "LEFT JOIN (SELECT product_id, COUNT(*) AS available FROM product_codes WHERE status='available' GROUP BY product_id) pc ON pc.product_id=p.id"
    if category == "*":
        rows = db_execute(f"SELECT p.*, CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock FROM products p {stock_join} WHERE p.active=1 ORDER BY p.id DESC LIMIT %s OFFSET %s", (per_page, offset), "all")
        total = db_execute("SELECT COUNT(*) AS c FROM products WHERE active=1", fetch="one")["c"]
    else:
        rows = db_execute(f"SELECT p.*, CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock FROM products p {stock_join} WHERE p.active=1 AND p.category=%s ORDER BY p.id DESC LIMIT %s OFFSET %s", (category, per_page, offset), "all")
        total = db_execute("SELECT COUNT(*) AS c FROM products WHERE active=1 AND category=%s", (category,), "one")["c"]
    buttons=[]
    for idx,p in enumerate(rows, start=offset+1):
        stock=int(p["effective_stock"] or 0)
        buttons.append([
            InlineKeyboardButton(text=f"{idx}. {'🟢' if stock>0 else '🔴'} 💰 {float(p['price']):g} {currency()}", callback_data=f"product:{p['id']}"),
            InlineKeyboardButton(text=setting("button_buy","🛒 Buy Now") if stock>0 else setting("button_sold_out","⛔ Sold Out"), callback_data=f"buy:{p['id']}" if stock>0 else f"soldout:{p['id']}")
        ])
    total_pages=max(1,(int(total)+per_page-1)//per_page)
    buttons.append([
        InlineKeyboardButton(text=setting("inline_first","⏮ First"),callback_data=f"page:{category}:0"),
        InlineKeyboardButton(text=setting("inline_back","◀️ Back"),callback_data=f"page:{category}:{max(0,page-1)}"),
        InlineKeyboardButton(text=setting("inline_next","▶️ Next"),callback_data=f"page:{category}:{min(total_pages-1,page+1)}"),
        InlineKeyboardButton(text=setting("inline_last","⏭ Last"),callback_data=f"page:{category}:{total_pages-1}")])
    buttons.append([InlineKeyboardButton(text=setting("inline_refresh","🔄 Refresh"),callback_data=f"page:{category}:{page}"),InlineKeyboardButton(text=setting("inline_under5","💵 Under 5"),callback_data=f"price5:{category}")])
    buttons.append([InlineKeyboardButton(text=setting("inline_categories","📂 Categories"),callback_data="shop")])
    buttons.append([InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


@router.message(CommandStart())
async def start(m: Message):
    args=(m.text or "").split(maxsplit=1)
    was_new = db_execute("SELECT id FROM users WHERE tg_id=%s", (m.from_user.id,), "one") is None
    u=get_user(m.from_user)
    if was_new and len(args)>1 and args[1].startswith("ref_"):
        try:
            ref_tg=int(args[1][4:])
            if ref_tg != m.from_user.id:
                ref=db_execute("SELECT id FROM users WHERE tg_id=%s", (ref_tg,), "one")
                if ref:
                    db_execute("UPDATE users SET referred_by=%s WHERE id=%s AND referred_by IS NULL", (ref["id"],u["id"]))
                    u=get_user(m.from_user)
        except (ValueError,TypeError):
            pass
    if u["blocked"] and not is_admin(m.from_user.id): return await m.answer("🚫 Your account is blocked.")
    # Clear any legacy Reply Keyboard that may still be pinned in the user's Telegram chat.
    # The current bot UI uses InlineKeyboard only. This removal message is one-time/harmless.
    try:
        clear_msg = await m.answer("\u2060", reply_markup=ReplyKeyboardRemove())
        try:
            await clear_msg.delete()
        except Exception:
            pass
    except Exception:
        pass
    await m.answer(premium_home_text(u), reply_markup=premium_home_kb())


@router.message(Command("shop", "listings"))
@router.message(F.text == "🛍️ Shop")
@router.message(F.text == "🛒 Shop")
@router.message(F.text == "🛍️ Premium Shop")
@router.message(F.text == "💎 Shop")
async def shop(m: Message):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id): return await m.answer("🚫 Your account is blocked.")
    if maintenance_active() and not is_admin(m.from_user.id): return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."))
    await m.answer(custom_text("shop_title", "💎 <b>Premium Gaming Store</b>\n\n🎮 Choose a game category to continue:"), reply_markup=categories_kb())

@router.callback_query(F.data == "shop")
async def shop_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    await c.answer(); await c.message.edit_text(custom_text("category_title", "📂 <b>Product Categories</b>\n\nChoose a category to browse products."), reply_markup=categories_kb())

@router.callback_query(F.data.startswith("game:"))
async def game_folder_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    game = c.data.split(":", 1)[1]
    await c.answer()
    await c.message.edit_text(
        f"🎮 <b>{game}</b>\n\n💎 <i>Select your preferred pack</i>\n\n📂 Choose a pack:",
        reply_markup=game_packs_kb(game)
    )

@router.callback_query(F.data.startswith("cat:"))
async def category_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    category=c.data.split(":",1)[1]
    await c.answer()
    title = "All Products" if category == "*" else category
    total_row=db_execute("SELECT COUNT(*) AS c FROM products WHERE active=1 AND (category=%s OR %s='*')",(category,category),"one")
    total=int(total_row["c"]) if total_row else 0
    await c.message.edit_text(
        f"🛍️ <b>Shop / Listings</b>\n📂 {html.escape(title)}\n📄 Page <b>1</b> • Total: <b>{total}</b>\n\n👆 Tap the price for details • 🛒 Buy Now for checkout",
        reply_markup=products_kb(category, 0)
    )

@router.callback_query(F.data.startswith("page:"))
async def page_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    _,category,page=c.data.split(":",2); await c.answer(); await c.message.edit_reply_markup(reply_markup=products_kb(category,int(page)))

async def notify_user(bot, tg_id, text, reply_markup=None):
    try:
        await bot.send_message(tg_id, text, reply_markup=reply_markup)
    except Exception:
        pass


def user_favorite(product_id, tg_id):
    row=db_execute("SELECT 1 FROM favorites f JOIN users u ON u.id=f.user_id WHERE f.product_id=%s AND u.tg_id=%s",(product_id,tg_id),"one")
    return bool(row)


def favorite_count(tg_id):
    row=db_execute("SELECT COUNT(*) AS c FROM favorites f JOIN users u ON u.id=f.user_id WHERE u.tg_id=%s",(tg_id,),"one")
    return int(row["c"]) if row else 0


@router.callback_query(F.data == "main_menu")
async def main_menu_callback(c: CallbackQuery):
    if user_blocked(c.from_user.id) and not is_admin(c.from_user.id):
        return await c.answer("🚫 Your account is blocked.", show_alert=True)
    u = get_user(c.from_user)
    await c.answer()
    await c.message.edit_text(premium_home_text(u), reply_markup=premium_home_kb())


async def render_orders_callback(c: CallbackQuery):
    u=get_user(c.from_user)
    rows=db_execute("SELECT o.id,o.total,o.status,o.created_at,p.name FROM orders o JOIN products p ON p.id=o.product_id WHERE o.user_id=%s ORDER BY o.id DESC LIMIT 10",(u["id"],),"all")
    if not rows:
        return await c.message.edit_text("📦 <b>My Orders</b>\n\nYou have no orders yet.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))
    lines=["📦 <b>My Orders</b>\n"]
    buttons=[]
    for r in rows:
        lines.append(f"#{r['id']} • {html.escape(r['name'])}\n💰 {fmt_money(r['total'])} • {status_emoji(r['status'])} {r['status'].title()}\n🕒 {r['created_at']}\n")
        buttons.append([InlineKeyboardButton(text=f"🧾 Order #{r['id']}",callback_data=f"order_detail:{r['id']}")])
    buttons.append([InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")])
    return await c.message.edit_text("\n".join(lines),reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

async def render_profile_callback(c: CallbackQuery):
    u=get_user(c.from_user); row=db_execute("SELECT COUNT(*) AS c FROM orders WHERE user_id=%s",(u["id"],),"one")
    points=int(u.get("loyalty_points") or 0); tier,pct,next_points=vip_progress(points); bar="█"*max(0,pct//10)+"░"*(10-max(0,pct//10))
    text=(f"👤 <b>My Premium Account</b>\n\n🆔 ID: <code>{u['tg_id']}</code>\n💳 Wallet: <b>{fmt_money(u['balance'])}</b>\n🧾 Orders: <b>{row['c']}</b>\n⭐ Points: <b>{points}</b>\n🏅 VIP: <b>{tier}</b>\n📈 {bar} {pct}%\n💰 Lifetime spend: <b>{fmt_money(u.get('lifetime_spend') or 0)}</b>\n📅 Member since: <code>{u['created_at']}</code>")
    return await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("inline_rewards", "⭐ Rewards"),callback_data="home:rewards"),InlineKeyboardButton(text=setting("inline_referral", "🤝 Referral"),callback_data="home:refer")],[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))

async def render_rewards_callback(c: CallbackQuery):
    u=get_user(c.from_user); refs=db_execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=%s",(u["id"],),"one"); points=int(u.get("loyalty_points") or 0); spend=float(u.get("lifetime_spend") or 0); tier_name,pct,next_points=vip_progress(points); bar="█"*max(0,pct//10)+"░"*(10-max(0,pct//10))
    text=(f"⭐ <b>Premium Rewards Center</b>\n\n🏅 VIP Tier: <b>{tier_name}</b>\n📈 Progress: <b>{bar}</b> {pct}%\n⭐ Loyalty points: <b>{points}</b>\n🎯 Next milestone: <b>{next_points} points</b>\n💰 Lifetime spend: <b>{fmt_money(spend)}</b>\n🤝 Successful referrals: <b>{int(refs['c'])}</b>\n\nEarn points from completed purchases and referrals.")
    return await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("inline_shop", "🛍️ Shop"),callback_data="home:shop"),InlineKeyboardButton(text=setting("inline_referral", "🤝 Referral"),callback_data="home:refer")],[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))

async def render_refer_callback(c: CallbackQuery):
    u=get_user(c.from_user); me=await c.bot.get_me(); link=f"https://t.me/{me.username}?start=ref_{c.from_user.id}" if me.username else f"Use /start ref_{c.from_user.id}"; refs=db_execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=%s",(u["id"],),"one")
    text=("🤝 <b>Refer & Earn</b>\n\nInvite friends with your personal link. When a referred buyer completes their first purchase, both accounts receive loyalty recognition.\n\n" f"🔗 <b>Your link</b>\n<code>{html.escape(link)}</code>\n\n👥 Your referrals: <b>{int(refs['c'])}</b>")
    return await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("inline_rewards", "⭐ Rewards"),callback_data="home:rewards"),InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))

async def render_favorites_callback(c: CallbackQuery):
    u=get_user(c.from_user); rows=db_execute("SELECT p.* FROM favorites f JOIN products p ON p.id=f.product_id WHERE f.user_id=%s AND p.active=1 ORDER BY f.created_at DESC LIMIT 30",(u["id"],),"all")
    if not rows:
        return await c.message.edit_text("⭐ <b>Favorites</b>\n\nNo saved products yet.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("inline_shop", "🛍️ Shop"),callback_data="home:shop"),InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))
    buttons=[[InlineKeyboardButton(text=f"{'🟢' if effective_stock(p)>0 else '🔴'} {html.escape(p['name'])} • {float(p['price']):g} {currency()}",callback_data=f"product:{p['id']}")] for p in rows]
    buttons.append([InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")])
    return await c.message.edit_text(f"⭐ <b>Favorites</b> ({len(rows)})\n\nTap a product to view or buy.",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

async def render_support_callback(c: CallbackQuery):
    return await c.message.edit_text(f"🆘 <b>Support</b>\n\nContact: {html.escape(setting('support',SUPPORT))}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))


@router.callback_query(F.data.startswith("home:"))
async def premium_home_callback(c: CallbackQuery, state: FSMContext):
    if user_blocked(c.from_user.id) and not is_admin(c.from_user.id):
        return await c.answer("🚫 Your account is blocked.", show_alert=True)
    action=c.data.split(":",1)[1]
    await c.answer()
    if action=="shop":
        if maintenance_active() and not is_admin(c.from_user.id):
            return await c.answer("Shop is under maintenance.", show_alert=True)
        return await c.message.edit_text("📂 <b>Product Categories</b>\n\nChoose a category to browse products.", reply_markup=categories_kb())
    if action=="orders":
        return await render_orders_callback(c)
    if action=="profile":
        return await render_profile_callback(c)
    if action=="rewards":
        return await render_rewards_callback(c)
    if action=="refer":
        return await render_refer_callback(c)
    if action=="favorites":
        return await render_favorites_callback(c)
    if action=="support":
        return await render_support_callback(c)
    if action=="search":
        await state.set_state(SearchState.query)
        return await c.message.answer("🔍 <b>Search Products</b>\n\nType a product name or keyword.\nSend /cancel to stop.")
    if action=="deposit":
        if maintenance_active() and not is_admin(c.from_user.id):
            return await c.answer("Deposits are temporarily unavailable.", show_alert=True)
        await state.set_state(PaymentState.amount)
        return await c.message.answer(f"💰 <b>Add Balance</b>\n\nPayment methods: bKash / Nagad\n{setting('payment_info',PAYMENT_INFO)}\n\nSend amount. Example: <code>50</code>")

@router.callback_query(F.data.startswith("soldout:"))
async def soldout_callback(c: CallbackQuery):
    await c.answer("⛔ This product is currently sold out.", show_alert=True)


@router.callback_query(F.data.startswith("price5:"))
async def price5_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id):
        return await c.answer("Shop is under maintenance.", show_alert=True)
    category = c.data.split(":", 1)[1]
    rows = db_execute(
        """SELECT p.*, CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock
           FROM products p
           LEFT JOIN (SELECT product_id, COUNT(*) AS available FROM product_codes WHERE status='available' GROUP BY product_id) pc ON pc.product_id=p.id
           WHERE p.active=1 AND p.price < 5 AND (p.category=%s OR %s='*')
           ORDER BY p.id DESC LIMIT 7""",
        (category, category),
        "all"
    )
    if not rows:
        return await c.answer("No products below 5 found.", show_alert=True)
    buttons=[]
    for idx,p in enumerate(rows,1):
        stock=int(p.get("effective_stock") or 0)
        buttons.append([
            InlineKeyboardButton(text=f"{idx}. {'🟢' if stock>0 else '🔴'} {float(p['price']):g} {currency()}", callback_data=f"product:{p['id']}"),
            InlineKeyboardButton(text=setting("button_purchase","🛒 Purchase") if stock>0 else setting("button_sold_out","⛔ Sold Out"), callback_data=f"buy:{p['id']}" if stock>0 else f"soldout:{p['id']}")
        ])
    buttons.append([InlineKeyboardButton(text=setting("button_back_listings","⬅️ Back to Listings"), callback_data=f"page:{category}:0")])
    await c.answer()
    await c.message.edit_text(
        "💵 <b>Products Below 5</b>\n\nSelect a product to view details or purchase.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("product:"))
async def product_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    pid=int(c.data.split(":")[1]); p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    stock=effective_stock(p)
    delivery="Instant Code" if is_auto_code_product(p) else "Manual"
    badge = "🔥 AVAILABLE" if stock > 0 else "⛔ SOLD OUT"
    delivery_line = "⚡ Instant Delivery" if is_auto_code_product(p) else "🛠️ Manual Delivery"
    text=(f"📦 <b>Product Details</b>\n\n🎮 <b>{html.escape(p['name'])}</b>\n\n💰 Price: <b>{float(p['price']):g} {currency()}</b>\n📦 Stock: <b>{stock}</b>\n⚡ Delivery: <b>{'Instant' if is_auto_code_product(p) else 'Manual'}</b>\n🌍 Category: <b>{html.escape(p['category'])}</b>\n\n{badge}\n\n📝 {html.escape(p['description'] or 'Premium gaming product.')}")
    buttons=[]
    if stock>0: buttons.append([InlineKeyboardButton(text=setting("button_buy","🛒 Buy Now"),callback_data=f"buy:{pid}")])
    fav_label = setting("button_favorite_remove","💔 Remove Favorite") if user_favorite(pid,c.from_user.id) else setting("button_favorite_add","⭐ Add to Favorites")
    buttons.append([InlineKeyboardButton(text=fav_label,callback_data=f"fav:{pid}")])
    buttons.append([InlineKeyboardButton(text=setting("button_back","⬅️ Back"),callback_data=f"cat:{p['category']}")])
    await c.answer()
    markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    if p["image_file_id"]:
        try:
            await c.message.answer_photo(p["image_file_id"], caption=text, reply_markup=markup)
            return
        except Exception:
            pass
    await c.message.edit_text(text,reply_markup=markup)

@router.callback_query(F.data.startswith("fav:"))
async def favorite_toggle(c: CallbackQuery):
    if user_blocked(c.from_user.id) and not is_admin(c.from_user.id):
        return await c.answer("Account blocked.",show_alert=True)
    pid=int(c.data.split(":",1)[1]); u=get_user(c.from_user)
    p=db_execute("SELECT id,name,category,price,stock,delivery_type,active,description,image_file_id,quantity FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    exists=db_execute("SELECT 1 FROM favorites WHERE user_id=%s AND product_id=%s",(u["id"],pid),"one")
    if exists:
        db_execute("DELETE FROM favorites WHERE user_id=%s AND product_id=%s",(u["id"],pid))
        msg="💔 Removed from favorites."
    else:
        db_execute("INSERT INTO favorites(user_id,product_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",(u["id"],pid))
        msg="⭐ Added to favorites!"
    await c.answer(msg,show_alert=False)
    stock=effective_stock(p)
    delivery="Instant Code" if is_auto_code_product(p) else "Manual"
    badge = "🔥 AVAILABLE" if stock > 0 else "⛔ SOLD OUT"
    delivery_line = "⚡ Instant Delivery" if is_auto_code_product(p) else "🛠️ Manual Delivery"
    text=(f"📦 <b>Product Details</b>\n\n🎮 <b>{html.escape(p['name'])}</b>\n\n💰 Price: <b>{float(p['price']):g} {currency()}</b>\n📦 Stock: <b>{stock}</b>\n⚡ Delivery: <b>{'Instant' if is_auto_code_product(p) else 'Manual'}</b>\n🌍 Category: <b>{html.escape(p['category'])}</b>\n\n{badge}\n\n📝 {html.escape(p['description'] or 'Premium gaming product.')}")
    buttons=[]
    if stock>0: buttons.append([InlineKeyboardButton(text=setting("button_buy","🛒 Buy Now"),callback_data=f"buy:{pid}")])
    fav_label=setting("button_favorite_remove","💔 Remove Favorite") if not exists else setting("button_favorite_add","⭐ Add to Favorites")
    buttons.append([InlineKeyboardButton(text=fav_label,callback_data=f"fav:{pid}")])
    buttons.append([InlineKeyboardButton(text=setting("button_back","⬅️ Back"),callback_data=f"cat:{p['category']}")])
    try:
        await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    except Exception:
        pass


@router.message(F.text=="⭐ Favorites")
@router.message(F.text=="❤️ Favorites")
@router.message(Command("favorites"))
async def favorites(m:Message):
    if user_access_denied(m.from_user.id) and not is_admin(m.from_user.id):
        return await m.answer("🔧 Shop is temporarily unavailable. Please try again later.")
    u=get_user(m.from_user)
    rows=db_execute("SELECT p.* FROM favorites f JOIN products p ON p.id=f.product_id WHERE f.user_id=%s AND p.active=1 ORDER BY f.created_at DESC LIMIT 30",(u["id"],),"all")
    if not rows: return await m.answer("⭐ <b>Your Favorites</b>\n\nNo saved products yet. Open a product and tap ⭐ Add to Favorites.")
    buttons=[[InlineKeyboardButton(text=f"{'🟢' if effective_stock(p)>0 else '🔴'} {p['name']} • {float(p['price']):g} {currency()}",callback_data=f"product:{p['id']}")] for p in rows]
    await m.answer(f"⭐ <b>Your Favorites</b> ({len(rows)})\n\nTap a product to view or buy.",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("buy:"))
async def buy(c: CallbackQuery,state:FSMContext):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    pid=int(c.data.split(":")[1]); p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one"); u=get_user(c.from_user)
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    if u["blocked"] and not is_admin(c.from_user.id): return await c.answer("Account blocked.",show_alert=True)
    if effective_stock(p)<1: return await c.answer("Out of stock.",show_alert=True)
    await state.update_data(pid=pid); await state.set_state(Buy.uid); await c.answer(); await c.message.answer("🆔 <b>Send your game/player UID.</b>\n\nSend /cancel to cancel.")

@router.message(Buy.uid)
async def buy_uid(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear()
        return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."), reply_markup=inline_home_kb())
    uid=(m.text or "").strip()
    if uid.lower()=="/cancel": await state.clear(); return await m.answer("❌ Cancelled.")
    if len(uid)<2 or len(uid)>64: return await m.answer("❌ Please send a valid UID.")
    d=await state.get_data(); p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(d["pid"],),"one"); u=get_user(m.from_user)
    if not p or effective_stock(p)<1: await state.clear(); return await m.answer("❌ Product is out of stock.")
    if float(u["balance"])<float(p["price"]): await state.clear(); return await m.answer(f"❌ <b>Insufficient balance</b>\n\nPrice: {fmt_money(p['price'])}\nBalance: {fmt_money(u['balance'])}\nNeed: {fmt_money(float(p['price'])-float(u['balance']))}")
    await state.update_data(game_uid=uid); await state.set_state(Buy.confirm)
    buy_prompt = custom_text("buy_prompt", "Confirm your purchase:")
    await m.answer(f"🛒 <b>Purchase Confirmation</b>\n\n🎮 Product: <b>{html.escape(p['name'])}</b>\n🆔 UID: <code>{uid}</code>\n💰 Price: <b>{fmt_money(p['price'])}</b>\n📦 Quantity: <b>1</b>\n⭐ Total: <b>{fmt_money(p['price'])}</b>\n\n💳 Your balance: <b>{fmt_money(u['balance'])}</b>\n\n{html.escape(buy_prompt)}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("button_confirm","✅ Confirm Purchase"),callback_data="order:confirm")],[InlineKeyboardButton(text=setting("button_back","⬅️ Back"),callback_data="order:cancel"),InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))

@router.callback_query(Buy.confirm,F.data=="order:cancel")
async def order_cancel(c:CallbackQuery,state:FSMContext): await state.clear(); await c.answer("Cancelled"); await c.message.answer("❌ Order cancelled.")

def award_completed_order_rewards(cur, order_id, user_id, total):
    """Award purchase/referral rewards exactly once, only after an order is completed."""
    cur.execute("SELECT rewards_awarded FROM orders WHERE id=%s FOR UPDATE", (order_id,))
    row = cur.fetchone()
    if not row or row["rewards_awarded"]:
        return 0
    earned = max(1, int(float(total) // 10))
    cur.execute(
        "UPDATE users SET loyalty_points=loyalty_points+%s, lifetime_spend=lifetime_spend+%s, updated_at=NOW() WHERE id=%s",
        (earned, total, user_id),
    )
    cur.execute("SELECT referred_by FROM users WHERE id=%s FOR UPDATE", (user_id,))
    ref_row = cur.fetchone()
    if ref_row and ref_row["referred_by"]:
        cur.execute(
            "SELECT COUNT(*) AS c FROM orders WHERE user_id=%s AND status='completed' AND id<>%s",
            (user_id, order_id),
        )
        if int(cur.fetchone()["c"]) == 0:
            cur.execute("UPDATE users SET loyalty_points=loyalty_points+50,updated_at=NOW() WHERE id=%s", (ref_row["referred_by"],))
            cur.execute("UPDATE users SET loyalty_points=loyalty_points+50,updated_at=NOW() WHERE id=%s", (user_id,))
    cur.execute("UPDATE orders SET rewards_awarded=TRUE,updated_at=NOW() WHERE id=%s", (order_id,))
    return earned


@router.callback_query(Buy.confirm,F.data=="order:confirm")
async def order_confirm(c:CallbackQuery,state:FSMContext):
    if maintenance_active() and not is_admin(c.from_user.id):
        await state.clear()
        return await c.answer("Shop is under maintenance.", show_alert=True)
    d=await state.get_data(); await state.clear()
    with DB_LOCK:
        try:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM users WHERE tg_id=%s FOR UPDATE",(c.from_user.id,)); u=cur.fetchone()
                    cur.execute("SELECT * FROM products WHERE id=%s AND active=1 FOR UPDATE",(d["pid"],)); p=cur.fetchone()
                    if not u or not p: raise RuntimeError("Order unavailable.")
                    total=float(p["price"])
                    if float(u["balance"])<total: raise RuntimeError("Balance changed. Please retry.")
                    cur.execute("SELECT COUNT(*) AS c FROM product_codes WHERE product_id=%s AND status='available'",(p["id"],)); code_count=int(cur.fetchone()["c"])
                    auto_code=(p["delivery_type"]=="code" or code_count>0)
                    delivered_code=None
                    if auto_code:
                        cur.execute("SELECT * FROM product_codes WHERE product_id=%s AND status='available' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",(p["id"],)); code_row=cur.fetchone()
                        if not code_row: raise RuntimeError("Code stock changed. Please retry.")
                        cur.execute("UPDATE product_codes SET status='sold',sold_to=%s,sold_at=NOW() WHERE id=%s AND status='available'",(u["id"],code_row["id"]))
                        delivered_code=code_row["code"]; status="completed"
                    else:
                        cur.execute("UPDATE products SET stock=stock-1,updated_at=NOW() WHERE id=%s AND stock>0",(p["id"],))
                        if cur.rowcount!=1: raise RuntimeError("Stock changed. Please retry.")
                        status="pending"
                    cur.execute("UPDATE users SET balance=balance-%s,updated_at=NOW() WHERE id=%s AND balance>=%s",(total,u["id"],total))
                    if cur.rowcount!=1: raise RuntimeError("Balance changed. Please retry.")
                    cur.execute("INSERT INTO orders(user_id,product_id,game_uid,total,delivered_code,status) VALUES(%s,%s,%s,%s,%s,%s) RETURNING id",(u["id"],p["id"],d["game_uid"],total,delivered_code,status)); order_id=cur.fetchone()["id"]
                    if delivered_code: cur.execute("UPDATE product_codes SET order_id=%s WHERE id=%s",(order_id,code_row["id"]))
                    cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(u["id"],-total,"purchase",f"Order #{order_id}"))
                    if status == "completed":
                        earned = award_completed_order_rewards(cur, order_id, u["id"], total)
                    if auto_code: sync_code_product_stock(p["id"],conn)
        except Exception as exc:
            print(f"order_confirm error: {exc}")
            return await c.answer("Order could not be completed. Your balance and stock were not charged.",show_alert=True)
    await c.answer("✅ Payment successful")
    if delivered_code:
        await c.message.answer(f"✅ <b>Purchase Successful!</b>\n\n🎉 Order placed successfully.\n\n🧾 Order ID: <b>#{order_id}</b>\n🎮 Product: <b>{html.escape(p['name'])}</b>\n🆔 UID: <code>{d['game_uid']}</code>\n💰 Amount: <b>{fmt_money(total)}</b>\n📅 Status: <b>Delivered</b>\n\n🎁 <b>Your Code</b>\n<code>{delivered_code}</code>\n\n⚠️ Keep this code private.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("inline_view_orders", "📦 View My Orders"),callback_data="home:orders")],[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))
    else:
        await c.message.answer(f"⏳ <b>Order Submitted</b>\n\n🧾 Order ID: <b>#{order_id}</b>\n🎮 Product: <b>{html.escape(p['name'])}</b>\n🆔 UID: <code>{d['game_uid']}</code>\n💰 Amount: <b>{fmt_money(total)}</b>\n📌 Status: <b>Pending</b>\n\nYou will be notified when delivery is completed.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("inline_view_orders", "📦 View My Orders"),callback_data="home:orders")],[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))
        for admin_id in ADMIN_IDS:
            try: await c.bot.send_message(admin_id,f"🧾 <b>New Order #{order_id}</b>\n\n👤 User: <code>{u['tg_id']}</code>\n🎮 Product: {html.escape(p['name'])}\n🆔 UID: <code>{d['game_uid']}</code>\n💰 Total: {fmt_money(total)}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_order_complete", "✅ Complete"),callback_data=f"order_complete:{order_id}"),InlineKeyboardButton(text=setting("admin_order_reject_refund", "❌ Reject + Refund"),callback_data=f"order_reject:{order_id}")]]))
            except Exception: pass

@router.message(Command("version"))
async def version_command(m:Message): await m.answer(f"🚀 <b>{html.escape(shop_name())} {APP_VERSION}</b>\n☁️ PostgreSQL database enabled\n⚡ Instant code delivery enabled\n🔎 Smart product search enabled\n⭐ Favorites / wishlist enabled\n🏅 Loyalty rewards enabled\n🤝 Referral rewards enabled\n📦 Order tracking enabled\n🔔 Buyer notifications enabled\n📸 Payment receipt verification enabled\n💎 Premium storefront UI enabled\n📢 Announcement banner enabled")

@router.message(Command("profile"))
@router.message(F.text=="👤 Profile")
@router.message(F.text=="👤 My Account")
async def profile(m:Message):
    u=get_user(m.from_user); row=db_execute("SELECT COUNT(*) AS c FROM orders WHERE user_id=%s",(u["id"],),"one")
    points=int(u.get("loyalty_points") or 0)
    tier,pct,next_points=vip_progress(points)
    bar="█"*max(0,pct//10)+"░"*(10-max(0,pct//10))
    await m.answer(f"👤 <b>My Premium Account</b>\n\n🆔 ID: <code>{u['tg_id']}</code>\n💳 Wallet: <b>{fmt_money(u['balance'])}</b>\n🧾 Orders: <b>{row['c']}</b>\n⭐ Points: <b>{points}</b>\n🏅 VIP: <b>{tier}</b>\n📈 {bar} {pct}%\n💰 Lifetime spend: <b>{fmt_money(u.get('lifetime_spend') or 0)}</b>\n📅 Member since: <code>{u['created_at']}</code>")

@router.message(Command("orders"))
@router.message(F.text=="📦 My Orders")
@router.message(F.text=="🧾 My Orders")
async def my_orders(m:Message):
    u=get_user(m.from_user); rows=db_execute("SELECT o.id,o.total,o.status,o.created_at,p.name FROM orders o JOIN products p ON p.id=o.product_id WHERE o.user_id=%s ORDER BY o.id DESC LIMIT 10",(u["id"],),"all")
    if not rows: return await m.answer("📦 You have no orders yet.")
    lines=["📦 <b>Your Recent Orders</b>\n"]
    buttons=[]
    for r in rows:
        lines.append(f"#{r['id']} • {r['name']}\n💰 {fmt_money(r['total'])} • {status_emoji(r['status'])} {r['status'].title()}\n🕒 {r['created_at']}\n")
        buttons.append([InlineKeyboardButton(text=f"🧾 Order #{r['id']}",callback_data=f"order_detail:{r['id']}")])
    await m.answer("\n".join(lines),reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("order_detail:"))
async def order_detail(c:CallbackQuery):
    if user_blocked(c.from_user.id) and not is_admin(c.from_user.id):
        return await c.answer("Account blocked.",show_alert=True)
    oid=int(c.data.split(":",1)[1]); u=get_user(c.from_user)
    o=db_execute("SELECT o.*,p.name,p.category,p.delivery_type FROM orders o JOIN products p ON p.id=o.product_id WHERE o.id=%s AND o.user_id=%s",(oid,u["id"]),"one")
    if not o: return await c.answer("Order not found.",show_alert=True)
    text=(f"🧾 <b>Order #{o['id']}</b>\n\n🎮 Product: <b>{html.escape(o['name'])}</b>\n🏷 Category: <b>{html.escape(o['category'])}</b>\n🆔 UID: <code>{html.escape(o['game_uid'] or '-')}</code>\n💰 Total: <b>{fmt_money(o['total'])}</b>\n📌 Status: <b>{status_emoji(o['status'])} {o['status'].title()}</b>\n🕒 Created: <code>{o['created_at']}</code>")
    if o['delivered_code']:
        text += f"\n\n🎁 Code: <code>{html.escape(o['delivered_code'])}</code>"
    if o['admin_note']:
        text += f"\n📝 Note: {html.escape(o['admin_note'])}"
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("inline_my_orders", "⬅️ My Orders"),callback_data="my_orders_back")]]))

@router.callback_query(F.data=="my_orders_back")
async def my_orders_back(c:CallbackQuery):
    u=get_user(c.from_user); rows=db_execute("SELECT o.id,o.total,o.status,o.created_at,p.name FROM orders o JOIN products p ON p.id=o.product_id WHERE o.user_id=%s ORDER BY o.id DESC LIMIT 10",(u["id"],),"all")
    if not rows: return await c.answer("No orders.",show_alert=True)
    buttons=[[InlineKeyboardButton(text=f"🧾 Order #{r['id']}",callback_data=f"order_detail:{r['id']}")] for r in rows]
    await c.answer(); await c.message.edit_text("📦 <b>Your Recent Orders</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.message(Command("balance", "deposit"))
@router.message(F.text=="💰 Deposit")
@router.message(F.text=="💰 Add Balance")
@router.message(F.text=="💳 Wallet")
async def add_balance(m:Message,state:FSMContext):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id): return await m.answer("🚫 Your account is blocked.")
    if maintenance_active() and not is_admin(m.from_user.id): return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."))
    await state.set_state(PaymentState.amount); await m.answer(custom_text("deposit_prompt", "💳 <b>Add Balance</b>\n\nSend amount. Example: <code>500</code>") + f"\n\n💳 {setting('payment_info',PAYMENT_INFO)}")

@router.message(PaymentState.amount)
async def payment_amount(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear()
        return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."), reply_markup=inline_home_kb())
    try: amount=float((m.text or "").strip())
    except ValueError: return await m.answer("❌ Enter a valid amount.")
    min_amt=float(setting("deposit_min","10") or 10); max_raw=float(setting("deposit_max","0") or 0); max_amt=max_raw if max_raw>0 else 1000000000;
    if amount<min_amt or amount>max_amt: return await m.answer(f"❌ Amount must be between {min_amt:g} and {max_amt:g}.")
    await state.update_data(amount=amount); await state.set_state(PaymentState.method); await m.answer(custom_text("payment_method_prompt", "💳 Choose payment method:"),reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("payment_bkash_label","bKash"),callback_data="paymethod:bkash"),InlineKeyboardButton(text=setting("payment_nagad_label","Nagad"),callback_data="paymethod:nagad")]]))

@router.callback_query(PaymentState.method,F.data.startswith("paymethod:"))
async def payment_method(c:CallbackQuery,state:FSMContext):
    if maintenance_active() and not is_admin(c.from_user.id):
        await state.clear()
        return await c.answer("Shop is under maintenance.", show_alert=True)
    method=c.data.split(":",1)[1]; await state.update_data(method=method); await state.set_state(PaymentState.trx); await c.answer(); await c.message.answer(f"💳 Method: <b>{method.title()}</b>\n\nSend payment TrxID.")

@router.message(PaymentState.trx)
async def payment_trx(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear()
        return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."), reply_markup=inline_home_kb())
    trx=(m.text or "").strip()
    if len(trx)<3 or len(trx)>100: return await m.answer("❌ Invalid TrxID.")
    d=await state.get_data(); u=get_user(m.from_user)
    try:
        row=db_insert_returning("INSERT INTO payments(user_id,amount,method,trx_id) VALUES(%s,%s,%s,%s) RETURNING id",(u["id"],d["amount"],d["method"],trx))
        payment_id=row["id"]
    except errors.UniqueViolation:
        await state.clear(); return await m.answer("❌ This TrxID was already submitted.")
    await state.update_data(payment_id=payment_id, trx_id=trx)
    await state.set_state(PaymentState.receipt)
    await m.answer(
        f"📸 <b>Payment screenshot</b>\n\nPayment Request: <b>#{payment_id}</b>\nSend the payment screenshot as a photo/document.\n\nIf you cannot send one, use <code>/skip</code>.",
        reply_markup=inline_home_kb(),
    )

@router.message(PaymentState.receipt, F.photo)
@router.message(PaymentState.receipt, F.document)
async def payment_receipt_upload(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear()
        return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."), reply_markup=inline_home_kb())
    d=await state.get_data(); payment_id=d.get("payment_id")
    if not payment_id:
        await state.clear()
        return await m.answer("❌ Payment session expired. Please start again.", reply_markup=inline_home_kb())
    file_id = m.photo[-1].file_id if m.photo else m.document.file_id
    db_execute("INSERT INTO payment_receipts(payment_id,file_id) VALUES(%s,%s) ON CONFLICT(payment_id) DO UPDATE SET file_id=EXCLUDED.file_id",(payment_id,file_id))
    await _finish_payment_submission(m, state, receipt=True)

@router.message(PaymentState.receipt, Command("skip"))
async def payment_receipt_skip(m:Message,state:FSMContext):
    await _finish_payment_submission(m, state, receipt=False)

async def _finish_payment_submission(m:Message,state:FSMContext,receipt=False):
    d=await state.get_data(); payment_id=d.get("payment_id"); u=get_user(m.from_user)
    if not payment_id:
        await state.clear()
        return await m.answer("❌ Payment session expired. Please start again.", reply_markup=inline_home_kb())
    row=db_execute("SELECT * FROM payments WHERE id=%s",(payment_id,),"one")
    if not row:
        await state.clear()
        return await m.answer("❌ Payment request not found.", reply_markup=inline_home_kb())
    await state.clear()
    receipt_text="📸 Receipt attached" if receipt else "📎 No receipt attached"
    await m.answer(
        f"✅ <b>Payment Request #{payment_id}</b>\n\n💰 Amount: {fmt_money(row['amount'])}\n💳 Method: {row['method'].title()}\n🧾 TrxID: <code>{html.escape(row['trx_id'])}</code>\n{receipt_text}\n⏳ Waiting for admin approval.",
        reply_markup=inline_home_kb(),
    )
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_credit", "✅ Credit"),callback_data=f"pay_credit:{payment_id}"),InlineKeyboardButton(text=setting("admin_reject", "❌ Reject"),callback_data=f"pay_reject:{payment_id}")]])
    for admin_id in ADMIN_IDS:
        try:
            admin_text=(f"💳 <b>New Payment #{payment_id}</b>\n\n👤 User: <code>{u['tg_id']}</code>\n💰 Amount: {fmt_money(row['amount'])}\nMethod: {row['method'].title()}\nTrxID: <code>{html.escape(row['trx_id'])}</code>\n{receipt_text}")
            await m.bot.send_message(admin_id,admin_text,reply_markup=kb)
            if receipt:
                receipt_row=db_execute("SELECT file_id FROM payment_receipts WHERE payment_id=%s",(payment_id,),"one")
                if receipt_row:
                    await m.bot.send_document(admin_id,receipt_row["file_id"],caption=f"📸 Payment #{payment_id} receipt",reply_markup=kb)
        except Exception:
            pass

@router.message(PaymentState.receipt)
async def payment_receipt_invalid(m:Message,state:FSMContext):
    await m.answer("📸 Please send a screenshot/photo or document. If unavailable, use /skip.")

def loyalty_tier(points):
    points=int(points or 0)
    if points >= 1000: return "💎 Diamond"
    if points >= 500: return "🥇 Gold"
    if points >= 200: return "🥈 Silver"
    return "🥉 Starter"


@router.message(Command("rewards"))
@router.message(F.text=="⭐ Rewards")
@router.message(F.text=="🏆 Rewards")
async def rewards(m:Message):
    u=get_user(m.from_user)
    refs=db_execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=%s",(u["id"],),"one")
    points=int(u.get("loyalty_points") or 0)
    spend=float(u.get("lifetime_spend") or 0)
    tier=loyalty_tier(points)
    tier_name,pct,next_points=vip_progress(points)
    bar="█"*max(0,pct//10)+"░"*(10-max(0,pct//10))
    await m.answer(
        f"⭐ <b>Premium Rewards Center</b>\n\n"
        f"🏅 VIP Tier: <b>{tier_name}</b>\n"
        f"📈 Progress: <b>{bar}</b> {pct}%\n"
        f"⭐ Loyalty points: <b>{points}</b>\n"
        f"🎯 Next milestone: <b>{next_points} points</b>\n"
        f"💰 Lifetime spend: <b>{fmt_money(spend)}</b>\n"
        f"🤝 Successful referrals: <b>{int(refs['c'])}</b>\n\n"
        "Earn points automatically from completed purchases and referrals."
    )


@router.message(Command("refer"))
@router.message(F.text=="🤝 Refer & Earn")
async def refer(m:Message):
    u=get_user(m.from_user)
    me=await m.bot.get_me()
    link=f"https://t.me/{me.username}?start=ref_{m.from_user.id}" if me.username else f"Use /start ref_{m.from_user.id}"
    refs=db_execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=%s",(u["id"],),"one")
    await m.answer(
        "🤝 <b>Refer & Earn</b>\n\n"
        "Invite friends with your personal link. When a referred buyer completes their first purchase, both accounts receive loyalty recognition.\n\n"
        f"🔗 <b>Your link</b>\n<code>{html.escape(link)}</code>\n\n"
        f"👥 Your referrals: <b>{int(refs['c'])}</b>"
    )


@router.message(Command("support", "help"))
@router.message(F.text=="💬 Support")
@router.message(F.text=="🎧 Support")
@router.message(F.text=="🆘 Support")
async def support(m:Message): await m.answer(f"🎧 <b>Support</b>\n\nContact: {setting('support',SUPPORT)}")

@router.message(Command("cancel"))
async def cancel(m:Message,state:FSMContext): await state.clear(); await m.answer("❌ Cancelled.")

@router.message(Command("announcement"))
async def announcement_command(m:Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Denied")
    parts=m.text.split(" ",1)
    if len(parts)==1:
        return await m.answer(f"📢 <b>Current announcement</b>\n\n{setting('announcement','(none)')}\n\nUse: <code>/announcement Your message</code>")
    value=html.escape(parts[1].strip())
    set_setting("announcement", value)
    admin_log(m.from_user.id,"announcement_update",value)
    await m.answer("✅ Premium home announcement updated. Users will see it on /start.")

# ---------------- Admin ----------------
@router.message(Command("admin"))
async def admin_command(m:Message):
    if not is_admin(m.from_user.id): return await m.answer("❌ Access denied.")
    await m.answer(f"👑 <b>{html.escape(shop_name())} {APP_VERSION}</b>\nAdmin Control Center",reply_markup=admin_menu())

@router.callback_query(F.data=="admin:dashboard")
async def admin_dashboard(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    row=db_execute("""SELECT COUNT(*) AS users,
        (SELECT COUNT(*) FROM products WHERE active=1) AS products,
        (SELECT COUNT(*) FROM products WHERE active=1 AND (CASE WHEN delivery_type='code' THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=products.id AND pc.status='available') ELSE stock END)<=%s) AS low_stock,
        (SELECT COUNT(*) FROM orders WHERE status='pending') AS pending_orders,
        (SELECT COUNT(*) FROM orders WHERE status='completed') AS completed,
        (SELECT COUNT(*) FROM payments WHERE status='pending') AS pending_payments,
        (SELECT COALESCE(SUM(total),0) FROM orders WHERE status='completed') AS sales,
        (SELECT COALESCE(SUM(total),0) FROM orders WHERE status='completed' AND created_at::date=CURRENT_DATE) AS today_sales,
        (SELECT COALESCE(SUM(balance),0) FROM users) AS balance,
        (SELECT COUNT(*) FROM product_codes WHERE status='available') AS codes
        FROM users""",(low_stock_threshold(),),fetch="one")
    text=(f"📊 <b>{shop_name()} — {APP_VERSION} Dashboard</b>\n\n👥 Users: <b>{row['users']}</b>\n🛍 Active Products: <b>{row['products']}</b>\n⚠️ Low-stock Products: <b>{row['low_stock']}</b>\n🎫 Available Codes: <b>{row['codes']}</b>\n🧾 Pending Orders: <b>{row['pending_orders']}</b>\n💳 Pending Payments: <b>{row['pending_payments']}</b>\n✅ Completed Orders: <b>{row['completed']}</b>\n💵 Today Sales: <b>{fmt_money(row['today_sales'])}</b>\n💰 All-time Sales: <b>{fmt_money(row['sales'])}</b>\n👛 User Wallet Total: <b>{fmt_money(row['balance'])}</b>")
    await c.answer(); await c.message.edit_text(text,reply_markup=admin_menu())

@router.callback_query(F.data=="admin:premium")
async def admin_premium(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    row=db_execute("""SELECT
        (SELECT COUNT(*) FROM users WHERE referred_by IS NOT NULL) referred_users,
        (SELECT COUNT(*) FROM users WHERE loyalty_points>0) loyalty_users,
        (SELECT COALESCE(SUM(loyalty_points),0) FROM users) total_points,
        (SELECT COALESCE(AVG(total),0) FROM orders WHERE status='completed') avg_order
    """,fetch="one")
    await c.answer()
    await c.message.answer(
        "💎 <b>Premium Analytics</b>\n\n"
        f"🤝 Referred buyers: <b>{row['referred_users']}</b>\n"
        f"⭐ Loyalty users: <b>{row['loyalty_users']}</b>\n"
        f"🏅 Total points issued: <b>{row['total_points']}</b>\n"
        f"🧾 Average completed order: <b>{fmt_money(row['avg_order'])}</b>"
    )


@router.callback_query(F.data=="admin:reports")
async def reports(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    row=db_execute("""SELECT
        COALESCE(SUM(total) FILTER (WHERE status='completed' AND created_at::date=CURRENT_DATE),0) AS today_sales,
        COUNT(*) FILTER (WHERE created_at::date=CURRENT_DATE) AS today_orders,
        COALESCE(SUM(total) FILTER (WHERE status='completed' AND created_at >= NOW()-INTERVAL '7 days'),0) AS week_sales,
        COUNT(*) FILTER (WHERE created_at >= NOW()-INTERVAL '7 days') AS week_orders,
        COALESCE(SUM(total) FILTER (WHERE status='completed'),0) AS all_sales,
        COUNT(*) FILTER (WHERE status='pending') AS pending,
        COUNT(*) FILTER (WHERE status='refunded') AS refunded
        FROM orders""",fetch="one")
    users=db_execute("SELECT COUNT(*) AS c FROM users",fetch="one")["c"]
    top=db_execute("""SELECT p.name,COUNT(*) AS orders,COALESCE(SUM(o.total),0) AS sales
        FROM orders o JOIN products p ON p.id=o.product_id
        WHERE o.status='completed' GROUP BY p.id,p.name ORDER BY orders DESC,sales DESC LIMIT 5""",fetch="all")
    top_text="\n".join(f"• {html.escape(r['name'])}: {r['orders']} orders / {fmt_money(r['sales'])}" for r in top) or "• No completed sales yet"
    text=(f"📊 <b>{APP_VERSION} Sales Report</b>\n\n📅 Today sales: <b>{fmt_money(row['today_sales'])}</b>\n🧾 Today orders: <b>{row['today_orders']}</b>\n📆 7-day sales: <b>{fmt_money(row['week_sales'])}</b>\n🧾 7-day orders: <b>{row['week_orders']}</b>\n💰 All-time sales: <b>{fmt_money(row['all_sales'])}</b>\n👥 Users: <b>{users}</b>\n⏳ Pending orders: <b>{row['pending']}</b>\n↩️ Refunded orders: <b>{row['refunded']}</b>\n\n🏆 <b>Top Products</b>\n{top_text}")
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]))

@router.callback_query(F.data=="admin:products")
async def admin_products(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute("SELECT * FROM products ORDER BY id DESC",fetch="all"); buttons=[]
    for p in rows: buttons.append([InlineKeyboardButton(text=f"{'🟢' if p['active'] else '🔴'} {p['name'][:24]} • {effective_stock(p)}",callback_data=f"p:{p['id']}")])
    buttons += [[InlineKeyboardButton(text=setting("admin_add_product", "➕ Add Product"),callback_data="admin:add_product")],[InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]
    await c.answer(); await c.message.edit_text("🛍 <b>Product Management</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("p:"))
async def product_manage(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1]); p=db_execute("SELECT * FROM products WHERE id=%s",(pid,),"one")
    if not p: return await c.answer("Not found",show_alert=True)
    text=f"🎮 <b>{html.escape(p['name'])}</b>\n🏷 Category: {html.escape(p['category'])}\n📦 Quantity: {p['quantity']}\n💰 Price: {fmt_money(p['price'])}\n🚚 Delivery: {p['delivery_type']}\n📊 Stock: {effective_stock(p)}\n🔘 Active: {'Yes' if p['active'] else 'No'}\n\n{p['description'] or 'No description.'}"
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_edit", "✏️ Edit"),callback_data=f"pedit:{pid}"),InlineKeyboardButton(text=setting("admin_toggle_product", "🔄 Enable/Disable"),callback_data=f"ptoggle:{pid}")],[InlineKeyboardButton(text=setting("admin_add_codes", "🎫 Add Codes"),callback_data=f"codes_add:{pid}")],[InlineKeyboardButton(text=setting("admin_delete", "🗑 Delete"),callback_data=f"pdelete:{pid}")],[InlineKeyboardButton(text=setting("admin_products_back", "⬅️ Products"),callback_data="admin:products")]]))

@router.callback_query(F.data=="admin:add_product")
async def admin_add_product_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await c.answer(); await state.set_state(AdminState.add_product); await c.message.answer("➕ <b>Add Product</b>\n\nSend exactly:\n<code>Name | Category | Quantity | Price | Delivery | Stock | Description</code>\n\nCode example:\n<code>60 UC | UC | 60 | 150 | code | 0 | 60 UC code</code>\nManual example:\n<code>UC Topup | UC | 60 | 150 | manual | 10 | Manual topup</code>\n\neFootball coin example:\n<code>130 Coins | eFootball Coins | 130 | 100 | manual | 999 | eFootball 130 coins</code>\n\nUse category <code>Game &gt; Pack</code>, e.g. <code>eFootball &gt; Coins</code>, <code>PUBG Mobile &gt; UC</code>, <code>Free Fire &gt; Diamonds</code>, or <code>COD Mobile &gt; CP</code>.")

@router.message(AdminState.add_product)
async def admin_add_product(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return
    parts=[x.strip() for x in (m.text or "").split("|",6)]
    if len(parts)!=7: return await m.answer("❌ Invalid format.")
    name,category,quantity,price,delivery,stock,description=parts
    try: quantity=int(quantity); price=float(price); stock=int(stock)
    except ValueError: return await m.answer("❌ Quantity, price and stock must be numbers.")
    delivery=delivery.lower()
    if delivery not in {"code","manual"} or price<=0 or quantity<0 or stock<0: return await m.answer("❌ Invalid values.")
    if delivery=="code": stock=0
    row=db_insert_returning("INSERT INTO products(name,category,quantity,price,delivery_type,stock,description) VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id",(name,category or "Gaming",quantity,price,delivery,stock,description)); pid=row["id"]
    admin_log(m.from_user.id,"add_product",f"product #{pid}"); await state.clear(); await m.answer(f"✅ Product #{pid} created.",reply_markup=admin_menu())

@router.callback_query(F.data.startswith("pedit:"))
async def product_edit_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1])
    p=db_execute("SELECT * FROM products WHERE id=%s",(pid,),"one")
    if not p: return await c.answer("Not found",show_alert=True)
    await state.update_data(pid=pid)
    await state.set_state(AdminState.edit_product)
    await c.answer()
    await c.message.answer(
        "✏️ <b>Edit Product</b>\n\n"
        "Send exactly: <code>Name | Category | Quantity | Price | Delivery | Stock | Description</code>\n\n"
        f"Current: <code>{html.escape(p['name'])} | {html.escape(p['category'])} | {p['quantity']} | {p['price']} | {p['delivery_type']} | {effective_stock(p)} | {html.escape(p['description'] or '')}</code>"
    )

@router.message(AdminState.edit_product)
async def product_edit_save(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return
    d=await state.get_data(); pid=d.get("pid")
    parts=[x.strip() for x in (m.text or "").split("|",6)]
    if len(parts)!=7: return await m.answer("❌ Invalid format. Use 7 fields separated by |.")
    name,category,quantity,price,delivery,stock,description=parts
    if not name or not category: return await m.answer("❌ Name and category cannot be empty.")
    try:
        quantity=int(quantity); price=float(price); stock=int(stock)
    except ValueError:
        return await m.answer("❌ Quantity, price and stock must be numbers.")
    delivery=delivery.lower()
    if delivery not in {"code","manual"} or price<=0 or quantity<0 or stock<0:
        return await m.answer("❌ Invalid values.")
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM products WHERE id=%s FOR UPDATE",(pid,))
                current=cur.fetchone()
                if not current:
                    await state.clear()
                    return await m.answer("❌ Product not found.")
                cur.execute("SELECT COUNT(*) AS c FROM product_codes WHERE product_id=%s AND status='available'",(pid,))
                available=int(cur.fetchone()["c"])
                if delivery=="manual" and available:
                    return await m.answer("❌ This product still has available codes. Remove/sell those codes before switching delivery to manual.")
                if delivery=="code":
                    stock=available
                cur.execute(
                    "UPDATE products SET name=%s,category=%s,quantity=%s,price=%s,delivery_type=%s,stock=%s,description=%s,updated_at=NOW() WHERE id=%s",
                    (name,category,quantity,price,delivery,stock,description,pid),
                )
    admin_log(m.from_user.id,"edit_product",f"product #{pid}")
    await state.clear()
    await m.answer(f"✅ Product #{pid} updated.",reply_markup=admin_menu())

@router.callback_query(F.data.startswith("ptoggle:"))
async def product_toggle(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1]); db_execute("UPDATE products SET active=CASE WHEN active=1 THEN 0 ELSE 1 END,updated_at=NOW() WHERE id=%s",(pid,)); admin_log(c.from_user.id,"toggle_product",f"product #{pid}"); await c.answer("Updated"); await product_manage(c)

@router.callback_query(F.data.startswith("pdelete:"))
async def product_delete(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1]); row=db_execute("SELECT COUNT(*) AS c FROM orders WHERE product_id=%s",(pid,),"one")
    if row["c"]: return await c.answer("Cannot delete a product with order history. Disable it instead.",show_alert=True)
    db_execute("DELETE FROM products WHERE id=%s",(pid,)); admin_log(c.from_user.id,"delete_product",f"product #{pid}"); await c.answer("Deleted"); await admin_products(c)

@router.callback_query(F.data=="admin:codes")
async def admin_codes(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute("SELECT p.id,p.name,COUNT(pc.id) FILTER (WHERE pc.status='available') AS available,COUNT(pc.id) FILTER (WHERE pc.status='sold') AS sold FROM products p LEFT JOIN product_codes pc ON pc.product_id=p.id WHERE p.delivery_type='code' GROUP BY p.id ORDER BY p.id DESC",fetch="all")
    buttons=[[InlineKeyboardButton(text=f"🎫 {r['name'][:20]} • {r['available'] or 0} available",callback_data=f"codes_add:{r['id']}")] for r in rows]
    buttons.append([InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]); await c.answer(); await c.message.edit_text("🎫 <b>Code Inventory</b>\nSelect a product:",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("codes_add:"))
async def codes_add_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1]); p=db_execute("SELECT * FROM products WHERE id=%s",(pid,),"one")
    if not p: return await c.answer("Not found",show_alert=True)
    await c.answer(); await state.update_data(pid=pid); await state.set_state(AdminState.add_codes); await c.message.answer(f"🎫 <b>{html.escape(p['name'])}</b>\n\nSend one code per line. Duplicate codes are skipped.")

@router.message(AdminState.add_codes)
async def codes_add(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return
    d=await state.get_data(); lines=list(dict.fromkeys(x.strip() for x in (m.text or "").splitlines() if x.strip()))
    if not lines: return await m.answer("❌ No codes found.")
    added=0; duplicates=0
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                for raw in lines:
                    cur.execute(
                        "INSERT INTO product_codes(product_id,code) VALUES(%s,%s) "
                        "ON CONFLICT(code) DO NOTHING RETURNING id",
                        (d["pid"], raw),
                    )
                    if cur.fetchone():
                        added += 1
                    else:
                        duplicates += 1
                cur.execute(
                    "UPDATE products SET delivery_type='code', "
                    "stock=(SELECT COUNT(*) FROM product_codes "
                    "WHERE product_id=%s AND status='available'), updated_at=NOW() "
                    "WHERE id=%s",
                    (d["pid"], d["pid"]),
                )
    admin_log(m.from_user.id,"add_codes",f"product #{d['pid']} added={added} duplicates={duplicates}"); await state.clear(); await m.answer(f"✅ Added: <b>{added}</b>\n♻️ Duplicates skipped: <b>{duplicates}</b>",reply_markup=admin_menu())

@router.callback_query(F.data=="admin:orders")
async def admin_orders(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute("SELECT o.id,o.total,o.status,o.created_at,p.name,u.tg_id FROM orders o JOIN products p ON p.id=o.product_id JOIN users u ON u.id=o.user_id ORDER BY o.id DESC LIMIT 20",fetch="all")
    text="🧾 No orders yet." if not rows else "🧾 <b>Recent Orders</b>\n\n"+"\n".join(f"#{r['id']} • {html.escape(r['name'][:18])}\n👤 <code>{r['tg_id']}</code> • {fmt_money(r['total'])}\n{status_emoji(r['status'])} {r['status'].title()}\n" for r in rows)
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]))

@router.callback_query(F.data.startswith("order_complete:"))
async def manual_order_complete(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":")[1])
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,)); o=cur.fetchone()
                if not o or o["status"]!="pending": return await c.answer("Already processed.",show_alert=True)
                cur.execute("UPDATE orders SET status='completed',processed_at=NOW(),updated_at=NOW() WHERE id=%s",(oid,))
                award_completed_order_rewards(cur, oid, o["user_id"], o["total"])
                cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
    admin_log(c.from_user.id,"complete_order",f"order #{oid}"); await c.answer("Completed"); await c.message.edit_text(f"✅ Order #{oid} completed.")
    try: await c.bot.send_message(u["tg_id"],f"✅ <b>Order #{oid} completed</b>\nYour manual top-up/order has been processed.")
    except Exception: pass

@router.callback_query(F.data.startswith("order_reject:"))
async def manual_order_reject(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":")[1])
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,)); o=cur.fetchone()
                if not o or o["status"]!="pending": return await c.answer("Already processed.",show_alert=True)
                cur.execute("UPDATE orders SET status='refunded',refund_amount=total,processed_at=NOW(),updated_at=NOW() WHERE id=%s",(oid,))
                cur.execute("UPDATE users SET balance=balance+%s,updated_at=NOW() WHERE id=%s",(o["total"],o["user_id"]))
                if o["delivered_code"]:
                    cur.execute("UPDATE product_codes SET status='available',sold_to=NULL,order_id=NULL,sold_at=NULL WHERE order_id=%s",(oid,))
                    sync_code_product_stock(o["product_id"],conn)
                else: cur.execute("UPDATE products SET stock=stock+1,updated_at=NOW() WHERE id=%s",(o["product_id"],))
                cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(o["user_id"],o["total"],"refund",f"Order #{oid} rejected")); cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
    admin_log(c.from_user.id,"reject_refund",f"order #{oid}"); await c.answer("Rejected + refunded"); await c.message.edit_text(f"↩️ Order #{oid} rejected and refunded.")
    try: await c.bot.send_message(u["tg_id"],f"↩️ <b>Order #{oid} refunded</b>\nRefunded: {fmt_money(o['total'])}")
    except Exception: pass

@router.callback_query(F.data=="admin:payments")
async def admin_payments(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute("SELECT py.id,py.amount,py.method,py.trx_id,py.created_at,u.tg_id FROM payments py JOIN users u ON u.id=py.user_id WHERE py.status='pending' ORDER BY py.id DESC LIMIT 15",fetch="all")
    if not rows: text="💳 No pending payments."; kb=[[InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]
    else:
        text="💳 <b>Pending Payments</b>\n\n"+"\n".join(f"#{r['id']} • {fmt_money(r['amount'])}\n👤 <code>{r['tg_id']}</code> • {r['method'].title()}\nTrxID: <code>{r['trx_id']}</code>\n" for r in rows)
        kb=[]
        for r in rows:
            receipt=db_execute("SELECT 1 FROM payment_receipts WHERE payment_id=%s",(r['id'],),"one")
            row_buttons=[InlineKeyboardButton(text=f"✅ Credit #{r['id']}",callback_data=f"pay_credit:{r['id']}"),InlineKeyboardButton(text=setting("admin_reject", "❌ Reject"),callback_data=f"pay_reject:{r['id']}")]
            kb.append(row_buttons)
            if receipt:
                kb.append([InlineKeyboardButton(text=f"📸 Receipt #{r['id']}",callback_data=f"pay_receipt:{r['id']}")])
        kb.append([InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")])
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("pay_receipt:"))
async def payment_receipt_view(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":",1)[1]); row=db_execute("SELECT file_id FROM payment_receipts WHERE payment_id=%s",(pid,),"one")
    if not row: return await c.answer("Receipt not found.",show_alert=True)
    await c.answer("Sending receipt...")
    try:
        await c.message.answer_document(row["file_id"],caption=f"📸 Payment #{pid} receipt")
    except Exception:
        try: await c.message.answer_photo(row["file_id"],caption=f"📸 Payment #{pid} receipt")
        except Exception: return await c.message.answer("❌ Could not open receipt.")

@router.callback_query(F.data.startswith("pay_credit:"))
async def payment_credit(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1])
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(pid,)); p=cur.fetchone()
                if not p or p["status"]!="pending": return await c.answer("Already processed.",show_alert=True)
                cur.execute("UPDATE payments SET status='credited',updated_at=NOW() WHERE id=%s",(pid,))
                cur.execute("UPDATE users SET balance=balance+%s,updated_at=NOW() WHERE id=%s",(p["amount"],p["user_id"]))
                cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(p["user_id"],p["amount"],"payment",f"Payment #{pid}")); cur.execute("SELECT tg_id FROM users WHERE id=%s",(p["user_id"],)); u=cur.fetchone()
    admin_log(c.from_user.id,"credit_payment",f"payment #{pid}"); await c.answer("Credited"); await c.message.edit_text(f"✅ Payment #{pid} credited.")
    await notify_user(c.bot,u["tg_id"],f"💰 <b>Balance Added</b>\n\nPayment: #{pid}\nAmount: <b>{fmt_money(p['amount'])}</b>\n\nYour wallet is ready for your next purchase. 🛒")

@router.callback_query(F.data.startswith("pay_reject:"))
async def payment_reject(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1])
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(pid,))
                p=cur.fetchone()
                if not p or p["status"]!="pending": return await c.answer("Already processed.",show_alert=True)
                cur.execute("UPDATE payments SET status='rejected',updated_at=NOW() WHERE id=%s",(pid,))
                cur.execute("SELECT tg_id FROM users WHERE id=%s",(p["user_id"],))
                u=cur.fetchone()
    admin_log(c.from_user.id,"reject_payment",f"payment #{pid}")
    await c.answer("Rejected")
    await c.message.edit_text(f"❌ Payment #{pid} rejected.")
    await notify_user(c.bot,u["tg_id"],f"❌ <b>Payment Rejected</b>\n\nPayment: #{pid}\nAmount: <b>{fmt_money(p['amount'])}</b>\n\nYour payment could not be verified. Please contact support if you believe this is an error.")

@router.callback_query(F.data=="admin:users")
async def admin_users(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute("SELECT * FROM users ORDER BY id DESC LIMIT 20",fetch="all"); buttons=[[InlineKeyboardButton(text=f"{'🚫' if u['blocked'] else '🟢'} {(u['name'] or 'User')[:18]} • {float(u['balance']):.0f}",callback_data=f"user:{u['id']}")] for u in rows]; buttons.append([InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]); await c.answer(); await c.message.edit_text("👥 <b>User Management</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("user:"))
async def user_detail(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    uid=int(c.data.split(":")[1]); u=db_execute("SELECT * FROM users WHERE id=%s",(uid,),"one")
    if not u: return await c.answer("Not found.",show_alert=True)
    orders=db_execute("SELECT COUNT(*) AS c FROM orders WHERE user_id=%s",(uid,),"one")["c"]
    await c.answer(); await c.message.edit_text(f"👤 <b>{u['name']}</b>\n\nTelegram ID: <code>{u['tg_id']}</code>\nUsername: @{u['username'] or '-'}\nBalance: <b>{fmt_money(u['balance'])}</b>\nOrders: <b>{orders}</b>\nStatus: {'🚫 Blocked' if u['blocked'] else '🟢 Active'}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_unblock", "🔓 Unblock") if u['blocked'] else setting("admin_block", "🚫 Block"),callback_data=f"user_toggle:{uid}")],[InlineKeyboardButton(text=setting("admin_users_back", "⬅️ Users"),callback_data="admin:users")]]))

@router.callback_query(F.data.startswith("user_toggle:"))
async def user_toggle(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    uid=int(c.data.split(":")[1]); row=db_execute("SELECT blocked FROM users WHERE id=%s",(uid,),"one")
    if not row: return await c.answer("Not found.",show_alert=True)
    new=0 if row["blocked"] else 1; db_execute("UPDATE users SET blocked=%s,updated_at=NOW() WHERE id=%s",(new,uid)); admin_log(c.from_user.id,"toggle_user",f"user #{uid} -> {new}"); await c.answer("Updated"); await user_detail(c)

@router.callback_query(F.data=="admin:balance")
async def admin_balance_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await c.answer(); await state.set_state(AdminState.balance); await c.message.answer("💰 <b>Manual Balance</b>\n\nSend:\n<code>TelegramID | amount | add/deduct | note</code>")

@router.message(AdminState.balance)
async def admin_balance(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return
    parts=[x.strip() for x in (m.text or "").split("|",3)]
    if len(parts)!=4: return await m.answer("❌ Invalid format.")
    try: tg_id=int(parts[0]); amount=float(parts[1])
    except ValueError: return await m.answer("❌ Invalid ID or amount.")
    action=parts[2].lower(); note=parts[3]
    if amount<=0 or action not in {"add","deduct"}: return await m.answer("❌ Invalid amount/action.")
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users WHERE tg_id=%s FOR UPDATE",(tg_id,)); u=cur.fetchone()
                if not u: await state.clear(); return await m.answer("❌ User not found.")
                delta=amount if action=="add" else -amount
                cur.execute("UPDATE users SET balance=balance+%s,updated_at=NOW() WHERE id=%s AND (%s >= 0 OR balance >= %s)",(delta,u["id"],delta,amount))
                if cur.rowcount!=1:
                    await state.clear()
                    return await m.answer("❌ User balance is too low.")
                cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(u["id"],delta,f"admin_{action}",note))
    admin_log(m.from_user.id,f"balance_{action}",f"user {tg_id}, amount {amount}"); await state.clear(); await m.answer("✅ Balance updated.",reply_markup=admin_menu())

@router.callback_query(F.data=="admin:broadcast")
async def admin_broadcast_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await c.answer(); await state.set_state(AdminState.broadcast); await c.message.answer("📢 Send the broadcast message. HTML supported.")

@router.message(AdminState.broadcast)
async def admin_broadcast(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return
    text=m.text or ""; users=db_execute("SELECT tg_id FROM users WHERE blocked=0",fetch="all"); sent=failed=0
    for u in users:
        try: await m.bot.send_message(u["tg_id"],text); sent+=1; await asyncio.sleep(0.04)
        except Exception: failed+=1
    admin_log(m.from_user.id,"broadcast",f"sent={sent},failed={failed}"); await state.clear(); await m.answer(f"📢 Broadcast finished.\n✅ Sent: {sent}\n❌ Failed: {failed}",reply_markup=admin_menu())

@router.callback_query(F.data=="admin:logs")
async def admin_logs(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute("SELECT * FROM admin_logs ORDER BY id DESC LIMIT 20",fetch="all"); text="📝 No admin logs." if not rows else "📝 <b>Recent Admin Logs</b>\n\n"+"\n".join(f"#{r['id']} • {r['action']}\n{r['details']}\n🕒 {r['created_at']}\n" for r in rows); await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]))

@router.callback_query(F.data=="admin:settings")
async def settings(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    text=(f"⚙️ <b>{APP_VERSION} — Custom Control Center</b>\n\n"
          f"🏪 Shop: <code>{html.escape(shop_name())}</code>\n"
          f"💱 Currency: <code>{html.escape(currency())}</code>\n"
          f"🎧 Support: <code>{html.escape(setting('support',SUPPORT))}</code>\n"
          f"🔧 Maintenance: <b>{'ON' if maintenance_active() else 'OFF'}</b>\n\n"
          "Choose a section to customize without editing code.")
    kb=[[InlineKeyboardButton(text=setting("custom_shop", "🏪 Shop & Branding"),callback_data="custom:shop"),InlineKeyboardButton(text=setting("custom_ui", "🎨 Buttons & UI"),callback_data="custom:ui")],
        [InlineKeyboardButton(text=setting("custom_payments", "💳 Payments"),callback_data="custom:payments"),InlineKeyboardButton(text=setting("custom_money", "💰 Money & Rewards"),callback_data="custom:money")],
        [InlineKeyboardButton(text=setting("custom_orders", "📦 Orders"),callback_data="custom:orders"),InlineKeyboardButton(text=setting("custom_messages", "📝 Messages"),callback_data="custom:messages")],
        [InlineKeyboardButton(text=setting("custom_system", "🔧 System"),callback_data="custom:system")],
        [InlineKeyboardButton(text=setting("admin_toggle_maintenance", "🔧 Toggle Maintenance"),callback_data="set:maintenance")],
        [InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

CUSTOM_GROUPS={
 "shop":[("shop_name","🏪 Shop Name"),("currency","💱 Currency"),("support","🎧 Support"),("announcement","📢 Announcement"),("footer_text","🔻 Footer")],
 "ui":[("button_shop","🛍 Shop Button"),("button_search","🔍 Search Button"),("button_orders","📦 Orders Button"),("button_favorites","❤️ Favorites Button"),("button_profile","👤 Profile Button"),("button_deposit","💰 Deposit Button"),("button_rewards","⭐ Rewards Button"),("button_refer","🤝 Refer Button"),("button_support","🆘 Support Button"),("button_buy","🛒 Buy Button"),("button_purchase","🛒 Purchase Button"),("button_confirm","✅ Confirm Button"),("button_back","⬅️ Back Button"),("button_back_listings","⬅️ Back Listings Button"),("button_main_menu","🏠 Main Menu Button"),("button_favorite_add","⭐ Add Favorite Button"),("button_favorite_remove","💔 Remove Favorite Button"),("button_sold_out","⛔ Sold Out Button"), ("inline_rewards","⭐ Rewards Button"),("inline_referral","🤝 Referral Button"),("inline_shop","🛍️ Shop Button"),("inline_view_orders","📦 View My Orders Button"),("admin_block","🚫 Block Button"), ("inline_all_products","✨ All Products Button"),("inline_games_back","⬅️ Games Button"),("inline_first","⏮ First Button"),("inline_back","◀️ Back Button"),("inline_next","▶️ Next Button"),("inline_last","⏭ Last Button"),("inline_refresh","🔄 Refresh Button"),("inline_under5","💵 Under 5 Button"),("inline_categories","📂 Categories Button"),("inline_main_menu","🏠 Main Menu Button"),("inline_my_orders","⬅️ My Orders Button"),("inline_admin_back","⬅️ Admin Button"),("admin_add_product","➕ Add Product Button"),("admin_edit","✏️ Edit Button"),("admin_toggle_product","🔄 Enable/Disable Button"),("admin_add_codes","🎫 Add Codes Button"),("admin_delete","🗑 Delete Button"),("admin_order_complete","✅ Complete Button"),("admin_order_reject_refund","❌ Reject + Refund Button"),("admin_credit","✅ Credit Button"),("admin_reject","❌ Reject Button"),("admin_unblock","🔓 Unblock Button"),("admin_block","🚫 Block Button"),("admin_users_back","⬅️ Users Button"),("admin_products_back","⬅️ Products Button"),("admin_settings_back","⬅️ Settings Button"),("admin_back","⬅️ Admin Button"),("admin_database","📊 Database Button"),("admin_logs","📝 Logs Button"),("admin_dashboard","📊 Dashboard Button"),("admin_reports","📈 Reports Button"),("admin_premium","💎 Premium Analytics Button"),("admin_orders","🧾 Orders Button"),("admin_payments","💳 Payments Button"),("admin_users","👥 Users Button"),("admin_products","🛍 Products Button"),("admin_codes","🎫 Codes Button"),("admin_balance","💰 Balance Button"),("admin_broadcast","📢 Broadcast Button"),("admin_settings","⚙️ Settings Button"),("admin_toggle_maintenance","🔧 Toggle Maintenance Button"),("custom_shop","🏪 Shop & Branding Button"),("custom_ui","🎨 Buttons & UI Button"),("custom_payments","💳 Payments Button"),("custom_money","💰 Money & Rewards Button"),("custom_orders","📦 Orders Button"),("custom_messages","📝 Messages Button"),("custom_system","🔧 System Button"),("home_shop","🏠 Home Shop Button"),("home_orders","🏠 Home Orders Button"),("home_deposit","🏠 Home Deposit Button"),("home_profile","🏠 Home Profile Button"),("home_rewards","🏠 Home Rewards Button"),("home_refer","🏠 Home Referral Button"),("home_favorites","🏠 Home Favorites Button"),("home_support","🏠 Home Support Button"),("home_search","🏠 Home Search Button"),("admin_dashboard","📊 Admin Dashboard Button"),("admin_reports","📈 Admin Reports Button"),("admin_premium","💎 Admin Premium Button"),("admin_orders","🧾 Admin Orders Button"),("admin_payments","💳 Admin Payments Button"),("admin_users","👥 Admin Users Button"),("admin_products","🛍 Admin Products Button"),("admin_codes","🎫 Admin Codes Button"),("admin_balance","💰 Admin Balance Button"),("admin_broadcast","📢 Admin Broadcast Button"),("admin_settings","⚙️ Admin Settings Button"),("admin_database","📊 Admin Database Button"),("admin_logs","📝 Admin Logs Button")],
 "payments":[("payment_info","💳 Payment Instructions"),("payment_bkash_label","bKash Button Label"),("payment_nagad_label","Nagad Button Label"),("payment_method_prompt","Payment Method Prompt"),("deposit_min","⬇️ Minimum Deposit"),("deposit_max","⬆️ Maximum Deposit (0=unlimited)"),("payment_timeout_minutes","⏱ Payment Timeout")],
 "money":[("signup_bonus","🎁 Signup Bonus"),("referral_reward","🤝 Referral Reward")],
 "orders":[("order_timeout_minutes","⏱ Order Timeout")],
 "messages":[("welcome_message","👋 Welcome Message"),("maintenance_message","🔧 Maintenance Message"),("fallback_message","↩️ Fallback Message"),("search_prompt","🔎 Search Prompt"),("deposit_prompt","💰 Deposit Prompt"),("shop_title","🛍 Shop Title"),("category_title","📂 Category Title"),("buy_prompt","🛒 Buy Prompt")],
 "system":[("low_stock_threshold","⚠️ Low Stock Threshold")],
}

@router.callback_query(F.data.startswith("custom:"))
async def custom_group(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    group=c.data.split(":",1)[1]
    items=CUSTOM_GROUPS.get(group)
    if not items: return await c.answer("Unknown section",show_alert=True)
    kb=[]
    for i in range(0,len(items),2):
        kb.append([InlineKeyboardButton(text=label,callback_data=f"editset:{key}") for key,label in items[i:i+2]])
    kb.append([InlineKeyboardButton(text=setting("admin_settings_back", "⬅️ Settings"),callback_data="admin:settings")])
    await c.answer(); await c.message.edit_text(f"⚙️ <b>{group.title()} Customization</b>\n\nTap an item to edit.",reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("editset:"))
async def custom_edit_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    key=c.data.split(":",1)[1]
    allowed={k for items in CUSTOM_GROUPS.values() for k,_ in items}
    if key not in allowed: return await c.answer("Not editable",show_alert=True)
    await state.update_data(setting_key=key); await state.set_state(AdminState.settings); await c.answer()
    await c.message.answer(f"✏️ <b>Edit {html.escape(key)}</b>\n\nCurrent:\n<code>{html.escape(setting(key,'' )[:1500])}</code>\n\nSend the new value. /cancel to abort.")

@router.callback_query(F.data=="set:maintenance")
async def toggle_maintenance(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    val="0" if setting("maintenance","0")=="1" else "1"; set_setting("maintenance",val); admin_log(c.from_user.id,"maintenance",val); await c.answer("Updated"); await settings(c)

@router.message(AdminState.settings)
async def setting_save(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return await m.answer("Denied")
    if (m.text or "").strip().lower()=="/cancel": await state.clear(); return await m.answer("❌ Cancelled.",reply_markup=admin_menu())
    d=await state.get_data(); key=d.get("setting_key"); value=(m.text or "").strip()
    if not key: await state.clear(); return await m.answer("❌ Invalid setting session.",reply_markup=admin_menu())
    if not value: return await m.answer("❌ Value cannot be empty.")
    numeric={"low_stock_threshold","signup_bonus","referral_reward","deposit_min","deposit_max","order_timeout_minutes","payment_timeout_minutes"}
    if key in numeric:
        try:
            n=float(value); assert n>=0
            value=str(int(n)) if key in {"low_stock_threshold","order_timeout_minutes","payment_timeout_minutes"} else str(n).rstrip("0").rstrip(".")
        except Exception: return await m.answer("❌ Send a valid non-negative number.")
    if len(value)>4000: return await m.answer("❌ Value too long.")
    set_setting(key,value); await state.clear(); admin_log(m.from_user.id,"custom_setting_changed",f"{key}={value[:300]}")
    await m.answer(f"✅ <b>{html.escape(key)}</b> updated.",reply_markup=admin_menu())

@router.callback_query(F.data=="admin:dbinfo")
async def dbinfo(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    row=db_execute("SELECT current_database() AS db,current_schema() AS schema",fetch="one")
    await c.answer(); await c.message.edit_text(f"☁️ <b>Database</b>\n\nEngine: <b>PostgreSQL</b>\nDatabase: <code>{row['db']}</code>\nSchema: <code>{row['schema']}</code>\n\n✅ Data is stored outside the Render service filesystem.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]))

@router.message(Command("search"))
async def search_command(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."))
    q=(m.text or "").split(maxsplit=1)
    if len(q)>1 and q[1].strip():
        await run_product_search(m,q[1].strip())
        return
    await state.set_state(SearchState.query)
    await m.answer("🔎 <b>Smart Product Search</b>\n\nSend a product name, game, category, or keyword.\nExample: <code>Free Fire</code> or <code>Robux</code>\n\nSend /cancel to exit.")


@router.message(F.text=="🔎 Search")
@router.message(F.text=="🔍 Search")
async def search_button(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."))
    await state.set_state(SearchState.query)
    await m.answer("🔎 <b>Smart Product Search</b>\n\nSend a product name, game, category, or keyword.")


@router.message(SearchState.query)
async def search_input(m:Message,state:FSMContext):
    if (m.text or "").strip().lower()=="/cancel":
        await state.clear(); return await m.answer("❌ Search cancelled.")
    term=(m.text or "").strip()
    if len(term)<2 or len(term)>80:
        return await m.answer("❌ Search must be 2–80 characters.")
    await state.clear()
    await run_product_search(m,term)


async def run_product_search(m:Message,term:str):
    rows=db_execute("""SELECT p.*, CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock
        FROM products p
        LEFT JOIN (SELECT product_id, COUNT(*) AS available FROM product_codes WHERE status='available' GROUP BY product_id) pc ON pc.product_id=p.id
        WHERE p.active=1 AND (p.name ILIKE %s OR p.category ILIKE %s OR p.description ILIKE %s)
        ORDER BY CASE WHEN p.name ILIKE %s THEN 0 WHEN p.category ILIKE %s THEN 1 ELSE 2 END,p.id DESC LIMIT 30""",
        (f"%{term}%",f"%{term}%",f"%{term}%",f"%{term}%",f"%{term}%"),"all")
    if not rows:
        return await m.answer(f"🔎 No products found for <b>{html.escape(term)}</b>.\n\nTry another keyword.")
    kb=[[InlineKeyboardButton(text=f"{'🟢' if int(p['effective_stock'] or 0)>0 else '🔴'} {p['name']} • {float(p['price']):g} {currency()}",callback_data=f"product:{p['id']}")] for p in rows]
    await m.answer(f"🔎 <b>Search Results</b>\n\nFound <b>{len(rows)}</b> product(s) for <code>{html.escape(term)}</code>.",reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.message(Command("export_orders"))
async def export_orders(m:Message):
    if not is_admin(m.from_user.id): return await m.answer("Denied")
    path=Path("orders_export.csv"); rows=db_execute("SELECT o.id,u.tg_id,u.username,p.name,o.game_uid,o.total,o.status,o.created_at,o.updated_at FROM orders o JOIN users u ON u.id=o.user_id JOIN products p ON p.id=o.product_id ORDER BY o.id DESC",fetch="all")
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["order_id","tg_id","username","product","game_uid","total","status","created_at","updated_at"])
        keys = ["id","tg_id","username","name","game_uid","total","status","created_at","updated_at"]
        for r in rows: w.writerow([r[k] for k in keys])
    await m.answer_document(FSInputFile(str(path)),caption="📄 Orders export")

@router.message(Command("backup"))
async def manual_backup(m:Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Denied")
    await m.answer("💾 Creating secure database backup…")
    path = None
    try:
        path = await asyncio.to_thread(create_database_backup)
        cleanup_old_backups()
        admin_log(m.from_user.id, "database_backup", path.name)
        await m.answer_document(FSInputFile(str(path)), caption=f"💾 Database backup • {APP_VERSION}")
    except Exception:
        await m.answer("❌ Backup failed. Check server logs.")
    finally:
        if path:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass


async def automatic_backup_loop(bot):
    if AUTO_DB_BACKUP_HOURS <= 0:
        return
    await asyncio.sleep(300)
    while True:
        path = None
        try:
            path = await asyncio.to_thread(create_database_backup)
            cleanup_old_backups()
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_document(admin_id, FSInputFile(str(path)), caption=f"💾 Automatic database backup • {APP_VERSION}")
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            if path:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
        await asyncio.sleep(AUTO_DB_BACKUP_HOURS * 3600)


@router.message(Command("lowstock"))
async def lowstock(m:Message):
    if not is_admin(m.from_user.id): return await m.answer("Denied")
    rows=db_execute("""
        SELECT p.name,
               CASE WHEN p.delivery_type='code'
                    THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available')
                    ELSE p.stock END AS effective_stock
        FROM products p
        WHERE p.active=1
          AND (CASE WHEN p.delivery_type='code'
                    THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available')
                    ELSE p.stock END) <= %s
        ORDER BY effective_stock
    """,(low_stock_threshold(),),"all")
    if not rows: return await m.answer("✅ No low-stock products.")
    await m.answer("⚠️ <b>Low Stock Alert</b>\n\n"+"\n".join(f"• {html.escape(r['name'])}: <b>{r['effective_stock']}</b>" for r in rows))

@router.message(F.text)
async def dynamic_custom_button(m:Message,state:FSMContext):
    if await state.get_state(): return
    mapping={setting("button_shop","🛍️ Shop"): shop,setting("button_search","🔍 Search"): search_button,setting("button_orders","📦 My Orders"): my_orders,setting("button_favorites","❤️ Favorites"): favorites,setting("button_profile","👤 Profile"): profile,setting("button_deposit","💰 Deposit"): add_balance,setting("button_rewards","⭐ Rewards"): rewards,setting("button_refer","🤝 Refer & Earn"): refer,setting("button_support","🆘 Support"): support}
    fn=mapping.get((m.text or "").strip())
    if not fn: return
    if fn in {search_button,add_balance}: return await fn(m,state)
    return await fn(m)

@router.message()
async def fallback(m:Message):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id): return await m.answer("🚫 Your account is blocked.")
    if setting("maintenance","0")=="1" and not is_admin(m.from_user.id): return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."))
    await m.answer(custom_text("fallback_message", "Use the menu below or /shop to continue."))


USER_BOT_COMMANDS = [
    BotCommand(command="start", description="Launch the bot and open the main menu"),
    BotCommand(command="shop", description="Browse available products"),
    BotCommand(command="listings", description="Browse products (shortcut)"),
    BotCommand(command="search", description="Search products by name"),
    BotCommand(command="orders", description="View your recent orders"),
    BotCommand(command="favorites", description="View saved products"),
    BotCommand(command="rewards", description="View loyalty rewards"),
    BotCommand(command="refer", description="Refer friends and earn"),
    BotCommand(command="profile", description="View your profile"),
    BotCommand(command="balance", description="Add balance to your wallet"),
    BotCommand(command="deposit", description="Add balance (shortcut)"),
    BotCommand(command="support", description="Contact support"),
    BotCommand(command="help", description="Contact support (shortcut)"),
    BotCommand(command="version", description="Show bot version and features"),
]

ADMIN_BOT_COMMANDS = USER_BOT_COMMANDS + [
    BotCommand(command="admin", description="Open admin control center"),
    BotCommand(command="backup", description="Create a database backup"),
    BotCommand(command="lowstock", description="Check low-stock products"),
    BotCommand(command="export_orders", description="Export orders as CSV"),
    BotCommand(command="announcement", description="Set premium home announcement"),
]


async def setup_bot_commands(bot: Bot):
    # Default command menu: every customer/buyer gets these commands.
    await bot.set_my_commands(USER_BOT_COMMANDS, scope=BotCommandScopeDefault())
    # Admins get the same customer menu plus admin-only shortcuts.
    for admin_id in ADMIN_IDS:
        try:
            await bot.set_my_commands(
                ADMIN_BOT_COMMANDS,
                scope=BotCommandScopeChat(chat_id=admin_id)
            )
        except Exception:
            pass


async def main():
    database_integrity_check()
    reconcile_all_code_stock()
    start_health_server()
    bot=Bot(TOKEN,default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await setup_bot_commands(bot)
    dp=Dispatcher(); dp.include_router(router)

    async def low_stock_loop():
        while True:
            try:
                await notify_low_stock(bot)
            except Exception:
                pass
            await asyncio.sleep(600)

    asyncio.create_task(low_stock_loop())
    asyncio.create_task(automatic_backup_loop(bot))
    if ADMIN_WEB_TOKEN:
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"🌐 <b>{APP_VERSION} Web Admin enabled</b>\n"
                    "Open: <code>/admin?token=YOUR_ADMIN_WEB_TOKEN</code> on your Render URL.\n"
                    "💾 Use /backup for a manual database backup."
                )
            except Exception:
                pass

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__=="__main__":
    asyncio.run(main())
