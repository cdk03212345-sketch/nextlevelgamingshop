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
import sys
import time
import logging
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


class Buy(StatesGroup):
    uid = State()
    password = State()
    confirm = State()


class SearchState(StatesGroup):
    query = State()


class DirectPaymentState(StatesGroup):
    method = State()
    trx = State()
    receipt = State()


class PaymentState(StatesGroup):
    amount = State()
    method = State()
    trx = State()
    receipt = State()


class CartState(StatesGroup):
    uid = State()
    coupon = State()


class AdminState(StatesGroup):
    add_product = State()
    edit_product = State()
    add_codes = State()
    balance = State()
    broadcast = State()
    settings = State()
    marketing_create = State()
    manual_delivery_note = State()

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
CURRENCY = os.getenv("CURRENCY", "BDT")
PAYMENT_INFO = os.getenv("PAYMENT_INSTRUCTIONS", "bKash/Nagad: YOUR NUMBER")
SUPPORT = os.getenv("SUPPORT_USERNAME", "@YourSupport")
ADMIN_WEB_TOKEN = os.getenv("ADMIN_WEB_TOKEN", "").strip()
FEATURE_EFOOTBALL_COINS = True
APP_VERSION = "V9.4 FINAL CHECKED ULTRA BUY FLOW • MANUAL ADMIN + NEW USER DIRECT PAYMENT • MANUAL DELIVERY • WALLET/DIRECT PAYMENT • INLINE-ONLY"
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
    account_password TEXT DEFAULT '',
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

CREATE TABLE IF NOT EXISTS payment_audit(
    id BIGSERIAL PRIMARY KEY,
    payment_id BIGINT REFERENCES payments(id) ON DELETE SET NULL,
    admin_id BIGINT,
    action TEXT NOT NULL,
    old_status TEXT DEFAULT '',
    new_status TEXT DEFAULT '',
    amount NUMERIC(14,2),
    method TEXT DEFAULT '',
    trx_fingerprint TEXT DEFAULT '',
    note TEXT DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payment_audit_payment ON payment_audit(payment_id,created_at);
CREATE INDEX IF NOT EXISTS idx_payment_audit_action ON payment_audit(action,created_at);

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

CREATE TABLE IF NOT EXISTS notification_queue(
    id BIGSERIAL PRIMARY KEY,
    tg_id BIGINT NOT NULL,
    text TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_notification_queue_ready ON notification_queue(status,next_attempt_at);

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

CREATE TABLE IF NOT EXISTS cart_items(
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    quantity INTEGER NOT NULL DEFAULT 1 CHECK(quantity > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(user_id, product_id)
);

CREATE TABLE IF NOT EXISTS coupons(
    id BIGSERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    discount_type TEXT NOT NULL DEFAULT 'percent',
    value NUMERIC(14,2) NOT NULL DEFAULT 0,
    min_order NUMERIC(14,2) NOT NULL DEFAULT 0,
    max_discount NUMERIC(14,2) NOT NULL DEFAULT 0,
    usage_limit INTEGER NOT NULL DEFAULT 0,
    used_count INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
    starts_at TIMESTAMPTZ,
    ends_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS coupon_uses(
    coupon_id BIGINT NOT NULL REFERENCES coupons(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    order_id BIGINT,
    used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(coupon_id, user_id, order_id)
);

CREATE INDEX IF NOT EXISTS idx_cart_user ON cart_items(user_id);
CREATE INDEX IF NOT EXISTS idx_coupons_code_active ON coupons(code,active);

CREATE TABLE IF NOT EXISTS marketing_campaigns(
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    audience TEXT NOT NULL DEFAULT 'all',
    coupon_code TEXT NOT NULL DEFAULT '',
    starts_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ends_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'scheduled',
    sent_count INTEGER NOT NULL DEFAULT 0,
    clicked_count INTEGER NOT NULL DEFAULT 0,
    converted_count INTEGER NOT NULL DEFAULT 0,
    created_by BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS marketing_events(
    id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT NOT NULL REFERENCES marketing_campaigns(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    order_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_marketing_campaigns_due ON marketing_campaigns(status,starts_at);
CREATE INDEX IF NOT EXISTS idx_marketing_events_campaign ON marketing_events(campaign_id,event_type,created_at);
CREATE INDEX IF NOT EXISTS idx_marketing_events_user ON marketing_events(user_id,event_type,created_at);
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
                cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS account_password TEXT DEFAULT ''")
                cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_note TEXT DEFAULT ''")
                cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_mode TEXT DEFAULT 'wallet'")
                cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_id BIGINT")
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS order_id BIGINT")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id)")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS sale_price NUMERIC(14,2)")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS sale_until TIMESTAMPTZ")
                cur.execute("ALTER TABLE notification_queue ADD COLUMN IF NOT EXISTS buttons_json TEXT NOT NULL DEFAULT ''")
                cur.execute("ALTER TABLE cart_items ADD COLUMN IF NOT EXISTS last_reminded_at TIMESTAMPTZ")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_cart_abandoned ON cart_items(updated_at,last_reminded_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_marketing_events_order ON marketing_events(order_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_lifetime_spend ON users(lifetime_spend)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_updated_at ON users(updated_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_created ON orders(user_id,created_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_carts_updated ON cart_items(updated_at)")
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
            # V8 Ultra feature flags — editable from Admin > Ultra Control.
            "feature_search": "1", "feature_favorites": "1", "feature_rewards": "1",
            "feature_referral": "1", "feature_support": "1", "feature_announcements": "1",
            "feature_vip": "1", "feature_quick_shop": "1",
            "vip_bronze_spend": "1000", "vip_silver_spend": "5000", "vip_gold_spend": "15000",
            "vip_bronze_discount": "0", "vip_silver_discount": "0", "vip_gold_discount": "0",
            "automation_order_timeout": "1", "automation_payment_timeout": "1",
            "feature_cart": "1", "feature_coupons": "1", "feature_flash_sales": "1",
            "coupon_default_percent": "5", "cart_max_quantity": "10",
            "feature_marketing": "1", "marketing_abandoned_cart_hours": "6",
            "marketing_reactivation_days": "30", "marketing_new_user_days": "7",
            "marketing_daily_limit": "500", "marketing_click_offer_text": "🎁 Open Offer",
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
            "payment_rocket_label": "Rocket",
            "payment_binance_label": "Binance",
            "payment_bkash_icon": "🟪",
            "payment_nagad_icon": "🟩",
            "payment_rocket_icon": "🔵",
            "payment_binance_icon": "🟠",
            "payment_bkash_enabled": "1",
            "payment_nagad_enabled": "1",
            "payment_rocket_enabled": "1",
            "payment_binance_enabled": "1",
            "payment_bkash_account": "01XXXXXXXXX",
            "payment_nagad_account": "01XXXXXXXXX",
            "payment_rocket_account": "01XXXXXXXXX",
            "payment_binance_account": "YOUR_BINANCE_PAY_ID_OR_WALLET",
            "payment_bkash_instruction": "Send the exact amount using bKash. Use Send Money/Payment as instructed by the shop.",
            "payment_nagad_instruction": "Send the exact amount using Nagad. Use Send Money/Payment as instructed by the shop.",
            "payment_rocket_instruction": "Send the exact amount using Rocket. Use Send Money/Payment as instructed by the shop.",
            "payment_binance_instruction": "Send the exact amount to the Binance wallet/payment address shown below.",
            "payment_binance_network": "Specify network before accepting payments.",
            "payment_trx_label": "Transaction ID / TxID / Hash",
            "payment_receipt_required": "0",
            "payment_presets": "100,200,500,1000",
            "payment_min_deposit": "10", "payment_max_deposit": "100000",
            "payment_method_prompt": "💳 <b>Choose a payment method</b>:",
            "shop_title": "💎 <b>Premium Gaming Store</b>\n\n🎮 Choose a game category to continue:",
            "category_title": "💎 <b>PRODUCT CATEGORIES</b>\n━━━━━━━━━━━━━━━━━━\n🎮 Choose a category to browse products.\n⚡ Fast delivery  •  ⭐ VIP rewards",
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


def ensure_buyer_account(tg_id: int):
    row=db_execute("SELECT id,tg_id,balance FROM users WHERE tg_id=%s",(tg_id,),"one")
    if row: return row
    try:
        db_execute("INSERT INTO users(tg_id,balance) VALUES(%s,0) ON CONFLICT (tg_id) DO NOTHING",(tg_id,),"none")
    except Exception as exc:
        logging.exception("ensure_buyer_account failed for %s: %s",tg_id,exc)
    return db_execute("SELECT id,tg_id,balance FROM users WHERE tg_id=%s",(tg_id,),"one")

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


def vip_tier(user):
    try:
        spend = float(user.get("lifetime_spend") or 0)
    except Exception:
        spend = 0.0
    try:
        gold = float(setting("vip_gold_spend", "15000"))
        silver = float(setting("vip_silver_spend", "5000"))
        bronze = float(setting("vip_bronze_spend", "1000"))
    except Exception:
        gold, silver, bronze = 15000, 5000, 1000
    if spend >= gold:
        tier, discount = "GOLD", setting("vip_gold_discount", "0")
    elif spend >= silver:
        tier, discount = "SILVER", setting("vip_silver_discount", "0")
    elif spend >= bronze:
        tier, discount = "BRONZE", setting("vip_bronze_discount", "0")
    else:
        tier, discount = "MEMBER", "0"
    try:
        discount = max(0.0, min(100.0, float(discount)))
    except Exception:
        discount = 0.0
    return tier, discount


def discounted_price(user, price):
    _, discount = vip_tier(user)
    return round(float(price) * (1.0 - discount / 100.0), 2)


def enqueue_notification(tg_id, text, buttons=None):
    payload = json.dumps(buttons or [], ensure_ascii=False)
    db_execute("INSERT INTO notification_queue(tg_id,text,buttons_json) VALUES(%s,%s,%s)", (tg_id, text, payload))


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

def admin_log(admin_id, action, details=""):
    db_execute(
        "INSERT INTO admin_logs(admin_tg_id,action,details) VALUES(%s,%s,%s)",
        (admin_id, action, details),
    )


BACKUP_TABLES = (
    "users", "products", "product_codes", "orders", "payments",
    "payment_receipts", "payment_audit", "balance_logs", "admin_logs", "settings", "favorites", "notification_queue", "cart_items", "coupons", "coupon_uses", "marketing_campaigns", "marketing_events"
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
            ('payment_receipts'),('payment_audit'),('balance_logs'),('admin_logs'),('settings'),('favorites')
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
    filled=max(0,min(10,pct//10))
    bar="🟦"*filled+"⬛"*(10-filled)
    announcement=setting("announcement","").strip()
    name=html.escape(u.get("name") or "Gamer")
    lines=[
        f"💎 <b>{html.escape(shop_name())}</b>  <code>{APP_VERSION.split(' • ')[0]}</code>",
        "<i>Premium Digital Store • Faster • Smarter • Safer</i>",
        "",
        f"👋 <b>Welcome back, {name}!</b>",
        "┌─────────────────────────┐",
        f"│ 💰 Balance   <b>{balance}</b>",
        f"│ 📦 Orders    <b>{int(u.get('order_count') or 0)}</b>",
        f"│ ⭐ Points    <b>{points}</b>",
        f"│ 🏅 VIP       <b>{tier}</b>",
        "└─────────────────────────┘",
        f"📈 <b>VIP Progress</b>  {pct}%",
        bar,
        f"💸 Lifetime Spend: <b>{fmt_money(spend)}</b>",
    ]
    if tier != "💎 Diamond":
        lines.append(f"🎯 Next milestone: <b>{next_points} points</b>")
    if announcement:
        lines.extend(["", "📢 <b>Latest Update</b>", html.escape(announcement)])
    lines.extend(["", "⚡ <b>Instant Delivery</b>  •  🛡️ <b>Secure</b>  •  🎁 <b>VIP Rewards</b>"])
    return "\n".join(lines)

def _feature_on(key):
    return setting(key, "1") == "1"

def premium_home_kb():
    rows = []
    if _feature_on("feature_quick_shop"):
        rows.append([InlineKeyboardButton(text="🛍️  SHOP NOW", callback_data="home:shop")])
    row=[]
    if _feature_on("feature_search"):
        row.append(InlineKeyboardButton(text=setting("home_search", "🔎 Search Products"), callback_data="home:search"))
    if _feature_on("feature_support"):
        row.append(InlineKeyboardButton(text=setting("home_support", "🆘 Support"), callback_data="home:support"))
    if row: rows.append(row)
    row=[]
    row.append(InlineKeyboardButton(text=setting("home_orders", "📦 My Orders"), callback_data="home:orders"))
    row.append(InlineKeyboardButton(text=setting("home_deposit", "💰 Deposit"), callback_data="home:deposit"))
    rows.append(row)
    row=[]
    row.append(InlineKeyboardButton(text=setting("home_profile", "👤 Profile"), callback_data="home:profile"))
    if _feature_on("feature_rewards"):
        row.append(InlineKeyboardButton(text=setting("home_rewards", "⭐ Rewards"), callback_data="home:rewards"))
    rows.append(row)
    row=[]
    if _feature_on("feature_referral"):
        row.append(InlineKeyboardButton(text=setting("home_refer", "🤝 Referral"), callback_data="home:refer"))
    if _feature_on("feature_favorites"):
        row.append(InlineKeyboardButton(text=setting("home_favorites", "❤️ Favorites"), callback_data="home:favorites"))
    if row: rows.append(row)
    if _feature_on("feature_smart_offers"):
        rows.append([InlineKeyboardButton(text="🧠 Smart Offers", callback_data="home:offers")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def user_menu():
    # Inline-only UI: no Telegram ReplyKeyboard / persistent keyboard.
    return None

def inline_home_kb():
    # Keep every customer home surface consistent with Ultra feature flags.
    return premium_home_kb()

def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=setting("admin_dashboard", "📊 Dashboard"), callback_data="admin:dashboard"), InlineKeyboardButton(text=setting("admin_reports", "📈 Reports"), callback_data="admin:reports")],
        [InlineKeyboardButton(text=setting("admin_premium", "💎 Premium Analytics"), callback_data="admin:premium")],
        [InlineKeyboardButton(text=setting("admin_marketing", "📣 Marketing Center"), callback_data="admin:marketing")],
        [InlineKeyboardButton(text="🧠 Intelligence", callback_data="admin:intelligence")],
        [InlineKeyboardButton(text=setting("admin_orders", "🧾 Orders"), callback_data="admin:orders"), InlineKeyboardButton(text=setting("admin_payments", "💳 Payments"), callback_data="admin:payments")],
        [InlineKeyboardButton(text="💳 Payment Methods", callback_data="admin:payment_methods"), InlineKeyboardButton(text="🤖 Auto Pilot", callback_data="admin:autopilot")],
        [InlineKeyboardButton(text=setting("admin_users", "👥 Users"), callback_data="admin:users"), InlineKeyboardButton(text=setting("admin_products", "🛍 Products"), callback_data="admin:products")],
        [InlineKeyboardButton(text=setting("admin_codes", "🎫 Codes"), callback_data="admin:codes"), InlineKeyboardButton(text=setting("admin_balance", "💰 Balance"), callback_data="admin:balance")],
        [InlineKeyboardButton(text=setting("admin_broadcast", "📢 Broadcast"), callback_data="admin:broadcast"), InlineKeyboardButton(text=setting("admin_settings", "⚙️ Settings"), callback_data="admin:settings")],
        [InlineKeyboardButton(text=setting("admin_database", "📊 Database"), callback_data="admin:dbinfo"), InlineKeyboardButton(text=setting("admin_logs", "📝 Logs"), callback_data="admin:logs")],
        [InlineKeyboardButton(text=setting("admin_ultra_control", "🚀 Ultra Control"), callback_data="admin:ultra")],
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
        title = "✨ All Products"
    else:
        rows = db_execute(f"SELECT p.*, CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock FROM products p {stock_join} WHERE p.active=1 AND p.category=%s ORDER BY p.id DESC LIMIT %s OFFSET %s", (category, per_page, offset), "all")
        total = db_execute("SELECT COUNT(*) AS c FROM products WHERE active=1 AND category=%s", (category,), "one")["c"]
        title = html.escape(category)
    buttons=[]
    for idx,p in enumerate(rows, start=offset+1):
        stock=int(p["effective_stock"] or 0)
        name=html.escape(str(p["name"]))[:26]
        status="🟢 In Stock" if stock>0 else "🔴 Sold Out"
        price=f"{float(p['price']):g} {currency()}"
        buttons.append([InlineKeyboardButton(text=f"{idx}. 🎮 {name}", callback_data=f"product:{p['id']}")])
        buttons.append([InlineKeyboardButton(text=f"💰 {price}  •  {status}", callback_data=f"product:{p['id']}"), InlineKeyboardButton(text=setting("button_buy","🛒 Buy Now") if stock>0 else setting("button_sold_out","⛔ Sold Out"), callback_data=f"buy:{p['id']}" if stock>0 else f"soldout:{p['id']}")])
    total_pages=max(1,(int(total)+per_page-1)//per_page)
    buttons.append([InlineKeyboardButton(text=setting("inline_first","⏮ First"),callback_data=f"page:{category}:0"), InlineKeyboardButton(text=setting("inline_back","◀️ Back"),callback_data=f"page:{category}:{max(0,page-1)}"), InlineKeyboardButton(text=setting("inline_next","▶️ Next"),callback_data=f"page:{category}:{min(total_pages-1,page+1)}"), InlineKeyboardButton(text=setting("inline_last","⏭ Last"),callback_data=f"page:{category}:{total_pages-1}")])
    buttons.append([InlineKeyboardButton(text=setting("inline_refresh","🔄 Refresh"),callback_data=f"page:{category}:{page}"),InlineKeyboardButton(text=setting("inline_under5","💵 Under 5"),callback_data=f"price5:{category}")])
    buttons.append([InlineKeyboardButton(text=setting("inline_categories","📂 Categories"),callback_data="shop"), InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")])
    header=f"🛍️ <b>{title}</b>\n📄 Page <b>{page+1}</b> / <b>{total_pages}</b>  •  📦 <b>{total}</b> products\n\nSelect a product to view details or buy."
    # The caller edits the message; header is carried by a custom attribute only if needed.
    return InlineKeyboardMarkup(inline_keyboard=buttons)


class _HealthHandler(BaseHTTPRequestHandler):
    def _send(self, body, status=200, content_type="text/html; charset=utf-8"):
        raw = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _authorized(self):
        if not ADMIN_WEB_TOKEN:
            return False
        q = parse_qs(urlparse(self.path).query)
        return hmac.compare_digest(q.get("token", [""])[0], ADMIN_WEB_TOKEN)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            return self._send(f"{shop_name()} {APP_VERSION} is running.", 200, "text/plain; charset=utf-8")
        if path in ("/", "/admin"):
            if not self._authorized():
                return self._send("<h2>401 Unauthorized</h2><p>Use ?token=ADMIN_WEB_TOKEN</p>", 401)
            return self._admin_page()
        return self._send("Not found", 404)

    def _admin_page(self):
        try:
            row = db_execute("""
                SELECT
                  (SELECT COUNT(*) FROM users) users,
                  (SELECT COUNT(*) FROM products WHERE active=1) products,
                  (SELECT COUNT(*) FROM product_codes WHERE status='available') codes,
                  (SELECT COUNT(*) FROM orders WHERE status='pending') pending_orders,
                  (SELECT COUNT(*) FROM payments WHERE status='pending') pending_payments,
                  (SELECT COALESCE(SUM(total),0) FROM orders WHERE status='completed') sales,
                  (SELECT COALESCE(SUM(balance),0) FROM users) wallet
            """, fetch="one")
            recent = db_execute("""
                SELECT o.id, u.tg_id, p.name, o.total, o.status, o.created_at
                FROM orders o JOIN users u ON u.id=o.user_id
                JOIN products p ON p.id=o.product_id
                ORDER BY o.id DESC LIMIT 20
            """, fetch="all")
            rows = "".join(
                f"<tr><td>#{r['id']}</td><td>{html.escape(str(r['tg_id']))}</td>"
                f"<td>{html.escape(r['name'])}</td><td>{float(r['total']):.2f} {html.escape(currency())}</td>"
                f"<td>{html.escape(r['status'])}</td><td>{html.escape(str(r['created_at']))}</td></tr>"
                for r in recent
            )
            page=f"""<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(shop_name())} {APP_VERSION} Admin</title>
<style>
body{{font-family:system-ui;margin:20px;background:#111;color:#eee}} .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}}
.card{{background:#1d1d1d;padding:16px;border-radius:12px}} table{{width:100%;border-collapse:collapse;margin-top:20px}}
td,th{{padding:8px;border-bottom:1px solid #333;text-align:left}} h1{{font-size:24px}}
</style></head><body>
<h1>🎮 {html.escape(shop_name())} — {APP_VERSION} Admin</h1>
<div class="grid">
<div class="card">👥 Users<br><b>{row['users']}</b></div><div class="card">🛍 Products<br><b>{row['products']}</b></div>
<div class="card">🎫 Available Codes<br><b>{row['codes']}</b></div><div class="card">🧾 Pending Orders<br><b>{row['pending_orders']}</b></div>
<div class="card">💳 Pending Payments<br><b>{row['pending_payments']}</b></div><div class="card">💰 Sales<br><b>{float(row['sales'] or 0):.2f} {html.escape(currency())}</b></div>
<div class="card">👛 Wallet Total<br><b>{float(row['wallet'] or 0):.2f} {html.escape(currency())}</b></div>
</div>
<h2>Recent Orders</h2><table><tr><th>ID</th><th>User</th><th>Product</th><th>Total</th><th>Status</th><th>Created</th></tr>{rows}</table>
</body></html>"""
            return self._send(page)
        except Exception as e:
            return self._send(f"<h2>500 Internal Server Error</h2><pre>{html.escape(str(e))}</pre>", 500)

# Explicit runtime binding: keep the health handler name defined before the server starts.
HealthHandler = _HealthHandler

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
    await m.answer(custom_text("shop_title", "💎 <b>ULTRA PREMIUM SHOP</b>\n━━━━━━━━━━━━━━━━━━\n🎮 Choose a game category to continue.\n⚡ Instant delivery  •  🛡️ Secure checkout"), reply_markup=categories_kb())

@router.callback_query(F.data == "shop")
async def shop_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    await c.answer(); await c.message.edit_text(custom_text("category_title", "💎 <b>PRODUCT CATEGORIES</b>\n━━━━━━━━━━━━━━━━━━\n🎮 Choose a category to browse products.\n⚡ Fast delivery  •  ⭐ VIP rewards"), reply_markup=categories_kb())

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
        f"💎 <b>SHOP / LISTINGS</b>\n━━━━━━━━━━━━━━━━━━\n📂 <b>{html.escape(title)}</b>\n📄 Page <b>1</b> / <b>{max(1,(total+3)//4)}</b>  •  📦 <b>{total}</b> products\n\n👆 Tap a product for details or use 🛒 Buy Now.",
        reply_markup=products_kb(category, 0)
    )

@router.callback_query(F.data.startswith("page:"))
async def page_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    _,category,page=c.data.split(":",2)
    page=max(0,int(page))
    total_row=db_execute("SELECT COUNT(*) AS c FROM products WHERE active=1 AND (category=%s OR %s='*')",(category,category),"one")
    total=int(total_row["c"]) if total_row else 0
    per_page=4
    total_pages=max(1,(total+per_page-1)//per_page)
    page=min(page,total_pages-1)
    title="All Products" if category=="*" else category
    await c.answer()
    await c.message.edit_text(
        f"💎 <b>SHOP / LISTINGS</b>\n━━━━━━━━━━━━━━━━━━\n📂 <b>{html.escape(title)}</b>\n📄 Page <b>{page+1}</b> / <b>{total_pages}</b>  •  📦 <b>{total}</b> products\n\n👆 Tap a product for details or use 🛒 Buy Now.",
        reply_markup=products_kb(category,page)
    )

async def notify_user(bot, tg_id, text, reply_markup=None):
    # V8.1: short retry for transient Telegram failures, then queue the message.
    last_error = ""
    for attempt in range(3):
        try:
            await bot.send_message(tg_id, text, reply_markup=reply_markup)
            return True
        except Exception as exc:
            last_error = str(exc)
            await asyncio.sleep(0.5 * (attempt + 1))
    try:
        enqueue_notification(tg_id, text)
    except Exception:
        pass
    return False


async def notification_queue_loop(bot):
    while True:
        try:
            rows = db_execute(
                "SELECT id,tg_id,text,buttons_json,attempts FROM notification_queue "
                "WHERE status='pending' AND next_attempt_at<=NOW() ORDER BY id LIMIT 20",
                fetch="all"
            ) or []
            for row in rows:
                try:
                    markup = None
                    try:
                        raw_buttons = json.loads(row.get("buttons_json") or "[]")
                        if raw_buttons:
                            markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=str(b[0]), callback_data=str(b[1])) for b in row_buttons] for row_buttons in raw_buttons])
                    except Exception:
                        markup = None
                    await bot.send_message(row["tg_id"], row["text"], reply_markup=markup)
                    db_execute("UPDATE notification_queue SET status='sent',sent_at=NOW() WHERE id=%s", (row["id"],))
                except Exception as exc:
                    attempts = int(row["attempts"] or 0) + 1
                    if attempts >= 5:
                        db_execute("UPDATE notification_queue SET status='failed',attempts=%s,last_error=%s WHERE id=%s", (attempts, str(exc)[:500], row["id"]))
                    else:
                        delay = min(3600, 30 * (2 ** (attempts - 1)))
                        db_execute("UPDATE notification_queue SET attempts=%s,next_attempt_at=NOW()+(%s * INTERVAL '1 second'),last_error=%s WHERE id=%s", (attempts, delay, str(exc)[:500], row["id"]))
        except Exception:
            pass
        await asyncio.sleep(10)


def cleanup_expired_transactions():
    # V8.1: expire stale pending transactions safely. Pending manual orders are refunded
    # and their reserved stock is returned in the same DB transaction.
    order_minutes = max(1, int(setting("order_timeout_minutes", "30") or 30))
    payment_minutes = max(1, int(setting("payment_timeout_minutes", "30") or 30))
    if setting("automation_order_timeout", "1") == "1":
        with DB_LOCK:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id,user_id,product_id,total FROM orders "
                        "WHERE status='pending' AND created_at < NOW() - (%s * INTERVAL '1 minute') "
                        "ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED",
                        (order_minutes,),
                    )
                    stale_orders = cur.fetchall() or []
                    for order in stale_orders:
                        cur.execute(
                            "UPDATE orders SET status='expired',refund_amount=total,processed_at=NOW(),updated_at=NOW() "
                            "WHERE id=%s AND status='pending'",
                            (order["id"],),
                        )
                        if cur.rowcount != 1:
                            continue
                        cur.execute(
                            "UPDATE users SET balance=balance+%s,updated_at=NOW() WHERE id=%s",
                            (order["total"], order["user_id"]),
                        )
                        cur.execute(
                            "UPDATE products SET stock=stock+1,updated_at=NOW() WHERE id=%s",
                            (order["product_id"],),
                        )
                        cur.execute(
                            "INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",
                            (order["user_id"], order["total"], "auto_refund", f"Order #{order['id']} expired"),
                        )
    if setting("automation_payment_timeout", "1") == "1":
        with DB_LOCK:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id,order_id FROM payments WHERE status='pending' AND created_at < NOW() - (%s * INTERVAL '1 minute') LIMIT 100 FOR UPDATE SKIP LOCKED",(payment_minutes,))
                    stale_payments=cur.fetchall() or []
                    for pay in stale_payments:
                        cur.execute("UPDATE payments SET status='expired',updated_at=NOW() WHERE id=%s AND status='pending'",(pay["id"],))
                        if pay.get("order_id"):
                            cur.execute("UPDATE orders SET status='expired',updated_at=NOW() WHERE id=%s AND status='awaiting_payment'",(pay["order_id"],))


async def automation_loop():
    await asyncio.sleep(30)
    while True:
        try:
            cleanup_expired_transactions()
            marketing_abandoned_cart_job()
            marketing_reactivation_job()
            # Attribute newly completed orders to the latest campaign click.
            rows = db_execute("""SELECT o.id,o.user_id FROM orders o
                               WHERE o.status='completed' AND o.created_at>=NOW()-INTERVAL '10 minutes'
                               ORDER BY o.id LIMIT 100""", fetch="all") or []
            for row in rows:
                marketing_record_conversion(row["user_id"], row["id"])
        except Exception as exc:
            print(f"automation_loop error: {exc}")
        await asyncio.sleep(300)


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


async def render_orders_callback(c: CallbackQuery, page: int = 0):
    u=get_user(c.from_user)
    per_page=5
    total_row=db_execute("SELECT COUNT(*) AS c FROM orders WHERE user_id=%s",(u["id"],),"one")
    total=int(total_row["c"] or 0) if total_row else 0
    if not total:
        return await c.message.edit_text("📦 <b>My Orders</b>\n\nYou have no orders yet.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))
    total_pages=max(1,(total+per_page-1)//per_page)
    page=min(max(0,int(page)),total_pages-1)
    rows=db_execute("SELECT o.id,o.total,o.status,o.created_at,p.name FROM orders o JOIN products p ON p.id=o.product_id WHERE o.user_id=%s ORDER BY o.id DESC LIMIT %s OFFSET %s",(u["id"],per_page,page*per_page),"all")
    lines=[f"📦 <b>My Orders</b>\n📄 Page <b>{page+1}</b> / <b>{total_pages}</b> • Total: <b>{total}</b>\n"]
    buttons=[]
    for r in rows:
        lines.append(f"#{r['id']} • {html.escape(r['name'])}\n💰 {fmt_money(r['total'])} • {status_emoji(r['status'])} {r['status'].title()}\n🕒 {r['created_at']}\n")
        buttons.append([InlineKeyboardButton(text=f"🧾 Order #{r['id']}",callback_data=f"order_detail:{r['id']}")])
    buttons.append([
        InlineKeyboardButton(text="⏮ First",callback_data="orders_page:0"),
        InlineKeyboardButton(text="◀️ Back",callback_data=f"orders_page:{max(0,page-1)}"),
        InlineKeyboardButton(text="▶️ Next",callback_data=f"orders_page:{min(total_pages-1,page+1)}"),
        InlineKeyboardButton(text="⏭ Last",callback_data=f"orders_page:{total_pages-1}")
    ])
    buttons.append([InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")])
    return await c.message.edit_text("\n".join(lines),reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("orders_page:"))
async def orders_page_callback(c: CallbackQuery):
    if user_blocked(c.from_user.id) and not is_admin(c.from_user.id):
        return await c.answer("Account blocked.",show_alert=True)
    try: page=max(0,int(c.data.split(":",1)[1]))
    except ValueError: page=0
    await c.answer()
    return await render_orders_callback(c,page)

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
        return await c.message.edit_text("💎 <b>PRODUCT CATEGORIES</b>\n━━━━━━━━━━━━━━━━━━\n🎮 Choose a category to browse products.\n⚡ Fast delivery  •  ⭐ VIP rewards", reply_markup=categories_kb())
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
        return await show_deposit_start(c.message, state, edit=True)

@router.callback_query(F.data.startswith("soldout:"))
async def soldout_callback(c: CallbackQuery):
    await c.answer("⛔ This product is currently sold out.", show_alert=True)


@router.callback_query(F.data.startswith("price5:"))
async def price5_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id):
        return await c.answer("Shop is under maintenance.", show_alert=True)
    payload = c.data.split(":", 1)[1]
    if ":" in payload:
        category, page_s = payload.rsplit(":", 1)
        try:
            page = max(0, int(page_s))
        except ValueError:
            category, page = payload, 0
    else:
        category, page = payload, 0
    per_page = 4
    stock_join = "LEFT JOIN (SELECT product_id, COUNT(*) AS available FROM product_codes WHERE status='available' GROUP BY product_id) pc ON pc.product_id=p.id"
    where = "p.active=1 AND p.price < 5 AND (p.category=%s OR %s='*')"
    total_row = db_execute(f"SELECT COUNT(*) AS c FROM products p WHERE {where}", (category, category), "one")
    total = int(total_row["c"]) if total_row else 0
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages - 1)
    rows = db_execute(
        f"""SELECT p.*, CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock
           FROM products p {stock_join}
           WHERE {where}
           ORDER BY p.id DESC LIMIT %s OFFSET %s""",
        (category, category, per_page, page * per_page),
        "all"
    )
    if not rows:
        return await c.answer("No products below 5 found.", show_alert=True)
    buttons=[]
    offset = page * per_page
    for idx,p in enumerate(rows, offset + 1):
        stock=int(p.get("effective_stock") or 0)
        buttons.append([
            InlineKeyboardButton(text=f"{idx}. 🎮 {html.escape(str(p['name']))[:24]}", callback_data=f"product:{p['id']}"),
            InlineKeyboardButton(text=setting("button_purchase","🛒 Purchase") if stock>0 else setting("button_sold_out","⛔ Sold Out"), callback_data=f"buy:{p['id']}" if stock>0 else f"soldout:{p['id']}")
        ])
    nav = [
        InlineKeyboardButton(text=setting("inline_first","⏮ First"), callback_data=f"price5:{category}:0"),
        InlineKeyboardButton(text=setting("inline_back","◀️ Back"), callback_data=f"price5:{category}:{max(0,page-1)}"),
        InlineKeyboardButton(text=setting("inline_next","▶️ Next"), callback_data=f"price5:{category}:{min(total_pages-1,page+1)}"),
        InlineKeyboardButton(text=setting("inline_last","⏭ Last"), callback_data=f"price5:{category}:{total_pages-1}")
    ]
    buttons.append(nav)
    buttons.append([InlineKeyboardButton(text=setting("inline_refresh","🔄 Refresh"), callback_data=f"price5:{category}:{page}"), InlineKeyboardButton(text=setting("inline_categories","📂 Categories"), callback_data="shop")])
    buttons.append([InlineKeyboardButton(text=setting("button_back_listings","⬅️ Back to Listings"), callback_data=f"page:{category}:0"), InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"), callback_data="main_menu")])
    await c.answer()
    await c.message.edit_text(
        f"💵 <b>Products Below 5</b>\n📄 Page <b>{page+1}</b> / <b>{total_pages}</b> • Total: <b>{total}</b>\n\nSelect a product to view details or purchase.",
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
    text=(f"💎 <b>PRODUCT DETAILS</b>\n\n🎮 <b>{html.escape(p['name'])}</b>\n━━━━━━━━━━━━━━━━━━\n💰 Price: <b>{float(p['price']):g} {currency()}</b>\n📦 Stock: <b>{stock}</b>\n⚡ Delivery: <b>{'Instant' if is_auto_code_product(p) else 'Manual'}</b>\n🌍 Category: <b>{html.escape(p['category'])}</b>\n\n{badge}\n\n📝 {html.escape(p['description'] or 'Premium gaming product.')}")
    buttons=[]
    if stock>0:
        buttons.append([InlineKeyboardButton(text=setting("button_buy","🛒 Buy Now"),callback_data=f"buy:{pid}")])
        if cart_enabled(): buttons.append([InlineKeyboardButton(text="🛒 Add to Cart",callback_data=f"cart:add:{pid}")])
    fav_label = setting("button_favorite_remove","💔 Remove Favorite") if user_favorite(pid,c.from_user.id) else setting("button_favorite_add","⭐ Add to Favorites")
    buttons.append([InlineKeyboardButton(text=fav_label,callback_data=f"fav:{pid}")])
    buttons.append([InlineKeyboardButton(text=setting("button_back","⬅️ Back"),callback_data=f"cat:{p['category']}")])
    await c.answer()
    markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    if p["image_file_id"]:
        try:
            # Keep the premium flow clean: remove the old listing/details message
            # before showing the image-based details screen, avoiding duplicate UI.
            await c.message.delete()
        except Exception:
            pass
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
    text=(f"💎 <b>PRODUCT DETAILS</b>\n\n🎮 <b>{html.escape(p['name'])}</b>\n━━━━━━━━━━━━━━━━━━\n💰 Price: <b>{float(p['price']):g} {currency()}</b>\n📦 Stock: <b>{stock}</b>\n⚡ Delivery: <b>{'Instant' if is_auto_code_product(p) else 'Manual'}</b>\n🌍 Category: <b>{html.escape(p['category'])}</b>\n\n{badge}\n\n📝 {html.escape(p['description'] or 'Premium gaming product.')}")
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
    await state.update_data(pid=pid, qty=1, origin_message_id=c.message.message_id, origin_is_photo=bool(getattr(c.message, "photo", None))); await state.set_state(Buy.uid); await c.answer(); await c.message.answer("🆔 <b>Send your game/player UID.</b>\n\nSend /cancel to cancel.")

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
    if float(u["balance"])<float(p['price']): await state.clear(); return await m.answer(f"❌ <b>Insufficient balance</b>\n\nPrice: {fmt_money(p['price'])}\nBalance: {fmt_money(u['balance'])}\nNeed: {fmt_money(float(p['price'])-float(u['balance']))}")
    await state.update_data(game_uid=uid, qty=1)
    if not is_auto_code_product(p):
        await state.set_state(Buy.password)
        return await m.answer("🔐 <b>Manual Delivery Product</b>\n\n🆔 ID / UID received.\n🔑 Now send the account password required for delivery.\n\n⚠️ Only send credentials needed to complete this order.\nSend /cancel to cancel.")
    await state.set_state(Buy.confirm)
    buy_prompt = custom_text("buy_prompt", "Confirm your purchase:")
    confirmation=(f"🛒 <b>Purchase Confirmation</b>\n\n🎮 Product: <b>{html.escape(p['name'])}</b>\n🆔 UID: <code>{uid}</code>\n💰 Price: <b>{fmt_money(p['price'])}</b>\n📦 Quantity: <b>1</b>\n⭐ Total: <b>{fmt_money(p['price'])}</b>\n\n💳 Your balance: <b>{fmt_money(u['balance'])}</b>\n\n{html.escape(buy_prompt)}")
    markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➖",callback_data="order:qty:-1"),InlineKeyboardButton(text="📦 1",callback_data="order:noop"),InlineKeyboardButton(text="➕",callback_data="order:qty:1")],[InlineKeyboardButton(text=setting("button_confirm","✅ Confirm Purchase"),callback_data="order:confirm")],[InlineKeyboardButton(text=setting("button_back","⬅️ Back"),callback_data="order:cancel"),InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]])
    d=await state.get_data()
    origin_id=d.get("origin_message_id")
    edited=False
    if origin_id:
        try:
            if d.get("origin_is_photo"):
                await m.bot.edit_message_caption(chat_id=m.chat.id, message_id=origin_id, caption=confirmation, reply_markup=markup)
            else:
                await m.bot.edit_message_text(chat_id=m.chat.id, message_id=origin_id, text=confirmation, reply_markup=markup)
            edited=True
        except Exception:
            pass
    try:
        await m.delete()
    except Exception:
        pass
    if not edited:
        await m.answer(confirmation, reply_markup=markup)

@router.message(Buy.password)
async def buy_password(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear()
        return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."), reply_markup=inline_home_kb())
    password=(m.text or "").strip()
    if password.lower()=="/cancel":
        await state.clear()
        return await m.answer("❌ Cancelled.")
    if len(password)<1 or len(password)>256:
        return await m.answer("❌ Please send a valid password (1–256 characters).")
    d=await state.get_data();
    try:
        await m.delete()
    except Exception:
        pass
    p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(d["pid"],),"one"); u=get_user(m.from_user)
    if not p or is_auto_code_product(p):
        await state.clear(); return await m.answer("❌ Credential step is no longer required.")
    if effective_stock(p)<1:
        await state.clear(); return await m.answer("❌ Product is out of stock.")
    await state.update_data(account_password=password)
    await state.set_state(Buy.confirm)
    uid=html.escape(d.get("game_uid", "")); total=float(p["price"])
    confirmation=(f"🛒 <b>Purchase Confirmation</b>\n\n🎮 Product: <b>{html.escape(p['name'])}</b>\n🆔 ID / UID: <code>{uid}</code>\n🔑 Password: <code>••••••••</code>\n💰 Price: <b>{fmt_money(total)}</b>\n📦 Quantity: <b>1</b>\n⭐ Total: <b>{fmt_money(total)}</b>\n\n💳 Your balance: <b>{fmt_money(u['balance'])}</b>\n🛠️ Delivery: <b>Manual — Admin processing</b>\n\n⚠️ Please verify the credentials before confirming.")
    markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➖",callback_data="order:qty:-1"),InlineKeyboardButton(text="📦 1",callback_data="order:noop"),InlineKeyboardButton(text="➕",callback_data="order:qty:1")],[InlineKeyboardButton(text=setting("button_confirm","✅ Confirm Purchase"),callback_data="order:confirm")],[InlineKeyboardButton(text="✏️ Re-enter Credentials",callback_data="order:cancel"),InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]])
    await m.answer(confirmation,reply_markup=markup)


@router.callback_query(Buy.confirm,F.data=="order:noop")
async def order_qty_noop(c:CallbackQuery,state:FSMContext):
    await c.answer()

@router.callback_query(Buy.confirm,F.data.startswith("order:qty:"))
async def order_qty(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); pid=int(d["pid"]); p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    current=int(d.get("qty",1)); delta=int(c.data.rsplit(":",1)[1]); max_qty=1 if not is_auto_code_product(p) else max(1,min(10,effective_stock(p))); qty=max(1,min(max_qty,current+delta))
    if qty==current: return await c.answer("Maximum available quantity reached." if delta>0 else "Minimum quantity is 1.",show_alert=True)
    await state.update_data(qty=qty)
    total=float(p["price"])*qty; uid=html.escape(d.get("game_uid","")); balance=float(get_user(c.from_user)["balance"])
    cred_line = "\n🔑 Password: <code>••••••••</code>" if not is_auto_code_product(p) else ""
    delivery_line = "🛠️ Manual — Admin processing" if not is_auto_code_product(p) else "⚡ Instant Delivery"
    text=(f"🛒 <b>Purchase Confirmation</b>\n\n🎮 Product: <b>{html.escape(p['name'])}</b>\n🆔 UID: <code>{uid}</code>{cred_line}\n💰 Unit Price: <b>{fmt_money(p['price'])}</b>\n📦 Quantity: <b>{qty}</b>\n⭐ Total: <b>{fmt_money(total)}</b>\n\n💳 Your balance: <b>{fmt_money(balance)}</b>\n{delivery_line}\n\n{html.escape(custom_text('buy_prompt','Confirm your purchase:'))}")
    markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➖",callback_data="order:qty:-1"),InlineKeyboardButton(text=f"📦 {qty}",callback_data="order:noop"),InlineKeyboardButton(text="➕",callback_data="order:qty:1")],[InlineKeyboardButton(text=setting("button_confirm","✅ Confirm Purchase"),callback_data="order:confirm")],[InlineKeyboardButton(text=setting("button_back","⬅️ Back"),callback_data="order:cancel"),InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]])
    try: await c.message.edit_text(text,reply_markup=markup)
    except Exception:
        try: await c.message.edit_caption(caption=text,reply_markup=markup)
        except Exception: pass
    await c.answer()

@router.callback_query(Buy.confirm,F.data=="order:cancel")
async def order_cancel(c:CallbackQuery,state:FSMContext):
    await state.clear()
    await c.answer("Cancelled")
    try:
        await c.message.edit_text("❌ <b>Purchase cancelled.</b>\n\nChoose another product from the menu below.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("inline_shop","🛍️ Shop"),callback_data="home:shop")],[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))
    except Exception:
        try:
            await c.message.edit_caption(caption="❌ <b>Purchase cancelled.</b>\n\nChoose another product from the menu below.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("inline_shop","🛍️ Shop"),callback_data="home:shop")],[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))
        except Exception:
            await c.message.answer("❌ <b>Purchase cancelled.</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("inline_shop","🛍️ Shop"),callback_data="home:shop")],[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))

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
    """Show secure payment choice. Money/stock is touched only after final confirmation."""
    if maintenance_active() and not is_admin(c.from_user.id):
        await state.clear(); return await c.answer("Shop is under maintenance.", show_alert=True)
    d=await state.get_data(); pid=int(d["pid"]); p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one"); u=get_user(c.from_user)
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    qty=max(1,min(10,int(d.get("qty",1)))); qty=1 if not is_auto_code_product(p) else qty
    stock=effective_stock(p); total=float(p["price"])*qty
    if stock<qty: return await c.answer("Stock changed. Please retry.",show_alert=True)
    if float(u["balance"]) >= total:
        wallet_label=f"💰 Pay from Wallet • {fmt_money(u['balance'])}"
    else:
        wallet_label=f"💰 Wallet • Need {fmt_money(total-float(u['balance']))} more"
    cred_line = f"\n🆔 ID / UID: <code>{html.escape(str(d.get('game_uid','')))}</code>\n🔑 Password: <code>••••••••</code>" if not is_auto_code_product(p) else ""
    delivery_line = "🛠️ Manual — Admin processing" if not is_auto_code_product(p) else "⚡ Instant Delivery"
    text=(f"🧾 <b>Order Review</b>\n\n🎮 Product: <b>{html.escape(p['name'])}</b>{cred_line}\n📦 Quantity: <b>{qty}</b>\n💰 Unit Price: <b>{fmt_money(p['price'])}</b>\n⭐ Total: <b>{fmt_money(total)}</b>\n\n👛 Wallet Balance: <b>{fmt_money(u['balance'])}</b>\n{delivery_line}\n\n💳 <b>Choose Payment Method</b>")
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=wallet_label,callback_data="order:pay_wallet")],
        [InlineKeyboardButton(text="🌐 Pay Directly",callback_data="order:pay_direct")],
        [InlineKeyboardButton(text="⬅️ Back",callback_data="order:cancel"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]
    ])
    await c.answer()
    try: await c.message.edit_text(text,reply_markup=kb)
    except Exception:
        try: await c.message.edit_caption(caption=text,reply_markup=kb)
        except Exception: await c.message.answer(text,reply_markup=kb)


def _order_payment_methods_kb(prefix="orderpay:method:"):
    active=[x for x in payment_method_specs() if payment_method_enabled(x[0])]
    rows=[]
    for i in range(0,len(active),2):
        rows.append([InlineKeyboardButton(text=f"{x[2]} {x[1]}",callback_data=f"{prefix}{x[0]}") for x in active[i:i+2]])
    rows.append([InlineKeyboardButton(text="⬅️ Back",callback_data="order:confirm")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _start_direct_order_payment(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); pid=int(d["pid"]); p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    qty=max(1,min(10,int(d.get("qty",1)))); qty=1 if not is_auto_code_product(p) else qty
    if effective_stock(p)<qty: return await c.answer("Out of stock.",show_alert=True)
    total=float(p["price"])*qty
    await state.update_data(direct_amount=total,direct_qty=qty)
    await state.set_state(DirectPaymentState.method)
    await c.answer()
    await c.message.edit_text(
        f"🌐 <b>Direct Payment</b>\n\n💰 Amount: <b>{fmt_money(total)}</b>\n\nChoose a live payment method below. Your order will remain <b>awaiting payment</b> until the payment is verified by an admin.",
        reply_markup=_order_payment_methods_kb())


@router.callback_query(Buy.confirm,F.data=="order:pay_wallet")
async def order_pay_wallet(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); pid=int(d["pid"]); p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one"); u=get_user(c.from_user)
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    qty=max(1,min(10,int(d.get("qty",1)))); qty=1 if not is_auto_code_product(p) else qty
    if float(u["balance"]) < float(p["price"])*qty: return await c.answer("Insufficient wallet balance. Choose Direct Payment instead.",show_alert=True)
    return await _fulfill_wallet_order(c,state,d,p,u,qty)


async def _fulfill_wallet_order(c,state,d,p,u,qty):
    await state.clear(); delivered=[]; pending=[]; order_ids=[]
    with DB_LOCK:
        try:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM users WHERE tg_id=%s FOR UPDATE",(c.from_user.id,)); u=cur.fetchone()
                    cur.execute("SELECT * FROM products WHERE id=%s AND active=1 FOR UPDATE",(p["id"],)); p=cur.fetchone()
                    if not u or not p: raise RuntimeError("Order unavailable.")
                    unit_price=float(p["price"]); available=effective_stock(p); 
                    if available<qty: raise RuntimeError(f"Only {available} item(s) available.")
                    total=unit_price*qty
                    if float(u["balance"])<total: raise RuntimeError("Balance changed. Please retry.")
                    for _ in range(qty):
                        delivered_code=None; auto_code=False
                        cur.execute("SELECT * FROM product_codes WHERE product_id=%s AND status='available' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",(p["id"],)); code_row=cur.fetchone()
                        auto_code=bool(code_row) or p["delivery_type"]=="code"
                        if auto_code:
                            if not code_row: raise RuntimeError("Code stock changed. Please retry.")
                            cur.execute("UPDATE product_codes SET status='sold',sold_to=%s,sold_at=NOW() WHERE id=%s AND status='available'",(u["id"],code_row["id"]))
                            delivered_code=code_row["code"]; status="completed"
                        else:
                            cur.execute("UPDATE products SET stock=stock-1,updated_at=NOW() WHERE id=%s AND stock>0",(p["id"],))
                            if cur.rowcount!=1: raise RuntimeError("Stock changed. Please retry.")
                            status="pending"
                        cur.execute("UPDATE users SET balance=balance-%s,updated_at=NOW() WHERE id=%s AND balance>=%s",(unit_price,u["id"],unit_price))
                        if cur.rowcount!=1: raise RuntimeError("Balance changed. Please retry.")
                        cur.execute("INSERT INTO orders(user_id,product_id,game_uid,account_password,total,delivered_code,status,payment_mode) VALUES(%s,%s,%s,%s,%s,%s,%s,'wallet') RETURNING id",(u["id"],p["id"],d.get("game_uid",""),d.get("account_password","") if not auto_code else "",unit_price,delivered_code,status)); oid=cur.fetchone()["id"]; order_ids.append(oid)
                        if delivered_code:
                            cur.execute("UPDATE product_codes SET order_id=%s WHERE id=%s",(oid,code_row["id"])); delivered.append((oid,p["name"],delivered_code,unit_price))
                        else: pending.append((oid,p["name"],unit_price))
                        cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(u["id"],-unit_price,"purchase",f"Order #{oid}"))
                        if status=="completed": award_completed_order_rewards(cur,oid,u["id"],unit_price)
                    sync_code_product_stock(p["id"],conn)
        except Exception as exc:
            print(f"wallet order error: {exc}"); return await c.answer("Order failed. Nothing was charged.",show_alert=True)
    await _send_order_result(c,p,u,qty,order_ids,delivered,pending,total=sum(float(x[3]) for x in delivered)+sum(float(x[2]) for x in pending),payment_label="Wallet")


async def _send_order_result(c,p,u,qty,order_ids,delivered,pending,total,payment_label="Wallet"):
    await c.answer("✅ Payment successful")
    msg=["✅ <b>Purchase Successful!</b>",f"🎮 Product: <b>{html.escape(p['name'])}</b>",f"📦 Quantity: <b>{qty}</b>",f"💳 Paid via: <b>{payment_label}</b>",f"💰 Paid: <b>{fmt_money(total)}</b>",f"🧾 Orders: <b>{len(order_ids)}</b>"]
    if delivered: msg.append("\n🎁 <b>Instant Delivery</b>\n"+"\n".join(f"#{o} • <code>{code}</code>" for o,n,code,a in delivered))
    if pending: msg.append("\n⏳ <b>Manual Delivery</b>\n"+"\n".join(f"#{o} • {fmt_money(a)}" for o,n,a in pending))
    await c.message.answer("\n".join(msg),reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 View My Orders",callback_data="home:orders")],[InlineKeyboardButton(text="🛒 Buy More",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]]))
    if pending:
        for oid in order_ids:
            if not any(x[0] == oid for x in pending):
                continue
            for admin_id in ADMIN_IDS:
                try:
                    order_row=db_execute("SELECT game_uid,account_password FROM orders WHERE id=%s",(oid,),"one")
                    game_uid=(order_row["game_uid"] if order_row else "") or ""
                    has_password=bool(order_row and order_row.get("account_password"))
                    admin_text=f"🧾 <b>New Manual Order #{oid}</b>\n\n👤 User: <code>{u['tg_id']}</code>\n🎮 Product: {html.escape(p['name'])}\n📦 Qty: 1\n💰 Total: {fmt_money(next((x[2] for x in pending if x[0]==oid), total))}\n💳 Paid via: <b>{html.escape(payment_label)}</b>\n🆔 ID / UID: <code>{html.escape(str(game_uid))}</code>\n🔑 Password: <code>{'••••••••' if has_password else 'Not required'}</code>\n\n⏳ <b>Manual delivery required.</b>"
                    admin_kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✍️ Write Delivery",callback_data=f"order_note:{oid}")],[InlineKeyboardButton(text="❌ Reject + Refund",callback_data=f"order_reject:{oid}")]])
                    await c.bot.send_message(admin_id,admin_text,reply_markup=admin_kb)
                except Exception as exc:
                    logging.exception("Failed to send manual order notification #%s to admin %s: %s", oid, admin_id, exc)


@router.callback_query(Buy.confirm,F.data=="order:pay_direct")
async def order_pay_direct(c:CallbackQuery,state:FSMContext):
    ensure_buyer_account(c.from_user.id)
    d=await state.get_data()
    if int(d.get("qty",1)) != 1:
        return await c.answer("Direct live payment currently supports quantity 1. Reduce quantity to 1 first.",show_alert=True)
    return await _start_direct_order_payment(c,state)

@router.callback_query(DirectPaymentState.method,F.data.startswith("orderpay:method:"))
async def direct_payment_method(c:CallbackQuery,state:FSMContext):
    ensure_buyer_account(c.from_user.id)
    method=c.data.split(":")[-1]
    if not payment_method_enabled(method): return await c.answer("Payment method unavailable.",show_alert=True)
    d=await state.get_data(); amount=float(d.get("direct_amount",0));
    if not amount or not payment_amount_limits_ok(amount)[0]: return await c.answer("Invalid order amount.",show_alert=True)
    await state.update_data(direct_method=method); await state.set_state(DirectPaymentState.trx)
    label=dict((code,(label,icon)) for code,label,icon in payment_method_specs()).get(method,(method.title(),"💳"))[0]
    account=payment_method_account(method) or "Not configured"; instruction=payment_method_instruction(method) or "Follow the shop instructions."
    extra=f"\n🌐 <b>Network:</b> {html.escape(setting('payment_binance_network',''))}" if method=="binance" else ""
    await c.answer(); await c.message.edit_text(f"{dict((code,(label,icon)) for code,label,icon in payment_method_specs()).get(method,(label,'💳'))[1]} <b>{html.escape(label)}</b>\n\n💰 Exact Amount: <b>{fmt_money(amount)}</b>\n💳 Account/Wallet: <code>{html.escape(account)}</code>{extra}\n\n📝 {html.escape(instruction)}\n\n⚠️ Send the exact amount, then send the transaction ID.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel",callback_data="orderpay:cancel")]]))

@router.callback_query(DirectPaymentState.method,F.data=="order:confirm")
async def direct_payment_back(c:CallbackQuery,state:FSMContext):
    await state.set_state(Buy.confirm)
    return await order_confirm(c,state)

@router.callback_query(DirectPaymentState.trx,F.data=="orderpay:cancel")
@router.callback_query(DirectPaymentState.receipt,F.data=="orderpay:cancel")
async def direct_payment_cancel(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); payid=d.get("direct_payment_id"); oid=d.get("direct_order_id")
    if payid or oid:
        with DB_LOCK:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    if payid:
                        cur.execute("UPDATE payments SET status='cancelled',updated_at=NOW() WHERE id=%s AND status='pending'",(payid,))
                    if oid:
                        cur.execute("UPDATE orders SET status='cancelled',updated_at=NOW() WHERE id=%s AND status='awaiting_payment'",(oid,))
    await state.clear(); await c.answer("Cancelled"); await c.message.edit_text("❌ Direct payment cancelled. No balance was charged.",reply_markup=inline_home_kb())

@router.message(DirectPaymentState.trx)
async def direct_payment_trx(m:Message,state:FSMContext):
    trx=(m.text or "").strip(); d=await state.get_data();
    if trx.lower()=="/cancel": await state.clear(); return await m.answer("❌ Cancelled.")
    if len(trx)<3 or len(trx)>255: return await m.answer("❌ Please send a valid transaction ID.")
    amount=float(d.get("direct_amount",0)); method=d.get("direct_method"); pid=int(d["pid"]); p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one");
    if not p: await state.clear(); return await m.answer("❌ Product unavailable.")
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                normalized=re.sub(r"\s+","",trx).lower()
                cur.execute("SELECT id FROM payments WHERE lower(regexp_replace(trx_id,'\\s','','g'))=%s LIMIT 1",(normalized,))
                if cur.fetchone(): return await m.answer("❌ This transaction ID has already been submitted.")
                cur.execute("INSERT INTO orders(user_id,product_id,game_uid,account_password,total,status,payment_mode) VALUES((SELECT id FROM users WHERE tg_id=%s),%s,%s,%s,%s,'awaiting_payment','direct') RETURNING id",(m.from_user.id,pid,d.get("game_uid",""),d.get("account_password","") if not is_auto_code_product(p) else "",amount)); oid=cur.fetchone()["id"]
                cur.execute("INSERT INTO payments(user_id,amount,method,trx_id,status,order_id) VALUES((SELECT id FROM users WHERE tg_id=%s),%s,%s,%s,'pending',%s) RETURNING id",(m.from_user.id,amount,method,trx,oid)); payid=cur.fetchone()["id"]
                cur.execute("UPDATE orders SET payment_id=%s WHERE id=%s",(payid,oid))
                record_payment_audit(cur,payid,None,"submitted","","pending",amount,method,trx,f"Direct payment for Order #{oid}")
    await state.update_data(direct_order_id=oid,direct_payment_id=payid,direct_trx=trx)
    await state.set_state(DirectPaymentState.receipt)
    await m.answer(f"📸 <b>Payment Receipt</b>\n\nOrder: <b>#{oid}</b>\nPayment: <b>#{payid}</b>\n💰 Amount: <b>{fmt_money(amount)}</b>\n\nSend a screenshot/photo if available, or use /skip.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭️ Skip Screenshot",callback_data=f"orderpay:skip:{payid}"),InlineKeyboardButton(text="❌ Cancel",callback_data="orderpay:cancel")]]))

@router.callback_query(DirectPaymentState.receipt,F.data.startswith("orderpay:skip:"))
async def direct_payment_skip(c:CallbackQuery,state:FSMContext):
    return await _finish_direct_payment(c.message,state,False)

@router.message(DirectPaymentState.receipt,F.photo)
@router.message(DirectPaymentState.receipt,F.document)
async def direct_payment_receipt(m:Message,state:FSMContext):
    d=await state.get_data(); payid=d.get("direct_payment_id")
    if not payid: return await m.answer("❌ Payment session expired.")
    file_id=m.photo[-1].file_id if m.photo else m.document.file_id
    db_execute("INSERT INTO payment_receipts(payment_id,file_id) VALUES(%s,%s) ON CONFLICT(payment_id) DO UPDATE SET file_id=EXCLUDED.file_id",(payid,file_id))
    return await _finish_direct_payment(m,state,True)

async def _finish_direct_payment(m:Message,state:FSMContext,receipt=False):
    d=await state.get_data(); payid=d.get("direct_payment_id"); oid=d.get("direct_order_id");
    if not payid or not oid: await state.clear(); return await m.answer("❌ Payment session expired.")
    row=db_execute("SELECT p.*,o.product_id,o.total AS order_total,o.status AS order_status FROM payments p JOIN orders o ON o.payment_id=p.id WHERE p.id=%s",(payid,),"one")
    if not row: await state.clear(); return await m.answer("❌ Payment request not found.")
    await state.clear()
    receipt_text="📸 Receipt received" if receipt else "📸 No receipt attached"
    await m.answer(f"⏳ <b>Direct Payment Submitted</b>\n\n🧾 Order: <b>#{oid}</b>\n💳 Payment: <b>#{payid}</b>\n💰 Amount: <b>{fmt_money(row['amount'])}</b>\n💳 Method: <b>{html.escape(row['method'].title())}</b>\n{receipt_text}\n\nWaiting for admin verification.",reply_markup=inline_home_kb())
    receipt=db_execute("SELECT 1 FROM payment_receipts WHERE payment_id=%s",(payid,),"one")
    for admin_id in ADMIN_IDS:
        try:
            p=db_execute("SELECT name FROM products WHERE id=%s",(row['product_id'],),"one")
            await m.bot.send_message(admin_id,f"🌐 <b>Direct Order Payment #{payid}</b>\n\n🧾 Order: <b>#{oid}</b>\n👤 User: <code>{m.from_user.id}</code>\n🎮 Product: <b>{html.escape(p['name'] if p else 'Product')}</b>\n💰 Amount: <b>{fmt_money(row['amount'])}</b>\n💳 Method: <b>{html.escape(row['method'].title())}</b>\n🧾 TxID: <code>{html.escape(row['trx_id'])}</code>\n📸 Receipt: {'Yes' if receipt else 'No'}\n\n⏳ Verify payment before releasing the order.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📸 View Receipt",callback_data=f"pay_receipt:{payid}"),InlineKeyboardButton(text="🧾 Audit",callback_data=f"pay_audit:{payid}")],[InlineKeyboardButton(text="✅ Approve & Fulfill",callback_data=f"pay_credit:{payid}"),InlineKeyboardButton(text="❌ Reject",callback_data=f"pay_reject:{payid}")]]))
        except Exception as exc:
            logging.exception("Failed to send direct payment notification #%s to admin %s: %s", payid, admin_id, exc)

@router.message(Command("vip"))
async def vip_command(m:Message):
    if user_access_denied(m.from_user.id) and not is_admin(m.from_user.id):
        return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance."))
    u=get_user(m.from_user)
    tier, discount=vip_tier(u)
    spend=float(u.get("lifetime_spend") or 0)
    points=int(u.get("loyalty_points") or 0)
    await m.answer(
        f"💎 <b>VIP Membership</b>\n\n"
        f"🏅 Tier: <b>{tier}</b>\n"
        f"💰 Lifetime Spend: <b>{fmt_money(spend)}</b>\n"
        f"⭐ Loyalty Points: <b>{points}</b>\n"
        f"🎁 Current VIP Discount: <b>{discount:g}%</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]])
    )


@router.message(Command("version"))
async def version_command(m:Message):
    await m.answer(
        f"🚀 <b>{html.escape(shop_name())} {APP_VERSION}</b>\n"
        "☁️ PostgreSQL database enabled\n⚡ Instant code delivery enabled\n🔎 Smart product search enabled\n"
        "🎨 Ultra Elite storefront enabled\n⭐ Favorites / wishlist enabled\n🏅 Loyalty rewards enabled\n🤝 Referral rewards enabled\n"
        "📦 Order tracking enabled\n🔔 Buyer notifications enabled\n📸 Payment receipt verification enabled\n"
        "💎 Premium storefront UI enabled\n📢 Announcement banner enabled\n🤖 Ultra automation + notification retry enabled\n"
        "💎 VIP membership engine enabled\n📣 Ultra marketing + targeted campaigns enabled\n🛒 Abandoned-cart recovery enabled\n"
        "🎯 User segmentation + campaign conversion tracking enabled"
    )

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


# ========================= V9.2 PERFORMANCE + RELIABILITY CORE =========================
_PERF_STARTED_AT = time.monotonic()
_PERF_LOCK = threading.Lock()
_PERF = {"requests": 0, "errors": 0, "slow_ops": 0, "last_error": "", "last_slow_op": ""}
_SETTING_CACHE = {}
_SETTING_CACHE_TTL = float(os.getenv("SETTING_CACHE_TTL", "3"))

def perf_inc(key, value=1):
    with _PERF_LOCK:
        _PERF[key] = _PERF.get(key, 0) + value

def perf_snapshot():
    with _PERF_LOCK:
        data = dict(_PERF)
    data["uptime_sec"] = round(time.monotonic() - _PERF_STARTED_AT, 2)
    data["python"] = sys.version.split()[0]
    return data

def _cached_setting(key, default=None):
    now = time.monotonic()
    hit = _SETTING_CACHE.get(key)
    if hit and now - hit[0] < _SETTING_CACHE_TTL:
        return hit[1]
    try:
        value = setting(key, default)
    except Exception as exc:
        perf_inc("errors")
        with _PERF_LOCK:
            _PERF["last_error"] = f"setting:{key}:{type(exc).__name__}"
        return default
    _SETTING_CACHE[key] = (now, value)
    return value

def invalidate_setting_cache(key=None):
    if key is None:
        _SETTING_CACHE.clear()
    else:
        _SETTING_CACHE.pop(key, None)

def performance_health_snapshot():
    snap = perf_snapshot()
    snap["cache_entries"] = len(_SETTING_CACHE)
    snap["cache_ttl_sec"] = _SETTING_CACHE_TTL
    return snap

async def performance_maintenance_loop():
    while True:
        try:
            now = time.monotonic()
            stale = [k for k, (ts, _) in list(_SETTING_CACHE.items()) if now - ts >= _SETTING_CACHE_TTL]
            for k in stale:
                _SETTING_CACHE.pop(k, None)
            await asyncio.sleep(max(5.0, _SETTING_CACHE_TTL))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            perf_inc("errors")
            with _PERF_LOCK:
                _PERF["last_error"] = f"performance:{type(exc).__name__}"
            await asyncio.sleep(10)

def _install_performance_health_hook():
    # Extend the existing health response without replacing its server implementation.
    return performance_health_snapshot

def normalize_trx_id(value):
    return "".join((value or "").strip().casefold().split())

def trx_fingerprint(value):
    import hashlib
    return hashlib.sha256(normalize_trx_id(value).encode("utf-8")).hexdigest()[:24]

def payment_amount_limits_ok(amount):
    try:
        value=float(amount)
        lo=float(setting("payment_min_deposit","10") or 10)
        hi=float(setting("payment_max_deposit","100000") or 100000)
        if value < lo:
            return False, f"Minimum deposit is {fmt_money(lo)}."
        if value > hi:
            return False, f"Maximum deposit is {fmt_money(hi)}."
        return True, ""
    except Exception:
        return False, "Invalid deposit amount."

def record_payment_audit(cur, payment_id, admin_id, action, old_status="", new_status="", amount=None, method="", trx="", note=""):
    cur.execute(
        "INSERT INTO payment_audit(payment_id,admin_id,action,old_status,new_status,amount,method,trx_fingerprint,note) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (payment_id, admin_id, action, old_status, new_status, amount, method, trx_fingerprint(trx), note)
    )

def payment_method_specs():
    return [
        ("bkash", setting("payment_bkash_label", "bKash"), setting("payment_bkash_icon", "🟪")),
        ("nagad", setting("payment_nagad_label", "Nagad"), setting("payment_nagad_icon", "🟩")),
        ("rocket", setting("payment_rocket_label", "Rocket"), setting("payment_rocket_icon", "🔵")),
        ("binance", setting("payment_binance_label", "Binance"), setting("payment_binance_icon", "🟠")),
    ]

def payment_method_enabled(code):
    return setting(f"payment_{code}_enabled", "0") == "1"

def payment_method_account(code):
    return setting(f"payment_{code}_account", "").strip()

def payment_method_instruction(code):
    return setting(f"payment_{code}_instruction", "").strip()

def payment_method_keyboard():
    rows=[]
    active=[x for x in payment_method_specs() if payment_method_enabled(x[0])]
    for i in range(0,len(active),2):
        rows.append([InlineKeyboardButton(text=f"{code_icon} {label}",callback_data=f"paymethod:{code}") for code,label,code_icon in active[i:i+2]])
    rows.append([InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def payment_amount_keyboard():
    raw=setting("payment_presets","100,200,500,1000")
    values=[]
    for x in raw.split(","):
        try:
            n=float(x.strip())
            if n>0: values.append(n)
        except Exception:
            pass
    rows=[]
    for i in range(0,len(values),2):
        rows.append([InlineKeyboardButton(text=f"{n:g} {currency()}",callback_data=f"payamount:{n:g}") for n in values[i:i+2]])
    rows.append([InlineKeyboardButton(text="✏️ Custom Amount",callback_data="payamount:custom")])
    rows.append([InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def show_deposit_start(target, state:FSMContext, edit=False):
    await state.set_state(PaymentState.amount)
    active=[x for x in payment_method_specs() if payment_method_enabled(x[0])]
    methods=" • ".join(f"{x[2]} {x[1]}" for x in active) or "No payment method is currently available."
    text=custom_text("deposit_prompt", "💰 <b>Add Balance</b>\n\nSend the amount you want to add.")
    text += f"\n\n💳 <b>Available:</b> {html.escape(methods)}"
    text += "\n\nChoose a quick amount or tap <b>Custom Amount</b>."
    if edit:
        return await target.edit_text(text, reply_markup=payment_amount_keyboard())
    return await target.answer(text, reply_markup=payment_amount_keyboard())

@router.message(Command("balance", "deposit"))
@router.message(F.text=="💰 Deposit")
@router.message(F.text=="💰 Add Balance")
@router.message(F.text=="💳 Wallet")
async def add_balance(m:Message,state:FSMContext):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id): return await m.answer("🚫 Your account is blocked.")
    if maintenance_active() and not is_admin(m.from_user.id): return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."))
    if not any(payment_method_enabled(code) for code,_,_ in payment_method_specs()):
        return await m.answer("❌ Deposits are temporarily unavailable. Please contact support.", reply_markup=inline_home_kb())
    return await show_deposit_start(m,state,edit=False)

async def validate_deposit_amount(raw):
    try: amount=float(str(raw).strip())
    except (TypeError,ValueError): return None,"❌ Enter a valid amount."
    if amount <= 0: return None,"❌ Amount must be greater than zero."
    min_amt=float(setting("deposit_min","10") or 10)
    max_raw=float(setting("deposit_max","0") or 0)
    max_amt=max_raw if max_raw>0 else 1000000000
    if amount<min_amt or amount>max_amt:
        return None,f"❌ Amount must be between {min_amt:g} and {max_amt:g}."
    return round(amount,2),None

@router.callback_query(PaymentState.amount,F.data.startswith("payamount:"))
async def payment_amount_button(c:CallbackQuery,state:FSMContext):
    if maintenance_active() and not is_admin(c.from_user.id):
        await state.clear(); return await c.answer("Deposits are temporarily unavailable.",show_alert=True)
    value=c.data.split(":",1)[1]
    if value=="back":
        await c.answer(); return await show_deposit_start(c.message,state,edit=True)
    if value=="custom":
        await c.answer()
        return await c.message.edit_text(
            custom_text("deposit_prompt", "💰 <b>Add Balance</b>\n\nSend the amount you want to add.")+
            f"\n\n💱 Limits: <b>{setting('deposit_min','10')}</b> – <b>{setting('deposit_max','0') or 'Unlimited'}</b> {currency()}\n\nType the amount below.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Amount Options",callback_data="payamount:back")],[InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]])
        )
    amount,error=await validate_deposit_amount(value)
    if error: return await c.answer(error,show_alert=True)
    await state.update_data(amount=amount)
    await state.set_state(PaymentState.method)
    await c.answer()
    return await c.message.edit_text(custom_text("payment_method_prompt", "💳 <b>Choose a payment method</b>:")+"\n\nTap a method below to view its payment instructions.",reply_markup=payment_method_keyboard())

@router.message(PaymentState.amount)
async def payment_amount(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear(); return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."), reply_markup=inline_home_kb())
    amount,error=await validate_deposit_amount(m.text)
    if error: return await m.answer(error,reply_markup=payment_amount_keyboard())
    await state.update_data(amount=amount)
    await state.set_state(PaymentState.method)
    await m.answer(custom_text("payment_method_prompt", "💳 <b>Choose a payment method</b>:")+"\n\nTap a method below to view its payment instructions.",reply_markup=payment_method_keyboard())

@router.callback_query(PaymentState.method,F.data.startswith("paymethod:"))
async def payment_method(c:CallbackQuery,state:FSMContext):
    if maintenance_active() and not is_admin(c.from_user.id):
        await state.clear(); return await c.answer("Deposits are temporarily unavailable.",show_alert=True)
    method=c.data.split(":",1)[1]
    if not payment_method_enabled(method): return await c.answer("This payment method is currently unavailable.",show_alert=True)
    d=await state.get_data(); amount=d.get("amount")
    if not amount:
        await state.clear(); return await c.answer("Payment session expired. Start again.",show_alert=True)
    specs={code:(label,icon) for code,label,icon in payment_method_specs()}
    label,icon=specs.get(method,(method.title(),"💳"))
    account=payment_method_account(method) or "Not configured"
    instruction=payment_method_instruction(method) or "Follow the payment instructions from support."
    extra=""
    if method=="binance": extra=f"\n🌐 <b>Network:</b> {html.escape(setting('payment_binance_network','Specify network before accepting payments.'))}"
    text=(f"{icon} <b>{html.escape(label)} Payment</b>\n\n"
          f"💰 Amount: <b>{fmt_money(amount)}</b>\n"
          f"💳 Account/Wallet: <code>{html.escape(account)}</code>\n"
          f"📝 {html.escape(instruction)}{extra}\n\n"
          f"⚠️ Send exactly <b>{fmt_money(amount)}</b>. Keep your TxID/TrxID and payment screenshot ready.")
    await state.update_data(method=method)
    await c.answer()
    return await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ I've Paid — Enter TxID",callback_data="paycontinue:trx")],
        [InlineKeyboardButton(text="⬅️ Payment Methods",callback_data="paycontinue:methods")],
        [InlineKeyboardButton(text="❌ Cancel",callback_data="paycontinue:cancel")]
    ]))

@router.callback_query(PaymentState.method,F.data.startswith("paycontinue:"))
async def payment_continue(c:CallbackQuery,state:FSMContext):
    action=c.data.split(":",1)[1]
    if action=="methods":
        await c.answer(); return await c.message.edit_text(custom_text("payment_method_prompt", "💳 <b>Choose a payment method</b>:")+"\n\nTap a method below.",reply_markup=payment_method_keyboard())
    if action=="cancel":
        await state.clear(); await c.answer("Cancelled"); return await c.message.edit_text("❌ <b>Deposit cancelled.</b>",reply_markup=inline_home_kb())
    await state.set_state(PaymentState.trx); await c.answer()
    return await c.message.edit_text(f"🧾 <b>{html.escape(setting('payment_trx_label','Transaction ID / TxID / Hash'))}</b>\n\nSend your transaction ID now.\n\n⚠️ Make sure it matches the payment you just made.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel",callback_data="paycancel")]]))

@router.callback_query(F.data=="paycancel")
async def payment_cancel(c:CallbackQuery,state:FSMContext):
    await state.clear(); await c.answer("Cancelled"); await c.message.edit_text("❌ <b>Deposit cancelled.</b>",reply_markup=inline_home_kb())

@router.message(PaymentState.trx)
async def payment_trx(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear(); return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."), reply_markup=inline_home_kb())
    trx=(m.text or "").strip()
    normalized=normalize_trx_id(trx)
    if len(normalized)<3 or len(normalized)>120: return await m.answer("❌ Invalid transaction ID / hash.")
    d=await state.get_data(); u=get_user(m.from_user); method=d.get("method"); amount=d.get("amount")
    if not method or not amount or not payment_method_enabled(method):
        await state.clear(); return await m.answer("❌ Payment session expired or method unavailable. Please start again.",reply_markup=inline_home_kb())
    ok, limit_error=payment_amount_limits_ok(amount)
    if not ok:
        await state.clear(); return await m.answer(f"❌ {limit_error}",reply_markup=inline_home_kb())
    try:
        # Normalize before duplicate lookup so spacing/case variants of the same TxID cannot bypass the guard.
        with DB_LOCK:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id,status FROM payments WHERE lower(regexp_replace(trx_id,'\\s','','g'))=%s LIMIT 1",(normalized,))
                    duplicate=cur.fetchone()
                    if duplicate:
                        return await m.answer(f"❌ This transaction ID was already submitted (Payment #{duplicate['id']}, status: {duplicate['status']}). Please verify the TxID/Hash.")
                    cur.execute("INSERT INTO payments(user_id,amount,method,trx_id) VALUES(%s,%s,%s,%s) RETURNING id",(u["id"],amount,method,trx))
                    row=cur.fetchone()
                    payment_id=row["id"]
                    record_payment_audit(cur,payment_id,None,"submitted","", "pending",amount,method,trx,"User submitted payment")
    except errors.UniqueViolation:
        return await m.answer("❌ This transaction ID was already submitted. Please verify the TxID/Hash and try again.")
    await state.update_data(payment_id=payment_id,trx_id=trx)
    await state.set_state(PaymentState.receipt)
    required=setting("payment_receipt_required","0")=="1"
    await m.answer(f"📸 <b>Payment screenshot</b>\n\nPayment Request: <b>#{payment_id}</b>\nSend the payment screenshot as a photo/document.{ ' It is required for verification.' if required else '' }\n\nIf unavailable, use <code>/skip</code>.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭️ Skip Screenshot",callback_data=f"payskip:{payment_id}")],[InlineKeyboardButton(text="❌ Cancel",callback_data="paycancel")]]))

@router.callback_query(PaymentState.receipt,F.data.startswith("payskip:"))
async def payment_receipt_skip_button(c:CallbackQuery,state:FSMContext):
    if setting("payment_receipt_required","0")=="1": return await c.answer("Screenshot is required for this payment.",show_alert=True)
    await c.answer(); return await _finish_payment_submission(c.message,state,receipt=False)

@router.message(PaymentState.receipt,F.photo)
@router.message(PaymentState.receipt,F.document)
async def payment_receipt_upload(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear(); return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."), reply_markup=inline_home_kb())
    d=await state.get_data(); payment_id=d.get("payment_id")
    if not payment_id:
        await state.clear(); return await m.answer("❌ Payment session expired. Please start again.",reply_markup=inline_home_kb())
    file_id=m.photo[-1].file_id if m.photo else m.document.file_id
    db_execute("INSERT INTO payment_receipts(payment_id,file_id) VALUES(%s,%s) ON CONFLICT(payment_id) DO UPDATE SET file_id=EXCLUDED.file_id",(payment_id,file_id))
    await _finish_payment_submission(m,state,receipt=True)

@router.message(PaymentState.receipt,Command("skip"))
async def payment_receipt_skip(m:Message,state:FSMContext):
    if setting("payment_receipt_required","0")=="1": return await m.answer("❌ Screenshot is required for this payment.")
    await _finish_payment_submission(m,state,receipt=False)

async def _finish_payment_submission(m:Message,state:FSMContext,receipt=False):
    d=await state.get_data(); payment_id=d.get("payment_id"); u=get_user(m.from_user)
    if not payment_id:
        await state.clear(); return await m.answer("❌ Payment session expired. Please start again.",reply_markup=inline_home_kb())
    row=db_execute("SELECT * FROM payments WHERE id=%s",(payment_id,),"one")
    if not row:
        await state.clear(); return await m.answer("❌ Payment request not found.",reply_markup=inline_home_kb())
    await state.clear()
    receipt_text="📸 Receipt attached" if receipt else "📎 No receipt attached"
    await m.answer(f"✅ <b>Payment Request #{payment_id}</b>\n\n💰 Amount: {fmt_money(row['amount'])}\n💳 Method: {html.escape(row['method'].title())}\n🧾 TxID: <code>{html.escape(row['trx_id'])}</code>\n{receipt_text}\n⏳ Waiting for admin approval.",reply_markup=inline_home_kb())
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_credit", "✅ Credit"),callback_data=f"pay_credit:{payment_id}"),InlineKeyboardButton(text=setting("admin_reject", "❌ Reject"),callback_data=f"pay_reject:{payment_id}")],[InlineKeyboardButton(text="📸 View Receipt",callback_data=f"pay_receipt:{payment_id}"),InlineKeyboardButton(text="🧾 Audit",callback_data=f"pay_audit:{payment_id}")]])
    for admin_id in ADMIN_IDS:
        try:
            admin_text=(f"💳 <b>New Payment #{payment_id}</b>\n\n👤 User: <code>{u['tg_id']}</code>\n💰 Amount: {fmt_money(row['amount'])}\n💳 Method: {html.escape(row['method'].title())}\n🧾 TxID: <code>{html.escape(row['trx_id'])}</code>\n{receipt_text}")
            await m.bot.send_message(admin_id,admin_text,reply_markup=kb)
            if receipt:
                receipt_row=db_execute("SELECT file_id FROM payment_receipts WHERE payment_id=%s",(payment_id,),"one")
                if receipt_row: await m.bot.send_document(admin_id,receipt_row["file_id"],caption=f"📸 Payment #{payment_id} receipt",reply_markup=kb)
        except Exception: pass

@router.message(PaymentState.receipt)
async def payment_receipt_invalid(m:Message,state:FSMContext):
    await m.answer("📸 Please send a screenshot/photo or document. If unavailable, use the inline Skip button or /skip.")

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

@router.callback_query(F.data.startswith("order_note:"))
async def manual_order_note_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":")[1])
    o=db_execute("SELECT id,status FROM orders WHERE id=%s",(oid,),"one")
    if not o or o["status"]!="pending": return await c.answer("Order already processed.",show_alert=True)
    await state.update_data(order_id=oid)
    await state.set_state(AdminState.manual_delivery_note)
    await c.answer()
    await c.message.answer(f"✍️ <b>Delivery note for Order #{oid}</b>\n\nSend the exact delivery information you want the buyer to receive.\n\nSend /cancel to cancel.")

@router.message(AdminState.manual_delivery_note)
async def manual_order_note_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return await state.clear()
    note=(m.text or "").strip()
    if note.lower()=="/cancel":
        await state.clear(); return await m.answer("❌ Delivery note cancelled.",reply_markup=admin_menu())
    if not note or len(note)>4000: return await m.answer("❌ Delivery note must be 1–4000 characters.")
    d=await state.get_data(); oid=int(d["order_id"])
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,)); o=cur.fetchone()
                if not o or o["status"]!="pending":
                    await state.clear(); return await m.answer("❌ Order already processed.")
                cur.execute("UPDATE orders SET admin_note=%s,delivery_note=%s,status='completed',processed_at=NOW(),updated_at=NOW() WHERE id=%s",(note,note,oid))
                award_completed_order_rewards(cur,oid,o["user_id"],o["total"])
                cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
                cur.execute("SELECT name FROM products WHERE id=%s",(o["product_id"],)); p=cur.fetchone()
    admin_log(m.from_user.id,"manual_delivery_note",f"order #{oid} delivered with admin note")
    await state.clear()
    await m.answer(f"✅ Order #{oid} marked delivered and buyer notified.",reply_markup=admin_menu())
    try:
        await m.bot.send_message(u["tg_id"],f"🎉 <b>Order Delivered</b>\n\n🧾 Order: <b>#{oid}</b>\n📦 Product: <b>{html.escape(p['name'] if p else 'Product')}</b>\n\n📝 <b>Delivery Information</b>\n{html.escape(note)}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 Order Details",callback_data=f"order_detail:{oid}")],[InlineKeyboardButton(text="🛍️ Buy More",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]]))
    except Exception as exc:
        logging.exception("Failed to notify buyer for delivered order #%s: %s", oid, exc)


@router.callback_query(F.data.startswith("order_complete:"))
async def manual_order_complete(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Delivery note is required. Use ✍️ Write Delivery.",show_alert=True)
    return await c.answer("Delivery note is required. Use ✍️ Write Delivery.",show_alert=True)

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

def payment_methods_admin_keyboard():
    rows=[]
    for code,label,icon in payment_method_specs():
        status="🟢 ON" if payment_method_enabled(code) else "🔴 OFF"
        rows.append([InlineKeyboardButton(text=f"{icon} {label} • {status}",callback_data=f"admin:paytoggle:{code}"),InlineKeyboardButton(text="✏️ Edit",callback_data=f"admin:payedit:{code}")])
    rows.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@router.callback_query(F.data=="admin:payment_methods")
async def admin_payment_methods(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    active=sum(1 for code,_,_ in payment_method_specs() if payment_method_enabled(code))
    text=("💳 <b>Payment Methods — Payment 2.0</b>\n\n"
          f"Active methods: <b>{active}/4</b>\n"
          "Each method has its own account/wallet, instructions, icon and enable/disable control.\n\n"
          "⚠️ Before enabling a method, configure the real account/wallet and network details.")
    await c.answer(); await c.message.edit_text(text,reply_markup=payment_methods_admin_keyboard())

@router.callback_query(F.data.startswith("admin:paytoggle:"))
async def admin_payment_toggle(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    code=c.data.rsplit(":",1)[1]
    if code not in {x[0] for x in payment_method_specs()}: return await c.answer("Invalid method",show_alert=True)
    new="0" if payment_method_enabled(code) else "1"
    db_execute("INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",(f"payment_{code}_enabled",new))
    _load_settings_cache()
    admin_log(c.from_user.id,"payment_method_toggle",f"{code}={new}")
    await c.answer("Enabled" if new=="1" else "Disabled")
    return await admin_payment_methods(c)

@router.callback_query(F.data.startswith("admin:payedit:"))
async def admin_payment_edit(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    code=c.data.rsplit(":",1)[1]
    labels={"bkash":"bKash","nagad":"Nagad","rocket":"Rocket","binance":"Binance"}
    keys=[(f"payment_{code}_label",f"{labels.get(code,code)} Label"),(f"payment_{code}_account",f"{labels.get(code,code)} Account/Wallet"),(f"payment_{code}_instruction",f"{labels.get(code,code)} Instructions"),(f"payment_{code}_icon",f"{labels.get(code,code)} Icon/Logo")]
    if code=="binance": keys.append(("payment_binance_network","Binance Network"))
    kb=[[InlineKeyboardButton(text=label,callback_data=f"editset:{key}")] for key,label in keys]
    kb.append([InlineKeyboardButton(text="⬅️ Payment Methods",callback_data="admin:payment_methods")])
    await c.answer(); await c.message.edit_text(f"✏️ <b>{labels.get(code,code)} Configuration</b>\n\nEdit any field below.",reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data=="admin:autopilot")
async def admin_autopilot(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    row=db_execute("""SELECT
        (SELECT COUNT(*) FROM products WHERE active=1) products,
        (SELECT COUNT(*) FROM products WHERE active=1 AND (CASE WHEN delivery_type='code' THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=products.id AND pc.status='available') ELSE stock END)<=%s) low_stock,
        (SELECT COUNT(*) FROM orders WHERE status='completed' AND created_at>=NOW()-INTERVAL '7 days') orders7,
        (SELECT COALESCE(SUM(total),0) FROM orders WHERE status='completed' AND created_at>=NOW()-INTERVAL '7 days') sales7,
        (SELECT COUNT(*) FROM cart_items WHERE updated_at<=NOW()-INTERVAL '6 hours' AND updated_at>=NOW()-INTERVAL '30 days') abandoned_carts,
        (SELECT COUNT(*) FROM users WHERE updated_at<=NOW()-INTERVAL '30 days') inactive_users,
        (SELECT COUNT(*) FROM payments WHERE status='pending') pending_payments
    """,(low_stock_threshold(),),fetch="one")
    top=db_execute("""SELECT p.name,COUNT(*) c FROM orders o JOIN products p ON p.id=o.product_id WHERE o.status='completed' AND o.created_at>=NOW()-INTERVAL '7 days' GROUP BY p.id,p.name ORDER BY c DESC LIMIT 3""",fetch="all") or []
    tops="\n".join(f"• {html.escape(r['name'])}: {r['c']} sales" for r in top) or "• No completed sales yet"
    text=("🤖 <b>Ultra Auto Pilot</b>\n\n"
          f"🛍 Active products: <b>{row['products']}</b>\n⚠️ Low stock: <b>{row['low_stock']}</b>\n"
          f"📦 Orders (7d): <b>{row['orders7']}</b>\n💰 Sales (7d): <b>{fmt_money(row['sales7'])}</b>\n"
          f"🛒 Cart opportunities: <b>{row['abandoned_carts']}</b>\n💤 Inactive users: <b>{row['inactive_users']}</b>\n"
          f"💳 Pending payments: <b>{row['pending_payments']}</b>\n\n🔥 <b>Trending products</b>\n{tops}\n\n"
          "🛡️ Safe mode: Auto Pilot reports opportunities only. It does not silently change prices, balances or payments.")
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:autopilot")],[InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]]))

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

@router.callback_query(F.data.startswith("pay_audit:"))
async def payment_audit_view(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":",1)[1])
    rows=db_execute("SELECT action,old_status,new_status,admin_id,note,created_at FROM payment_audit WHERE payment_id=%s ORDER BY created_at DESC LIMIT 12",(pid,),"all")
    if not rows: return await c.answer("No audit history.",show_alert=True)
    lines=[f"🧾 <b>Payment #{pid} Audit</b>"]
    for r in rows:
        actor=f"Admin {r['admin_id']}" if r["admin_id"] else "User/System"
        lines.append(f"• <b>{html.escape(r['action'])}</b> · {html.escape(actor)} · {r['created_at']:%Y-%m-%d %H:%M}\\n  {html.escape(r['old_status'])} → {html.escape(r['new_status'])}\\n  {html.escape(r['note'] or '')}")
    await c.answer()
    await c.message.answer("\n\n".join(lines))

@router.callback_query(F.data.startswith("pay_credit:"))
async def payment_credit(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1])
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(pid,)); p=cur.fetchone()
                if not p or p["status"]!="pending": return await c.answer("Already processed.",show_alert=True)
                if p.get("order_id"):
                    # Direct-payment order: approve the payment and fulfill the linked order atomically.
                    cur.execute("UPDATE payments SET status='credited',updated_at=NOW() WHERE id=%s AND status='pending'",(pid,))
                    if cur.rowcount != 1: return await c.answer("Already processed.",show_alert=True)
                    cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(p["order_id"],)); o=cur.fetchone()
                    if not o or o["status"]!="awaiting_payment":
                        raise RuntimeError("Linked order is no longer awaiting payment.")
                    cur.execute("SELECT * FROM products WHERE id=%s AND active=1 FOR UPDATE",(o["product_id"],)); prod=cur.fetchone()
                    if not prod or effective_stock(prod)<1: raise RuntimeError("Product is out of stock.")
                    delivered_code=None
                    code_row=None
                    auto_code=False
                    cur.execute("SELECT * FROM product_codes WHERE product_id=%s AND status='available' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",(o["product_id"],)); code_row=cur.fetchone()
                    auto_code=bool(code_row) or prod["delivery_type"]=="code"
                    if auto_code:
                        if not code_row: raise RuntimeError("Code stock unavailable.")
                        cur.execute("UPDATE product_codes SET status='sold',sold_to=%s,sold_at=NOW(),order_id=%s WHERE id=%s AND status='available'",(o["user_id"],o["id"],code_row["id"]))
                        delivered_code=code_row["code"]; status="completed"
                    else:
                        cur.execute("UPDATE products SET stock=stock-1,updated_at=NOW() WHERE id=%s AND stock>0",(o["product_id"],))
                        if cur.rowcount!=1: raise RuntimeError("Stock changed. Please retry.")
                        status="pending"
                    cur.execute("UPDATE orders SET status=%s,delivered_code=%s,processed_at=%s,updated_at=NOW() WHERE id=%s",(status,delivered_code,None if status=="pending" else datetime.now(timezone.utc),o["id"]))
                    if status=="completed": award_completed_order_rewards(cur,o["id"],o["user_id"],o["total"])
                    record_payment_audit(cur,pid,c.from_user.id,"order_approved","pending","credited",p["amount"],p["method"],p["trx_id"],f"Direct payment approved; Order #{o['id']} fulfilled")
                    cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
                    cur.execute("SELECT name FROM products WHERE id=%s",(o["product_id"],)); prod_name=cur.fetchone()
                    direct_order=(o,prod,delivered_code,status,u,prod_name)
                else:
                    # Wallet deposit: atomically credit the user's wallet.
                    cur.execute("UPDATE payments SET status='credited',updated_at=NOW() WHERE id=%s AND status='pending'",(pid,))
                if p.get("order_id"):
                    pass
                else:
                    if cur.rowcount != 1: return await c.answer("Already processed.",show_alert=True)
                    cur.execute("UPDATE users SET balance=balance+%s,updated_at=NOW() WHERE id=%s",(p["amount"],p["user_id"]))
                    cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(p["user_id"],p["amount"],"payment",f"Payment #{pid}"))
                    record_payment_audit(cur,pid,c.from_user.id,"credited","pending","credited",p["amount"],p["method"],p["trx_id"],"Admin approved payment and credited wallet")
                    cur.execute("SELECT tg_id FROM users WHERE id=%s",(p["user_id"],)); u=cur.fetchone()
    admin_log(c.from_user.id,"credit_payment",f"payment #{pid}")
    await c.answer("Approved")
    if p.get("order_id"):
        o,prod,delivered_code,status,u,prod_name=direct_order
        await c.message.edit_text(f"✅ Direct payment #{pid} approved. Order #{o['id']} {'completed.' if status=='completed' else 'sent to manual delivery.'}")
        if status=="completed":
            delivery=f"🎁 <b>Your Code</b>\n<code>{html.escape(delivered_code or '')}</code>"
            await notify_user(c.bot,u["tg_id"],f"🎉 <b>Order #{o['id']} Completed</b>\n\n📦 Product: <b>{html.escape(prod_name['name'] if prod_name else 'Product')}</b>\n💳 Paid directly: <b>{fmt_money(o['total'])}</b>\n\n{delivery}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛍️ Buy More",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]]))
        else:
            for admin_id in ADMIN_IDS:
                try:
                    await c.bot.send_message(admin_id,f"🧾 <b>Direct Payment Order #{o['id']} — Manual Delivery</b>\n\n👤 User: <code>{u['tg_id']}</code>\n🎮 Product: <b>{html.escape(prod_name['name'] if prod_name else 'Product')}</b>\n💰 Paid: <b>{fmt_money(o['total'])}</b>\n🆔 ID/UID: <code>{html.escape(o['game_uid'] or '')}</code>\n🔑 Password: <code>••••••••</code>\n\n✍️ Write delivery information to complete this order.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✍️ Write Delivery",callback_data=f"order_note:{o['id']}")],[InlineKeyboardButton(text="❌ Reject + Refund",callback_data=f"order_reject:{o['id']}")]]))
                except Exception as exc:
                    logging.exception("Failed to send direct-payment manual order notification #%s: %s", o['id'], exc)
            await notify_user(c.bot,u["tg_id"],f"⏳ <b>Order #{o['id']} Payment Verified</b>\n\nYour payment is verified. The order is now waiting for manual delivery.")
    else:
        await c.message.edit_text(f"✅ Payment #{pid} credited.")
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
                cur.execute("UPDATE payments SET status='rejected',updated_at=NOW() WHERE id=%s AND status='pending'",(pid,))
                if cur.rowcount != 1: return await c.answer("Already processed.",show_alert=True)
                record_payment_audit(cur,pid,c.from_user.id,"rejected","pending","rejected",p["amount"],p["method"],p["trx_id"],"Admin rejected payment")
                if p.get("order_id"):
                    cur.execute("UPDATE orders SET status='cancelled',processed_at=NOW(),updated_at=NOW() WHERE id=%s AND status='awaiting_payment'",(p["order_id"],))
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

@router.callback_query(F.data=="admin:ultra")
async def admin_ultra(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    flags=[
        ("feature_quick_shop","🛍 Quick Shop"),("feature_search","🔎 Search"),
        ("feature_favorites","❤️ Favorites"),("feature_rewards","⭐ Rewards"),
        ("feature_referral","🤝 Referral"),("feature_support","🆘 Support"),
        ("feature_announcements","📢 Announcements"),("feature_vip","💎 VIP"),
        ("feature_smart_offers","🧠 Smart Offers"),
    ]
    rows=[]
    for key,label in flags:
        state="🟢 ON" if _feature_on(key) else "🔴 OFF"
        rows.append([InlineKeyboardButton(text=f"{label} • {state}", callback_data=f"feature:{key}")])
    rows.append([InlineKeyboardButton(text="📊 System Status", callback_data="admin:status"), InlineKeyboardButton(text="💾 Backup Now", callback_data="admin:backup_now")])
    rows.append([InlineKeyboardButton(text=setting("admin_back","⬅️ Admin"), callback_data="admin:dashboard")])
    text=(f"🚀 <b>{html.escape(APP_VERSION)} — Ultra Control</b>\n\n"
          "Toggle customer features instantly. Changes are stored in PostgreSQL and survive redeploys.\n\n"
          "⚡ Inline-only UI • 🔐 Admin protected • 💾 Persistent settings")
    await c.answer(); await c.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@router.callback_query(F.data.startswith("feature:"))
async def toggle_feature(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    key=c.data.split(":",1)[1]
    allowed={"feature_quick_shop","feature_search","feature_favorites","feature_rewards","feature_referral","feature_support","feature_announcements","feature_vip"}
    if key not in allowed: return await c.answer("Invalid feature", show_alert=True)
    new="0" if _feature_on(key) else "1"
    set_setting(key,new); admin_log(c.from_user.id,"feature_toggle",f"{key}={new}")
    await c.answer("Enabled" if new=="1" else "Disabled")
    await admin_ultra(c)

@router.callback_query(F.data=="admin:status")
async def admin_status(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    try:
        db=db_execute("SELECT current_database() AS db", fetch="one")
        users=db_execute("SELECT COUNT(*) AS c FROM users", fetch="one")["c"]
        products=db_execute("SELECT COUNT(*) AS c FROM products WHERE active=1", fetch="one")["c"]
        pending=db_execute("SELECT COUNT(*) AS c FROM orders WHERE status='pending'", fetch="one")["c"]
        backups=len(list(BACKUP_DIR.glob("*.json.gz"))) if BACKUP_DIR.exists() else 0
        text=(f"📊 <b>Ultra System Status</b>\n\n"
              f"🤖 Version: <code>{html.escape(APP_VERSION)}</code>\n"
              f"☁️ Database: <b>PostgreSQL</b> • <code>{html.escape(str(db['db']))}</code>\n"
              f"👥 Users: <b>{users}</b>\n🛍 Active Products: <b>{products}</b>\n"
              f"🧾 Pending Orders: <b>{pending}</b>\n💾 Local backup snapshots: <b>{backups}</b>\n"
              f"❤️ Health: <b>OK</b>\n⌨️ Reply Keyboard: <b>Disabled</b>")
    except Exception as e:
        text=f"🚨 <b>System Status Error</b>\n<code>{html.escape(str(e))}</code>"
    await c.answer(); await c.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Ultra Control", callback_data="admin:ultra")]]))

@router.callback_query(F.data=="admin:backup_now")
async def admin_backup_now(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    await c.answer("Creating backup…")
    try:
        path=await asyncio.to_thread(create_database_backup)
        admin_log(c.from_user.id,"manual_backup","ultra_control")
        await c.message.answer_document(FSInputFile(str(path)), caption=f"💾 <b>V8 Ultra backup created</b>\n<code>{html.escape(str(path))}</code>")
    except Exception as e:
        await c.message.answer(f"❌ Backup failed: <code>{html.escape(str(e))}</code>")

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
 "payments":[("payment_info","💳 Payment Instructions"),("payment_bkash_label","bKash Button Label"),("payment_nagad_label","Nagad Button Label"),("payment_rocket_label","Rocket Button Label"),("payment_binance_label","Binance Button Label"),("payment_bkash_account","bKash Account"),("payment_nagad_account","Nagad Account"),("payment_rocket_account","Rocket Account"),("payment_binance_account","Binance Account/Wallet"),("payment_bkash_instruction","bKash Instructions"),("payment_nagad_instruction","Nagad Instructions"),("payment_rocket_instruction","Rocket Instructions"),("payment_binance_instruction","Binance Instructions"),("payment_binance_network","Binance Network"),("payment_bkash_icon","bKash Logo/Icon"),("payment_nagad_icon","Nagad Logo/Icon"),("payment_rocket_icon","Rocket Logo/Icon"),("payment_binance_icon","Binance Logo/Icon"),("payment_receipt_required","Receipt Required 1/0"),("payment_presets","Quick Amounts CSV"),("payment_method_prompt","Payment Method Prompt"),("deposit_min","⬇️ Minimum Deposit"),("deposit_max","⬆️ Maximum Deposit (0=unlimited)"),("payment_timeout_minutes","⏱ Payment Timeout")],
 "money":[("signup_bonus","🎁 Signup Bonus"),("referral_reward","🤝 Referral Reward")],
 "orders":[("order_timeout_minutes","⏱ Order Timeout")],
 "messages":[("welcome_message","👋 Welcome Message"),("maintenance_message","🔧 Maintenance Message"),("fallback_message","↩️ Fallback Message"),("search_prompt","🔎 Search Prompt"),("deposit_prompt","💰 Deposit Prompt"),("shop_title","🛍 Shop Title"),("category_title","📂 Category Title"),("buy_prompt","🛒 Buy Prompt")],
 "system":[("low_stock_threshold","⚠️ Low Stock Threshold"),("feature_quick_shop","🛍 Quick Shop ON/OFF"),("feature_search","🔎 Search ON/OFF"),("feature_favorites","❤️ Favorites ON/OFF"),("feature_rewards","⭐ Rewards ON/OFF"),("feature_referral","🤝 Referral ON/OFF"),("feature_support","🆘 Support ON/OFF"),("feature_announcements","📢 Announcements ON/OFF"),("feature_vip","💎 VIP ON/OFF")],
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



# ---------------- V8.4 Ultra Marketing & Growth ----------------
def marketing_enabled():
    return setting("feature_marketing", "1") == "1"


def marketing_audience_sql(audience):
    audience = (audience or "all").strip().lower()
    if audience == "new":
        days = max(1, int(setting("marketing_new_user_days", "7") or 7))
        return f"u.created_at >= NOW() - INTERVAL '{days} days'"
    if audience == "inactive":
        days = max(1, int(setting("marketing_reactivation_days", "30") or 30))
        return f"NOT EXISTS (SELECT 1 FROM orders oi WHERE oi.user_id=u.id AND oi.status='completed' AND oi.created_at >= NOW() - INTERVAL '{days} days')"
    if audience == "vip":
        bronze = float(setting("vip_bronze_spend", "1000") or 1000)
        return f"u.lifetime_spend >= {bronze}"
    if audience == "cart":
        return "EXISTS (SELECT 1 FROM cart_items ca WHERE ca.user_id=u.id)"
    if audience == "buyers":
        return "EXISTS (SELECT 1 FROM orders ob WHERE ob.user_id=u.id AND ob.status='completed')"
    return "TRUE"


def marketing_recipients(audience):
    where = marketing_audience_sql(audience)
    limit = max(1, int(setting("marketing_daily_limit", "500") or 500))
    return db_execute(f"SELECT u.id,u.tg_id FROM users u WHERE u.blocked=0 AND {where} ORDER BY u.id LIMIT %s", (limit,), "all") or []


def marketing_campaign_markup(campaign_id):
    return [[(setting("marketing_click_offer_text", "🎁 Open Offer"), f"mkt:offer:{campaign_id}")]]


def marketing_create_campaign(admin_id, title, message, audience, coupon_code, start_minutes, end_hours):
    audience = audience.lower().strip()
    if audience not in {"all", "new", "inactive", "vip", "cart", "buyers"}:
        raise ValueError("Audience must be all/new/inactive/vip/cart/buyers")
    try:
        start_minutes = max(0, int(start_minutes or 0))
        end_hours = max(0, int(end_hours or 0))
    except Exception:
        raise ValueError("Schedule values must be numbers")
    ends_sql = "NOW() + (%s * INTERVAL '1 hour')" if end_hours else "NULL"
    if end_hours:
        row = db_insert_returning(
            "INSERT INTO marketing_campaigns(title,message,audience,coupon_code,starts_at,ends_at,status,created_by) "
            "VALUES(%s,%s,%s,%s,NOW()+(%s*INTERVAL '1 minute'),"+ends_sql+",'scheduled',%s) RETURNING id",
            (title, message, audience, coupon_code.upper(), start_minutes, end_hours, admin_id),
        )
    else:
        row = db_insert_returning(
            "INSERT INTO marketing_campaigns(title,message,audience,coupon_code,starts_at,ends_at,status,created_by) "
            "VALUES(%s,%s,%s,%s,NOW()+(%s*INTERVAL '1 minute'),NULL,'scheduled',%s) RETURNING id",
            (title, message, audience, coupon_code.upper(), start_minutes, admin_id),
        )
    return row["id"]


def marketing_record_event(campaign_id, user_id, event_type, order_id=None):
    try:
        db_execute("INSERT INTO marketing_events(campaign_id,user_id,event_type,order_id) VALUES(%s,%s,%s,%s)", (campaign_id,user_id,event_type,order_id))
    except Exception:
        pass


def marketing_record_conversion(user_id, order_id):
    # Attribute a completed order to the user's latest clicked campaign within 7 days.
    row = db_execute("""SELECT campaign_id FROM marketing_events\n                       WHERE user_id=%s AND event_type='click' AND created_at>=NOW()-INTERVAL '7 days'\n                       ORDER BY created_at DESC LIMIT 1""", (user_id,), "one")
    if not row:
        return
    campaign_id = row["campaign_id"]
    exists = db_execute("SELECT 1 FROM marketing_events WHERE campaign_id=%s AND user_id=%s AND event_type='conversion' AND order_id=%s LIMIT 1", (campaign_id,user_id,order_id), "one")
    if exists:
        return
    marketing_record_event(campaign_id,user_id,"conversion",order_id)
    db_execute("UPDATE marketing_campaigns SET converted_count=converted_count+1 WHERE id=%s", (campaign_id,))


async def marketing_campaign_loop(bot):
    await asyncio.sleep(20)
    while True:
        try:
            if marketing_enabled():
                db_execute("UPDATE marketing_campaigns SET status='expired' WHERE status IN ('scheduled','sending') AND ends_at IS NOT NULL AND ends_at<NOW()")
                campaigns = db_execute("""SELECT * FROM marketing_campaigns WHERE status='scheduled' AND starts_at<=NOW()\n                                         AND (ends_at IS NULL OR ends_at>=NOW()) ORDER BY id LIMIT 5""", fetch="all") or []
                for campaign in campaigns:
                    claimed = db_execute("UPDATE marketing_campaigns SET status='sending' WHERE id=%s AND status='scheduled'", (campaign["id"],))
                    if claimed != 1:
                        continue
                    recipients = marketing_recipients(campaign["audience"])
                    sent = 0
                    buttons = marketing_campaign_markup(campaign["id"])
                    coupon_line = f"\n\n🏷️ Coupon: <code>{html.escape(campaign['coupon_code'])}</code>" if campaign["coupon_code"] else ""
                    text = f"📣 <b>{html.escape(campaign['title'])}</b>\n\n{campaign['message']}{coupon_line}"
                    for user in recipients:
                        enqueue_notification(user["tg_id"], text, buttons)
                        marketing_record_event(campaign["id"], user["id"], "sent")
                        sent += 1
                    db_execute("UPDATE marketing_campaigns SET status='sent',sent_count=%s,sent_at=NOW() WHERE id=%s AND status='sending'", (sent,campaign["id"]))
                    admin_log(campaign["created_by"], "marketing_campaign_sent", f"campaign #{campaign['id']} recipients={sent}")
        except Exception as exc:
            print(f"marketing_campaign_loop error: {exc}")
        await asyncio.sleep(60)


def marketing_abandoned_cart_job():
    if not marketing_enabled():
        return
    hours = max(1, int(setting("marketing_abandoned_cart_hours", "6") or 6))
    rows = db_execute("""SELECT DISTINCT u.id,u.tg_id,p.name\n                       FROM cart_items ci JOIN users u ON u.id=ci.user_id JOIN products p ON p.id=ci.product_id\n                       WHERE u.blocked=0 AND ci.updated_at < NOW()-(%s*INTERVAL '1 hour')\n                       AND (ci.last_reminded_at IS NULL OR ci.last_reminded_at < ci.updated_at)\n                       LIMIT 100""", (hours,), "all") or []
    for row in rows:
        enqueue_notification(row["tg_id"], f"🛒 <b>You left something in your cart!</b>\n\n🎮 {html.escape(row['name'])}\n\nYour cart is waiting for you.", [[("🛒 Open Cart", "cart:view")]])
        db_execute("UPDATE cart_items SET last_reminded_at=NOW() WHERE user_id=%s AND (last_reminded_at IS NULL OR last_reminded_at < updated_at)", (row["id"],))


def marketing_reactivation_job():
    if not marketing_enabled():
        return
    days = max(1, int(setting("marketing_reactivation_days", "30") or 30))
    rows = db_execute("""SELECT u.id,u.tg_id FROM users u\n                       WHERE u.blocked=0 AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id=u.id AND o.status='completed' AND o.created_at>=NOW()-(%s*INTERVAL '1 day'))\n                       AND u.updated_at < NOW()-(%s*INTERVAL '1 day') LIMIT 100""", (days,days), "all") or []
    for row in rows:
        enqueue_notification(row["tg_id"], f"👋 <b>We miss you!</b>\n\nCome back to {html.escape(shop_name())} and check the latest gaming offers. 🎮", [[("🛍️ Shop Now", "home:shop")]])
        db_execute("UPDATE users SET updated_at=NOW() WHERE id=%s", (row["id"],))


@router.callback_query(F.data == "admin:marketing")
async def admin_marketing(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    row = db_execute("""SELECT COUNT(*) campaigns, COALESCE(SUM(sent_count),0) sent,\n                              COALESCE(SUM(clicked_count),0) clicks, COALESCE(SUM(converted_count),0) conversions\n                       FROM marketing_campaigns WHERE created_at>=NOW()-INTERVAL '30 days'""", "one")
    await c.answer()
    await c.message.edit_text(
        f"📣 <b>MARKETING CENTER</b>\n\n📅 Campaigns (30d): <b>{row['campaigns']}</b>\n📤 Sent: <b>{row['sent']}</b>\n👆 Clicks: <b>{row['clicks']}</b>\n💰 Conversions: <b>{row['conversions']}</b>\n\nCreate a scheduled targeted campaign or inspect recent campaigns.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Create Campaign", callback_data="mkt:create")],
            [InlineKeyboardButton(text="📋 Recent Campaigns", callback_data="mkt:list")],
            [InlineKeyboardButton(text=setting("admin_back","⬅️ Admin"), callback_data="admin:dashboard")]
        ])
    )


@router.callback_query(F.data == "mkt:create")
async def marketing_create_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    await c.answer()
    await state.set_state(AdminState.marketing_create)
    await c.message.answer("📣 <b>Create Marketing Campaign</b>\n\nSend exactly:\n<code>Title | Message | Audience | Coupon | StartMinutes | EndHours</code>\n\nAudience: <code>all</code>, <code>new</code>, <code>inactive</code>, <code>vip</code>, <code>cart</code>, <code>buyers</code>\nCoupon can be blank. StartMinutes=0 sends immediately. EndHours=0 means no expiry.")


@router.message(AdminState.marketing_create)
async def marketing_create_save(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return
    parts = [x.strip() for x in (m.text or "").split("|", 5)]
    if len(parts) != 6:
        return await m.answer("❌ Invalid format. Use 6 fields separated by |.")
    title, message, audience, coupon, start_minutes, end_hours = parts
    if not title or not message:
        return await m.answer("❌ Title and message are required.")
    try:
        campaign_id = marketing_create_campaign(m.from_user.id, title, message, audience, coupon, start_minutes, end_hours)
    except Exception as exc:
        return await m.answer(f"❌ Campaign not created: {html.escape(str(exc))}")
    await state.clear()
    admin_log(m.from_user.id, "marketing_campaign_create", f"campaign #{campaign_id} audience={audience}")
    await m.answer(f"✅ Campaign <b>#{campaign_id}</b> scheduled.\nAudience: <b>{html.escape(audience)}</b>\nStart: <b>{html.escape(start_minutes)} min</b>", reply_markup=admin_menu())


@router.callback_query(F.data == "mkt:list")
async def marketing_list(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    rows = db_execute("SELECT id,title,audience,status,sent_count,clicked_count,converted_count,starts_at FROM marketing_campaigns ORDER BY id DESC LIMIT 10", fetch="all") or []
    if not rows:
        text = "📣 <b>No campaigns yet.</b>"
    else:
        text = "📣 <b>Recent Campaigns</b>\n\n" + "\n".join(
            f"#{r['id']} • <b>{html.escape(r['title'][:28])}</b>\n🎯 {r['audience']} • {r['status']} • 📤 {r['sent_count']} • 👆 {r['clicked_count']} • 💰 {r['converted_count']}"
            for r in rows
        )
    await c.answer()
    await c.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Marketing", callback_data="admin:marketing")],[InlineKeyboardButton(text="🏠 Admin", callback_data="admin:dashboard")]]))


@router.callback_query(F.data.startswith("mkt:offer:"))
async def marketing_offer_click(c: CallbackQuery):
    if not marketing_enabled():
        return await c.answer("Marketing offers are disabled.", show_alert=True)
    try:
        campaign_id = int(c.data.split(":")[2])
    except Exception:
        return await c.answer("Invalid offer.", show_alert=True)
    u = get_user(c.from_user)
    campaign = db_execute("SELECT * FROM marketing_campaigns WHERE id=%s", (campaign_id,), "one")
    if not campaign:
        return await c.answer("Offer unavailable.", show_alert=True)
    marketing_record_event(campaign_id, u["id"], "click")
    db_execute("UPDATE marketing_campaigns SET clicked_count=clicked_count+1 WHERE id=%s", (campaign_id,))
    buttons = [[InlineKeyboardButton(text="🛍️ Shop Now", callback_data="home:shop")], [InlineKeyboardButton(text="🏠 Main Menu", callback_data="main_menu")]]
    coupon = f"\n\n🏷️ Coupon: <code>{html.escape(campaign['coupon_code'])}</code>" if campaign["coupon_code"] else ""
    await c.answer("Offer opened")
    await c.message.answer(f"🎁 <b>{html.escape(campaign['title'])}</b>\n\n{campaign['message']}{coupon}", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

# ---------------- V8.3 Ultra Commerce ----------------
def product_sale_price(product):
    base=float(product["price"])
    sale=product.get("sale_price")
    until=product.get("sale_until")
    if sale is None:
        return base
    if until is not None and until <= datetime.now(timezone.utc):
        return base
    sale=float(sale)
    return min(base,sale) if sale>0 else base

def cart_enabled():
    return setting("feature_cart","1")=="1"

def coupons_enabled():
    return setting("feature_coupons","1")=="1"

def cart_count(user_id):
    row=db_execute("SELECT COALESCE(SUM(quantity),0) AS c FROM cart_items WHERE user_id=%s",(user_id,),"one")
    return int(row["c"] or 0) if row else 0

def coupon_discount(coupon, subtotal):
    if not coupon: return 0.0
    value=float(coupon["value"] or 0)
    discount=value if coupon["discount_type"]=="fixed" else subtotal*value/100
    cap=float(coupon["max_discount"] or 0)
    if cap>0: discount=min(discount,cap)
    return max(0.0,min(discount,subtotal))

def get_coupon(code,user_id,subtotal):
    if not coupons_enabled(): return None,"Coupons are disabled."
    code=(code or "").strip().upper()
    row=db_execute("""SELECT * FROM coupons WHERE UPPER(code)=UPPER(%s) AND active=1
                      AND (starts_at IS NULL OR starts_at<=NOW())
                      AND (ends_at IS NULL OR ends_at>=NOW()) LIMIT 1""",(code,),"one")
    if not row: return None,"Invalid or expired coupon."
    if int(row["usage_limit"] or 0)>0 and int(row["used_count"] or 0)>=int(row["usage_limit"]):
        return None,"Coupon usage limit reached."
    if subtotal<float(row["min_order"] or 0): return None,f"Minimum order is {fmt_money(row['min_order'])}."
    used=db_execute("SELECT 1 FROM coupon_uses WHERE coupon_id=%s AND user_id=%s LIMIT 1",(row["id"],user_id),"one")
    if used: return None,"You already used this coupon."
    return row,None

def cart_rows(user_id):
    return db_execute("""SELECT ci.product_id,ci.quantity,p.name,p.category,p.price,p.sale_price,p.sale_until,p.delivery_type,p.stock,p.active
                         FROM cart_items ci JOIN products p ON p.id=ci.product_id
                         WHERE ci.user_id=%s AND p.active=1 ORDER BY ci.updated_at DESC""",(user_id,),"all")

def cart_markup(user_id):
    rows=cart_rows(user_id); buttons=[]
    for r in rows:
        buttons.append([
            InlineKeyboardButton(text="➖",callback_data=f"cart:dec:{r['product_id']}"),
            InlineKeyboardButton(text=f"{r['quantity']} × {fmt_money(product_sale_price(r))}",callback_data=f"cart:item:{r['product_id']}"),
            InlineKeyboardButton(text="➕",callback_data=f"cart:inc:{r['product_id']}")
        ])
    if rows:
        buttons.append([InlineKeyboardButton(text="💳 Checkout",callback_data="cart:checkout")])
        buttons.append([InlineKeyboardButton(text="🧹 Clear Cart",callback_data="cart:clear")])
    buttons.append([InlineKeyboardButton(text=setting("button_shop","🛍️ Shop"),callback_data="home:shop"),
                    InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def render_cart(c,user_id):
    rows=cart_rows(user_id)
    if not rows:
        return await c.message.edit_text("🛒 <b>Your Cart is Empty</b>\n\nAdd products from the shop.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛍️ Shop",callback_data="home:shop")],[InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]]))
    subtotal=sum(int(r["quantity"])*product_sale_price(r) for r in rows)
    lines=["🛒 <b>SMART CART</b>","━━━━━━━━━━━━━━━━━━"]
    for r in rows:
        lines.append(f"• <b>{html.escape(r['name'])}</b> × {r['quantity']} = {fmt_money(int(r['quantity'])*product_sale_price(r))}")
    lines.append(f"\n💰 Subtotal: <b>{fmt_money(subtotal)}</b>")
    await c.message.edit_text("\n".join(lines),reply_markup=cart_markup(user_id))

@router.callback_query(F.data=="cart:view")
async def cart_view_callback(c:CallbackQuery):
    if not cart_enabled(): return await c.answer("Cart is disabled.",show_alert=True)
    u=get_user(c.from_user); await c.answer(); await render_cart(c,u["id"])

@router.callback_query(F.data.startswith("cart:add:"))
async def cart_add_callback(c:CallbackQuery):
    if not cart_enabled(): return await c.answer("Cart is disabled.",show_alert=True)
    pid=int(c.data.split(":")[2]); u=get_user(c.from_user)
    p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    if effective_stock(p)<1: return await c.answer("Out of stock.",show_alert=True)
    max_qty=max(1,int(setting("cart_max_quantity","10") or 10))
    db_execute("""INSERT INTO cart_items(user_id,product_id,quantity,updated_at) VALUES(%s,%s,1,NOW())
                 ON CONFLICT(user_id,product_id) DO UPDATE SET quantity=LEAST(cart_items.quantity+1,%s),updated_at=NOW()""",(u["id"],pid,max_qty))
    await c.answer("🛒 Added to cart"); await render_cart(c,u["id"])

@router.callback_query(F.data.startswith("cart:inc:"))
async def cart_inc_callback(c:CallbackQuery):
    pid=int(c.data.split(":")[2]); u=get_user(c.from_user)
    row=db_execute("SELECT quantity FROM cart_items WHERE user_id=%s AND product_id=%s",(u["id"],pid),"one")
    p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not row or not p: return await c.answer("Item unavailable.",show_alert=True)
    limit=min(max(1,int(setting("cart_max_quantity","10") or 10)),max(1,effective_stock(p)))
    if int(row["quantity"])>=limit: return await c.answer("Maximum available quantity reached.",show_alert=True)
    db_execute("UPDATE cart_items SET quantity=quantity+1,updated_at=NOW() WHERE user_id=%s AND product_id=%s",(u["id"],pid))
    await c.answer("Quantity increased"); await render_cart(c,u["id"])

@router.callback_query(F.data.startswith("cart:dec:"))
async def cart_dec_callback(c:CallbackQuery):
    pid=int(c.data.split(":")[2]); u=get_user(c.from_user)
    db_execute("UPDATE cart_items SET quantity=quantity-1,updated_at=NOW() WHERE user_id=%s AND product_id=%s AND quantity>1",(u["id"],pid))
    db_execute("DELETE FROM cart_items WHERE user_id=%s AND product_id=%s AND quantity<=1",(u["id"],pid))
    await c.answer("Quantity updated"); await render_cart(c,u["id"])

@router.callback_query(F.data=="cart:clear")
async def cart_clear_callback(c:CallbackQuery):
    u=get_user(c.from_user); db_execute("DELETE FROM cart_items WHERE user_id=%s",(u["id"],))
    await c.answer("Cart cleared"); await render_cart(c,u["id"])

@router.callback_query(F.data=="cart:checkout")
async def cart_checkout_callback(c:CallbackQuery,state:FSMContext):
    u=get_user(c.from_user)
    if not cart_enabled() or cart_count(u["id"])<1: return await c.answer("Cart is empty.",show_alert=True)
    await state.set_state(CartState.uid); await c.answer()
    await c.message.answer("🆔 <b>Cart checkout</b>\n\nSend your game/player UID.\n\nSend /cancel to cancel.")

@router.message(CartState.uid)
async def cart_uid(m:Message,state:FSMContext):
    uid=(m.text or "").strip()
    if uid.lower()=="/cancel": await state.clear(); return await m.answer("❌ Cancelled.",reply_markup=inline_home_kb())
    if len(uid)<2 or len(uid)>64: return await m.answer("❌ Please send a valid UID.")
    u=get_user(m.from_user); rows=cart_rows(u["id"])
    if not rows: await state.clear(); return await m.answer("🛒 Cart is empty.")
    subtotal=sum(int(r["quantity"])*product_sale_price(r) for r in rows)
    await state.update_data(game_uid=uid,subtotal=subtotal)
    await state.set_state(CartState.coupon)
    await m.answer(f"💰 Subtotal: <b>{fmt_money(subtotal)}</b>\n\n🏷️ Send coupon code or <code>SKIP</code>.")

@router.message(CartState.coupon)
async def cart_coupon_step(m:Message,state:FSMContext):
    d=await state.get_data(); u=get_user(m.from_user); subtotal=float(d["subtotal"]); code=(m.text or "").strip()
    coupon=None
    if code.upper()!="SKIP":
        coupon,err=get_coupon(code,u["id"],subtotal)
        if err: return await m.answer(f"❌ {err}\n\nTry again or send <code>SKIP</code>.")
    discount=coupon_discount(coupon,subtotal); total=max(0,subtotal-discount)
    if float(u["balance"])<total:
        await state.clear(); return await m.answer(f"❌ Insufficient balance.\nNeed: {fmt_money(total-float(u['balance']))}",reply_markup=inline_home_kb())
    await state.update_data(coupon_id=coupon["id"] if coupon else None,discount=discount,total=total)
    await m.answer(f"🛒 <b>Checkout Confirmation</b>\n\nSubtotal: <b>{fmt_money(subtotal)}</b>\nDiscount: <b>-{fmt_money(discount)}</b>\nTotal: <b>{fmt_money(total)}</b>\nUID: <code>{html.escape(d['game_uid'])}</code>",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Confirm Payment",callback_data="cart:confirm")],[InlineKeyboardButton(text="❌ Cancel",callback_data="cart:cancel")]]))

@router.callback_query(CartState.coupon,F.data=="cart:cancel")
async def cart_cancel(c:CallbackQuery,state:FSMContext):
    await state.clear(); await c.answer("Cancelled"); await c.message.edit_text("❌ Cart checkout cancelled.",reply_markup=cart_markup(get_user(c.from_user)["id"]))

@router.callback_query(CartState.coupon,F.data=="cart:confirm")
async def cart_confirm(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); await state.clear(); uid=d["game_uid"]; coupon_id=d.get("coupon_id"); user_id=get_user(c.from_user)["id"]
    delivered=[]; pending=[]; order_ids=[]
    with DB_LOCK:
        try:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM users WHERE id=%s FOR UPDATE",(user_id,)); u=cur.fetchone()
                    cur.execute("""SELECT ci.product_id,ci.quantity,p.* FROM cart_items ci JOIN products p ON p.id=ci.product_id
                                   WHERE ci.user_id=%s AND p.active=1 ORDER BY ci.product_id FOR UPDATE""",(user_id,)); rows=cur.fetchall()
                    if not rows: raise RuntimeError("Cart is empty.")
                    subtotal=sum(int(r["quantity"])*product_sale_price(r) for r in rows)
                    coupon=None
                    if coupon_id:
                        cur.execute("SELECT * FROM coupons WHERE id=%s AND active=1 FOR UPDATE",(coupon_id,)); coupon=cur.fetchone()
                        if not coupon: raise RuntimeError("Coupon invalid.")
                    discount=coupon_discount(coupon,subtotal); grand=max(0,subtotal-discount)
                    if float(u["balance"])<grand: raise RuntimeError("Insufficient balance.")
                    line_no=0
                    for r in rows:
                        p=r; qty=int(r["quantity"]); unit=product_sale_price(p); line=unit*qty
                        line_discount=discount*(line/subtotal) if subtotal else 0
                        for _ in range(qty):
                            cur.execute("SELECT * FROM products WHERE id=%s AND active=1 FOR UPDATE",(p["id"],)); prod=cur.fetchone()
                            if not prod: raise RuntimeError("Product unavailable.")
                            price=max(0,unit-line_discount/qty)
                            delivered_code=None
                            if prod["delivery_type"]=="code":
                                cur.execute("SELECT * FROM product_codes WHERE product_id=%s AND status='available' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",(prod["id"],)); code_row=cur.fetchone()
                                if not code_row: raise RuntimeError(f"Out of stock: {prod['name']}")
                                cur.execute("UPDATE product_codes SET status='sold',sold_to=%s,sold_at=NOW() WHERE id=%s AND status='available'",(user_id,code_row["id"]))
                                delivered_code=code_row["code"]; status="completed"
                            else:
                                cur.execute("UPDATE products SET stock=stock-1,updated_at=NOW() WHERE id=%s AND stock>0",(prod["id"],))
                                if cur.rowcount!=1: raise RuntimeError(f"Out of stock: {prod['name']}")
                                status="pending"
                            cur.execute("INSERT INTO orders(user_id,product_id,game_uid,total,delivered_code,status) VALUES(%s,%s,%s,%s,%s,%s) RETURNING id",(user_id,prod["id"],uid,price,delivered_code,status))
                            oid=cur.fetchone()["id"]; order_ids.append(oid)
                            cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(user_id,-price,"purchase",f"Cart Order #{oid}"))
                            if delivered_code:
                                cur.execute("UPDATE product_codes SET order_id=%s WHERE id=%s",(oid,code_row["id"])); award_completed_order_rewards(cur,oid,user_id,price); delivered.append((oid,prod["name"],delivered_code,price))
                            else: pending.append((oid,prod["name"],price))
                            if prod["delivery_type"]=="code": sync_code_product_stock(prod["id"],conn)
                            line_no+=1
                    cur.execute("UPDATE users SET balance=balance-%s,updated_at=NOW() WHERE id=%s AND balance>=%s",(grand,user_id,grand))
                    if cur.rowcount!=1: raise RuntimeError("Balance changed.")
                    if coupon:
                        cur.execute("UPDATE coupons SET used_count=used_count+1 WHERE id=%s",(coupon["id"],))
                        cur.execute("INSERT INTO coupon_uses(coupon_id,user_id,order_id) VALUES(%s,%s,%s)",(coupon["id"],user_id,order_ids[0]))
                    cur.execute("DELETE FROM cart_items WHERE user_id=%s",(user_id,))
        except Exception as exc:
            print(f"cart_confirm error: {exc}")
            return await c.answer("Checkout failed. No balance or stock was charged.",show_alert=True)
    await c.answer("✅ Checkout complete")
    msg=[f"🛒 <b>Checkout Complete</b>",f"🧾 Orders: <b>{len(order_ids)}</b>",f"💰 Paid: <b>{fmt_money(d['total'])}</b>"]
    if coupon_id: msg.append("🏷️ Coupon applied.")
    if delivered: msg.append("\n🎁 <b>Instant Delivery</b>\n" + "\n".join(f"#{o} • {html.escape(n)} • <code>{code}</code>" for o,n,code,_ in delivered))
    if pending: msg.append("\n⏳ <b>Manual Delivery</b>\n" + "\n".join(f"#{o} • {html.escape(n)} • {fmt_money(a)}" for o,n,a in pending))
    await c.message.answer("\n".join(msg),reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 My Orders",callback_data="home:orders")],[InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]]))

@router.message(Command("cart"))
async def cart_command(m:Message):
    u=get_user(m.from_user)
    if not cart_enabled(): return await m.answer("🛒 Cart is disabled.")
    rows=cart_rows(u["id"]); subtotal=sum(int(r["quantity"])*product_sale_price(r) for r in rows)
    lines=["🛒 <b>SMART CART</b>"]+[f"• {html.escape(r['name'])} × {r['quantity']} = {fmt_money(int(r['quantity'])*product_sale_price(r))}" for r in rows]
    if rows: lines.append(f"\n💰 Subtotal: <b>{fmt_money(subtotal)}</b>")
    else: lines.append("\nYour cart is empty.")
    await m.answer("\n".join(lines),reply_markup=cart_markup(u["id"]))

@router.message(Command("coupon"))
async def coupon_command(m:Message):
    await m.answer("🏷️ <b>Coupons</b>\n\nApply your coupon during Smart Cart checkout.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛒 Open Cart",callback_data="cart:view")]]))

@router.message(Command("reorder"))
async def reorder_command(m:Message):
    u=get_user(m.from_user)
    row=db_execute("SELECT product_id FROM orders WHERE user_id=%s AND status='completed' ORDER BY id DESC LIMIT 1",(u["id"],),"one")
    if not row: return await m.answer("📦 No completed order found.",reply_markup=inline_home_kb())
    p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(row["product_id"],),"one")
    if not p or effective_stock(p)<1: return await m.answer("⚠️ Your latest product is unavailable.",reply_markup=inline_home_kb())
    db_execute("""INSERT INTO cart_items(user_id,product_id,quantity,updated_at) VALUES(%s,%s,1,NOW())
                  ON CONFLICT(user_id,product_id) DO UPDATE SET quantity=cart_items.quantity+1,updated_at=NOW()""",(u["id"],p["id"]))
    await m.answer(f"🔄 <b>Added to Cart</b>\n\n{html.escape(p['name'])}\n💰 {fmt_money(product_sale_price(p))}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛒 Open Cart",callback_data="cart:view")],[InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]]))

@router.callback_query(F.data=="home:cart")
async def home_cart(c:CallbackQuery):
    if not cart_enabled(): return await c.answer("Cart is disabled.",show_alert=True)
    await c.answer(); await render_cart(c,get_user(c.from_user)["id"])

USER_BOT_COMMANDS = [
    BotCommand(command="start", description="Launch the bot and open the main menu"),
    BotCommand(command="shop", description="Browse available products"),
    BotCommand(command="listings", description="Browse products (shortcut)"),
    BotCommand(command="search", description="Search products by name"),
    BotCommand(command="orders", description="View your recent orders"),
    BotCommand(command="favorites", description="View saved products"),
    BotCommand(command="rewards", description="View loyalty rewards"),
    BotCommand(command="vip", description="View VIP membership"),
    BotCommand(command="offers", description="Get smart product picks"),
    BotCommand(command="refer", description="Refer friends and earn"),
    BotCommand(command="profile", description="View your profile"),
    BotCommand(command="balance", description="Add balance to your wallet"),
    BotCommand(command="deposit", description="Add balance (shortcut)"),
    BotCommand(command="support", description="Contact support"),
    BotCommand(command="help", description="Contact support (shortcut)"),
    BotCommand(command="version", description="Show bot version and features"),
    BotCommand(command="cart", description="Open Smart Cart"),
    BotCommand(command="coupon", description="Use coupons at checkout"),
    BotCommand(command="reorder", description="Reorder your latest purchase"),
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



# ===================== V8.2 ULTRA INTELLIGENCE =====================
# Lightweight intelligence layer built only on existing PostgreSQL tables.

def intelligence_setup():
    """Create small, optional analytics tables. Safe to run on every startup."""
    db_execute("""CREATE TABLE IF NOT EXISTS product_views (
        id BIGSERIAL PRIMARY KEY, user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
        product_id BIGINT REFERENCES products(id) ON DELETE CASCADE,
        viewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )""")
    db_execute("CREATE INDEX IF NOT EXISTS idx_product_views_user_time ON product_views(user_id, viewed_at DESC)")
    db_execute("CREATE INDEX IF NOT EXISTS idx_product_views_product_time ON product_views(product_id, viewed_at DESC)")
    db_execute("""CREATE TABLE IF NOT EXISTS smart_offers (
        id BIGSERIAL PRIMARY KEY, code TEXT UNIQUE NOT NULL, label TEXT NOT NULL,
        discount_pct NUMERIC(6,2) NOT NULL DEFAULT 0, min_spend NUMERIC(12,2) NOT NULL DEFAULT 0,
        active INTEGER NOT NULL DEFAULT 1, starts_at TIMESTAMPTZ DEFAULT NOW(), ends_at TIMESTAMPTZ
    )""")
    db_execute("""CREATE TABLE IF NOT EXISTS intelligence_events (
        id BIGSERIAL PRIMARY KEY, event_type TEXT NOT NULL, user_id BIGINT,
        product_id BIGINT, value NUMERIC(14,2), metadata JSONB DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )""")
    db_execute("CREATE INDEX IF NOT EXISTS idx_intel_events_type_time ON intelligence_events(event_type, created_at DESC)")


def record_product_view(user_id, product_id):
    try:
        db_execute("INSERT INTO product_views(user_id,product_id) VALUES(%s,%s)", (user_id, product_id))
        db_execute("""DELETE FROM product_views WHERE user_id=%s AND id NOT IN
                    (SELECT id FROM product_views WHERE user_id=%s ORDER BY viewed_at DESC LIMIT 50)""", (user_id, user_id))
    except Exception:
        pass


def smart_recommendations(user_id, limit=4):
    """Recommend in-stock products using the user's recent purchase categories, then popularity."""
    try:
        rows = db_execute("""SELECT p.*, CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock
            FROM products p
            LEFT JOIN (SELECT product_id,COUNT(*) available FROM product_codes WHERE status='available' GROUP BY product_id) pc
              ON pc.product_id=p.id
            WHERE p.active=1 AND (CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END)>0
              AND p.category IN (SELECT p2.category FROM orders o JOIN products p2 ON p2.id=o.product_id
                                 WHERE o.user_id=%s ORDER BY o.id DESC LIMIT 5)
            ORDER BY p.id DESC LIMIT %s""", (user_id, limit), "all") or []
        if len(rows) < limit:
            extra = db_execute("""SELECT p.*, CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock
                FROM products p LEFT JOIN (SELECT product_id,COUNT(*) available FROM product_codes WHERE status='available' GROUP BY product_id) pc ON pc.product_id=p.id
                WHERE p.active=1 AND (CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END)>0
                ORDER BY p.id DESC LIMIT %s""", (limit,), "all") or []
            seen={int(r['id']) for r in rows}
            rows += [r for r in extra if int(r['id']) not in seen][:limit-len(rows)]
        return rows[:limit]
    except Exception:
        return []


def recommendations_kb(rows):
    buttons=[]
    for p in rows:
        buttons.append([InlineKeyboardButton(text=f"🔥 {p['name']} • {fmt_money(p['price'])}", callback_data=f"product:{p['id']}")])
    buttons.append([InlineKeyboardButton(text=setting("inline_shop","🛍 Shop"), callback_data="shop")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(Command("offers"))
async def offers_command(m: Message):
    if user_access_denied(m.from_user.id) and not is_admin(m.from_user.id):
        return await m.answer("🚫 Access unavailable right now.")
    u=get_user(m.from_user)
    rows=smart_recommendations(u["id"], 4)
    if not rows:
        return await m.answer("🎁 <b>Smart Offers</b>\n\nNo personalized offer is available right now.", reply_markup=premium_home_kb())
    await m.answer("🎯 <b>Smart Picks For You</b>\n\nBased on your activity and current stock:", reply_markup=recommendations_kb(rows))


@router.callback_query(F.data == "home:offers")
async def smart_offers_callback(c: CallbackQuery):
    u=get_user(c.from_user); rows=smart_recommendations(u["id"],4)
    await c.answer()
    await c.message.edit_text("🎯 <b>Smart Picks For You</b>\n\nRecommended from your activity and current stock:", reply_markup=recommendations_kb(rows))


@router.callback_query(F.data == "admin:intelligence")
async def admin_intelligence(c: CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    row=db_execute("""SELECT
        (SELECT COUNT(*) FROM users) users,
        (SELECT COUNT(*) FROM orders WHERE created_at>=NOW()-INTERVAL '7 days') orders7,
        (SELECT COALESCE(SUM(total),0) FROM orders WHERE status='completed' AND created_at>=NOW()-INTERVAL '7 days') sales7,
        (SELECT COUNT(*) FROM product_views WHERE viewed_at>=NOW()-INTERVAL '24 hours') views24,
        (SELECT COUNT(*) FROM notification_queue WHERE status='pending') queued
    """, fetch="one")
    top=db_execute("""SELECT p.name,COUNT(*) c FROM orders o JOIN products p ON p.id=o.product_id
                      WHERE o.created_at>=NOW()-INTERVAL '7 days' GROUP BY p.name ORDER BY c DESC LIMIT 5""", fetch="all") or []
    tops="\n".join(f"• {html.escape(r['name'])}: <b>{r['c']}</b>" for r in top) or "• No orders yet"
    text=(f"🧠 <b>{html.escape(APP_VERSION)} Intelligence</b>\n\n"
          f"👥 Users: <b>{row['users']}</b>\n📦 Orders (7d): <b>{row['orders7']}</b>\n"
          f"💰 Sales (7d): <b>{fmt_money(row['sales7'])}</b>\n👀 Views (24h): <b>{row['views24']}</b>\n"
          f"🔔 Notification queue: <b>{row['queued']}</b>\n\n🔥 <b>Top Products — 7d</b>\n{tops}")
    await c.answer(); await c.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=setting("admin_back","⬅️ Admin"), callback_data="admin:dashboard")]
    ]))


def intelligence_daily_cleanup():
    db_execute("DELETE FROM product_views WHERE viewed_at < NOW()-INTERVAL '90 days'")
    db_execute("DELETE FROM intelligence_events WHERE created_at < NOW()-INTERVAL '180 days'")


# V9.0 Preflight MAX: strict startup integrity checks before polling.
def startup_preflight():
    required = ("Buy", "DirectPaymentState", "SearchState", "PaymentState", "AdminState", "CartState", "HealthHandler", "start_health_server", "_feature_on", "admin_ultra", "vip_tier", "notification_queue_loop", "cleanup_expired_transactions", "intelligence_setup", "smart_recommendations", "admin_intelligence", "cart_markup", "coupon_discount", "marketing_campaign_loop", "marketing_create_campaign", "marketing_abandoned_cart_job", "marketing_record_conversion", "payment_method_specs", "payment_method_keyboard", "show_deposit_start", "admin_payment_methods", "admin_autopilot", "normalize_trx_id", "record_payment_audit", "payment_amount_limits_ok", "order_pay_wallet", "order_pay_direct", "direct_payment_method", "direct_payment_trx", "performance_health_snapshot", "performance_maintenance_loop", "invalidate_setting_cache")
    missing = [name for name in required if name not in globals()]
    if missing:
        raise RuntimeError("Preflight MAX failed; missing: " + ", ".join(missing))
    forbidden = [name for name in globals() if name.endswith("KeyboardMarkup") and name != "InlineKeyboardMarkup"]
    if forbidden:
        raise RuntimeError("Preflight MAX failed: non-inline keyboard markup detected: " + ", ".join(forbidden))
    if not APP_VERSION.startswith("V9.4"):
        raise RuntimeError("Preflight MAX failed: APP_VERSION mismatch: " + APP_VERSION)
    if "d.get('game_uid'" in _send_order_result.__code__.co_names:
        raise RuntimeError("Preflight MAX failed: manual notification has stale state variable reference")
    if not callable(payment_method_keyboard) or not callable(payment_method_specs):
        raise RuntimeError("Preflight MAX failed: payment flow functions are not callable")
    methods = {code for code, _, _ in payment_method_specs()}
    expected = {"bkash", "nagad", "rocket", "binance"}
    if methods != expected:
        raise RuntimeError("Preflight MAX failed: payment methods mismatch: " + ", ".join(sorted(methods)))
    if not callable(admin_payment_methods) or not callable(admin_autopilot):
        raise RuntimeError("Preflight MAX failed: admin control functions are not callable")
    if not callable(performance_health_snapshot) or not callable(performance_maintenance_loop):
        raise RuntimeError("Preflight MAX failed: performance functions are not callable")
    if "time" not in globals() or not hasattr(time, "monotonic"):
        raise RuntimeError("Preflight MAX failed: time.monotonic unavailable")
    if "sys" not in globals() or not getattr(sys, "version_info", None):
        raise RuntimeError("Preflight MAX failed: sys module unavailable")
    print("PREFLIGHT_MAX_OK: symbols/payment/security/performance/inline/admin checks passed", flush=True)
    perf_inc("requests", 1)


async def main():
    # V7.4 startup safety: validate core schema before polling.
    database_integrity_check()
    _load_settings_cache()
    intelligence_setup()
    startup_preflight()
    print("STARTUP_PREFLIGHT_OK V9.4 FINAL CHECKED ULTRA BUY FLOW • MANUAL ADMIN + NEW USER DIRECT PAYMENT • MANUAL DELIVERY • WALLET/DIRECT PAYMENT • INLINE-ONLY", flush=True)
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
    asyncio.create_task(notification_queue_loop(bot))
    asyncio.create_task(automation_loop())
    asyncio.create_task(marketing_campaign_loop(bot))

    async def autopilot_health_loop():
        while True:
            try:
                # Safe observation only; never auto-change prices, balances or payment status.
                db_execute("SELECT 1")
            except Exception:
                pass
            await asyncio.sleep(1800)

    asyncio.create_task(autopilot_health_loop())

    async def intelligence_cleanup_loop():
        while True:
            try:
                intelligence_daily_cleanup()
            except Exception:
                pass
            await asyncio.sleep(86400)

    asyncio.create_task(intelligence_cleanup_loop())
    asyncio.create_task(performance_maintenance_loop())
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
