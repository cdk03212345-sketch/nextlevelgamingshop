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
    FSInputFile, ReplyKeyboardMarkup, KeyboardButton, BotCommand, BotCommandScopeDefault, BotCommandScopeChat
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
APP_VERSION = "V6.4.1"
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
                defaults = {
                    "shop_name": "Next Level Gaming Shop",
                    "support": SUPPORT,
                    "payment_info": PAYMENT_INFO,
                    "maintenance": "0",
                    "low_stock_threshold": "3",
                }
                for key, value in defaults.items():
                    cur.execute(
                        "INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO NOTHING",
                        (key, str(value)),
                    )


init_db()
router = Router()


def setting(key, fallback=""):
    row = db_execute("SELECT value FROM settings WHERE key=%s", (key,), "one")
    return row["value"] if row else fallback


def set_setting(key, value):
    db_execute(
        "INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
        (key, str(value)),
    )


def shop_name():
    return setting("shop_name", "Next Level Gaming Shop")


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
    row = db_execute("SELECT id FROM users WHERE tg_id=%s", (tg.id,), "one")
    username = getattr(tg, "username", None)
    name = getattr(tg, "full_name", None) or str(tg.id)
    if row:
        db_execute(
            "UPDATE users SET username=%s,name=%s,updated_at=NOW() WHERE tg_id=%s",
            (username, name, tg.id),
        )
    else:
        db_insert_returning(
            "INSERT INTO users(tg_id,username,name) VALUES(%s,%s,%s) RETURNING id",
            (tg.id, username, name),
        )
    return db_execute("SELECT * FROM users WHERE tg_id=%s", (tg.id,), "one")


def admin_log(admin_id, action, details=""):
    db_execute(
        "INSERT INTO admin_logs(admin_tg_id,action,details) VALUES(%s,%s,%s)",
        (admin_id, action, details),
    )


BACKUP_TABLES = (
    "users", "products", "product_codes", "orders", "payments",
    "payment_receipts", "balance_logs", "admin_logs", "settings"
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
            ('payment_receipts'),('balance_logs'),('admin_logs'),('settings')
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
                cur.execute("SELECT id FROM products WHERE delivery_type='code'")
                ids = [r["id"] for r in cur.fetchall()]
                for pid in ids:
                    sync_code_product_stock(pid, conn)



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
        f"🎮 {r['name']} — <b>{r['effective_stock']}</b> left" for r in rows
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id,text_msg)
        except Exception:
            pass

def fmt_money(value):
    return f"{float(value):.2f} {CURRENCY}"


def status_emoji(status):
    return {"pending":"⏳","completed":"✅","rejected":"❌","refunded":"↩️","credited":"✅"}.get(status,"•")


def user_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🛒 Shop"), KeyboardButton(text="👤 Profile")],
        [KeyboardButton(text="💰 Add Balance"), KeyboardButton(text="📦 My Orders")],
        [KeyboardButton(text="🔎 Search"), KeyboardButton(text="💬 Support")]
    ], resize_keyboard=True)


def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Dashboard", callback_data="admin:dashboard"), InlineKeyboardButton(text="📈 Reports", callback_data="admin:reports")],
        [InlineKeyboardButton(text="🧾 Orders", callback_data="admin:orders"), InlineKeyboardButton(text="💳 Payments", callback_data="admin:payments")],
        [InlineKeyboardButton(text="👥 Users", callback_data="admin:users"), InlineKeyboardButton(text="🛍 Products", callback_data="admin:products")],
        [InlineKeyboardButton(text="🎫 Codes", callback_data="admin:codes"), InlineKeyboardButton(text="💰 Balance", callback_data="admin:balance")],
        [InlineKeyboardButton(text="📢 Broadcast", callback_data="admin:broadcast"), InlineKeyboardButton(text="⚙️ Settings", callback_data="admin:settings")],
        [InlineKeyboardButton(text="📊 Database", callback_data="admin:dbinfo"), InlineKeyboardButton(text="📝 Logs", callback_data="admin:logs")],
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
        [InlineKeyboardButton(text=f"🎮 {game} ({count})",
                              callback_data=f"game:{game}")]
        for game, count in sorted(games.items())
    ]
    buttons.append([InlineKeyboardButton(text="🔎 All Products", callback_data="cat:*")])
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
            label = f"📦 {pack} ({int(r['c'])})"
        else:
            label = f"🛍 Products ({int(r['c'])})"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"cat:{category}")])

    buttons.append([InlineKeyboardButton(text="⬅️ Games", callback_data="shop")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def products_kb(category="*", page=0, per_page=8):
    offset = page * per_page
    if category == "*":
        rows = db_execute("SELECT * FROM products WHERE active=1 ORDER BY id DESC LIMIT %s OFFSET %s", (per_page, offset), "all")
        total = db_execute("SELECT COUNT(*) AS c FROM products WHERE active=1", fetch="one")["c"]
    else:
        rows = db_execute("SELECT * FROM products WHERE active=1 AND category=%s ORDER BY id DESC LIMIT %s OFFSET %s", (category, per_page, offset), "all")
        total = db_execute("SELECT COUNT(*) AS c FROM products WHERE active=1 AND category=%s", (category,), "one")["c"]
    buttons = []
    for p in rows:
        stock = effective_stock(p)
        buttons.append([InlineKeyboardButton(text=f"{'🟢' if stock > 0 else '🔴'} {p['name']} • {float(p['price']):g} {CURRENCY}", callback_data=f"product:{p['id']}")])
    nav=[]
    if page>0: nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"page:{category}:{page-1}"))
    if offset+len(rows)<total: nav.append(InlineKeyboardButton(text="➡️", callback_data=f"page:{category}:{page+1}"))
    if nav: buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="📂 Categories", callback_data="shop")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


class Buy(StatesGroup):
    uid = State()
    confirm = State()

class PaymentState(StatesGroup):
    amount = State()
    method = State()
    trx = State()
    receipt = State()

class AdminState(StatesGroup):
    add_product = State()
    edit_product = State()
    add_codes = State()
    balance = State()
    broadcast = State()
    settings = State()


class HealthHandler(BaseHTTPRequestHandler):
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
            return self._send("Next Level Gaming Shop V6.4.0 is running.", 200, "text/plain; charset=utf-8")
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
                f"<td>{html.escape(r['name'])}</td><td>{float(r['total']):.2f} {html.escape(CURRENCY)}</td>"
                f"<td>{html.escape(r['status'])}</td><td>{html.escape(str(r['created_at']))}</td></tr>"
                for r in recent
            )
            page=f"""<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(shop_name())} V6.4.0 Admin</title>
<style>
body{{font-family:system-ui;margin:20px;background:#111;color:#eee}} .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}}
.card{{background:#1d1d1d;padding:16px;border-radius:12px}} table{{width:100%;border-collapse:collapse;margin-top:20px}}
td,th{{padding:8px;border-bottom:1px solid #333;text-align:left}} h1{{font-size:24px}}
</style></head><body>
<h1>🎮 {html.escape(shop_name())} — V6.4.0 Admin</h1>
<div class="grid">
<div class="card">👥 Users<br><b>{row['users']}</b></div>
<div class="card">🛍 Products<br><b>{row['products']}</b></div>
<div class="card">🎫 Codes<br><b>{row['codes']}</b></div>
<div class="card">🧾 Pending Orders<br><b>{row['pending_orders']}</b></div>
<div class="card">💳 Pending Payments<br><b>{row['pending_payments']}</b></div>
<div class="card">💰 Sales<br><b>{float(row['sales']):.2f} {html.escape(CURRENCY)}</b></div>
<div class="card">👛 Wallet Total<br><b>{float(row['wallet']):.2f} {html.escape(CURRENCY)}</b></div>
</div>
<h2>Recent Orders</h2>
<table><tr><th>ID</th><th>User</th><th>Product</th><th>Total</th><th>Status</th><th>Created</th></tr>{rows}</table>
</body></html>"""
            return self._send(page)
        except Exception as exc:
            return self._send("<h2>Dashboard temporarily unavailable</h2>", 500)

    def log_message(self, *args):
        return


def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


@router.message(CommandStart())
async def start(m: Message):
    u=get_user(m.from_user)
    if u["blocked"] and not is_admin(m.from_user.id): return await m.answer("🚫 Your account is blocked.")
    await m.answer(f"🎮 <b>{shop_name()}</b>\n\n⚡ Fast & secure gaming top-up/code delivery\n💰 Wallet balance • 🧾 Order history • 🎫 Instant codes", reply_markup=user_menu())


@router.message(Command("shop", "listings"))
@router.message(F.text == "🛒 Shop")
async def shop(m: Message):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id): return await m.answer("🚫 Your account is blocked.")
    if maintenance_active() and not is_admin(m.from_user.id): return await m.answer("🔧 Shop is temporarily under maintenance. Please try again later.")
    await m.answer("🛍 <b>Shop Categories</b>\nChoose a category:", reply_markup=categories_kb())

@router.callback_query(F.data == "shop")
async def shop_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    await c.answer(); await c.message.edit_text("🛍 <b>Shop Categories</b>\nChoose a category:", reply_markup=categories_kb())

@router.callback_query(F.data.startswith("game:"))
async def game_folder_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    game = c.data.split(":", 1)[1]
    await c.answer()
    await c.message.edit_text(
        f"🎮 <b>{game}</b>\n\n📂 Choose a pack:",
        reply_markup=game_packs_kb(game)
    )

@router.callback_query(F.data.startswith("cat:"))
async def category_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    category=c.data.split(":",1)[1]; await c.answer(); await c.message.edit_text(f"🛍 <b>{'All Products' if category=='*' else category}</b>", reply_markup=products_kb(category,0))

@router.callback_query(F.data.startswith("page:"))
async def page_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    _,category,page=c.data.split(":",2); await c.answer(); await c.message.edit_reply_markup(reply_markup=products_kb(category,int(page)))

@router.callback_query(F.data.startswith("product:"))
async def product_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.", show_alert=True)
    pid=int(c.data.split(":")[1]); p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    stock=effective_stock(p)
    delivery="Instant Code" if is_auto_code_product(p) else "Manual"
    text=(f"🎮 <b>{p['name']}</b>\n🏷 Category: <b>{p['category']}</b>\n📦 Quantity: <b>{p['quantity']}</b>\n💰 Price: <b>{float(p['price']):g} {CURRENCY}</b>\n📦 Stock: <b>{stock}</b>\n⚡ Delivery: <b>{delivery}</b>\n\n{p['description'] or 'No description.'}")
    buttons=[]
    if stock>0: buttons.append([InlineKeyboardButton(text="🛒 Buy Now",callback_data=f"buy:{pid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Back",callback_data=f"cat:{p['category']}")])
    await c.answer()
    markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    if p["image_file_id"]:
        try:
            await c.message.answer_photo(p["image_file_id"], caption=text, reply_markup=markup)
            return
        except Exception:
            pass
    await c.message.edit_text(text,reply_markup=markup)

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
        return await m.answer("🔧 Shop is temporarily under maintenance. Please try again later.", reply_markup=user_menu())
    uid=(m.text or "").strip()
    if uid.lower()=="/cancel": await state.clear(); return await m.answer("❌ Cancelled.",reply_markup=user_menu())
    if len(uid)<2 or len(uid)>64: return await m.answer("❌ Please send a valid UID.")
    d=await state.get_data(); p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(d["pid"],),"one"); u=get_user(m.from_user)
    if not p or effective_stock(p)<1: await state.clear(); return await m.answer("❌ Product is out of stock.")
    if float(u["balance"])<float(p["price"]): await state.clear(); return await m.answer(f"❌ <b>Insufficient balance</b>\n\nPrice: {fmt_money(p['price'])}\nBalance: {fmt_money(u['balance'])}\nNeed: {fmt_money(float(p['price'])-float(u['balance']))}")
    await state.update_data(game_uid=uid); await state.set_state(Buy.confirm)
    await m.answer(f"🧾 <b>Order Confirmation</b>\n\n🎮 Product: <b>{p['name']}</b>\n🆔 UID: <code>{uid}</code>\n💰 Total: <b>{fmt_money(p['price'])}</b>\n\nConfirm your purchase:",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Confirm",callback_data="order:confirm"),InlineKeyboardButton(text="❌ Cancel",callback_data="order:cancel")]]))

@router.callback_query(Buy.confirm,F.data=="order:cancel")
async def order_cancel(c:CallbackQuery,state:FSMContext): await state.clear(); await c.answer("Cancelled"); await c.message.answer("❌ Order cancelled.",reply_markup=user_menu())

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
                    if auto_code: sync_code_product_stock(p["id"],conn)
        except Exception as exc:
            print(f"order_confirm error: {exc}")
            return await c.answer("Order could not be completed. Your balance and stock were not charged.",show_alert=True)
    await c.answer("✅ Payment successful")
    if delivered_code:
        await c.message.answer(f"✅ <b>Payment Successful</b>\n\n🧾 Order: <b>#{order_id}</b>\n🎮 Product: <b>{p['name']}</b>\n🆔 UID: <code>{d['game_uid']}</code>\n💰 Paid: <b>{fmt_money(total)}</b>\n\n🎁 <b>Your Code</b>\n<code>{delivered_code}</code>\n\n⚠️ Keep this code private.",reply_markup=user_menu())
    else:
        await c.message.answer(f"⏳ <b>Order #{order_id} submitted</b>\n\n🎮 Product: <b>{p['name']}</b>\n🆔 UID: <code>{d['game_uid']}</code>\n💰 Paid: <b>{fmt_money(total)}</b>\n📌 Status: Waiting for admin processing.",reply_markup=user_menu())
        for admin_id in ADMIN_IDS:
            try: await c.bot.send_message(admin_id,f"🧾 <b>New Manual Top-up Order #{order_id}</b>\n\n👤 User: <code>{u['tg_id']}</code>\n🎮 Product: {p['name']}\n🆔 UID: <code>{d['game_uid']}</code>\n💰 Total: {fmt_money(total)}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Complete",callback_data=f"order_complete:{order_id}"),InlineKeyboardButton(text="❌ Reject + Refund",callback_data=f"order_reject:{order_id}")]]))
            except Exception: pass

@router.message(Command("version"))
async def version_command(m:Message): await m.answer(f"🚀 <b>Next Level Gaming Shop {APP_VERSION}</b>\n☁️ PostgreSQL database enabled\n⚡ Instant code delivery enabled\n📸 Payment receipt verification enabled")

@router.message(Command("profile"))
@router.message(F.text=="👤 Profile")
async def profile(m:Message):
    u=get_user(m.from_user); row=db_execute("SELECT COUNT(*) AS c FROM orders WHERE user_id=%s",(u["id"],),"one")
    await m.answer(f"👤 <b>Your Profile</b>\n\n🆔 Telegram ID: <code>{u['tg_id']}</code>\n💰 Balance: <b>{fmt_money(u['balance'])}</b>\n🧾 Orders: <b>{row['c']}</b>\n📅 Joined: <code>{u['created_at']}</code>",reply_markup=user_menu())

@router.message(Command("orders"))
@router.message(F.text=="📦 My Orders")
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
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ My Orders",callback_data="my_orders_back")]]))

@router.callback_query(F.data=="my_orders_back")
async def my_orders_back(c:CallbackQuery):
    u=get_user(c.from_user); rows=db_execute("SELECT o.id,o.total,o.status,o.created_at,p.name FROM orders o JOIN products p ON p.id=o.product_id WHERE o.user_id=%s ORDER BY o.id DESC LIMIT 10",(u["id"],),"all")
    if not rows: return await c.answer("No orders.",show_alert=True)
    buttons=[[InlineKeyboardButton(text=f"🧾 Order #{r['id']}",callback_data=f"order_detail:{r['id']}")] for r in rows]
    await c.answer(); await c.message.edit_text("📦 <b>Your Recent Orders</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.message(Command("balance", "deposit"))
@router.message(F.text=="💰 Add Balance")
async def add_balance(m:Message,state:FSMContext):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id): return await m.answer("🚫 Your account is blocked.")
    if maintenance_active() and not is_admin(m.from_user.id): return await m.answer("🔧 Shop is temporarily under maintenance. Please try again later.")
    await state.set_state(PaymentState.amount); await m.answer(f"💳 <b>Add Balance</b>\n\nPayment methods: bKash / Nagad\n{setting('payment_info',PAYMENT_INFO)}\n\nSend amount. Example: <code>500</code>")

@router.message(PaymentState.amount)
async def payment_amount(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear()
        return await m.answer("🔧 Shop is temporarily under maintenance. Please try again later.", reply_markup=user_menu())
    try: amount=float((m.text or "").strip())
    except ValueError: return await m.answer("❌ Enter a valid amount.")
    if amount<10 or amount>100000: return await m.answer("❌ Amount must be between 10 and 100000.")
    await state.update_data(amount=amount); await state.set_state(PaymentState.method); await m.answer("💳 Choose payment method:",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="bKash",callback_data="paymethod:bkash"),InlineKeyboardButton(text="Nagad",callback_data="paymethod:nagad")]]))

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
        return await m.answer("🔧 Shop is temporarily under maintenance. Please try again later.", reply_markup=user_menu())
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
        reply_markup=user_menu(),
    )

@router.message(PaymentState.receipt, F.photo)
@router.message(PaymentState.receipt, F.document)
async def payment_receipt_upload(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear()
        return await m.answer("🔧 Shop is temporarily under maintenance. Please try again later.", reply_markup=user_menu())
    d=await state.get_data(); payment_id=d.get("payment_id")
    if not payment_id:
        await state.clear()
        return await m.answer("❌ Payment session expired. Please start again.", reply_markup=user_menu())
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
        return await m.answer("❌ Payment session expired. Please start again.", reply_markup=user_menu())
    row=db_execute("SELECT * FROM payments WHERE id=%s",(payment_id,),"one")
    if not row:
        await state.clear()
        return await m.answer("❌ Payment request not found.", reply_markup=user_menu())
    await state.clear()
    receipt_text="📸 Receipt attached" if receipt else "📎 No receipt attached"
    await m.answer(
        f"✅ <b>Payment Request #{payment_id}</b>\n\n💰 Amount: {fmt_money(row['amount'])}\n💳 Method: {row['method'].title()}\n🧾 TrxID: <code>{html.escape(row['trx_id'])}</code>\n{receipt_text}\n⏳ Waiting for admin approval.",
        reply_markup=user_menu(),
    )
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Credit",callback_data=f"pay_credit:{payment_id}"),InlineKeyboardButton(text="❌ Reject",callback_data=f"pay_reject:{payment_id}")]])
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

@router.message(Command("support", "help"))
@router.message(F.text=="💬 Support")
async def support(m:Message): await m.answer(f"🎧 <b>Support</b>\n\nContact: {setting('support',SUPPORT)}")

@router.message(Command("cancel"))
async def cancel(m:Message,state:FSMContext): await state.clear(); await m.answer("❌ Cancelled.",reply_markup=user_menu())

# ---------------- Admin ----------------
@router.message(Command("admin"))
async def admin_command(m:Message):
    if not is_admin(m.from_user.id): return await m.answer("❌ Access denied.")
    await m.answer("👑 <b>Next Level Gaming Shop V6.3.3</b>\nAdmin Control Center",reply_markup=admin_menu())

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
    text=(f"📊 <b>Next Level Gaming Shop — V6.3.3 Dashboard</b>\n\n👥 Users: <b>{row['users']}</b>\n🛍 Active Products: <b>{row['products']}</b>\n⚠️ Low-stock Products: <b>{row['low_stock']}</b>\n🎫 Available Codes: <b>{row['codes']}</b>\n🧾 Pending Orders: <b>{row['pending_orders']}</b>\n💳 Pending Payments: <b>{row['pending_payments']}</b>\n✅ Completed Orders: <b>{row['completed']}</b>\n💵 Today Sales: <b>{fmt_money(row['today_sales'])}</b>\n💰 All-time Sales: <b>{fmt_money(row['sales'])}</b>\n👛 User Wallet Total: <b>{fmt_money(row['balance'])}</b>")
    await c.answer(); await c.message.edit_text(text,reply_markup=admin_menu())

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
    text=(f"📊 <b>V6.3.3 Sales Report</b>\n\n📅 Today sales: <b>{fmt_money(row['today_sales'])}</b>\n🧾 Today orders: <b>{row['today_orders']}</b>\n📆 7-day sales: <b>{fmt_money(row['week_sales'])}</b>\n🧾 7-day orders: <b>{row['week_orders']}</b>\n💰 All-time sales: <b>{fmt_money(row['all_sales'])}</b>\n👥 Users: <b>{users}</b>\n⏳ Pending orders: <b>{row['pending']}</b>\n↩️ Refunded orders: <b>{row['refunded']}</b>\n\n🏆 <b>Top Products</b>\n{top_text}")
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]]))

@router.callback_query(F.data=="admin:products")
async def admin_products(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute("SELECT * FROM products ORDER BY id DESC",fetch="all"); buttons=[]
    for p in rows: buttons.append([InlineKeyboardButton(text=f"{'🟢' if p['active'] else '🔴'} {p['name'][:24]} • {effective_stock(p)}",callback_data=f"p:{p['id']}")])
    buttons += [[InlineKeyboardButton(text="➕ Add Product",callback_data="admin:add_product")],[InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]]
    await c.answer(); await c.message.edit_text("🛍 <b>Product Management</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("p:"))
async def product_manage(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1]); p=db_execute("SELECT * FROM products WHERE id=%s",(pid,),"one")
    if not p: return await c.answer("Not found",show_alert=True)
    text=f"🎮 <b>{p['name']}</b>\n🏷 Category: {p['category']}\n📦 Quantity: {p['quantity']}\n💰 Price: {fmt_money(p['price'])}\n🚚 Delivery: {p['delivery_type']}\n📊 Stock: {effective_stock(p)}\n🔘 Active: {'Yes' if p['active'] else 'No'}\n\n{p['description'] or 'No description.'}"
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✏️ Edit",callback_data=f"pedit:{pid}"),InlineKeyboardButton(text="🔄 Enable/Disable",callback_data=f"ptoggle:{pid}")],[InlineKeyboardButton(text="🎫 Add Codes",callback_data=f"codes_add:{pid}")],[InlineKeyboardButton(text="🗑 Delete",callback_data=f"pdelete:{pid}")],[InlineKeyboardButton(text="⬅️ Products",callback_data="admin:products")]]))

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
    buttons.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]); await c.answer(); await c.message.edit_text("🎫 <b>Code Inventory</b>\nSelect a product:",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("codes_add:"))
async def codes_add_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1]); p=db_execute("SELECT * FROM products WHERE id=%s",(pid,),"one")
    if not p: return await c.answer("Not found",show_alert=True)
    await c.answer(); await state.update_data(pid=pid); await state.set_state(AdminState.add_codes); await c.message.answer(f"🎫 <b>{p['name']}</b>\n\nSend one code per line. Duplicate codes are skipped.")

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
    text="🧾 No orders yet." if not rows else "🧾 <b>Recent Orders</b>\n\n"+"\n".join(f"#{r['id']} • {r['name'][:18]}\n👤 <code>{r['tg_id']}</code> • {fmt_money(r['total'])}\n{status_emoji(r['status'])} {r['status'].title()}\n" for r in rows)
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]]))

@router.callback_query(F.data.startswith("order_complete:"))
async def manual_order_complete(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":")[1])
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,)); o=cur.fetchone()
                if not o or o["status"]!="pending": return await c.answer("Already processed.",show_alert=True)
                cur.execute("UPDATE orders SET status='completed',processed_at=NOW(),updated_at=NOW() WHERE id=%s",(oid,)); cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
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
    if not rows: text="💳 No pending payments."; kb=[[InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]]
    else:
        text="💳 <b>Pending Payments</b>\n\n"+"\n".join(f"#{r['id']} • {fmt_money(r['amount'])}\n👤 <code>{r['tg_id']}</code> • {r['method'].title()}\nTrxID: <code>{r['trx_id']}</code>\n" for r in rows)
        kb=[]
        for r in rows:
            receipt=db_execute("SELECT 1 FROM payment_receipts WHERE payment_id=%s",(r['id'],),"one")
            row_buttons=[InlineKeyboardButton(text=f"✅ Credit #{r['id']}",callback_data=f"pay_credit:{r['id']}"),InlineKeyboardButton(text="❌ Reject",callback_data=f"pay_reject:{r['id']}")]
            kb.append(row_buttons)
            if receipt:
                kb.append([InlineKeyboardButton(text=f"📸 Receipt #{r['id']}",callback_data=f"pay_receipt:{r['id']}")])
        kb.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")])
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
    try: await c.bot.send_message(u["tg_id"],f"💰 <b>Balance Added</b>\n\nPayment: #{pid}\nAmount: <b>{fmt_money(p['amount'])}</b>")
    except Exception: pass

@router.callback_query(F.data.startswith("pay_reject:"))
async def payment_reject(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1]); row=db_execute("UPDATE payments SET status='rejected',updated_at=NOW() WHERE id=%s AND status='pending'",(pid,))
    if row!=1: return await c.answer("Already processed.",show_alert=True)
    admin_log(c.from_user.id,"reject_payment",f"payment #{pid}"); await c.answer("Rejected"); await c.message.edit_text(f"❌ Payment #{pid} rejected.")

@router.callback_query(F.data=="admin:users")
async def admin_users(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute("SELECT * FROM users ORDER BY id DESC LIMIT 20",fetch="all"); buttons=[[InlineKeyboardButton(text=f"{'🚫' if u['blocked'] else '🟢'} {(u['name'] or 'User')[:18]} • {float(u['balance']):.0f}",callback_data=f"user:{u['id']}")] for u in rows]; buttons.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]); await c.answer(); await c.message.edit_text("👥 <b>User Management</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("user:"))
async def user_detail(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    uid=int(c.data.split(":")[1]); u=db_execute("SELECT * FROM users WHERE id=%s",(uid,),"one")
    if not u: return await c.answer("Not found.",show_alert=True)
    orders=db_execute("SELECT COUNT(*) AS c FROM orders WHERE user_id=%s",(uid,),"one")["c"]
    await c.answer(); await c.message.edit_text(f"👤 <b>{u['name']}</b>\n\nTelegram ID: <code>{u['tg_id']}</code>\nUsername: @{u['username'] or '-'}\nBalance: <b>{fmt_money(u['balance'])}</b>\nOrders: <b>{orders}</b>\nStatus: {'🚫 Blocked' if u['blocked'] else '🟢 Active'}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔓 Unblock" if u['blocked'] else "🚫 Block",callback_data=f"user_toggle:{uid}")],[InlineKeyboardButton(text="⬅️ Users",callback_data="admin:users")]]))

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
    rows=db_execute("SELECT * FROM admin_logs ORDER BY id DESC LIMIT 20",fetch="all"); text="📝 No admin logs." if not rows else "📝 <b>Recent Admin Logs</b>\n\n"+"\n".join(f"#{r['id']} • {r['action']}\n{r['details']}\n🕒 {r['created_at']}\n" for r in rows); await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]]))

@router.callback_query(F.data=="admin:settings")
async def settings(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    text=f"⚙️ <b>Shop Settings</b>\n\n🏪 Name: <code>{shop_name()}</code>\n🎧 Support: <code>{setting('support',SUPPORT)}</code>\n💳 Payment info: <code>{setting('payment_info',PAYMENT_INFO)}</code>\n⚠️ Low stock: <code>{low_stock_threshold()}</code>\n🔧 Maintenance: <code>{'ON' if setting('maintenance','0')=='1' else 'OFF'}</code>"
    kb=[[InlineKeyboardButton(text="🏪 Change Name",callback_data="set:shop_name"),InlineKeyboardButton(text="🎧 Support",callback_data="set:support")],[InlineKeyboardButton(text="💳 Payment Info",callback_data="set:payment_info"),InlineKeyboardButton(text="⚠️ Low Stock",callback_data="set:low_stock")],[InlineKeyboardButton(text="🔧 Toggle Maintenance",callback_data="set:maintenance")],[InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]]
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data=="set:maintenance")
async def toggle_maintenance(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    val="0" if setting("maintenance","0")=="1" else "1"; set_setting("maintenance",val); admin_log(c.from_user.id,"maintenance",val); await c.answer("Updated"); await settings(c)

@router.callback_query(F.data.startswith("set:"))
async def setting_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    key=c.data.split(":",1)[1]
    if key=="maintenance": return
    await state.update_data(setting_key=key); await state.set_state(AdminState.settings)
    prompts={"shop_name":"🏪 Send the new shop name.","support":"🎧 Send support username/contact.","payment_info":"💳 Send payment instructions/number.","low_stock":"⚠️ Send low-stock threshold number."}
    await c.answer(); await c.message.answer(prompts[key])

@router.message(AdminState.settings)
async def setting_save(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return await m.answer("Denied")
    d=await state.get_data(); key=d.get("setting_key"); value=(m.text or "").strip()
    if not value: return await m.answer("❌ Value cannot be empty.")
    if key=="low_stock":
        try: value=str(max(0,int(value)))
        except ValueError: return await m.answer("❌ Send a whole number.")
        key="low_stock_threshold"
    if len(value)>500: return await m.answer("❌ Value too long.")
    set_setting(key,value); await state.clear(); admin_log(m.from_user.id,"setting_changed",f"{key}={value}"); await m.answer(f"✅ Setting updated: <b>{key}</b>",reply_markup=admin_menu())

@router.callback_query(F.data=="admin:dbinfo")
async def dbinfo(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    row=db_execute("SELECT current_database() AS db,current_schema() AS schema",fetch="one")
    await c.answer(); await c.message.edit_text(f"☁️ <b>Database</b>\n\nEngine: <b>PostgreSQL</b>\nDatabase: <code>{row['db']}</code>\nSchema: <code>{row['schema']}</code>\n\n✅ Data is stored outside the Render service filesystem.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]]))

@router.message(Command("search"))
async def search_command(m:Message):
    if maintenance_active() and not is_admin(m.from_user.id): return await m.answer("🔧 Shop is temporarily under maintenance. Please try again later.")
    q=(m.text or "").split(maxsplit=1); term=q[1].strip() if len(q)>1 else ""
    if not term: return await m.answer("🔎 Use: <code>/search PUBG</code>")
    rows=db_execute("SELECT * FROM products WHERE active=1 AND (name ILIKE %s OR category ILIKE %s OR description ILIKE %s) ORDER BY id DESC LIMIT 20",(f"%{term}%",f"%{term}%",f"%{term}%"),"all")
    if not rows: return await m.answer("🔎 No products found.")
    kb=[[InlineKeyboardButton(text=f"{'🟢' if effective_stock(p)>0 else '🔴'} {p['name']} • {float(p['price']):g} {CURRENCY}",callback_data=f"product:{p['id']}")] for p in rows]
    await m.answer(f"🔎 Results for <b>{term}</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.message(F.text=="🔎 Search")
async def search_button(m:Message):
    if maintenance_active() and not is_admin(m.from_user.id): return await m.answer("🔧 Shop is temporarily under maintenance. Please try again later.")
    await m.answer("🔎 Send <code>/search product-name</code> to search the shop.")

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
    await m.answer("⚠️ <b>Low Stock Alert</b>\n\n"+"\n".join(f"• {r['name']}: <b>{r['effective_stock']}</b>" for r in rows))

@router.message()
async def fallback(m:Message):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id): return await m.answer("🚫 Your account is blocked.")
    if setting("maintenance","0")=="1" and not is_admin(m.from_user.id): return await m.answer("🔧 Shop is temporarily under maintenance. Please try again later.")
    await m.answer("Use the menu below or /shop to continue.",reply_markup=user_menu())


USER_BOT_COMMANDS = [
    BotCommand(command="start", description="Launch the bot and open the main menu"),
    BotCommand(command="shop", description="Browse available products"),
    BotCommand(command="listings", description="Browse products (shortcut)"),
    BotCommand(command="search", description="Search products by name"),
    BotCommand(command="orders", description="View your recent orders"),
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
