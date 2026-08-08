import os
import asyncio
import sqlite3
import threading
import shutil
import csv
from pathlib import Path
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

load_dotenv()

# =========================
# Configuration
# =========================
TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
CURRENCY = os.getenv("CURRENCY", "BDT")
PAYMENT_INFO = os.getenv(
    "PAYMENT_INSTRUCTIONS",
    "bKash/Nagad: YOUR NUMBER"
)
SUPPORT = os.getenv("SUPPORT_USERNAME", "@YourSupport")
DB_FILE = os.getenv("DATABASE_FILE", "nextlevel_v4.db")

if not TOKEN:
    raise RuntimeError("BOT_TOKEN missing")

# =========================
# Database
# =========================
db = sqlite3.connect(DB_FILE, check_same_thread=False)
db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA foreign_keys=ON")
db.execute("PRAGMA busy_timeout=5000")
DB_LOCK = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_id INTEGER UNIQUE NOT NULL,
    username TEXT,
    name TEXT,
    balance REAL NOT NULL DEFAULT 0,
    blocked INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'Gaming',
    quantity INTEGER NOT NULL DEFAULT 0,
    price REAL NOT NULL,
    stock INTEGER NOT NULL DEFAULT 0,
    delivery_type TEXT NOT NULL DEFAULT 'code',
    active INTEGER NOT NULL DEFAULT 1,
    description TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_codes(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    code TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL DEFAULT 'available',
    sold_to INTEGER,
    order_id INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sold_at TEXT,
    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS orders(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    game_uid TEXT,
    total REAL NOT NULL,
    delivered_code TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    admin_note TEXT DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(product_id) REFERENCES products(id)
);

CREATE TABLE IF NOT EXISTS payments(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    method TEXT NOT NULL,
    trx_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    admin_note TEXT DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(method, trx_id),
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS balance_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    action TEXT NOT NULL,
    note TEXT DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS admin_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_tg_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    details TEXT DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_products_category
ON products(category);

CREATE INDEX IF NOT EXISTS idx_codes_product_status
ON product_codes(product_id, status);

CREATE INDEX IF NOT EXISTS idx_orders_user
ON orders(user_id);

CREATE INDEX IF NOT EXISTS idx_orders_status
ON orders(status);

CREATE INDEX IF NOT EXISTS idx_payments_status
ON payments(status);
"""

with DB_LOCK:
    db.executescript(SCHEMA)
    # V4 migrations are intentionally additive so an existing V3 database can be upgraded.
    db.execute("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS payment_receipts(payment_id INTEGER PRIMARY KEY, file_id TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(payment_id) REFERENCES payments(id) ON DELETE CASCADE)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_payments_created ON payments(created_at)")
    defaults = {
        "shop_name": "Next Level Gaming Shop",
        "support": SUPPORT,
        "payment_info": PAYMENT_INFO,
        "maintenance": "0",
        "low_stock_threshold": "3",
    }
    for k, v in defaults.items():
        db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k, str(v)))
    db.commit()

# =========================
# Helpers
# =========================
router = Router()


def setting(key, fallback=""):
    with DB_LOCK:
        row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else fallback


def set_setting(key, value):
    with DB_LOCK:
        db.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
        db.commit()


def shop_name():
    return setting("shop_name", "Next Level Gaming Shop")


def low_stock_threshold():
    try:
        return max(0, int(setting("low_stock_threshold", "3")))
    except ValueError:
        return 3


def backup_database():
    Path("backups").mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    target = Path("backups") / f"nextlevel_v4_{stamp}.db"
    with DB_LOCK:
        db.commit()
        # SQLite backup API is safer than a raw file copy while WAL is active.
        dst = sqlite3.connect(target)
        try:
            db.backup(dst)
        finally:
            dst.close()
    # Keep latest 10 backups.
    files = sorted(Path("backups").glob("nextlevel_v4_*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in files[10:]:
        try: old.unlink()
        except OSError: pass
    return target


def report_text():
    with DB_LOCK:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sales_today = db.execute("SELECT COALESCE(SUM(total),0) v FROM orders WHERE status='completed' AND date(created_at)=date(?)", (today,)).fetchone()["v"]
        sales_all = db.execute("SELECT COALESCE(SUM(total),0) v FROM orders WHERE status='completed'").fetchone()["v"]
        orders_today = db.execute("SELECT COUNT(*) c FROM orders WHERE date(created_at)=date(?)", (today,)).fetchone()["c"]
        users = db.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        pending_payments = db.execute("SELECT COUNT(*) c FROM payments WHERE status='pending'").fetchone()["c"]
        low = db.execute("SELECT COUNT(*) c FROM products WHERE active=1 AND stock<=?", (low_stock_threshold(),)).fetchone()["c"]
    return (f"📊 <b>V4 Sales Report</b>\n\n"
            f"📅 Today sales: <b>{fmt_money(sales_today)}</b>\n"
            f"🧾 Today orders: <b>{orders_today}</b>\n"
            f"💰 All-time sales: <b>{fmt_money(sales_all)}</b>\n"
            f"👥 Users: <b>{users}</b>\n"
            f"💳 Pending payments: <b>{pending_payments}</b>\n"
            f"⚠️ Low-stock products: <b>{low}</b>")



def now_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS


def user_blocked(tg_id: int) -> bool:
    with DB_LOCK:
        row = db.execute(
            "SELECT blocked FROM users WHERE tg_id=?", (tg_id,)
        ).fetchone()
    return bool(row and row["blocked"])


def get_user(tg):
    username = getattr(tg, "username", None)
    name = getattr(tg, "full_name", None) or str(getattr(tg, "id", "User"))
    with DB_LOCK:
        row = db.execute(
            "SELECT * FROM users WHERE tg_id=?", (tg.id,)
        ).fetchone()
        if row:
            db.execute(
                "UPDATE users SET username=?, name=?, updated_at=? WHERE tg_id=?",
                (username, name, now_text(), tg.id)
            )
            db.commit()
            return db.execute(
                "SELECT * FROM users WHERE tg_id=?", (tg.id,)
            ).fetchone()

        db.execute(
            "INSERT INTO users(tg_id,username,name) VALUES(?,?,?)",
            (tg.id, username, name)
        )
        db.commit()
        return db.execute(
            "SELECT * FROM users WHERE tg_id=?", (tg.id,)
        ).fetchone()


def admin_log(admin_id, action, details=""):
    with DB_LOCK:
        db.execute(
            "INSERT INTO admin_logs(admin_tg_id,action,details) VALUES(?,?,?)",
            (admin_id, action, details)
        )
        db.commit()


def available_code_count(product_id):
    with DB_LOCK:
        return db.execute(
            "SELECT COUNT(*) c FROM product_codes "
            "WHERE product_id=? AND status='available'",
            (product_id,)
        ).fetchone()["c"]


def effective_stock(product):
    if product["delivery_type"] == "code":
        return available_code_count(product["id"])
    return max(0, int(product["stock"]))


def product_stock_text(product):
    stock = effective_stock(product)
    if product["delivery_type"] == "code":
        return f"🎫 Codes: <b>{stock}</b>"
    return f"📦 Stock: <b>{stock}</b>"


def status_emoji(status):
    return {
        "pending": "⏳",
        "completed": "✅",
        "rejected": "❌",
        "refunded": "↩️",
        "credited": "✅"
    }.get(status, "•")


def fmt_money(value):
    return f"{float(value):.2f} {CURRENCY}"


def user_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛒 Shop"),
             KeyboardButton(text="👤 Profile")],
            [KeyboardButton(text="💰 Add Balance"),
             KeyboardButton(text="📦 My Orders")],
            [KeyboardButton(text="🔎 Search"), KeyboardButton(text="💬 Support")]
        ],
        resize_keyboard=True
    )


def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Dashboard", callback_data="admin:dashboard"),
         InlineKeyboardButton(text="📈 Reports", callback_data="admin:reports")],
        [InlineKeyboardButton(text="🧾 Orders", callback_data="admin:orders"),
         InlineKeyboardButton(text="💳 Payments", callback_data="admin:payments")],
        [InlineKeyboardButton(text="👥 Users", callback_data="admin:users"),
         InlineKeyboardButton(text="🛍 Products", callback_data="admin:products")],
        [InlineKeyboardButton(text="🎫 Codes", callback_data="admin:codes"),
         InlineKeyboardButton(text="💰 Balance", callback_data="admin:balance")],
        [InlineKeyboardButton(text="📢 Broadcast", callback_data="admin:broadcast"),
         InlineKeyboardButton(text="⚙️ Settings", callback_data="admin:settings")],
        [InlineKeyboardButton(text="💾 Backup", callback_data="admin:backup"),
         InlineKeyboardButton(text="📝 Logs", callback_data="admin:logs")]
    ])

def categories_kb():
    with DB_LOCK:
        rows = db.execute(
            "SELECT category, COUNT(*) c FROM products "
            "WHERE active=1 GROUP BY category ORDER BY category"
        ).fetchall()

    buttons = []
    for r in rows:
        buttons.append([
            InlineKeyboardButton(
                text=f"🎮 {r['category']} ({r['c']})",
                callback_data=f"cat:{r['category']}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="🔎 All Products", callback_data="cat:*")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def products_kb(category="*", page=0, per_page=8):
    offset = page * per_page
    with DB_LOCK:
        if category == "*":
            rows = db.execute(
                "SELECT * FROM products WHERE active=1 "
                "ORDER BY id DESC LIMIT ? OFFSET ?",
                (per_page, offset)
            ).fetchall()
            total = db.execute(
                "SELECT COUNT(*) c FROM products WHERE active=1"
            ).fetchone()["c"]
        else:
            rows = db.execute(
                "SELECT * FROM products WHERE active=1 AND category=? "
                "ORDER BY id DESC LIMIT ? OFFSET ?",
                (category, per_page, offset)
            ).fetchall()
            total = db.execute(
                "SELECT COUNT(*) c FROM products WHERE active=1 AND category=?",
                (category,)
            ).fetchone()["c"]

    buttons = []
    for p in rows:
        stock = effective_stock(p)
        icon = "🟢" if stock > 0 else "🔴"
        buttons.append([
            InlineKeyboardButton(
                text=f"{icon} {p['name']} • {p['price']:g} {CURRENCY}",
                callback_data=f"product:{p['id']}"
            )
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(
            text="⬅️", callback_data=f"page:{category}:{page-1}"
        ))
    if offset + len(rows) < total:
        nav.append(InlineKeyboardButton(
            text="➡️", callback_data=f"page:{category}:{page+1}"
        ))
    if nav:
        buttons.append(nav)

    buttons.append([
        InlineKeyboardButton(text="📂 Categories", callback_data="shop")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def order_details(order_id):
    with DB_LOCK:
        return db.execute(
            """SELECT o.*, p.name product_name, p.category,
                      u.tg_id, u.name user_name
               FROM orders o
               JOIN products p ON p.id=o.product_id
               JOIN users u ON u.id=o.user_id
               WHERE o.id=?""",
            (order_id,)
        ).fetchone()


def dashboard_text():
    with DB_LOCK:
        users = db.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        products = db.execute(
            "SELECT COUNT(*) c FROM products WHERE active=1"
        ).fetchone()["c"]
        pending_orders = db.execute(
            "SELECT COUNT(*) c FROM orders WHERE status='pending'"
        ).fetchone()["c"]
        completed = db.execute(
            "SELECT COUNT(*) c FROM orders WHERE status='completed'"
        ).fetchone()["c"]
        pending_payments = db.execute(
            "SELECT COUNT(*) c FROM payments WHERE status='pending'"
        ).fetchone()["c"]
        sales = db.execute(
            "SELECT COALESCE(SUM(total),0) s FROM orders "
            "WHERE status='completed'"
        ).fetchone()["s"]
        balance = db.execute(
            "SELECT COALESCE(SUM(balance),0) s FROM users"
        ).fetchone()["s"]
        codes = db.execute(
            "SELECT COUNT(*) c FROM product_codes WHERE status='available'"
        ).fetchone()["c"]

    return (
        "📊 <b>Next Level Gaming Shop — V3 Dashboard</b>\n\n"
        f"👥 Users: <b>{users}</b>\n"
        f"🛍 Active Products: <b>{products}</b>\n"
        f"🎫 Available Codes: <b>{codes}</b>\n"
        f"🧾 Pending Orders: <b>{pending_orders}</b>\n"
        f"💳 Pending Payments: <b>{pending_payments}</b>\n"
        f"✅ Completed Orders: <b>{completed}</b>\n"
        f"💵 Completed Sales: <b>{sales:.2f} {CURRENCY}</b>\n"
        f"👛 User Wallet Total: <b>{balance:.2f} {CURRENCY}</b>"
    )


# =========================
# States
# =========================
class Buy(StatesGroup):
    uid = State()
    confirm = State()


class PaymentState(StatesGroup):
    amount = State()
    method = State()
    trx = State()


class AdminState(StatesGroup):
    add_product = State()
    edit_product = State()
    add_codes = State()
    balance = State()
    broadcast = State()
    settings = State()


# =========================
# Health server for Render
# =========================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Next Level Gaming Shop V4 is running.")

    def log_message(self, *args):
        return


def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


# =========================
# User handlers
# =========================
@router.message(CommandStart())
async def start(m: Message):
    u = get_user(m.from_user)
    if u["blocked"] and not is_admin(m.from_user.id):
        return await m.answer("🚫 Your account is blocked.")

    await m.answer(
        f"🎮 <b>{shop_name()}</b>\n\n"
        "⚡ Fast & secure gaming top-up/code delivery\n"
        "💰 Wallet balance • 🧾 Order history • 🎫 Instant codes",
        reply_markup=user_menu()
    )


@router.message(Command("shop"))
@router.message(F.text == "🛒 Shop")
async def shop(m: Message):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id):
        return await m.answer("🚫 Your account is blocked.")
    await m.answer(
        "🛍 <b>Shop Categories</b>\nChoose a category:",
        reply_markup=categories_kb()
    )


@router.callback_query(F.data == "shop")
async def shop_callback(c: CallbackQuery):
    await c.answer()
    await c.message.edit_text(
        "🛍 <b>Shop Categories</b>\nChoose a category:",
        reply_markup=categories_kb()
    )


@router.callback_query(F.data.startswith("cat:"))
async def category_callback(c: CallbackQuery):
    category = c.data.split(":", 1)[1]
    await c.answer()
    await c.message.edit_text(
        f"🛍 <b>{'All Products' if category == '*' else category}</b>",
        reply_markup=products_kb(category, 0)
    )


@router.callback_query(F.data.startswith("page:"))
async def page_callback(c: CallbackQuery):
    _, category, page = c.data.split(":", 2)
    await c.answer()
    await c.message.edit_reply_markup(
        reply_markup=products_kb(category, int(page))
    )


@router.callback_query(F.data.startswith("product:"))
async def product_callback(c: CallbackQuery):
    pid = int(c.data.split(":")[1])
    with DB_LOCK:
        p = db.execute(
            "SELECT * FROM products WHERE id=? AND active=1",
            (pid,)
        ).fetchone()

    if not p:
        return await c.answer("Product unavailable.", show_alert=True)

    stock = effective_stock(p)
    text = (
        f"🎮 <b>{p['name']}</b>\n"
        f"🏷 Category: <b>{p['category']}</b>\n"
        f"📦 Quantity: <b>{p['quantity']}</b>\n"
        f"💰 Price: <b>{p['price']:g} {CURRENCY}</b>\n"
        f"{product_stock_text(p)}\n"
        f"⚡ Delivery: <b>{'Instant Code' if p['delivery_type'] == 'code' else 'Manual'}</b>\n\n"
        f"{p['description'] or 'No description.'}"
    )

    buttons = []
    if stock > 0:
        buttons.append([
            InlineKeyboardButton(
                text="🛒 Buy Now",
                callback_data=f"buy:{pid}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(
            text="⬅️ Back",
            callback_data=f"cat:{p['category']}"
        )
    ])

    await c.answer()
    await c.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("buy:"))
async def buy(c: CallbackQuery, state: FSMContext):
    pid = int(c.data.split(":")[1])
    with DB_LOCK:
        p = db.execute(
            "SELECT * FROM products WHERE id=? AND active=1",
            (pid,)
        ).fetchone()
        u = db.execute(
            "SELECT * FROM users WHERE tg_id=?", (c.from_user.id,)
        ).fetchone()

    if not p:
        return await c.answer("Product unavailable.", show_alert=True)
    if not u:
        get_user(c.from_user)
        u = get_user(c.from_user)

    if u["blocked"] and not is_admin(c.from_user.id):
        return await c.answer("Account blocked.", show_alert=True)

    if effective_stock(p) < 1:
        return await c.answer("Out of stock.", show_alert=True)

    await state.update_data(pid=pid)
    await state.set_state(Buy.uid)
    await c.answer()
    await c.message.answer(
        "🆔 <b>Send your game/player UID.</b>\n\n"
        "Send /cancel to cancel."
    )


@router.message(Buy.uid)
async def buy_uid(m: Message, state: FSMContext):
    uid = (m.text or "").strip()
    if uid.lower() == "/cancel":
        await state.clear()
        return await m.answer("❌ Cancelled.", reply_markup=user_menu())

    if len(uid) < 2 or len(uid) > 64:
        return await m.answer("❌ Please send a valid UID.")

    d = await state.get_data()
    with DB_LOCK:
        p = db.execute(
            "SELECT * FROM products WHERE id=? AND active=1",
            (d["pid"],)
        ).fetchone()
        u = db.execute(
            "SELECT * FROM users WHERE tg_id=?", (m.from_user.id,)
        ).fetchone()

    if not p or effective_stock(p) < 1:
        await state.clear()
        return await m.answer("❌ Product is out of stock.")

    if u["balance"] < p["price"]:
        await state.clear()
        return await m.answer(
            f"❌ <b>Insufficient balance</b>\n\n"
            f"Price: {fmt_money(p['price'])}\n"
            f"Balance: {fmt_money(u['balance'])}\n"
            f"Need: {fmt_money(p['price'] - u['balance'])}"
        )

    await state.update_data(game_uid=uid)
    await state.set_state(Buy.confirm)
    await m.answer(
        f"🧾 <b>Order Confirmation</b>\n\n"
        f"🎮 Product: <b>{p['name']}</b>\n"
        f"🆔 UID: <code>{uid}</code>\n"
        f"💰 Total: <b>{fmt_money(p['price'])}</b>\n\n"
        "Confirm your purchase:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Confirm", callback_data="order:confirm"),
             InlineKeyboardButton(text="❌ Cancel", callback_data="order:cancel")]
        ])
    )


@router.callback_query(Buy.confirm, F.data == "order:cancel")
async def order_cancel(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.answer("Cancelled")
    await c.message.answer("❌ Order cancelled.", reply_markup=user_menu())


@router.callback_query(Buy.confirm, F.data == "order:confirm")
async def order_confirm(c: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await state.clear()

    with DB_LOCK:
        u = db.execute(
            "SELECT * FROM users WHERE tg_id=?", (c.from_user.id,)
        ).fetchone()
        p = db.execute(
            "SELECT * FROM products WHERE id=? AND active=1",
            (d["pid"],)
        ).fetchone()

    if not u or not p:
        return await c.answer("Order unavailable.", show_alert=True)

    total = float(p["price"])

    try:
        with DB_LOCK:
            db.execute("BEGIN IMMEDIATE")

            u = db.execute(
                "SELECT * FROM users WHERE id=?", (u["id"],)
            ).fetchone()
            p = db.execute(
                "SELECT * FROM products WHERE id=?", (p["id"],)
            ).fetchone()

            if not p["active"]:
                raise RuntimeError("Product is disabled.")
            if u["balance"] < total:
                raise RuntimeError("Insufficient balance.")
            if effective_stock(p) < 1:
                raise RuntimeError("Out of stock.")

            delivered_code = None

            cur = db.execute(
                "UPDATE users SET balance=balance-?, updated_at=? "
                "WHERE id=? AND balance>=?",
                (total, now_text(), u["id"], total)
            )
            if cur.rowcount != 1:
                raise RuntimeError("Balance changed. Please retry.")

            if p["delivery_type"] == "code":
                code_row = db.execute(
                    "SELECT * FROM product_codes "
                    "WHERE product_id=? AND status='available' "
                    "ORDER BY id LIMIT 1",
                    (p["id"],)
                ).fetchone()
                if not code_row:
                    raise RuntimeError("Code stock changed. Please retry.")

                cur = db.execute(
                    "UPDATE product_codes SET status='sold', sold_to=?, sold_at=? "
                    "WHERE id=? AND status='available'",
                    (u["id"], now_text(), code_row["id"])
                )
                if cur.rowcount != 1:
                    raise RuntimeError("Code was already sold. Please retry.")
                delivered_code = code_row["code"]

                status = "completed"
            else:
                cur = db.execute(
                    "UPDATE products SET stock=stock-1, updated_at=? "
                    "WHERE id=? AND stock>0",
                    (now_text(), p["id"])
                )
                if cur.rowcount != 1:
                    raise RuntimeError("Stock changed. Please retry.")
                status = "pending"

            cur = db.execute(
                """INSERT INTO orders
                (user_id,product_id,game_uid,total,delivered_code,status)
                VALUES(?,?,?,?,?,?)""",
                (u["id"], p["id"], d["game_uid"], total, delivered_code, status)
            )
            order_id = cur.lastrowid

            if delivered_code:
                db.execute(
                    "UPDATE product_codes SET order_id=? WHERE code=?",
                    (order_id, delivered_code)
                )

            db.execute(
                "INSERT INTO balance_logs(user_id,amount,action,note) "
                "VALUES(?,?,?,?)",
                (u["id"], -total, "purchase", f"Order #{order_id}")
            )
            db.commit()

    except Exception as exc:
        with DB_LOCK:
            try:
                db.rollback()
            except Exception:
                pass
        return await c.answer(str(exc), show_alert=True)

    await c.answer("Order created")

    if delivered_code:
        text = (
            f"✅ <b>Payment Successful</b>\n\n"
            f"🧾 Order: <b>#{order_id}</b>\n"
            f"🎮 Product: <b>{p['name']}</b>\n"
            f"🆔 UID: <code>{d['game_uid']}</code>\n"
            f"💰 Paid: <b>{fmt_money(total)}</b>\n\n"
            f"🎁 <b>Your Code</b>\n"
            f"<code>{delivered_code}</code>\n\n"
            "⚠️ Keep this code private."
        )
    else:
        text = (
            f"⏳ <b>Order #{order_id} submitted</b>\n\n"
            f"🎮 Product: <b>{p['name']}</b>\n"
            f"🆔 UID: <code>{d['game_uid']}</code>\n"
            f"💰 Paid: <b>{fmt_money(total)}</b>\n"
            "📌 Status: Waiting for admin processing."
        )

    await c.message.answer(text, reply_markup=user_menu())

    if status == "pending":
        for admin_id in ADMIN_IDS:
            try:
                await c.bot.send_message(
                    admin_id,
                    f"🧾 <b>New Manual Order #{order_id}</b>\n\n"
                    f"👤 User: <code>{u['tg_id']}</code>\n"
                    f"🎮 Product: {p['name']}\n"
                    f"🆔 UID: <code>{d['game_uid']}</code>\n"
                    f"💰 Total: {fmt_money(total)}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(
                            text="✅ Complete",
                            callback_data=f"order_complete:{order_id}"
                        ),
                         InlineKeyboardButton(
                            text="❌ Reject + Refund",
                            callback_data=f"order_reject:{order_id}"
                        )]
                    ])
                )
            except Exception:
                pass


@router.message(F.text == "👤 Profile")
async def profile(m: Message):
    u = get_user(m.from_user)
    with DB_LOCK:
        orders = db.execute(
            "SELECT COUNT(*) c FROM orders WHERE user_id=?", (u["id"],)
        ).fetchone()["c"]

    await m.answer(
        f"👤 <b>Your Profile</b>\n\n"
        f"🆔 Telegram ID: <code>{u['tg_id']}</code>\n"
        f"💰 Balance: <b>{fmt_money(u['balance'])}</b>\n"
        f"🧾 Orders: <b>{orders}</b>\n"
        f"📅 Joined: <code>{u['created_at']}</code>",
        reply_markup=user_menu()
    )


@router.message(Command("orders"))
@router.message(F.text == "📦 My Orders")
async def my_orders(m: Message):
    u = get_user(m.from_user)
    with DB_LOCK:
        rows = db.execute(
            """SELECT o.id,o.total,o.status,o.created_at,p.name
               FROM orders o JOIN products p ON p.id=o.product_id
               WHERE o.user_id=? ORDER BY o.id DESC LIMIT 10""",
            (u["id"],)
        ).fetchall()

    if not rows:
        return await m.answer("📦 You have no orders yet.")

    lines = ["📦 <b>Your Recent Orders</b>\n"]
    for r in rows:
        lines.append(
            f"#{r['id']} • {r['name']}\n"
            f"💰 {fmt_money(r['total'])} • "
            f"{status_emoji(r['status'])} {r['status'].title()}\n"
            f"🕒 {r['created_at']}\n"
        )
    await m.answer("\n".join(lines))


@router.message(F.text == "💰 Add Balance")
async def add_balance(m: Message, state: FSMContext):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id):
        return await m.answer("🚫 Your account is blocked.")
    await state.set_state(PaymentState.amount)
    await m.answer(
        f"💳 <b>Add Balance</b>\n\n"
        f"Payment methods: bKash / Nagad\n"
        f"{PAYMENT_INFO}\n\n"
        "Send the amount you want to add.\n"
        "Example: <code>500</code>"
    )


@router.message(PaymentState.amount)
async def payment_amount(m: Message, state: FSMContext):
    try:
        amount = float((m.text or "").strip())
    except ValueError:
        return await m.answer("❌ Enter a valid amount.")

    if amount < 10 or amount > 100000:
        return await m.answer("❌ Amount must be between 10 and 100000.")

    await state.update_data(amount=amount)
    await state.set_state(PaymentState.method)
    await m.answer(
        "💳 Choose payment method:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="bKash", callback_data="paymethod:bkash"),
             InlineKeyboardButton(text="Nagad", callback_data="paymethod:nagad")]
        ])
    )


@router.callback_query(PaymentState.method, F.data.startswith("paymethod:"))
async def payment_method(c: CallbackQuery, state: FSMContext):
    method = c.data.split(":", 1)[1]
    await state.update_data(method=method)
    await state.set_state(PaymentState.trx)
    await c.answer()
    await c.message.answer(
        f"💳 Method: <b>{method.title()}</b>\n\n"
        "Send your payment transaction ID (TrxID).\n"
        "The same TrxID cannot be submitted twice."
    )


@router.message(PaymentState.trx)
async def payment_trx(m: Message, state: FSMContext):
    trx = (m.text or "").strip()
    if len(trx) < 3 or len(trx) > 100:
        return await m.answer("❌ Invalid TrxID.")

    d = await state.get_data()
    u = get_user(m.from_user)

    with DB_LOCK:
        exists = db.execute(
            "SELECT id FROM payments WHERE method=? AND trx_id=?",
            (d["method"], trx)
        ).fetchone()
        if exists:
            await state.clear()
            return await m.answer(
                f"❌ This TrxID was already submitted as Payment #{exists['id']}."
            )

        cur = db.execute(
            "INSERT INTO payments(user_id,amount,method,trx_id) VALUES(?,?,?,?)",
            (u["id"], d["amount"], d["method"], trx)
        )
        payment_id = cur.lastrowid
        db.commit()

    await state.clear()
    await m.answer(
        f"✅ <b>Payment Request #{payment_id}</b>\n\n"
        f"💰 Amount: {fmt_money(d['amount'])}\n"
        f"💳 Method: {d['method'].title()}\n"
        f"🧾 TrxID: <code>{trx}</code>\n"
        "⏳ Waiting for admin approval.",
        reply_markup=user_menu()
    )

    for admin_id in ADMIN_IDS:
        try:
            await m.bot.send_message(
                admin_id,
                f"💳 <b>New Payment #{payment_id}</b>\n\n"
                f"👤 User: <code>{u['tg_id']}</code>\n"
                f"💰 Amount: {fmt_money(d['amount'])}\n"
                f"Method: {d['method'].title()}\n"
                f"TrxID: <code>{trx}</code>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="✅ Credit",
                        callback_data=f"pay_credit:{payment_id}"
                    ),
                     InlineKeyboardButton(
                        text="❌ Reject",
                        callback_data=f"pay_reject:{payment_id}"
                    )]
                ])
            )
        except Exception:
            pass


@router.message(F.text == "💬 Support")
async def support(m: Message):
    await m.answer(
        f"🎧 <b>Support</b>\n\n"
        f"Contact: {SUPPORT}"
    )


@router.message(Command("cancel"))
async def cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("❌ Cancelled.", reply_markup=user_menu())


# =========================
# Admin: main
# =========================
@router.message(Command("admin"))
async def admin_command(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ Access denied.")
    await m.answer(
        "👑 <b>Next Level Gaming Shop V3</b>\nAdmin Control Center",
        reply_markup=admin_menu()
    )


@router.callback_query(F.data == "admin:dashboard")
async def admin_dashboard(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    await c.answer()
    await c.message.edit_text(
        dashboard_text(),
        reply_markup=admin_menu()
    )


# =========================
# Admin: products
# =========================
@router.callback_query(F.data == "admin:products")
async def admin_products(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    with DB_LOCK:
        rows = db.execute(
            "SELECT * FROM products ORDER BY id DESC"
        ).fetchall()

    buttons = []
    for p in rows:
        stock = effective_stock(p)
        buttons.append([
            InlineKeyboardButton(
                text=f"{'🟢' if p['active'] else '🔴'} "
                     f"{p['name'][:24]} • {stock}",
                callback_data=f"p:{p['id']}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(
            text="➕ Add Product",
            callback_data="admin:add_product"
        )
    ])
    buttons.append([
        InlineKeyboardButton(text="⬅️ Admin", callback_data="admin:dashboard")
    ])

    await c.answer()
    await c.message.edit_text(
        "🛍 <b>Product Management</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("p:"))
async def product_manage(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    pid = int(c.data.split(":")[1])
    with DB_LOCK:
        p = db.execute(
            "SELECT * FROM products WHERE id=?", (pid,)
        ).fetchone()

    if not p:
        return await c.answer("Not found", show_alert=True)

    text = (
        f"🎮 <b>{p['name']}</b>\n"
        f"🏷 Category: {p['category']}\n"
        f"📦 Quantity: {p['quantity']}\n"
        f"💰 Price: {fmt_money(p['price'])}\n"
        f"🚚 Delivery: {p['delivery_type']}\n"
        f"📊 Stock: {effective_stock(p)}\n"
        f"🔘 Active: {'Yes' if p['active'] else 'No'}\n\n"
        f"{p['description'] or 'No description.'}"
    )

    await c.answer()
    await c.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="✏️ Edit",
                callback_data=f"pedit:{pid}"
            ),
             InlineKeyboardButton(
                text="🔄 Enable/Disable",
                callback_data=f"ptoggle:{pid}"
            )],
            [InlineKeyboardButton(
                text="🎫 Add Codes",
                callback_data=f"codes_add:{pid}"
            )],
            [InlineKeyboardButton(
                text="🗑 Delete",
                callback_data=f"pdelete:{pid}"
            )],
            [InlineKeyboardButton(
                text="⬅️ Products",
                callback_data="admin:products"
            )]
        ])
    )


@router.callback_query(F.data == "admin:add_product")
async def admin_add_product_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    await c.answer()
    await state.set_state(AdminState.add_product)
    await c.message.answer(
        "➕ <b>Add Product</b>\n\n"
        "Send exactly:\n"
        "<code>Name | Category | Quantity | Price | Delivery | Stock | Description</code>\n\n"
        "For code products use Delivery = <code>code</code> and Stock = 0.\n"
        "Example:\n"
        "<code>60 UC | UC | 60 | 150 | code | 0 | 60 UC code</code>\n\n"
        "Manual product example:\n"
        "<code>UC Topup | UC | 60 | 150 | manual | 10 | Manual topup</code>"
    )


@router.message(AdminState.add_product)
async def admin_add_product(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return

    parts = [x.strip() for x in (m.text or "").split("|", 6)]
    if len(parts) != 7:
        return await m.answer("❌ Invalid format.")

    name, category, quantity, price, delivery, stock, description = parts

    try:
        quantity = int(quantity)
        price = float(price)
        stock = int(stock)
    except ValueError:
        return await m.answer("❌ Quantity, price and stock must be numbers.")

    delivery = delivery.lower()
    if delivery not in {"code", "manual"}:
        return await m.answer("❌ Delivery must be code or manual.")
    if price <= 0 or quantity < 0 or stock < 0:
        return await m.answer("❌ Invalid numeric values.")
    if delivery == "code":
        stock = 0

    with DB_LOCK:
        cur = db.execute(
            """INSERT INTO products
            (name,category,quantity,price,delivery_type,stock,description)
            VALUES(?,?,?,?,?,?,?)""",
            (name, category or "Gaming", quantity, price,
             delivery, stock, description)
        )
        pid = cur.lastrowid
        db.commit()

    admin_log(m.from_user.id, "add_product", f"product #{pid}")
    await state.clear()
    await m.answer(
        f"✅ Product #{pid} created.",
        reply_markup=admin_menu()
    )


@router.callback_query(F.data.startswith("pedit:"))
async def admin_edit_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    pid = int(c.data.split(":")[1])
    with DB_LOCK:
        p = db.execute(
            "SELECT * FROM products WHERE id=?", (pid,)
        ).fetchone()
    if not p:
        return await c.answer("Not found", show_alert=True)

    await c.answer()
    await state.update_data(pid=pid)
    await state.set_state(AdminState.edit_product)
    await c.message.answer(
        f"✏️ Editing <b>{p['name']}</b>\n\n"
        "Send:\n"
        "<code>Name | Category | Quantity | Price | Delivery | Stock | Description</code>"
    )


@router.message(AdminState.edit_product)
async def admin_edit_product(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return

    d = await state.get_data()
    parts = [x.strip() for x in (m.text or "").split("|", 6)]
    if len(parts) != 7:
        return await m.answer("❌ Invalid format.")

    name, category, quantity, price, delivery, stock, description = parts
    try:
        quantity = int(quantity)
        price = float(price)
        stock = int(stock)
    except ValueError:
        return await m.answer("❌ Invalid numbers.")

    delivery = delivery.lower()
    if delivery not in {"code", "manual"}:
        return await m.answer("❌ Delivery must be code or manual.")
    if delivery == "code":
        stock = 0

    with DB_LOCK:
        db.execute(
            """UPDATE products SET
               name=?, category=?, quantity=?, price=?,
               delivery_type=?, stock=?, description=?, updated_at=?
               WHERE id=?""",
            (name, category or "Gaming", quantity, price, delivery,
             stock, description, now_text(), d["pid"])
        )
        db.commit()

    admin_log(m.from_user.id, "edit_product", f"product #{d['pid']}")
    await state.clear()
    await m.answer("✅ Product updated.", reply_markup=admin_menu())


@router.callback_query(F.data.startswith("ptoggle:"))
async def product_toggle(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    pid = int(c.data.split(":")[1])
    with DB_LOCK:
        db.execute(
            "UPDATE products SET active=CASE active WHEN 1 THEN 0 ELSE 1 END, "
            "updated_at=? WHERE id=?",
            (now_text(), pid)
        )
        db.commit()

    admin_log(c.from_user.id, "toggle_product", f"product #{pid}")
    await c.answer("Updated")
    await product_manage(c)


@router.callback_query(F.data.startswith("pdelete:"))
async def product_delete(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    pid = int(c.data.split(":")[1])
    with DB_LOCK:
        order_count = db.execute(
            "SELECT COUNT(*) c FROM orders WHERE product_id=?",
            (pid,)
        ).fetchone()["c"]
        if order_count:
            return await c.answer(
                "Cannot delete a product with order history. Disable it instead.",
                show_alert=True
            )
        db.execute("DELETE FROM product_codes WHERE product_id=?", (pid,))
        db.execute("DELETE FROM products WHERE id=?", (pid,))
        db.commit()

    admin_log(c.from_user.id, "delete_product", f"product #{pid}")
    await c.answer("Deleted")
    await admin_products(c)


# =========================
# Admin: codes
# =========================
@router.callback_query(F.data == "admin:codes")
async def admin_codes(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    with DB_LOCK:
        rows = db.execute(
            """SELECT p.id,p.name,
                      SUM(CASE WHEN pc.status='available' THEN 1 ELSE 0 END) available,
                      SUM(CASE WHEN pc.status='sold' THEN 1 ELSE 0 END) sold
               FROM products p
               LEFT JOIN product_codes pc ON pc.product_id=p.id
               WHERE p.delivery_type='code'
               GROUP BY p.id ORDER BY p.id DESC"""
        ).fetchall()

    buttons = []
    for r in rows:
        buttons.append([
            InlineKeyboardButton(
                text=f"🎫 {r['name'][:20]} • {r['available'] or 0} available",
                callback_data=f"codes_add:{r['id']}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="⬅️ Admin", callback_data="admin:dashboard")
    ])

    await c.answer()
    await c.message.edit_text(
        "🎫 <b>Code Inventory</b>\n"
        "Select a code-based product to add inventory:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("codes_add:"))
async def codes_add_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    pid = int(c.data.split(":")[1])
    with DB_LOCK:
        p = db.execute(
            "SELECT * FROM products WHERE id=?", (pid,)
        ).fetchone()
    if not p:
        return await c.answer("Not found", show_alert=True)

    await c.answer()
    await state.update_data(pid=pid)
    await state.set_state(AdminState.add_codes)
    await c.message.answer(
        f"🎫 <b>{p['name']}</b>\n\n"
        "Send one code per line. Duplicate codes are skipped.\n\n"
        "<code>UC-CODE-001\nUC-CODE-002\nUC-CODE-003</code>"
    )


@router.message(AdminState.add_codes)
async def codes_add(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return

    d = await state.get_data()
    lines = [x.strip() for x in (m.text or "").splitlines() if x.strip()]
    if not lines:
        return await m.answer("❌ No codes found.")

    added = 0
    duplicates = 0

    with DB_LOCK:
        for raw in lines:
            try:
                db.execute(
                    "INSERT INTO product_codes(product_id,code) VALUES(?,?)",
                    (d["pid"], raw)
                )
                added += 1
            except sqlite3.IntegrityError:
                duplicates += 1
        db.commit()

    admin_log(
        m.from_user.id,
        "add_codes",
        f"product #{d['pid']} added={added} duplicates={duplicates}"
    )
    await state.clear()
    await m.answer(
        f"✅ Added: <b>{added}</b>\n"
        f"♻️ Duplicates skipped: <b>{duplicates}</b>",
        reply_markup=admin_menu()
    )


# =========================
# Admin: orders
# =========================
@router.callback_query(F.data == "admin:orders")
async def admin_orders(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    with DB_LOCK:
        rows = db.execute(
            """SELECT o.id,o.total,o.status,o.created_at,p.name,u.tg_id
               FROM orders o
               JOIN products p ON p.id=o.product_id
               JOIN users u ON u.id=o.user_id
               ORDER BY o.id DESC LIMIT 20"""
        ).fetchall()

    if not rows:
        text = "🧾 No orders yet."
    else:
        lines = ["🧾 <b>Recent Orders</b>\n"]
        for r in rows:
            lines.append(
                f"#{r['id']} • {r['name'][:18]}\n"
                f"👤 <code>{r['tg_id']}</code> • "
                f"{fmt_money(r['total'])}\n"
                f"{status_emoji(r['status'])} {r['status'].title()}\n"
            )
        text = "\n".join(lines)

    await c.answer()
    await c.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="⬅️ Admin",
                callback_data="admin:dashboard"
            )]
        ])
    )


@router.callback_query(F.data.startswith("order_complete:"))
async def manual_order_complete(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    oid = int(c.data.split(":")[1])

    with DB_LOCK:
        o = db.execute(
            "SELECT * FROM orders WHERE id=?", (oid,)
        ).fetchone()
        if not o or o["status"] != "pending":
            return await c.answer("Already processed.", show_alert=True)

        db.execute(
            "UPDATE orders SET status='completed',updated_at=? WHERE id=?",
            (now_text(), oid)
        )
        db.commit()
        u = db.execute(
            "SELECT tg_id FROM users WHERE id=?", (o["user_id"],)
        ).fetchone()

    admin_log(c.from_user.id, "complete_order", f"order #{oid}")
    await c.answer("Completed")
    await c.message.edit_text(f"✅ Order #{oid} completed.")

    try:
        await c.bot.send_message(
            u["tg_id"],
            f"✅ <b>Order #{oid} completed</b>\n"
            "Your manual top-up/order has been processed."
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("order_reject:"))
async def manual_order_reject(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    oid = int(c.data.split(":")[1])

    with DB_LOCK:
        o = db.execute(
            "SELECT * FROM orders WHERE id=?", (oid,)
        ).fetchone()
        if not o or o["status"] != "pending":
            return await c.answer("Already processed.", show_alert=True)

        db.execute(
            "UPDATE orders SET status='refunded',updated_at=? WHERE id=?",
            (now_text(), oid)
        )
        db.execute(
            "UPDATE users SET balance=balance+?,updated_at=? WHERE id=?",
            (o["total"], now_text(), o["user_id"])
        )
        db.execute(
            "UPDATE products SET stock=stock+1,updated_at=? WHERE id=?",
            (now_text(), o["product_id"])
        )
        db.execute(
            "INSERT INTO balance_logs(user_id,amount,action,note) "
            "VALUES(?,?,?,?)",
            (o["user_id"], o["total"], "refund", f"Order #{oid} rejected")
        )
        db.commit()

        u = db.execute(
            "SELECT tg_id FROM users WHERE id=?", (o["user_id"],)
        ).fetchone()

    admin_log(c.from_user.id, "reject_refund", f"order #{oid}")
    await c.answer("Rejected + refunded")
    await c.message.edit_text(
        f"↩️ Order #{oid} rejected and refunded."
    )

    try:
        await c.bot.send_message(
            u["tg_id"],
            f"↩️ <b>Order #{oid} refunded</b>\n"
            f"Refunded: {fmt_money(o['total'])}"
        )
    except Exception:
        pass


# =========================
# Admin: payments
# =========================
@router.callback_query(F.data == "admin:payments")
async def admin_payments(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    with DB_LOCK:
        rows = db.execute(
            """SELECT py.id,py.amount,py.method,py.trx_id,py.created_at,u.tg_id
               FROM payments py JOIN users u ON u.id=py.user_id
               WHERE py.status='pending'
               ORDER BY py.id DESC LIMIT 15"""
        ).fetchall()

    if not rows:
        text = "💳 No pending payments."
        kb = [[InlineKeyboardButton(
            text="⬅️ Admin", callback_data="admin:dashboard"
        )]]
    else:
        lines = ["💳 <b>Pending Payments</b>\n"]
        kb = []
        for r in rows:
            lines.append(
                f"#{r['id']} • {fmt_money(r['amount'])}\n"
                f"👤 <code>{r['tg_id']}</code> • {r['method'].title()}\n"
                f"TrxID: <code>{r['trx_id']}</code>\n"
            )
            kb.append([
                InlineKeyboardButton(
                    text=f"✅ Credit #{r['id']}",
                    callback_data=f"pay_credit:{r['id']}"
                ),
                InlineKeyboardButton(
                    text="❌ Reject",
                    callback_data=f"pay_reject:{r['id']}"
                )
            ])
        kb.append([
            InlineKeyboardButton(text="⬅️ Admin", callback_data="admin:dashboard")
        ])
        text = "\n".join(lines)

    await c.answer()
    await c.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )


@router.callback_query(F.data.startswith("pay_credit:"))
async def payment_credit(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    pid = int(c.data.split(":")[1])

    with DB_LOCK:
        try:
            db.execute("BEGIN IMMEDIATE")
            p = db.execute(
                "SELECT * FROM payments WHERE id=?", (pid,)
            ).fetchone()
            if not p or p["status"] != "pending":
                db.rollback()
                return await c.answer("Already processed.", show_alert=True)

            db.execute(
                "UPDATE payments SET status='credited',updated_at=? WHERE id=?",
                (now_text(), pid)
            )
            db.execute(
                "UPDATE users SET balance=balance+?,updated_at=? WHERE id=?",
                (p["amount"], now_text(), p["user_id"])
            )
            db.execute(
                "INSERT INTO balance_logs(user_id,amount,action,note) "
                "VALUES(?,?,?,?)",
                (p["user_id"], p["amount"], "payment", f"Payment #{pid}")
            )
            db.commit()

            u = db.execute(
                "SELECT tg_id FROM users WHERE id=?", (p["user_id"],)
            ).fetchone()
        except Exception:
            db.rollback()
            return await c.answer("Credit failed.", show_alert=True)

    admin_log(c.from_user.id, "credit_payment", f"payment #{pid}")
    await c.answer("Credited")
    await c.message.edit_text(f"✅ Payment #{pid} credited.")

    try:
        await c.bot.send_message(
            u["tg_id"],
            f"💰 <b>Balance Added</b>\n\n"
            f"Payment: #{pid}\n"
            f"Amount: <b>{fmt_money(p['amount'])}</b>"
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("pay_reject:"))
async def payment_reject(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    pid = int(c.data.split(":")[1])
    with DB_LOCK:
        cur = db.execute(
            "UPDATE payments SET status='rejected',updated_at=? "
            "WHERE id=? AND status='pending'",
            (now_text(), pid)
        )
        db.commit()

    if cur.rowcount != 1:
        return await c.answer("Already processed.", show_alert=True)

    admin_log(c.from_user.id, "reject_payment", f"payment #{pid}")
    await c.answer("Rejected")
    await c.message.edit_text(f"❌ Payment #{pid} rejected.")


# =========================
# Admin: users
# =========================
@router.callback_query(F.data == "admin:users")
async def admin_users(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    with DB_LOCK:
        rows = db.execute(
            "SELECT * FROM users ORDER BY id DESC LIMIT 20"
        ).fetchall()

    buttons = []
    for u in rows:
        buttons.append([
            InlineKeyboardButton(
                text=f"{'🚫' if u['blocked'] else '🟢'} "
                     f"{(u['name'] or 'User')[:18]} • {u['balance']:.0f}",
                callback_data=f"user:{u['id']}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="⬅️ Admin", callback_data="admin:dashboard")
    ])

    await c.answer()
    await c.message.edit_text(
        "👥 <b>User Management</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("user:"))
async def user_detail(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    uid = int(c.data.split(":")[1])
    with DB_LOCK:
        u = db.execute(
            "SELECT * FROM users WHERE id=?", (uid,)
        ).fetchone()
        orders = db.execute(
            "SELECT COUNT(*) c FROM orders WHERE user_id=?", (uid,)
        ).fetchone()["c"] if u else 0

    if not u:
        return await c.answer("Not found.", show_alert=True)

    await c.answer()
    await c.message.edit_text(
        f"👤 <b>{u['name']}</b>\n\n"
        f"Telegram ID: <code>{u['tg_id']}</code>\n"
        f"Username: @{u['username'] or '-'}\n"
        f"Balance: <b>{fmt_money(u['balance'])}</b>\n"
        f"Orders: <b>{orders}</b>\n"
        f"Status: {'🚫 Blocked' if u['blocked'] else '🟢 Active'}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="🔓 Unblock" if u["blocked"] else "🚫 Block",
                callback_data=f"user_toggle:{uid}"
            )],
            [InlineKeyboardButton(
                text="⬅️ Users",
                callback_data="admin:users"
            )]
        ])
    )


@router.callback_query(F.data.startswith("user_toggle:"))
async def user_toggle(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    uid = int(c.data.split(":")[1])
    with DB_LOCK:
        row = db.execute(
            "SELECT blocked FROM users WHERE id=?", (uid,)
        ).fetchone()
        if not row:
            return await c.answer("Not found.", show_alert=True)
        new_value = 0 if row["blocked"] else 1
        db.execute(
            "UPDATE users SET blocked=?,updated_at=? WHERE id=?",
            (new_value, now_text(), uid)
        )
        db.commit()

    admin_log(c.from_user.id, "toggle_user", f"user #{uid} -> {new_value}")
    await c.answer("Updated")
    await user_detail(c)


# =========================
# Admin: balance
# =========================
@router.callback_query(F.data == "admin:balance")
async def admin_balance_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    await c.answer()
    await state.set_state(AdminState.balance)
    await c.message.answer(
        "💰 <b>Manual Balance</b>\n\n"
        "Send:\n"
        "<code>TelegramID | amount | add/deduct | note</code>\n\n"
        "Example:\n"
        "<code>123456789 | 100 | add | Customer adjustment</code>"
    )


@router.message(AdminState.balance)
async def admin_balance(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return

    parts = [x.strip() for x in (m.text or "").split("|", 3)]
    if len(parts) != 4:
        return await m.answer("❌ Invalid format.")

    try:
        tg_id = int(parts[0])
        amount = float(parts[1])
    except ValueError:
        return await m.answer("❌ Invalid ID or amount.")

    action = parts[2].lower()
    note = parts[3]

    if amount <= 0 or action not in {"add", "deduct"}:
        return await m.answer("❌ Amount must be positive and action add/deduct.")

    with DB_LOCK:
        u = db.execute(
            "SELECT * FROM users WHERE tg_id=?", (tg_id,)
        ).fetchone()
        if not u:
            await state.clear()
            return await m.answer("❌ User not found.")

        if action == "deduct" and u["balance"] < amount:
            return await m.answer("❌ User balance is too low.")

        delta = amount if action == "add" else -amount

        db.execute(
            "UPDATE users SET balance=balance+?,updated_at=? WHERE id=?",
            (delta, now_text(), u["id"])
        )
        db.execute(
            "INSERT INTO balance_logs(user_id,amount,action,note) VALUES(?,?,?,?)",
            (u["id"], delta, f"admin_{action}", note)
        )
        db.commit()

    admin_log(
        m.from_user.id,
        f"balance_{action}",
        f"user {tg_id}, amount {amount}"
    )
    await state.clear()
    await m.answer("✅ Balance updated.", reply_markup=admin_menu())

    try:
        await m.bot.send_message(
            tg_id,
            f"💰 <b>Balance Updated</b>\n"
            f"Amount: {fmt_money(abs(delta))}\n"
            f"Action: {action}\n"
            f"Note: {note}"
        )
    except Exception:
        pass


# =========================
# Admin: broadcast
# =========================
@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    await c.answer()
    await state.set_state(AdminState.broadcast)
    await c.message.answer(
        "📢 Send the broadcast message.\n"
        "HTML formatting is supported."
    )


@router.message(AdminState.broadcast)
async def admin_broadcast(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return

    text = m.text or ""
    if not text:
        return await m.answer("❌ Text required.")

    with DB_LOCK:
        users = db.execute(
            "SELECT tg_id FROM users WHERE blocked=0"
        ).fetchall()

    sent = 0
    failed = 0
    for u in users:
        try:
            await m.bot.send_message(u["tg_id"], text)
            sent += 1
            await asyncio.sleep(0.04)
        except Exception:
            failed += 1

    admin_log(
        m.from_user.id,
        "broadcast",
        f"sent={sent}, failed={failed}"
    )
    await state.clear()
    await m.answer(
        f"📢 Broadcast finished.\n"
        f"✅ Sent: {sent}\n"
        f"❌ Failed: {failed}",
        reply_markup=admin_menu()
    )


# =========================
# Admin logs
# =========================
@router.callback_query(F.data == "admin:logs")
async def admin_logs(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    with DB_LOCK:
        rows = db.execute(
            "SELECT * FROM admin_logs ORDER BY id DESC LIMIT 20"
        ).fetchall()

    if not rows:
        text = "📝 No admin logs."
    else:
        lines = ["📝 <b>Recent Admin Logs</b>\n"]
        for r in rows:
            lines.append(
                f"#{r['id']} • {r['action']}\n"
                f"{r['details']}\n"
                f"🕒 {r['created_at']}\n"
            )
        text = "\n".join(lines)

    await c.answer()
    await c.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="⬅️ Admin",
                callback_data="admin:dashboard"
            )]
        ])
    )


# =========================
# V4 premium features
# =========================
@router.message(Command("search"))
async def search_command(m: Message):
    q = (m.text or "").split(maxsplit=1)
    term = q[1].strip() if len(q) > 1 else ""
    if not term:
        return await m.answer("🔎 Use: <code>/search PUBG</code>")
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id):
        return await m.answer("🚫 Your account is blocked.")
    like = f"%{term}%"
    with DB_LOCK:
        rows = db.execute("SELECT * FROM products WHERE active=1 AND (name LIKE ? OR category LIKE ? OR description LIKE ?) ORDER BY id DESC LIMIT 20", (like, like, like)).fetchall()
    if not rows:
        return await m.answer("🔎 No products found.")
    kb=[]
    for p in rows:
        stock=effective_stock(p)
        icon="🟢" if stock>0 else "🔴"
        kb.append([InlineKeyboardButton(text=f"{icon} {p['name']} • {p['price']:g} {CURRENCY}", callback_data=f"product:{p['id']}")])
    await m.answer(f"🔎 Results for <b>{term}</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.message(F.text == "🔎 Search")
async def search_button(m: Message):
    await m.answer("🔎 Send <code>/search product-name</code> to search the shop.")


@router.callback_query(F.data == "admin:reports")
async def v4_reports(c: CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    await c.answer()
    await c.message.edit_text(report_text(), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Admin", callback_data="admin:dashboard")]]))


@router.callback_query(F.data == "admin:backup")
async def v4_backup(c: CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    try:
        path=backup_database()
        await c.answer("Backup created")
        await c.message.answer_document(FSInputFile(str(path)), caption=f"💾 Database backup\n{path.name}")
    except Exception as e:
        await c.answer("Backup failed", show_alert=True)
        admin_log(c.from_user.id, "backup_failed", str(e))


@router.callback_query(F.data == "admin:settings")
async def v4_settings(c: CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    text=(f"⚙️ <b>Shop Settings</b>\n\n"
          f"🏪 Name: <code>{shop_name()}</code>\n"
          f"🎧 Support: <code>{setting('support', SUPPORT)}</code>\n"
          f"💳 Payment info: <code>{setting('payment_info', PAYMENT_INFO)}</code>\n"
          f"⚠️ Low stock alert: <code>{low_stock_threshold()}</code>\n"
          f"🔧 Maintenance: <code>{'ON' if setting('maintenance','0')=='1' else 'OFF'}</code>")
    kb=[[InlineKeyboardButton(text="🏪 Change Name", callback_data="set:shop_name"), InlineKeyboardButton(text="🎧 Support", callback_data="set:support")],
        [InlineKeyboardButton(text="💳 Payment Info", callback_data="set:payment_info"), InlineKeyboardButton(text="⚠️ Low Stock", callback_data="set:low_stock")],
        [InlineKeyboardButton(text="🔧 Toggle Maintenance", callback_data="set:maintenance")],
        [InlineKeyboardButton(text="⬅️ Admin", callback_data="admin:dashboard")]]
    await c.answer(); await c.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data == "set:maintenance")
async def v4_toggle_maintenance(c: CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    val="0" if setting("maintenance","0")=="1" else "1"
    set_setting("maintenance", val); admin_log(c.from_user.id,"maintenance",val)
    await c.answer("Updated")
    await v4_settings(c)


@router.callback_query(F.data.startswith("set:"))
async def v4_setting_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    key=c.data.split(":",1)[1]
    if key=="maintenance": return
    await state.update_data(setting_key=key)
    await state.set_state(AdminState.settings)
    prompts={"shop_name":"🏪 Send the new shop name.","support":"🎧 Send support username/contact.","payment_info":"💳 Send payment instructions/number.","low_stock":"⚠️ Send low-stock threshold number."}
    await c.answer(); await c.message.answer(prompts[key])


@router.message(AdminState.settings)
async def v4_setting_save(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id): return await m.answer("Denied")
    d=await state.get_data(); key=d.get("setting_key"); value=(m.text or "").strip()
    if not value: return await m.answer("❌ Value cannot be empty.")
    if key=="low_stock":
        try:
            value=str(max(0,int(value)))
        except ValueError:
            return await m.answer("❌ Send a whole number.")
        key="low_stock_threshold"
    if len(value)>500: return await m.answer("❌ Value too long.")
    set_setting(key,value); await state.clear(); admin_log(m.from_user.id,"setting_changed",f"{key}={value}")
    await m.answer(f"✅ Setting updated: <b>{key}</b>", reply_markup=admin_menu())


@router.callback_query(F.data == "admin:lowstock")
async def v4_lowstock(c: CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    with DB_LOCK:
        rows=db.execute("SELECT id,name,stock,price FROM products WHERE active=1 AND stock<=? ORDER BY stock ASC",(low_stock_threshold(),)).fetchall()
    if not rows: text="✅ No low-stock products."
    else: text="⚠️ <b>Low Stock</b>\n\n"+"\n".join(f"#{r['id']} {r['name']} — {r['stock']} left" for r in rows)
    await c.answer(); await c.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]]))


@router.message(Command("export_orders"))
async def v4_export_orders(m: Message):
    if not is_admin(m.from_user.id): return await m.answer("Denied")
    path=Path("orders_export.csv")
    with DB_LOCK:
        rows=db.execute("""SELECT o.id,u.tg_id,u.username,p.name,o.game_uid,o.total,o.status,o.created_at,o.updated_at FROM orders o JOIN users u ON u.id=o.user_id JOIN products p ON p.id=o.product_id ORDER BY o.id DESC""").fetchall()
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["order_id","tg_id","username","product","game_uid","total","status","created_at","updated_at"])
        for r in rows: w.writerow(list(r))
    await m.answer_document(FSInputFile(str(path)),caption="📄 Orders export")


@router.message(Command("lowstock"))
async def v4_lowstock_command(m: Message):
    if not is_admin(m.from_user.id): return await m.answer("Denied")
    with DB_LOCK:
        rows=db.execute("SELECT name,stock FROM products WHERE active=1 AND stock<=? ORDER BY stock",(low_stock_threshold(),)).fetchall()
    if not rows: return await m.answer("✅ No low-stock products.")
    await m.answer("⚠️ <b>Low Stock Alert</b>\n\n"+"\n".join(f"• {r['name']}: <b>{r['stock']}</b>" for r in rows))

# =========================
# Error-safe blocked-user guard
# =========================
@router.message()
async def fallback(m: Message):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id):
        return await m.answer("🚫 Your account is blocked.")
    if setting("maintenance", "0") == "1" and not is_admin(m.from_user.id):
        return await m.answer("🔧 Shop is temporarily under maintenance. Please try again later.")
    await m.answer(
        "Use the menu below or /shop to continue.",
        reply_markup=user_menu()
    )


# =========================
# Main
# =========================
async def main():
    start_health_server()
    bot = Bot(
        TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
