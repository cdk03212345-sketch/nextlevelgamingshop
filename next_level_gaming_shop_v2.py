import os
import sqlite3
import asyncio
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
PAYMENT_INFO = os.getenv("PAYMENT_INSTRUCTIONS", "bKash/Nagad: YOUR NUMBER")
SUPPORT = os.getenv("SUPPORT_USERNAME", "@YourSupport")
CURRENCY = os.getenv("CURRENCY", "BDT")

if not TOKEN:
    raise RuntimeError("BOT_TOKEN missing")

# ---------- Database ----------
db = sqlite3.connect("nextlevel.db", check_same_thread=False)
db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")

db.executescript("""
CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_id INTEGER UNIQUE NOT NULL,
    username TEXT,
    name TEXT,
    balance REAL DEFAULT 0,
    blocked INTEGER DEFAULT 0,
    referred_by INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    category TEXT DEFAULT 'Gaming',
    quantity INTEGER DEFAULT 0,
    price REAL NOT NULL,
    stock INTEGER DEFAULT 0,
    active INTEGER DEFAULT 1,
    description TEXT DEFAULT '',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_codes(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    code TEXT UNIQUE NOT NULL,
    status TEXT DEFAULT 'available',
    sold_to INTEGER,
    order_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    used_at DATETIME
);

CREATE TABLE IF NOT EXISTS orders(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    game_uid TEXT,
    total REAL NOT NULL,
    discount REAL DEFAULT 0,
    promo_code TEXT,
    delivered_code TEXT,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payments(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    method TEXT,
    trx_id TEXT,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS promocodes(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE NOT NULL,
    discount_type TEXT NOT NULL,
    discount_value REAL NOT NULL,
    max_use INTEGER DEFAULT 0,
    used_count INTEGER DEFAULT 0,
    active INTEGER DEFAULT 1,
    expires_at DATETIME
);

CREATE TABLE IF NOT EXISTS promo_uses(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    promo_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    used_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(promo_id, user_id)
);

CREATE TABLE IF NOT EXISTS referrals(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    referrer_id INTEGER NOT NULL,
    referred_id INTEGER UNIQUE NOT NULL,
    bonus REAL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS balance_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    action TEXT NOT NULL,
    note TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS admin_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_tg_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    details TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
""")

# Migrate old databases safely.
for statement in [
    "ALTER TABLE users ADD COLUMN referred_by INTEGER",
    "ALTER TABLE users ADD COLUMN created_at DATETIME",
    "ALTER TABLE products ADD COLUMN description TEXT DEFAULT ''",
    "ALTER TABLE orders ADD COLUMN discount REAL DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN promo_code TEXT",
    "ALTER TABLE orders ADD COLUMN delivered_code TEXT",
]:
    try:
        db.execute(statement)
    except sqlite3.OperationalError:
        pass
db.commit()

# ---------- Render health server ----------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *_):
        pass

def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

# ---------- Helpers ----------
router = Router()

def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS

def now_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛒 Shop"), KeyboardButton(text="👤 Profile")],
            [KeyboardButton(text="💰 Add Balance"), KeyboardButton(text="📦 My Orders")],
            [KeyboardButton(text="🧾 Order History"), KeyboardButton(text="💬 Support")],
            [KeyboardButton(text="💬 Support")]
        ],
        resize_keyboard=True
    )

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Statistics", callback_data="admin:stats"),
         InlineKeyboardButton(text="📢 Broadcast", callback_data="admin:broadcast")],
        [InlineKeyboardButton(text="📦 Products", callback_data="admin:products"),
         InlineKeyboardButton(text="➕ Add Product", callback_data="admin:addproduct")],
        [InlineKeyboardButton(text="🎫 Codes", callback_data="admin:codes"),
         InlineKeyboardButton(text="💰 Balance", callback_data="admin:balance")],
        [InlineKeyboardButton(text="👤 Users", callback_data="admin:users")],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin:panel")]
    ])

def product_order_sql():
    return """
    CASE category
        WHEN 'UC' THEN 1
        WHEN 'Shell' THEN 2
        ELSE 3
    END, quantity, id
    """

def products_kb():
    rows = db.execute(
        f"SELECT * FROM products WHERE active=1 ORDER BY {product_order_sql()}"
    ).fetchall()
    buttons = []
    for p in rows:
        available_codes = db.execute(
            "SELECT COUNT(*) c FROM product_codes WHERE product_id=? AND status='available'",
            (p["id"],)
        ).fetchone()["c"]
        stock = available_codes if available_codes > 0 else p["stock"]
        buttons.append([
            InlineKeyboardButton(
                text=f"🎮 {p['name']} — {p['price']:g} {CURRENCY} | 📦 {stock}",
                callback_data=f"product:{p['id']}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def user_is_blocked(tg_id: int) -> bool:
    row = db.execute("SELECT blocked FROM users WHERE tg_id=?", (tg_id,)).fetchone()
    return bool(row and row["blocked"])

def get_user(tg):
    u = db.execute("SELECT * FROM users WHERE tg_id=?", (tg.id,)).fetchone()
    if not u:
        db.execute(
            "INSERT INTO users(tg_id,username,name) VALUES(?,?,?)",
            (tg.id, tg.username, tg.full_name)
        )
        db.commit()
        u = db.execute("SELECT * FROM users WHERE tg_id=?", (tg.id,)).fetchone()
    else:
        db.execute(
            "UPDATE users SET username=?,name=? WHERE tg_id=?",
            (tg.username, tg.full_name, tg.id)
        )
        db.commit()
        u = db.execute("SELECT * FROM users WHERE tg_id=?", (tg.id,)).fetchone()
    return u

def admin_log(admin_id, action, details=""):
    db.execute(
        "INSERT INTO admin_logs(admin_tg_id,action,details) VALUES(?,?,?)",
        (admin_id, action, details)
    )
    db.commit()

def available_code_count(pid):
    return db.execute(
        "SELECT COUNT(*) c FROM product_codes WHERE product_id=? AND status='available'",
        (pid,)
    ).fetchone()["c"]

def effective_stock(p):
    count = available_code_count(p["id"])
    return count if count else p["stock"]

def validate_promo(code, user_id, subtotal):
    code = code.strip().upper()
    p = db.execute("SELECT * FROM promocodes WHERE code=? AND active=1", (code,)).fetchone()
    if not p:
        return None, 0, "❌ Promo code not found or inactive."
    if p["max_use"] and p["used_count"] >= p["max_use"]:
        return None, 0, "❌ This promo code has reached its usage limit."
    if p["expires_at"]:
        try:
            if datetime.now(timezone.utc) > datetime.fromisoformat(
                p["expires_at"].replace("Z", "+00:00")
            ):
                return None, 0, "❌ This promo code has expired."
        except ValueError:
            pass
    used = db.execute(
        "SELECT 1 FROM promo_uses WHERE promo_id=? AND user_id=?",
        (p["id"], user_id)
    ).fetchone()
    if used:
        return None, 0, "❌ You already used this promo code."
    if p["discount_type"] == "percent":
        discount = subtotal * min(p["discount_value"], 100) / 100
    else:
        discount = min(p["discount_value"], subtotal)
    return p, round(discount, 2), None

# ---------- States ----------
class Buy(StatesGroup):
    uid = State()
    confirm = State()

class Pay(StatesGroup):
    amount = State()
    method = State()
    trx = State()

class AdminAction(StatesGroup):
    broadcast = State()
    add_product = State()
    edit_product = State()
    add_code = State()
    add_promo = State()
    balance_user = State()

# ---------- User commands ----------
@router.message(CommandStart())
async def start(m: Message):
    u = get_user(m.from_user)
    args = (m.text or "").split(maxsplit=1)
    if len(args) == 2:
        ref = args[1].strip()
        if ref.isdigit():
            ref_tg = int(ref)
            if ref_tg != m.from_user.id and not u["referred_by"]:
                ref_user = db.execute(
                    "SELECT * FROM users WHERE tg_id=?", (ref_tg,)
                ).fetchone()
                if ref_user and not db.execute(
                    "SELECT 1 FROM referrals WHERE referred_id=?", (u["id"],)
                ).fetchone():
                    db.execute(
                        "UPDATE users SET referred_by=? WHERE id=?",
                        (ref_user["id"], u["id"])
                    )
                    db.execute(
                        "INSERT INTO referrals(referrer_id,referred_id,bonus) VALUES(?,?,?)",
                        (ref_user["id"], u["id"], REFERRAL_BONUS)
                    )
                    db.execute(
                        "UPDATE users SET balance=balance+? WHERE id=?",
                        (REFERRAL_BONUS, ref_user["id"])
                    )
                    db.execute(
                        "INSERT INTO balance_logs(user_id,amount,action,note) VALUES(?,?,?,?)",
                        (ref_user["id"], REFERRAL_BONUS, "referral", f"Referral: {m.from_user.id}")
                    )
                    db.commit()
                    try:
                        await m.bot.send_message(
                            ref_user["tg_id"],
                            f"🎉 Referral bonus: +{REFERRAL_BONUS:g} {CURRENCY}"
                        )
                    except Exception:
                        pass
    await m.answer(
        f"🎮 <b>Welcome to Next Level Gaming Shop!</b>\n\n"
        f"💰 Balance: <b>{u['balance']:.2f} {CURRENCY}</b>\n"
        f"🛒 Choose an option below.",
        reply_markup=menu()
    )

@router.message(Command("shop"))
@router.message(F.text == "🛒 Shop")
async def shop(m: Message):
    if not db.execute("SELECT 1 FROM products WHERE active=1").fetchone():
        return await m.answer("❌ No products available.")
    await m.answer("🛒 <b>Choose a product:</b>", reply_markup=products_kb())

@router.callback_query(F.data.startswith("product:"))
async def product(c: CallbackQuery):
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    await c.answer()
    if not p or not p["active"]:
        return await c.message.answer("❌ Product not found.")
    stock = effective_stock(p)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Buy Now", callback_data=f"buy:{pid}")],
        [InlineKeyboardButton(text="⬅️ Shop", callback_data="shop")]
    ])
    await c.message.edit_text(
        f"🎮 <b>{p['name']}</b>\n"
        f"🏷 Category: {p['category'] or 'Gaming'}\n"
        f"📦 Quantity: {p['quantity']}\n"
        f"💰 Price: {p['price']:g} {CURRENCY}\n"
        f"📦 Stock: {stock}\n\n"
        f"{p['description'] or 'No description.'}",
        reply_markup=kb
    )

@router.callback_query(F.data == "shop")
async def shop_cb(c: CallbackQuery):
    await c.answer()
    await c.message.edit_text("🛒 <b>Choose a product:</b>", reply_markup=products_kb())

@router.callback_query(F.data.startswith("buy:"))
async def buy(c: CallbackQuery, state: FSMContext):
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT * FROM products WHERE id=? AND active=1", (pid,)).fetchone()
    if not p or effective_stock(p) < 1:
        return await c.answer("Out of stock", show_alert=True)
    await state.update_data(pid=pid)
    await state.set_state(Buy.uid)
    await c.answer()
    await c.message.answer("🆔 Send your game/player UID.\n\nSend /cancel to cancel.")

@router.message(Buy.uid)
async def uid(m: Message, state: FSMContext):
    if m.text and m.text.lower() == "/cancel":
        await state.clear()
        return await m.answer("❌ Cancelled.", reply_markup=menu())
    d = await state.get_data()
    p = db.execute("SELECT * FROM products WHERE id=?", (d["pid"],)).fetchone()
    u = get_user(m.from_user)
    if not p or not p["active"] or effective_stock(p) < 1:
        await state.clear()
        return await m.answer("❌ Out of stock.")
    if u["balance"] < p["price"]:
        await state.clear()
        return await m.answer(
            f"❌ Insufficient balance.\nYour balance: {u['balance']:.2f} {CURRENCY}"
        )
    await state.update_data(game_uid=m.text.strip(), promo=None, discount=0)
    await state.set_state(Buy.confirm)
    await m.answer(
        f"💰 Price: <b>{p['price']:g} {CURRENCY}</b>\n"
        "Reply <b>YES</b> to confirm or <b>NO</b> to cancel."
    )

@router.message(Buy.confirm)
async def confirm_order(m: Message, state: FSMContext):
    answer = (m.text or "").strip().upper()
    if answer in {"NO", "N", "/CANCEL"}:
        await state.clear()
        return await m.answer("❌ Order cancelled.", reply_markup=menu())
    if answer not in {"YES", "Y"}:
        return await m.answer("Reply YES to confirm or NO to cancel.")

    d = await state.get_data()
    p = db.execute("SELECT * FROM products WHERE id=?", (d["pid"],)).fetchone()
    u = get_user(m.from_user)
    if not p or not p["active"] or effective_stock(p) < 1:
        await state.clear()
        return await m.answer("❌ Out of stock.")

    total = float(p["price"])
    if u["balance"] < total:
        await state.clear()
        return await m.answer(
            f"❌ Insufficient balance.\nNeed: {total:.2f} {CURRENCY}\n"
            f"Balance: {u['balance']:.2f} {CURRENCY}"
        )

    code_row = db.execute(
        "SELECT * FROM product_codes WHERE product_id=? AND status='available' ORDER BY id LIMIT 1",
        (p["id"],)
    ).fetchone()

    try:
        db.execute("BEGIN")
        cur = db.execute(
            "UPDATE users SET balance=balance-? WHERE id=? AND balance>=?",
            (total, u["id"], total)
        )
        if cur.rowcount != 1:
            raise RuntimeError("Insufficient balance or balance changed; please retry.")

        delivered_code = None
        if code_row:
            cur = db.execute(
                """UPDATE product_codes
                   SET status='sold', sold_to=?, used_at=?
                   WHERE id=? AND status='available'""",
                (u["id"], now_text(), code_row["id"])
            )
            if cur.rowcount != 1:
                raise RuntimeError("Code was already sold; please retry.")
            delivered_code = code_row["code"]
        else:
            cur = db.execute(
                "UPDATE products SET stock=stock-1 WHERE id=? AND stock>0",
                (p["id"],)
            )
            if cur.rowcount != 1:
                raise RuntimeError("Stock changed; please retry.")

        cur = db.execute(
            """INSERT INTO orders
            (user_id,product_id,game_uid,total,discount,promo_code,delivered_code,status)
            VALUES(?,?,?,?,?,?,?,?)""",
            (u["id"], p["id"], d["game_uid"], total, 0, None, delivered_code, "pending")
        )
        oid = cur.lastrowid

        if delivered_code:
            db.execute(
                "UPDATE product_codes SET order_id=? WHERE code=? AND status='sold'",
                (oid, delivered_code)
            )

        db.execute(
            "INSERT INTO balance_logs(user_id,amount,action,note) VALUES(?,?,?,?)",
            (u["id"], -total, "purchase", f"Order #{oid}")
        )
        db.commit()
    except Exception as e:
        db.rollback()
        await state.clear()
        return await m.answer(f"❌ Order failed: {e}")

    await state.clear()
    delivery = (
        f"\n\n🎁 <b>Your Code:</b>\n<code>{delivered_code}</code>"
        if delivered_code else
        "\n\n⏳ Waiting for admin approval."
    )
    await m.answer(
        f"✅ <b>Order #{oid} created!</b>\n"
        f"🎮 Product: {p['name']}\n"
        f"🆔 UID: <code>{d['game_uid']}</code>\n"
        f"💰 Total: {total:.2f} {CURRENCY}\n"
        f"📌 Status: ⏳ Pending{delivery}",
        reply_markup=menu()
    )

    for aid in ADMIN_IDS:
        try:
            await m.bot.send_message(
                aid,
                f"🧾 <b>New Order #{oid}</b>\n"
                f"User: <code>{u['tg_id']}</code>\n"
                f"Product: {p['name']}\n"
                f"UID: <code>{d['game_uid']}</code>\n"
                f"Total: {total:.2f} {CURRENCY}\n"
                f"Code delivered: {'Yes' if delivered_code else 'No'}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve:{oid}"),
                     InlineKeyboardButton(text="❌ Reject", callback_data=f"reject:{oid}")]
                ])
            )
        except Exception:
            pass

@router.message(F.text == "👤 Profile")
async def profile(m: Message):
    u = get_user(m.from_user)
    refs = db.execute(
        "SELECT COUNT(*) c FROM referrals WHERE referrer_id=?", (u["id"],)
    ).fetchone()["c"]
    await m.answer(
        f"👤 <b>Profile</b>\n"
        f"ID: <code>{u['tg_id']}</code>\n"
        f"Balance: <b>{u['balance']:.2f} {CURRENCY}</b>\n"
        f"👥 Referrals: <b>{refs}</b>"
    )

@router.message(F.text == "📦 My Orders")
async def orders(m: Message):
    u = get_user(m.from_user)
    rows = db.execute(
        """SELECT o.*,p.name FROM orders o JOIN products p ON p.id=o.product_id
        WHERE o.user_id=? ORDER BY o.id DESC LIMIT 10""",
        (u["id"],)
    ).fetchall()
    if not rows:
        return await m.answer("📦 No orders yet.")
    lines = []
    for r in rows:
        lines.append(
            f"#{r['id']} — {r['name']} — {r['total']:.2f} {CURRENCY} — {r['status']}"
        )
    await m.answer("📦 <b>Your orders</b>\n\n" + "\n".join(lines))

@router.message(F.text == "💰 Add Balance")
async def add_balance(m: Message, state: FSMContext):
    await state.set_state(Pay.amount)
    await m.answer(
        f"💳 <b>Add Balance</b>\n\n{PAYMENT_INFO}\n\n"
        "Send the amount you paid.\nSend /cancel to cancel."
    )

@router.message(Pay.amount)
async def pay_amount(m: Message, state: FSMContext):
    if m.text and m.text.lower() == "/cancel":
        await state.clear()
        return await m.answer("❌ Cancelled.", reply_markup=menu())
    try:
        amount = float(m.text)
    except ValueError:
        return await m.answer("❌ Send a valid amount.")
    if amount <= 0:
        return await m.answer("❌ Amount must be positive.")
    await state.update_data(amount=amount)
    await state.set_state(Pay.method)
    await m.answer("Send payment method (bKash/Nagad/etc.):")

@router.message(Pay.method)
async def pay_method(m: Message, state: FSMContext):
    await state.update_data(method=m.text.strip())
    await state.set_state(Pay.trx)
    await m.answer("Send your TrxID:")

@router.message(Pay.trx)
async def pay_trx(m: Message, state: FSMContext):
    d = await state.get_data()
    u = get_user(m.from_user)
    cur = db.execute(
        "INSERT INTO payments(user_id,amount,method,trx_id) VALUES(?,?,?,?)",
        (u["id"], d["amount"], d["method"], m.text.strip())
    )
    db.commit()
    pid = cur.lastrowid
    await state.clear()
    await m.answer(f"✅ Payment request #{pid} submitted. Admin will verify it.")
    for aid in ADMIN_IDS:
        try:
            await m.bot.send_message(
                aid,
                f"💳 <b>Payment #{pid}</b>\n"
                f"User: <code>{u['tg_id']}</code>\n"
                f"Amount: {d['amount']} {CURRENCY}\n"
                f"Method: {d['method']}\n"
                f"TrxID: <code>{m.text.strip()}</code>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Credit", callback_data=f"credit:{pid}"),
                     InlineKeyboardButton(text="❌ Reject", callback_data=f"payreject:{pid}")]
                ])
            )
        except Exception:
            pass

@router.message(F.text == "💬 Support")
async def support(m: Message):
    await m.answer(f"💬 Support: {SUPPORT}")

# ---------- Admin ----------
@router.message(Command("admin"))
async def admin_command(m: Message, state: FSMContext):
    await state.clear()
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Access denied.")
    await m.answer("🛠 <b>Premium Admin Panel</b>", reply_markup=admin_kb())

@router.message(Command("cancel"))
async def cancel_command(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("❌ Cancelled.", reply_markup=menu())

def admin_stats_text():
    users = db.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
    products = db.execute("SELECT COUNT(*) c FROM products").fetchone()["c"]
    active = db.execute("SELECT COUNT(*) c FROM products WHERE active=1").fetchone()["c"]
    codes = db.execute("SELECT COUNT(*) c FROM product_codes WHERE status='available'").fetchone()["c"]
    orders = db.execute("SELECT COUNT(*) c FROM orders").fetchone()["c"]
    completed = db.execute("SELECT COUNT(*) c FROM orders WHERE status='completed'").fetchone()["c"]
    pending = db.execute("SELECT COUNT(*) c FROM orders WHERE status='pending'").fetchone()["c"]
    payments = db.execute("SELECT COUNT(*) c FROM payments").fetchone()["c"]
    pp = db.execute("SELECT COUNT(*) c FROM payments WHERE status='pending'").fetchone()["c"]
    balance = db.execute("SELECT COALESCE(SUM(balance),0) total FROM users").fetchone()["total"]
    sales = db.execute(
        "SELECT COALESCE(SUM(total),0) total FROM orders WHERE status='completed'"
    ).fetchone()["total"]
    return (
        f"📊 <b>Statistics</b>\n\n"
        f"👥 Users: <b>{users}</b>\n"
        f"🛒 Products: <b>{products}</b> (Active {active})\n"
        f"🎫 Available codes: <b>{codes}</b>\n"
        f"🧾 Orders: <b>{orders}</b>\n"
        f"⏳ Pending orders: <b>{pending}</b>\n"
        f"✅ Completed: <b>{completed}</b>\n"
        f"💳 Payments: <b>{payments}</b> (Pending {pp})\n"
        f"💰 User balances: <b>{balance:.2f} {CURRENCY}</b>\n"
        f"📈 Completed sales: <b>{sales:.2f} {CURRENCY}</b>"
    )

@router.callback_query(F.data == "admin:panel")
async def admin_panel(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    await c.answer()
    await c.message.edit_text("🛠 <b>Premium Admin Panel</b>", reply_markup=admin_kb())

@router.callback_query(F.data == "admin:stats")
async def admin_stats(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    await c.answer()
    await c.message.edit_text(
        admin_stats_text(),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:panel")]
        ])
    )

# ----- Product management -----
@router.callback_query(F.data == "admin:products")
async def admin_products(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    rows = db.execute("SELECT * FROM products ORDER BY id DESC").fetchall()
    kb = []
    for p in rows:
        kb.append([
            InlineKeyboardButton(
                text=f"{'🟢' if p['active'] else '🔴'} {p['name']} | {p['price']:g}",
                callback_data=f"pmanage:{p['id']}"
            )
        ])
    kb += [
        [InlineKeyboardButton(text="➕ Add Product", callback_data="admin:addproduct")],
        [InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:panel")]
    ]
    await c.answer()
    await c.message.edit_text(
        "🛒 <b>Product Management</b>\nSelect a product:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

@router.callback_query(F.data.startswith("pmanage:"))
async def product_manage(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    if not p:
        return await c.answer("Not found", show_alert=True)
    await c.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Edit", callback_data=f"pedit:{pid}"),
         InlineKeyboardButton(text="🗑 Delete", callback_data=f"pdelete:{pid}")],
        [InlineKeyboardButton(text="🎫 Add Codes", callback_data=f"codeadd:{pid}")],
        [InlineKeyboardButton(text="🔄 Toggle", callback_data=f"toggle:{pid}")],
        [InlineKeyboardButton(text="⬅️ Products", callback_data="admin:products")]
    ])
    await c.message.edit_text(
        f"🎮 <b>{p['name']}</b>\n"
        f"Category: {p['category'] or 'Gaming'}\n"
        f"Quantity: {p['quantity']}\n"
        f"Price: {p['price']:g} {CURRENCY}\n"
        f"Manual stock: {p['stock']}\n"
        f"Code stock: {available_code_count(pid)}\n"
        f"Active: {'Yes' if p['active'] else 'No'}\n\n"
        f"{p['description'] or ''}",
        reply_markup=kb
    )

@router.callback_query(F.data == "admin:addproduct")
async def admin_add_product_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    await c.answer()
    await state.set_state(AdminAction.add_product)
    await c.message.answer(
        "➕ Send product as:\n"
        "<code>Name | Category | Quantity | Price | Stock | Description</code>\n\n"
        "Example:\n"
        "<code>60 UC | UC | 60 | 55 | 0 | 60 UC code</code>"
    )

@router.message(AdminAction.add_product)
async def admin_add_product(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return
    parts = [x.strip() for x in m.text.split("|", 5)]
    if len(parts) != 6:
        return await m.answer("❌ Format invalid. Use Name | Category | Quantity | Price | Stock | Description")
    try:
        name, category, quantity, price, stock, description = parts
        quantity, price, stock = int(quantity), float(price), int(stock)
        if price < 0 or quantity < 0 or stock < 0:
            raise ValueError
    except ValueError:
        return await m.answer("❌ Quantity/price/stock values are invalid.")
    db.execute(
        """INSERT INTO products(name,category,quantity,price,stock,active,description)
        VALUES(?,?,?,?,?,1,?)""",
        (name, category, quantity, price, stock, description)
    )
    db.commit()
    admin_log(m.from_user.id, "add_product", name)
    await state.clear()
    await m.answer(f"✅ Product <b>{name}</b> added.", reply_markup=admin_kb())

@router.callback_query(F.data.startswith("pedit:"))
async def admin_edit_product_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    if not p:
        return await c.answer("Not found", show_alert=True)
    await c.answer()
    await state.update_data(pid=pid)
    await state.set_state(AdminAction.edit_product)
    await c.message.answer(
        f"✏️ Current: <b>{p['name']}</b>\n"
        "Send:\n<code>Name | Category | Quantity | Price | Stock | Description</code>"
    )

@router.message(AdminAction.edit_product)
async def admin_edit_product(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return
    parts = [x.strip() for x in m.text.split("|", 5)]
    if len(parts) != 6:
        return await m.answer("❌ Invalid format.")
    try:
        name, category, quantity, price, stock, description = parts
        quantity, price, stock = int(quantity), float(price), int(stock)
    except ValueError:
        return await m.answer("❌ Invalid numbers.")
    d = await state.get_data()
    db.execute(
        """UPDATE products SET name=?,category=?,quantity=?,price=?,stock=?,description=?
        WHERE id=?""",
        (name, category, quantity, price, stock, description, d["pid"])
    )
    db.commit()
    admin_log(m.from_user.id, "edit_product", f"#{d['pid']} {name}")
    await state.clear()
    await m.answer("✅ Product updated.", reply_markup=admin_kb())

@router.callback_query(F.data.startswith("pdelete:"))
async def admin_delete_product(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    if not p:
        return await c.answer("Not found", show_alert=True)
    db.execute("UPDATE products SET active=0 WHERE id=?", (pid,))
    db.commit()
    admin_log(c.from_user.id, "disable_product", f"#{pid} {p['name']}")
    await c.answer("Product disabled")
    await c.message.edit_text("✅ Product disabled.", reply_markup=admin_kb())

@router.callback_query(F.data.startswith("toggle:"))
async def admin_toggle(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT active,name FROM products WHERE id=?", (pid,)).fetchone()
    if not p:
        return await c.answer("Not found", show_alert=True)
    new = 0 if p["active"] else 1
    db.execute("UPDATE products SET active=? WHERE id=?", (new, pid))
    db.commit()
    admin_log(c.from_user.id, "toggle_product", f"#{pid} -> {new}")
    await c.answer("Enabled" if new else "Disabled")

# ----- Code inventory -----
@router.callback_query(F.data == "admin:codes")
async def admin_codes(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    rows = db.execute(
        """SELECT p.id,p.name,COUNT(pc.id) c
        FROM products p LEFT JOIN product_codes pc
        ON p.id=pc.product_id AND pc.status='available'
        GROUP BY p.id ORDER BY p.id"""
    ).fetchall()
    kb = [[InlineKeyboardButton(text=f"{r['name']} — {r['c']} codes", callback_data=f"codeadd:{r['id']}")] for r in rows]
    kb.append([InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:panel")])
    await c.answer()
    await c.message.edit_text("🎫 <b>Code Inventory</b>\nSelect a product:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("codeadd:"))
async def code_add_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT name FROM products WHERE id=?", (pid,)).fetchone()
    if not p:
        return await c.answer("Not found", show_alert=True)
    await c.answer()
    await state.update_data(pid=pid)
    await state.set_state(AdminAction.add_code)
    await c.message.answer(
        f"🎫 <b>{p['name']}</b>\n"
        "Send codes one per line.\n\n"
        "Example:\n<code>ABC-123\nXYZ-456\nTEST-789</code>"
    )

@router.message(AdminAction.add_code)
async def code_add(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return
    d = await state.get_data()
    codes = [x.strip() for x in m.text.splitlines() if x.strip()]
    added = 0
    for code in codes:
        try:
            db.execute(
                "INSERT INTO product_codes(product_id,code) VALUES(?,?)",
                (d["pid"], code)
            )
            added += 1
        except sqlite3.IntegrityError:
            pass
    db.commit()
    admin_log(m.from_user.id, "add_codes", f"product #{d['pid']}: {added}")
    await state.clear()
    await m.answer(f"✅ Added {added} code(s).", reply_markup=admin_kb())

# ----- Promo management -----
# ----- Balance admin -----
@router.callback_query(F.data == "admin:balance")
async def admin_balance_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    await c.answer()
    await state.set_state(AdminAction.balance_user)
    await c.message.answer(
        "💰 Send:\n<code>TelegramID | amount | add/deduct | note</code>\n\n"
        "Example: <code>123456789 | 100 | add | Manual bonus</code>"
    )

@router.message(AdminAction.balance_user)
async def admin_balance_set(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return
    parts = [x.strip() for x in m.text.split("|", 3)]
    if len(parts) != 4:
        return await m.answer("❌ Invalid format.")
    try:
        tg_id, amount = int(parts[0]), float(parts[1])
    except ValueError:
        return await m.answer("❌ Invalid ID/amount.")
    action = parts[2].lower()
    note = parts[3]
    if amount <= 0 or action not in {"add", "deduct"}:
        return await m.answer("❌ Use positive amount and add/deduct.")
    u = db.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,)).fetchone()
    if not u:
        await state.clear()
        return await m.answer("❌ User not found.")
    delta = amount if action == "add" else -amount
    if action == "deduct" and u["balance"] < amount:
        return await m.answer("❌ User balance is too low.")
    db.execute("UPDATE users SET balance=balance+? WHERE id=?", (delta, u["id"]))
    db.execute(
        "INSERT INTO balance_logs(user_id,amount,action,note) VALUES(?,?,?,?)",
        (u["id"], delta, f"admin_{action}", note)
    )
    db.commit()
    admin_log(m.from_user.id, f"balance_{action}", f"user {tg_id}: {amount}")
    await state.clear()
    await m.answer("✅ Balance updated.", reply_markup=admin_kb())
    try:
        await m.bot.send_message(
            tg_id,
            f"💰 Balance {'added' if action == 'add' else 'deducted'}: "
            f"{amount:g} {CURRENCY}\nNote: {note}"
        )
    except Exception:
        pass

# ----- Broadcast -----
@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_start(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    await c.answer()
    await state.set_state(AdminAction.broadcast)
    await c.message.answer("📢 Send the broadcast message now.\nSend /cancel to cancel.")

@router.message(AdminAction.broadcast)
async def admin_broadcast_send(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return
    if m.text and m.text.strip().lower() == "/cancel":
        await state.clear()
        return await m.answer("❌ Broadcast cancelled.")
    rows = db.execute("SELECT tg_id FROM users WHERE blocked=0").fetchall()
    sent = failed = 0
    for row in rows:
        try:
            await m.bot.copy_message(row["tg_id"], m.chat.id, m.message_id)
            sent += 1
        except Exception:
            failed += 1
    await state.clear()
    await m.answer(
        f"📢 Broadcast finished.\n✅ Sent: {sent}\n❌ Failed: {failed}",
        reply_markup=admin_kb()
    )

# ----- Order/payment approval -----
@router.callback_query(F.data.startswith("approve:"))
async def approve(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    oid = int(c.data.split(":")[1])
    o = db.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    if not o or o["status"] != "pending":
        return await c.answer("Already processed", show_alert=True)
    db.execute("UPDATE orders SET status='completed' WHERE id=?", (oid,))
    db.commit()
    u = db.execute("SELECT * FROM users WHERE id=?", (o["user_id"],)).fetchone()
    await c.answer("Approved")
    await c.message.edit_text(f"✅ Order #{oid} completed.")
    try:
        await c.bot.send_message(u["tg_id"], f"🎉 <b>Order #{oid} completed!</b>")
    except Exception:
        pass

@router.callback_query(F.data.startswith("reject:"))
async def reject(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    oid = int(c.data.split(":")[1])
    o = db.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    if not o or o["status"] != "pending":
        return await c.answer("Already processed", show_alert=True)

    db.execute("UPDATE orders SET status='rejected' WHERE id=?", (oid,))
    db.execute("UPDATE users SET balance=balance+? WHERE id=?", (o["total"], o["user_id"]))
    if o["delivered_code"]:
        db.execute(
            """UPDATE product_codes SET status='available',sold_to=NULL,order_id=NULL,used_at=NULL
            WHERE code=? AND status='sold'""",
            (o["delivered_code"],)
        )
    else:
        db.execute("UPDATE products SET stock=stock+1 WHERE id=?", (o["product_id"],))
    db.execute(
        "INSERT INTO balance_logs(user_id,amount,action,note) VALUES(?,?,?,?)",
        (o["user_id"], o["total"], "refund", f"Rejected order #{oid}")
    )
    db.commit()
    await c.answer("Rejected/refunded")
    await c.message.edit_text(f"❌ Order #{oid} rejected and refunded.")

@router.callback_query(F.data.startswith("credit:"))
async def credit(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT * FROM payments WHERE id=?", (pid,)).fetchone()
    if not p or p["status"] != "pending":
        return await c.answer("Already processed", show_alert=True)
    db.execute("UPDATE payments SET status='credited' WHERE id=?", (pid,))
    db.execute("UPDATE users SET balance=balance+? WHERE id=?", (p["amount"], p["user_id"]))
    db.execute(
        "INSERT INTO balance_logs(user_id,amount,action,note) VALUES(?,?,?,?)",
        (p["user_id"], p["amount"], "payment", f"Payment #{pid}")
    )
    db.commit()
    u = db.execute("SELECT * FROM users WHERE id=?", (p["user_id"],)).fetchone()
    await c.answer("Credited")
    await c.message.edit_text(f"✅ Payment #{pid} credited.")
    try:
        await c.bot.send_message(
            u["tg_id"], f"💰 Added {p['amount']:.2f} {CURRENCY} to your balance."
        )
    except Exception:
        pass

@router.callback_query(F.data.startswith("payreject:"))
async def payreject(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[1])
    db.execute(
        "UPDATE payments SET status='rejected' WHERE id=? AND status='pending'",
        (pid,)
    )
    db.commit()
    await c.answer("Rejected")
    await c.message.edit_text(f"❌ Payment #{pid} rejected.")


# ----- Premium user management -----
@router.callback_query(F.data == "admin:users")
async def admin_users(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    rows = db.execute("SELECT * FROM users ORDER BY id DESC LIMIT 20").fetchall()
    buttons = [
        [InlineKeyboardButton(
            text=f"{'🚫' if r['blocked'] else '🟢'} {r['name'][:18]} | {r['balance']:.0f}",
            callback_data=f"user:{r['id']}"
        )] for r in rows
    ]
    buttons.append([InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:panel")])
    await c.answer()
    await c.message.edit_text(
        "👥 <b>User Management</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

@router.callback_query(F.data.startswith("user:"))
async def admin_user_detail(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    uid = int(c.data.split(":")[1])
    u = db.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not u:
        return await c.answer("Not found", show_alert=True)
    count = db.execute("SELECT COUNT(*) c FROM orders WHERE user_id=?", (uid,)).fetchone()["c"]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🔓 Unblock" if u["blocked"] else "🚫 Block",
            callback_data=f"user_toggle:{uid}"
        )],
        [InlineKeyboardButton(text="⬅️ Users", callback_data="admin:users")]
    ])
    await c.answer()
    await c.message.edit_text(
        f"👤 <b>{u['name']}</b>\n"
        f"ID: <code>{u['tg_id']}</code>\n"
        f"Balance: <b>{u['balance']:.2f} {CURRENCY}</b>\n"
        f"Orders: <b>{count}</b>\n"
        f"Status: {'🚫 Blocked' if u['blocked'] else '🟢 Active'}",
        reply_markup=kb
    )

@router.callback_query(F.data.startswith("user_toggle:"))
async def admin_user_toggle(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    uid = int(c.data.split(":")[1])
    row = db.execute("SELECT blocked FROM users WHERE id=?", (uid,)).fetchone()
    if not row:
        return await c.answer("Not found", show_alert=True)
    new = 0 if row["blocked"] else 1
    db.execute("UPDATE users SET blocked=? WHERE id=?", (new, uid))
    db.commit()
    admin_log(c.from_user.id, "toggle_user", f"user #{uid} -> {new}")
    await c.answer("Updated")
    await admin_user_detail(c)

# ---------- Main ----------
async def main():
    start_health_server()
    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
