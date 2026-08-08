
import os
import csv
import io
import time
import shutil
import sqlite3
import asyncio
import logging
import threading
from datetime import datetime, timedelta
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

TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
PAYMENT_INFO = os.getenv("PAYMENT_INSTRUCTIONS", "bKash/Nagad: YOUR NUMBER")
SUPPORT = os.getenv("SUPPORT_USERNAME", "@YourSupport")
CURRENCY = os.getenv("CURRENCY", "BDT")
DB_FILE = os.getenv("DB_FILE", "nextlevel.db")
LOW_STOCK = int(os.getenv("LOW_STOCK", "5"))
FLOOD_LIMIT = int(os.getenv("FLOOD_LIMIT", "8"))
FLOOD_WINDOW = int(os.getenv("FLOOD_WINDOW", "10"))
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
# If OWNER_ID is not configured, treat the first ADMIN_IDS entry as the owner.
if not OWNER_ID and ADMIN_IDS:
    OWNER_ID = next(iter(ADMIN_IDS))

if not TOKEN:
    raise RuntimeError("BOT_TOKEN missing in .env")

logging.basicConfig(
    filename="bot_errors.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("nextlevel")

db = sqlite3.connect(DB_FILE, check_same_thread=False)
db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")
db.executescript("""
CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_id INTEGER UNIQUE,
    username TEXT,
    name TEXT,
    balance REAL DEFAULT 0,
    blocked INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS products(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    category TEXT,
    quantity INTEGER DEFAULT 1,
    price REAL DEFAULT 0,
    stock INTEGER DEFAULT 0,
    active INTEGER DEFAULT 1,
    description TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS orders(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    product_id INTEGER,
    game_uid TEXT,
    total REAL,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS payments(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    method TEXT,
    trx_id TEXT,
    proof_file_id TEXT,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS balance_history(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    kind TEXT,
    note TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS withdrawals(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    method TEXT,
    account TEXT,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS admin_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id INTEGER,
    action TEXT,
    target TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS admins(
    tg_id INTEGER PRIMARY KEY,
    role TEXT DEFAULT 'staff'
);
CREATE TABLE IF NOT EXISTS product_codes(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER,
    category TEXT,
    code TEXT NOT NULL UNIQUE,
    status TEXT DEFAULT 'unused',
    order_id INTEGER,
    user_id INTEGER,
    added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    used_at DATETIME
);
""")
# Code inventory now supports category-based code pools.
# Existing product-specific codes remain compatible.
try:
    db.execute("ALTER TABLE product_codes ADD COLUMN category TEXT")
except sqlite3.OperationalError:
    pass
# Backfill category for old codes from their linked product.
try:
    db.execute("""UPDATE product_codes SET category=(SELECT category FROM products WHERE products.id=product_codes.product_id) WHERE category IS NULL OR category=''""")
except sqlite3.OperationalError:
    pass
# Product-code category migration
try:
    db.execute("ALTER TABLE product_codes ADD COLUMN category TEXT")
    db.commit()
except sqlite3.OperationalError:
    pass
# Backfill category for legacy product codes
try:
    db.execute("UPDATE product_codes SET category=(SELECT category FROM products WHERE products.id=product_codes.product_id) WHERE category IS NULL AND product_id IS NOT NULL")
    db.commit()
except Exception:
    logger.exception("Failed to backfill product-code categories")

# Backward-compatible order delivery field
try:
    db.execute("ALTER TABLE orders ADD COLUMN delivery_code TEXT")
except sqlite3.OperationalError:
    pass
db.commit()

# Owner/admin roles. OWNER > MANAGER > STAFF.
if OWNER_ID:
    # Force the configured owner to owner even if an older DB already stored staff.
    db.execute("INSERT INTO admins(tg_id,role) VALUES(?,?) ON CONFLICT(tg_id) DO UPDATE SET role=excluded.role", (OWNER_ID, "owner"))
for aid in ADMIN_IDS:
    if aid == OWNER_ID:
        continue
    db.execute("INSERT OR IGNORE INTO admins(tg_id,role) VALUES(?,?)", (aid, "staff"))
db.commit()

router = Router()
flood = {}

def log_admin(admin_id, action, target=""):
    db.execute(
        "INSERT INTO admin_logs(admin_id,action,target) VALUES(?,?,?)",
        (admin_id, action, str(target))
    )
    db.commit()

def role_of(uid):
    row = db.execute("SELECT role FROM admins WHERE tg_id=?", (uid,)).fetchone()
    return row["role"] if row else None

def is_admin(uid):
    return role_of(uid) is not None or uid in ADMIN_IDS

def role_level(role):
    return {"owner": 3, "manager": 2, "staff": 1}.get(role, 0)

def can(uid, needed="staff"):
    return role_level(role_of(uid)) >= role_level(needed)

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

def blocked(uid):
    u = db.execute("SELECT blocked FROM users WHERE tg_id=?", (uid,)).fetchone()
    return bool(u and u["blocked"])

def anti_spam(uid):
    now = time.time()
    arr = [t for t in flood.get(uid, []) if now - t < FLOOD_WINDOW]
    arr.append(now)
    flood[uid] = arr
    return len(arr) <= FLOOD_LIMIT

async def guard(m: Message):
    get_user(m.from_user)
    if blocked(m.from_user.id):
        await m.answer("⛔ You are blocked.")
        return False
    if not anti_spam(m.from_user.id):
        await m.answer("⚠️ Too many messages. Please wait a little.")
        return False
    return True

def menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🛒 Shop"), KeyboardButton(text="👤 Profile")],
        [KeyboardButton(text="💰 Add Balance"), KeyboardButton(text="📦 My Orders")],
        [KeyboardButton(text="💸 Withdraw"), KeyboardButton(text="💬 Support")]
    ], resize_keyboard=True)

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Statistics", callback_data="adm:stats"),
         InlineKeyboardButton(text="📈 Sales", callback_data="adm:sales")],
        [InlineKeyboardButton(text="➕ Add Product", callback_data="adm:addp"),
         InlineKeyboardButton(text="✏️ Edit Product", callback_data="adm:editp")],
        [InlineKeyboardButton(text="🗑 Delete Product", callback_data="adm:delp"),
         InlineKeyboardButton(text="📦 Stock", callback_data="adm:stock")],
        [InlineKeyboardButton(text="🟢/🔴 Products", callback_data="adm:toggle"),
         InlineKeyboardButton(text="💰 Change Price", callback_data="adm:price")],
        [InlineKeyboardButton(text="🔎 Search User", callback_data="adm:user"),
         InlineKeyboardButton(text="🔍 Search Order", callback_data="adm:order")],
        [InlineKeyboardButton(text="💵 Balance", callback_data="adm:balance"),
         InlineKeyboardButton(text="👑 Roles", callback_data="adm:roles")],
        [InlineKeyboardButton(text="🔑 Product Codes", callback_data="adm:codes"),
         InlineKeyboardButton(text="➕ Add Codes", callback_data="adm:addcodes")],
        [InlineKeyboardButton(text="💳 Payments", callback_data="adm:payments"),
         InlineKeyboardButton(text="💸 Withdrawals", callback_data="adm:withdraw")],
        [InlineKeyboardButton(text="📢 Broadcast", callback_data="adm:broadcast")],
        [InlineKeyboardButton(text="📤 Export CSV", callback_data="adm:export")],
        [InlineKeyboardButton(text="💾 Backup DB", callback_data="adm:backup"),
         InlineKeyboardButton(text="📜 Action Logs", callback_data="adm:logs")]
    ])

def products_kb():
    rows = db.execute("SELECT * FROM products WHERE active=1 ORDER BY id DESC").fetchall()
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{p['name']} — {p['price']} {CURRENCY}",
                              callback_data=f"product:{p['id']}")]
        for p in rows
    ])

class Buy(StatesGroup):
    uid = State()

class Pay(StatesGroup):
    amount = State()
    method = State()
    trx = State()
    proof = State()

class Withdraw(StatesGroup):
    amount = State()
    method = State()
    account = State()

class AdminState(StatesGroup):
    action = State()
    text = State()
    text2 = State()
    text3 = State()

class GameOrder(StatesGroup):
    region = State()
    login_id = State()
    password = State()

@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    # /start must always reset any unfinished payment/order/admin flow.
    await state.clear()
    if not await guard(m): return
    u = get_user(m.from_user)
    await m.answer(
        f"🎮 <b>Welcome to Next Level Gaming Shop!</b>\n\n"
        f"💰 Balance: <b>{u['balance']:.2f} {CURRENCY}</b>",
        reply_markup=menu()
    )


# Keep /admin before all FSM input handlers. Otherwise a user stuck in
# Pay.amount can have /admin consumed by pay_amount() as an invalid amount.
@router.message(Command("admin"))
async def admin(m: Message, state: FSMContext):
    await state.clear()
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Access denied.")
    await m.answer(
        f"🛠 <b>Admin Panel</b>\nRole: <b>{role_of(m.from_user.id) or 'staff'}</b>",
        reply_markup=admin_kb()
    )

@router.message(Command("shop"))
@router.message(F.text == "🛒 Shop")
async def shop(m: Message):
    if not await guard(m): return
    if not db.execute("SELECT 1 FROM products WHERE active=1").fetchone():
        return await m.answer("❌ No products available.")
    await m.answer("🛒 <b>Choose a product:</b>", reply_markup=products_kb())

@router.callback_query(F.data.startswith("product:"))
async def product(c: CallbackQuery):
    p = db.execute("SELECT * FROM products WHERE id=?", (int(c.data.split(":")[1]),)).fetchone()
    await c.answer()
    if not p:
        return await c.message.answer("❌ Product not found.")
    code_count = db.execute("SELECT COUNT(*) n FROM product_codes pc JOIN products pp ON pp.id=pc.product_id WHERE pc.category=? AND pc.status='unused'", (p['category'],)).fetchone()['n']
    delivery = f"\n🔑 Code stock: {code_count}" if code_count else ""
    await c.message.edit_text(
        f"🎮 <b>{p['name']}</b>\n"
        f"🆔 Product ID: <code>{p['id']}</code>\n"
        f"🏷 Category: {p['category']}\n"
        f"💰 Price: {p['price']} {CURRENCY}\n"
        f"📦 Stock: {p['stock']}" + delivery + "\n\n"
        f"{p['description'] or 'No description.'}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛒 Buy Now", callback_data=f"buy:{p['id']}")],
            [InlineKeyboardButton(text="⬅️ Shop", callback_data="shop")]
        ])
    )

@router.callback_query(F.data == "shop")
async def shop_cb(c: CallbackQuery):
    await c.answer()
    await c.message.edit_text("🛒 <b>Choose a product:</b>", reply_markup=products_kb())

@router.callback_query(F.data.startswith("buy:"))
async def buy(c: CallbackQuery, state: FSMContext):
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    if not p or not p["active"] or p["stock"] < 1:
        return await c.answer("Out of stock", show_alert=True)
    if has_code_inventory(pid) and not available_code(pid):
        return await c.answer("Product code out of stock", show_alert=True)
    if p["category"].lower() == "pubg":
        await state.update_data(pid=pid)
        await state.set_state(GameOrder.region)
        return await c.message.answer("🌍 Send PUBG region (Global/KR/other):")
    if p["category"].lower() == "efootball":
        await state.update_data(pid=pid)
        await state.set_state(GameOrder.login_id)
        return await c.message.answer("🎮 Send eFootball Login ID:")
    await state.update_data(pid=pid)
    await state.set_state(Buy.uid)
    await c.answer()
    await c.message.answer("🆔 Send your game/player UID:")

def available_code(product_id):
    # Prefer category pool so codes can be loaded by category (e.g. 161).
    row = db.execute("SELECT category FROM products WHERE id=?", (product_id,)).fetchone()
    category = row["category"] if row else None
    if category:
        code = db.execute(
            "SELECT * FROM product_codes WHERE category=? AND status='unused' ORDER BY id LIMIT 1",
            (category,)
        ).fetchone()
        if code:
            return code
    # Legacy fallback for older product-specific codes.
    return db.execute(
        "SELECT * FROM product_codes WHERE product_id=? AND status='unused' ORDER BY id LIMIT 1",
        (product_id,)
    ).fetchone()

def has_code_inventory(product_id):
    row = db.execute("SELECT category FROM products WHERE id=?", (product_id,)).fetchone()
    category = row["category"] if row else None
    if category:
        if db.execute(
            "SELECT 1 FROM product_codes WHERE category=? AND status='unused' LIMIT 1",
            (category,),
        ).fetchone():
            return True
    return db.execute(
        "SELECT 1 FROM product_codes WHERE product_id=? AND status='unused' LIMIT 1",
        (product_id,),
    ).fetchone() is not None

async def deliver_code(bot, order_id, user_tg_id):
    """Deliver exactly one code to the buyer.

    Important: never mark a code as used until Telegram delivery succeeds.
    If HTML parsing or another Telegram error occurs, retry with plain text and
    return failure without consuming the code.
    """
    row = db.execute(
        "SELECT o.*, p.name, p.category FROM orders o JOIN products p ON p.id=o.product_id WHERE o.id=?",
        (order_id,)
    ).fetchone()
    if not row:
        return False, "Order not found"

    # Never deliver more than one code for the same order.
    if row["delivery_code"]:
        return True, row["delivery_code"]

    code = available_code(row["product_id"])
    if not code:
        return False, "No unused product code is available."

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Reserve this exact code atomically so two approvals cannot take it.
    cur = db.execute(
        "UPDATE product_codes SET status='used', order_id=?, user_id=?, used_at=? "
        "WHERE id=? AND status='unused'",
        (order_id, row["user_id"], now, code["id"])
    )
    if cur.rowcount != 1:
        return False, "Code was already claimed. Please try again."

    db.execute(
        "UPDATE orders SET delivery_code=? WHERE id=? AND delivery_code IS NULL",
        (code["code"], order_id)
    )
    db.commit()

    safe_product = html.escape(str(row["name"]))
    safe_code = html.escape(str(code["code"]))

    message = (
        f"🎁 <b>Automatic Code Delivery</b>\n\n"
        f"📦 Product: {safe_product}\n"
        f"🧾 Order: #{order_id}\n"
        f"🔑 Your Code: <code>{safe_code}</code>\n\n"
        f"✅ Please keep this code safe."
    )

    try:
        await bot.send_message(
            chat_id=int(user_tg_id),
            text=message,
            parse_mode=ParseMode.HTML,
        )
    except Exception as first_error:
        logger.exception("Product-code HTML delivery failed for order %s", order_id)
        # Fallback: plain text avoids failures caused by special characters
        # in product/code values or HTML parsing.
        try:
            await bot.send_message(
                chat_id=int(user_tg_id),
                text=(
                    "🎁 Automatic Code Delivery\n\n"
                    f"📦 Product: {row['name']}\n"
                    f"🧾 Order: #{order_id}\n"
                    f"🔑 Your Code: {code['code']}\n\n"
                    "✅ Please keep this code safe."
                ),
                parse_mode=None,
            )
        except Exception as second_error:
            logger.exception(
                "Product-code plain-text delivery also failed for order %s",
                order_id,
            )
            # Do not consume a code if the buyer did not receive it.
            db.execute(
                "UPDATE product_codes SET status='unused', order_id=NULL, user_id=NULL, used_at=NULL "
                "WHERE id=? AND order_id=? AND status='used'",
                (code["id"], order_id),
            )
            db.execute(
                "UPDATE orders SET delivery_code=NULL WHERE id=? AND delivery_code=?",
                (order_id, code["code"]),
            )
            db.commit()
            return False, f"Telegram delivery failed: {second_error}"

    return True, code["code"]

async def create_order(m: Message, state: FSMContext, extra_uid):
    data = await state.get_data()
    pid = int(data["pid"])
    p = db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    u = get_user(m.from_user)
    if not p or not p["active"] or p["stock"] < 1:
        await state.clear()
        return await m.answer("❌ Out of stock.")
    if u["balance"] < p["price"]:
        await state.clear()
        return await m.answer(f"❌ Insufficient balance. Balance: {u['balance']:.2f} {CURRENCY}")
    db.execute("UPDATE users SET balance=balance-? WHERE id=?", (p["price"], u["id"]))
    db.execute("UPDATE products SET stock=stock-1 WHERE id=?", (p["id"],))
    cur = db.execute(
        "INSERT INTO orders(user_id,product_id,game_uid,total) VALUES(?,?,?,?)",
        (u["id"], p["id"], extra_uid, p["price"])
    )
    db.execute(
        "INSERT INTO balance_history(user_id,amount,kind,note) VALUES(?,?,?,?)",
        (u["id"], -p["price"], "purchase", f"Order #{cur.lastrowid}")
    )
    db.commit()
    oid = cur.lastrowid
    await state.clear()
    await m.answer(
        f"✅ <b>Order #{oid} created!</b>\n"
        f"Product: {p['name']}\nTotal: {p['price']} {CURRENCY}\nStatus: ⏳ Pending"
    )
    for aid in ADMIN_IDS:
        try:
            await m.bot.send_message(
                aid,
                f"🧾 <b>New Order #{oid}</b>\n"
                f"User: <code>{u['tg_id']}</code>\n"
                f"Product: {p['name']}\n"
                f"Details: <code>{extra_uid}</code>\n"
                f"Total: {p['price']} {CURRENCY}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Approve", callback_data=f"approve:{oid}"),
                    InlineKeyboardButton(text="❌ Reject", callback_data=f"reject:{oid}")
                ]])
            )
        except Exception:
            logger.exception("Failed to notify admin")

@router.message(Buy.uid)
async def uid(m: Message, state: FSMContext):
    if not await guard(m): return
    await create_order(m, state, m.text.strip())

@router.message(GameOrder.region)
async def game_region(m: Message, state: FSMContext):
    await state.update_data(region=m.text.strip())
    await state.set_state(Buy.uid)
    await m.answer("🆔 Send PUBG UID:")

@router.message(GameOrder.login_id)
async def efootball_id(m: Message, state: FSMContext):
    await state.update_data(login_id=m.text.strip())
    await state.set_state(GameOrder.password)
    await m.answer("🔐 Send eFootball password. It will be used only for this order; avoid reusing an important password.")

@router.message(GameOrder.password)
async def efootball_password(m: Message, state: FSMContext):
    data = await state.get_data()
    details = f"eFootball ID={data['login_id']} | password={m.text.strip()}"
    await create_order(m, state, details)

@router.message(F.text == "👤 Profile")
async def profile(m: Message):
    if not await guard(m): return
    u = get_user(m.from_user)
    await m.answer(f"👤 <b>Profile</b>\nID: <code>{u['tg_id']}</code>\nBalance: <b>{u['balance']:.2f} {CURRENCY}</b>")

@router.message(F.text == "📦 My Orders")
async def orders(m: Message):
    if not await guard(m): return
    u = get_user(m.from_user)
    rows = db.execute(
        "SELECT o.*,p.name FROM orders o JOIN products p ON p.id=o.product_id "
        "WHERE o.user_id=? ORDER BY o.id DESC LIMIT 20", (u["id"],)
    ).fetchall()
    if not rows:
        return await m.answer("📦 No orders yet.")
    await m.answer("📦 <b>Your orders</b>\n\n" + "\n".join(
        f"#{r['id']} — {r['name']} — {r['total']} {CURRENCY} — {r['status']}" for r in rows
    ))

@router.message(F.text == "💰 Add Balance")
async def add_balance(m: Message, state: FSMContext):
    if not await guard(m): return
    await state.set_state(Pay.amount)
    await m.answer(f"💳 <b>Add Balance</b>\n\n{PAYMENT_INFO}\n\nSend amount:")

@router.message(Pay.amount)
async def pay_amount(m: Message, state: FSMContext):
    try:
        amount = float(m.text)
    except Exception:
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
    await m.answer("Send TrxID:")

@router.message(Pay.trx)
async def pay_trx(m: Message, state: FSMContext):
    await state.update_data(trx=m.text.strip())
    await state.set_state(Pay.proof)
    await m.answer("📸 Now send your payment screenshot/photo. If you don't have one, send /skipproof")

@router.message(Command("skipproof"), Pay.proof)
async def skip_proof(m: Message, state: FSMContext):
    await save_payment(m, state, "")

@router.message(Pay.proof, F.photo)
async def proof_photo(m: Message, state: FSMContext):
    await save_payment(m, state, m.photo[-1].file_id)

async def save_payment(m: Message, state: FSMContext, proof_id):
    d = await state.get_data()
    u = get_user(m.from_user)
    cur = db.execute(
        "INSERT INTO payments(user_id,amount,method,trx_id,proof_file_id) VALUES(?,?,?,?,?)",
        (u["id"], d["amount"], d["method"], d["trx"], proof_id)
    )
    db.commit()
    pid = cur.lastrowid
    await state.clear()
    await m.answer(f"✅ Payment #{pid} submitted. Admin will review it.")
    for aid in ADMIN_IDS:
        try:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Credit", callback_data=f"credit:{pid}"),
                InlineKeyboardButton(text="❌ Reject", callback_data=f"payreject:{pid}")
            ]])
            text = (f"💳 <b>Payment #{pid}</b>\nUser: <code>{u['tg_id']}</code>\n"
                    f"Amount: {d['amount']} {CURRENCY}\nMethod: {d['method']}\nTrxID: <code>{d['trx']}</code>")
            if proof_id:
                await m.bot.send_photo(aid, proof_id, caption=text, reply_markup=kb)
            else:
                await m.bot.send_message(aid, text + "\n📸 No screenshot.", reply_markup=kb)
        except Exception:
            logger.exception("Payment admin notify failed")

@router.message(Pay.proof)
async def proof_required(m: Message):
    await m.answer("📸 Please send a screenshot/photo, or /skipproof.")

@router.message(F.text == "💸 Withdraw")
async def withdraw_start(m: Message, state: FSMContext):
    if not await guard(m): return
    await state.set_state(Withdraw.amount)
    await m.answer("💸 Send withdrawal amount:")

@router.message(Withdraw.amount)
async def withdraw_amount(m: Message, state: FSMContext):
    try: amount = float(m.text)
    except: return await m.answer("❌ Invalid amount.")
    u = get_user(m.from_user)
    if amount <= 0 or amount > u["balance"]:
        return await m.answer("❌ Invalid amount or insufficient balance.")
    await state.update_data(amount=amount)
    await state.set_state(Withdraw.method)
    await m.answer("Send withdrawal method (bKash/Nagad/etc.):")

@router.message(Withdraw.method)
async def withdraw_method(m: Message, state: FSMContext):
    await state.update_data(method=m.text.strip())
    await state.set_state(Withdraw.account)
    await m.answer("Send account/number:")

@router.message(Withdraw.account)
async def withdraw_account(m: Message, state: FSMContext):
    d = await state.get_data()
    u = get_user(m.from_user)
    if u["balance"] < d["amount"]:
        await state.clear()
        return await m.answer("❌ Balance changed. Try again.")
    db.execute("UPDATE users SET balance=balance-? WHERE id=?", (d["amount"], u["id"]))
    db.execute("INSERT INTO balance_history(user_id,amount,kind,note) VALUES(?,?,?,?)",
               (u["id"], -d["amount"], "withdraw_hold", "Withdrawal request"))
    cur = db.execute(
        "INSERT INTO withdrawals(user_id,amount,method,account) VALUES(?,?,?,?)",
        (u["id"], d["amount"], d["method"], m.text.strip())
    )
    db.commit()
    wid = cur.lastrowid
    await state.clear()
    await m.answer(f"✅ Withdraw request #{wid} submitted.")
    for aid in ADMIN_IDS:
        try:
            await m.bot.send_message(
                aid,
                f"💸 <b>Withdraw #{wid}</b>\nUser: <code>{u['tg_id']}</code>\n"
                f"Amount: {d['amount']} {CURRENCY}\nMethod: {d['method']}\nAccount: <code>{m.text.strip()}</code>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Pay", callback_data=f"wdapprove:{wid}"),
                    InlineKeyboardButton(text="❌ Reject", callback_data=f"wdreject:{wid}")
                ]])
            )
        except Exception:
            logger.exception("Withdraw notify failed")

@router.message(F.text == "💬 Support")
async def support(m: Message):
    if not await guard(m): return
    await m.answer(f"💬 Support: {SUPPORT}")

@router.callback_query(F.data.startswith("approve:"))
async def approve(c: CallbackQuery):
    if not can(c.from_user.id, "staff"): return await c.answer("Denied", show_alert=True)
    oid = int(c.data.split(":")[1])
    o = db.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    if not o or o["status"] != "pending": return await c.answer("Already processed", show_alert=True)
    # IMPORTANT: has_code_inventory() expects a PRODUCT ID, not a category.
    # Passing the category here (e.g. 161) caused code delivery to be skipped
    # while the order was still marked completed.
    needs_code = has_code_inventory(o["product_id"])
    if needs_code:
        user = db.execute("SELECT tg_id FROM users WHERE id=?", (o["user_id"],)).fetchone()
        if not user:
            return await c.answer("User not found", show_alert=True)
        ok, detail = await deliver_code(c.bot, oid, user["tg_id"])
        if not ok:
            return await c.answer(f"Code delivery failed: {detail}", show_alert=True)

    db.execute("UPDATE orders SET status='completed' WHERE id=?", (oid,))
    db.commit()
    log_admin(
        c.from_user.id,
        "approve_order_code_delivery" if needs_code else "approve_order",
        oid,
    )
    await c.answer("Approved")
    await c.message.edit_text(
        f"✅ Order #{oid} completed."
        + ("\n🔑 Product code delivered to the buyer." if needs_code else "")
    )

@router.callback_query(F.data.startswith("reject:"))
async def reject(c: CallbackQuery):
    if not can(c.from_user.id, "staff"): return await c.answer("Denied", show_alert=True)
    oid = int(c.data.split(":")[1])
    o = db.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    if not o or o["status"] != "pending": return await c.answer("Already processed", show_alert=True)
    db.execute("UPDATE orders SET status='rejected' WHERE id=?", (oid,))
    db.execute("UPDATE users SET balance=balance+? WHERE id=?", (o["total"], o["user_id"]))
    db.execute("UPDATE products SET stock=stock+1 WHERE id=?", (o["product_id"],))
    db.execute("INSERT INTO balance_history(user_id,amount,kind,note) VALUES(?,?,?,?)",
               (o["user_id"], o["total"], "refund", f"Order #{oid} rejected"))
    db.commit()
    log_admin(c.from_user.id, "reject_refund_order", oid)
    await c.answer("Rejected/refunded")
    await c.message.edit_text(f"❌ Order #{oid} rejected and refunded.")

@router.callback_query(F.data.startswith("credit:"))
async def credit(c: CallbackQuery):
    if not can(c.from_user.id, "staff"): return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT * FROM payments WHERE id=?", (pid,)).fetchone()
    if not p or p["status"] != "pending": return await c.answer("Already processed", show_alert=True)
    db.execute("UPDATE payments SET status='credited' WHERE id=?", (pid,))
    db.execute("UPDATE users SET balance=balance+? WHERE id=?", (p["amount"], p["user_id"]))
    db.execute("INSERT INTO balance_history(user_id,amount,kind,note) VALUES(?,?,?,?)",
               (p["user_id"], p["amount"], "deposit", f"Payment #{pid}"))
    db.commit()
    log_admin(c.from_user.id, "credit_payment", pid)
    await c.answer("Credited")
    await c.message.edit_text(f"✅ Payment #{pid} credited.")

@router.callback_query(F.data.startswith("payreject:"))
async def payreject(c: CallbackQuery):
    if not can(c.from_user.id, "staff"): return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[1])
    db.execute("UPDATE payments SET status='rejected' WHERE id=? AND status='pending'", (pid,))
    db.commit()
    log_admin(c.from_user.id, "reject_payment", pid)
    await c.answer("Rejected")
    await c.message.edit_text(f"❌ Payment #{pid} rejected.")

@router.callback_query(F.data.startswith("wdapprove:"))
async def wdapprove(c: CallbackQuery):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    wid = int(c.data.split(":")[1])
    w = db.execute("SELECT * FROM withdrawals WHERE id=?", (wid,)).fetchone()
    if not w or w["status"] != "pending": return await c.answer("Already processed", show_alert=True)
    db.execute("UPDATE withdrawals SET status='paid' WHERE id=?", (wid,))
    db.commit()
    log_admin(c.from_user.id, "approve_withdraw", wid)
    await c.answer("Paid")
    await c.message.edit_text(f"✅ Withdraw #{wid} marked paid.")

@router.callback_query(F.data.startswith("wdreject:"))
async def wdreject(c: CallbackQuery):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    wid = int(c.data.split(":")[1])
    w = db.execute("SELECT * FROM withdrawals WHERE id=?", (wid,)).fetchone()
    if not w or w["status"] != "pending": return await c.answer("Already processed", show_alert=True)
    db.execute("UPDATE withdrawals SET status='rejected' WHERE id=?", (wid,))
    db.execute("UPDATE users SET balance=balance+? WHERE id=?", (w["amount"], w["user_id"]))
    db.execute("INSERT INTO balance_history(user_id,amount,kind,note) VALUES(?,?,?,?)",
               (w["user_id"], w["amount"], "withdraw_refund", f"Withdraw #{wid} rejected"))
    db.commit()
    log_admin(c.from_user.id, "reject_withdraw_refund", wid)
    await c.answer("Rejected/refunded")
    await c.message.edit_text(f"❌ Withdraw #{wid} rejected and refunded.")

@router.callback_query(F.data == "adm:stats")
async def adm_stats(c: CallbackQuery):
    if not can(c.from_user.id, "staff"): return await c.answer("Denied", show_alert=True)
    users = db.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
    products = db.execute("SELECT COUNT(*) n FROM products WHERE active=1").fetchone()["n"]
    orders = db.execute("SELECT COUNT(*) n FROM orders").fetchone()["n"]
    sales = db.execute("SELECT COALESCE(SUM(total),0) s FROM orders WHERE status='completed'").fetchone()["s"]
    pending = db.execute("SELECT COUNT(*) n FROM orders WHERE status='pending'").fetchone()["n"]
    await c.message.answer(
        f"📊 <b>Statistics</b>\nUsers: {users}\nActive products: {products}\n"
        f"Orders: {orders}\nPending: {pending}\nCompleted sales: {sales:.2f} {CURRENCY}"
    )
    await c.answer()

@router.callback_query(F.data == "adm:sales")
async def adm_sales(c: CallbackQuery):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    day = db.execute(
        "SELECT COALESCE(SUM(total),0) s FROM orders WHERE status='completed' AND created_at>=datetime('now','-1 day')"
    ).fetchone()["s"]
    month = db.execute(
        "SELECT COALESCE(SUM(total),0) s FROM orders WHERE status='completed' AND created_at>=datetime('now','-30 day')"
    ).fetchone()["s"]
    await c.message.answer(f"📈 <b>Sales Dashboard</b>\n24h: {day:.2f} {CURRENCY}\n30d: {month:.2f} {CURRENCY}")
    await c.answer()

async def admin_prompt(c, state, action, text):
    await state.set_state(AdminState.action)
    await state.update_data(action=action)
    await c.message.answer(text)
    await c.answer()

@router.callback_query(F.data == "adm:addp")
async def adm_addp(c, state: FSMContext):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    await admin_prompt(c, state, "addp", "➕ Send: name | category | price | stock | description")

@router.callback_query(F.data == "adm:editp")
async def adm_editp(c, state: FSMContext):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    await admin_prompt(c, state, "editp", "✏️ Send: product_id | new description")

@router.callback_query(F.data == "adm:delp")
async def adm_delp(c, state: FSMContext):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    await admin_prompt(c, state, "delp", "🗑 Send product ID to delete")

@router.callback_query(F.data == "adm:stock")
async def adm_stock(c, state: FSMContext):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    await admin_prompt(c, state, "stock", "📦 Send: product_id | +5 or -5")

@router.callback_query(F.data == "adm:toggle")
async def adm_toggle(c, state: FSMContext):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    await admin_prompt(c, state, "toggle", "🟢/🔴 Send product ID")

@router.callback_query(F.data == "adm:price")
async def adm_price(c, state: FSMContext):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    await admin_prompt(c, state, "price", "💰 Send: product_id | new price")

@router.callback_query(F.data == "adm:user")
async def adm_user(c, state: FSMContext):
    if not can(c.from_user.id, "staff"): return await c.answer("Denied", show_alert=True)
    await admin_prompt(c, state, "user", "🔎 Send Telegram user ID")

@router.callback_query(F.data == "adm:order")
async def adm_order(c, state: FSMContext):
    if not can(c.from_user.id, "staff"): return await c.answer("Denied", show_alert=True)
    await admin_prompt(c, state, "order", "🔍 Send Order ID or UID")

@router.callback_query(F.data == "adm:balance")
async def adm_balance(c, state: FSMContext):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    await admin_prompt(c, state, "balance", "💰 Send: user_id | +100 or -100")

@router.callback_query(F.data == "adm:roles")
async def adm_roles(c, state: FSMContext):
    if not can(c.from_user.id, "owner"): return await c.answer("Owner only", show_alert=True)
    await admin_prompt(c, state, "roles", "👑 Send: user_id | owner/manager/staff")

@router.callback_query(F.data == "adm:broadcast")
async def adm_broadcast(c, state: FSMContext):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    await admin_prompt(c, state, "broadcast", "📢 Send broadcast text")

@router.callback_query(F.data == "adm:codes")
async def adm_codes(c: CallbackQuery):
    if not can(c.from_user.id, "manager"):
        return await c.answer("Manager only", show_alert=True)
    await c.message.answer(
        "🔑 <b>Product Code Manager</b>\n\n"
        "🆔 Product ID is the number shown after adding a product or in <code>/products</code>.\n\n"
        "➕ Add by Product ID: <code>Product_ID | code1\ncode2\ncode3</code>\n"
        "📋 View: <code>/codes CATEGORY</code> or <code>/codes CATEGORY</code>\n"
        "🗑 Delete unused: <code>/delcode CODE</code>\n\n"
        "Each code is delivered only once after its order is approved."
    )
    await c.answer()

@router.message(Command("products"))
async def products_cmd(m: Message):
    if not can(m.from_user.id, "manager"):
        return await m.answer("⛔ Manager only.")
    rows = db.execute("SELECT id,name,category,price,stock,active FROM products ORDER BY id DESC").fetchall()
    if not rows:
        return await m.answer("📦 No products found.")
    lines = ["📦 <b>Product List</b>"]
    for p in rows:
        status = "🟢" if p["active"] else "🔴"
        lines.append(
            f"{status} <b>{p['name']}</b> — ID: <code>{p['id']}</code>\n"
            f"   🏷 {p['category']} | 💰 {p['price']} {CURRENCY} | 📦 Stock: {p['stock']}"
        )
    await m.answer("\n".join(lines))


@router.message(Command("codes"))
async def codes_cmd(m: Message):
    if not can(m.from_user.id, "manager"):
        return await m.answer("⛔ Manager only.")
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return await m.answer("Use: /codes CATEGORY")
    key = parts[1].strip()
    p = None
    try:
        pid = int(key)
        p = db.execute("SELECT id,name,category FROM products WHERE id=?", (pid,)).fetchone()
    except ValueError:
        pass
    if p:
        rows = db.execute("SELECT code,status,order_id FROM product_codes WHERE category=? OR (category IS NULL AND product_id=?) ORDER BY id DESC LIMIT 50", (p['category'], p['id'])).fetchall()
        title = f"{p['name']} — category {p['category']}"
    else:
        rows = db.execute("SELECT code,status,order_id FROM product_codes WHERE category=? ORDER BY id DESC LIMIT 50", (key,)).fetchall()
        title = f"Category {key}"
    if not rows:
        return await m.answer(f"🔑 {title}: no codes.")
    text = [f"🔑 <b>{title}</b>"]
    for r in rows:
        # Never expose used codes in admin lists.
        shown = r['code'] if r['status'] == 'unused' else "•••••••• (used)"
        text.append(f"• {shown} — {r['status']}")
    await m.answer("\n".join(text))

@router.message(Command("delcode"))
async def delcode_cmd(m: Message):
    if not can(m.from_user.id, "manager"):
        return await m.answer("⛔ Manager only.")
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return await m.answer("Use: /delcode CODE")
    code = parts[1].strip()
    row = db.execute("SELECT id FROM product_codes WHERE code=? AND status='unused'", (code,)).fetchone()
    if not row:
        return await m.answer("❌ Unused code not found.")
    db.execute("DELETE FROM product_codes WHERE id=?", (row['id'],))
    db.commit()
    log_admin(m.from_user.id, "delete_unused_product_code", "hidden")
    await m.answer("🗑 Unused product code deleted.")

@router.callback_query(F.data == "adm:addcodes")
async def adm_addcodes(c: CallbackQuery, state: FSMContext):
    if not can(c.from_user.id, "manager"):
        return await c.answer("Manager only", show_alert=True)
    await admin_prompt(c, state, "addcodes", "🔑 Send: Product ID | code1\ncode2\ncode3\nExample: 1 | UC-001\nUC-002\nUC-003")

@router.callback_query(F.data == "adm:payments")
async def adm_payments(c: CallbackQuery):
    if not can(c.from_user.id, "staff"): return await c.answer("Denied", show_alert=True)
    rows = db.execute("SELECT * FROM payments WHERE status='pending' ORDER BY id DESC LIMIT 20").fetchall()
    if not rows: return await c.message.answer("💳 No pending payments.")
    await c.message.answer("\n".join(
        f"#{r['id']} — {r['amount']} {CURRENCY} — {r['method']} — {r['trx_id']}" for r in rows
    ))
    await c.answer()

@router.callback_query(F.data == "adm:withdraw")
async def adm_withdraw(c: CallbackQuery):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    rows = db.execute("SELECT * FROM withdrawals WHERE status='pending' ORDER BY id DESC LIMIT 20").fetchall()
    if not rows: return await c.message.answer("💸 No pending withdrawals.")
    await c.message.answer("\n".join(
        f"#{r['id']} — {r['amount']} {CURRENCY} — {r['method']} — {r['account']}" for r in rows
    ))
    await c.answer()

@router.callback_query(F.data == "adm:backup")
async def adm_backup(c: CallbackQuery):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = f"backup_{stamp}.db"
    db.commit()
    shutil.copy2(DB_FILE, out)
    log_admin(c.from_user.id, "backup_database", out)
    await c.message.answer_document(
        __import__("aiogram").types.FSInputFile(out),
        caption=f"💾 Database backup: {out}"
    )
    await c.answer("Backup created")

@router.callback_query(F.data == "adm:logs")
async def adm_logs(c: CallbackQuery):
    if not can(c.from_user.id, "owner"): return await c.answer("Owner only", show_alert=True)
    rows = db.execute("SELECT * FROM admin_logs ORDER BY id DESC LIMIT 20").fetchall()
    if not rows: return await c.message.answer("No logs.")
    await c.message.answer("\n".join(
        f"#{r['id']} admin={r['admin_id']} {r['action']} target={r['target']} {r['created_at']}" for r in rows
    ))
    await c.answer()

@router.callback_query(F.data == "adm:export")
async def adm_export(c: CallbackQuery):
    if not can(c.from_user.id, "manager"): return await c.answer("Manager only", show_alert=True)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["order_id","user_id","product_id","game_uid","total","status","created_at"])
    for r in db.execute("SELECT * FROM orders ORDER BY id DESC").fetchall():
        w.writerow([r["id"],r["user_id"],r["product_id"],r["game_uid"],r["total"],r["status"],r["created_at"]])
    doc = __import__("aiogram").types.BufferedInputFile(
        out.getvalue().encode(), filename="orders.csv"
    )
    await c.message.answer_document(doc, caption="📤 Orders CSV")
    await c.answer()

@router.message(AdminState.action)
async def admin_action(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear()
        return
    d = await state.get_data()
    action = d["action"]
    text = m.text.strip()

    try:
        if action == "addcodes":
            parts = text.split("|", 1)
            if len(parts) != 2:
                raise ValueError("Use: Product ID | code1\ncode2\ncode3\nExample: 1 | UC-001\nUC-002")
            try:
                product_id = int(parts[0].strip())
            except ValueError:
                raise ValueError("Product ID must be a number")
            product = db.execute("SELECT id,name,category FROM products WHERE id=?", (product_id,)).fetchone()
            if not product:
                raise ValueError(f"No product found with Product ID: {product_id}")
            category = product["category"]
            codes = [line.strip() for line in parts[1].splitlines() if line.strip()]
            if not codes:
                raise ValueError("No codes supplied")
            added = 0
            duplicates = 0
            for code in codes:
                try:
                    # Product ID selects the product; category remains the delivery pool key.
                    db.execute("INSERT INTO product_codes(product_id,category,code) VALUES(?,?,?)",
                               (product["id"], category, code))
                    added += 1
                except sqlite3.IntegrityError:
                    duplicates += 1
            db.commit()
            log_admin(m.from_user.id, "add_product_codes",
                      f"product_id={product_id},category={category},added={added}")
            await m.answer(
                f"✅ <b>{product['name']}</b> (Product ID: <code>{product_id}</code>)\n"
                f"🏷 Category: <code>{category}</code>\n"
                f"🔑 Added: {added} code(s)\n"
                f"⚠️ Duplicate/skipped: {duplicates}\n\n"
                f"📦 One approved order = one code."
            )
        elif action == "addp":
            name, category, price, stock, desc = [x.strip() for x in text.split("|", 4)]
            cur = db.execute("INSERT INTO products(name,category,price,stock,description) VALUES(?,?,?,?,?)",
                             (name, category, float(price), int(stock), desc))
            db.commit()
            product_id = cur.lastrowid
            log_admin(m.from_user.id, "add_product", product_id)
            await m.answer(
                f"✅ <b>Product added.</b>\n\n"
                f"🆔 Product ID: <code>{product_id}</code>\n"
                f"🎮 Name: <b>{name}</b>\n"
                f"🏷 Category: {category}\n"
                f"💰 Price: {float(price):g} {CURRENCY}\n"
                f"📦 Stock: {int(stock)}\n\n"
                f"🔑 Code add করতে এই Product ID ব্যবহার করো: <code>{product_id} | YOUR-CODE</code>"
            )
        elif action == "editp":
            pid, desc = [x.strip() for x in text.split("|", 1)]
            db.execute("UPDATE products SET description=? WHERE id=?", (desc, int(pid)))
            db.commit()
            log_admin(m.from_user.id, "edit_description", pid)
            await m.answer("✅ Description updated.")
        elif action == "delp":
            pid = int(text)
            db.execute("DELETE FROM products WHERE id=?", (pid,))
            db.commit()
            log_admin(m.from_user.id, "delete_product", pid)
            await m.answer("🗑 Product deleted.")
        elif action == "stock":
            pid, delta = [x.strip() for x in text.split("|", 1)]
            db.execute("UPDATE products SET stock=MAX(0,stock+?) WHERE id=?", (int(delta), int(pid)))
            db.commit()
            log_admin(m.from_user.id, "change_stock", pid)
            await m.answer("📦 Stock updated.")
        elif action == "toggle":
            pid = int(text)
            db.execute("UPDATE products SET active=CASE active WHEN 1 THEN 0 ELSE 1 END WHERE id=?", (pid,))
            db.commit()
            log_admin(m.from_user.id, "toggle_product", pid)
            await m.answer("🟢/🔴 Product status changed.")
        elif action == "price":
            pid, price = [x.strip() for x in text.split("|", 1)]
            db.execute("UPDATE products SET price=? WHERE id=?", (float(price), int(pid)))
            db.commit()
            log_admin(m.from_user.id, "change_price", pid)
            await m.answer("💰 Price updated.")
        elif action == "user":
            uid = int(text)
            u = db.execute("SELECT * FROM users WHERE tg_id=?", (uid,)).fetchone()
            await m.answer("❌ User not found." if not u else
                            f"👤 ID: {u['tg_id']}\nName: {u['name']}\nBalance: {u['balance']:.2f}\nBlocked: {bool(u['blocked'])}")
        elif action == "order":
            rows = db.execute(
                "SELECT o.*,p.name,u.tg_id FROM orders o JOIN products p ON p.id=o.product_id "
                "JOIN users u ON u.id=o.user_id WHERE CAST(o.id AS TEXT)=? OR o.game_uid LIKE ? "
                "ORDER BY o.id DESC LIMIT 10", (text, f"%{text}%")
            ).fetchall()
            await m.answer("❌ No order found." if not rows else "\n".join(
                f"#{r['id']} {r['name']} UID={r['game_uid']} user={r['tg_id']} {r['total']} {CURRENCY} {r['status']}"
                for r in rows
            ))
        elif action == "balance":
            uid, delta = [x.strip() for x in text.split("|", 1)]
            uid, delta = int(uid), float(delta)
            db.execute("UPDATE users SET balance=balance+? WHERE tg_id=?", (delta, uid))
            u = db.execute("SELECT id FROM users WHERE tg_id=?", (uid,)).fetchone()
            if u:
                db.execute("INSERT INTO balance_history(user_id,amount,kind,note) VALUES(?,?,?,?)",
                           (u["id"], delta, "admin_adjustment", f"Admin {m.from_user.id}"))
            db.commit()
            log_admin(m.from_user.id, "balance_adjustment", uid)
            await m.answer("💰 Balance updated.")
        elif action == "roles":
            uid, role = [x.strip() for x in text.split("|", 1)]
            uid = int(uid)
            if role not in ("owner","manager","staff"):
                raise ValueError("Role must be owner/manager/staff")
            db.execute("INSERT INTO admins(tg_id,role) VALUES(?,?) ON CONFLICT(tg_id) DO UPDATE SET role=excluded.role",
                       (uid, role))
            db.commit()
            log_admin(m.from_user.id, "change_role", f"{uid}:{role}")
            await m.answer("👑 Role updated.")
        elif action == "broadcast":
            users = db.execute("SELECT tg_id FROM users WHERE blocked=0").fetchall()
            sent = 0
            for u in users:
                try:
                    await m.bot.send_message(u["tg_id"], text)
                    sent += 1
                    await asyncio.sleep(0.04)
                except Exception:
                    pass
            log_admin(m.from_user.id, "broadcast", sent)
            await m.answer(f"📢 Broadcast finished. Sent: {sent}")
    except Exception as e:
        logger.exception("Admin action failed")
        await m.answer(f"❌ Error: {e}")
    finally:
        await state.clear()

@router.message(Command("ban"))
async def ban(m: Message):
    if not can(m.from_user.id, "manager"): return await m.answer("⛔ Manager only.")
    try:
        uid = int(m.text.split(maxsplit=1)[1])
        db.execute("UPDATE users SET blocked=1 WHERE tg_id=?", (uid,))
        db.commit(); log_admin(m.from_user.id, "ban_user", uid)
        await m.answer("🔴 User banned.")
    except:
        await m.answer("Use: /ban USER_ID")

@router.message(Command("unban"))
async def unban(m: Message):
    if not can(m.from_user.id, "manager"): return await m.answer("⛔ Manager only.")
    try:
        uid = int(m.text.split(maxsplit=1)[1])
        db.execute("UPDATE users SET blocked=0 WHERE tg_id=?", (uid,))
        db.commit(); log_admin(m.from_user.id, "unban_user", uid)
        await m.answer("🟢 User unbanned.")
    except:
        await m.answer("Use: /unban USER_ID")

@router.message(Command("backup"))
async def backup_cmd(m: Message):
    if not can(m.from_user.id, "manager"): return await m.answer("⛔ Manager only.")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = f"backup_{stamp}.db"
    db.commit(); shutil.copy2(DB_FILE, out)
    log_admin(m.from_user.id, "backup_database", out)
    await m.answer_document(__import__("aiogram").types.FSInputFile(out), caption=out)

@router.errors()
async def global_error(event):
    logger.exception("Unhandled bot error: %s", event.exception)

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass

def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()

async def low_stock_loop(bot: Bot):
    warned = set()
    while True:
        try:
            rows = db.execute("SELECT id,name,stock FROM products WHERE active=1 AND stock<?", (LOW_STOCK,)).fetchall()
            for p in rows:
                key = (p["id"], p["stock"])
                if key in warned:
                    continue
                warned.add(key)
                for aid in ADMIN_IDS:
                    try:
                        await bot.send_message(aid, f"⚠️ Low stock: {p['name']} — {p['stock']} left.")
                    except Exception:
                        pass
        except Exception:
            logger.exception("Low stock loop failed")
        await asyncio.sleep(60)

async def main():
    threading.Thread(target=start_health_server, daemon=True).start()
    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    asyncio.create_task(low_stock_loop(bot))
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
