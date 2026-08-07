import os
import sqlite3
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from decimal import Decimal, InvalidOperation
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
ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
PAYMENT_INFO = os.getenv("PAYMENT_INSTRUCTIONS", "bKash/Nagad: YOUR NUMBER")
SUPPORT = os.getenv("SUPPORT_USERNAME", "@YourSupport")
CURRENCY = os.getenv("CURRENCY", "BDT")
DB_PATH = os.getenv("DB_PATH", "nextlevel.db")
PORT = int(os.getenv("PORT", "10000"))

if not TOKEN:
    raise RuntimeError("BOT_TOKEN missing in .env")

db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA foreign_keys=ON")
db.executescript("""
CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_id INTEGER UNIQUE NOT NULL,
    username TEXT,
    name TEXT,
    balance REAL DEFAULT 0,
    blocked INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    category TEXT NOT NULL,
    quantity INTEGER DEFAULT 0,
    price REAL DEFAULT 0,
    stock INTEGER DEFAULT 0,
    active INTEGER DEFAULT 1,
    description TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS orders(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    total REAL NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payments(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    method TEXT NOT NULL,
    trx_id TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
""")

product_catalog = [
    ("20 UC", "UC", 20, 19, 35),
    ("36 UC", "UC", 36, 35, 31),
    ("80 UC", "UC", 80, 75, 79),
    ("160 UC", "UC", 160, 149, 147),
    ("161 UC", "UC", 161, 150, 216),
    ("162 UC", "UC", 162, 147, 0),
    ("405 UC", "UC", 405, 376, 25),
    ("800 UC", "UC", 800, 741, 32),
    ("810 UC", "UC", 810, 751, 17),
    ("1625 UC", "UC", 1625, 1505, 3),
    ("2000 UC", "UC", 2000, 1870, 0),
    ("650 Shell", "Shell", 650, 0, 0),
    ("1300 Shell", "Shell", 1300, 2075, 0),
]

for name, category, quantity, price, stock in product_catalog:
    existing = db.execute("SELECT id FROM products WHERE name=?", (name,)).fetchone()
    if not existing:
        db.execute(
            """INSERT INTO products(name,category,quantity,price,stock,active)
               VALUES(?,?,?,?,?,1)""",
            (name, category, quantity, price, stock),
        )
db.commit()


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass


def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


router = Router()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛒 Shop"), KeyboardButton(text="👤 Profile")],
            [KeyboardButton(text="💰 Add Balance"), KeyboardButton(text="📦 My Orders")],
            [KeyboardButton(text="💬 Support")],
        ],
        resize_keyboard=True,
    )


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Stats", callback_data="admin:stats"),
            InlineKeyboardButton(text="💳 Payments", callback_data="admin:payments"),
        ],
        [
            InlineKeyboardButton(text="🧾 Orders", callback_data="admin:orders"),
            InlineKeyboardButton(text="📦 Stock", callback_data="admin:stock"),
        ],
        [
            InlineKeyboardButton(text="📢 Broadcast", callback_data="admin:broadcast"),
            InlineKeyboardButton(text="🔄 Products", callback_data="admin:products"),
        ],
    ])


def back_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:home")]
    ])


def products_kb():
    order_sql = """
        CASE name
            WHEN '20 UC' THEN 1 WHEN '36 UC' THEN 2 WHEN '80 UC' THEN 3
            WHEN '160 UC' THEN 4 WHEN '161 UC' THEN 5 WHEN '162 UC' THEN 6
            WHEN '405 UC' THEN 7 WHEN '800 UC' THEN 8 WHEN '810 UC' THEN 9
            WHEN '1625 UC' THEN 10 WHEN '2000 UC' THEN 11
            WHEN '650 Shell' THEN 12 WHEN '1300 Shell' THEN 13 ELSE 99
        END
    """
    rows = db.execute(
        f"SELECT * FROM products WHERE active=1 AND stock>0 ORDER BY {order_sql}"
    ).fetchall()

    buttons = []
    for p in rows:
        buttons.append([
            InlineKeyboardButton(
                text=f"{p['name']} — {p['price']} {CURRENCY} | Stock {p['stock']}",
                callback_data=f"product:{p['id']}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons or [[
        InlineKeyboardButton(text="❌ No stock available", callback_data="noop")
    ]])


def admin_products_kb():
    rows = db.execute("SELECT * FROM products ORDER BY id").fetchall()
    buttons = []
    for p in rows:
        state = "ON" if p["active"] else "OFF"
        buttons.append([
            InlineKeyboardButton(
                text=f"{p['name']} | {p['stock']} | {state}",
                callback_data=f"admin:editproduct:{p['id']}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:home")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)



class Pay(StatesGroup):
    amount = State()
    method = State()
    trx = State()


class AdminState(StatesGroup):
    broadcast = State()
    stock = State()
    price = State()


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
            "UPDATE users SET username=?, name=? WHERE tg_id=?",
            (tg.username, tg.full_name, tg.id)
        )
        db.commit()
        u = db.execute("SELECT * FROM users WHERE tg_id=?", (tg.id,)).fetchone()
    return u


@router.message(CommandStart())
async def start(m: Message):
    u = get_user(m.from_user)
    if u["blocked"]:
        return await m.answer("⛔ Your account is blocked.")
    await m.answer(
        f"🎮 <b>Welcome to Nextlevelgamingshop!</b>\n\n"
        f"Balance: <b>{u['balance']} {CURRENCY}</b>",
        reply_markup=menu()
    )


@router.message(Command("shop"))
@router.message(F.text == "🛒 Shop")
async def shop(m: Message):
    u = get_user(m.from_user)
    if u["blocked"]:
        return await m.answer("⛔ Your account is blocked.")
    await m.answer("🛒 <b>Choose a product:</b>", reply_markup=products_kb())


@router.callback_query(F.data == "noop")
async def noop(c: CallbackQuery):
    await c.answer("No stock available.", show_alert=True)


@router.callback_query(F.data.startswith("product:"))
async def product(c: CallbackQuery):
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    await c.answer()
    if not p or not p["active"]:
        return await c.message.answer("❌ Product not found.")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Buy Now", callback_data=f"buy:{pid}")],
        [InlineKeyboardButton(text="⬅️ Shop", callback_data="shop")],
    ])
    await c.message.edit_text(
        f"🎮 <b>{p['name']}</b>\n"
        f"💰 {p['price']} {CURRENCY}\n"
        f"📦 Stock: {p['stock']}\n\n"
        f"{p['description'] or ''}",
        reply_markup=kb
    )


@router.callback_query(F.data == "shop")
async def shop_cb(c: CallbackQuery):
    await c.answer()
    await c.message.edit_text("🛒 <b>Choose a product:</b>", reply_markup=products_kb())


@router.callback_query(F.data.startswith("buy:"))
async def buy(c: CallbackQuery):
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    u = get_user(c.from_user)

    if not p or not p["active"] or p["stock"] < 1:
        return await c.answer("Out of stock.", show_alert=True)
    if u["balance"] < p["price"]:
        return await c.answer("Insufficient balance.", show_alert=True)

    db.execute("UPDATE users SET balance=balance-? WHERE id=?", (p["price"], u["id"]))
    db.execute("UPDATE products SET stock=stock-1 WHERE id=?", (p["id"],))
    cur = db.execute(
        "INSERT INTO orders(user_id,product_id,total) VALUES(?,?,?)",
        (u["id"], p["id"], p["price"])
    )
    db.commit()
    oid = cur.lastrowid

    await c.answer("Order created!")
    await c.message.answer(
        f"✅ <b>Order #{oid} created!</b>\n"
        f"Product: {p['name']}\n"
        f"Total: {p['price']} {CURRENCY}\n"
        f"Status: ⏳ Pending"
    )

    for aid in ADMIN_IDS:
        try:
            await c.bot.send_message(
                aid,
                f"🧾 <b>New Order #{oid}</b>\n"
                f"User: <code>{u['tg_id']}</code>\n"
                f"Product: {p['name']}\n"
                f"Total: {p['price']} {CURRENCY}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Approve", callback_data=f"approve:{oid}"),
                    InlineKeyboardButton(text="❌ Reject", callback_data=f"reject:{oid}")
                ]])
            )
        except Exception:
            pass


@router.message(F.text == "👤 Profile")
async def profile(m: Message):
    u = get_user(m.from_user)
    await m.answer(
        f"👤 <b>Profile</b>\n"
        f"ID: <code>{u['tg_id']}</code>\n"
        f"Username: @{u['username'] or 'N/A'}\n"
        f"Balance: <b>{u['balance']} {CURRENCY}</b>"
    )


@router.message(F.text == "📦 My Orders")
async def orders(m: Message):
    u = get_user(m.from_user)
    rows = db.execute(
        """SELECT o.*, p.name FROM orders o
           JOIN products p ON p.id=o.product_id
           WHERE o.user_id=? ORDER BY o.id DESC LIMIT 20""",
        (u["id"],)
    ).fetchall()
    if not rows:
        return await m.answer("📦 No orders yet.")
    await m.answer(
        "📦 <b>Your orders</b>\n\n" +
        "\n".join(
            f"#{r['id']} — {r['name']} — {r['total']} {CURRENCY} — {r['status']}"
            for r in rows
        )
    )


@router.message(F.text == "💰 Add Balance")
async def add(m: Message, state: FSMContext):
    await state.set_state(Pay.amount)
    await m.answer(
        f"💳 <b>Add Balance</b>\n\n{PAYMENT_INFO}\n\n"
        f"Send the amount you paid:"
    )


@router.message(Pay.amount)
async def pay_amount(m: Message, state: FSMContext):
    try:
        amount = float(Decimal(m.text.strip()))
    except (InvalidOperation, ValueError, AttributeError):
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

    await m.answer(
        f"✅ Payment request #{pid} submitted. Admin will verify it."
    )
    for aid in ADMIN_IDS:
        try:
            await m.bot.send_message(
                aid,
                f"💳 <b>Payment #{pid}</b>\n"
                f"User: <code>{u['tg_id']}</code>\n"
                f"Amount: {d['amount']} {CURRENCY}\n"
                f"Method: {d['method']}\n"
                f"TrxID: <code>{m.text.strip()}</code>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Credit", callback_data=f"credit:{pid}"),
                    InlineKeyboardButton(text="❌ Reject", callback_data=f"payreject:{pid}")
                ]])
            )
        except Exception:
            pass


@router.message(F.text == "💬 Support")
async def support(m: Message):
    await m.answer(f"💬 Support: {SUPPORT}")


# ---------------- ADMIN ----------------

@router.message(Command("admin"))
async def admin(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Access denied.")
    await m.answer("🛠 <b>Admin Panel</b>", reply_markup=admin_kb())


@router.callback_query(F.data == "admin:home")
async def admin_home(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    await c.answer()
    await c.message.edit_text("🛠 <b>Admin Panel</b>", reply_markup=admin_kb())


@router.callback_query(F.data == "admin:stats")
async def admin_stats(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    users = db.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
    orders_n = db.execute("SELECT COUNT(*) n FROM orders").fetchone()["n"]
    pending_o = db.execute("SELECT COUNT(*) n FROM orders WHERE status='pending'").fetchone()["n"]
    payments_n = db.execute("SELECT COUNT(*) n FROM payments").fetchone()["n"]
    pending_p = db.execute("SELECT COUNT(*) n FROM payments WHERE status='pending'").fetchone()["n"]
    sales = db.execute(
        "SELECT COALESCE(SUM(total),0) s FROM orders WHERE status='completed'"
    ).fetchone()["s"]
    stock = db.execute("SELECT COALESCE(SUM(stock),0) s FROM products").fetchone()["s"]

    await c.answer()
    await c.message.edit_text(
        f"📊 <b>Statistics</b>\n\n"
        f"👤 Users: {users}\n"
        f"🧾 Orders: {orders_n}\n"
        f"⏳ Pending orders: {pending_o}\n"
        f"💳 Payments: {payments_n}\n"
        f"⏳ Pending payments: {pending_p}\n"
        f"💰 Completed sales: {sales} {CURRENCY}\n"
        f"📦 Total stock: {stock}",
        reply_markup=back_admin_kb()
    )


@router.callback_query(F.data == "admin:payments")
async def admin_payments(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    rows = db.execute(
        """SELECT p.*,u.tg_id,u.username FROM payments p
           JOIN users u ON u.id=p.user_id
           WHERE p.status='pending' ORDER BY p.id DESC LIMIT 15"""
    ).fetchall()
    await c.answer()
    if not rows:
        return await c.message.edit_text(
            "💳 <b>Pending Payments</b>\n\nNo pending payments.",
            reply_markup=back_admin_kb()
        )
    text = "💳 <b>Pending Payments</b>\n\n"
    for r in rows:
        text += (
            f"#{r['id']} — {r['amount']} {CURRENCY}\n"
            f"User: <code>{r['tg_id']}</code>\n"
            f"Method: {r['method']}\nTrxID: <code>{r['trx_id']}</code>\n\n"
        )
    await c.message.edit_text(text, reply_markup=back_admin_kb())


@router.callback_query(F.data == "admin:orders")
async def admin_orders(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    rows = db.execute(
        """SELECT o.*,p.name,u.tg_id FROM orders o
           JOIN products p ON p.id=o.product_id
           JOIN users u ON u.id=o.user_id
           WHERE o.status='pending' ORDER BY o.id DESC LIMIT 15"""
    ).fetchall()
    await c.answer()
    if not rows:
        return await c.message.edit_text(
            "🧾 <b>Pending Orders</b>\n\nNo pending orders.",
            reply_markup=back_admin_kb()
        )

    buttons = []
    for r in rows:
        buttons.append([
            InlineKeyboardButton(
                text=f"#{r['id']} {r['name']} | {r['total']} {CURRENCY}",
                callback_data=f"admin:order:{r['id']}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:home")])
    await c.message.edit_text(
        "🧾 <b>Pending Orders</b>\nChoose an order:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("admin:order:"))
async def admin_order_detail(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    oid = int(c.data.split(":")[2])
    o = db.execute(
        """SELECT o.*,p.name,u.tg_id,u.username FROM orders o
           JOIN products p ON p.id=o.product_id
           JOIN users u ON u.id=o.user_id WHERE o.id=?""",
        (oid,)
    ).fetchone()
    if not o:
        return await c.answer("Order not found", show_alert=True)
    await c.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Approve", callback_data=f"approve:{oid}"),
            InlineKeyboardButton(text="❌ Reject", callback_data=f"reject:{oid}")
        ],
        [InlineKeyboardButton(text="⬅️ Orders", callback_data="admin:orders")]
    ])
    await c.message.edit_text(
        f"🧾 <b>Order #{oid}</b>\n"
        f"User: <code>{o['tg_id']}</code>\n"
        f"Product: {o['name']}\n"
        f"Total: {o['total']} {CURRENCY}\n"
        f"Status: {o['status']}",
        reply_markup=kb
    )


@router.callback_query(F.data == "admin:stock")
async def admin_stock(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    rows = db.execute(
        "SELECT name,stock,price,active FROM products ORDER BY id"
    ).fetchall()
    await c.answer()
    text = "📦 <b>Product Stock</b>\n\n"
    for r in rows:
        state = "ON" if r["active"] else "OFF"
        text += f"• {r['name']} — stock: {r['stock']} — {r['price']} {CURRENCY} — {state}\n"
    await c.message.edit_text(text, reply_markup=admin_products_kb())


@router.callback_query(F.data == "admin:products")
async def admin_products(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    await c.answer()
    await c.message.edit_text(
        "🔄 <b>Product Management</b>\nSelect a product:",
        reply_markup=admin_products_kb()
    )


@router.callback_query(F.data.startswith("admin:editproduct:"))
async def admin_edit_product(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[2])
    p = db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    if not p:
        return await c.answer("Product not found", show_alert=True)
    await c.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add 1", callback_data=f"admin:addstock:{pid}:1"),
         InlineKeyboardButton(text="➕ Add 10", callback_data=f"admin:addstock:{pid}:10")],
        [InlineKeyboardButton(text="➕ Add 50", callback_data=f"admin:addstock:{pid}:50"),
         InlineKeyboardButton(text="➖ Remove 1", callback_data=f"admin:addstock:{pid}:-1")],
        [InlineKeyboardButton(
            text=("🔴 Disable" if p["active"] else "🟢 Enable"),
            callback_data=f"admin:toggle:{pid}"
        )],
        [InlineKeyboardButton(text="💰 Change Price", callback_data=f"admin:price:{pid}")],
        [InlineKeyboardButton(text="⬅️ Products", callback_data="admin:products")]
    ])
    await c.message.edit_text(
        f"📦 <b>{p['name']}</b>\n"
        f"Stock: {p['stock']}\nPrice: {p['price']} {CURRENCY}\n"
        f"Status: {'Active' if p['active'] else 'Disabled'}",
        reply_markup=kb
    )


@router.callback_query(F.data.startswith("admin:addstock:"))
async def admin_add_stock(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    _, _, pid, delta = c.data.split(":")
    pid, delta = int(pid), int(delta)
    p = db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    if not p:
        return await c.answer("Not found", show_alert=True)
    new_stock = max(0, p["stock"] + delta)
    db.execute("UPDATE products SET stock=? WHERE id=?", (new_stock, pid))
    db.commit()
    await c.answer(f"Stock: {new_stock}")
    await admin_edit_product(c)


@router.callback_query(F.data.startswith("admin:toggle:"))
async def admin_toggle(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[2])
    p = db.execute("SELECT active FROM products WHERE id=?", (pid,)).fetchone()
    if not p:
        return await c.answer("Not found", show_alert=True)
    new_value = 0 if p["active"] else 1
    db.execute("UPDATE products SET active=? WHERE id=?", (new_value, pid))
    db.commit()
    await c.answer("Updated")
    await admin_edit_product(c)


@router.callback_query(F.data.startswith("admin:price:"))
async def admin_price_prompt(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[2])
    await state.update_data(price_pid=pid)
    await state.set_state(AdminState.price)
    await c.answer()
    await c.message.answer("💰 Send the new price:")


@router.message(AdminState.price)
async def admin_price_set(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear()
        return await m.answer("⛔ Access denied.")
    try:
        price = float(Decimal(m.text.strip()))
    except (InvalidOperation, ValueError, AttributeError):
        return await m.answer("❌ Send a valid price.")
    if price < 0:
        return await m.answer("❌ Price cannot be negative.")
    data = await state.get_data()
    pid = data["price_pid"]
    db.execute("UPDATE products SET price=? WHERE id=?", (price, pid))
    db.commit()
    await state.clear()
    await m.answer(f"✅ Product price updated to {price} {CURRENCY}.", reply_markup=admin_kb())


@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_prompt(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    await state.set_state(AdminState.broadcast)
    await c.answer()
    await c.message.answer(
        "📢 <b>Broadcast</b>\n\n"
        "Send the message you want to broadcast to all unblocked users."
    )


@router.message(AdminState.broadcast)
async def admin_broadcast(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear()
        return await m.answer("⛔ Access denied.")

    users = db.execute("SELECT tg_id FROM users WHERE blocked=0").fetchall()
    sent = 0
    failed = 0

    for row in users:
        try:
            await m.bot.copy_message(
                chat_id=row["tg_id"],
                from_chat_id=m.chat.id,
                message_id=m.message_id
            )
            sent += 1
        except Exception:
            failed += 1

    await state.clear()
    await m.answer(
        f"📢 <b>Broadcast finished</b>\n"
        f"✅ Sent: {sent}\n"
        f"❌ Failed: {failed}",
        reply_markup=admin_kb()
    )


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
    db.execute(
        "UPDATE users SET balance=balance+? WHERE id=?",
        (o["total"], o["user_id"])
    )
    db.execute(
        "UPDATE products SET stock=stock+1 WHERE id=?",
        (o["product_id"],)
    )
    db.commit()

    u = db.execute("SELECT * FROM users WHERE id=?", (o["user_id"],)).fetchone()
    await c.answer("Rejected/refunded")
    await c.message.edit_text(f"❌ Order #{oid} rejected and refunded.")
    try:
        await c.bot.send_message(
            u["tg_id"],
            f"❌ Order #{oid} rejected.\n"
            f"💸 {o['total']} {CURRENCY} refunded to your wallet."
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("credit:"))
async def credit(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[1])
    p = db.execute("SELECT * FROM payments WHERE id=?", (pid,)).fetchone()
    if not p or p["status"] != "pending":
        return await c.answer("Already processed", show_alert=True)

    db.execute("UPDATE payments SET status='credited' WHERE id=?", (pid,))
    db.execute(
        "UPDATE users SET balance=balance+? WHERE id=?",
        (p["amount"], p["user_id"])
    )
    db.commit()

    u = db.execute("SELECT * FROM users WHERE id=?", (p["user_id"],)).fetchone()
    await c.answer("Credited")
    await c.message.edit_text(f"✅ Payment #{pid} credited.")
    try:
        await c.bot.send_message(
            u["tg_id"],
            f"💰 Added {p['amount']} {CURRENCY} to your balance."
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("payreject:"))
async def payreject(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    pid = int(c.data.split(":")[1])
    cur = db.execute(
        "UPDATE payments SET status='rejected' WHERE id=? AND status='pending'",
        (pid,)
    )
    db.commit()
    if cur.rowcount == 0:
        return await c.answer("Already processed", show_alert=True)

    p = db.execute("SELECT user_id,amount FROM payments WHERE id=?", (pid,)).fetchone()
    await c.answer("Rejected")
    await c.message.edit_text(f"❌ Payment #{pid} rejected.")
    if p:
        u = db.execute("SELECT tg_id FROM users WHERE id=?", (p["user_id"],)).fetchone()
        try:
            await c.bot.send_message(
                u["tg_id"],
                f"❌ Payment #{pid} rejected.\n"
                f"No balance was added."
            )
        except Exception:
            pass


async def main():
    start_health_server()
    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
