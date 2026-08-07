import os
import sqlite3
import asyncio
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from decimal import Decimal
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
PAYMENT_INFO = os.getenv("PAYMENT_INSTRUCTIONS", "bKash/Nagad: YOUR NUMBER")
SUPPORT = os.getenv("SUPPORT_USERNAME", "@YourSupport")
CURRENCY = os.getenv("CURRENCY", "BDT")

if not TOKEN:
    raise RuntimeError("BOT_TOKEN missing")

db = sqlite3.connect("nextlevel.db", check_same_thread=False)
db.row_factory = sqlite3.Row
db.executescript("""
CREATE TABLE IF NOT EXISTS users(
 id INTEGER PRIMARY KEY AUTOINCREMENT, tg_id INTEGER UNIQUE, username TEXT,
 name TEXT, balance REAL DEFAULT 0, blocked INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS products(
 id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, category TEXT, quantity INTEGER,
 price REAL, stock INTEGER DEFAULT 0, active INTEGER DEFAULT 1, description TEXT);
CREATE TABLE IF NOT EXISTS orders(
 id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, product_id INTEGER,
 game_uid TEXT, total REAL, status TEXT DEFAULT 'pending', created_at DATETIME DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS payments(
 id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL,
 method TEXT, trx_id TEXT, status TEXT DEFAULT 'pending', created_at DATETIME DEFAULT CURRENT_TIMESTAMP);
""")

# Product list matching the requested shop list.
# Existing products are updated and missing products are added, so the list
# stays correct even if the SQLite database already contains older sample data.
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
    existing = db.execute(
        "SELECT id FROM products WHERE name=?", (name,)
    ).fetchone()
    # Seed only missing products. Existing stock/price/active values are
    # preserved so admin changes survive restarts and Render redeploys.
    if not existing:
        db.execute(
            "INSERT INTO products(name,category,quantity,price,stock,active) VALUES(?,?,?,?,?,1)",
            (name, category, quantity, price, stock),
        )
db.commit()


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass

def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

router = Router()

def menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🛒 Shop"), KeyboardButton(text="👤 Profile")],
        [KeyboardButton(text="💰 Add Balance"), KeyboardButton(text="📦 My Orders")],
        [KeyboardButton(text="💬 Support")]], resize_keyboard=True)

def products_kb():
    order_sql = """
        CASE name
            WHEN '20 UC' THEN 1
            WHEN '36 UC' THEN 2
            WHEN '80 UC' THEN 3
            WHEN '160 UC' THEN 4
            WHEN '161 UC' THEN 5
            WHEN '162 UC' THEN 6
            WHEN '405 UC' THEN 7
            WHEN '800 UC' THEN 8
            WHEN '810 UC' THEN 9
            WHEN '1625 UC' THEN 10
            WHEN '2000 UC' THEN 11
            WHEN '650 Shell' THEN 12
            WHEN '1300 Shell' THEN 13
            ELSE 99
        END
    """
    rows = db.execute(
        f"SELECT * FROM products WHERE active=1 ORDER BY {order_sql}"
    ).fetchall()
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{p['name']} — {p['price']} {CURRENCY}", callback_data=f"product:{p['id']}")]
        for p in rows])

class Buy(StatesGroup):
    uid = State()

class Pay(StatesGroup):
    amount = State()
    method = State()
    trx = State()

class AdminAction(StatesGroup):
    broadcast = State()
    stock = State()
    price = State()

def get_user(tg):
    u = db.execute("SELECT * FROM users WHERE tg_id=?", (tg.id,)).fetchone()
    if not u:
        db.execute("INSERT INTO users(tg_id,username,name) VALUES(?,?,?)",
                   (tg.id, tg.username, tg.full_name))
        db.commit()
        u = db.execute("SELECT * FROM users WHERE tg_id=?", (tg.id,)).fetchone()
    else:
        db.execute("UPDATE users SET username=?,name=? WHERE tg_id=?",
                   (tg.username,tg.full_name,tg.id)); db.commit()
        u = db.execute("SELECT * FROM users WHERE tg_id=?", (tg.id,)).fetchone()
    return u

@router.message(CommandStart())
async def start(m: Message):
    u=get_user(m.from_user)
    await m.answer(f"🎮 <b>Welcome to Nextlevelgamingshop!</b>\n\nBalance: <b>{u['balance']} {CURRENCY}</b>", reply_markup=menu())

@router.message(Command("shop"))
@router.message(F.text=="🛒 Shop")
async def shop(m: Message):
    if not db.execute("SELECT 1 FROM products WHERE active=1").fetchone():
        await m.answer("❌ No products available.")
        return
    await m.answer("🛒 <b>Choose a product:</b>", reply_markup=products_kb())

@router.callback_query(F.data.startswith("product:"))
async def product(c: CallbackQuery):
    pid=int(c.data.split(":")[1])
    p=db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    await c.answer()
    if not p: return await c.message.answer("❌ Product not found.")
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Buy Now", callback_data=f"buy:{pid}")],
        [InlineKeyboardButton(text="⬅️ Shop", callback_data="shop")]])
    await c.message.edit_text(f"🎮 <b>{p['name']}</b>\n💰 {p['price']} {CURRENCY}\n📦 Stock: {p['stock']}\n\n{p['description'] or ''}", reply_markup=kb)

@router.callback_query(F.data=="shop")
async def shop_cb(c: CallbackQuery):
    await c.answer()
    await c.message.edit_text("🛒 <b>Choose a product:</b>", reply_markup=products_kb())

@router.callback_query(F.data.startswith("buy:"))
async def buy(c: CallbackQuery, state: FSMContext):
    pid=int(c.data.split(":")[1])
    await state.update_data(pid=pid)
    await state.set_state(Buy.uid)
    await c.answer()
    await c.message.answer("🆔 Send your game/player UID:")

@router.message(Command("admin"))
async def admin_command(m: Message, state: FSMContext):
    # Commands must work even if the user was previously inside a UID/payment state.
    await state.clear()
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Access denied.")
    await m.answer("🛠 <b>Admin Panel</b>\nChoose an action:", reply_markup=admin_kb())

@router.message(Command("cancel"))
async def cancel_command(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("❌ Cancelled.", reply_markup=menu())

@router.message(Buy.uid)
async def uid(m: Message, state: FSMContext):
    data=await state.get_data(); p=db.execute("SELECT * FROM products WHERE id=?", (data["pid"],)).fetchone()
    u=get_user(m.from_user)
    if not p or p["active"]!=1 or p["stock"]<1:
        await state.clear(); return await m.answer("❌ Out of stock.")
    if u["balance"] < p["price"]:
        await state.clear(); return await m.answer(f"❌ Insufficient balance. Your balance: {u['balance']} {CURRENCY}")
    db.execute("UPDATE users SET balance=balance-? WHERE id=?", (p["price"],u["id"]))
    db.execute("UPDATE products SET stock=stock-1 WHERE id=?", (p["id"],))
    cur=db.execute("INSERT INTO orders(user_id,product_id,game_uid,total) VALUES(?,?,?,?)",
                   (u["id"],p["id"],m.text.strip(),p["price"]))
    db.commit(); oid=cur.lastrowid
    await state.clear()
    await m.answer(f"✅ <b>Order #{oid} created!</b>\nProduct: {p['name']}\nUID: <code>{m.text.strip()}</code>\nTotal: {p['price']} {CURRENCY}\nStatus: ⏳ Pending")
    for aid in ADMIN_IDS:
        try: await m.bot.send_message(aid, f"🧾 <b>New Order #{oid}</b>\nUser: <code>{u['tg_id']}</code>\nProduct: {p['name']}\nUID: <code>{m.text.strip()}</code>\nTotal: {p['price']} {CURRENCY}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Approve",callback_data=f"approve:{oid}"),InlineKeyboardButton(text="❌ Reject",callback_data=f"reject:{oid}")]]))
        except: pass

@router.message(F.text=="👤 Profile")
async def profile(m: Message):
    u=get_user(m.from_user)
    await m.answer(f"👤 <b>Profile</b>\nID: <code>{u['tg_id']}</code>\nBalance: <b>{u['balance']} {CURRENCY}</b>")

@router.message(F.text=="📦 My Orders")
async def orders(m: Message):
    u=get_user(m.from_user)
    rows=db.execute("SELECT o.*,p.name FROM orders o JOIN products p ON p.id=o.product_id WHERE o.user_id=? ORDER BY o.id DESC LIMIT 10",(u["id"],)).fetchall()
    if not rows: return await m.answer("📦 No orders yet.")
    await m.answer("📦 <b>Your orders</b>\n\n" + "\n".join(f"#{r['id']} — {r['name']} — {r['total']} {CURRENCY} — {r['status']}" for r in rows))

@router.message(F.text=="💰 Add Balance")
async def add(m: Message,state:FSMContext):
    await state.set_state(Pay.amount)
    await m.answer(f"💳 <b>Add Balance</b>\n\n{PAYMENT_INFO}\n\nSend the amount you paid:")

@router.message(Pay.amount)
async def pay_amount(m:Message,state:FSMContext):
    try: amount=float(m.text)
    except: return await m.answer("❌ Send a valid amount.")
    if amount<=0: return await m.answer("❌ Amount must be positive.")
    await state.update_data(amount=amount); await state.set_state(Pay.method)
    await m.answer("Send payment method (bKash/Nagad/etc.):")

@router.message(Pay.method)
async def pay_method(m:Message,state:FSMContext):
    await state.update_data(method=m.text); await state.set_state(Pay.trx)
    await m.answer("Send your TrxID:")

@router.message(Pay.trx)
async def pay_trx(m:Message,state:FSMContext):
    d=await state.get_data(); u=get_user(m.from_user)
    cur=db.execute("INSERT INTO payments(user_id,amount,method,trx_id) VALUES(?,?,?,?)",(u["id"],d["amount"],d["method"],m.text.strip()))
    db.commit(); pid=cur.lastrowid; await state.clear()
    await m.answer(f"✅ Payment request #{pid} submitted. Admin will verify it.")
    for aid in ADMIN_IDS:
        try: await m.bot.send_message(aid,f"💳 <b>Payment #{pid}</b>\nUser: <code>{u['tg_id']}</code>\nAmount: {d['amount']} {CURRENCY}\nMethod: {d['method']}\nTrxID: <code>{m.text.strip()}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Credit",callback_data=f"credit:{pid}"),InlineKeyboardButton(text="❌ Reject",callback_data=f"payreject:{pid}")]]))
        except: pass

@router.message(F.text=="💬 Support")
async def support(m:Message): await m.answer(f"💬 Support: {SUPPORT}")

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Statistics", callback_data="admin:stats"),
         InlineKeyboardButton(text="📢 Broadcast", callback_data="admin:broadcast")],
        [InlineKeyboardButton(text="📦 Stock", callback_data="admin:stock"),
         InlineKeyboardButton(text="💰 Change Price", callback_data="admin:price")],
        [InlineKeyboardButton(text="🟢/🔴 Products", callback_data="admin:products")],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin:panel")],
    ])

def is_admin(user_id:int) -> bool:
    return user_id in ADMIN_IDS

def admin_stats_text():
    users = db.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
    orders = db.execute("SELECT COUNT(*) c FROM orders").fetchone()["c"]
    pending_orders = db.execute("SELECT COUNT(*) c FROM orders WHERE status='pending'").fetchone()["c"]
    completed = db.execute("SELECT COUNT(*) c FROM orders WHERE status='completed'").fetchone()["c"]
    rejected = db.execute("SELECT COUNT(*) c FROM orders WHERE status='rejected'").fetchone()["c"]
    payments = db.execute("SELECT COUNT(*) c FROM payments").fetchone()["c"]
    pending_payments = db.execute("SELECT COUNT(*) c FROM payments WHERE status='pending'").fetchone()["c"]
    balance = db.execute("SELECT COALESCE(SUM(balance),0) total FROM users").fetchone()["total"]
    sales = db.execute("SELECT COALESCE(SUM(total),0) total FROM orders WHERE status='completed'").fetchone()["total"]
    products = db.execute("SELECT COUNT(*) c FROM products").fetchone()["c"]
    active = db.execute("SELECT COUNT(*) c FROM products WHERE active=1").fetchone()["c"]
    return (f"📊 <b>Admin Statistics</b>\n\n"
            f"👥 Users: <b>{users}</b>\n"
            f"📦 Products: <b>{products}</b> (Active: {active})\n"
            f"🧾 Orders: <b>{orders}</b>\n"
            f"⏳ Pending orders: <b>{pending_orders}</b>\n"
            f"✅ Completed: <b>{completed}</b>\n"
            f"❌ Rejected: <b>{rejected}</b>\n"
            f"💳 Payments: <b>{payments}</b> (Pending: {pending_payments})\n"
            f"💰 User balances: <b>{balance:.2f} {CURRENCY}</b>\n"
            f"📈 Completed sales: <b>{sales:.2f} {CURRENCY}</b>")

@router.callback_query(F.data=="admin:panel")
async def admin_panel(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    await c.answer()
    await c.message.edit_text("🛠 <b>Admin Panel</b>\nChoose an action:", reply_markup=admin_kb())

@router.callback_query(F.data=="admin:stats")
async def admin_stats(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    await c.answer()
    await c.message.edit_text(admin_stats_text(), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:panel")]]))

@router.callback_query(F.data=="admin:broadcast")
async def admin_broadcast_start(c:CallbackQuery, state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    await c.answer()
    await state.set_state(AdminAction.broadcast)
    await c.message.answer("📢 Send the broadcast message now.\n\nSend /cancel to cancel.")

@router.message(AdminAction.broadcast)
async def admin_broadcast_send(m:Message, state:FSMContext):
    if not is_admin(m.from_user.id): return
    if m.text and m.text.strip().lower()=="/cancel":
        await state.clear(); return await m.answer("❌ Broadcast cancelled.")
    rows=db.execute("SELECT tg_id FROM users WHERE blocked=0").fetchall()
    sent=0; failed=0
    for row in rows:
        try:
            await m.bot.copy_message(row["tg_id"], m.chat.id, m.message_id)
            sent += 1
        except Exception:
            failed += 1
    await state.clear()
    await m.answer(f"📢 Broadcast finished.\n✅ Sent: {sent}\n❌ Failed: {failed}", reply_markup=admin_kb())

@router.callback_query(F.data=="admin:stock")
async def admin_stock_start(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    await c.answer()
    rows=db.execute("SELECT id,name,stock,active FROM products ORDER BY id").fetchall()
    kb=[[InlineKeyboardButton(text=f"{r['name']} — stock {r['stock']}", callback_data=f"stockpick:{r['id']}")] for r in rows]
    kb.append([InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:panel")])
    await c.message.edit_text("📦 <b>Stock Management</b>\nSelect a product:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("stockpick:"))
async def admin_stock_pick(c:CallbackQuery, state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    pid=int(c.data.split(":")[1]); p=db.execute("SELECT * FROM products WHERE id=?",(pid,)).fetchone()
    if not p: return await c.answer("Product not found", show_alert=True)
    await c.answer(); await state.update_data(pid=pid); await state.set_state(AdminAction.stock)
    await c.message.answer(f"📦 <b>{p['name']}</b> current stock: {p['stock']}\nSend new stock number (0 or more):")

@router.message(AdminAction.stock)
async def admin_stock_set(m:Message, state:FSMContext):
    if not is_admin(m.from_user.id): return
    try: stock=int(m.text.strip())
    except: return await m.answer("❌ Send a whole number, e.g. 50")
    if stock<0: return await m.answer("❌ Stock cannot be negative.")
    d=await state.get_data(); db.execute("UPDATE products SET stock=? WHERE id=?",(stock,d["pid"])); db.commit(); await state.clear()
    p=db.execute("SELECT name FROM products WHERE id=?",(d["pid"],)).fetchone()
    await m.answer(f"✅ Stock updated: <b>{p['name']}</b> → {stock}", reply_markup=admin_kb())

@router.callback_query(F.data=="admin:price")
async def admin_price_start(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    await c.answer()
    rows=db.execute("SELECT id,name,price FROM products ORDER BY id").fetchall()
    kb=[[InlineKeyboardButton(text=f"{r['name']} — {r['price']} {CURRENCY}", callback_data=f"pricepick:{r['id']}")] for r in rows]
    kb.append([InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:panel")])
    await c.message.edit_text("💰 <b>Price Management</b>\nSelect a product:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("pricepick:"))
async def admin_price_pick(c:CallbackQuery, state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    pid=int(c.data.split(":")[1]); p=db.execute("SELECT * FROM products WHERE id=?",(pid,)).fetchone()
    if not p: return await c.answer("Product not found", show_alert=True)
    await c.answer(); await state.update_data(pid=pid); await state.set_state(AdminAction.price)
    await c.message.answer(f"💰 <b>{p['name']}</b> current price: {p['price']} {CURRENCY}\nSend new price:")

@router.message(AdminAction.price)
async def admin_price_set(m:Message, state:FSMContext):
    if not is_admin(m.from_user.id): return
    try: price=float(m.text.strip())
    except: return await m.answer("❌ Send a valid number, e.g. 149")
    if price<0: return await m.answer("❌ Price cannot be negative.")
    d=await state.get_data(); db.execute("UPDATE products SET price=? WHERE id=?",(price,d["pid"])); db.commit(); await state.clear()
    p=db.execute("SELECT name FROM products WHERE id=?",(d["pid"],)).fetchone()
    await m.answer(f"✅ Price updated: <b>{p['name']}</b> → {price:g} {CURRENCY}", reply_markup=admin_kb())

@router.callback_query(F.data=="admin:products")
async def admin_products(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    await c.answer()
    rows=db.execute("SELECT id,name,price,stock,active FROM products ORDER BY id").fetchall()
    kb=[]
    for r in rows:
        status="🟢 ON" if r["active"] else "🔴 OFF"
        kb.append([InlineKeyboardButton(text=f"{status} {r['name']}", callback_data=f"toggle:{r['id']}")])
    kb.append([InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:panel")])
    await c.message.edit_text("🟢/🔴 <b>Product Enable / Disable</b>\nTap a product to toggle:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("toggle:"))
async def admin_toggle(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    pid=int(c.data.split(":")[1]); p=db.execute("SELECT * FROM products WHERE id=?",(pid,)).fetchone()
    if not p: return await c.answer("Product not found", show_alert=True)
    new=0 if p["active"] else 1
    db.execute("UPDATE products SET active=? WHERE id=?",(new,pid)); db.commit()
    await c.answer("Enabled" if new else "Disabled")
    rows=db.execute("SELECT id,name,active FROM products ORDER BY id").fetchall()
    kb=[[InlineKeyboardButton(text=f"{'🟢 ON' if r['active'] else '🔴 OFF'} {r['name']}", callback_data=f"toggle:{r['id']}")] for r in rows]
    kb.append([InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:panel")])
    await c.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("approve:"))
async def approve(c:CallbackQuery):
    if c.from_user.id not in ADMIN_IDS: return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":")[1]); o=db.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    if not o or o["status"]!="pending": return await c.answer("Already processed",show_alert=True)
    db.execute("UPDATE orders SET status='completed' WHERE id=?",(oid,)); db.commit()
    u=db.execute("SELECT * FROM users WHERE id=?",(o["user_id"],)).fetchone()
    await c.answer("Approved"); await c.message.edit_text(f"✅ Order #{oid} completed.")
    try: await c.bot.send_message(u["tg_id"],f"🎉 <b>Order #{oid} completed!</b>")
    except: pass

@router.callback_query(F.data.startswith("reject:"))
async def reject(c:CallbackQuery):
    if c.from_user.id not in ADMIN_IDS: return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":")[1]); o=db.execute("SELECT * FROM orders WHERE id=?",(oid,)).fetchone()
    if not o or o["status"]!="pending": return await c.answer("Already processed",show_alert=True)
    db.execute("UPDATE orders SET status='rejected' WHERE id=?",(oid,))
    db.execute("UPDATE users SET balance=balance+? WHERE id=?",(o["total"],o["user_id"]))
    db.execute("UPDATE products SET stock=stock+1 WHERE id=?",(o["product_id"],)); db.commit()
    await c.answer("Rejected/refunded"); await c.message.edit_text(f"❌ Order #{oid} rejected and refunded.")

@router.callback_query(F.data.startswith("credit:"))
async def credit(c:CallbackQuery):
    if c.from_user.id not in ADMIN_IDS: return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1]); p=db.execute("SELECT * FROM payments WHERE id=?",(pid,)).fetchone()
    if not p or p["status"]!="pending": return await c.answer("Already processed",show_alert=True)
    db.execute("UPDATE payments SET status='credited' WHERE id=?",(pid,))
    db.execute("UPDATE users SET balance=balance+? WHERE id=?",(p["amount"],p["user_id"])); db.commit()
    u=db.execute("SELECT * FROM users WHERE id=?",(p["user_id"],)).fetchone()
    await c.answer("Credited"); await c.message.edit_text(f"✅ Payment #{pid} credited.")
    try: await c.bot.send_message(u["tg_id"],f"💰 Added {p['amount']} {CURRENCY} to your balance.")
    except: pass

@router.callback_query(F.data.startswith("payreject:"))
async def payreject(c:CallbackQuery):
    if c.from_user.id not in ADMIN_IDS: return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1]); db.execute("UPDATE payments SET status='rejected' WHERE id=? AND status='pending'",(pid,)); db.commit()
    await c.answer("Rejected"); await c.message.edit_text(f"❌ Payment #{pid} rejected.")

async def main():
    start_health_server()
    bot=Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp=Dispatcher(); dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__=="__main__":
    asyncio.run(main())
