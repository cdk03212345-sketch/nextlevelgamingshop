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

db.executescript("""
CREATE TABLE IF NOT EXISTS admin_roles(
    tg_id INTEGER PRIMARY KEY,
    role TEXT NOT NULL DEFAULT 'staff'
);
CREATE TABLE IF NOT EXISTS admin_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
""")
for _aid in ADMIN_IDS:
    db.execute("INSERT OR IGNORE INTO admin_roles(tg_id, role) VALUES(?, 'owner')", (_aid,))
db.commit()

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

class Pay(StatesGroup):
    amount = State()
    method = State()
    trx = State()

class AdminAction(StatesGroup):
    broadcast = State()
    stock = State()
    price = State()
    add_product = State()
    edit_description = State()
    search_user = State()
    balance_user = State()

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
async def buy(c: CallbackQuery):
    pid=int(c.data.split(":")[1])
    p=db.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    u=get_user(c.from_user)
    if not p or p["active"]!=1 or p["stock"]<1:
        return await c.answer("❌ Out of stock.", show_alert=True)
    if u["balance"] < p["price"]:
        return await c.answer("❌ Insufficient balance.", show_alert=True)
    db.execute("UPDATE users SET balance=balance-? WHERE id=?", (p["price"],u["id"]))
    db.execute("UPDATE products SET stock=stock-1 WHERE id=?", (p["id"],))
    cur=db.execute("INSERT INTO orders(user_id,product_id,total) VALUES(?,?,?)",
                   (u["id"],p["id"],p["price"]))
    db.commit()
    oid=cur.lastrowid
    await c.answer("Order created!")
    await c.message.answer(
        f"✅ <b>Order #{oid} created!</b>\nProduct: {p['name']}\n"
        f"Total: {p['price']} {CURRENCY}\nStatus: ⏳ Pending"
    )
    for aid in ADMIN_IDS:
        try:
            await c.bot.send_message(
                aid,
                f"🧾 <b>New Order #{oid}</b>\nUser: <code>{u['tg_id']}</code>\n"
                f"Product: {p['name']}\nTotal: {p['price']} {CURRENCY}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Approve",callback_data=f"approve:{oid}"),
                    InlineKeyboardButton(text="❌ Reject",callback_data=f"reject:{oid}")
                ]])
            )
        except: pass

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
        [InlineKeyboardButton(text="🛒 Product Management", callback_data="p1:menu")],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin:panel")],
    ])
def admin_role(user_id:int):
    row=db.execute("SELECT role FROM admin_roles WHERE tg_id=?", (user_id,)).fetchone()
    return row["role"] if row else None

def admin_can(user_id:int, minimum="staff"):
    role=admin_role(user_id)
    rank={"staff":1,"manager":2,"owner":3}
    return role is not None and rank.get(role,0) >= rank.get(minimum,99)

def is_admin(user_id:int) -> bool:
    return admin_role(user_id) is not None

def log_admin(user_id:int, action:str):
    db.execute("INSERT INTO admin_logs(admin_id,action) VALUES(?,?)", (user_id,action))
    db.commit()

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

def p1_product_list(callback_prefix):
    rows=db.execute("SELECT id,name,price,stock FROM products ORDER BY id").fetchall()
    kb=[[InlineKeyboardButton(
        text=f"{r['name']} — {r['price']} {CURRENCY} | Stock {r['stock']}",
        callback_data=f"{callback_prefix}:{r['id']}"
    )] for r in rows]
    kb.append([InlineKeyboardButton(text="⬅️ Admin Panel", callback_data="admin:panel")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


@router.callback_query(F.data=="p1:menu")
async def p1_menu(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await c.answer()
    await c.message.edit_text(
        "🛒 <b>Product & User Management</b>\n\n"
        "➕ Add product\n✏️ Edit description\n🗑 Delete product\n"
        "🔎 Search user\n💰 Add/Deduct balance\n👑 Roles",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Add Product",callback_data="p1:add")],
            [InlineKeyboardButton(text="✏️ Edit Description",callback_data="p1:editdesc"),
             InlineKeyboardButton(text="🗑 Delete Product",callback_data="p1:delete")],
            [InlineKeyboardButton(text="🔎 Search User",callback_data="p1:search")],
            [InlineKeyboardButton(text="💰 Add/Deduct Balance",callback_data="p1:balance")],
            [InlineKeyboardButton(text="👑 Admin Roles",callback_data="p1:roles")],
            [InlineKeyboardButton(text="⬅️ Admin Panel",callback_data="admin:panel")]
        ])
    )


@router.callback_query(F.data=="p1:add")
async def p1_add(c:CallbackQuery,state:FSMContext):
    if not admin_can(c.from_user.id,"manager"): return await c.answer("Manager/Owner only.",show_alert=True)
    await state.set_state(AdminAction.add_product)
    await c.answer()
    await c.message.answer(
        "➕ Send:\n<code>Name | Category | Price | Stock | Description</code>\n"
        "Example: <code>100 UC | UC | 95 | 10 | PUBG UC</code>"
    )


@router.message(AdminAction.add_product)
async def p1_add_save(m:Message,state:FSMContext):
    if not admin_can(m.from_user.id,"manager"):
        await state.clear(); return await m.answer("⛔ Manager/Owner only.")
    parts=[x.strip() for x in (m.text or "").split("|",4)]
    if len(parts)!=5: return await m.answer("❌ Format: Name | Category | Price | Stock | Description")
    name,category,price_s,stock_s,description=parts
    try:
        price=float(price_s); stock=int(stock_s)
        if price<0 or stock<0: raise ValueError
    except ValueError:
        return await m.answer("❌ Price/Stock number হতে হবে.")
    try:
        db.execute(
            "INSERT INTO products(name,category,quantity,price,stock,active,description) VALUES(?,?,?,?,?,?,?)",
            (name,category,0,price,stock,1,description)
        )
        db.commit()
    except sqlite3.IntegrityError:
        return await m.answer("❌ এই নামে product already আছে.")
    log_admin(m.from_user.id,f"Added product: {name}")
    await state.clear()
    await m.answer(f"✅ <b>{name}</b> added.",reply_markup=admin_kb())


@router.callback_query(F.data=="p1:delete")
async def p1_delete_list(c:CallbackQuery):
    if not admin_can(c.from_user.id,"owner"): return await c.answer("Owner only.",show_alert=True)
    await c.answer()
    await c.message.edit_text("🗑 <b>Select product:</b>",reply_markup=p1_product_list("p1:del"))


@router.callback_query(F.data.startswith("p1:del:"))
async def p1_delete(c:CallbackQuery):
    if not admin_can(c.from_user.id,"owner"): return await c.answer("Owner only.",show_alert=True)
    pid=int(c.data.split(":")[2])
    p=db.execute("SELECT name FROM products WHERE id=?",(pid,)).fetchone()
    if not p: return await c.answer("Not found",show_alert=True)
    db.execute("DELETE FROM products WHERE id=?",(pid,)); db.commit()
    log_admin(c.from_user.id,f"Deleted product: {p['name']} id={pid}")
    await c.answer("Deleted")
    await c.message.edit_text("✅ Product deleted.",reply_markup=admin_kb())


@router.callback_query(F.data=="p1:editdesc")
async def p1_editdesc_list(c:CallbackQuery):
    if not admin_can(c.from_user.id,"manager"): return await c.answer("Manager/Owner only.",show_alert=True)
    await c.answer()
    await c.message.edit_text("✏️ <b>Select product:</b>",reply_markup=p1_product_list("p1:desc"))


@router.callback_query(F.data.startswith("p1:desc:"))
async def p1_editdesc_prompt(c:CallbackQuery,state:FSMContext):
    if not admin_can(c.from_user.id,"manager"): return await c.answer("Manager/Owner only.",show_alert=True)
    pid=int(c.data.split(":")[2])
    p=db.execute("SELECT name,description FROM products WHERE id=?",(pid,)).fetchone()
    if not p: return await c.answer("Not found",show_alert=True)
    await state.update_data(pid=pid)
    await state.set_state(AdminAction.edit_description)
    await c.answer()
    await c.message.answer(f"✏️ <b>{p['name']}</b>\nSend the new description:")


@router.message(AdminAction.edit_description)
async def p1_editdesc_save(m:Message,state:FSMContext):
    if not admin_can(m.from_user.id,"manager"):
        await state.clear(); return await m.answer("⛔ Manager/Owner only.")
    d=await state.get_data()
    db.execute("UPDATE products SET description=? WHERE id=?",(m.text or "",d["pid"])); db.commit()
    p=db.execute("SELECT name FROM products WHERE id=?",(d["pid"],)).fetchone()
    log_admin(m.from_user.id,f"Edited description: {p['name']} id={d['pid']}")
    await state.clear()
    await m.answer("✅ Description updated.",reply_markup=admin_kb())


@router.callback_query(F.data=="p1:search")
async def p1_search(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await state.set_state(AdminAction.search_user)
    await c.answer()
    await c.message.answer("🔎 Send Telegram User ID or @username:")


@router.message(AdminAction.search_user)
async def p1_search_save(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear(); return await m.answer("⛔ Access denied.")
    q=(m.text or "").strip().lstrip("@")
    row=db.execute("SELECT * FROM users WHERE tg_id=?",(int(q),)).fetchone() if q.isdigit() else db.execute(
        "SELECT * FROM users WHERE lower(username)=lower(?)",(q,)).fetchone()
    await state.clear()
    if not row: return await m.answer("❌ User not found.",reply_markup=admin_kb())
    await m.answer(
        f"👤 <b>User</b>\nID: <code>{row['tg_id']}</code>\n"
        f"Username: @{row['username'] or 'N/A'}\nBalance: <b>{row['balance']} {CURRENCY}</b>\n"
        f"Blocked: {'Yes' if row['blocked'] else 'No'}",
        reply_markup=admin_kb()
    )


@router.callback_query(F.data=="p1:balance")
async def p1_balance(c:CallbackQuery,state:FSMContext):
    if not admin_can(c.from_user.id,"manager"): return await c.answer("Manager/Owner only.",show_alert=True)
    await state.set_state(AdminAction.balance_user)
    await c.answer()
    await c.message.answer("💰 Send: <code>USER_ID | AMOUNT</code>\nPositive = Add, Negative = Deduct.")


@router.message(AdminAction.balance_user)
async def p1_balance_save(m:Message,state:FSMContext):
    if not admin_can(m.from_user.id,"manager"):
        await state.clear(); return await m.answer("⛔ Manager/Owner only.")
    parts=[x.strip() for x in (m.text or "").split("|",1)]
    if len(parts)!=2 or not parts[0].isdigit(): return await m.answer("❌ Format: USER_ID | AMOUNT")
    try: amount=float(parts[1])
    except ValueError: return await m.answer("❌ Invalid amount.")
    tg_id=int(parts[0])
    user=db.execute("SELECT * FROM users WHERE tg_id=?",(tg_id,)).fetchone()
    if not user: return await m.answer("❌ User not found.")
    new_balance=user["balance"]+amount
    if new_balance<0: return await m.answer("❌ Balance cannot be below 0.")
    db.execute("UPDATE users SET balance=? WHERE tg_id=?",(new_balance,tg_id)); db.commit()
    log_admin(m.from_user.id,f"Balance change {amount} for user {tg_id}")
    await state.clear()
    await m.answer(f"✅ New balance: <b>{new_balance} {CURRENCY}</b>",reply_markup=admin_kb())


@router.callback_query(F.data=="p1:roles")
async def p1_roles(c:CallbackQuery):
    if not admin_can(c.from_user.id,"owner"): return await c.answer("Owner only.",show_alert=True)
    rows=db.execute("SELECT tg_id,role FROM admin_roles ORDER BY role,tg_id").fetchall()
    text="👑 <b>Admin Roles</b>\n\n" + "\n".join(
        f"<code>{r['tg_id']}</code> — {r['role'].title()}" for r in rows
    )
    text += "\n\nCurrent ADMIN_IDS are Owners. Manager/Staff can be added to the database later."
    await c.answer()
    await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Admin Panel",callback_data="admin:panel")]]
    ))


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
