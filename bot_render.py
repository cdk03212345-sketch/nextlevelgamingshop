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

# Add sample products only on an empty database.
if db.execute("SELECT COUNT(*) c FROM products").fetchone()["c"] == 0:
    samples = [
        ("20 UC","UC",20,19,35),("36 UC","UC",36,35,31),
        ("80 UC","UC",80,75,79),("160 UC","UC",160,149,147),
        ("405 UC","UC",405,376,25),("800 UC","UC",800,741,32),
        ("810 UC","UC",810,751,17),("1625 UC","UC",1625,1505,3),
        ("2000 UC","UC",2000,1870,0),("650 Shell","Shell",650,0,0),
        ("1300 Shell","Shell",1300,2075,0)]
    db.executemany("INSERT INTO products(name,category,quantity,price,stock) VALUES(?,?,?,?,?)", samples)
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
    rows = db.execute("SELECT * FROM products WHERE active=1 ORDER BY id").fetchall()
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{p['name']} — {p['price']} {CURRENCY}", callback_data=f"product:{p['id']}")]
        for p in rows])

class Buy(StatesGroup):
    uid = State()

class Pay(StatesGroup):
    amount = State()
    method = State()
    trx = State()

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

@router.message(Command("admin"))
async def admin(m:Message):
    if m.from_user.id not in ADMIN_IDS: return await m.answer("⛔ Access denied.")
    await m.answer("🛠 <b>Admin Panel</b>\nUse the buttons on pending orders/payments sent to this chat.")

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
