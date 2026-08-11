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
import inspect
import queue
import contextlib
import uuid
import traceback
from urllib.parse import urlparse, parse_qs
from pathlib import Path
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from dotenv import load_dotenv
from psycopg import connect, errors
from psycopg.rows import dict_row

from aiogram import Bot, Dispatcher, Router, F
from aiogram import BaseMiddleware
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
    support_note = State()

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
CURRENCY = os.getenv("CURRENCY", "BDT")
PAYMENT_INFO = os.getenv("PAYMENT_INSTRUCTIONS", "bKash/Nagad: YOUR NUMBER")
SUPPORT = os.getenv("SUPPORT_USERNAME", "@YourSupport")
ADMIN_WEB_TOKEN = os.getenv("ADMIN_WEB_TOKEN", "").strip()
FEATURE_EFOOTBALL_COINS = True
APP_VERSION = "V10.20 RELEASE CANDIDATE • FINAL PRODUCTION AUDIT • V10.19 OBSERVABILITY CORE"
AUTO_DB_BACKUP_HOURS = max(0, int(os.getenv("AUTO_DB_BACKUP_HOURS", "24") or "24"))
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", "/tmp/next_level_backups"))
CREDENTIAL_SECRET = (os.getenv("CREDENTIAL_SECRET", "").strip() or TOKEN)
CREDENTIAL_REVEAL_SECONDS = max(15, min(300, int(os.getenv("CREDENTIAL_REVEAL_SECONDS", "60") or "60")))
DB_POOL_MIN = max(0, int(os.getenv("DB_POOL_MIN", "1") or "1"))
DB_POOL_MAX = max(DB_POOL_MIN or 1, int(os.getenv("DB_POOL_MAX", "8") or "8"))
DB_POOL_WAIT_SECONDS = max(1.0, float(os.getenv("DB_POOL_WAIT_SECONDS", "8") or "8"))
RATE_LIMIT_MESSAGES = max(3, int(os.getenv("RATE_LIMIT_MESSAGES", "12") or "12"))
RATE_LIMIT_CALLBACKS = max(5, int(os.getenv("RATE_LIMIT_CALLBACKS", "25") or "25"))
RATE_LIMIT_WINDOW_SECONDS = max(1.0, float(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "4") or "4"))
INSTANCE_ID = (os.getenv("RENDER_INSTANCE_ID") or os.getenv("HOSTNAME") or f"pid-{os.getpid()}")[:120]
LEADER_LOCK_NAME = os.getenv("LEADER_LOCK_NAME", "next_level_gaming_shop:telegram_poller:v10").strip() or "next_level_gaming_shop:telegram_poller:v10"
LEADER_RETRY_SECONDS = max(3.0, float(os.getenv("LEADER_RETRY_SECONDS", "10") or "10"))
LEADER_HEARTBEAT_SECONDS = max(3.0, float(os.getenv("LEADER_HEARTBEAT_SECONDS", "10") or "10"))
WORKER_LEASE_MINUTES = max(1, int(os.getenv("WORKER_LEASE_MINUTES", "5") or "5"))
OPS_ARCHIVE_NOTIFICATION_DAYS = max(7, int(os.getenv("OPS_ARCHIVE_NOTIFICATION_DAYS", "30") or "30"))
OPS_ARCHIVE_LOG_DAYS = max(30, int(os.getenv("OPS_ARCHIVE_LOG_DAYS", "180") or "180"))
OPS_ARCHIVE_BATCH = max(50, min(5000, int(os.getenv("OPS_ARCHIVE_BATCH", "500") or "500")))
ERROR_EVENT_RETENTION_DAYS = max(30, int(os.getenv("ERROR_EVENT_RETENTION_DAYS", "180") or "180"))
RISK_HIGH_VALUE_AMOUNT = max(0.0, float(os.getenv("RISK_HIGH_VALUE_AMOUNT", "5000") or "5000"))
RISK_AGED_PENDING_MINUTES = max(5, int(os.getenv("RISK_AGED_PENDING_MINUTES", "20") or "20"))
RISK_REPEAT_REJECT_COUNT = max(2, int(os.getenv("RISK_REPEAT_REJECT_COUNT", "3") or "3"))
RISK_ALERT_COOLDOWN_SECONDS = max(300, int(os.getenv("RISK_ALERT_COOLDOWN_SECONDS", "1800") or "1800"))
FRAUD_REVIEW_SCORE = max(25, min(100, int(os.getenv("FRAUD_REVIEW_SCORE", "60") or "60")))
FRAUD_VELOCITY_WINDOW_MINUTES = max(5, int(os.getenv("FRAUD_VELOCITY_WINDOW_MINUTES", "30") or "30"))
FRAUD_VELOCITY_COUNT = max(2, int(os.getenv("FRAUD_VELOCITY_COUNT", "3") or "3"))
STARTUP_DB_RETRY_LIMIT = max(1, int(os.getenv("STARTUP_DB_RETRY_LIMIT", "30") or "30"))
BACKUP_KEEP_COUNT = max(1, min(50, int(os.getenv("BACKUP_KEEP_COUNT", "5") or "5")))
OBSERVABILITY_INTERVAL_SECONDS = max(30, int(os.getenv("OBSERVABILITY_INTERVAL_SECONDS", "60") or "60"))
OBSERVABILITY_ALERT_COOLDOWN_SECONDS = max(300, int(os.getenv("OBSERVABILITY_ALERT_COOLDOWN_SECONDS", "1800") or "1800"))
HEALTH_DEGRADED_SCORE = max(40, min(95, int(os.getenv("HEALTH_DEGRADED_SCORE", "75") or "75")))

if not TOKEN:
    raise RuntimeError("BOT_TOKEN missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing. Add a PostgreSQL connection string in Render Environment Variables.")
if not ADMIN_IDS:
    raise RuntimeError("ADMIN_IDS missing")

DB_LOCK = threading.RLock()
BUYER_CHECKOUT_LOCKS = {}

def _buyer_checkout_lock(tg_id):
    """Serialize checkout mutations per Telegram buyer inside the active leader process."""
    lock = BUYER_CHECKOUT_LOCKS.get(int(tg_id))
    if lock is None:
        lock = asyncio.Lock()
        BUYER_CHECKOUT_LOCKS[int(tg_id)] = lock
    return lock


def _new_raw_db_connection(*, autocommit=False):
    return connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=10,
        autocommit=autocommit,
        application_name=f"next_level_gaming_shop:{INSTANCE_ID}",
        options="-c statement_timeout=15000 -c idle_in_transaction_session_timeout=30000",
    )


class _ConnectionLease:
    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn
        self._released = False

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
        except Exception:
            self._pool.release(self._conn, broken=True)
            self._released = True
            raise
        finally:
            if not self._released:
                self._pool.release(self._conn)
                self._released = True
        return False

    def cursor(self, *args, **kwargs):
        return self._conn.cursor(*args, **kwargs)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        if self._released:
            return
        try:
            self._conn.rollback()
        except Exception:
            self._pool.release(self._conn, broken=True)
        else:
            self._pool.release(self._conn)
        self._released = True

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _DBConnectionPool:
    """Small dependency-free PostgreSQL pool compatible with existing db_conn() calls."""
    def __init__(self, min_size=0, max_size=12, wait_seconds=8.0):
        self.min_size = min_size
        self.max_size = max_size
        self.wait_seconds = wait_seconds
        self._idle = queue.LifoQueue(maxsize=max_size)
        self._lock = threading.Lock()
        self._created = 0
        self._closed = False
        self._waits = 0

    def _create(self):
        try:
            conn = _new_raw_db_connection()
        except Exception:
            with self._lock:
                self._created = max(0, self._created - 1)
            raise
        return conn

    def acquire(self):
        deadline = time.monotonic() + self.wait_seconds
        while True:
            if self._closed:
                raise RuntimeError("Database pool is closed")
            try:
                conn = self._idle.get_nowait()
            except queue.Empty:
                create = False
                with self._lock:
                    if self._created < self.max_size:
                        self._created += 1
                        create = True
                if create:
                    conn = self._create()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("Database pool exhausted")
                    with self._lock:
                        self._waits += 1
                    try:
                        conn = self._idle.get(timeout=remaining)
                    except queue.Empty as exc:
                        raise TimeoutError("Database pool exhausted") from exc
            if getattr(conn, "closed", False):
                with self._lock:
                    self._created = max(0, self._created - 1)
                continue
            return _ConnectionLease(self, conn)

    def release(self, conn, broken=False):
        if broken or self._closed or getattr(conn, "closed", False):
            try:
                conn.close()
            except Exception:
                pass
            with self._lock:
                self._created = max(0, self._created - 1)
            return
        try:
            self._idle.put_nowait(conn)
        except queue.Full:
            try:
                conn.close()
            finally:
                with self._lock:
                    self._created = max(0, self._created - 1)

    def prewarm(self):
        target = min(self.min_size, self.max_size)
        leases = []
        try:
            for _ in range(target):
                leases.append(self.acquire())
        finally:
            for lease in leases:
                lease.close()

    def stats(self):
        with self._lock:
            return {
                "created": self._created,
                "idle": self._idle.qsize(),
                "max": self.max_size,
                "waits": self._waits,
            }

    def closeall(self):
        self._closed = True
        while True:
            try:
                conn = self._idle.get_nowait()
            except queue.Empty:
                break
            try:
                conn.close()
            except Exception:
                pass
        with self._lock:
            self._created = 0


DB_POOL = _DBConnectionPool(DB_POOL_MIN, DB_POOL_MAX, DB_POOL_WAIT_SECONDS)


def db_conn():
    return DB_POOL.acquire()


def db_execute(sql, params=(), fetch=None):
    """Short-lived PostgreSQL helper for single-statement operations."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetch == "one":
                return cur.fetchone()
            if fetch == "all":
                return cur.fetchall()
            return cur.rowcount


def db_insert_returning(sql, params=()):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


async def adb_execute(sql, params=(), fetch=None):
    """Run a short synchronous PostgreSQL operation off the asyncio event loop."""
    return await asyncio.to_thread(db_execute, sql, params, fetch)


async def adb_insert_returning(sql, params=()):
    """Async wrapper for db_insert_returning; keeps Telegram handlers responsive under DB wait/load."""
    return await asyncio.to_thread(db_insert_returning, sql, params)


async def aget_user(tg):
    """Async wrapper for user upsert/read used by high-traffic Telegram handlers."""
    return await asyncio.to_thread(get_user, tg)


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
    stock_reserved BOOLEAN NOT NULL DEFAULT FALSE,
    reservation_kind TEXT NOT NULL DEFAULT '',
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

CREATE TABLE IF NOT EXISTS payment_trx_claims(
    normalized_trx_id TEXT PRIMARY KEY,
    payment_id BIGINT UNIQUE NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payment_receipts(
    payment_id BIGINT PRIMARY KEY REFERENCES payments(id) ON DELETE CASCADE,
    file_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payment_support_cases(
    id BIGSERIAL PRIMARY KEY, payment_id BIGINT NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE, status TEXT NOT NULL DEFAULT 'open',
    reason TEXT NOT NULL DEFAULT 'buyer_payment_issue', admin_note TEXT NOT NULL DEFAULT '',
    assigned_admin BIGINT, resolved_at TIMESTAMPTZ, resolved_by BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_payment_support_cases_payment ON payment_support_cases(payment_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_payment_support_cases_status_updated ON payment_support_cases(status,updated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_support_cases_open ON payment_support_cases(payment_id,user_id) WHERE status='open';

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

CREATE TABLE IF NOT EXISTS security_events(
    id BIGSERIAL PRIMARY KEY,
    admin_tg_id BIGINT,
    event_type TEXT NOT NULL,
    order_id BIGINT REFERENCES orders(id) ON DELETE SET NULL,
    details TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_security_events_order_time ON security_events(order_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_security_events_type_time ON security_events(event_type,created_at DESC);

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
    claimed_at TIMESTAMPTZ,
    claimed_by TEXT DEFAULT '',
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
    claimed_at TIMESTAMPTZ,
    claimed_by TEXT DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS marketing_delivery_claims(
    campaign_id BIGINT NOT NULL REFERENCES marketing_campaigns(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(campaign_id, user_id)
);

CREATE TABLE IF NOT EXISTS marketing_events(
    id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT NOT NULL REFERENCES marketing_campaigns(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    order_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS error_events(
    error_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL DEFAULT '',
    scope TEXT NOT NULL,
    error_type TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    context_json TEXT NOT NULL DEFAULT '{}',
    traceback_text TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_error_events_time ON error_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_error_events_scope_time ON error_events(scope,created_at DESC);

CREATE TABLE IF NOT EXISTS order_status_audit(
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT REFERENCES orders(id) ON DELETE SET NULL,
    old_status TEXT NOT NULL DEFAULT '',
    new_status TEXT NOT NULL DEFAULT '',
    payment_mode TEXT NOT NULL DEFAULT '',
    payment_id BIGINT,
    operation TEXT NOT NULL DEFAULT '',
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_status_audit_order_time ON order_status_audit(order_id,changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_order_status_audit_status_time ON order_status_audit(new_status,changed_at DESC);

CREATE TABLE IF NOT EXISTS ops_archive(
    id BIGSERIAL PRIMARY KEY,
    archive_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(archive_type, source_id)
);

CREATE INDEX IF NOT EXISTS idx_ops_archive_type_time ON ops_archive(archive_type,archived_at DESC);

CREATE INDEX IF NOT EXISTS idx_marketing_campaigns_due ON marketing_campaigns(status,starts_at);
CREATE INDEX IF NOT EXISTS idx_marketing_campaigns_claim ON marketing_campaigns(status,claimed_at);
CREATE INDEX IF NOT EXISTS idx_notification_queue_claim ON notification_queue(status,claimed_at,next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_marketing_events_campaign ON marketing_events(campaign_id,event_type,created_at);
CREATE INDEX IF NOT EXISTS idx_marketing_events_user ON marketing_events(user_id,event_type,created_at);
"""


def init_db():
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA)
                # V9.8 credential encryption uses PostgreSQL pgcrypto so no extra
                # Python crypto package is required. Managed PostgreSQL providers
                # normally expose this extension; fail fast rather than store plaintext.
                try:
                    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
                except Exception as exc:
                    raise RuntimeError("V9.8 requires PostgreSQL pgcrypto for encrypted order credentials") from exc
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
                cur.execute("""
                    CREATE OR REPLACE FUNCTION next_level_order_status_audit() RETURNS TRIGGER AS $$
                    DECLARE prior_status TEXT := '';
                    BEGIN
                        IF TG_OP = 'UPDATE' AND NEW.status IS NOT DISTINCT FROM OLD.status THEN RETURN NEW; END IF;
                        IF TG_OP = 'UPDATE' THEN prior_status := COALESCE(OLD.status, ''); END IF;
                        INSERT INTO order_status_audit(order_id,old_status,new_status,payment_mode,payment_id,operation)
                        VALUES(NEW.id,prior_status,COALESCE(NEW.status,''),COALESCE(NEW.payment_mode,''),NEW.payment_id,TG_OP);
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                """)
                cur.execute("DROP TRIGGER IF EXISTS trg_next_level_order_status_audit ON orders")
                cur.execute("CREATE TRIGGER trg_next_level_order_status_audit AFTER INSERT OR UPDATE OF status ON orders FOR EACH ROW EXECUTE FUNCTION next_level_order_status_audit()")
                cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS stock_reserved BOOLEAN NOT NULL DEFAULT FALSE")
                cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS reservation_kind TEXT NOT NULL DEFAULT ''")
                # Security cleanup: credentials are only needed while a manual order is active.
                cur.execute("UPDATE orders SET account_password='' WHERE COALESCE(account_password,'')<>'' AND status IN ('completed','refunded','rejected','expired','cancelled','refund_pending')")
                # Encrypt any active legacy plaintext credential in place.
                cur.execute(
                    "UPDATE orders SET account_password='enc:v1:' || encode(pgp_sym_encrypt(account_password,%s,'cipher-algo=aes256'),'base64') "
                    "WHERE COALESCE(account_password,'')<>'' AND account_password NOT LIKE 'enc:v1:%%' "
                    "AND status IN ('pending','awaiting_payment')",
                    (CREDENTIAL_SECRET,),
                )
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS order_id BIGINT")
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS fraud_score INTEGER NOT NULL DEFAULT 0")
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS fraud_flags TEXT NOT NULL DEFAULT ''")
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS review_required BOOLEAN NOT NULL DEFAULT FALSE")
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS review_cleared_at TIMESTAMPTZ")
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS review_cleared_by BIGINT")
                cur.execute("ALTER TABLE payment_receipts ADD COLUMN IF NOT EXISTS media_type TEXT NOT NULL DEFAULT 'unknown'")
                cur.execute("ALTER TABLE payment_receipts ADD COLUMN IF NOT EXISTS uploader_tg_id BIGINT")
                cur.execute("CREATE TABLE IF NOT EXISTS payment_support_cases(id BIGSERIAL PRIMARY KEY,payment_id BIGINT NOT NULL REFERENCES payments(id) ON DELETE CASCADE,user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,status TEXT NOT NULL DEFAULT 'open',reason TEXT NOT NULL DEFAULT 'buyer_payment_issue',admin_note TEXT NOT NULL DEFAULT '',created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payment_support_cases_payment ON payment_support_cases(payment_id,created_at DESC)")
                cur.execute("ALTER TABLE payment_support_cases ADD COLUMN IF NOT EXISTS assigned_admin BIGINT")
                cur.execute("ALTER TABLE payment_support_cases ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ")
                cur.execute("ALTER TABLE payment_support_cases ADD COLUMN IF NOT EXISTS resolved_by BIGINT")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payment_support_cases_status_updated ON payment_support_cases(status,updated_at DESC)")
                cur.execute("""DELETE FROM payment_support_cases a USING payment_support_cases b WHERE a.status='open' AND b.status='open' AND a.payment_id=b.payment_id AND a.user_id=b.user_id AND a.id>b.id""")
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_support_cases_open ON payment_support_cases(payment_id,user_id) WHERE status='open'")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_review_pending ON payments(review_required,review_cleared_at,created_at) WHERE status='pending'")
                # V9.8 keeps the V9.7 durable normalized TrxID race guard.
                cur.execute("""
                    INSERT INTO payment_trx_claims(normalized_trx_id,payment_id)
                    SELECT lower(regexp_replace(trx_id,'[[:space:]]+','','g')), id
                    FROM payments
                    WHERE regexp_replace(trx_id,'[[:space:]]+','','g') <> ''
                    ORDER BY id
                    ON CONFLICT DO NOTHING
                """)
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS sale_price NUMERIC(14,2)")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS sale_until TIMESTAMPTZ")
                cur.execute("ALTER TABLE notification_queue ADD COLUMN IF NOT EXISTS buttons_json TEXT NOT NULL DEFAULT ''")
                cur.execute("ALTER TABLE notification_queue ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ")
                cur.execute("ALTER TABLE notification_queue ADD COLUMN IF NOT EXISTS claimed_by TEXT NOT NULL DEFAULT ''")
                cur.execute("ALTER TABLE marketing_campaigns ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ")
                cur.execute("ALTER TABLE marketing_campaigns ADD COLUMN IF NOT EXISTS claimed_by TEXT NOT NULL DEFAULT ''")
                cur.execute("CREATE TABLE IF NOT EXISTS marketing_delivery_claims(campaign_id BIGINT NOT NULL REFERENCES marketing_campaigns(id) ON DELETE CASCADE,user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),PRIMARY KEY(campaign_id,user_id))")
                cur.execute("ALTER TABLE cart_items ADD COLUMN IF NOT EXISTS last_reminded_at TIMESTAMPTZ")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_cart_abandoned ON cart_items(updated_at,last_reminded_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_marketing_events_order ON marketing_events(order_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_lifetime_spend ON users(lifetime_spend)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_updated_at ON users(updated_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_created ON orders(user_id,created_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_carts_updated ON cart_items(updated_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)")
                # V9.8 hot-path partial indexes for payment/order workers.
                cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_pending_created ON orders(created_at) WHERE status='pending'")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_awaiting_payment_created ON orders(created_at) WHERE status='awaiting_payment'")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_pending_created ON payments(created_at) WHERE status='pending'")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_codes_reserved_order ON product_codes(order_id) WHERE status='reserved'")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_notification_queue_claim ON notification_queue(status,claimed_at,next_attempt_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_marketing_campaigns_claim ON marketing_campaigns(status,claimed_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_error_events_time ON error_events(created_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_order_status_audit_order_time ON order_status_audit(order_id,changed_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_order_status_audit_status_time ON order_status_audit(new_status,changed_at DESC)")
                cur.execute("ALTER TABLE ops_archive ALTER COLUMN source_id TYPE TEXT USING source_id::text")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ops_archive_type_time ON ops_archive(archive_type,archived_at DESC)")
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



_RUNTIME_STATE_LOCK = threading.Lock()
_RUNTIME_STATE = {
    "role": "starting",
    "leader": False,
    "instance_id": INSTANCE_ID,
    "leader_since": None,
    "last_leader_heartbeat": None,
    "bootstrap_complete": False,
    "deployment_ok": False,
    "health_score": 100,
    "health_status": "starting",
    "health_reasons": [],
    "last_observability_check": None,
    "last_self_heal": None,
    "worker_restarts": {},
}

def runtime_state_update(**values):
    with _RUNTIME_STATE_LOCK:
        _RUNTIME_STATE.update(values)

def runtime_state_snapshot():
    with _RUNTIME_STATE_LOCK:
        return dict(_RUNTIME_STATE)


class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, limit, window_seconds):
        self.limit = int(limit)
        self.window = float(window_seconds)
        self._hits = {}
        self._last_cleanup = time.monotonic()

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if not user:
            return await handler(event, data)
        now = time.monotonic()
        limit = self.limit * (4 if is_admin(user.id) else 1)
        key = int(user.id)
        hits = self._hits.setdefault(key, [])
        cutoff = now - self.window
        while hits and hits[0] < cutoff:
            hits.pop(0)
        if len(hits) >= limit:
            perf_inc("rate_limited")
            if isinstance(event, CallbackQuery):
                try:
                    await event.answer("⏳ Too many requests. Please try again in a moment.", show_alert=False)
                except Exception:
                    pass
            return None
        hits.append(now)
        if now - self._last_cleanup > 60:
            stale_before = now - max(60.0, self.window * 3)
            self._hits = {k:v for k,v in self._hits.items() if v and v[-1] >= stale_before}
            self._last_cleanup = now
        return await handler(event, data)


class ErrorBoundaryMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            return await handler(event, data)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            user = getattr(event, "from_user", None)
            context = {
                "user_id": getattr(user, "id", None),
                "event_type": type(event).__name__,
                "callback": (getattr(event, "data", None) or "")[:120],
            }
            error_id = record_runtime_error("telegram_handler", exc, context)
            try:
                if isinstance(event, CallbackQuery):
                    await event.answer(f"Something went wrong. Ref: {error_id}", show_alert=True)
                elif isinstance(event, Message):
                    await event.answer(f"❌ Something went wrong. Reference: <code>{html.escape(error_id)}</code>")
            except Exception:
                pass
            return None


class SafeCallbackMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        raw = (getattr(event, "data", None) or "")
        invalid = (not raw or len(raw.encode("utf-8")) > 64 or any(ord(ch) < 32 for ch in raw))
        if invalid:
            perf_inc("invalid_callbacks")
            try:
                await event.answer("Invalid or expired action.", show_alert=False)
            except Exception:
                pass
            return None
        return await handler(event, data)


router = Router()
router.message.outer_middleware(ErrorBoundaryMiddleware())
router.callback_query.outer_middleware(ErrorBoundaryMiddleware())
router.message.outer_middleware(RateLimitMiddleware(RATE_LIMIT_MESSAGES, RATE_LIMIT_WINDOW_SECONDS))
router.callback_query.outer_middleware(SafeCallbackMiddleware())
router.callback_query.outer_middleware(RateLimitMiddleware(RATE_LIMIT_CALLBACKS, RATE_LIMIT_WINDOW_SECONDS))

_SETTINGS_CACHE = {}
_SETTINGS_CACHE_LOCK = threading.RLock()

def _load_settings_cache():
    rows = db_execute("SELECT key,value FROM settings", fetch="all") or []
    with _SETTINGS_CACHE_LOCK:
        _SETTINGS_CACHE.clear()
        _SETTINGS_CACHE.update({r["key"]: r["value"] for r in rows})


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


def security_log(event_type, admin_id=None, order_id=None, details=""):
    db_execute(
        "INSERT INTO security_events(admin_tg_id,event_type,order_id,details) VALUES(%s,%s,%s,%s)",
        (admin_id, event_type, order_id, (details or "")[:1000]),
    )


def _new_error_id():
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"ERR-{stamp}-{uuid.uuid4().hex[:8].upper()}"


def record_runtime_error(scope, exc, context=None):
    """Persist a safe operational error reference without exposing secrets to users."""
    error_id = _new_error_id()
    safe_context = context if isinstance(context, dict) else {"context": str(context or "")}
    trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-12000:]
    event = {
        "event": "runtime_error",
        "error_id": error_id,
        "instance_id": INSTANCE_ID,
        "scope": str(scope or "unknown")[:120],
        "error_type": type(exc).__name__,
        "message": str(exc)[:1000],
        "context": safe_context,
    }
    try:
        logging.error(json.dumps(event, ensure_ascii=False, default=str))
    except Exception:
        logging.error("runtime_error %s %s", error_id, type(exc).__name__)
    try:
        db_execute(
            "INSERT INTO error_events(error_id,instance_id,scope,error_type,message,context_json,traceback_text) VALUES(%s,%s,%s,%s,%s,%s,%s)",
            (error_id, INSTANCE_ID, event["scope"], event["error_type"], event["message"], json.dumps(safe_context, ensure_ascii=False, default=str)[:4000], trace),
        )
    except Exception:
        pass
    try:
        if "perf_inc" in globals():
            perf_inc("errors")
            with _PERF_LOCK:
                _PERF["last_error"] = error_id
    except Exception:
        pass
    return error_id


def encrypt_credential_cursor(cur, value):
    value = (value or "").strip()
    if not value:
        return ""
    cur.execute(
        "SELECT encode(pgp_sym_encrypt(%s,%s,'cipher-algo=aes256'),'base64') AS token",
        (value, CREDENTIAL_SECRET),
    )
    row = cur.fetchone()
    if not row or not row.get("token"):
        raise RuntimeError("Credential encryption failed")
    return "enc:v1:" + row["token"]


def decrypt_order_credential(order_id):
    try:
        row = db_execute(
            "SELECT CASE "
            "WHEN account_password LIKE 'enc:v1:%%' THEN pgp_sym_decrypt(decode(substr(account_password,8),'base64'),%s) "
            "ELSE account_password END AS credential "
            "FROM orders WHERE id=%s AND status='pending'",
            (CREDENTIAL_SECRET, order_id),
            "one",
        )
        if not row:
            return None, "Order is not awaiting manual delivery."
        credential = (row.get("credential") or "").strip()
        if not credential:
            return None, "No credential is stored for this order."
        return credential, ""
    except Exception as exc:
        logging.exception("Credential decrypt failed for order #%s: %s", order_id, exc)
        return None, "Credential could not be decrypted. Check CREDENTIAL_SECRET."


async def _delete_sensitive_message_later(bot, chat_id, message_id, delay=None):
    await asyncio.sleep(delay or CREDENTIAL_REVEAL_SECONDS)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


MANAGED_TABLES = (
    "users", "products", "product_codes", "orders", "payments",
    "payment_trx_claims", "payment_receipts", "payment_support_cases", "payment_audit",
    "security_events", "error_events", "order_status_audit", "ops_archive", "balance_logs",
    "admin_logs", "settings", "favorites", "notification_queue", "cart_items", "coupons",
    "coupon_uses", "marketing_campaigns", "marketing_delivery_claims", "marketing_events",
    "product_views", "intelligence_events", "smart_offers"
)
BACKUP_TABLES = MANAGED_TABLES


def create_database_backup():
    """Create an atomic, validated compressed JSON snapshot of all application tables."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    path = BACKUP_DIR / f"next_level_db_backup_{stamp}.json.gz"
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    payload = {"app_version": APP_VERSION, "created_at": now_text(), "tables": {}}
    try:
        with DB_LOCK:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    for table in BACKUP_TABLES:
                        cur.execute(f"SELECT * FROM {table} ORDER BY 1")
                        payload["tables"][table] = cur.fetchall()
        with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, default=str)
        # Validate the artifact before publishing it as a completed backup.
        with gzip.open(tmp_path, "rt", encoding="utf-8") as f:
            verified = json.load(f)
        if set((verified.get("tables") or {}).keys()) != set(BACKUP_TABLES):
            raise RuntimeError("Backup validation failed: table coverage mismatch")
        os.replace(tmp_path, path)
        return path
    except Exception:
        with contextlib.suppress(OSError):
            tmp_path.unlink(missing_ok=True)
        raise


def cleanup_old_backups(keep=None):
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    keep = BACKUP_KEEP_COUNT if keep is None else max(1, int(keep))
    files = sorted(BACKUP_DIR.glob("next_level_db_backup_*.json.gz"), key=lambda x: x.stat().st_mtime, reverse=True)
    for old in files[keep:]:
        try:
            old.unlink()
        except OSError:
            pass


def database_integrity_check():
    """Fail fast if PostgreSQL is unavailable or any managed application table is missing."""
    row = db_execute("SELECT current_database() AS db, current_schema() AS schema", fetch="one")
    existing = db_execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema()",
        fetch="all",
    ) or []
    present = {r["table_name"] for r in existing}
    missing = sorted(set(MANAGED_TABLES) - present)
    if missing:
        raise RuntimeError("Database integrity check failed; missing tables: " + ", ".join(missing))
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
    rows=await asyncio.to_thread(db_execute,"""
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
    return {"pending":"⏳","awaiting_payment":"💳","processing":"🔄","completed":"✅","rejected":"❌","expired":"⌛","cancelled":"🚫","refunded":"↩️","refund_pending":"🕐","credited":"✅"}.get(status,"•")

def buyer_status_text(status):
    return {
        "pending": "Order received — waiting for processing",
        "awaiting_payment": "Payment submitted — verification pending",
        "processing": "Processing your order",
        "completed": "Delivered successfully",
        "rejected": "Order/payment was not approved",
        "expired": "Payment session expired",
        "cancelled": "Order cancelled",
        "refunded": "Refund completed",
        "refund_pending": "Refund is being processed",
        "credited": "Wallet credited",
    }.get(str(status or ""), str(status or "Unknown").replace("_"," ").title())

def buyer_status_progress(status):
    status=str(status or "")
    if status == "completed": return "✅ Order  →  ✅ Payment  →  ✅ Delivered"
    if status in ("processing","pending"): return "✅ Order  →  ✅ Payment  →  🔄 Processing"
    if status == "awaiting_payment": return "✅ Order  →  💳 Verification  →  ⏳ Delivery"
    if status in ("refunded","refund_pending"): return "✅ Order  →  ↩️ Refund  →  " + ("✅ Done" if status=="refunded" else "🕐 Pending")
    if status in ("rejected","expired","cancelled"): return "✅ Order  →  ⚠️ Not completed"
    return "✅ Order received"


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
        "<i>🎮 Premium Gaming Store • ⚡ Fast • 🛡️ Secure • 🎁 Rewarding</i>",
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
        rows.append([InlineKeyboardButton(text="🎮  BUY GAMING PRODUCTS", callback_data="home:shop")])
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
        [InlineKeyboardButton(text="🧠 Intelligence", callback_data="admin:intelligence"), InlineKeyboardButton(text="🛡 Risk Radar", callback_data="admin:risk")],
        [InlineKeyboardButton(text="📈 Sales & Stock", callback_data="admin:sales_stock"), InlineKeyboardButton(text="🧭 Ops Center", callback_data="admin:ops")],
        [InlineKeyboardButton(text="🧪 Deploy Check", callback_data="admin:deploy_check")],
        [InlineKeyboardButton(text=setting("admin_orders", "🧾 Orders"), callback_data="admin:orders"), InlineKeyboardButton(text=setting("admin_payments", "💳 Payments"), callback_data="admin:payments")],
        [InlineKeyboardButton(text="💸 Refund Queue", callback_data="admin:refunds")],
        [InlineKeyboardButton(text="🆘 Support Center", callback_data="admin:support")],
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
            # Liveness must never depend on PostgreSQL or settings-cache availability.
            state = runtime_state_snapshot()
            body = f"{APP_VERSION} alive. role={state.get('role')} instance={INSTANCE_ID}"
            return self._send(body, 200, "text/plain; charset=utf-8")
        if path == "/ready":
            state = runtime_state_snapshot()
            role = str(state.get("role") or "starting")
            ready = bool(state.get("bootstrap_complete")) and bool(state.get("deployment_ok")) and role not in {"starting", "db_unavailable", "stopping", "leader_lost"}
            body = json.dumps({"ready": ready, "role": role, "instance_id": INSTANCE_ID, "app_version": APP_VERSION}, ensure_ascii=False)
            return self._send(body, 200 if ready else 503, "application/json; charset=utf-8")
        if path == "/health/details":
            if not self._authorized():
                return self._send("Unauthorized", 401, "text/plain; charset=utf-8")
            body = json.dumps(performance_health_snapshot(), ensure_ascii=False, default=str)
            return self._send(body, 200, "application/json; charset=utf-8")
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
_HEALTH_SERVER = None

def start_health_server():
    global _HEALTH_SERVER
    port = int(os.getenv("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True, name="health-server").start()
    _HEALTH_SERVER = server
    return server

def stop_health_server():
    global _HEALTH_SERVER
    server = _HEALTH_SERVER
    _HEALTH_SERVER = None
    if server is None:
        return
    try:
        server.shutdown()
        server.server_close()
    except Exception:
        pass


@router.message(CommandStart())
async def start(m: Message):
    args=(m.text or "").split(maxsplit=1)
    was_new = await adb_execute("SELECT id FROM users WHERE tg_id=%s", (m.from_user.id,), "one") is None
    u=await aget_user(m.from_user)
    if was_new and len(args)>1 and args[1].startswith("ref_"):
        try:
            ref_tg=int(args[1][4:])
            if ref_tg != m.from_user.id:
                ref=await adb_execute("SELECT id FROM users WHERE tg_id=%s", (ref_tg,), "one")
                if ref:
                    await adb_execute("UPDATE users SET referred_by=%s WHERE id=%s AND referred_by IS NULL", (ref["id"],u["id"]))
                    u=await aget_user(m.from_user)
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
    total_row=await adb_execute("SELECT COUNT(*) AS c FROM products WHERE active=1 AND (category=%s OR %s='*')",(category,category),"one")
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
    total_row=await adb_execute("SELECT COUNT(*) AS c FROM products WHERE active=1 AND (category=%s OR %s='*')",(category,category),"one")
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

def _queue_buttons_from_markup(reply_markup):
    """Convert an InlineKeyboardMarkup into the durable queue's compact button format."""
    if not isinstance(reply_markup, InlineKeyboardMarkup):
        return []
    rows = []
    for row in reply_markup.inline_keyboard:
        out_row = []
        for button in row:
            # Queue worker supports callback buttons only; never persist unsupported button types silently.
            if getattr(button, "callback_data", None):
                out_row.append([str(button.text), str(button.callback_data)])
        if out_row:
            rows.append(out_row)
    return rows


async def notify_user(bot, tg_id, text, reply_markup=None):
    # V10.9: short retry for transient Telegram failures, then durable queue with callback buttons preserved.
    last_error = ""
    for attempt in range(3):
        try:
            await bot.send_message(tg_id, text, reply_markup=reply_markup)
            return True
        except Exception as exc:
            last_error = str(exc)
            await asyncio.sleep(0.5 * (attempt + 1))
    try:
        enqueue_notification(tg_id, text, _queue_buttons_from_markup(reply_markup))
    except Exception as exc:
        record_runtime_error("notify_user_queue_fallback", exc, {"tg_id": tg_id, "send_error": last_error[:300]})
    return False


def claim_notification_batch(limit=20):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """WITH picked AS (
                       SELECT id FROM notification_queue
                       WHERE (status='pending' AND next_attempt_at<=NOW())
                          OR (status='sending' AND COALESCE(claimed_at,created_at) < NOW()-(%s * INTERVAL '1 minute'))
                       ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED
                   )
                   UPDATE notification_queue q
                   SET status='sending', claimed_at=NOW(), claimed_by=%s
                   FROM picked p WHERE q.id=p.id
                   RETURNING q.id,q.tg_id,q.text,q.buttons_json,q.attempts""",
                (WORKER_LEASE_MINUTES, limit, INSTANCE_ID),
            )
            return cur.fetchall() or []


async def notification_queue_loop(bot):
    while True:
        try:
            rows = await asyncio.to_thread(claim_notification_batch, 20)
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
                    await asyncio.to_thread(
                        db_execute,
                        "UPDATE notification_queue SET status='sent',sent_at=NOW(),claimed_at=NULL,claimed_by='' WHERE id=%s AND status='sending' AND claimed_by=%s",
                        (row["id"], INSTANCE_ID),
                    )
                except Exception as exc:
                    attempts = int(row["attempts"] or 0) + 1
                    if attempts >= 5:
                        await asyncio.to_thread(
                            db_execute,
                            "UPDATE notification_queue SET status='failed',attempts=%s,last_error=%s,claimed_at=NULL,claimed_by='' WHERE id=%s AND claimed_by=%s",
                            (attempts, str(exc)[:500], row["id"], INSTANCE_ID),
                        )
                    else:
                        delay = min(3600, 30 * (2 ** (attempts - 1)))
                        await asyncio.to_thread(
                            db_execute,
                            "UPDATE notification_queue SET status='pending',attempts=%s,next_attempt_at=NOW()+(%s * INTERVAL '1 second'),last_error=%s,claimed_at=NULL,claimed_by='' WHERE id=%s AND claimed_by=%s",
                            (attempts, delay, str(exc)[:500], row["id"], INSTANCE_ID),
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            record_runtime_error("notification_queue_loop",exc,{"instance_id":INSTANCE_ID})
        await asyncio.sleep(5)


def validate_backup_snapshot(path):
    """Validate a completed backup without restoring or mutating production data."""
    path = Path(path)
    if not path.exists() or path.suffix != ".gz":
        raise RuntimeError("Backup snapshot missing or unsupported")
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        payload = json.load(fh)
    tables = payload.get("tables") if isinstance(payload, dict) else None
    if not isinstance(tables, dict):
        raise RuntimeError("Backup snapshot has no table map")
    missing = [name for name in MANAGED_TABLES if name not in tables]
    if missing:
        raise RuntimeError("Backup snapshot missing managed tables: " + ", ".join(missing))
    malformed = [name for name in MANAGED_TABLES if not isinstance(tables.get(name), list)]
    if malformed:
        raise RuntimeError("Backup snapshot contains malformed table payloads: " + ", ".join(malformed))
    return {"ok": True, "path": str(path), "tables": len(tables), "managed_tables": len(MANAGED_TABLES)}


def validate_latest_backup_snapshot():
    """Best-effort startup validation of the newest local completed backup."""
    if not BACKUP_DIR.exists():
        return {"status": "none"}
    files = sorted(BACKUP_DIR.glob("next_level_db_backup_*.json.gz"), key=lambda x: x.stat().st_mtime, reverse=True)
    if not files:
        return {"status": "none"}
    result = validate_backup_snapshot(files[0])
    result["status"] = "valid"
    return result


def startup_reconcile_operations():
    """Repair only deterministic crash leftovers; ambiguous financial states are reported, not guessed."""
    counts = {
        "notification_leases_requeued": 0,
        "orphan_code_holds_released": 0,
        "direct_reservations_released": 0,
        "ambiguous_pending_payments": 0,
    }
    product_ids = set()
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                # A crashed worker may leave queue rows in sending. Requeue only expired leases.
                cur.execute(
                    """UPDATE notification_queue
                       SET status='pending',claimed_at=NULL,claimed_by='',next_attempt_at=LEAST(next_attempt_at,NOW())
                       WHERE status='sending' AND COALESCE(claimed_at,created_at) < NOW()-(%s * INTERVAL '1 minute')""",
                    (WORKER_LEASE_MINUTES,),
                )
                counts["notification_leases_requeued"] = cur.rowcount

                # Reserved codes are valid only for an awaiting-payment direct order.
                cur.execute(
                    """SELECT pc.id,pc.product_id
                       FROM product_codes pc
                       LEFT JOIN orders o ON o.id=pc.order_id
                       WHERE pc.status='reserved'
                         AND (pc.order_id IS NULL OR o.id IS NULL OR o.status<>'awaiting_payment')
                       FOR UPDATE OF pc SKIP LOCKED"""
                )
                orphan_codes = cur.fetchall() or []
                for row in orphan_codes:
                    cur.execute(
                        "UPDATE product_codes SET status='available',sold_to=NULL,order_id=NULL,sold_at=NULL WHERE id=%s AND status='reserved'",
                        (row["id"],),
                    )
                    if cur.rowcount:
                        counts["orphan_code_holds_released"] += 1
                        product_ids.add(int(row["product_id"]))

                # If an awaiting-payment order is linked to a terminal non-success payment,
                # releasing its reservation is deterministic and cannot charge/refund money.
                cur.execute(
                    """SELECT o.*,p.status AS linked_payment_status
                       FROM orders o
                       JOIN payments p ON p.id=o.payment_id
                       WHERE o.status='awaiting_payment' AND o.stock_reserved=TRUE
                         AND p.status IN ('expired','rejected')
                       ORDER BY o.id FOR UPDATE OF o SKIP LOCKED"""
                )
                terminal_orders = cur.fetchall() or []
                for order in terminal_orders:
                    target_status = order.get("linked_payment_status") or "expired"
                    release_direct_order_reservation(cur, order, conn)
                    cur.execute(
                        "UPDATE orders SET status=%s,account_password='',processed_at=COALESCE(processed_at,NOW()),updated_at=NOW() WHERE id=%s AND status='awaiting_payment'",
                        (target_status, order["id"]),
                    )
                    if cur.rowcount:
                        counts["direct_reservations_released"] += 1

                # Pending payment linked to a non-awaiting order is financially ambiguous.
                # Count and alert; never auto-credit/reject during recovery.
                cur.execute(
                    """SELECT COUNT(*) AS c
                       FROM payments p JOIN orders o ON o.id=p.order_id
                       WHERE p.status='pending' AND o.status<>'awaiting_payment'"""
                )
                counts["ambiguous_pending_payments"] = int((cur.fetchone() or {}).get("c") or 0)

            for product_id in product_ids:
                sync_code_product_stock(product_id, conn)
    return counts


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
                        "SELECT id,user_id,product_id,total,payment_mode FROM orders "
                        "WHERE status='pending' AND COALESCE(payment_mode,'wallet')='wallet' "
                        "AND created_at < NOW() - (%s * INTERVAL '1 minute') "
                        "ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED",
                        (order_minutes,),
                    )
                    stale_orders = cur.fetchall() or []
                    for order in stale_orders:
                        cur.execute(
                            "UPDATE orders SET status='expired',refund_amount=total,account_password='',processed_at=NOW(),updated_at=NOW() "
                            "WHERE id=%s AND status='pending' AND COALESCE(payment_mode,'wallet')='wallet'",
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
                    cur.execute("SELECT * FROM payments WHERE status='pending' AND created_at < NOW() - (%s * INTERVAL '1 minute') LIMIT 100 FOR UPDATE SKIP LOCKED",(payment_minutes,))
                    stale_payments=cur.fetchall() or []
                    for pay in stale_payments:
                        expired_order = None
                        if pay.get("order_id"):
                            cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(pay["order_id"],))
                            order=cur.fetchone()
                            if order and order["status"]=="awaiting_payment":
                                release_direct_order_reservation(cur,order,conn)
                                cur.execute("UPDATE orders SET status='expired',account_password='',processed_at=NOW(),updated_at=NOW() WHERE id=%s AND status='awaiting_payment'",(order["id"],))
                                if cur.rowcount == 1:
                                    expired_order = order
                        cur.execute("UPDATE payments SET status='expired',updated_at=NOW() WHERE id=%s AND status='pending'",(pay["id"],))
                        if cur.rowcount:
                            record_payment_audit(cur,pay["id"],None,"expired","pending","expired",pay["amount"],pay["method"],pay["trx_id"],"Payment verification timeout; any pending stock reservation was released")
                            if expired_order:
                                cur.execute("SELECT tg_id FROM users WHERE id=%s", (expired_order["user_id"],))
                                buyer = cur.fetchone()
                                if buyer:
                                    retry_buttons = [[{"text":"🔁 Buy Again","callback_data":f"buy:{expired_order['product_id']}"}], [{"text":"📦 My Orders","callback_data":"home:orders"}]]
                                    cur.execute(
                                        "INSERT INTO notification_queue(tg_id,text,buttons_json) VALUES(%s,%s,%s)",
                                        (buyer["tg_id"], f"⌛ <b>Payment Expired</b>\n\nOrder <b>#{expired_order['id']}</b> was not verified in time. Any reserved stock has been released.\n\nYou can safely start the purchase again.", json.dumps(retry_buttons, ensure_ascii=False)),
                                    )


async def automation_loop():
    await asyncio.sleep(30)
    while True:
        try:
            await asyncio.to_thread(cleanup_expired_transactions)
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
    pid=int(c.data.split(":")[1]); p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    stock=effective_stock(p)
    delivery="Instant Code" if is_auto_code_product(p) else "Manual"
    badge = "🔥 AVAILABLE" if stock > 0 else "⛔ SOLD OUT"
    delivery_line = "⚡ Instant Delivery" if is_auto_code_product(p) else "🛠️ Manual Delivery"
    text=(f"🎮 <b>{html.escape(p['name'])}</b>\n━━━━━━━━━━━━━━━━━━\n"
          f"💰 <b>{float(p['price']):g} {currency()}</b>   •   📦 <b>{stock} available</b>\n"
          f"{delivery_line}   •   🛡️ Secure Checkout\n"
          f"🏷️ {html.escape(p['category'])}\n\n"
          f"{badge}\n\n📝 {html.escape(p['description'] or 'Premium gaming product.')}\n\n"
          f"{'👇 <b>Ready? Tap Buy Now for fast checkout.</b>' if stock > 0 else '🔔 Please check again when stock is available.'}")
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
            # Send the replacement first. If Telegram rejects the photo/caption,
            # keep the original message alive so the text fallback still works.
            await c.message.answer_photo(p["image_file_id"], caption=text, reply_markup=markup)
        except Exception:
            pass
        else:
            try:
                await c.message.delete()
            except Exception:
                pass
            return
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
    text=(f"🎮 <b>{html.escape(p['name'])}</b>\n━━━━━━━━━━━━━━━━━━\n"
          f"💰 <b>{float(p['price']):g} {currency()}</b>   •   📦 <b>{stock} available</b>\n"
          f"{delivery_line}   •   🛡️ Secure Checkout\n"
          f"🏷️ {html.escape(p['category'])}\n\n"
          f"{badge}\n\n📝 {html.escape(p['description'] or 'Premium gaming product.')}\n\n"
          f"{'👇 <b>Ready? Tap Buy Now for fast checkout.</b>' if stock > 0 else '🔔 Please check again when stock is available.'}")
    buttons=[]
    if stock>0: buttons.append([InlineKeyboardButton(text=setting("button_buy","🛒 Buy Now"),callback_data=f"buy:{pid}")])
    fav_label=setting("button_favorite_remove","💔 Remove Favorite") if not exists else setting("button_favorite_add","⭐ Add to Favorites")
    buttons.append([InlineKeyboardButton(text=fav_label,callback_data=f"fav:{pid}")])
    buttons.append([InlineKeyboardButton(text=setting("button_back","⬅️ Back"),callback_data=f"cat:{p['category']}")])
    markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    try:
        if getattr(c.message, "photo", None):
            await c.message.edit_caption(caption=text,reply_markup=markup)
        else:
            await c.message.edit_text(text,reply_markup=markup)
    except Exception:
        pass


@router.message(F.text=="⭐ Favorites")
@router.message(F.text=="❤️ Favorites")
@router.message(Command("favorites"))
async def favorites(m:Message):
    if user_access_denied(m.from_user.id) and not is_admin(m.from_user.id):
        return await m.answer("🔧 Shop is temporarily unavailable. Please try again later.")
    u=await aget_user(m.from_user)
    rows=db_execute("SELECT p.* FROM favorites f JOIN products p ON p.id=f.product_id WHERE f.user_id=%s AND p.active=1 ORDER BY f.created_at DESC LIMIT 30",(u["id"],),"all")
    if not rows: return await m.answer("⭐ <b>Your Favorites</b>\n\nNo saved products yet. Open a product and tap ⭐ Add to Favorites.")
    buttons=[[InlineKeyboardButton(text=f"{'🟢' if effective_stock(p)>0 else '🔴'} {p['name']} • {float(p['price']):g} {currency()}",callback_data=f"product:{p['id']}")] for p in rows]
    await m.answer(f"⭐ <b>Your Favorites</b> ({len(rows)})\n\nTap a product to view or buy.",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("buy:"))
async def buy(c: CallbackQuery,state:FSMContext):
    if maintenance_active() and not is_admin(c.from_user.id):
        return await c.answer("Shop is under maintenance.", show_alert=True)
    pid=int(c.data.split(":")[1])
    p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    u=await aget_user(c.from_user)
    if not p:
        return await c.answer("Product unavailable.",show_alert=True)
    if u["blocked"] and not is_admin(c.from_user.id):
        return await c.answer("Account blocked.",show_alert=True)
    if effective_stock(p)<1:
        return await c.answer("Out of stock.",show_alert=True)
    await state.update_data(
        pid=pid, qty=1, game_uid="", account_password="",
        origin_message_id=c.message.message_id,
        origin_is_photo=bool(getattr(c.message, "photo", None)),
    )
    # V10.4 Fast Checkout: instant-code products do not need a player UID.
    # Send the buyer straight to payment selection; quantity defaults to 1 and
    # can still be changed explicitly from the payment screen.
    if is_auto_code_product(p):
        await state.set_state(Buy.confirm)
        return await order_confirm(c,state)
    await state.set_state(Buy.uid)
    await c.answer()
    await c.message.answer("🆔 <b>Send your game/player UID.</b>\n\nSend /cancel to cancel.")

@router.message(Buy.uid)
async def buy_uid(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear()
        return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."), reply_markup=inline_home_kb())
    uid=(m.text or "").strip()
    if uid.lower()=="/cancel": await state.clear(); return await m.answer("❌ Cancelled.")
    if len(uid)<2 or len(uid)>64: return await m.answer("❌ Please send a valid UID.")
    d=await state.get_data(); p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(d["pid"],),"one"); u=await aget_user(m.from_user)
    if not p or effective_stock(p)<1: await state.clear(); return await m.answer("❌ Product is out of stock.")
    await state.update_data(game_uid=uid, qty=1)
    if not is_auto_code_product(p):
        await state.set_state(Buy.password)
        prompt_msg = await m.answer(
            "🔐 <b>Manual Delivery Product</b>\n\n"
            "🆔 ID / UID received.\n"
            "🔑 Now send the account password required for delivery.\n\n"
            "⚠️ Only send credentials needed to complete this order.\n"
            "Send /cancel to cancel."
        )
        await state.update_data(credential_prompt_message_id=prompt_msg.message_id)
        return
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
    old_prompt_id = d.get("credential_prompt_message_id")
    if old_prompt_id:
        try:
            await m.bot.delete_message(chat_id=m.chat.id, message_id=int(old_prompt_id))
        except Exception:
            pass
    p=db_execute("SELECT * FROM products WHERE id=%s AND active=1",(d["pid"],),"one"); u=await aget_user(m.from_user)
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




@router.callback_query(Buy.confirm,F.data=="order:change_qty")
async def order_change_qty(c:CallbackQuery,state:FSMContext):
    d=await state.get_data()
    p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(int(d["pid"]),),"one")
    if not p or not is_auto_code_product(p):
        return await c.answer("Quantity cannot be changed for this product.",show_alert=True)
    stock=effective_stock(p)
    if stock<1:
        return await c.answer("Out of stock.",show_alert=True)
    qty=max(1,min(10,int(d.get("qty",1)),stock))
    await state.update_data(qty=qty)
    u=await aget_user(c.from_user)
    total=float(p["price"])*qty
    text=(f"📦 <b>Choose Quantity</b>\n\n🎮 Product: <b>{html.escape(p['name'])}</b>\n"
          f"💰 Unit Price: <b>{fmt_money(p['price'])}</b>\n📦 Quantity: <b>{qty}</b>\n"
          f"⭐ Total: <b>{fmt_money(total)}</b>\n\n👛 Wallet Balance: <b>{fmt_money(u['balance'])}</b>\n\n"
          "Use − / +, then continue to payment.")
    markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➖",callback_data="order:qty:-1"),InlineKeyboardButton(text=f"📦 {qty}",callback_data="order:noop"),InlineKeyboardButton(text="➕",callback_data="order:qty:1")],
        [InlineKeyboardButton(text="💳 Continue to Payment",callback_data="order:confirm")],
        [InlineKeyboardButton(text="❌ Cancel",callback_data="order:cancel"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")],
    ])
    await c.answer()
    try:
        await c.message.edit_text(text,reply_markup=markup)
    except Exception:
        try:
            await c.message.edit_caption(caption=text,reply_markup=markup)
        except Exception:
            await c.message.answer(text,reply_markup=markup)


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
    d=await state.get_data(); pid=int(d["pid"]); p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one"); u=await aget_user(c.from_user)
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
    payment_rows=[
        [InlineKeyboardButton(text=wallet_label,callback_data="order:pay_wallet")],
        [InlineKeyboardButton(text="🌐 Pay Directly",callback_data="order:pay_direct")],
    ]
    if is_auto_code_product(p):
        payment_rows.append([InlineKeyboardButton(text=f"📦 Change Quantity • {qty}",callback_data="order:change_qty")])
    payment_rows.append([InlineKeyboardButton(text="⬅️ Back",callback_data="order:cancel"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")])
    kb=InlineKeyboardMarkup(inline_keyboard=payment_rows)
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
    # V10.1 safety guard: direct-payment fulfillment currently reserves one order/code.
    # Block multi-quantity before the buyer is shown an amount or asked to send money.
    if qty != 1:
        return await c.answer("Direct Payment currently supports 1 item per transaction. Please set quantity to 1 or use Wallet.", show_alert=True)
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
    # V10.5: serialize wallet checkout per buyer so rapid double taps cannot create duplicate purchases.
    async with _buyer_checkout_lock(c.from_user.id):
        d=await state.get_data()
        if not d.get("pid"):
            return await c.answer("This checkout was already processed or expired.", show_alert=True)
        pid=int(d["pid"]); p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one"); u=await aget_user(c.from_user)
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
                    stored_account_password = encrypt_credential_cursor(cur, d.get("account_password", "")) if p["delivery_type"] != "code" else ""
                    for _ in range(qty):
                        delivered_code=None; auto_code=False
                        cur.execute("SELECT * FROM product_codes WHERE product_id=%s AND status='available' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",(p["id"],)); code_row=cur.fetchone()
                        auto_code=(p["delivery_type"]=="code")
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
                        cur.execute("INSERT INTO orders(user_id,product_id,game_uid,account_password,total,delivered_code,status,payment_mode) VALUES(%s,%s,%s,%s,%s,%s,%s,'wallet') RETURNING id",(u["id"],p["id"],d.get("game_uid",""),stored_account_password if not auto_code else "",unit_price,delivered_code,status)); oid=cur.fetchone()["id"]; order_ids.append(oid)
                        if delivered_code:
                            cur.execute("UPDATE product_codes SET order_id=%s WHERE id=%s",(oid,code_row["id"])); delivered.append((oid,p["name"],delivered_code,unit_price))
                        else: pending.append((oid,p["name"],unit_price))
                        cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(u["id"],-unit_price,"purchase",f"Order #{oid}"))
                        if status=="completed": award_completed_order_rewards(cur,oid,u["id"],unit_price)
                    sync_code_product_stock(p["id"],conn)
        except Exception as exc:
            error_id = record_runtime_error("wallet_order_transaction", exc, {"user_id": c.from_user.id, "product_id": p.get("id") if p else None, "quantity": qty})
            return await c.answer(f"Order failed safely. Nothing was charged. Ref: {error_id}", show_alert=True)
    await _send_order_result(c,p,u,qty,order_ids,delivered,pending,total=sum(float(x[3]) for x in delivered)+sum(float(x[2]) for x in pending),payment_label="Wallet")


async def _send_order_result(c,p,u,qty,order_ids,delivered,pending,total,payment_label="Wallet"):
    await c.answer("✅ Payment successful")
    msg=["🎉 <b>ORDER CONFIRMED</b>","━━━━━━━━━━━━━━━━━━",f"🎮 <b>{html.escape(p['name'])}</b>",f"📦 Quantity: <b>{qty}</b>   •   💰 <b>{fmt_money(total)}</b>",f"💳 {html.escape(payment_label)}   •   🧾 {len(order_ids)} order(s)","", "✅ Payment secured successfully"]
    if delivered: msg.append("\n🎁 <b>Instant Delivery</b>\n"+"\n".join(f"#{o} • <code>{code}</code>" for o,n,code,a in delivered))
    if pending: msg.append("\n⏳ <b>Manual Delivery</b>\n"+"\n".join(f"#{o} • {fmt_money(a)}" for o,n,a in pending))
    await c.message.answer("\n".join(msg),reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 View My Orders",callback_data="home:orders")],[InlineKeyboardButton(text="🛒 Buy More",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]]))
    if pending:
        for oid in order_ids:
            if not any(x[0] == oid for x in pending):
                continue
            for admin_id in ADMIN_IDS:
                try:
                    order_row=db_execute("SELECT game_uid,CASE WHEN COALESCE(account_password,'')<>'' THEN 1 ELSE 0 END AS has_credential FROM orders WHERE id=%s",(oid,),"one")
                    game_uid=(order_row["game_uid"] if order_row else "") or ""
                    credential_line = "🔐 Credential: <b>Encrypted — reveal only when needed</b>" if order_row and order_row.get("has_credential") else "🔐 Credential: <b>Not required</b>"
                    admin_text=f"🧾 <b>New Manual Order #{oid}</b>\n\n👤 User: <code>{u['tg_id']}</code>\n🎮 Product: {html.escape(p['name'])}\n📦 Qty: 1\n💰 Total: {fmt_money(next((x[2] for x in pending if x[0]==oid), total))}\n💳 Paid via: <b>{html.escape(payment_label)}</b>\n🆔 ID / UID: <code>{html.escape(str(game_uid))}</code>\n{credential_line}\n\n⏳ <b>Manual delivery required.</b>"
                    action_rows=[]
                    if order_row and order_row.get("has_credential"):
                        action_rows.append([InlineKeyboardButton(text="🔐 Reveal Credential",callback_data=f"order_credential:{oid}")])
                    action_rows += [[InlineKeyboardButton(text="✍️ Write Delivery",callback_data=f"order_note:{oid}")],[InlineKeyboardButton(text="❌ Reject + Refund",callback_data=f"order_reject:{oid}")]]
                    admin_kb=InlineKeyboardMarkup(inline_keyboard=action_rows)
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
    amount_ok, amount_error = direct_order_amount_ok(amount)
    if not amount_ok: return await c.answer(amount_error,show_alert=True)
    await state.update_data(direct_method=method)
    await state.set_state(DirectPaymentState.trx)
    label, icon = dict((code,(label,icon)) for code,label,icon in payment_method_specs()).get(
        method, (method.title(), "💳")
    )
    account = payment_method_account(method) or "Not configured"
    instruction = payment_method_instruction(method) or "Follow the shop instructions."
    extra = f"\n🌐 <b>Network:</b> {html.escape(setting('payment_binance_network',''))}" if method=="binance" else ""
    payment_text = (
        f"{icon} <b>{html.escape(label)} Payment</b>\n\n"
        f"💰 <b>Exact Amount:</b> {fmt_money(amount)}\n"
        f"💳 <b>Account/Wallet:</b> <code>{html.escape(account)}</code>{extra}\n\n"
        f"📝 {html.escape(instruction)}\n\n"
        "⚠️ <b>Important:</b> Send the exact amount first.\n"
        "Then tap <b>I've Paid — Enter TrxID</b> and send <b>only your Transaction ID</b>.\n"
        "❌ Send /cancel anytime to cancel."
    )
    await c.answer()
    await c.message.edit_text(
        payment_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ I've Paid — Enter TrxID", callback_data="orderpay:enter_trx")],
            [InlineKeyboardButton(text="❌ Cancel", callback_data="orderpay:cancel")]
        ])
    )

@router.callback_query(DirectPaymentState.trx,F.data=="orderpay:enter_trx")
async def direct_payment_enter_trx(c:CallbackQuery,state:FSMContext):
    d = await state.get_data()
    method = d.get("direct_method", "payment")
    label = dict((code,(label,icon)) for code,label,icon in payment_method_specs()).get(
        method, (method.title(), "💳")
    )[0]
    await c.answer()
    await c.message.edit_text(
        f"🧾 <b>Enter Transaction ID</b>\n\n"
        f"💳 Method: <b>{html.escape(label)}</b>\n"
        f"💰 Amount: <b>{fmt_money(d.get('direct_amount', 0))}</b>\n\n"
        "Send <b>only the Transaction ID / TrxID</b> now.\n"
        "Do not send the amount, password, or other text here.\n\n"
        "Send /cancel to cancel.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="orderpay:cancel")]
        ])
    )


@router.callback_query(DirectPaymentState.method,F.data=="order:confirm")
async def direct_payment_back(c:CallbackQuery,state:FSMContext):
    await state.set_state(Buy.confirm)
    return await order_confirm(c,state)

@router.callback_query(DirectPaymentState.trx,F.data=="orderpay:cancel")
@router.callback_query(DirectPaymentState.receipt,F.data=="orderpay:cancel")
async def direct_payment_cancel(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); payid=d.get("direct_payment_id"); oid=d.get("direct_order_id")
    try:
        if payid or oid:
            with DB_LOCK:
                with db_conn() as conn:
                    with conn.cursor() as cur:
                        payment=None
                        if payid:
                            # Lock ordering matches approve/reject/timeout: payment -> order -> stock.
                            cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(payid,))
                            payment=cur.fetchone()
                            if not payment or payment["status"]!="pending":
                                raise ValueError("already_processed")
                        if oid:
                            cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,))
                            order=cur.fetchone()
                            if not order or order["status"]!="awaiting_payment":
                                raise ValueError("already_processed")
                            release_direct_order_reservation(cur, order, conn)
                            cur.execute("UPDATE orders SET status='cancelled',account_password='',processed_at=NOW(),updated_at=NOW() WHERE id=%s AND status='awaiting_payment'",(oid,))
                        if payment:
                            cur.execute("UPDATE payments SET status='cancelled',updated_at=NOW() WHERE id=%s AND status='pending'",(payid,))
                            if cur.rowcount != 1:
                                raise RuntimeError("Payment status changed during cancellation.")
                            record_payment_audit(cur,payid,None,"cancelled","pending","cancelled",payment["amount"],payment["method"],payment["trx_id"],"Buyer cancelled direct payment before verification")
    except ValueError as exc:
        if str(exc)=="already_processed":
            await state.clear()
            return await c.answer("This payment/order was already processed.",show_alert=True)
        raise
    except Exception as exc:
        error_id=record_runtime_error("direct_payment_cancel",exc,{"user_id":c.from_user.id,"payment_id":payid,"order_id":oid})
        return await c.answer(f"Could not cancel safely. Ref: {error_id}",show_alert=True)
    await state.clear()
    await c.answer("Cancelled")
    await c.message.edit_text("❌ Direct payment cancelled. Any reserved stock was released.",reply_markup=inline_home_kb())

@router.message(DirectPaymentState.trx)
async def direct_payment_trx(m:Message,state:FSMContext):
    trx=(m.text or "").strip(); d=await state.get_data()
    if trx.lower()=="/cancel":
        await state.clear(); return await m.answer("❌ Cancelled.")
    if len(trx)<3 or len(trx)>255:
        return await m.answer("❌ Please send a valid transaction ID.")
    method=(d.get("direct_method") or "").strip().lower()
    if not method or not payment_method_enabled(method):
        await state.clear(); return await m.answer("❌ Payment method is no longer available. Please restart checkout.")
    try:
        pid=int(d["pid"]); qty=int(d.get("direct_qty",1)); amount=float(d.get("direct_amount",0))
    except Exception:
        await state.clear(); return await m.answer("❌ Payment session is invalid. Please restart checkout.")
    if qty != 1:
        await state.clear(); return await m.answer("❌ Direct payment session is invalid. Please restart checkout.")
    normalized=normalize_trx_id(trx)
    if len(normalized)<3:
        return await m.answer("❌ Please send a valid transaction ID.")
    try:
        with DB_LOCK:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT payment_id FROM payment_trx_claims WHERE normalized_trx_id=%s",(normalized,))
                    if cur.fetchone():
                        raise ValueError("duplicate_trx")
                    cur.execute("SELECT * FROM users WHERE tg_id=%s FOR UPDATE",(m.from_user.id,))
                    user=cur.fetchone()
                    cur.execute("SELECT * FROM products WHERE id=%s AND active=1 FOR UPDATE",(pid,))
                    p=cur.fetchone()
                    if not user or not p:
                        raise ValueError("product_unavailable")
                    current_amount=float(p["price"])*qty
                    amount_ok,_=direct_order_amount_ok(current_amount)
                    if not amount_ok:
                        raise ValueError("invalid_amount")
                    if abs(current_amount-amount)>0.009:
                        raise ValueError(f"price_changed:{current_amount}")
                    code_row=None
                    reservation_kind="manual"
                    if is_auto_code_product(p):
                        reservation_kind="code"
                        cur.execute("SELECT * FROM product_codes WHERE product_id=%s AND status='available' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",(pid,))
                        code_row=cur.fetchone()
                        if not code_row:
                            raise ValueError("out_of_stock")
                    elif int(p.get("stock") or 0) < 1:
                        raise ValueError("out_of_stock")
                    stored_account_password = encrypt_credential_cursor(cur, d.get("account_password", "")) if reservation_kind=="manual" else ""
                    cur.execute(
                        "INSERT INTO orders(user_id,product_id,game_uid,account_password,total,status,payment_mode,stock_reserved,reservation_kind) "
                        "VALUES(%s,%s,%s,%s,%s,'awaiting_payment','direct',TRUE,%s) RETURNING id",
                        (user["id"],pid,d.get("game_uid",""),stored_account_password,current_amount,reservation_kind),
                    )
                    oid=cur.fetchone()["id"]
                    if reservation_kind=="code":
                        cur.execute(
                            "UPDATE product_codes SET status='reserved',sold_to=%s,order_id=%s,sold_at=NULL WHERE id=%s AND status='available'",
                            (user["id"],oid,code_row["id"]),
                        )
                        if cur.rowcount != 1:
                            raise RuntimeError("Code reservation changed. Please retry.")
                        sync_code_product_stock(pid,conn)
                    else:
                        cur.execute("UPDATE products SET stock=stock-1,updated_at=NOW() WHERE id=%s AND stock>0",(pid,))
                        if cur.rowcount != 1:
                            raise ValueError("out_of_stock")
                    cur.execute(
                        "INSERT INTO payments(user_id,amount,method,trx_id,status,order_id) VALUES(%s,%s,%s,%s,'pending',%s) RETURNING id",
                        (user["id"],current_amount,method,trx,oid),
                    )
                    payid=cur.fetchone()["id"]
                    assess_payment_fraud(cur,payid,user["id"],current_amount,method,trx)
                    cur.execute("INSERT INTO payment_trx_claims(normalized_trx_id,payment_id) VALUES(%s,%s)",(normalized,payid))
                    cur.execute("UPDATE orders SET payment_id=%s WHERE id=%s",(payid,oid))
                    record_payment_audit(cur,payid,None,"submitted","","pending",current_amount,method,trx,f"Direct payment for Order #{oid}; stock reserved")
                    amount=current_amount
    except errors.UniqueViolation:
        logging.warning("Duplicate normalized TrxID rejected for user %s", m.from_user.id)
        return await m.answer("❌ This transaction ID has already been submitted.")
    except ValueError as exc:
        reason=str(exc)
        if reason=="duplicate_trx":
            return await m.answer("❌ This transaction ID has already been submitted.")
        if reason=="product_unavailable":
            await state.clear(); return await m.answer("❌ Product unavailable. Please restart checkout.")
        if reason=="out_of_stock":
            await state.clear(); return await m.answer("❌ Product is out of stock. No order was created.")
        if reason=="invalid_amount":
            await state.clear(); return await m.answer("❌ Direct payment amount is outside the configured limits. Please restart checkout or contact support.")
        if reason.startswith("price_changed:"):
            await state.clear()
            try: new_amount=float(reason.split(":",1)[1])
            except Exception: new_amount=0
            return await m.answer(f"⚠️ Product price changed to <b>{fmt_money(new_amount)}</b>. No order was created. Please restart checkout and review the new price.")
        error_id=record_runtime_error("direct_payment_validation",exc,{"user_id":m.from_user.id,"product_id":pid})
        return await m.answer(f"❌ Could not create the payment request. Ref: <code>{html.escape(error_id)}</code>")
    except Exception as exc:
        error_id=record_runtime_error("direct_payment_transaction",exc,{"user_id":m.from_user.id,"product_id":pid,"method":method})
        return await m.answer(f"❌ Could not create the payment request safely. Nothing was reserved or charged. Ref: <code>{html.escape(error_id)}</code>")
    await state.update_data(direct_order_id=oid,direct_payment_id=payid,direct_trx=trx)
    await state.set_state(DirectPaymentState.receipt)
    await m.answer(
        f"📸 <b>Payment Receipt</b>\n\nOrder: <b>#{oid}</b>\nPayment: <b>#{payid}</b>\n"
        f"💰 Amount: <b>{fmt_money(amount)}</b>\n📦 Stock: <b>Reserved pending verification</b>\n\n"
        "Send a screenshot/photo if available, or use /skip.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭️ Skip Screenshot",callback_data=f"orderpay:skip:{payid}"),InlineKeyboardButton(text="❌ Cancel",callback_data="orderpay:cancel")]])
    )

@router.callback_query(DirectPaymentState.receipt,F.data.startswith("orderpay:skip:"))
async def direct_payment_skip(c:CallbackQuery,state:FSMContext):
    return await _finish_direct_payment(c.message,state,False)

@router.message(DirectPaymentState.receipt,F.photo)
@router.message(DirectPaymentState.receipt,F.document)
async def direct_payment_receipt(m:Message,state:FSMContext):
    d=await state.get_data(); payid=d.get("direct_payment_id")
    if not payid: return await m.answer("❌ Payment session expired.")
    file_id=m.photo[-1].file_id if m.photo else m.document.file_id
    await adb_execute("INSERT INTO payment_receipts(payment_id,file_id,media_type,uploader_tg_id) VALUES(%s,%s,%s,%s) ON CONFLICT(payment_id) DO UPDATE SET file_id=EXCLUDED.file_id,media_type=EXCLUDED.media_type,uploader_tg_id=EXCLUDED.uploader_tg_id,created_at=NOW()",(payid,file_id,"photo" if m.photo else "document",m.from_user.id))
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
    u=await aget_user(m.from_user)
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
        "🎯 User segmentation + campaign conversion tracking enabled\n"
        "🧭 V10 Ops Center + error reference IDs + DB order-status audit + safe archival enabled"
    )

@router.message(Command("profile"))
@router.message(F.text=="👤 Profile")
@router.message(F.text=="👤 My Account")
async def profile(m:Message):
    u=await aget_user(m.from_user); row=db_execute("SELECT COUNT(*) AS c FROM orders WHERE user_id=%s",(u["id"],),"one")
    points=int(u.get("loyalty_points") or 0)
    tier,pct,next_points=vip_progress(points)
    bar="█"*max(0,pct//10)+"░"*(10-max(0,pct//10))
    await m.answer(f"👤 <b>My Premium Account</b>\n\n🆔 ID: <code>{u['tg_id']}</code>\n💳 Wallet: <b>{fmt_money(u['balance'])}</b>\n🧾 Orders: <b>{row['c']}</b>\n⭐ Points: <b>{points}</b>\n🏅 VIP: <b>{tier}</b>\n📈 {bar} {pct}%\n💰 Lifetime spend: <b>{fmt_money(u.get('lifetime_spend') or 0)}</b>\n📅 Member since: <code>{u['created_at']}</code>")

@router.message(Command("orders"))
@router.message(F.text=="📦 My Orders")
@router.message(F.text=="🧾 My Orders")
async def my_orders(m:Message):
    u=await aget_user(m.from_user)
    rows=await adb_execute("SELECT o.id,o.total,o.status,o.created_at,p.name FROM orders o JOIN products p ON p.id=o.product_id WHERE o.user_id=%s ORDER BY o.id DESC LIMIT 10",(u["id"],),"all")
    if not rows:
        return await m.answer("📦 <b>No orders yet</b>\n\n🎮 Find your first gaming product in the shop.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎮 Shop Now",callback_data="home:shop")]]))
    lines=["📦 <b>MY ORDERS</b>","━━━━━━━━━━━━━━━━━━","Tap an order to see full details.\n"]
    buttons=[]
    for r in rows:
        status=buyer_status_text(r['status'])
        lines.append(f"<b>#{r['id']}</b> • {html.escape(r['name'])}\n{status_emoji(r['status'])} {html.escape(status)} • <b>{fmt_money(r['total'])}</b>\n")
        buttons.append([InlineKeyboardButton(text=f"{status_emoji(r['status'])} Order #{r['id']} • {str(r['status']).replace('_',' ').title()}",callback_data=f"order_detail:{r['id']}")])
    buttons.append([InlineKeyboardButton(text="🎮 Continue Shopping",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Home",callback_data="main_menu")])
    await m.answer("\n".join(lines),reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("order_detail:"))
async def order_detail(c:CallbackQuery):
    if user_blocked(c.from_user.id) and not is_admin(c.from_user.id):
        return await c.answer("Account blocked.",show_alert=True)
    oid=int(c.data.split(":",1)[1]); u=await aget_user(c.from_user)
    o=await adb_execute("SELECT o.*,p.id AS product_id,p.name,p.category,p.delivery_type FROM orders o JOIN products p ON p.id=o.product_id WHERE o.id=%s AND o.user_id=%s",(oid,u["id"]),"one")
    if not o: return await c.answer("Order not found.",show_alert=True)
    status=buyer_status_text(o['status'])
    text=(f"🧾 <b>ORDER #{o['id']}</b>\n━━━━━━━━━━━━━━━━━━\n"
          f"{status_emoji(o['status'])} <b>{html.escape(status)}</b>\n"
          f"{buyer_status_progress(o['status'])}\n\n"
          f"🎮 <b>{html.escape(o['name'])}</b>\n"
          f"🏷️ {html.escape(o['category'])}\n"
          f"💰 Total: <b>{fmt_money(o['total'])}</b>\n"
          f"🆔 UID: <code>{html.escape(o['game_uid'] or 'Not required')}</code>\n"
          f"🕒 Created: <code>{o['created_at']}</code>")
    if o['delivered_code']:
        text += f"\n\n🎁 <b>YOUR CODE</b>\n<code>{html.escape(o['delivered_code'])}</code>\n✅ Save this code safely."
    if o['admin_note']:
        text += f"\n\n📝 <b>Delivery note</b>\n{html.escape(o['admin_note'])}"
    payment=None
    if o.get("payment_id"):
        payment=await adb_execute("SELECT id,status FROM payments WHERE id=%s AND user_id=%s",(o["payment_id"],u["id"]),"one")
        if payment: text += f"\n\n🛡 <b>PAYMENT TRUST TIMELINE</b>\n{buyer_payment_timeline(payment['status'],o['status'])}"
    rows=[]
    if payment: rows.append([InlineKeyboardButton(text="💳 Payment Status",callback_data=f"buyer_payment:{payment['id']}")])
    if o['status'] in ('completed','rejected','refunded','expired','cancelled'):
        rows.append([InlineKeyboardButton(text="🔁 Buy Again",callback_data=f"buy:{o['product_id']}")])
    rows.append([InlineKeyboardButton(text=setting("inline_my_orders", "⬅️ My Orders"),callback_data="my_orders_back"),InlineKeyboardButton(text="🎮 Shop",callback_data="home:shop")])
    rows.append([InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")])
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@router.callback_query(F.data=="my_orders_back")
async def my_orders_back(c:CallbackQuery):
    u=await aget_user(c.from_user); rows=await adb_execute("SELECT o.id,o.total,o.status,o.created_at,p.name FROM orders o JOIN products p ON p.id=o.product_id WHERE o.user_id=%s ORDER BY o.id DESC LIMIT 10",(u["id"],),"all")
    if not rows: return await c.answer("No orders.",show_alert=True)
    buttons=[[InlineKeyboardButton(text=f"🧾 Order #{r['id']}",callback_data=f"order_detail:{r['id']}")] for r in rows]
    buttons.append([InlineKeyboardButton(text="🎮 Continue Shopping",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Home",callback_data="main_menu")]); await c.answer(); await c.message.edit_text("📦 <b>MY ORDERS</b>\n━━━━━━━━━━━━━━━━━━\nSelect an order for details.",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


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

def operational_archive_cleanup():
    """Archive only terminal operational rows; never delete financial/order/payment history."""
    counts = {"notification_queue": 0, "admin_logs": 0, "security_events": 0, "error_events": 0}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                WITH doomed AS (
                    SELECT id FROM notification_queue
                    WHERE status IN ('sent','failed') AND created_at < NOW()-(%s * INTERVAL '1 day')
                    ORDER BY id LIMIT %s
                ), moved AS (
                    DELETE FROM notification_queue q USING doomed d WHERE q.id=d.id RETURNING q.*
                )
                INSERT INTO ops_archive(archive_type,source_id,payload)
                SELECT 'notification_queue',m.id::text,to_jsonb(m) FROM moved m
                ON CONFLICT(archive_type,source_id) DO NOTHING
                RETURNING 1
            """, (OPS_ARCHIVE_NOTIFICATION_DAYS, OPS_ARCHIVE_BATCH))
            counts["notification_queue"] = len(cur.fetchall() or [])

            for table, archive_type, days, pk in (
                ("admin_logs", "admin_logs", OPS_ARCHIVE_LOG_DAYS, "id"),
                ("security_events", "security_events", OPS_ARCHIVE_LOG_DAYS, "id"),
                ("error_events", "error_events", ERROR_EVENT_RETENTION_DAYS, "error_id"),
            ):
                # table/pk values are hard-coded above, never user controlled.
                cur.execute(f"""
                    WITH doomed AS (
                        SELECT {pk} AS source_key FROM {table}
                        WHERE created_at < NOW()-(%s * INTERVAL '1 day')
                        ORDER BY {pk} LIMIT %s
                    ), moved AS (
                        DELETE FROM {table} q USING doomed d WHERE q.{pk}=d.source_key RETURNING q.*
                    )
                    INSERT INTO ops_archive(archive_type,source_id,payload)
                    SELECT %s,(to_jsonb(m)->>%s),to_jsonb(m) FROM moved m
                    ON CONFLICT(archive_type,source_id) DO NOTHING
                    RETURNING 1
                """, (days, OPS_ARCHIVE_BATCH, archive_type, pk))
                counts[archive_type] = len(cur.fetchall() or [])
    return counts


def deployment_self_check():
    """Return non-secret production readiness checks for startup and admin diagnostics."""
    checks = []
    def add(name, ok, detail, severity="critical"):
        checks.append({"name": name, "ok": bool(ok), "detail": str(detail), "severity": severity})

    add("BOT_TOKEN", bool(TOKEN), "configured" if TOKEN else "missing")
    add("DATABASE_URL", bool(DATABASE_URL), "configured" if DATABASE_URL else "missing")
    add("ADMIN_IDS", bool(ADMIN_IDS), f"{len(ADMIN_IDS)} admin(s) configured" if ADMIN_IDS else "missing")
    add("Dedicated credential secret", bool(os.getenv("CREDENTIAL_SECRET", "").strip()), "CREDENTIAL_SECRET configured" if os.getenv("CREDENTIAL_SECRET", "").strip() else "fallback to BOT_TOKEN; set CREDENTIAL_SECRET", "warning")
    add("Web admin token", bool(ADMIN_WEB_TOKEN), "configured" if ADMIN_WEB_TOKEN else "not configured; /health/details and web admin remain protected/unavailable", "warning")
    add("DB pool", DB_POOL_MAX >= 2 and DB_POOL_MAX >= DB_POOL_MIN, f"min={DB_POOL_MIN} max={DB_POOL_MAX}", "warning" if DB_POOL_MAX < 2 else "critical")
    add("Leader lock name", bool(LEADER_LOCK_NAME), LEADER_LOCK_NAME)
    add("Leader lock version", "v10" in LEADER_LOCK_NAME.lower(), "V10 lock namespace" if "v10" in LEADER_LOCK_NAME.lower() else "custom/legacy lock name; ensure every instance uses the same V10 value", "warning")
    add("Startup DB retry budget", STARTUP_DB_RETRY_LIMIT >= 3, f"{STARTUP_DB_RETRY_LIMIT} attempt(s)", "warning")
    try:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        probe = BACKUP_DIR / f".write_probe_{os.getpid()}"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        add("Backup directory", True, f"writable: {BACKUP_DIR}")
    except Exception as exc:
        add("Backup directory", False, f"not writable: {type(exc).__name__}")
    add("Automatic backups", AUTO_DB_BACKUP_HOURS > 0, f"every {AUTO_DB_BACKUP_HOURS}h; retain {BACKUP_KEEP_COUNT}" if AUTO_DB_BACKUP_HOURS > 0 else "disabled", "warning")

    try:
        ext = db_execute("SELECT extversion FROM pg_extension WHERE extname='pgcrypto'", fetch="one")
        add("pgcrypto", bool(ext), f"installed {ext['extversion']}" if ext else "missing")
    except Exception as exc:
        add("pgcrypto", False, f"check failed: {type(exc).__name__}")

    try:
        trigger = db_execute("SELECT 1 AS ok FROM pg_trigger WHERE tgname='trg_next_level_order_status_audit' AND NOT tgisinternal", fetch="one")
        add("Order audit trigger", bool(trigger), "installed" if trigger else "missing")
    except Exception as exc:
        add("Order audit trigger", False, f"check failed: {type(exc).__name__}")

    try:
        db_execute("SELECT 1", fetch="one")
        add("Database connectivity", True, "query OK")
    except Exception as exc:
        add("Database connectivity", False, type(exc).__name__)

    try:
        placeholders = []
        for code, label, _ in payment_method_specs():
            if not payment_method_enabled(code):
                continue
            account = (payment_method_account(code) or "").strip().upper()
            if not account or "XXXX" in account or account.startswith("YOUR_") or account == "NOT CONFIGURED":
                placeholders.append(label)
        add("Payment accounts", not placeholders, "configured" if not placeholders else "placeholder/missing: " + ", ".join(placeholders), "warning")
    except Exception as exc:
        add("Payment accounts", False, f"check failed: {type(exc).__name__}", "warning")

    critical_failures = sum(1 for x in checks if x["severity"] == "critical" and not x["ok"])
    warnings = sum(1 for x in checks if x["severity"] == "warning" and not x["ok"])
    return {"ok": critical_failures == 0, "critical_failures": critical_failures, "warnings": warnings, "checks": checks}


def deployment_check_text():
    report = deployment_self_check()
    lines = [
        "🧪 <b>Deployment Self-Check</b>",
        f"Status: <b>{'✅ READY' if report['ok'] else '❌ ACTION REQUIRED'}</b>",
        f"Critical failures: <b>{report['critical_failures']}</b> • Warnings: <b>{report['warnings']}</b>",
        "",
    ]
    for item in report["checks"]:
        icon = "✅" if item["ok"] else ("⚠️" if item["severity"] == "warning" else "❌")
        lines.append(f"{icon} <b>{html.escape(item['name'])}</b> — {html.escape(item['detail'])}")
    return "\n".join(lines)


def performance_health_snapshot():
    snap = perf_snapshot()
    snap["cache_entries"] = len(_SETTING_CACHE)
    snap["cache_ttl_sec"] = _SETTING_CACHE_TTL
    snap["db_pool"] = DB_POOL.stats()
    snap["runtime"] = runtime_state_snapshot()
    try:
        row = db_execute("""SELECT
            (SELECT COUNT(*) FROM error_events WHERE created_at>=NOW()-INTERVAL '24 hours') errors24,
            (SELECT COUNT(*) FROM order_status_audit WHERE changed_at>=NOW()-INTERVAL '24 hours') order_transitions24,
            (SELECT COUNT(*) FROM notification_queue WHERE status='pending') notifications_pending,
            (SELECT COUNT(*) FROM notification_queue WHERE status='failed') notifications_failed,
            (SELECT COUNT(*) FROM orders WHERE status='refund_pending') refunds_pending
        """, fetch="one") or {}
        snap["operations"] = dict(row)
    except Exception as exc:
        snap["operations"] = {"error": type(exc).__name__}
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

def release_direct_order_reservation(cur, order, conn=None):
    """Release an unverified direct-payment stock hold inside the caller transaction."""
    if not order or not bool(order.get("stock_reserved")):
        return False
    oid=int(order["id"]); product_id=int(order["product_id"])
    kind=(order.get("reservation_kind") or "").strip().lower()
    released=False
    if kind=="code":
        cur.execute("UPDATE product_codes SET status='available',sold_to=NULL,order_id=NULL,sold_at=NULL WHERE order_id=%s AND status='reserved'",(oid,))
        released=cur.rowcount>0
        if conn is not None: sync_code_product_stock(product_id,conn)
    elif kind=="manual":
        cur.execute("UPDATE products SET stock=stock+1,updated_at=NOW() WHERE id=%s",(product_id,))
        released=cur.rowcount>0
    else:
        # V10.2: fail closed. Unknown/corrupt reservation metadata must never invent stock.
        # A caller transaction will roll back, preserving the reservation for operator review.
        raise RuntimeError(f"Unknown reservation kind for order #{oid}: {kind or 'empty'}")
    if not released:
        # stock_reserved=True but no corresponding row/item was actually released.
        # Treat this as an integrity error instead of silently clearing the hold flag.
        raise RuntimeError(f"Reservation integrity check failed for order #{oid} ({kind})")
    cur.execute("UPDATE orders SET stock_reserved=FALSE,reservation_kind='',updated_at=NOW() WHERE id=%s AND stock_reserved=TRUE",(oid,))
    if cur.rowcount != 1:
        raise RuntimeError(f"Reservation state changed while releasing order #{oid}")
    return True

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

def direct_order_amount_ok(amount):
    """Validate a product checkout amount without applying the wallet-deposit minimum."""
    try:
        value=float(amount)
        hi=float(setting("payment_max_deposit","100000") or 100000)
        if value <= 0:
            return False, "Invalid order amount."
        if value > hi:
            return False, f"Maximum direct payment is {fmt_money(hi)}."
        return True, ""
    except Exception:
        return False, "Invalid order amount."

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
    d=await state.get_data(); u=await aget_user(m.from_user); method=d.get("method"); amount=d.get("amount")
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
                    assess_payment_fraud(cur,payment_id,u["id"],amount,method,trx)
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
    await adb_execute("INSERT INTO payment_receipts(payment_id,file_id,media_type,uploader_tg_id) VALUES(%s,%s,%s,%s) ON CONFLICT(payment_id) DO UPDATE SET file_id=EXCLUDED.file_id,media_type=EXCLUDED.media_type,uploader_tg_id=EXCLUDED.uploader_tg_id,created_at=NOW()",(payment_id,file_id,"photo" if m.photo else "document",m.from_user.id))
    await _finish_payment_submission(m,state,receipt=True)

@router.message(PaymentState.receipt,Command("skip"))
async def payment_receipt_skip(m:Message,state:FSMContext):
    if setting("payment_receipt_required","0")=="1": return await m.answer("❌ Screenshot is required for this payment.")
    await _finish_payment_submission(m,state,receipt=False)

async def _finish_payment_submission(m:Message,state:FSMContext,receipt=False):
    d=await state.get_data(); payment_id=d.get("payment_id"); u=await aget_user(m.from_user)
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
    u=await aget_user(m.from_user)
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
    u=await aget_user(m.from_user)
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
    text=f"🎮 <b>{html.escape(p['name'])}</b>\n🏷 Category: {html.escape(p['category'])}\n📦 Quantity: {p['quantity']}\n💰 Price: {fmt_money(p['price'])}\n🚚 Delivery: {html.escape(str(p['delivery_type']))}\n📊 Stock: {effective_stock(p)}\n🔘 Active: {'Yes' if p['active'] else 'No'}\n\n{html.escape(p['description'] or 'No description.')}"
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
    if not name or not category: return await m.answer("❌ Name and category cannot be empty.")
    if len(category.encode("utf-8")) > 48: return await m.answer("❌ Category is too long for Telegram navigation. Keep it within 48 UTF-8 bytes.")
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
    if len(category.encode("utf-8")) > 48:
        return await m.answer("❌ Category is too long for Telegram navigation. Keep it within 48 UTF-8 bytes.")
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM products WHERE id=%s FOR UPDATE",(pid,))
                current=cur.fetchone()
                if not current:
                    await state.clear()
                    return await m.answer("❌ Product not found.")
                cur.execute("SELECT COUNT(*) AS c FROM product_codes WHERE product_id=%s AND status IN ('available','reserved')",(pid,))
                code_inventory=int(cur.fetchone()["c"])
                cur.execute("SELECT COUNT(*) AS c FROM orders WHERE product_id=%s AND status IN ('awaiting_payment','pending')",(pid,))
                active_orders=int(cur.fetchone()["c"])
                if delivery != current["delivery_type"] and active_orders:
                    return await m.answer("❌ Delivery type cannot be changed while this product has awaiting/pending orders.")
                if delivery=="manual" and code_inventory:
                    return await m.answer("❌ This product still has available/reserved codes. Resolve those codes before switching delivery to manual.")
                if delivery=="code":
                    cur.execute("SELECT COUNT(*) AS c FROM product_codes WHERE product_id=%s AND status='available'",(pid,))
                    stock=int(cur.fetchone()["c"])
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
    rows=db_execute("SELECT p.id,p.name,COUNT(pc.id) FILTER (WHERE pc.status='available') AS available,COUNT(pc.id) FILTER (WHERE pc.status='reserved') AS reserved,COUNT(pc.id) FILTER (WHERE pc.status='sold') AS sold FROM products p LEFT JOIN product_codes pc ON pc.product_id=p.id WHERE p.delivery_type='code' GROUP BY p.id ORDER BY p.id DESC",fetch="all")
    buttons=[[InlineKeyboardButton(text=f"🎫 {r['name'][:18]} • {r['available'] or 0} avail • {r['reserved'] or 0} held",callback_data=f"codes_add:{r['id']}")] for r in rows]
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
                cur.execute("SELECT * FROM products WHERE id=%s FOR UPDATE",(d["pid"],))
                product=cur.fetchone()
                if not product:
                    await state.clear(); return await m.answer("❌ Product not found.")
                if product["delivery_type"] != "code":
                    cur.execute("SELECT COUNT(*) AS c FROM orders WHERE product_id=%s AND status IN ('awaiting_payment','pending')",(d["pid"],))
                    if int(cur.fetchone()["c"]):
                        return await m.answer("❌ Cannot switch this product to code delivery while it has awaiting/pending orders.")
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

@router.callback_query(F.data.startswith("order_credential:"))
async def order_credential_reveal(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":",1)[1])
    # V10.8: decrypt off the Telegram event loop, then reveal only in the
    # requesting admin's private chat. Never post plaintext credentials back
    # into a group/supergroup where other members could see them.
    credential,error = await asyncio.to_thread(decrypt_order_credential, oid)
    if not credential:
        await asyncio.to_thread(security_log, "credential_reveal_failed", c.from_user.id, oid, error)
        return await c.answer(error or "Credential unavailable.",show_alert=True)
    try:
        msg = await c.bot.send_message(
            chat_id=c.from_user.id,
            text=(
                f"🔐 <b>Sensitive Credential — Order #{oid}</b>\n\n"
                f"<code>{html.escape(credential)}</code>\n\n"
                f"⚠️ Private admin view. This message will be deleted automatically in {CREDENTIAL_REVEAL_SECONDS} seconds."
            ),
        )
    except Exception as exc:
        error_id=record_runtime_error("credential_private_delivery",exc,{"admin_id":c.from_user.id,"order_id":oid})
        await asyncio.to_thread(security_log, "credential_reveal_failed", c.from_user.id, oid, f"Private DM failed ref={error_id}")
        return await c.answer(f"Could not send credential privately. Open a private chat with the bot and retry. Ref: {error_id}",show_alert=True)
    await asyncio.to_thread(security_log, "credential_revealed_private", c.from_user.id, oid, f"Private DM; auto-delete in {CREDENTIAL_REVEAL_SECONDS}s")
    await c.answer(f"Credential sent to your private chat for {CREDENTIAL_REVEAL_SECONDS} seconds.",show_alert=True)
    asyncio.create_task(_delete_sensitive_message_later(c.bot, c.from_user.id, msg.message_id))


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
                cur.execute("UPDATE orders SET admin_note=%s,delivery_note=%s,account_password='',status='completed',processed_at=NOW(),updated_at=NOW() WHERE id=%s",(note,note,oid))
                award_completed_order_rewards(cur,oid,o["user_id"],o["total"])
                cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
                cur.execute("SELECT name FROM products WHERE id=%s",(o["product_id"],)); p=cur.fetchone()
    admin_log(m.from_user.id,"manual_delivery_note",f"order #{oid} delivered with admin note")
    await state.clear()
    delivery_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 Order Details",callback_data=f"order_detail:{oid}")],[InlineKeyboardButton(text="🛍️ Buy More",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]])
    delivered_to_buyer = await notify_user(
        m.bot,
        u["tg_id"],
        f"🎉 <b>Order Delivered</b>\n\n🧾 Order: <b>#{oid}</b>\n📦 Product: <b>{html.escape(p['name'] if p else 'Product')}</b>\n\n📝 <b>Delivery Information</b>\n{html.escape(note)}",
        reply_markup=delivery_markup,
    )
    if delivered_to_buyer:
        await m.answer(f"✅ Order #{oid} completed and delivered to buyer.",reply_markup=admin_menu())
    else:
        await m.answer(f"✅ Order #{oid} completed. Buyer notification is queued for automatic retry.",reply_markup=admin_menu())


@router.callback_query(F.data.startswith("order_complete:"))
async def manual_order_complete(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Delivery note is required. Use ✍️ Write Delivery.",show_alert=True)
    return await c.answer("Delivery note is required. Use ✍️ Write Delivery.",show_alert=True)

@router.callback_query(F.data.startswith("order_reject:"))
async def manual_order_reject(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":")[1])
    try:
        with DB_LOCK:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,)); o=cur.fetchone()
                    if not o or o["status"]!="pending":
                        return await c.answer("Already processed.",show_alert=True)
                    payment_mode=(o.get("payment_mode") or "wallet").strip().lower()
                    # Restore the manual item to stock before resolving the money side.
                    if o.get("delivered_code"):
                        cur.execute("UPDATE product_codes SET status='available',sold_to=NULL,order_id=NULL,sold_at=NULL WHERE order_id=%s",(oid,))
                        sync_code_product_stock(o["product_id"],conn)
                    else:
                        cur.execute("UPDATE products SET stock=stock+1,updated_at=NOW() WHERE id=%s",(o["product_id"],))
                    cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
                    if payment_mode=="direct":
                        cur.execute("UPDATE orders SET status='refund_pending',refund_amount=0,account_password='',processed_at=NULL,updated_at=NOW() WHERE id=%s",(oid,))
                        payment=None
                        if o.get("payment_id"):
                            cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(o["payment_id"],)); payment=cur.fetchone()
                            if payment and payment["status"]=="credited":
                                cur.execute("UPDATE payments SET status='refund_pending',updated_at=NOW() WHERE id=%s",(payment["id"],))
                                record_payment_audit(cur,payment["id"],c.from_user.id,"refund_requested","credited","refund_pending",payment["amount"],payment["method"],payment["trx_id"],f"Order #{oid} rejected after direct payment; external refund required")
                        refund_mode="external"
                    else:
                        cur.execute("UPDATE orders SET status='refunded',refund_amount=total,account_password='',processed_at=NOW(),updated_at=NOW() WHERE id=%s",(oid,))
                        cur.execute("UPDATE users SET balance=balance+%s,updated_at=NOW() WHERE id=%s",(o["total"],o["user_id"]))
                        cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(o["user_id"],o["total"],"refund",f"Order #{oid} rejected"))
                        refund_mode="wallet"
    except Exception as exc:
        error_id=record_runtime_error("order_reject_refund",exc,{"admin_id":c.from_user.id,"order_id":oid})
        return await c.answer(f"Refund transition failed safely. Ref: {error_id}",show_alert=True)
    admin_log(c.from_user.id,"reject_refund",f"order #{oid} refund_mode={refund_mode}")
    if refund_mode=="external":
        await c.answer("Rejected — external refund required")
        await c.message.edit_text(
            f"↩️ Order #{oid} rejected. Stock restored.\n\n💳 <b>Refund Pending:</b> send the refund through the original payment method, then confirm below.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Mark External Refund Sent",callback_data=f"order_refund_complete:{oid}")]])
        )
        await notify_user(c.bot,u["tg_id"],f"↩️ <b>Order #{oid} rejected</b>\n\nYour refund of <b>{fmt_money(o['total'])}</b> is being returned through the original payment method.")
    else:
        await c.answer("Rejected + refunded")
        await c.message.edit_text(f"↩️ Order #{oid} rejected and refunded to buyer wallet.")
        await notify_user(c.bot,u["tg_id"],f"↩️ <b>Order #{oid} refunded</b>\nRefunded to your bot wallet: {fmt_money(o['total'])}")


@router.callback_query(F.data.startswith("order_refund_complete:"))
async def direct_refund_complete(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":",1)[1])
    try:
        with DB_LOCK:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,)); o=cur.fetchone()
                    if not o or o["status"]!="refund_pending" or (o.get("payment_mode") or "")!="direct":
                        return await c.answer("Refund is not pending for this order.",show_alert=True)
                    payment=None
                    if o.get("payment_id"):
                        cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(o["payment_id"],)); payment=cur.fetchone()
                    cur.execute("UPDATE orders SET status='refunded',refund_amount=total,processed_at=NOW(),updated_at=NOW() WHERE id=%s AND status='refund_pending'",(oid,))
                    if cur.rowcount != 1:
                        raise RuntimeError("Order refund state changed")
                    if payment and payment["status"]=="refund_pending":
                        cur.execute("UPDATE payments SET status='refunded',updated_at=NOW() WHERE id=%s",(payment["id"],))
                        record_payment_audit(cur,payment["id"],c.from_user.id,"refund_completed","refund_pending","refunded",payment["amount"],payment["method"],payment["trx_id"],f"Admin confirmed external refund for Order #{oid}")
                    cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
    except Exception as exc:
        error_id=record_runtime_error("external_refund_complete",exc,{"admin_id":c.from_user.id,"order_id":oid})
        return await c.answer(f"Could not confirm refund safely. Ref: {error_id}",show_alert=True)
    admin_log(c.from_user.id,"external_refund_complete",f"order #{oid}")
    await c.answer("Refund marked sent")
    await c.message.edit_text(f"✅ External refund for Order #{oid} marked completed.")
    await notify_user(c.bot,u["tg_id"],f"✅ <b>Refund Completed</b>\n\nOrder #{oid}: <b>{fmt_money(o['total'])}</b> was marked refunded through the original payment method.")


def payment_methods_admin_keyboard():
    rows=[]
    for code,label,icon in payment_method_specs():
        status="🟢 ON" if payment_method_enabled(code) else "🔴 OFF"
        rows.append([InlineKeyboardButton(text=f"{icon} {label} • {status}",callback_data=f"admin:paytoggle:{code}"),InlineKeyboardButton(text="✏️ Edit",callback_data=f"admin:payedit:{code}")])
    rows.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@router.callback_query(F.data=="admin:refunds")
async def admin_refund_queue(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute(
        "SELECT o.id,o.total,o.updated_at,u.tg_id,p.name,py.method "
        "FROM orders o JOIN users u ON u.id=o.user_id JOIN products p ON p.id=o.product_id "
        "LEFT JOIN payments py ON py.id=o.payment_id "
        "WHERE o.status='refund_pending' ORDER BY o.updated_at ASC LIMIT 20",
        fetch="all",
    ) or []
    if not rows:
        text="💸 <b>Refund Queue</b>\n\nNo external refunds are pending."
        kb=[[InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]]
    else:
        text="💸 <b>External Refund Queue</b>\n\n" + "\n".join(
            f"#{r['id']} • {html.escape(r['name'][:24])}\n👤 <code>{r['tg_id']}</code> • {fmt_money(r['total'])} • {html.escape((r['method'] or 'direct').title())}"
            for r in rows
        )
        kb=[[InlineKeyboardButton(text=f"✅ Refund Sent #{r['id']}",callback_data=f"order_refund_complete:{r['id']}")] for r in rows]
        kb.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")])
    await c.answer()
    await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


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

def assess_payment_fraud(cur, payment_id, user_id, amount, method, trx_id):
    """Conservative heuristic: high scores require manual review; nothing is auto-rejected."""
    score=0; flags=[]
    amount=float(amount or 0)
    if RISK_HIGH_VALUE_AMOUNT > 0 and amount >= RISK_HIGH_VALUE_AMOUNT:
        score += 25; flags.append("high_value")
    cur.execute("SELECT COUNT(*) AS c FROM payments WHERE user_id=%s AND id<>%s AND created_at>=NOW()-(%s * INTERVAL '1 minute')",(user_id,payment_id,FRAUD_VELOCITY_WINDOW_MINUTES))
    velocity=int((cur.fetchone() or {}).get("c") or 0)
    if velocity >= FRAUD_VELOCITY_COUNT:
        score += min(35,20 + (velocity-FRAUD_VELOCITY_COUNT)*5); flags.append(f"velocity:{velocity+1}")
    cur.execute("SELECT COUNT(*) AS c FROM payments WHERE user_id=%s AND id<>%s AND status='rejected' AND created_at>=NOW()-INTERVAL '24 hours'",(user_id,payment_id))
    rejects=int((cur.fetchone() or {}).get("c") or 0)
    if rejects:
        score += min(35,15*rejects); flags.append(f"recent_rejects:{rejects}")
    cur.execute("SELECT COUNT(*) AS c FROM payments WHERE user_id=%s AND id<>%s AND status IN ('pending','rejected') AND created_at>=NOW()-INTERVAL '2 hours'",(user_id,payment_id))
    attempts=int((cur.fetchone() or {}).get("c") or 0)
    if attempts >= 4:
        score += min(20,5*(attempts-3)); flags.append(f"rapid_attempts:{attempts+1}")
    score=max(0,min(100,int(score)))
    required=score >= FRAUD_REVIEW_SCORE
    cur.execute("UPDATE payments SET fraud_score=%s,fraud_flags=%s,review_required=%s,updated_at=NOW() WHERE id=%s",(score,",".join(flags),required,payment_id))
    return score,flags,required


def refresh_payment_fraud(payment_id):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id,user_id,amount,method,trx_id,status FROM payments WHERE id=%s FOR UPDATE",(payment_id,))
                p=cur.fetchone()
                if not p or p["status"]!="pending": return None
                score,flags,required=assess_payment_fraud(cur,p["id"],p["user_id"],p["amount"],p["method"],p["trx_id"])
                return {"score":score,"flags":flags,"required":required}


def payment_evidence_snapshot(payment_id):
    row=db_execute("""SELECT py.id,py.user_id,py.order_id,py.amount,py.method,py.trx_id,py.status,py.fraud_score,py.review_required,py.review_cleared_at,u.tg_id,r.file_id,r.media_type,o.status AS order_status,(SELECT COUNT(*) FROM payment_audit pa WHERE pa.payment_id=py.id) audit_count FROM payments py JOIN users u ON u.id=py.user_id LEFT JOIN payment_receipts r ON r.payment_id=py.id LEFT JOIN orders o ON o.id=py.order_id WHERE py.id=%s""",(payment_id,),"one")
    if not row: return None
    score=0
    if str(row.get("trx_id") or "").strip(): score+=35
    if row.get("file_id"): score+=35
    if row.get("order_id"): score+=15
    if not row.get("review_required") or row.get("review_cleared_at"): score+=15
    row["evidence_score"]=score; row["evidence_label"]="STRONG" if score>=85 else "MODERATE" if score>=60 else "LIMITED"
    return row

def buyer_payment_timeline(payment_status,order_status=None):
    p=str(payment_status or "pending"); o=str(order_status or "")
    if p=="approved" or o=="completed": return "✅ Submitted  →  ✅ Reviewed  →  ✅ Completed"
    if p=="rejected": return "✅ Submitted  →  ❌ Rejected  →  🆘 Support available"
    if p in ("refunded","refund_pending"): return "✅ Submitted  →  ✅ Reviewed  →  ↩️ Refund processing"
    return "✅ Submitted  →  ⏳ Admin review  →  📦 Fulfilment"

@router.callback_query(F.data.startswith("pay_evidence:"))
async def payment_evidence_view(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":",1)[1]); row=await asyncio.to_thread(payment_evidence_snapshot,pid)
    if not row: return await c.answer("Payment not found.",show_alert=True)
    receipt=f"Yes ({html.escape(str(row.get('media_type') or 'unknown'))})" if row.get("file_id") else "No"
    text=(f"🔎 <b>PAYMENT EVIDENCE #{pid}</b>\n━━━━━━━━━━━━━━━━━━\n📊 Evidence completeness: <b>{row['evidence_score']}/100 • {row['evidence_label']}</b>\n⚠️ <i>This is evidence completeness, not automatic payment verification.</i>\n\n👤 Buyer: <code>{row['tg_id']}</code>\n💰 Amount: <b>{fmt_money(row['amount'])}</b>\n💳 Method: <b>{html.escape(str(row['method']).title())}</b>\n🧾 TxID: <code>{html.escape(str(row['trx_id'] or ''))}</code>\n📸 Receipt: <b>{receipt}</b>\n🛡 Fraud risk: <b>{int(row.get('fraud_score') or 0)}/100</b>\n🧾 Audit events: <b>{int(row.get('audit_count') or 0)}</b>")
    kb=[]
    if row.get("file_id"): kb.append([InlineKeyboardButton(text="📸 Open Receipt",callback_data=f"pay_receipt:{pid}")])
    kb.append([InlineKeyboardButton(text="🧾 Audit Trail",callback_data=f"pay_audit:{pid}"),InlineKeyboardButton(text="⬅️ Payments",callback_data="admin:payments")])
    await c.answer(); await c.message.answer(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("buyer_payment:"))
async def buyer_payment_view(c:CallbackQuery):
    pid=int(c.data.split(":",1)[1]); u=await aget_user(c.from_user)
    row=await adb_execute("SELECT py.id,py.amount,py.method,py.status,py.review_required,py.review_cleared_at,py.order_id,r.file_id,o.status AS order_status FROM payments py LEFT JOIN payment_receipts r ON r.payment_id=py.id LEFT JOIN orders o ON o.id=py.order_id WHERE py.id=%s AND py.user_id=%s",(pid,u["id"]),"one")
    if not row: return await c.answer("Payment not found.",show_alert=True)
    text=(f"🛡 <b>PAYMENT STATUS #{pid}</b>\n━━━━━━━━━━━━━━━━━━\n{buyer_payment_timeline(row['status'],row.get('order_status'))}\n\n💰 Amount: <b>{fmt_money(row['amount'])}</b>\n💳 Method: <b>{html.escape(str(row['method']).title())}</b>\n📸 Receipt attached: <b>{'Yes' if row.get('file_id') else 'No'}</b>\n\nℹ️ Receipt submission does not itself confirm payment. An admin verifies the transaction before fulfilment.")
    kb=[[InlineKeyboardButton(text="🆘 Report Payment Issue",callback_data=f"payment_issue:{pid}")]]
    if row.get("order_id"): kb.append([InlineKeyboardButton(text="📦 Order Details",callback_data=f"order_detail:{row['order_id']}")])
    kb.append([InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")])
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("payment_issue:"))
async def payment_issue_create(c:CallbackQuery):
    pid=int(c.data.split(":",1)[1]); u=await aget_user(c.from_user)
    p=await adb_execute("SELECT id,amount,method,status FROM payments WHERE id=%s AND user_id=%s",(pid,u["id"]),"one")
    if not p: return await c.answer("Payment not found.",show_alert=True)
    case=await adb_execute("INSERT INTO payment_support_cases(payment_id,user_id) VALUES(%s,%s) ON CONFLICT DO NOTHING RETURNING id",(pid,u["id"]),"one")
    if not case:
        existing=await adb_execute("SELECT id FROM payment_support_cases WHERE payment_id=%s AND user_id=%s AND status='open' ORDER BY id DESC LIMIT 1",(pid,u["id"]),"one")
        if existing: return await c.answer(f"Support case #{existing['id']} is already open.",show_alert=True)
        return await c.answer("Could not create support case safely. Please retry.",show_alert=True)
    case_id=case["id"]
    for admin_id in ADMIN_IDS:
        try: await c.bot.send_message(admin_id,f"🆘 <b>Payment Support Case #{case_id}</b>\nPayment: <b>#{pid}</b>\nBuyer: <code>{c.from_user.id}</code>\nAmount: <b>{fmt_money(p['amount'])}</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🆘 Open Case",callback_data=f"support_case:{case_id}"),InlineKeyboardButton(text="🔎 Evidence",callback_data=f"pay_evidence:{pid}")],[InlineKeyboardButton(text="🧾 Audit",callback_data=f"pay_audit:{pid}")]]))
        except Exception as exc: record_runtime_error("payment_support_notify",exc,{"case_id":case_id,"payment_id":pid})
    await c.answer("Support case created."); await c.message.answer(f"🆘 <b>Support Case #{case_id} Created</b>\n\nYour payment issue has been flagged for admin review.")


def support_center_snapshot():
    summary=db_execute("""SELECT
      COUNT(*) FILTER (WHERE status='open') AS open_count,
      COUNT(*) FILTER (WHERE status='open' AND created_at < NOW()-INTERVAL '30 minutes') AS aged_count,
      COUNT(*) FILTER (WHERE status='resolved' AND resolved_at>=NOW()-INTERVAL '24 hours') AS resolved24
      FROM payment_support_cases""",fetch="one") or {}
    rows=db_execute("""SELECT sc.id,sc.payment_id,sc.user_id,sc.status,sc.reason,sc.admin_note,sc.assigned_admin,
      sc.created_at,sc.updated_at,sc.resolved_at,sc.resolved_by,u.tg_id,py.amount,py.method,py.status AS payment_status
      FROM payment_support_cases sc JOIN users u ON u.id=sc.user_id JOIN payments py ON py.id=sc.payment_id
      ORDER BY CASE WHEN sc.status='open' THEN 0 ELSE 1 END, sc.updated_at DESC LIMIT 20""",fetch="all") or []
    return summary,rows


@router.callback_query(F.data=="admin:support")
async def admin_support_center(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    summary,rows=await asyncio.to_thread(support_center_snapshot)
    text=(f"🆘 <b>ULTRA SUPPORT CENTER</b>\n━━━━━━━━━━━━━━━━━━\n"
          f"🔴 Open: <b>{int(summary.get('open_count') or 0)}</b> • ⏳ Aged 30m+: <b>{int(summary.get('aged_count') or 0)}</b> • ✅ Resolved 24h: <b>{int(summary.get('resolved24') or 0)}</b>\n\n"
          "Select a case to review payment evidence, notes, and resolution status.")
    kb=[]
    for r in rows:
        icon="🔴" if r['status']=='open' else "✅"
        kb.append([InlineKeyboardButton(text=f"{icon} Case #{r['id']} • Pay #{r['payment_id']} • {fmt_money(r['amount'])}",callback_data=f"support_case:{r['id']}")])
    kb.append([InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:support"),InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")])
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


def support_case_snapshot(case_id):
    return db_execute("""SELECT sc.*,u.tg_id,py.amount,py.method,py.status AS payment_status,py.order_id,py.trx_id
      FROM payment_support_cases sc JOIN users u ON u.id=sc.user_id JOIN payments py ON py.id=sc.payment_id
      WHERE sc.id=%s""",(case_id,),"one")


@router.callback_query(F.data.startswith("support_case:"))
async def admin_support_case(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    case_id=int(c.data.split(":",1)[1]); r=await asyncio.to_thread(support_case_snapshot,case_id)
    if not r: return await c.answer("Support case not found.",show_alert=True)
    note=html.escape(str(r.get('admin_note') or 'No admin note yet.'))
    status=str(r.get('status') or 'open')
    text=(f"🆘 <b>SUPPORT CASE #{case_id}</b>\n━━━━━━━━━━━━━━━━━━\n"
          f"Status: <b>{'OPEN 🔴' if status=='open' else 'RESOLVED ✅'}</b>\n👤 Buyer: <code>{r['tg_id']}</code>\n"
          f"💳 Payment: <b>#{r['payment_id']}</b> • {fmt_money(r['amount'])} • {html.escape(str(r['method']).title())}\n"
          f"Payment status: <b>{html.escape(str(r['payment_status']).upper())}</b>\n🧾 TxID: <code>{html.escape(str(r.get('trx_id') or ''))}</code>\n\n"
          f"📝 <b>Admin note</b>\n{note}\n\n🕒 Opened: {r['created_at']}\n🔄 Updated: {r['updated_at']}")
    kb=[[InlineKeyboardButton(text="🔎 Evidence",callback_data=f"pay_evidence:{r['payment_id']}"),InlineKeyboardButton(text="🧾 Audit",callback_data=f"pay_audit:{r['payment_id']}")],
        [InlineKeyboardButton(text="📝 Add/Replace Note",callback_data=f"support_note:{case_id}")]]
    if status=='open': kb.append([InlineKeyboardButton(text="✅ Resolve + Notify Buyer",callback_data=f"support_resolve:{case_id}")])
    else: kb.append([InlineKeyboardButton(text="↩️ Reopen + Notify Buyer",callback_data=f"support_reopen:{case_id}")])
    kb.append([InlineKeyboardButton(text="⬅️ Support Center",callback_data="admin:support")])
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("support_note:"))
async def support_note_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    case_id=int(c.data.split(":",1)[1]); r=await asyncio.to_thread(support_case_snapshot,case_id)
    if not r: return await c.answer("Support case not found.",show_alert=True)
    await state.update_data(support_case_id=case_id); await state.set_state(AdminState.support_note)
    await c.answer(); await c.message.answer(f"📝 Send the admin note for support case <b>#{case_id}</b>.\nUse /cancel to stop.")


@router.message(AdminState.support_note,Command("cancel"))
async def support_note_cancel(m:Message,state:FSMContext):
    await state.clear(); await m.answer("Support note cancelled.")


@router.message(AdminState.support_note,F.text)
async def support_note_save(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): await state.clear(); return
    d=await state.get_data(); case_id=int(d.get('support_case_id') or 0); note=(m.text or '').strip()[:1500]
    if not case_id or not note: return await m.answer("Please send a non-empty note or /cancel.")
    row=await adb_execute("UPDATE payment_support_cases SET admin_note=%s,assigned_admin=%s,updated_at=NOW() WHERE id=%s RETURNING id",(note,m.from_user.id,case_id),"one")
    await state.clear()
    if not row: return await m.answer("Support case not found.")
    await asyncio.to_thread(admin_log,m.from_user.id,"support_note",f"case={case_id}")
    await m.answer(f"✅ Note saved for support case <b>#{case_id}</b>.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🆘 Open Case",callback_data=f"support_case:{case_id}")]]))


async def _support_case_transition(c,case_id,new_status):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    expected='open' if new_status=='resolved' else 'resolved'
    if new_status=='resolved':
        row=await adb_execute("""UPDATE payment_support_cases SET status='resolved',resolved_at=NOW(),resolved_by=%s,assigned_admin=COALESCE(assigned_admin,%s),updated_at=NOW()
          WHERE id=%s AND status='open' RETURNING id,payment_id,user_id,admin_note""",(c.from_user.id,c.from_user.id,case_id),"one")
    else:
        row=await adb_execute("""UPDATE payment_support_cases SET status='open',resolved_at=NULL,resolved_by=NULL,assigned_admin=%s,updated_at=NOW()
          WHERE id=%s AND status='resolved' RETURNING id,payment_id,user_id,admin_note""",(c.from_user.id,case_id),"one")
    if not row: return await c.answer(f"Case is no longer {expected} or was already processed.",show_alert=True)
    u=await adb_execute("SELECT tg_id FROM users WHERE id=%s",(row['user_id'],),"one")
    if u:
        if new_status=='resolved':
            msg=f"✅ <b>Support Case #{case_id} Resolved</b>\n\nYour payment support case has been reviewed."
            if row.get('admin_note'): msg += f"\n\n📝 Admin note: {html.escape(str(row['admin_note']))}"
        else:
            msg=f"↩️ <b>Support Case #{case_id} Reopened</b>\n\nYour case is back under admin review."
        await asyncio.to_thread(enqueue_notification,u['tg_id'],msg,[[["🛡 Payment Status",f"buyer_payment:{row['payment_id']}"],["🏠 Main Menu","main_menu"]]])
    await asyncio.to_thread(admin_log,c.from_user.id,f"support_{new_status}",f"case={case_id};payment={row['payment_id']}")
    return await admin_support_case(c)


@router.callback_query(F.data.startswith("support_resolve:"))
async def support_case_resolve(c:CallbackQuery):
    return await _support_case_transition(c,int(c.data.split(":",1)[1]),'resolved')


@router.callback_query(F.data.startswith("support_reopen:"))
async def support_case_reopen(c:CallbackQuery):
    return await _support_case_transition(c,int(c.data.split(":",1)[1]),'open')


_SUPPORT_ALERT_STATE={"signature":"","sent_at":0.0}


async def support_case_alert_loop(bot):
    while True:
        try:
            snap=await asyncio.to_thread(db_execute,"""SELECT COUNT(*) FILTER (WHERE status='open') AS open_count,
              COUNT(*) FILTER (WHERE status='open' AND created_at<NOW()-INTERVAL '30 minutes') AS aged_count,
              MIN(created_at) FILTER (WHERE status='open') AS oldest FROM payment_support_cases""",(),"one") or {}
            open_count=int(snap.get('open_count') or 0); aged=int(snap.get('aged_count') or 0)
            signature=f"{open_count}:{aged}:{snap.get('oldest')}"; now=time.monotonic()
            if aged>0 and (signature!=_SUPPORT_ALERT_STATE['signature'] or now-_SUPPORT_ALERT_STATE['sent_at']>=RISK_ALERT_COOLDOWN_SECONDS):
                text=f"🆘 <b>Support Attention Required</b>\nOpen cases: <b>{open_count}</b> • Waiting 30m+: <b>{aged}</b>\nOldest: <b>{html.escape(str(snap.get('oldest') or 'n/a'))}</b>"
                kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🆘 Open Support Center",callback_data="admin:support")]])
                for admin_id in ADMIN_IDS:
                    try: await bot.send_message(admin_id,text,reply_markup=kb)
                    except Exception: perf_inc("errors")
                _SUPPORT_ALERT_STATE.update(signature=signature,sent_at=now)
            await asyncio.sleep(900)
        except asyncio.CancelledError: raise
        except Exception as exc:
            record_runtime_error("support_case_alert_loop",exc,{"instance_id":INSTANCE_ID}); await asyncio.sleep(900)


@router.callback_query(F.data=="admin:payments")
async def admin_payments(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    base_rows=await asyncio.to_thread(db_execute,"SELECT id FROM payments WHERE status='pending' ORDER BY id DESC LIMIT 15",(),"all")
    for _r in (base_rows or []):
        try: await asyncio.to_thread(refresh_payment_fraud,_r["id"])
        except Exception as exc: record_runtime_error("fraud_refresh",exc,{"payment_id":_r["id"]})
    rows=await asyncio.to_thread(db_execute,"SELECT py.id,py.amount,py.method,py.trx_id,py.created_at,py.order_id,py.fraud_score,py.fraud_flags,py.review_required,py.review_cleared_at,u.tg_id FROM payments py JOIN users u ON u.id=py.user_id WHERE py.status='pending' ORDER BY py.id DESC LIMIT 15",(),"all")
    if not rows: text="💳 No pending payments."; kb=[[InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]
    else:
        def _risk_line(r):
            if r.get("review_required") and not r.get("review_cleared_at"):
                return f"🛡 <b>MANUAL REVIEW</b> • Risk {int(r.get('fraud_score') or 0)}/100 • {html.escape(r.get('fraud_flags') or 'signal')}"
            if r.get("review_cleared_at"):
                return f"✅ Review cleared • Risk {int(r.get('fraud_score') or 0)}/100"
            return f"🟢 Risk {int(r.get('fraud_score') or 0)}/100"
        text="💳 <b>Pending Payments</b>\n\n"+"\n".join(f"#{r['id']} • {fmt_money(r['amount'])}\n👤 <code>{r['tg_id']}</code> • {html.escape(str(r['method']).title())}\nTrxID: <code>{html.escape(str(r['trx_id'] or ''))}</code>\n{_risk_line(r)}\n" for r in rows)
        kb=[]
        for r in rows:
            receipt=await adb_execute("SELECT 1 FROM payment_receipts WHERE payment_id=%s",(r['id'],),"one")
            approve_label = f"✅ Verify & Fulfill #{r['id']}" if r.get('order_id') else f"✅ Credit #{r['id']}"
            if r.get("review_required") and not r.get("review_cleared_at"):
                kb.append([InlineKeyboardButton(text=f"🛡 Clear Review #{r['id']}",callback_data=f"pay_review_clear:{r['id']}"),InlineKeyboardButton(text=setting("admin_reject", "❌ Reject"),callback_data=f"pay_reject:{r['id']}")])
            else:
                row_buttons=[InlineKeyboardButton(text=approve_label,callback_data=f"pay_credit:{r['id']}"),InlineKeyboardButton(text=setting("admin_reject", "❌ Reject"),callback_data=f"pay_reject:{r['id']}")]
                kb.append(row_buttons)
            evidence_row=[InlineKeyboardButton(text=f"🔎 Evidence #{r['id']}",callback_data=f"pay_evidence:{r['id']}")]
            if receipt: evidence_row.append(InlineKeyboardButton(text=f"📸 Receipt #{r['id']}",callback_data=f"pay_receipt:{r['id']}"))
            kb.append(evidence_row)
        kb.append([InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")])
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("pay_receipt:"))
async def payment_receipt_view(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":",1)[1]); row=await adb_execute("SELECT file_id FROM payment_receipts WHERE payment_id=%s",(pid,),"one")
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
    rows=await adb_execute("SELECT action,old_status,new_status,admin_id,note,created_at FROM payment_audit WHERE payment_id=%s ORDER BY created_at DESC LIMIT 12",(pid,),"all")
    if not rows: return await c.answer("No audit history.",show_alert=True)
    lines=[f"🧾 <b>Payment #{pid} Audit</b>"]
    for r in rows:
        actor=f"Admin {r['admin_id']}" if r["admin_id"] else "User/System"
        lines.append(f"• <b>{html.escape(r['action'])}</b> · {html.escape(actor)} · {r['created_at']:%Y-%m-%d %H:%M}\\n  {html.escape(r['old_status'])} → {html.escape(r['new_status'])}\\n  {html.escape(r['note'] or '')}")
    await c.answer()
    await c.message.answer("\n\n".join(lines))

@router.callback_query(F.data.startswith("pay_review_clear:"))
async def payment_review_clear(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":",1)[1])
    try:
        with DB_LOCK:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(pid,)); p=cur.fetchone()
                    if not p or p["status"]!="pending": return await c.answer("Payment is no longer pending.",show_alert=True)
                    if not p.get("review_required"):
                        return await c.answer("This payment does not require manual review.",show_alert=True)
                    cur.execute("UPDATE payments SET review_cleared_at=NOW(),review_cleared_by=%s,updated_at=NOW() WHERE id=%s AND status='pending'",(c.from_user.id,pid))
                    record_payment_audit(cur,pid,c.from_user.id,"fraud_review_cleared","pending","pending",p["amount"],p["method"],p["trx_id"],f"Manual fraud review cleared; score={p.get('fraud_score',0)} flags={p.get('fraud_flags','')}")
        admin_log(c.from_user.id,"fraud_review_cleared",f"payment #{pid}")
        await c.answer("Manual review cleared.",show_alert=True)
        return await admin_payments(c)
    except Exception as exc:
        error_id=record_runtime_error("fraud_review_clear",exc,{"admin_id":c.from_user.id,"payment_id":pid})
        return await c.answer(f"Could not clear review. Ref: {error_id}",show_alert=True)


@router.callback_query(F.data.startswith("pay_credit:"))
async def payment_credit(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1])
    direct_order=None
    try:
        with DB_LOCK:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(pid,)); p=cur.fetchone()
                    if not p or p["status"]!="pending":
                        return await c.answer("Already processed.",show_alert=True)
                    if p.get("review_required") and not p.get("review_cleared_at"):
                        return await c.answer(f"🛡 Manual review required first. Risk {int(p.get('fraud_score') or 0)}/100.",show_alert=True)
                    if p.get("order_id"):
                        cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(p["order_id"],)); o=cur.fetchone()
                        if not o or o["status"]!="awaiting_payment":
                            raise RuntimeError("Linked order is no longer awaiting payment.")
                        cur.execute("SELECT * FROM products WHERE id=%s FOR UPDATE",(o["product_id"],)); prod=cur.fetchone()
                        if not prod:
                            raise RuntimeError("Product record is unavailable.")
                        delivered_code=None
                        status="pending"
                        reservation_kind=(o.get("reservation_kind") or "").strip().lower()
                        if bool(o.get("stock_reserved")):
                            if reservation_kind=="code":
                                cur.execute("SELECT * FROM product_codes WHERE order_id=%s AND status='reserved' FOR UPDATE",(o["id"],))
                                code_row=cur.fetchone()
                                if not code_row:
                                    raise RuntimeError("Reserved code is missing; approval aborted.")
                                cur.execute("UPDATE product_codes SET status='sold',sold_at=NOW() WHERE id=%s AND status='reserved'",(code_row["id"],))
                                if cur.rowcount != 1:
                                    raise RuntimeError("Reserved code changed; approval aborted.")
                                delivered_code=code_row["code"]; status="completed"
                                sync_code_product_stock(o["product_id"],conn)
                            elif reservation_kind=="manual":
                                status="pending"
                            else:
                                raise RuntimeError("Unknown stock reservation type; approval aborted.")
                        else:
                            if prod["delivery_type"]=="code":
                                cur.execute("SELECT * FROM product_codes WHERE product_id=%s AND status='available' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",(o["product_id"],))
                                code_row=cur.fetchone()
                                if not code_row:
                                    raise RuntimeError("Code stock unavailable.")
                                cur.execute("UPDATE product_codes SET status='sold',sold_to=%s,sold_at=NOW(),order_id=%s WHERE id=%s AND status='available'",(o["user_id"],o["id"],code_row["id"]))
                                if cur.rowcount != 1:
                                    raise RuntimeError("Code stock changed; approval aborted.")
                                delivered_code=code_row["code"]; status="completed"
                                sync_code_product_stock(o["product_id"],conn)
                            else:
                                cur.execute("UPDATE products SET stock=stock-1,updated_at=NOW() WHERE id=%s AND stock>0",(o["product_id"],))
                                if cur.rowcount!=1:
                                    raise RuntimeError("Stock changed; approval aborted.")
                                status="pending"
                        cur.execute("UPDATE payments SET status='credited',updated_at=NOW() WHERE id=%s AND status='pending'",(pid,))
                        if cur.rowcount != 1:
                            raise RuntimeError("Payment status changed; approval aborted.")
                        clear_password = "" if status=="completed" else (o.get("account_password") or "")
                        cur.execute(
                            "UPDATE orders SET status=%s,delivered_code=%s,account_password=%s,stock_reserved=FALSE,reservation_kind='',processed_at=%s,updated_at=NOW() WHERE id=%s",
                            (status,delivered_code,clear_password,None if status=="pending" else datetime.now(timezone.utc),o["id"]),
                        )
                        if status=="completed":
                            award_completed_order_rewards(cur,o["id"],o["user_id"],o["total"])
                        record_payment_audit(cur,pid,c.from_user.id,"order_approved","pending","credited",p["amount"],p["method"],p["trx_id"],f"Direct payment approved; Order #{o['id']} fulfilled from reserved stock")
                        cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
                        cur.execute("SELECT name FROM products WHERE id=%s",(o["product_id"],)); prod_name=cur.fetchone()
                        direct_order=(o,prod,delivered_code,status,u,prod_name)
                    else:
                        cur.execute("UPDATE payments SET status='credited',updated_at=NOW() WHERE id=%s AND status='pending'",(pid,))
                        if cur.rowcount != 1:
                            return await c.answer("Already processed.",show_alert=True)
                        cur.execute("UPDATE users SET balance=balance+%s,updated_at=NOW() WHERE id=%s",(p["amount"],p["user_id"]))
                        cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(p["user_id"],p["amount"],"payment",f"Payment #{pid}"))
                        record_payment_audit(cur,pid,c.from_user.id,"credited","pending","credited",p["amount"],p["method"],p["trx_id"],"Admin approved payment and credited wallet")
                        cur.execute("SELECT tg_id FROM users WHERE id=%s",(p["user_id"],)); u=cur.fetchone()
    except Exception as exc:
        error_id=record_runtime_error("payment_approval",exc,{"admin_id":c.from_user.id,"payment_id":pid})
        try:
            await c.answer(f"Approval failed safely. Ref: {error_id}",show_alert=True)
        except Exception:
            pass
        return
    admin_log(c.from_user.id,"credit_payment",f"payment #{pid}")
    await c.answer("Approved")
    if p.get("order_id"):
        o,prod,delivered_code,status,u,prod_name=direct_order
        await c.message.edit_text(f"✅ Direct payment #{pid} approved. Order #{o['id']} {'completed.' if status=='completed' else 'sent to manual delivery.'}")
        if status=="completed":
            delivery=f"🎁 <b>Your Code</b>\n<code>{html.escape(delivered_code or '')}</code>"
            await notify_user(
                c.bot,u["tg_id"],
                f"🎉 <b>Order #{o['id']} Completed</b>\n\n📦 Product: <b>{html.escape(prod_name['name'] if prod_name else 'Product')}</b>\n"
                f"💳 Paid directly: <b>{fmt_money(o['total'])}</b>\n\n{delivery}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛍️ Buy More",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]])
            )
        else:
            for admin_id in ADMIN_IDS:
                try:
                    has_credential = bool((o.get("account_password") or "").strip())
                    direct_admin_text = (
                        f"🧾 <b>Direct Payment Order #{o['id']} — Manual Delivery</b>\n\n"
                        f"👤 User: <code>{u['tg_id']}</code>\n"
                        f"🎮 Product: <b>{html.escape(prod_name['name'] if prod_name else 'Product')}</b>\n"
                        f"💰 Paid: <b>{fmt_money(o['total'])}</b>\n"
                        f"💳 Method: <b>{html.escape(p['method'])}</b>\n"
                        f"🧾 TrxID: <code>{html.escape(p['trx_id'] or '')}</code>\n"
                        f"🆔 ID/UID: <code>{html.escape(o['game_uid'] or '')}</code>\n"
                        f"🔐 Credential: <b>{'Encrypted — reveal only when needed' if has_credential else 'Not required'}</b>\n\n"
                        "✍️ Write delivery information to complete this order."
                    )
                    action_rows=[]
                    if has_credential:
                        action_rows.append([InlineKeyboardButton(text="🔐 Reveal Credential",callback_data=f"order_credential:{o['id']}")])
                    action_rows += [
                        [InlineKeyboardButton(text="✍️ Write Delivery",callback_data=f"order_note:{o['id']}")],
                        [InlineKeyboardButton(text="❌ Reject + Refund",callback_data=f"order_reject:{o['id']}")]
                    ]
                    await c.bot.send_message(
                        admin_id,direct_admin_text,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=action_rows)
                    )
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
    try:
        with DB_LOCK:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(pid,))
                    p=cur.fetchone()
                    if not p or p["status"]!="pending":
                        return await c.answer("Already processed.",show_alert=True)
                    if p.get("order_id"):
                        cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(p["order_id"],))
                        order=cur.fetchone()
                        if not order:
                            raise RuntimeError("Linked order is missing; rejection aborted.")
                        if order["status"] != "awaiting_payment":
                            raise RuntimeError(f"Linked order state is {order['status']}; rejection aborted to prevent payment/order mismatch.")
                        release_direct_order_reservation(cur,order,conn)
                        cur.execute("UPDATE orders SET status='cancelled',account_password='',processed_at=NOW(),updated_at=NOW() WHERE id=%s AND status='awaiting_payment'",(order["id"],))
                        if cur.rowcount != 1:
                            raise RuntimeError("Linked order state changed during rejection.")
                    cur.execute("UPDATE payments SET status='rejected',updated_at=NOW() WHERE id=%s AND status='pending'",(pid,))
                    if cur.rowcount != 1:
                        raise RuntimeError("Payment status changed during rejection.")
                    record_payment_audit(cur,pid,c.from_user.id,"rejected","pending","rejected",p["amount"],p["method"],p["trx_id"],"Admin rejected payment; any pending stock reservation was released")
                    cur.execute("SELECT tg_id FROM users WHERE id=%s",(p["user_id"],))
                    u=cur.fetchone()
    except Exception as exc:
        error_id=record_runtime_error("payment_rejection",exc,{"admin_id":c.from_user.id,"payment_id":pid})
        try:
            await c.answer(f"Rejection failed safely. Ref: {error_id}",show_alert=True)
        except Exception:
            pass
        return
    admin_log(c.from_user.id,"reject_payment",f"payment #{pid}")
    await c.answer("Rejected")
    await c.message.edit_text(f"❌ Payment #{pid} rejected. Any reserved stock was released.")
    await notify_user(
        c.bot,u["tg_id"],
        f"❌ <b>Payment Rejected</b>\n\nPayment: #{pid}\nAmount: <b>{fmt_money(p['amount'])}</b>\n\nYour payment could not be verified. Any reserved stock was released. You can try the purchase again or contact support if you believe this is an error.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔁 Buy Again",callback_data="home:shop")],
            [InlineKeyboardButton(text=setting("button_support","🆘 Support"),callback_data="home:support"),InlineKeyboardButton(text=setting("button_orders","📦 My Orders"),callback_data="home:orders")]
        ])
    )

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
    await c.answer(); await c.message.edit_text(f"👤 <b>{html.escape(u['name'] or 'User')}</b>\n\nTelegram ID: <code>{u['tg_id']}</code>\nUsername: @{html.escape(u['username'] or '-')}\nBalance: <b>{fmt_money(u['balance'])}</b>\nOrders: <b>{orders}</b>\nStatus: {'🚫 Blocked' if u['blocked'] else '🟢 Active'}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_unblock", "🔓 Unblock") if u['blocked'] else setting("admin_block", "🚫 Block"),callback_data=f"user_toggle:{uid}")],[InlineKeyboardButton(text=setting("admin_users_back", "⬅️ Users"),callback_data="admin:users")]]))

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

def observability_snapshot():
    """Read-only health signals; never mutates financial state."""
    row = db_execute("""SELECT
        (SELECT COUNT(*) FROM notification_queue WHERE status='failed') failed_notifications,
        (SELECT COUNT(*) FROM notification_queue WHERE status='sending' AND claimed_at < NOW()-(%s * INTERVAL '1 minute')) stale_notification_leases,
        (SELECT COUNT(*) FROM notification_queue WHERE status='pending' AND next_attempt_at <= NOW()) due_notifications,
        (SELECT COALESCE(EXTRACT(EPOCH FROM (NOW()-MIN(created_at))),0) FROM notification_queue WHERE status='pending') oldest_pending_notification_seconds,
        (SELECT COUNT(*) FROM payments WHERE status='pending' AND created_at < NOW()-(%s * INTERVAL '1 minute')) aged_pending_payments,
        (SELECT COUNT(*) FROM orders WHERE status='refund_pending') refund_pending,
        (SELECT COUNT(*) FROM error_events WHERE created_at >= NOW()-INTERVAL '15 minutes') errors15
    """, (WORKER_LEASE_MINUTES, RISK_AGED_PENDING_MINUTES), fetch="one") or {}
    reasons=[]; score=100
    failed=int(row.get("failed_notifications") or 0); stale=int(row.get("stale_notification_leases") or 0)
    aged=int(row.get("aged_pending_payments") or 0); refunds=int(row.get("refund_pending") or 0)
    errors=int(row.get("errors15") or 0); oldest=float(row.get("oldest_pending_notification_seconds") or 0)
    if stale: score-=20; reasons.append(f"{stale} stale notification lease(s)")
    if failed: score-=min(25,5+failed*2); reasons.append(f"{failed} failed notification(s)")
    if oldest>900: score-=15; reasons.append(f"notification backlog oldest {int(oldest//60)}m")
    if aged: score-=min(20,5+aged*2); reasons.append(f"{aged} aged pending payment(s)")
    if refunds: score-=min(15,5+refunds); reasons.append(f"{refunds} refund-pending order(s)")
    if errors: score-=min(20,errors*3); reasons.append(f"{errors} runtime error(s) in 15m")
    return {**row,"health_score":max(0,min(100,score)),"health_reasons":reasons}


def self_heal_safe_operations():
    """Repair expired operational leases only; never approve/reject/refund money."""
    rows=db_execute("""UPDATE notification_queue
        SET status='pending',claimed_at=NULL,claimed_by='',next_attempt_at=NOW(),
            last_error=CASE WHEN last_error='' THEN 'V10.19 stale lease self-heal' ELSE last_error END
        WHERE status='sending' AND claimed_at < NOW()-(%s * INTERVAL '1 minute')
        RETURNING id""",(WORKER_LEASE_MINUTES,),fetch="all") or []
    return {"notification_leases_requeued":len(rows)}


def _health_status(score):
    if score>=90: return "healthy"
    if score>=HEALTH_DEGRADED_SCORE: return "watch"
    if score>=50: return "degraded"
    return "critical"


@router.callback_query(F.data=="admin:diagnostics")
async def admin_diagnostics(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    snap=await asyncio.to_thread(observability_snapshot); runtime=runtime_state_snapshot(); pool=DB_POOL.stats()
    score=int(snap.get("health_score") or 0); status=_health_status(score)
    reasons=snap.get("health_reasons") or []; reason_text="\n".join(f"• {html.escape(str(x))}" for x in reasons) or "• No active degradation signal"
    restarts=runtime.get("worker_restarts") or {}; restart_text=", ".join(f"{k}:{v}" for k,v in sorted(restarts.items())) or "none"
    text=(f"🩺 <b>Ultra Diagnostics</b>\n\n💚 Health: <b>{score}/100 • {status.upper()}</b>\n"
          f"🌐 Role: <b>{html.escape(str(runtime.get('role','unknown')).upper())}</b>\n"
          f"🗄 DB pool: <b>{pool['created']}/{pool['max']}</b> • idle {pool['idle']} • waits {pool['waits']}\n"
          f"♻️ Worker restarts: <code>{html.escape(restart_text)}</code>\n"
          f"🕒 Last check: <code>{html.escape(str(runtime.get('last_observability_check') or 'n/a'))}</code>\n\n"
          f"<b>Signals</b>\n{reason_text}\n\n🔔 Due queue: <b>{snap.get('due_notifications',0)}</b> • failed <b>{snap.get('failed_notifications',0)}</b>\n"
          f"💳 Aged pending payments: <b>{snap.get('aged_pending_payments',0)}</b>\n💸 Refund pending: <b>{snap.get('refund_pending',0)}</b>\n🚨 Errors 15m: <b>{snap.get('errors15',0)}</b>")
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:diagnostics"),InlineKeyboardButton(text="⬅️ Ops",callback_data="admin:ops")]]))


@router.callback_query(F.data=="admin:ops")
async def admin_ops_center(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    row = await asyncio.to_thread(db_execute, """SELECT
        (SELECT COUNT(*) FROM payments WHERE status='pending') pending_payments,
        (SELECT COUNT(*) FROM orders WHERE status='pending') pending_orders,
        (SELECT COUNT(*) FROM orders WHERE status='refund_pending') refund_pending,
        (SELECT COUNT(*) FROM notification_queue WHERE status='pending') queue_pending,
        (SELECT COUNT(*) FROM notification_queue WHERE status='failed') queue_failed,
        (SELECT COUNT(*) FROM error_events WHERE created_at>=NOW()-INTERVAL '24 hours') errors24,
        (SELECT COUNT(*) FROM order_status_audit WHERE changed_at>=NOW()-INTERVAL '24 hours') transitions24,
        (SELECT COUNT(*) FROM security_events WHERE created_at>=NOW()-INTERVAL '24 hours') security24,
        (SELECT COUNT(*) FROM ops_archive) archived
    """, (), "one") or {}
    pool = DB_POOL.stats(); runtime = runtime_state_snapshot()
    text=(f"🧭 <b>V10 Operations Center</b>\n\n"
          f"🌐 Role: <b>{html.escape(str(runtime.get('role','unknown')).upper())}</b> • Instance: <code>{html.escape(INSTANCE_ID)}</code>\n"
          f"💚 Health: <b>{runtime.get('health_score',100)}/100 • {html.escape(str(runtime.get('health_status','starting')).upper())}</b>\n"
          f"🗄 DB pool: <b>{pool['created']}/{pool['max']}</b> created • <b>{pool['idle']}</b> idle • waits <b>{pool['waits']}</b>\n\n"
          f"💳 Pending payments: <b>{row.get('pending_payments',0)}</b>\n"
          f"📦 Pending manual orders: <b>{row.get('pending_orders',0)}</b>\n"
          f"💸 Refund pending: <b>{row.get('refund_pending',0)}</b>\n"
          f"🔔 Queue: <b>{row.get('queue_pending',0)}</b> pending • <b>{row.get('queue_failed',0)}</b> failed\n"
          f"🚨 Errors (24h): <b>{row.get('errors24',0)}</b>\n"
          f"🔄 Order transitions (24h): <b>{row.get('transitions24',0)}</b>\n"
          f"🔐 Security events (24h): <b>{row.get('security24',0)}</b>\n"
          f"🗄 Archived ops rows: <b>{row.get('archived',0)}</b>")
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚨 Recent Errors",callback_data="admin:ops_errors"),InlineKeyboardButton(text="🔄 Order Audit",callback_data="admin:ops_orders")],
        [InlineKeyboardButton(text="🩺 Ultra Diagnostics",callback_data="admin:diagnostics")],
        [InlineKeyboardButton(text="🧪 Deployment Check",callback_data="admin:deploy_check"),InlineKeyboardButton(text="🗄 Run Safe Archive",callback_data="admin:ops_archive")],
        [InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:ops"),InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]
    ])
    await c.answer(); await c.message.edit_text(text,reply_markup=kb)


@router.callback_query(F.data=="admin:ops_errors")
async def admin_ops_errors(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute("SELECT error_id,scope,error_type,message,created_at FROM error_events ORDER BY created_at DESC LIMIT 8",fetch="all") or []
    if not rows:
        text="🚨 <b>Recent Errors</b>\n\nNo recorded runtime errors."
    else:
        text="🚨 <b>Recent Runtime Errors</b>\n\n"+"\n\n".join(
            f"<code>{html.escape(r['error_id'])}</code> • <b>{html.escape(r['error_type'])}</b>\n"
            f"📍 {html.escape(r['scope'])}\n{html.escape((r['message'] or '')[:180])}\n🕒 {r['created_at']}"
            for r in rows
        )
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Ops Center",callback_data="admin:ops")]]))


@router.callback_query(F.data=="admin:ops_orders")
async def admin_ops_order_audit(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute("SELECT order_id,old_status,new_status,payment_mode,operation,changed_at FROM order_status_audit ORDER BY id DESC LIMIT 20",fetch="all") or []
    if not rows:
        text="🔄 <b>Order Status Audit</b>\n\nNo status transitions recorded yet."
    else:
        text="🔄 <b>Order Status Audit</b>\n\n"+"\n".join(
            f"#{r['order_id'] or '-'} • {html.escape(r['old_status'] or 'NEW')} → <b>{html.escape(r['new_status'])}</b> • {html.escape(r['payment_mode'] or '-')} • {r['changed_at']:%m-%d %H:%M}"
            for r in rows
        )
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Ops Center",callback_data="admin:ops")]]))


@router.callback_query(F.data=="admin:deploy_check")
async def admin_deployment_check(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await c.answer(); await c.message.edit_text(deployment_check_text(),reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Recheck",callback_data="admin:deploy_check"),InlineKeyboardButton(text="⬅️ Ops Center",callback_data="admin:ops")]]))


@router.callback_query(F.data=="admin:ops_archive")
async def admin_ops_archive(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await c.answer("Running safe archive...")
    try:
        counts = await asyncio.to_thread(operational_archive_cleanup)
        admin_log(c.from_user.id,"ops_archive",json.dumps(counts,sort_keys=True))
        text=("🗄 <b>Safe Archive Complete</b>\n\n"
              f"🔔 Notification rows: <b>{counts.get('notification_queue',0)}</b>\n"
              f"📝 Admin logs: <b>{counts.get('admin_logs',0)}</b>\n"
              f"🔐 Security events: <b>{counts.get('security_events',0)}</b>\n"
              f"🚨 Error events: <b>{counts.get('error_events',0)}</b>\n\n"
              "Financial orders, payments, balance logs and payment audits are never deleted by this maintenance task.")
    except Exception as exc:
        error_id=record_runtime_error("admin_ops_archive",exc,{"admin_id":c.from_user.id})
        text=f"❌ Archive maintenance failed. Ref: <code>{html.escape(error_id)}</code>"
    await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Ops Center",callback_data="admin:ops")]]))


@router.message(Command("error"))
async def admin_error_lookup(m:Message):
    if not is_admin(m.from_user.id): return await m.answer("Denied")
    parts=(m.text or "").split(maxsplit=1)
    if len(parts)!=2 or not parts[1].strip().upper().startswith("ERR-"):
        return await m.answer("Usage: <code>/error ERR-...</code>")
    error_id=parts[1].strip().upper()[:80]
    row=db_execute("SELECT error_id,instance_id,scope,error_type,message,context_json,created_at FROM error_events WHERE error_id=%s",(error_id,),"one")
    if not row:
        return await m.answer(f"❌ Error reference <code>{html.escape(error_id)}</code> not found (it may have been archived).")
    await m.answer(
        f"🚨 <b>Error Reference</b>\n\n"
        f"ID: <code>{html.escape(row['error_id'])}</code>\n"
        f"Type: <b>{html.escape(row['error_type'])}</b>\n"
        f"Scope: <code>{html.escape(row['scope'])}</code>\n"
        f"Instance: <code>{html.escape(row['instance_id'])}</code>\n"
        f"Message: {html.escape((row['message'] or '')[:800])}\n"
        f"Context: <code>{html.escape((row['context_json'] or '{}')[:1200])}</code>\n"
        f"Time: <code>{row['created_at']}</code>"
    )


@router.callback_query(F.data=="admin:logs")
async def admin_logs(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=db_execute("SELECT * FROM admin_logs ORDER BY id DESC LIMIT 20",fetch="all"); text="📝 No admin logs." if not rows else "📝 <b>Recent Admin Logs</b>\n\n"+"\n".join(f"#{r['id']} • {html.escape(str(r['action']))}\n{html.escape(str(r['details'] or ''))}\n🕒 {r['created_at']}\n" for r in rows); await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]))

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
    term=(term or "").strip()
    pattern=f"%{term}%"
    prefix=f"{term}%"
    rows=await asyncio.to_thread(db_execute,"""
        SELECT p.*, CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock
        FROM products p
        LEFT JOIN (SELECT product_id, COUNT(*) AS available FROM product_codes WHERE status='available' GROUP BY product_id) pc ON pc.product_id=p.id
        WHERE p.active=1 AND (p.name ILIKE %s OR p.category ILIKE %s OR COALESCE(p.description,'') ILIKE %s)
        ORDER BY
          CASE WHEN LOWER(p.name)=LOWER(%s) THEN 0
               WHEN p.name ILIKE %s THEN 1
               WHEN p.category ILIKE %s THEN 2
               WHEN p.name ILIKE %s THEN 3
               ELSE 4 END,
          CASE WHEN (CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END)>0 THEN 0 ELSE 1 END,
          p.id DESC
        LIMIT 24
    """,(pattern,pattern,pattern,term,prefix,prefix,pattern),"all")
    if not rows:
        try:
            u=await aget_user(m.from_user)
            picks=await asyncio.to_thread(smart_recommendations,u["id"],4)
        except Exception:
            picks=[]
        if picks:
            return await m.answer(
                f"🔎 No exact match for <b>{html.escape(term)}</b>.\n\n🎯 <b>You may like these instead:</b>",
                reply_markup=recommendations_kb(picks)
            )
        return await m.answer(
            f"🔎 No products found for <b>{html.escape(term)}</b>.\n\nTry a game name, product name, or shorter keyword.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛍 Browse Shop",callback_data="shop")]])
        )
    kb=[]
    for p in rows:
        stock=int(p['effective_stock'] or 0)
        badge="⚡" if p.get('delivery_type')=='code' and stock>0 else ("🟢" if stock>0 else "🔴")
        kb.append([InlineKeyboardButton(text=f"{badge} {p['name']} • {fmt_money(p['price'])}",callback_data=f"product:{p['id']}")])
    kb.append([InlineKeyboardButton(text="🛍 Browse All",callback_data="shop"),InlineKeyboardButton(text="🏠 Home",callback_data="main_menu")])
    await m.answer(
        f"🔎 <b>Best Matches</b>\n\nFound <b>{len(rows)}</b> result(s) for <code>{html.escape(term)}</code>.\n⚡ Instant-delivery items are highlighted first when relevant.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )


def _build_orders_export():
    rows=db_execute("SELECT o.id,u.tg_id,u.username,p.name,o.game_uid,o.total,o.status,o.created_at,o.updated_at FROM orders o JOIN users u ON u.id=o.user_id JOIN products p ON p.id=o.product_id ORDER BY o.id DESC",fetch="all")
    fd, raw_path = tempfile.mkstemp(prefix="orders_export_", suffix=".csv")
    os.close(fd)
    path=Path(raw_path)
    try:
        with path.open("w",newline="",encoding="utf-8") as f:
            w=csv.writer(f); w.writerow(["order_id","tg_id","username","product","game_uid","total","status","created_at","updated_at"])
            keys = ["id","tg_id","username","name","game_uid","total","status","created_at","updated_at"]
            for r in rows: w.writerow([r[k] for k in keys])
        return path
    except Exception:
        path.unlink(missing_ok=True)
        raise


@router.message(Command("export_orders"))
async def export_orders(m:Message):
    if not is_admin(m.from_user.id): return await m.answer("Denied")
    path = None
    try:
        path = await asyncio.to_thread(_build_orders_export)
        await m.answer_document(FSInputFile(str(path)),caption="📄 Orders export")
    finally:
        if path:
            with contextlib.suppress(OSError):
                path.unlink(missing_ok=True)

@router.message(Command("backup"))
async def manual_backup(m:Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Denied")
    await m.answer("💾 Creating secure database backup…")
    path = None
    try:
        path = await asyncio.to_thread(create_database_backup)
        await asyncio.to_thread(cleanup_old_backups)
        await asyncio.to_thread(admin_log, m.from_user.id, "database_backup", path.name)
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
            await asyncio.to_thread(cleanup_old_backups)
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
    threshold = low_stock_threshold()
    rows=await asyncio.to_thread(db_execute,"""
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
    """,(threshold,),"all")
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


def claim_due_campaigns(limit=5):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """WITH picked AS (
                       SELECT id FROM marketing_campaigns
                       WHERE ((status='scheduled' AND starts_at<=NOW())
                           OR (status='sending' AND COALESCE(claimed_at,created_at) < NOW()-(%s * INTERVAL '1 minute')))
                         AND (ends_at IS NULL OR ends_at>=NOW())
                       ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED
                   )
                   UPDATE marketing_campaigns m
                   SET status='sending',claimed_at=NOW(),claimed_by=%s
                   FROM picked p WHERE m.id=p.id
                   RETURNING m.*""",
                (WORKER_LEASE_MINUTES, limit, INSTANCE_ID),
            )
            return cur.fetchall() or []


def enqueue_campaign_notification(campaign, user, text, buttons):
    payload = json.dumps(buttons or [], ensure_ascii=False)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO marketing_delivery_claims(campaign_id,user_id) VALUES(%s,%s) ON CONFLICT DO NOTHING RETURNING user_id",
                (campaign["id"], user["id"]),
            )
            if not cur.fetchone():
                return False
            cur.execute(
                "INSERT INTO notification_queue(tg_id,text,buttons_json) VALUES(%s,%s,%s)",
                (user["tg_id"], text, payload),
            )
            cur.execute(
                "INSERT INTO marketing_events(campaign_id,user_id,event_type) VALUES(%s,%s,'sent')",
                (campaign["id"], user["id"]),
            )
            return True


async def marketing_campaign_loop(bot):
    await asyncio.sleep(20)
    while True:
        try:
            if marketing_enabled():
                db_execute("UPDATE marketing_campaigns SET status='expired',claimed_at=NULL,claimed_by='' WHERE status IN ('scheduled','sending') AND ends_at IS NOT NULL AND ends_at<NOW()")
                campaigns = claim_due_campaigns(5)
                for campaign in campaigns:
                    recipients = marketing_recipients(campaign["audience"])
                    buttons = marketing_campaign_markup(campaign["id"])
                    coupon_line = f"\n\n🏷️ Coupon: <code>{html.escape(campaign['coupon_code'])}</code>" if campaign["coupon_code"] else ""
                    text = f"📣 <b>{html.escape(campaign['title'])}</b>\n\n{campaign['message']}{coupon_line}"
                    for user in recipients:
                        enqueue_campaign_notification(campaign, user, text, buttons)
                    row = db_execute("SELECT COUNT(*) AS c FROM marketing_delivery_claims WHERE campaign_id=%s", (campaign["id"],), "one")
                    sent = int(row["c"] or 0) if row else 0
                    db_execute(
                        "UPDATE marketing_campaigns SET status='sent',sent_count=%s,sent_at=NOW(),claimed_at=NULL,claimed_by='' WHERE id=%s AND status='sending' AND claimed_by=%s",
                        (sent, campaign["id"], INSTANCE_ID),
                    )
                    admin_log(campaign["created_by"], "marketing_campaign_sent", f"campaign #{campaign['id']} recipients={sent}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            record_runtime_error("marketing_campaign_loop",exc,{"instance_id":INSTANCE_ID})
        await asyncio.sleep(30)


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
    u=await aget_user(m.from_user); rows=cart_rows(u["id"])
    if not rows: await state.clear(); return await m.answer("🛒 Cart is empty.")
    subtotal=sum(int(r["quantity"])*product_sale_price(r) for r in rows)
    await state.update_data(game_uid=uid,subtotal=subtotal)
    await state.set_state(CartState.coupon)
    await m.answer(f"💰 Subtotal: <b>{fmt_money(subtotal)}</b>\n\n🏷️ Send coupon code or <code>SKIP</code>.")

@router.message(CartState.coupon)
async def cart_coupon_step(m:Message,state:FSMContext):
    d=await state.get_data(); u=await aget_user(m.from_user); subtotal=float(d["subtotal"]); code=(m.text or "").strip()
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
    u=await aget_user(m.from_user)
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
    u=await aget_user(m.from_user)
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
    BotCommand(command="error", description="Look up a V10 error reference"),
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
    """Rank in-stock products from purchases, views, favorites and store popularity."""
    try:
        limit=max(1,min(8,int(limit)))
        rows=db_execute("""
            WITH signals AS (
                SELECT p.id AS product_id,
                       COALESCE((SELECT COUNT(*)*12 FROM orders o WHERE o.user_id=%s AND o.product_id=p.id AND o.status='completed'),0) +
                       COALESCE((SELECT COUNT(*)*5 FROM product_views v WHERE v.user_id=%s AND v.product_id=p.id AND v.viewed_at>=NOW()-INTERVAL '30 days'),0) +
                       COALESCE((SELECT COUNT(*)*9 FROM favorites f WHERE f.user_id=%s AND f.product_id=p.id),0) +
                       COALESCE((SELECT COUNT(*)*2 FROM orders op WHERE op.product_id=p.id AND op.status='completed' AND op.created_at>=NOW()-INTERVAL '30 days'),0) AS score
                FROM products p
            ), stock AS (
                SELECT product_id,COUNT(*)::int AS available FROM product_codes WHERE status='available' GROUP BY product_id
            )
            SELECT p.*, CASE WHEN p.delivery_type='code' THEN COALESCE(st.available,0) ELSE p.stock END AS effective_stock,
                   COALESCE(s.score,0) AS recommendation_score
            FROM products p
            LEFT JOIN stock st ON st.product_id=p.id
            LEFT JOIN signals s ON s.product_id=p.id
            WHERE p.active=1
              AND (CASE WHEN p.delivery_type='code' THEN COALESCE(st.available,0) ELSE p.stock END)>0
            ORDER BY COALESCE(s.score,0) DESC, p.id DESC
            LIMIT %s
        """,(user_id,user_id,user_id,limit),"all") or []
        return rows[:limit]
    except Exception as exc:
        record_runtime_error("smart_recommendations",exc,{"user_id":user_id})
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
    u=await aget_user(m.from_user)
    rows=await asyncio.to_thread(smart_recommendations,u["id"],4)
    if not rows:
        return await m.answer("🎁 <b>Smart Offers</b>\n\nNo personalized offer is available right now.", reply_markup=premium_home_kb())
    await m.answer("🎯 <b>Smart Picks For You</b>\n\nBased on your activity and current stock:", reply_markup=recommendations_kb(rows))


@router.callback_query(F.data == "home:offers")
async def smart_offers_callback(c: CallbackQuery):
    u=await aget_user(c.from_user); rows=await asyncio.to_thread(smart_recommendations,u["id"],4)
    await c.answer()
    await c.message.edit_text("🎯 <b>Smart Picks For You</b>\n\nRecommended from your activity and current stock:", reply_markup=recommendations_kb(rows))


@router.callback_query(F.data == "admin:intelligence")
async def admin_intelligence(c: CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    row=await adb_execute("""SELECT
        (SELECT COUNT(*) FROM users) users,
        (SELECT COUNT(*) FROM orders WHERE created_at>=NOW()-INTERVAL '7 days') orders7,
        (SELECT COALESCE(SUM(total),0) FROM orders WHERE status='completed' AND created_at>=NOW()-INTERVAL '7 days') sales7,
        (SELECT COUNT(*) FROM product_views WHERE viewed_at>=NOW()-INTERVAL '24 hours') views24,
        (SELECT COUNT(*) FROM notification_queue WHERE status='pending') queued,
        (SELECT COUNT(*) FROM notification_queue WHERE status='failed') failed_notifications,
        (SELECT COUNT(*) FROM payments WHERE status='pending') pending_payments,
        (SELECT COUNT(*) FROM orders WHERE status='refund_pending') refund_pending
    """, fetch="one")
    top=await adb_execute("""SELECT p.name,COUNT(*) c,COALESCE(SUM(o.total),0) revenue
                      FROM orders o JOIN products p ON p.id=o.product_id
                      WHERE o.status='completed' AND o.created_at>=NOW()-INTERVAL '7 days'
                      GROUP BY p.name ORDER BY revenue DESC,c DESC LIMIT 5""", fetch="all") or []
    risk=await asyncio.to_thread(admin_risk_snapshot)
    tops="\n".join(f"• {html.escape(r['name'])}: <b>{r['c']}</b> • {fmt_money(r['revenue'])}" for r in top) or "• No completed orders yet"
    text=(f"🧠 <b>{html.escape(APP_VERSION)} Intelligence</b>\n\n"
          f"👥 Users: <b>{row['users']}</b>\n📦 Orders (7d): <b>{row['orders7']}</b>\n"
          f"💰 Sales (7d): <b>{fmt_money(row['sales7'])}</b>\n👀 Views (24h): <b>{row['views24']}</b>\n"
          f"💳 Pending payments: <b>{row['pending_payments']}</b> • 💸 Refunds: <b>{row['refund_pending']}</b>\n"
          f"🔔 Queue: <b>{row['queued']}</b> • Failed: <b>{row['failed_notifications']}</b>\n\n"
          f"🛡 Risk level: <b>{risk['level']}</b> • Score <b>{risk['score']}/100</b>\n\n"
          f"🔥 <b>Top Revenue Products — 7d</b>\n{tops}")
    await c.answer(); await c.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛡 Open Risk Radar",callback_data="admin:risk"),InlineKeyboardButton(text="📈 Sales & Stock",callback_data="admin:sales_stock")],
        [InlineKeyboardButton(text=setting("admin_back","⬅️ Admin"), callback_data="admin:dashboard")]
    ]))


def admin_risk_snapshot():
    row=db_execute("""SELECT
      (SELECT COUNT(*) FROM payments WHERE status='pending') pending,
      (SELECT COUNT(*) FROM payments WHERE status='pending' AND created_at < NOW()-(%s * INTERVAL '1 minute')) aged_pending,
      (SELECT COUNT(*) FROM payments WHERE status='pending' AND amount >= %s) high_value_pending,
      (SELECT COUNT(*) FROM payments WHERE status='rejected' AND updated_at>=NOW()-INTERVAL '24 hours') rejected24,
      (SELECT COUNT(*) FROM notification_queue WHERE status='failed') failed_notifications,
      (SELECT COUNT(*) FROM orders WHERE status='refund_pending') refund_pending,
      (SELECT COUNT(*) FROM error_events WHERE created_at>=NOW()-INTERVAL '1 hour') errors1h
    """,(RISK_AGED_PENDING_MINUTES,RISK_HIGH_VALUE_AMOUNT),fetch="one") or {}
    repeat=db_execute("""SELECT COUNT(*) c FROM (
      SELECT user_id FROM payments
      WHERE status='rejected' AND updated_at>=NOW()-INTERVAL '24 hours'
      GROUP BY user_id HAVING COUNT(*) >= %s
    ) x""",(RISK_REPEAT_REJECT_COUNT,),fetch="one") or {"c":0}
    score=min(100,
        int(row.get('aged_pending',0))*8 + int(row.get('high_value_pending',0))*12 +
        int(row.get('rejected24',0))*3 + int(repeat.get('c',0))*15 +
        int(row.get('failed_notifications',0))*5 + int(row.get('refund_pending',0))*6 +
        min(20,int(row.get('errors1h',0))*4))
    level='🟢 LOW' if score < 20 else ('🟡 WATCH' if score < 45 else ('🟠 HIGH' if score < 70 else '🔴 CRITICAL'))
    return {**dict(row), 'repeat_reject_users':int(repeat.get('c',0)), 'score':score, 'level':level}


def admin_flagged_payments(limit=8):
    return db_execute("""SELECT py.id,py.amount,py.method,py.status,py.created_at,u.tg_id,u.name,
      (SELECT COUNT(*) FROM payments p2 WHERE p2.user_id=py.user_id AND p2.status='rejected' AND p2.updated_at>=NOW()-INTERVAL '24 hours') rejects24
      FROM payments py JOIN users u ON u.id=py.user_id
      WHERE py.status='pending' AND (py.amount >= %s OR py.created_at < NOW()-(%s * INTERVAL '1 minute'))
      ORDER BY (py.amount >= %s) DESC,py.created_at ASC LIMIT %s
    """,(RISK_HIGH_VALUE_AMOUNT,RISK_AGED_PENDING_MINUTES,RISK_HIGH_VALUE_AMOUNT,limit),fetch="all") or []


@router.callback_query(F.data == "admin:risk")
async def admin_risk_radar(c: CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    snap,flagged=await asyncio.gather(asyncio.to_thread(admin_risk_snapshot),asyncio.to_thread(admin_flagged_payments,8))
    lines=[
      "🛡 <b>Ultra Risk Radar</b>",f"Risk: <b>{snap['level']}</b> • Score <b>{snap['score']}/100</b>","",
      f"⏳ Aged pending: <b>{snap.get('aged_pending',0)}</b>",f"💎 High-value pending: <b>{snap.get('high_value_pending',0)}</b>",
      f"❌ Rejected payments (24h): <b>{snap.get('rejected24',0)}</b>",f"🔁 Repeat-reject users: <b>{snap.get('repeat_reject_users',0)}</b>",
      f"💸 Refund pending: <b>{snap.get('refund_pending',0)}</b>",f"🔔 Failed notifications: <b>{snap.get('failed_notifications',0)}</b>",
      f"🚨 Runtime errors (1h): <b>{snap.get('errors1h',0)}</b>","","⚠️ <b>Flagged Pending Payments</b>"
    ]
    if flagged:
        for r in flagged:
            age=r['created_at'].strftime('%m-%d %H:%M') if r.get('created_at') else '-'
            lines.append(f"• #{r['id']} • {fmt_money(r['amount'])} • {html.escape(r['method'])} • rejects24={r['rejects24']} • {age}")
    else: lines.append("• No high-risk pending payment right now")
    await c.answer(); await c.message.edit_text("\n".join(lines),reply_markup=InlineKeyboardMarkup(inline_keyboard=[
      [InlineKeyboardButton(text="💳 Review Payments",callback_data="admin:payments"),InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:risk")],
      [InlineKeyboardButton(text="⬅️ Intelligence",callback_data="admin:intelligence")]
    ]))


def admin_sales_stock_snapshot():
    sales=db_execute("""SELECT p.id,p.name,COUNT(*) orders,COALESCE(SUM(o.total),0) revenue
      FROM orders o JOIN products p ON p.id=o.product_id
      WHERE o.status='completed' AND o.created_at>=NOW()-INTERVAL '24 hours'
      GROUP BY p.id,p.name ORDER BY revenue DESC,orders DESC LIMIT 8""",fetch="all") or []
    stock=db_execute("""SELECT p.id,p.name,p.delivery_type,
      CASE WHEN p.delivery_type='code' THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available') ELSE p.stock END available
      FROM products p WHERE p.active=1 AND
      (CASE WHEN p.delivery_type='code' THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available') ELSE p.stock END) <= %s
      ORDER BY available ASC,p.id ASC LIMIT 12""",(low_stock_threshold(),),fetch="all") or []
    return sales,stock


@router.callback_query(F.data == "admin:sales_stock")
async def admin_sales_stock(c: CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    sales,stock=await asyncio.to_thread(admin_sales_stock_snapshot)
    sales_text="\n".join(f"• {html.escape(r['name'])}: <b>{r['orders']}</b> orders • {fmt_money(r['revenue'])}" for r in sales) or "• No completed sales in the last 24h"
    stock_text="\n".join(f"• {html.escape(r['name'])}: <b>{r['available']}</b> left" for r in stock) or "• Stock levels healthy"
    await c.answer(); await c.message.edit_text(
      f"📈 <b>Sales & Stock Command Center</b>\n\n🔥 <b>Top Sellers — 24h</b>\n{sales_text}\n\n⚠️ <b>Low/Critical Stock</b>\n{stock_text}",
      reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛍 Products",callback_data="admin:products"),InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:sales_stock")],[InlineKeyboardButton(text="⬅️ Intelligence",callback_data="admin:intelligence")]])
    )


_ADMIN_INTEL_ALERT_STATE={"signature":"","sent_at":0.0}


async def admin_intelligence_alert_loop(bot):
    while True:
        try:
            snap=await asyncio.to_thread(admin_risk_snapshot)
            _,stock=await asyncio.to_thread(admin_sales_stock_snapshot)
            critical_stock=sum(1 for r in stock if int(r.get('available') or 0) <= 0)
            signal=(snap['level'],snap.get('high_value_pending',0),snap.get('aged_pending',0),snap.get('repeat_reject_users',0),snap.get('failed_notifications',0),critical_stock)
            signature=json.dumps(signal,separators=(',',':'))
            now=time.monotonic()
            should_alert=(snap['score']>=45 or critical_stock>0 or snap.get('failed_notifications',0)>0)
            if should_alert and (signature != _ADMIN_INTEL_ALERT_STATE['signature'] or now-_ADMIN_INTEL_ALERT_STATE['sent_at']>=RISK_ALERT_COOLDOWN_SECONDS):
                text=(f"🛡 <b>Ultra Admin Alert</b>\nRisk: <b>{snap['level']}</b> • Score <b>{snap['score']}/100</b>\n"
                      f"💎 High-value pending: <b>{snap.get('high_value_pending',0)}</b> • ⏳ Aged: <b>{snap.get('aged_pending',0)}</b>\n"
                      f"🔁 Repeat-reject users: <b>{snap.get('repeat_reject_users',0)}</b> • 🔔 Failed notifications: <b>{snap.get('failed_notifications',0)}</b>\n"
                      f"📦 Out-of-stock active products: <b>{critical_stock}</b>")
                kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛡 Risk Radar",callback_data="admin:risk"),InlineKeyboardButton(text="📈 Sales & Stock",callback_data="admin:sales_stock")]])
                for admin_id in ADMIN_IDS:
                    try: await bot.send_message(admin_id,text,reply_markup=kb)
                    except Exception: perf_inc("errors")
                _ADMIN_INTEL_ALERT_STATE.update(signature=signature,sent_at=now)
            await asyncio.sleep(900)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            record_runtime_error("admin_intelligence_alert_loop",exc,{"instance_id":INSTANCE_ID})
            await asyncio.sleep(900)


def intelligence_daily_cleanup():
    db_execute("DELETE FROM product_views WHERE viewed_at < NOW()-INTERVAL '90 days'")
    db_execute("DELETE FROM intelligence_events WHERE created_at < NOW()-INTERVAL '180 days'")


# V9.0 Preflight MAX: strict startup integrity checks before polling.
def startup_preflight():
    required = ("Buy", "DirectPaymentState", "SearchState", "PaymentState", "AdminState", "CartState", "HealthHandler", "start_health_server", "stop_health_server", "_feature_on", "admin_ultra", "vip_tier", "claim_notification_batch", "notification_queue_loop", "cleanup_expired_transactions", "intelligence_setup", "smart_recommendations", "admin_intelligence", "admin_risk_snapshot", "admin_risk_radar", "admin_sales_stock", "admin_intelligence_alert_loop", "assess_payment_fraud", "refresh_payment_fraud", "payment_review_clear", "payment_evidence_snapshot", "payment_evidence_view", "buyer_payment_view", "payment_issue_create", "support_center_snapshot", "admin_support_center", "admin_support_case", "support_case_resolve", "support_case_reopen", "support_case_alert_loop", "cart_markup", "coupon_discount", "claim_due_campaigns", "enqueue_campaign_notification", "marketing_campaign_loop", "marketing_create_campaign", "marketing_abandoned_cart_job", "marketing_record_conversion", "payment_method_specs", "payment_method_keyboard", "show_deposit_start", "admin_payment_methods", "admin_autopilot", "normalize_trx_id", "release_direct_order_reservation", "record_payment_audit", "security_log", "decrypt_order_credential", "direct_refund_complete", "payment_amount_limits_ok", "order_pay_wallet", "order_pay_direct", "direct_payment_method", "direct_payment_trx", "performance_health_snapshot", "performance_maintenance_loop", "operational_archive_cleanup", "deployment_self_check", "record_runtime_error", "invalidate_setting_cache", "startup_reconcile_operations", "validate_backup_snapshot", "validate_latest_backup_snapshot", "observability_snapshot", "self_heal_safe_operations", "admin_diagnostics")
    missing = [name for name in required if name not in globals()]
    if missing:
        raise RuntimeError("Preflight MAX failed; missing: " + ", ".join(missing))
    forbidden = [name for name in globals() if name.endswith("KeyboardMarkup") and name != "InlineKeyboardMarkup"]
    if forbidden:
        raise RuntimeError("Preflight MAX failed: non-inline keyboard markup detected: " + ", ".join(forbidden))
    if not APP_VERSION.startswith("V10.20"):
        raise RuntimeError("Preflight MAX failed: APP_VERSION mismatch: " + APP_VERSION)
    notify_src = inspect.getsource(notify_user)
    if "_queue_buttons_from_markup" not in notify_src or "enqueue_notification" not in notify_src:
        raise RuntimeError("Preflight MAX failed: durable buyer notification fallback missing")
    delivery_src = inspect.getsource(manual_delivery_note)
    if "await notify_user" not in delivery_src or "queued for automatic retry" not in delivery_src:
        raise RuntimeError("Preflight MAX failed: manual delivery durable notification guard missing")
    release_src = inspect.getsource(release_direct_order_reservation)
    if "fail closed" not in release_src or "Unknown reservation kind" not in release_src:
        raise RuntimeError("Preflight MAX failed: fail-closed reservation release guard missing")
    buy_uid_src = inspect.getsource(buy_uid)
    if "Insufficient balance" in buy_uid_src or 'float(u["balance"])<float(p["price"])' in buy_uid_src:
        raise RuntimeError("Preflight MAX failed: Buy.uid blocks Direct Payment when wallet is insufficient")
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
    if set(BACKUP_TABLES) != set(MANAGED_TABLES):
        raise RuntimeError("Preflight MAX failed: backup table registry drift")
    required_data_tables = {"product_views", "intelligence_events", "smart_offers", "payment_support_cases"}
    if not required_data_tables.issubset(set(BACKUP_TABLES)):
        raise RuntimeError("Preflight MAX failed: V10.17 managed-data backup coverage missing")
    integrity_src = inspect.getsource(database_integrity_check)
    if "MANAGED_TABLES" not in integrity_src or "information_schema.tables" not in integrity_src:
        raise RuntimeError("Preflight MAX failed: V10.17 integrity registry guard missing")
    low_stock_src = inspect.getsource(notify_low_stock)
    if "asyncio.to_thread" not in low_stock_src:
        raise RuntimeError("Preflight MAX failed: V10.17 low-stock DB offload missing")
    buy_src = inspect.getsource(buy)
    if "if is_auto_code_product(p):" not in buy_src or "return await order_confirm(c,state)" not in buy_src:
        raise RuntimeError("Preflight MAX failed: V10.4 instant-code fast checkout missing")
    wallet_src = inspect.getsource(order_pay_wallet)
    if "_buyer_checkout_lock" not in wallet_src or "already processed or expired" not in wallet_src:
        raise RuntimeError("Preflight MAX failed: V10.5 wallet duplicate-click guard missing")
    confirm_src = inspect.getsource(order_confirm)
    if "order:change_qty" not in confirm_src:
        raise RuntimeError("Preflight MAX failed: V10.4 quantity escape hatch missing")
    cleanup_src = inspect.getsource(cleanup_expired_transactions)
    if "Payment Expired" not in cleanup_src or "Buy Again" not in cleanup_src:
        raise RuntimeError("Preflight MAX failed: V10.6 expired-payment recovery notification missing")
    automation_src = inspect.getsource(automation_loop)
    if "to_thread(cleanup_expired_transactions)" not in automation_src:
        raise RuntimeError("Preflight MAX failed: V10.6 cleanup offload missing")
    credential_src = inspect.getsource(order_credential_reveal)
    if "c.bot.send_message" not in credential_src or "chat_id=c.from_user.id" not in credential_src or "c.message.answer" in credential_src:
        raise RuntimeError("Preflight MAX failed: V10.8 private-only credential reveal guard missing")
    if "to_thread(decrypt_order_credential" not in credential_src:
        raise RuntimeError("Preflight MAX failed: V10.8 credential decrypt offload missing")
    search_src = inspect.getsource(run_product_search)
    if "asyncio.to_thread" not in search_src or "You may like these instead" not in search_src:
        raise RuntimeError("Preflight MAX failed: V10.11 smart-search recovery missing")
    recommend_src = inspect.getsource(smart_recommendations)
    if "product_views" not in recommend_src or "favorites" not in recommend_src or "recommendation_score" not in recommend_src:
        raise RuntimeError("Preflight MAX failed: V10.11 personalized recommendation signals missing")
    risk_src = inspect.getsource(admin_risk_snapshot)
    if "high_value_pending" not in risk_src or "repeat_reject_users" not in risk_src or "failed_notifications" not in risk_src:
        raise RuntimeError("Preflight MAX failed: V10.12 risk-radar signals missing")
    alert_src = inspect.getsource(admin_intelligence_alert_loop)
    if "RISK_ALERT_COOLDOWN_SECONDS" not in alert_src or "admin:risk" not in alert_src:
        raise RuntimeError("Preflight MAX failed: V10.12 controlled admin alert loop missing")
    fraud_src = inspect.getsource(assess_payment_fraud)
    credit_src = inspect.getsource(payment_credit)
    review_src = inspect.getsource(payment_review_clear)
    if "FRAUD_REVIEW_SCORE" not in fraud_src or "velocity" not in fraud_src or "recent_rejects" not in fraud_src:
        raise RuntimeError("Preflight MAX failed: V10.13 fraud scoring signals missing")
    if "review_required" not in credit_src or "Manual review required first" not in credit_src:
        raise RuntimeError("Preflight MAX failed: V10.13 manual-review approval gate missing")
    evidence_src=inspect.getsource(payment_evidence_view)
    if "evidence completeness" not in evidence_src.lower() or "not automatic payment verification" not in evidence_src.lower(): raise RuntimeError("Preflight MAX failed: V10.14 evidence disclaimer missing")
    buyer_trust_src=inspect.getsource(buyer_payment_view)
    if "Report Payment Issue" not in buyer_trust_src or "does not itself confirm payment" not in buyer_trust_src: raise RuntimeError("Preflight MAX failed: V10.14 buyer trust flow missing")
    if "review_cleared_at=NOW()" not in review_src or "fraud_review_cleared" not in review_src:
        raise RuntimeError("Preflight MAX failed: V10.13 review-clear audit path missing")
    support_src=inspect.getsource(admin_support_center)
    transition_src=inspect.getsource(_support_case_transition)
    support_alert_src=inspect.getsource(support_case_alert_loop)
    if "Aged 30m+" not in support_src or "support_case:" not in support_src:
        raise RuntimeError("Preflight MAX failed: V10.15 support-center dashboard missing")
    if "status='open'" not in transition_src or "status='resolved'" not in transition_src or "enqueue_notification" not in transition_src:
        raise RuntimeError("Preflight MAX failed: V10.15 idempotent support transition/notification missing")
    if "30 minutes" not in support_alert_src or "RISK_ALERT_COOLDOWN_SECONDS" not in support_alert_src:
        raise RuntimeError("Preflight MAX failed: V10.15 aged-support alert guard missing")
    health_src = inspect.getsource(_HealthHandler.do_GET)
    if 'path == "/ready"' not in health_src or "bootstrap_complete" not in health_src or "shop_name()" in health_src.split('if path == "/health":',1)[1].split('if path == "/ready":',1)[0]:
        raise RuntimeError("Preflight MAX failed: V10.17 liveness/readiness isolation missing")
    backup_src = inspect.getsource(create_database_backup)
    if "os.replace" not in backup_src or "Backup validation failed" not in backup_src or ".tmp" not in backup_src:
        raise RuntimeError("Preflight MAX failed: V10.17 atomic validated backup guard missing")
    main_src = inspect.getsource(main)
    if "STARTUP_DB_RETRY_LIMIT" not in main_src or "retry budget exhausted" not in main_src or "bootstrap_complete=True" not in main_src:
        raise RuntimeError("Preflight MAX failed: V10.17 bounded bootstrap/readiness guard missing")
    export_src = inspect.getsource(export_orders)
    lowstock_src = inspect.getsource(lowstock)
    if "asyncio.to_thread" not in export_src or "asyncio.to_thread" not in lowstock_src:
        raise RuntimeError("Preflight MAX failed: V10.17 admin hot-path offload missing")
    recovery_src = inspect.getsource(startup_reconcile_operations)
    if "ambiguous_pending_payments" not in recovery_src or "notification_leases_requeued" not in recovery_src or "orphan_code_holds_released" not in recovery_src:
        raise RuntimeError("Preflight MAX failed: V10.18 startup reconciliation guard missing")
    queue_src = inspect.getsource(notification_queue_loop)
    if "asyncio.to_thread(claim_notification_batch" not in queue_src:
        raise RuntimeError("Preflight MAX failed: V10.18 notification queue async replay guard missing")
    backup_validate_src = inspect.getsource(validate_backup_snapshot)
    if "MANAGED_TABLES" not in backup_validate_src or "malformed table payloads" not in backup_validate_src:
        raise RuntimeError("Preflight MAX failed: V10.18 backup restore-validation guard missing")

    obs_src=inspect.getsource(observability_snapshot); heal_src=inspect.getsource(self_heal_safe_operations); diag_src=inspect.getsource(admin_diagnostics)
    if "health_score" not in obs_src or "stale_notification_leases" not in obs_src:
        raise RuntimeError("Preflight MAX failed: V10.20 health scoring regression")
    if "notification_leases_requeued" not in heal_src or "status='sending'" not in heal_src:
        raise RuntimeError("Preflight MAX failed: V10.20 safe self-heal regression")
    if "Ultra Diagnostics" not in diag_src or "worker_restarts" not in diag_src:
        raise RuntimeError("Preflight MAX failed: V10.20 diagnostics regression")
    backup_validate_src = inspect.getsource(validate_backup_snapshot)
    if "MANAGED_TABLES" not in backup_validate_src or "MANAGED_DB_TABLES" in backup_validate_src:
        raise RuntimeError("Preflight MAX failed: V10.20 backup validator registry mismatch")
    if set(BACKUP_TABLES) != set(MANAGED_TABLES):
        raise RuntimeError("Preflight MAX failed: V10.20 backup registry drift")
    print("PREFLIGHT_MAX_OK: V10.20 release-candidate production checks passed", flush=True)
    perf_inc("requests", 1)


def try_acquire_leader_lock():
    conn = _new_raw_db_connection(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtext(%s)::bigint) AS locked", (LEADER_LOCK_NAME,))
            row = cur.fetchone()
        if row and row["locked"]:
            runtime_state_update(role="leader", leader=True, leader_since=now_text(), last_leader_heartbeat=now_text())
            return conn
    except Exception:
        conn.close()
        raise
    conn.close()
    return None


def release_leader_lock(conn):
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s)::bigint)", (LEADER_LOCK_NAME,))
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass


async def leader_heartbeat_loop(conn):
    while True:
        await asyncio.sleep(LEADER_HEARTBEAT_SECONDS)
        try:
            def _ping():
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            await asyncio.to_thread(_ping)
            runtime_state_update(last_leader_heartbeat=now_text())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            runtime_state_update(role="leader_lost", leader=False)
            raise RuntimeError("Leader database session lost") from exc


async def _cancel_tasks(tasks):
    for task in tasks:
        if task and not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def run_leader_runtime(leader_conn):
    bot = Bot(TOKEN,default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    background = []
    polling_task = None
    heartbeat_task = None
    try:
        await setup_bot_commands(bot)
        # Keep Telegram updates queued across leader failover; do not discard customer actions.
        await bot.delete_webhook(drop_pending_updates=False)

        async def low_stock_loop():
            while True:
                try:
                    await notify_low_stock(bot)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    perf_inc("errors")
                await asyncio.sleep(600)

        async def autopilot_health_loop():
            while True:
                try:
                    await asyncio.to_thread(db_execute, "SELECT 1")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    perf_inc("errors")
                await asyncio.sleep(1800)

        async def intelligence_cleanup_loop():
            while True:
                try:
                    await asyncio.to_thread(intelligence_daily_cleanup)
                    archive_counts = await asyncio.to_thread(operational_archive_cleanup)
                    if any(archive_counts.values()):
                        logging.info(json.dumps({"event":"ops_archive_cleanup","instance_id":INSTANCE_ID,"counts":archive_counts},sort_keys=True))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    record_runtime_error("daily_cleanup", exc, {"instance_id": INSTANCE_ID})
                await asyncio.sleep(86400)

        worker_factories = {
            "low-stock": low_stock_loop,
            "db-backup": lambda: automatic_backup_loop(bot),
            "notification-worker": lambda: notification_queue_loop(bot),
            "automation-worker": automation_loop,
            "marketing-worker": lambda: marketing_campaign_loop(bot),
            "autopilot-health": autopilot_health_loop,
            "intelligence-cleanup": intelligence_cleanup_loop,
            "admin-intelligence-alerts": lambda: admin_intelligence_alert_loop(bot),
            "support-case-alerts": lambda: support_case_alert_loop(bot),
            "performance-maintenance": performance_maintenance_loop,
        }
        workers={name:asyncio.create_task(factory(),name=name) for name,factory in worker_factories.items()}
        background=list(workers.values())

        async def observability_supervisor_loop():
            alert_signature=None; last_alert=0.0
            while True:
                try:
                    for name,task in list(workers.items()):
                        if task.done() and not task.cancelled():
                            exc=task.exception(); record_runtime_error("background_worker_exit",exc or RuntimeError(f"{name} stopped"),{"worker":name})
                            replacement=asyncio.create_task(worker_factories[name](),name=name); workers[name]=replacement; background.append(replacement)
                            state=runtime_state_snapshot(); restarts=dict(state.get("worker_restarts") or {}); restarts[name]=int(restarts.get(name,0))+1; runtime_state_update(worker_restarts=restarts)
                    healed=await asyncio.to_thread(self_heal_safe_operations); snap=await asyncio.to_thread(observability_snapshot)
                    score=int(snap.get("health_score") or 0); reasons=list(snap.get("health_reasons") or []); status=_health_status(score)
                    prev=runtime_state_snapshot().get("last_self_heal")
                    runtime_state_update(health_score=score,health_status=status,health_reasons=reasons,last_observability_check=now_text(),last_self_heal=now_text() if any(healed.values()) else prev)
                    signature=(status,tuple(reasons),tuple(sorted((runtime_state_snapshot().get("worker_restarts") or {}).items()))); now_m=time.monotonic()
                    if score<HEALTH_DEGRADED_SCORE and (signature!=alert_signature or now_m-last_alert>=OBSERVABILITY_ALERT_COOLDOWN_SECONDS):
                        reason_text="\n".join(f"• {html.escape(str(x))}" for x in reasons[:6]) or "• Health score degraded"
                        for admin_id in ADMIN_IDS:
                            try: await bot.send_message(admin_id,f"🩺 <b>V10.19 Health Alert</b>\n\nScore: <b>{score}/100 • {status.upper()}</b>\n{reason_text}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🩺 Diagnostics",callback_data="admin:diagnostics")]]))
                            except Exception: pass
                        alert_signature=signature; last_alert=now_m
                except asyncio.CancelledError: raise
                except Exception as exc: record_runtime_error("observability_supervisor",exc,{"instance_id":INSTANCE_ID})
                await asyncio.sleep(OBSERVABILITY_INTERVAL_SECONDS)

        supervisor_task=asyncio.create_task(observability_supervisor_loop(),name="observability-supervisor"); background.append(supervisor_task)

        if ADMIN_WEB_TOKEN:
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f"🌐 <b>{APP_VERSION} Web Admin enabled</b>\n"
                        f"Instance: <code>{html.escape(INSTANCE_ID)}</code> • Role: <b>LEADER</b>\n"
                        "Open: <code>/admin?token=YOUR_ADMIN_WEB_TOKEN</code> on your Render URL.\n"
                        "💾 Use /backup for a manual database backup."
                    )
                except Exception:
                    pass

        polling_task = asyncio.create_task(dp.start_polling(bot), name="telegram-polling")
        heartbeat_task = asyncio.create_task(leader_heartbeat_loop(leader_conn), name="leader-heartbeat")
        done, _ = await asyncio.wait({polling_task, heartbeat_task}, return_when=asyncio.FIRST_COMPLETED)
        if heartbeat_task in done:
            exc = heartbeat_task.exception()
            if exc:
                raise exc
            raise RuntimeError("Leader heartbeat stopped unexpectedly")
        # Polling ended (normally due to shutdown signal).
        if polling_task in done:
            exc = polling_task.exception()
            if exc:
                raise exc
    finally:
        await _cancel_tasks([heartbeat_task, polling_task] + background)
        with contextlib.suppress(Exception):
            await bot.session.close()


async def main():
    leader_conn = None
    start_health_server()
    try:
        bootstrap_attempt = 0
        while True:
            bootstrap_attempt += 1
            try:
                await asyncio.to_thread(init_db)
                await asyncio.to_thread(DB_POOL.prewarm)
                await asyncio.to_thread(database_integrity_check)
                await asyncio.to_thread(_load_settings_cache)
                await asyncio.to_thread(intelligence_setup)
                runtime_state_update(bootstrap_complete=True, db_bootstrap_attempts=bootstrap_attempt)
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                runtime_state_update(role="db_unavailable", leader=False, bootstrap_complete=False, db_bootstrap_attempts=bootstrap_attempt)
                if bootstrap_attempt >= STARTUP_DB_RETRY_LIMIT:
                    logging.exception("Database bootstrap failed after %s attempt(s); aborting startup", bootstrap_attempt)
                    raise RuntimeError(f"Database bootstrap retry budget exhausted after {bootstrap_attempt} attempt(s)") from exc
                logging.exception("Database bootstrap failed; retry %s/%s: %s", bootstrap_attempt, STARTUP_DB_RETRY_LIMIT, exc)
                await asyncio.sleep(LEADER_RETRY_SECONDS)

        startup_preflight()
        deploy_report = deployment_self_check()
        runtime_state_update(deployment_ok=deploy_report["ok"], deployment_warnings=deploy_report["warnings"], deployment_critical_failures=deploy_report["critical_failures"])
        if not deploy_report["ok"]:
            failed = ", ".join(x["name"] for x in deploy_report["checks"] if x["severity"]=="critical" and not x["ok"])
            raise RuntimeError("Deployment self-check failed: " + failed)
        if deploy_report["warnings"]:
            logging.warning(json.dumps({"event":"deployment_warnings","instance_id":INSTANCE_ID,"warnings":[x["name"] for x in deploy_report["checks"] if x["severity"]=="warning" and not x["ok"]]}))
        print(f"STARTUP_PREFLIGHT_OK {APP_VERSION} instance={INSTANCE_ID}", flush=True)
        await asyncio.to_thread(database_integrity_check)
        await asyncio.to_thread(cleanup_expired_transactions)
        recovery_counts = await asyncio.to_thread(startup_reconcile_operations)
        latest_backup = await asyncio.to_thread(validate_latest_backup_snapshot)
        await asyncio.to_thread(reconcile_all_code_stock)
        runtime_state_update(
            role="follower", leader=False,
            recovery_complete=True, recovery_counts=recovery_counts,
            latest_backup_validation=latest_backup,
        )
        if recovery_counts.get("ambiguous_pending_payments"):
            logging.error(json.dumps({"event":"startup_recovery_ambiguous_payments","instance_id":INSTANCE_ID,"counts":recovery_counts}, sort_keys=True))

        while leader_conn is None:
            try:
                leader_conn = try_acquire_leader_lock()
            except Exception as exc:
                runtime_state_update(role="db_unavailable", leader=False)
                logging.exception("Leader election failed: %s", exc)
            if leader_conn is None:
                runtime_state_update(role="follower", leader=False)
                await asyncio.sleep(LEADER_RETRY_SECONDS)

        print(f"LEADER_ACQUIRED instance={INSTANCE_ID}", flush=True)
        await run_leader_runtime(leader_conn)
    finally:
        runtime_state_update(role="stopping", leader=False)
        release_leader_lock(leader_conn)
        stop_health_server()
        DB_POOL.closeall()


if __name__=="__main__":
    asyncio.run(main())
