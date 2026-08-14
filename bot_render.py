import os
import io
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
import ast
import queue
import re
import math
import collections
import hashlib
import contextlib
import difflib
import uuid
import traceback
import weakref
from urllib.parse import urlparse, parse_qs
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from pathlib import Path
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from dotenv import load_dotenv
from psycopg import connect, errors
from psycopg.rows import dict_row

from aiogram import Bot, Dispatcher, Router, F
from aiogram import BaseMiddleware
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, BotCommand, BotCommandScopeDefault, BotCommandScopeChat, ReplyKeyboardRemove,
    InputMediaPhoto
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
    bulk_products = State()
    bulk_edit_products = State()
    game_add = State()
    game_edit = State()
    game_logo = State()
    offer_create = State()
    template_create = State()
    template_target = State()
    order_search = State()
    payment_search = State()
    segment_broadcast = State()
    support_ticket_message = State()
    support_ticket_note = State()
    delivery_template_create = State()
    edit_product = State()
    add_codes = State()
    balance = State()
    broadcast = State()
    settings = State()
    marketing_create = State()
    manual_delivery_note = State()
    support_note = State()
    auto_topup_uid_test = State()
    auto_topup_map = State()
    admin_product_search = State()
    admin_product_quick_price = State()
    admin_product_quick_stock = State()
    crm_search = State()
    crm_note = State()
    crm_followup = State()

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
CURRENCY = os.getenv("CURRENCY", "BDT")
PAYMENT_INFO = os.getenv("PAYMENT_INSTRUCTIONS", "bKash/Nagad: YOUR NUMBER")
SUPPORT = os.getenv("SUPPORT_USERNAME", "@YourSupport")
ADMIN_WEB_TOKEN = os.getenv("ADMIN_WEB_TOKEN", "").strip()
FEATURE_EFOOTBALL_COINS = True
APP_VERSION = "V11 PHASE 6J.3 • SECURE LOGIN SUPPORT • 2-COLUMN PRODUCTS"
AUTO_DB_BACKUP_HOURS = max(0, int(os.getenv("AUTO_DB_BACKUP_HOURS", "24") or "24"))
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", "/tmp/next_level_backups"))
CREDENTIAL_SECRET_ENV = os.getenv("CREDENTIAL_SECRET", "").strip()
CREDENTIAL_SECRET = CREDENTIAL_SECRET_ENV or TOKEN
CREDENTIAL_SECRET_FALLBACK = not bool(CREDENTIAL_SECRET_ENV)
CREDENTIAL_SECRET_PREVIOUS = os.getenv("CREDENTIAL_SECRET_PREVIOUS", "").strip()
CREDENTIAL_SECRET_REQUIRED = os.getenv("CREDENTIAL_SECRET_REQUIRED", "0").strip().lower() in {"1","true","yes","on"}
ADMIN_READONLY_IDS = {int(x.strip()) for x in os.getenv("ADMIN_READONLY_IDS", "").split(",") if x.strip().isdigit()} & ADMIN_IDS
SECURITY_RATE_LIMIT_LOG_COOLDOWN_SECONDS = max(60, int(os.getenv("SECURITY_RATE_LIMIT_LOG_COOLDOWN_SECONDS", "300") or "300"))
CREDENTIAL_REVEAL_SECONDS = max(15, min(300, int(os.getenv("CREDENTIAL_REVEAL_SECONDS", "60") or "60")))
DB_POOL_MIN = max(0, int(os.getenv("DB_POOL_MIN", "1") or "1"))
DB_POOL_MAX = max(DB_POOL_MIN or 1, int(os.getenv("DB_POOL_MAX", "8") or "8"))
DB_POOL_WAIT_SECONDS = max(1.0, float(os.getenv("DB_POOL_WAIT_SECONDS", "8") or "8"))
RATE_LIMIT_MESSAGES = max(3, int(os.getenv("RATE_LIMIT_MESSAGES", "12") or "12"))
RATE_LIMIT_CALLBACKS = max(5, int(os.getenv("RATE_LIMIT_CALLBACKS", "25") or "25"))
RATE_LIMIT_WINDOW_SECONDS = max(1.0, float(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "4") or "4"))
INSTANCE_ID = (os.getenv("RAILWAY_REPLICA_ID") or os.getenv("RAILWAY_DEPLOYMENT_ID") or os.getenv("HOSTNAME") or f"pid-{os.getpid()}")[:120]
LEADER_LOCK_NAME = os.getenv("LEADER_LOCK_NAME", "next_level_gaming_shop:telegram_poller:v10").strip() or "next_level_gaming_shop:telegram_poller:v10"
LEADER_RETRY_SECONDS = max(3.0, float(os.getenv("LEADER_RETRY_SECONDS", "10") or "10"))
LEADER_HEARTBEAT_SECONDS = max(3.0, float(os.getenv("LEADER_HEARTBEAT_SECONDS", "10") or "10"))
WORKER_LEASE_MINUTES = max(1, int(os.getenv("WORKER_LEASE_MINUTES", "5") or "5"))
OPS_ARCHIVE_NOTIFICATION_DAYS = max(7, int(os.getenv("OPS_ARCHIVE_NOTIFICATION_DAYS", "30") or "30"))
OPS_ARCHIVE_LOG_DAYS = max(30, int(os.getenv("OPS_ARCHIVE_LOG_DAYS", "180") or "180"))
OPS_ARCHIVE_BATCH = max(50, min(5000, int(os.getenv("OPS_ARCHIVE_BATCH", "500") or "500")))
ERROR_EVENT_RETENTION_DAYS = max(30, int(os.getenv("ERROR_EVENT_RETENTION_DAYS", "180") or "180"))
RAILWAY_HEARTBEAT_SECONDS = max(60, int(os.getenv("RAILWAY_HEARTBEAT_SECONDS", "300") or "300"))
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
SELF_HEAL_NOTIFICATION_MAX_ATTEMPTS = max(5, min(20, int(os.getenv("SELF_HEAL_NOTIFICATION_MAX_ATTEMPTS", "8") or "8")))
SELF_HEAL_NOTIFICATION_BATCH = max(1, min(100, int(os.getenv("SELF_HEAL_NOTIFICATION_BATCH", "20") or "20")))
SELF_HEAL_ALERT_COOLDOWN_SECONDS = max(300, int(os.getenv("SELF_HEAL_ALERT_COOLDOWN_SECONDS", "1800") or "1800"))
SELF_HEAL_BACKUP_STALE_FACTOR = max(1.5, min(6.0, float(os.getenv("SELF_HEAL_BACKUP_STALE_FACTOR", "2.5") or "2.5")))
HEALTH_DEGRADED_SCORE = max(40, min(95, int(os.getenv("HEALTH_DEGRADED_SCORE", "75") or "75")))
ORDER_RECOVERY_PENDING_MINUTES = max(10, int(os.getenv("ORDER_RECOVERY_PENDING_MINUTES", "30") or "30"))
ORDER_RECOVERY_PROVIDER_MINUTES = max(5, int(os.getenv("ORDER_RECOVERY_PROVIDER_MINUTES", "15") or "15"))
ORDER_RECOVERY_REFUND_MINUTES = max(30, int(os.getenv("ORDER_RECOVERY_REFUND_MINUTES", "120") or "120"))
ORDER_RECOVERY_ALERT_COOLDOWN_SECONDS = max(900, int(os.getenv("ORDER_RECOVERY_ALERT_COOLDOWN_SECONDS", "3600") or "3600"))

if not TOKEN:
    raise RuntimeError("BOT_TOKEN missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing. Add the PostgreSQL connection string in Railway Variables.")
if not ADMIN_IDS:
    raise RuntimeError("ADMIN_IDS missing")
if CREDENTIAL_SECRET_FALLBACK and CREDENTIAL_SECRET_REQUIRED:
    raise RuntimeError("CREDENTIAL_SECRET missing while CREDENTIAL_SECRET_REQUIRED=1")
if CREDENTIAL_SECRET_FALLBACK:
    logging.warning("CREDENTIAL_SECRET is not set; BOT_TOKEN fallback is active. Set a dedicated CREDENTIAL_SECRET in Railway before rotating BOT_TOKEN.")
if CREDENTIAL_SECRET_PREVIOUS and CREDENTIAL_SECRET_PREVIOUS == CREDENTIAL_SECRET:
    logging.warning("CREDENTIAL_SECRET_PREVIOUS matches CREDENTIAL_SECRET; remove the duplicate previous secret.")

DB_LOCK = threading.RLock()
BUYER_CHECKOUT_LOCKS = weakref.WeakValueDictionary()

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
    archived INTEGER NOT NULL DEFAULT 0,
    description TEXT NOT NULL DEFAULT '',
    image_file_id TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops_health_history(
    id BIGSERIAL PRIMARY KEY,
    instance_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'unknown',
    health_score INTEGER NOT NULL DEFAULT 0,
    db_latency_ms NUMERIC(12,2) NOT NULL DEFAULT 0,
    queue_pending INTEGER NOT NULL DEFAULT 0,
    queue_failed INTEGER NOT NULL DEFAULT 0,
    errors15 INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ops_health_history_time ON ops_health_history(created_at DESC);

CREATE TABLE IF NOT EXISTS loyalty_profiles(
    user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    tier TEXT NOT NULL DEFAULT 'Bronze',
    points BIGINT NOT NULL DEFAULT 0,
    lifetime_spend NUMERIC(16,2) NOT NULL DEFAULT 0,
    completed_orders BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS autotopup_provider_products(
    id BIGSERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    provider_product_id TEXT NOT NULL,
    game_key TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL DEFAULT '',
    cost NUMERIC(16,4),
    currency TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 1,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(provider,provider_product_id)
);
CREATE INDEX IF NOT EXISTS idx_autotopup_provider_products_game
ON autotopup_provider_products(provider,game_key,active,name);

CREATE TABLE IF NOT EXISTS autotopup_product_map(
    product_id BIGINT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    provider_variant_code TEXT NOT NULL,
    provider_product_code TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    input_schema JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_autotopup_product_map_provider
ON autotopup_product_map(provider,enabled,provider_variant_code);

CREATE TABLE IF NOT EXISTS autotopup_orders(
    id BIGSERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    order_id BIGINT,
    provider_order_id TEXT NOT NULL DEFAULT '',
    provider_product_id TEXT NOT NULL DEFAULT '',
    game_uid TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'created',
    amount NUMERIC(16,4),
    currency TEXT NOT NULL DEFAULT '',
    request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_autotopup_orders_status
ON autotopup_orders(provider,status,created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_autotopup_orders_provider_order
ON autotopup_orders(provider,order_id)
WHERE order_id IS NOT NULL;


CREATE TABLE IF NOT EXISTS customer_support_tickets(
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    category TEXT NOT NULL,
    priority TEXT NOT NULL DEFAULT 'normal',
    status TEXT NOT NULL DEFAULT 'open',
    message TEXT NOT NULL DEFAULT '',
    admin_note TEXT NOT NULL DEFAULT '',
    assigned_admin BIGINT,
    resolved_at TIMESTAMPTZ,
    resolved_by BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_customer_support_status_priority
ON customer_support_tickets(status,priority,updated_at DESC);

CREATE TABLE IF NOT EXISTS delivery_templates(
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    body TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_by BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS game_catalog(
    game_key TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    emoji TEXT NOT NULL DEFAULT '🎮',
    image_file_id TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 100,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_game_catalog_active_order
ON game_catalog(active,sort_order,display_name);

CREATE TABLE IF NOT EXISTS flash_sales(
    id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    sale_price NUMERIC(14,2) NOT NULL CHECK(sale_price>0),
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_by BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK(ends_at>starts_at)
);
CREATE INDEX IF NOT EXISTS idx_flash_sales_product_time ON flash_sales(product_id,active,starts_at,ends_at);

CREATE TABLE IF NOT EXISTS product_templates(
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    source_category TEXT NOT NULL,
    items_json TEXT NOT NULL,
    created_by BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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

CREATE TABLE IF NOT EXISTS order_events(
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    status TEXT,
    message TEXT NOT NULL DEFAULT '',
    actor_tg_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_order_events_order_time ON order_events(order_id,created_at,id);

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

CREATE TABLE IF NOT EXISTS crm_customer_notes(
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    note TEXT NOT NULL,
    created_by BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_crm_customer_notes_user_time ON crm_customer_notes(user_id,created_at DESC);

CREATE TABLE IF NOT EXISTS crm_followups(
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    message TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    created_by BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    queued_at TIMESTAMPTZ,
    sent_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_crm_followups_user_time ON crm_followups(user_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_crm_followups_status_time ON crm_followups(status,created_at DESC);

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
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS provider_name TEXT NOT NULL DEFAULT ''")
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS provider_invoice_id TEXT NOT NULL DEFAULT ''")
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS provider_transaction_id TEXT NOT NULL DEFAULT ''")
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS provider_checkout_url TEXT NOT NULL DEFAULT ''")
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS provider_verified_at TIMESTAMPTZ")
                cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS provider_payload JSONB")
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_payments_provider_invoice ON payments(provider_name,provider_invoice_id) WHERE provider_name<>'' AND provider_invoice_id<>''")
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_payments_provider_trx ON payments(provider_name,provider_transaction_id) WHERE provider_name<>'' AND provider_transaction_id<>''")
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
                cur.execute("ALTER TABLE game_catalog ADD COLUMN IF NOT EXISTS image_file_id TEXT NOT NULL DEFAULT \'\'")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS archived INTEGER NOT NULL DEFAULT 0")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_products_archived_active ON products(archived,active,id DESC)")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS sale_price NUMERIC(14,2)")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS sale_until TIMESTAMPTZ")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS featured INTEGER NOT NULL DEFAULT 0")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS hot INTEGER NOT NULL DEFAULT 0")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS best_seller INTEGER NOT NULL DEFAULT 0")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS merch_rank INTEGER NOT NULL DEFAULT 100")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS stock_alert_level INTEGER NOT NULL DEFAULT -1")
                cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS stock_alerted_at TIMESTAMPTZ")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_products_stock_alert ON products(active,stock_alert_level)")
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
                cur.execute("ALTER TABLE error_events ADD COLUMN IF NOT EXISTS severity TEXT NOT NULL DEFAULT 'error'")
                cur.execute("ALTER TABLE error_events ADD COLUMN IF NOT EXISTS resolved BOOLEAN NOT NULL DEFAULT FALSE")
                cur.execute("ALTER TABLE error_events ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ")
                cur.execute("ALTER TABLE error_events ADD COLUMN IF NOT EXISTS resolved_by BIGINT")
                cur.execute("ALTER TABLE error_events ADD COLUMN IF NOT EXISTS fingerprint TEXT NOT NULL DEFAULT ''")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_error_events_open_severity ON error_events(resolved,severity,created_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_error_events_fingerprint_time ON error_events(fingerprint,created_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_order_status_audit_order_time ON order_status_audit(order_id,changed_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_order_status_audit_status_time ON order_status_audit(new_status,changed_at DESC)")
                cur.execute("ALTER TABLE ops_archive ALTER COLUMN source_id TYPE TEXT USING source_id::text")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ops_archive_type_time ON ops_archive(archive_type,archived_at DESC)")
                defaults = {
                    "shop_name": "Next Level Gaming Shop",
                    "support": SUPPORT,
                    "support_whatsapp_url": os.getenv("WHATSAPP_SUPPORT_URL", "").strip(),
                    "support_telegram_url": os.getenv("TELEGRAM_SUPPORT_URL", "").strip(),
                    "payment_info": PAYMENT_INFO,
                    "maintenance": "0",
                    "low_stock_threshold": "3",
            "announcement": "🔥 <b>Welcome to Next Level Gaming Shop!</b> ⚡ Fast delivery • 🛡️ Secure payments • ⭐ Premium rewards",
            "theme_preset": "blue",
            "currency": "BDT",
            "usdt_enabled": "1",
            "usdt_bdt_rate": os.getenv("USDT_BDT_RATE", "120"),
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
            "payment_binance_label": "USDT / Binance",
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
            "payment_binance_instruction": "Send the exact USDT amount shown for this order to the Binance/USDT wallet below.",
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
        self._last_security_log = {}
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
            last_log=self._last_security_log.get(key,0.0)
            if now-last_log >= SECURITY_RATE_LIMIT_LOG_COOLDOWN_SECONDS:
                self._last_security_log[key]=now
                try:
                    await asyncio.to_thread(security_log,"rate_limit_triggered",user.id,None,f"event={type(event).__name__}; limit={limit}; window={self.window}")
                except Exception:
                    pass
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
            self._last_security_log = {k:v for k,v in self._last_security_log.items() if now-v < 86400}
            self._last_cleanup = now
        return await handler(event, data)


def _is_benign_telegram_error(exc):
    """Telegram no-op edits are not operational failures."""
    return isinstance(exc, TelegramBadRequest) and "message is not modified" in str(exc).casefold()


class ErrorBoundaryMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        runtime_state_update(last_telegram_activity=now_text(), last_telegram_event=type(event).__name__)
        try:
            return await handler(event, data)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _is_benign_telegram_error(exc):
                if isinstance(event, CallbackQuery):
                    try:
                        await event.answer()
                    except Exception:
                        pass
                return None
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


class AdminMutationGuardMiddleware(BaseMiddleware):
    _MUTATION_PREFIXES = (
        "pay_credit:", "pay_reject:", "order_reject:", "order_refund_complete:",
        "payment_review_clear:", "pdelete_confirm:", "user_toggle:",
        "admin_autotopup_toggle:", "admin_autotopup_map_save:", "admin_offer_apply:",
    )
    _MUTATION_EXACT = {"admin:toggle_maintenance"}

    async def __call__(self, handler, event, data):
        user=getattr(event,"from_user",None)
        raw=(getattr(event,"data",None) or "")
        if user and is_admin(user.id) and admin_is_readonly(user.id):
            blocked = raw in self._MUTATION_EXACT or any(raw.startswith(p) for p in self._MUTATION_PREFIXES)
            if blocked:
                try:
                    await asyncio.to_thread(security_log,"readonly_admin_mutation_denied",user.id,None,raw[:120])
                except Exception:
                    pass
                try:
                    await event.answer("Read-only admin: this action is not permitted.",show_alert=True)
                except Exception:
                    pass
                return None
        return await handler(event,data)


router = Router()
router.message.outer_middleware(ErrorBoundaryMiddleware())
router.callback_query.outer_middleware(ErrorBoundaryMiddleware())
router.message.outer_middleware(RateLimitMiddleware(RATE_LIMIT_MESSAGES, RATE_LIMIT_WINDOW_SECONDS))
router.callback_query.outer_middleware(SafeCallbackMiddleware())
router.callback_query.outer_middleware(AdminMutationGuardMiddleware())
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
        db_execute("INSERT INTO users(tg_id,balance) VALUES(%s,0) ON CONFLICT (tg_id) DO NOTHING",(tg_id,))
    except Exception as exc:
        logging.exception("ensure_buyer_account failed for %s: %s",tg_id,exc)
    return db_execute("SELECT id,tg_id,balance FROM users WHERE tg_id=%s",(tg_id,),"one")

def is_admin(tg_id):
    return tg_id in ADMIN_IDS

def admin_is_readonly(tg_id):
    return int(tg_id) in ADMIN_READONLY_IDS

def admin_can(tg_id, permission="read"):
    if not is_admin(tg_id):
        return False
    if permission in {"mutate", "financial", "sensitive"} and admin_is_readonly(tg_id):
        return False
    return True

def mask_sensitive(value, keep=4):
    text = str(value or "")
    if not text:
        return ""
    if len(text) <= keep:
        return "*" * len(text)
    return "*" * max(4, len(text)-keep) + text[-keep:]

def _redact_security_value(value, key=""):
    key_l = str(key or "").casefold()
    if any(tok in key_l for tok in ("password","secret","token","credential","api_key","apikey")):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(k): _redact_security_value(v, str(k)) for k,v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact_security_value(v, key) for v in value]
    return value


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
        (admin_id, action, str(_redact_security_value(details, "details"))[:1000]),
    )

async def aadmin_log(admin_id, action, details=""):
    """Write admin audit events without blocking the Telegram event loop."""
    return await asyncio.to_thread(admin_log, admin_id, action, details)


def security_log(event_type, admin_id=None, order_id=None, details=""):
    db_execute(
        "INSERT INTO security_events(admin_tg_id,event_type,order_id,details) VALUES(%s,%s,%s,%s)",
        (admin_id, event_type, order_id, str(_redact_security_value(details, "details"))[:1000]),
    )


def _new_error_id():
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"ERR-{stamp}-{uuid.uuid4().hex[:8].upper()}"



def classify_runtime_error(scope, exc):
    """Conservative severity classifier; never auto-resolves real failures."""
    scope_s = str(scope or "unknown").casefold()
    error_type = type(exc).__name__
    message = str(exc or "").casefold()

    if _is_benign_telegram_error(exc):
        return "benign"
    if error_type in {"TimeoutError", "ConnectionError"}:
        return "warning"
    if any(key in scope_s for key in ("payment", "refund", "credential", "database_bootstrap", "startup")):
        return "critical"
    if any(key in message for key in ("undefinedcolumn", "does not exist", "preflight", "database pool exhausted")):
        return "critical"
    if error_type in {"TelegramNetworkError", "TelegramRetryAfter"}:
        return "warning"
    return "error"


def runtime_error_fingerprint(scope, exc):
    """Group similar failures without exposing secrets."""
    base = f"{str(scope or 'unknown').casefold()}|{type(exc).__name__}|{str(exc or '')[:240].casefold()}"
    return hashlib.sha256(base.encode("utf-8", "ignore")).hexdigest()[:24]



def record_runtime_error(scope, exc, context=None):
    """Persist a safe operational error reference without exposing secrets to users."""
    error_id = _new_error_id()
    safe_context = _redact_security_value(context if isinstance(context, dict) else {"context": str(context or "")})
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
        severity = classify_runtime_error(scope, exc)
        fingerprint = runtime_error_fingerprint(scope, exc)
        db_execute(
            """INSERT INTO error_events
               (error_id,instance_id,scope,error_type,message,context_json,traceback_text,severity,fingerprint)
               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                error_id, INSTANCE_ID, event["scope"], event["error_type"], event["message"],
                json.dumps(safe_context, ensure_ascii=False, default=str)[:4000], trace,
                severity, fingerprint,
            ),
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


def _decrypt_order_credential_with_secret(order_id, secret):
    return db_execute(
        "SELECT CASE "
        "WHEN account_password LIKE 'enc:v1:%%' THEN pgp_sym_decrypt(decode(substr(account_password,8),'base64'),%s) "
        "ELSE account_password END AS credential "
        "FROM orders WHERE id=%s AND status='pending'",
        (secret, order_id),
        "one",
    )

def decrypt_order_credential(order_id):
    secrets=[("current", CREDENTIAL_SECRET)]
    if CREDENTIAL_SECRET_PREVIOUS and CREDENTIAL_SECRET_PREVIOUS != CREDENTIAL_SECRET:
        secrets.append(("previous", CREDENTIAL_SECRET_PREVIOUS))
    last_exc=None
    for label,secret in secrets:
        try:
            row=_decrypt_order_credential_with_secret(order_id,secret)
            if not row:
                return None, "Order is not awaiting manual delivery."
            credential=(row.get("credential") or "").strip()
            if not credential:
                return None, "No credential is stored for this order."
            if label == "previous":
                try:
                    security_log("credential_decrypt_previous_secret", None, order_id, "Previous credential secret used; rotate pending credential when operationally safe.")
                except Exception:
                    pass
            return credential, ""
        except Exception as exc:
            last_exc=exc
            continue
    if last_exc:
        logging.error("Credential decrypt failed for order #%s with configured secret set: %s", order_id, last_exc)
    return None, "Credential could not be decrypted. Check CREDENTIAL_SECRET / CREDENTIAL_SECRET_PREVIOUS."


async def _delete_sensitive_message_later(bot, chat_id, message_id, delay=None):
    await asyncio.sleep(delay or CREDENTIAL_REVEAL_SECONDS)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


MANAGED_TABLES = (
    "users", "products", "product_templates", "game_catalog", "flash_sales", "ops_health_history", "order_events", "loyalty_profiles", "autotopup_provider_products", "autotopup_product_map", "autotopup_orders", "customer_support_tickets", "delivery_templates", "product_codes", "orders", "payments",
    "payment_trx_claims", "payment_receipts", "payment_support_cases", "payment_audit",
    "security_events", "error_events", "order_status_audit", "ops_archive", "balance_logs",
    "admin_logs", "settings", "favorites", "notification_queue", "cart_items", "coupons",
    "coupon_uses", "marketing_campaigns", "marketing_delivery_claims", "marketing_events",
    "product_views", "intelligence_events", "smart_offers", "crm_customer_notes", "crm_followups"
)
BACKUP_TABLES = MANAGED_TABLES



def set_backup_health(status, *, filename="", error=""):
    """Persist backup health across Railway restarts."""
    now = now_text()
    db_execute(
        """INSERT INTO settings(key,value) VALUES
           ('ops_backup_last_status',%s),
           ('ops_backup_last_at',%s),
           ('ops_backup_last_file',%s),
           ('ops_backup_last_error',%s)
           ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value""",
        (str(status)[:40], now, str(filename)[:250], str(error)[:500]),
    )


def backup_health_snapshot():
    keys = ["ops_backup_last_status","ops_backup_last_at","ops_backup_last_file","ops_backup_last_error"]
    rows = db_execute(
        "SELECT key,value FROM settings WHERE key = ANY(%s)",
        (keys,),
        "all",
    ) or []
    data = {r["key"]: r["value"] for r in rows}
    return {
        "status": data.get("ops_backup_last_status", "unknown"),
        "last_at": data.get("ops_backup_last_at", "never"),
        "last_file": data.get("ops_backup_last_file", ""),
        "last_error": data.get("ops_backup_last_error", ""),
        "auto_hours": AUTO_DB_BACKUP_HOURS,
        "keep_count": BACKUP_KEEP_COUNT,
    }



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
        set_backup_health("ok", filename=path.name)
        return path
    except Exception as exc:
        with contextlib.suppress(Exception):
            set_backup_health("failed", error=str(exc))
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


def stock_alert_level(stock):
    """Return the crossed alert threshold. Lower numbers are more urgent."""
    stock = max(0, int(stock or 0))
    if stock == 0:
        return 0
    if stock <= 1:
        return 1
    if stock <= 3:
        return 3
    if stock <= 5:
        return 5
    return -1


def stock_alert_candidates():
    """Read + maintain stock alert state. Financial/order state is never touched."""
    rows = db_execute("""
        SELECT p.id,p.name,p.category,p.delivery_type,p.stock,p.stock_alert_level,
               CASE WHEN p.delivery_type='code'
                    THEN (SELECT COUNT(*) FROM product_codes pc
                          WHERE pc.product_id=p.id AND pc.status='available')
                    ELSE p.stock END AS effective_stock
        FROM products p
        WHERE p.active=1
        ORDER BY p.id
    """, fetch="all") or []

    alerts = []
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                for r in rows:
                    stock = max(0, int(r["effective_stock"] or 0))
                    current = stock_alert_level(stock)
                    previous = int(r.get("stock_alert_level") if r.get("stock_alert_level") is not None else -1)

                    if current == -1:
                        if previous != -1:
                            cur.execute(
                                "UPDATE products SET stock_alert_level=-1 WHERE id=%s",
                                (r["id"],),
                            )
                        continue

                    # First entry into low stock, or a move to a more urgent threshold.
                    if previous == -1 or current < previous:
                        alerts.append({
                            "id": int(r["id"]),
                            "name": r["name"],
                            "category": r["category"],
                            "stock": stock,
                            "level": current,
                        })
                    elif current > previous:
                        # Restocked but still within low-stock range. Reset baseline silently.
                        cur.execute(
                            "UPDATE products SET stock_alert_level=%s WHERE id=%s",
                            (current, r["id"]),
                        )
    return alerts


def mark_stock_alerted(product_ids, level_map):
    if not product_ids:
        return
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                for pid in product_ids:
                    cur.execute(
                        "UPDATE products SET stock_alert_level=%s,stock_alerted_at=NOW() WHERE id=%s",
                        (int(level_map[pid]), int(pid)),
                    )


async def notify_low_stock(bot):
    alerts = await asyncio.to_thread(stock_alert_candidates)
    if not alerts:
        return

    severity = {0:"🚨 OUT", 1:"🔴 CRITICAL", 3:"🟠 VERY LOW", 5:"🟡 LOW"}
    lines = [
        f"{severity.get(a['level'],'⚠️ LOW')} • {html.escape(a['name'])} — <b>{a['stock']}</b> left"
        for a in alerts[:20]
    ]
    text_msg = (
        "📦 <b>Smart Stock Alert</b>\n\n"
        + "\n".join(lines)
        + "\n\nAlerts only fire when stock crosses 5 → 3 → 1 → 0."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📈 Sales & Stock",callback_data="admin:sales_stock"),
        InlineKeyboardButton(text="🛍 Products",callback_data="admin:products"),
    ]])

    delivered = False
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text_msg, reply_markup=kb)
            delivered = True
        except Exception as exc:
            record_runtime_error("smart_stock_alert_delivery", exc, {"admin_id":admin_id})

    if delivered:
        level_map = {a["id"]: a["level"] for a in alerts}
        await asyncio.to_thread(mark_stock_alerted, list(level_map), level_map)


def usdt_enabled():
    return setting("usdt_enabled", "1") == "1"

def usdt_bdt_rate():
    """Admin-controlled BDT value of 1 USDT. Internal accounting remains BDT."""
    try:
        rate = float(setting("usdt_bdt_rate", "120"))
    except Exception:
        rate = 120.0
    return max(0.01, rate)

def bdt_to_usdt(value):
    return float(value) / usdt_bdt_rate()

def fmt_bdt(value):
    amount = float(value)
    return f"৳{amount:,.2f}"

def fmt_usdt(value):
    amount = bdt_to_usdt(value)
    # Keep enough precision for low-price gaming products without visual noise.
    if amount >= 100:
        return f"{amount:,.2f} USDT"
    if amount >= 1:
        return f"{amount:,.3f} USDT"
    return f"{amount:,.4f} USDT"

def fmt_money(value):
    """Buyer/admin display only. Database, wallet, orders and analytics stay BDT."""
    bdt = fmt_bdt(value)
    return f"{bdt}  •  {fmt_usdt(value)}" if usdt_enabled() else bdt


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



_THEME_PRESETS={
    "blue":{"accent":"🔵","gem":"💎","bar":"🟦","name":"Blue Gaming"},
    "purple":{"accent":"🟣","gem":"🔮","bar":"🟪","name":"Purple Elite"},
    "green":{"accent":"🟢","gem":"💚","bar":"🟩","name":"Green Power"},
    "gold":{"accent":"🟡","gem":"🏆","bar":"🟨","name":"Gold VIP"},
    "minimal":{"accent":"⚪","gem":"🎮","bar":"⬜","name":"Minimal"},
}


def buyer_theme():
    key=setting("theme_preset","blue").casefold()
    return _THEME_PRESETS.get(key,_THEME_PRESETS["blue"])



def premium_home_text(u):
    points=int(u.get("loyalty_points") or 0)
    tier,pct,next_points=vip_progress(points)
    spend=float(u.get("lifetime_spend") or 0)
    balance=fmt_money(u["balance"])
    filled=max(0,min(10,pct//10))
    theme=buyer_theme()
    bar=theme["bar"]*filled+"⬛"*(10-filled)
    announcement=setting("announcement","").strip()
    name=html.escape(u.get("name") or "Gamer")
    lines=[
        f"{theme['gem']} <b>{html.escape(shop_name())}</b>",
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
    if announcement and _feature_on("feature_announcements"):
        lines.extend(["", "📢 <b>Latest Update</b>", html.escape(announcement)])
    lines.extend(["", f"{theme['accent']} <b>Instant Delivery</b>  •  🛡️ <b>Secure</b>  •  🎁 <b>VIP Rewards</b>"])
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
    rows.append([InlineKeyboardButton(text="🕘 Recently Viewed", callback_data="home:recent")])
    if _feature_on("feature_vip"):
        rows.append([InlineKeyboardButton(text="🎁 Loyalty & VIP", callback_data="home:loyalty")])
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
        [InlineKeyboardButton(text="🎛 Command Center",callback_data="admin:command_center")],
        [InlineKeyboardButton(text=setting("admin_dashboard","📊 Dashboard"),callback_data="admin:dashboard"),
         InlineKeyboardButton(text="📊 Analytics V3",callback_data="admin:analytics_v3:7")],
        [InlineKeyboardButton(text="📦 Stock Intelligence",callback_data="admin:stock_intel"),
         InlineKeyboardButton(text=setting("admin_reports","📈 Reports"),callback_data="admin:reports")],
        [InlineKeyboardButton(text=setting("admin_premium","💎 Premium Analytics"),callback_data="admin:premium")],
        [InlineKeyboardButton(text=setting("admin_marketing","📣 Marketing Center"),callback_data="admin:marketing")],
        [InlineKeyboardButton(text="🧠 Intelligence",callback_data="admin:intelligence"),
         InlineKeyboardButton(text="🛡 Risk Radar",callback_data="admin:risk")],
        [InlineKeyboardButton(text="📈 Sales & Stock",callback_data="admin:sales_stock"),
         InlineKeyboardButton(text="🧭 Ops Center",callback_data="admin:ops")],
        [InlineKeyboardButton(text="🧪 Deploy Check",callback_data="admin:deploy_check")],
        [InlineKeyboardButton(text=setting("admin_orders","🧾 Orders"),callback_data="admin:orders"),
         InlineKeyboardButton(text=setting("admin_payments","💳 Payments"),callback_data="admin:payments")],
        [InlineKeyboardButton(text="💸 Refund Queue",callback_data="admin:refunds"),
         InlineKeyboardButton(text="🧾 Financial Audit",callback_data="admin:financial_audit")],
        [InlineKeyboardButton(text="🧭 Recovery Center",callback_data="admin:recovery")],
        [InlineKeyboardButton(text="💳 Payment Support",callback_data="admin:support"),
         InlineKeyboardButton(text="🎧 Customer Support",callback_data="admin:customer_support")],
        [InlineKeyboardButton(text="💳 Payment Methods",callback_data="admin:payment_methods"),
         InlineKeyboardButton(text="🤖 Auto Pilot",callback_data="admin:autopilot")],
        [InlineKeyboardButton(text="🤖 AUTO TOP-UP",callback_data="admin:auto_topup")],
        [InlineKeyboardButton(text="📝 Delivery Templates",callback_data="admin:delivery_templates"),
         InlineKeyboardButton(text="🎨 Buyer Themes",callback_data="admin:themes")],
        [InlineKeyboardButton(text=setting("admin_users","👥 Users"),callback_data="admin:users"),
         InlineKeyboardButton(text=setting("admin_products","🛍 Products"),callback_data="admin:products")],
        [InlineKeyboardButton(text="🧩 Customer CRM",callback_data="admin:crm")],
        [InlineKeyboardButton(text=setting("admin_codes","🎫 Codes"),callback_data="admin:codes"),
         InlineKeyboardButton(text=setting("admin_balance","💰 Balance"),callback_data="admin:balance")],
        [InlineKeyboardButton(text=setting("admin_broadcast","📢 Broadcast"),callback_data="admin:broadcast"),
         InlineKeyboardButton(text=setting("admin_settings","⚙️ Settings"),callback_data="admin:settings")],
        [InlineKeyboardButton(text=setting("admin_database","📊 Database"),callback_data="admin:dbinfo"),
         InlineKeyboardButton(text=setting("admin_logs","📝 Logs"),callback_data="admin:logs")],
        [InlineKeyboardButton(text=setting("admin_ultra_control","🚀 Ultra Control"),callback_data="admin:ultra")],
    ])


def _split_category(category):
    category = (category or "Other").strip()
    if ">" in category:
        game, pack = category.split(">", 1)
        return game.strip(), pack.strip()
    return category, None


UID_ONLY_GAME_KEYS = {
    "free fire uid auto top-up",
    "free fire indonesia uid top-up",
    "pubg uc uid top-up",
    "free fire level up pass",
}


def is_secure_login_support_product(product):
    """Manual login-based products use payment -> Order ID -> support.
    Explicit UID-only products keep their UID flow. The bot never collects passwords here.
    """
    if not product or str(product.get("delivery_type") or "").strip().lower() != "manual":
        return False
    return not is_uid_only_manual_product(product)


def _telegram_support_url():
    configured = setting("support_telegram_url", "").strip()
    if configured.startswith(("https://", "http://")):
        return configured
    support_value = setting("support", SUPPORT).strip()
    if support_value.startswith("@") and len(support_value) > 1:
        return f"https://t.me/{support_value.lstrip('@')}"
    if support_value.startswith(("https://", "http://")):
        return support_value
    return ""


def _whatsapp_support_url():
    configured = setting("support_whatsapp_url", "").strip()
    return configured if configured.startswith(("https://", "http://")) else ""


def secure_login_support_message(order_id):
    return (
        f"Hello Next Level Gaming Shop, I have completed payment for my game top-up. "
        f"My Order ID is #{int(order_id)}. Please process my order."
    )


def secure_login_support_markup(order_id, *, include_track=True):
    rows = []
    wa = _whatsapp_support_url()
    tg = _telegram_support_url()
    if wa:
        rows.append([InlineKeyboardButton(text="💬 WhatsApp Support", url=wa)])
    if tg:
        rows.append([InlineKeyboardButton(text="✈️ Telegram Support", url=tg)])
    # Always leave a working in-bot support route even if external URLs are not configured.
    rows.append([InlineKeyboardButton(text="🆘 In-Bot Support", callback_data="support_new:delivery")])
    if include_track:
        rows.append([InlineKeyboardButton(text="📦 Track Order", callback_data=f"order_track:{int(order_id)}")])
    rows.append([InlineKeyboardButton(text="🏠 Main Menu", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

STATIC_SHOP_GROUPS = {
    "LIKE-FOLLOW-SUBSCRIBER": [
        "TikTok Coin",
        "TikTok Follower",
        "TikTok Video Like And Views",
        "Facebook Page / Followers",
        "Facebook Profile Meta Verified",
        "YouTube Views And Like",
    ],
}

def product_game_key(product):
    game,_ = _split_category((product or {}).get("category") if isinstance(product,dict) else "")
    return str(game or "").strip().casefold()

def is_uid_only_manual_product(product):
    return bool(product and str(product.get("delivery_type") or "").lower()=="manual" and product_game_key(product) in UID_ONLY_GAME_KEYS)



def game_catalog_rows():
    return db_execute(
        "SELECT game_key,display_name,emoji,image_file_id,sort_order,active FROM game_catalog ORDER BY sort_order,display_name,game_key",
        fetch="all",
    ) or []


def upsert_game_catalog(game_key,display_name=None,emoji="🎮",sort_order=100,active=1):
    game_key=(game_key or "").strip()
    display_name=(display_name or game_key).strip()
    emoji=(emoji or "🎮").strip()[:8]
    if not game_key:
        raise ValueError("Game key cannot be empty.")
    if len(game_key.encode("utf-8"))>48:
        raise ValueError("Game key exceeds 48 UTF-8 bytes.")
    if not display_name or len(display_name)>60:
        raise ValueError("Display name must be 1–60 characters.")
    sort_order=int(sort_order)
    active=1 if int(active) else 0
    db_execute(
        """INSERT INTO game_catalog(game_key,display_name,emoji,sort_order,active)
           VALUES(%s,%s,%s,%s,%s)
           ON CONFLICT(game_key) DO UPDATE SET
             display_name=EXCLUDED.display_name,
             emoji=EXCLUDED.emoji,
             sort_order=EXCLUDED.sort_order,
             active=EXCLUDED.active,
             updated_at=NOW()""",
        (game_key,display_name,emoji,sort_order,active),
    )


def game_meta_map():
    return {r["game_key"]:r for r in game_catalog_rows()}

def game_logo_file_id(game):
    """Return the Telegram file_id configured as the buyer-facing game/category banner."""
    row = db_execute(
        "SELECT image_file_id FROM game_catalog WHERE game_key=%s",
        ((game or "").strip(),),
        "one",
    )
    return str((row or {}).get("image_file_id") or "").strip()


async def _render_callback_surface(c, text, reply_markup, image_file_id=""):
    """Render either a text screen or a photo+caption screen without breaking callback navigation."""
    image_file_id = str(image_file_id or "").strip()
    current_is_photo = bool(getattr(c.message, "photo", None))

    if image_file_id:
        if current_is_photo:
            # Replace the current media with the configured game banner.
            # This matters when Back is pressed from a product that has its own product image.
            try:
                media = InputMediaPhoto(media=image_file_id, caption=text)
                return await c.message.edit_media(media=media, reply_markup=reply_markup)
            except Exception:
                # If Telegram refuses a no-op media edit, a caption edit may still be enough
                # when the current media is already the configured game banner.
                try:
                    return await c.message.edit_caption(caption=text, reply_markup=reply_markup)
                except Exception:
                    pass
        try:
            sent = await c.message.answer_photo(image_file_id, caption=text, reply_markup=reply_markup)
            if not current_is_photo:
                try:
                    await c.message.delete()
                except Exception:
                    pass
            return sent
        except Exception:
            # A stale Telegram file_id must never make the shop unusable.
            pass

    if current_is_photo:
        sent = await c.message.answer(text, reply_markup=reply_markup)
        try:
            await c.message.delete()
        except Exception:
            pass
        return sent
    return await c.message.edit_text(text, reply_markup=reply_markup)


def category_logo_file_id(category):
    if not category or category == "*":
        return ""
    game, _ = _split_category(category)
    return game_logo_file_id(game)

def categories_kb():
    rows=db_execute("SELECT category,COUNT(*) AS c FROM products WHERE active=1 GROUP BY category ORDER BY category",fetch="all") or []
    games={}
    for r in rows:
        game,_=_split_category(r["category"]); games[game]=games.get(game,0)+int(r["c"])
    for game in STATIC_SHOP_GROUPS: games.setdefault(game,0)
    meta=game_meta_map(); sortable=[]
    for game,count in games.items():
        m=meta.get(game)
        if m and not int(m.get("active") or 0): continue
        display=(m.get("display_name") if m else game) or game
        emoji=(m.get("emoji") if m else ("📣" if game=="LIKE-FOLLOW-SUBSCRIBER" else "🎮")) or "🎮"
        order=int(m.get("sort_order") if m else 100)
        sortable.append((order,str(display).casefold(),game,display,emoji,count))
    buttons=[[InlineKeyboardButton(text=f"{emoji} {display}  •  {count}",callback_data=f"game:{game}")] for _,_,game,display,emoji,count in sorted(sortable)]
    buttons.append([InlineKeyboardButton(text="⭐ Featured",callback_data="shop:featured"),InlineKeyboardButton(text="🏆 Popular",callback_data="shop:popular")])
    buttons.append([InlineKeyboardButton(text=setting("inline_all_products","✨ All Products"),callback_data="cat:*")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def game_packs_kb(game):
    rows=db_execute("SELECT category,COUNT(*) AS c FROM products WHERE active=1 AND (category=%s OR category LIKE %s) GROUP BY category ORDER BY category",(game,game+" > %"),"all") or []
    buttons=[]; seen=set()
    for r in rows:
        category=(r["category"] or "").strip(); _,pack=_split_category(category)
        label=f"💎 {pack}  •  {int(r['c'])}" if pack else f"🛍 Products  •  {int(r['c'])}"
        buttons.append([InlineKeyboardButton(text=label,callback_data=f"cat:{category}")]); seen.add(category.casefold())
    for pack in STATIC_SHOP_GROUPS.get(game,[]):
        category=f"{game} > {pack}"
        if category.casefold() not in seen:
            buttons.append([InlineKeyboardButton(text=f"📌 {pack}  •  0",callback_data=f"cat:{category}")])
    buttons.append([InlineKeyboardButton(text=setting("inline_games_back","⬅️ Games"),callback_data="shop")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def products_kb(category="*",page=0,per_page=4):
    offset=page*per_page
    stock_join="LEFT JOIN (SELECT product_id,COUNT(*) AS available FROM product_codes WHERE status='available' GROUP BY product_id) pc ON pc.product_id=p.id"
    archive_guard="COALESCE(p.archived,0)=0"
    if category=="*":
        rows=db_execute(
            f"""SELECT p.*,CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock
                FROM products p {stock_join}
                WHERE p.active=1 AND {archive_guard}
                ORDER BY p.featured DESC,p.hot DESC,p.best_seller DESC,p.merch_rank,p.id DESC
                LIMIT %s OFFSET %s""",(per_page,offset),"all")
        total=(db_execute("SELECT COUNT(*) AS c FROM products WHERE active=1 AND COALESCE(archived,0)=0",fetch="one") or {}).get("c",0)
    else:
        rows=db_execute(
            f"""SELECT p.*,CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock
                FROM products p {stock_join}
                WHERE p.active=1 AND {archive_guard} AND p.category=%s
                ORDER BY p.featured DESC,p.hot DESC,p.best_seller DESC,p.merch_rank,p.id DESC
                LIMIT %s OFFSET %s""",(category,per_page,offset),"all")
        total=(db_execute("SELECT COUNT(*) AS c FROM products WHERE active=1 AND COALESCE(archived,0)=0 AND category=%s",(category,),"one") or {}).get("c",0)
    buttons=[]
    # Buyer product list: 2 products per row (left/right).
    # Tapping a product opens its detail screen, where Buy remains available.
    for i in range(0,len(rows),2):
        pair=rows[i:i+2]
        button_row=[]
        for p in pair:
            raw=str(p["name"]); name=raw[:18]+"…" if len(raw)>19 else raw
            badges=product_merch_badges(p)
            elite_icon="⚡" if is_auto_code_product(p) else ("🆔" if is_uid_only_manual_product(p) else "🔐")
            prefix=(badges+" ") if badges else ""
            button_row.append(
                InlineKeyboardButton(
                    text=f"{prefix}{elite_icon} {name} • {product_button_price(p)}",
                    callback_data=f"product:{p['id']}"
                )
            )
        buttons.append(button_row)
    total_pages=max(1,(int(total)+per_page-1)//per_page)
    buttons.append([
        InlineKeyboardButton(text="⏮",callback_data=f"page:{category}:0"),
        InlineKeyboardButton(text="◀️",callback_data=f"page:{category}:{max(0,page-1)}"),
        InlineKeyboardButton(text=f"✨ {page+1}/{total_pages}",callback_data=f"page:{category}:{page}"),
        InlineKeyboardButton(text="▶️",callback_data=f"page:{category}:{min(total_pages-1,page+1)}"),
        InlineKeyboardButton(text="⏭",callback_data=f"page:{category}:{total_pages-1}")
    ])
    buttons.append([
        InlineKeyboardButton(text="⭐ Elite Picks",callback_data="shop:featured"),
        InlineKeyboardButton(text="🏆 Trending",callback_data="shop:popular")
    ])
    buttons.append([
        InlineKeyboardButton(text=setting("inline_categories","🎮 Games"),callback_data="shop"),
        InlineKeyboardButton(text=setting("button_main_menu","🏠 Home"),callback_data="main_menu")
    ])
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
            body = "OK"
            return self._send(body, 200, "text/plain; charset=utf-8")
        if path == "/ready":
            state = runtime_state_snapshot()
            role = str(state.get("role") or "starting")
            ready = bool(state.get("bootstrap_complete")) and bool(state.get("deployment_ok")) and role not in {"starting", "db_unavailable", "stopping", "leader_lost"}
            body = json.dumps({"ready": ready, "role": role}, ensure_ascii=False)
            return self._send(body, 200 if ready else 503, "application/json; charset=utf-8")
        if path == "/health/details":
            if not self._authorized():
                return self._send("Unauthorized", 401, "text/plain; charset=utf-8")
            body = json.dumps(performance_health_snapshot(), ensure_ascii=False, default=str)
            return self._send(body, 200, "application/json; charset=utf-8")
        if path == "/payments/uddoktapay/return":
            q=parse_qs(urlparse(self.path).query)
            invoice=(q.get("invoice_id") or [""])[0]
            if not invoice:
                return self._send("<h2>Payment received</h2><p>Return to Telegram and tap Check Payment.</p>",400)
            try:
                result=uddoktapay_process_invoice(invoice)
                if result.get("ok"):
                    return self._send("<h2>✅ Payment verified</h2><p>You may return to Telegram. Your bot has processed the payment.</p>",200)
                return self._send("<h2>⏳ Payment pending</h2><p>Return to Telegram and tap Check Payment shortly.</p>",202)
            except Exception as exc:
                record_runtime_error("uddoktapay_return",exc,{"invoice_id":str(invoice)[:120]})
                return self._send("<h2>Payment verification pending</h2><p>Please return to Telegram and try Check Payment.</p>",202)
        if path == "/payments/uddoktapay/cancel":
            return self._send("<h2>Payment cancelled</h2><p>No payment was credited. You may return to Telegram.</p>",200)
        if path in ("/", "/admin"):
            if not self._authorized():
                return self._send("<h2>401 Unauthorized</h2><p>Use ?token=ADMIN_WEB_TOKEN</p>", 401)
            return self._admin_page()
        return self._send("Not found", 404)


    def do_POST(self):
        path=urlparse(self.path).path
        if path=="/webhooks/uddoktapay":
            try:
                length=int(self.headers.get("Content-Length","0") or 0)
                if length<=0 or length>262144:
                    return self._send('{"ok":false,"error":"invalid body"}',400,"application/json; charset=utf-8")
                raw=self.rfile.read(length)
                content_type=(self.headers.get("Content-Type") or "").lower()
                if "application/json" in content_type:
                    payload=json.loads(raw.decode("utf-8"))
                else:
                    form=parse_qs(raw.decode("utf-8","replace"))
                    payload={k:(v[0] if isinstance(v,list) and v else v) for k,v in form.items()}
                invoice=_uddoktapay_extract_invoice(payload)
                if not invoice:
                    return self._send('{"ok":false,"error":"invoice_id missing"}',400,"application/json; charset=utf-8")
                result=uddoktapay_process_invoice(invoice)
                return self._send(json.dumps({"ok":bool(result.get("ok")),"status":result.get("status"),"payment_id":result.get("payment_id")},separators=(",",":")),200,"application/json; charset=utf-8")
            except ValueError as exc:
                record_runtime_error("uddoktapay_webhook_rejected",exc,{"path":path})
                return self._send(json.dumps({"ok":False,"error":str(exc)[:180]}),400,"application/json; charset=utf-8")
            except Exception as exc:
                record_runtime_error("uddoktapay_webhook_error",exc,{"path":path})
                return self._send('{"ok":false,"error":"internal"}',500,"application/json; charset=utf-8")
        if path!="/webhooks/bangjeff":
            return self._send("Not found",404,"text/plain; charset=utf-8")
        cfg=auto_topup_config()
        if not auto_topup_live_armed():
            return self._send('{"ok":false,"error":"auto top-up safety locked"}',503,"application/json; charset=utf-8")
        expected=str(cfg.get("webhook_token") or "")
        supplied=parse_qs(urlparse(self.path).query).get("token",[""])[0]
        if not expected:
            return self._send('{"ok":false,"error":"webhook disabled"}',503,"application/json; charset=utf-8")
        if not supplied or not hmac.compare_digest(supplied,expected):
            return self._send('{"ok":false,"error":"unauthorized"}',401,"application/json; charset=utf-8")
        try:
            length=int(self.headers.get("Content-Length","0") or 0)
            if length<=0 or length>262144:
                return self._send('{"ok":false,"error":"invalid body"}',400,"application/json; charset=utf-8")
            raw=self.rfile.read(length)
            payload=json.loads(raw.decode("utf-8"))
            result=auto_topup_process_webhook(payload)
            _auto_topup_queue_transition_notice(result)
            body=json.dumps({"ok":True,"status":result.get("status"),"order_id":result.get("order_id")},separators=(",",":"))
            return self._send(body,200,"application/json; charset=utf-8")
        except ValueError as exc:
            record_runtime_error("bangjeff_webhook_rejected",exc,{"path":path})
            return self._send(json.dumps({"ok":False,"error":str(exc)[:200]}),404,"application/json; charset=utf-8")
        except Exception as exc:
            record_runtime_error("bangjeff_webhook_error",exc,{"path":path})
            return self._send('{"ok":false,"error":"internal"}',500,"application/json; charset=utf-8")

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
    if was_new:
        await m.answer(
            "✅ <b>Account created successfully!</b>\n"
            f"Welcome to {html.escape(shop_name())}. Your buyer account is now ready."
        )
    await m.answer(premium_home_text(u), reply_markup=premium_home_kb())


@router.message(Command("shop", "listings"))
@router.message(F.text == "🛍️ Shop")
@router.message(F.text == "🛒 Shop")
@router.message(F.text == "🛍️ Premium Shop")
@router.message(F.text == "💎 Shop")
async def shop(m:Message):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id):
        return await m.answer("🚫 Your account is blocked.")
    if maintenance_active() and not is_admin(m.from_user.id):
        return await m.answer(custom_text("maintenance_message","🔧 Shop is temporarily under maintenance. Please try again later."))
    await m.answer(
        custom_text(
            "shop_title",
            "💎 <b>PREMIUM GAME SHOP</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "🎮 Choose a game, or open Featured / Popular picks.\n"
            "🔥 Live offers use the same price at checkout.\n"
            "⚡ Fast delivery  •  🛡️ Secure checkout"
        ),
        reply_markup=categories_kb()
    )

@router.callback_query(F.data == "shop")
async def shop_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id):
        return await c.answer("Shop is under maintenance.", show_alert=True)
    await c.answer()
    return await _render_callback_surface(
        c,
        custom_text(
            "category_title",
            "💎 <b>PRODUCT CATEGORIES</b>\n━━━━━━━━━━━━━━━━━━\n🎮 Choose a category to browse products.\n⚡ Fast delivery  •  ⭐ VIP rewards",
        ),
        categories_kb(),
        "",
    )


@router.callback_query(F.data.startswith("game:"))
async def game_folder_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id):
        return await c.answer("Shop is under maintenance.", show_alert=True)
    game = c.data.split(":", 1)[1]
    rows = await asyncio.to_thread(
        db_execute,
        """SELECT category,COUNT(*) AS c FROM products
           WHERE active=1 AND (category=%s OR category LIKE %s)
           GROUP BY category ORDER BY category""",
        (game, game + " > %"),
        "all",
    ) or []
    exact_count = sum(int(r["c"]) for r in rows if (r["category"] or "").strip() == game)
    has_subpacks = any(_split_category(r["category"])[1] for r in rows)
    meta = await asyncio.to_thread(
        db_execute,
        "SELECT display_name,emoji,image_file_id FROM game_catalog WHERE game_key=%s",
        (game,),
        "one",
    ) or {}
    display = str(meta.get("display_name") or game)
    emoji = str(meta.get("emoji") or "🎮")
    logo = str(meta.get("image_file_id") or "").strip()

    await c.answer()
    if exact_count > 0 and not has_subpacks:
        total_pages = max(1, (exact_count + 3) // 4)
        return await _render_callback_surface(
            c,
            f"{html.escape(emoji)} <b>{html.escape(display)}</b>\n"
            f"📦 <b>{exact_count}</b> package(s) • Page <b>1/{total_pages}</b>\n\n"
            "Choose a package or tap 🛒 Buy.",
            products_kb(game, 0),
            logo,
        )

    return await _render_callback_surface(
        c,
        f"{html.escape(emoji)} <b>{html.escape(display)}</b>\n\n"
        "💎 <i>Select your preferred pack</i>\n\n📂 Choose a pack:",
        game_packs_kb(game),
        logo,
    )


@router.callback_query(F.data.startswith("cat:"))
async def category_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id):
        return await c.answer("Shop is under maintenance.", show_alert=True)
    category = c.data.split(":", 1)[1]
    await c.answer()
    title = "All Products" if category == "*" else category
    total_row = await adb_execute(
        "SELECT COUNT(*) AS c FROM products WHERE active=1 AND (category=%s OR %s='*')",
        (category, category),
        "one",
    )
    total = int(total_row["c"]) if total_row else 0
    return await _render_callback_surface(
        c,
        f"💎 <b>SHOP / LISTINGS</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"📂 <b>{html.escape(title)}</b>\n"
        f"📄 Page <b>1</b> / <b>{max(1,(total+3)//4)}</b>  •  📦 <b>{total}</b> products\n\n"
        "👆 Tap a product for details or use 🛒 Buy Now.",
        products_kb(category, 0),
        await asyncio.to_thread(category_logo_file_id, category),
    )


@router.callback_query(F.data.startswith("page:"))
async def page_callback(c: CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id):
        return await c.answer("Shop is under maintenance.", show_alert=True)
    _, category, page = c.data.split(":", 2)
    page = max(0, int(page))
    total_row = await adb_execute(
        "SELECT COUNT(*) AS c FROM products WHERE active=1 AND (category=%s OR %s='*')",
        (category, category),
        "one",
    )
    total = int(total_row["c"]) if total_row else 0
    per_page = 4
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages - 1)
    title = "All Products" if category == "*" else category
    await c.answer()
    return await _render_callback_surface(
        c,
        f"💎 <b>SHOP / LISTINGS</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"📂 <b>{html.escape(title)}</b>\n"
        f"📄 Page <b>{page+1}</b> / <b>{total_pages}</b>  •  📦 <b>{total}</b> products\n\n"
        "👆 Tap a product for details or use 🛒 Buy Now.",
        products_kb(category, page),
        await asyncio.to_thread(category_logo_file_id, category),
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
            rows = await adb_execute("""SELECT o.id,o.user_id FROM orders o
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
    u = await aget_user(c.from_user)
    await c.answer()
    await c.message.edit_text(premium_home_text(u), reply_markup=premium_home_kb())


ORDER_CENTER_FILTERS = {
    "all": ("All", "", ()),
    "active": ("Active", " AND o.status IN ('awaiting_payment','pending','processing','manual_review','creating','uncertain','refund_pending')", ()),
    "completed": ("Completed", " AND o.status='completed'", ()),
    "issues": ("Issues", " AND o.status IN ('rejected','refunded','expired','cancelled')", ()),
}

async def render_orders_callback(c: CallbackQuery, page: int = 0, status_filter: str = "all"):
    u=await aget_user(c.from_user)
    per_page=5
    status_filter=status_filter if status_filter in ORDER_CENTER_FILTERS else "all"
    label,where_extra,_=ORDER_CENTER_FILTERS[status_filter]
    stats=await adb_execute(
        """SELECT COUNT(*) AS total,
                  COUNT(*) FILTER (WHERE status IN ('awaiting_payment','pending','processing','manual_review','creating','uncertain','refund_pending')) AS active,
                  COUNT(*) FILTER (WHERE status='completed') AS completed,
                  COUNT(*) FILTER (WHERE status IN ('rejected','refunded','expired','cancelled')) AS issues
           FROM orders WHERE user_id=%s""",(u["id"],),"one")
    total_all=int((stats or {}).get("total") or 0)
    if not total_all:
        return await c.message.edit_text("📦 <b>My Orders</b>\n\nYou have no orders yet.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎮 Shop Now",callback_data="home:shop")],[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))
    total_row=await adb_execute("SELECT COUNT(*) AS c FROM orders o WHERE o.user_id=%s"+where_extra,(u["id"],),"one")
    total=int(total_row["c"] or 0) if total_row else 0
    total_pages=max(1,(total+per_page-1)//per_page) if total else 1
    page=min(max(0,int(page)),total_pages-1)
    rows=[]
    if total:
        rows=await adb_execute("SELECT o.id,o.total,o.status,o.created_at,p.name FROM orders o JOIN products p ON p.id=o.product_id WHERE o.user_id=%s"+where_extra+" ORDER BY o.id DESC LIMIT %s OFFSET %s",(u["id"],per_page,page*per_page),"all")
    lines=[
        "📦 <b>ORDER CENTER</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"🔎 View: <b>{html.escape(label)}</b> • 📄 <b>{page+1}/{total_pages}</b>",
        f"📊 All <b>{total_all}</b> • ⏳ Active <b>{int((stats or {}).get('active') or 0)}</b> • ✅ Done <b>{int((stats or {}).get('completed') or 0)}</b> • ⚠️ Issues <b>{int((stats or {}).get('issues') or 0)}</b>",
        ""
    ]
    buttons=[[
        InlineKeyboardButton(text=("• " if status_filter=="all" else "")+"All",callback_data="orders_view:all:0"),
        InlineKeyboardButton(text=("• " if status_filter=="active" else "")+"Active",callback_data="orders_view:active:0"),
    ],[
        InlineKeyboardButton(text=("• " if status_filter=="completed" else "")+"Completed",callback_data="orders_view:completed:0"),
        InlineKeyboardButton(text=("• " if status_filter=="issues" else "")+"Issues",callback_data="orders_view:issues:0"),
    ]]
    if not rows:
        lines.append("No orders in this view.")
    for r in rows:
        friendly=buyer_status_text(r['status'])
        lines.append(f"<b>#{r['id']}</b> • {html.escape(r['name'])}\n{status_emoji(r['status'])} {html.escape(friendly)} • <b>{fmt_money(r['total'])}</b>\n🕒 {r['created_at']}\n")
        buttons.append([InlineKeyboardButton(text=f"{status_emoji(r['status'])} Order #{r['id']} • {html.escape(friendly)}",callback_data=f"order_detail:{r['id']}")])
    if total_pages>1:
        buttons.append([
            InlineKeyboardButton(text="⏮",callback_data=f"orders_view:{status_filter}:0"),
            InlineKeyboardButton(text="◀️",callback_data=f"orders_view:{status_filter}:{max(0,page-1)}"),
            InlineKeyboardButton(text=f"{page+1}/{total_pages}",callback_data=f"orders_view:{status_filter}:{page}"),
            InlineKeyboardButton(text="▶️",callback_data=f"orders_view:{status_filter}:{min(total_pages-1,page+1)}"),
            InlineKeyboardButton(text="⏭",callback_data=f"orders_view:{status_filter}:{total_pages-1}")
        ])
    buttons.append([InlineKeyboardButton(text="🔄 Refresh",callback_data=f"orders_view:{status_filter}:{page}"),InlineKeyboardButton(text="🎮 Shop",callback_data="home:shop")])
    buttons.append([InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")])
    return await c.message.edit_text("\n".join(lines),reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("orders_view:"))
async def orders_view_callback(c: CallbackQuery):
    if user_blocked(c.from_user.id) and not is_admin(c.from_user.id):
        return await c.answer("Account blocked.",show_alert=True)
    parts=c.data.split(":")
    status_filter=parts[1] if len(parts)>1 else "all"
    try: page=max(0,int(parts[2])) if len(parts)>2 else 0
    except ValueError: page=0
    await c.answer()
    return await render_orders_callback(c,page,status_filter)

@router.callback_query(F.data.startswith("orders_page:"))
async def orders_page_callback(c: CallbackQuery):
    if user_blocked(c.from_user.id) and not is_admin(c.from_user.id):
        return await c.answer("Account blocked.",show_alert=True)
    try: page=max(0,int(c.data.split(":",1)[1]))
    except ValueError: page=0
    await c.answer()
    return await render_orders_callback(c,page,"all")

async def render_profile_callback(c: CallbackQuery):
    u=await aget_user(c.from_user); row=await adb_execute("SELECT COUNT(*) AS c FROM orders WHERE user_id=%s",(u["id"],),"one")
    points=int(u.get("loyalty_points") or 0); tier,pct,next_points=vip_progress(points); bar="█"*max(0,pct//10)+"░"*(10-max(0,pct//10))
    text=(f"👤 <b>My Premium Account</b>\n\n🆔 ID: <code>{u['tg_id']}</code>\n💳 Wallet: <b>{fmt_money(u['balance'])}</b>\n🧾 Orders: <b>{row['c']}</b>\n⭐ Points: <b>{points}</b>\n🏅 VIP: <b>{tier}</b>\n📈 {bar} {pct}%\n💰 Lifetime spend: <b>{fmt_money(u.get('lifetime_spend') or 0)}</b>\n📅 Member since: <code>{u['created_at']}</code>")
    profile_row=[]
    if _feature_on("feature_rewards"):
        profile_row.append(InlineKeyboardButton(text=setting("inline_rewards", "⭐ Rewards"),callback_data="home:rewards"))
    if _feature_on("feature_referral"):
        profile_row.append(InlineKeyboardButton(text=setting("inline_referral", "🤝 Referral"),callback_data="home:refer"))
    profile_rows=([profile_row] if profile_row else [])+[[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]
    return await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=profile_rows))


@router.callback_query(F.data=="home:loyalty")
async def buyer_loyalty(c:CallbackQuery):
    if not _feature_on("feature_vip") and not is_admin(c.from_user.id):
        return await c.answer("Loyalty & VIP is currently disabled by admin.",show_alert=True)
    u=await aget_user(c.from_user)
    profile=await asyncio.to_thread(sync_loyalty_profile,u["id"])
    nxt=loyalty_next_progress(profile)
    progress="🏆 Highest VIP tier reached." if nxt["next"]=="MAX" else (
        f"Next: <b>{html.escape(nxt['next'])}</b>\n"
        f"Need: <b>{nxt['orders_needed']}</b> more completed order(s) and "
        f"<b>{fmt_money(nxt['spend_needed'])}</b> more lifetime spend."
    )
    await c.answer()
    await c.message.edit_text(
        f"🎁 <b>Loyalty & VIP</b>\n\n"
        f"Tier: <b>{html.escape(profile['tier'])}</b>\n"
        f"Points: <b>{int(profile['points'])}</b>\n"
        f"Completed orders: <b>{int(profile['completed_orders'])}</b>\n"
        f"Lifetime spend: <b>{fmt_money(profile['lifetime_spend'])}</b>\n\n{progress}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 My Orders",callback_data="home:orders")],
            [InlineKeyboardButton(text="🏠 Home",callback_data="main_menu")]
        ]))

async def render_rewards_callback(c: CallbackQuery):
    if not _feature_on("feature_rewards") and not is_admin(c.from_user.id):
        return await c.answer("Rewards are currently disabled by admin.",show_alert=True)
    u=await aget_user(c.from_user); refs=await adb_execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=%s",(u["id"],),"one")
    points=int(u.get("loyalty_points") or 0); spend=float(u.get("lifetime_spend") or 0); tier_name,pct,next_points=vip_progress(points); bar="█"*max(0,pct//10)+"░"*(10-max(0,pct//10))
    body=(f"⭐ <b>Premium Rewards Center</b>\n\n🏅 VIP Tier: <b>{tier_name}</b>\n📈 Progress: <b>{bar}</b> {pct}%\n⭐ Loyalty points: <b>{points}</b>\n🎯 Next milestone: <b>{next_points} points</b>\n💰 Lifetime spend: <b>{fmt_money(spend)}</b>\n🤝 Successful referrals: <b>{int(refs['c'])}</b>\n\nEarn points from completed purchases and referrals.")
    row=[]
    if _feature_on("feature_quick_shop"): row.append(InlineKeyboardButton(text=setting("inline_shop","🛍️ Shop"),callback_data="home:shop"))
    if _feature_on("feature_referral"): row.append(InlineKeyboardButton(text=setting("inline_referral","🤝 Referral"),callback_data="home:refer"))
    kb=([row] if row else [])+[[InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]
    return await c.message.edit_text(body,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

async def render_refer_callback(c: CallbackQuery):
    if not _feature_on("feature_referral") and not is_admin(c.from_user.id):
        return await c.answer("Referral is currently disabled by admin.",show_alert=True)
    u=await aget_user(c.from_user); me=await c.bot.get_me(); link=f"https://t.me/{me.username}?start=ref_{c.from_user.id}" if me.username else f"Use /start ref_{c.from_user.id}"; refs=await adb_execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=%s",(u["id"],),"one")
    body=("🤝 <b>Refer & Earn</b>\n\nInvite friends with your personal link. When a referred buyer completes their first purchase, both accounts receive loyalty recognition.\n\n" f"🔗 <b>Your link</b>\n<code>{html.escape(link)}</code>\n\n👥 Your referrals: <b>{int(refs['c'])}</b>")
    kb=[]
    if _feature_on("feature_rewards"): kb.append([InlineKeyboardButton(text=setting("inline_rewards","⭐ Rewards"),callback_data="home:rewards")])
    kb.append([InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")])
    return await c.message.edit_text(body,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

async def render_favorites_callback(c: CallbackQuery):
    if not _feature_on("feature_favorites") and not is_admin(c.from_user.id):
        return await c.answer("Favorites are currently disabled by admin.",show_alert=True)
    u=await aget_user(c.from_user); rows=await adb_execute("SELECT p.* FROM favorites f JOIN products p ON p.id=f.product_id WHERE f.user_id=%s AND p.active=1 ORDER BY f.created_at DESC LIMIT 30",(u["id"],),"all")
    if not rows:
        return await c.message.edit_text("⭐ <b>Favorites</b>\n\nNo saved products yet.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("inline_shop", "🛍️ Shop"),callback_data="home:shop"),InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]]))
    buttons=[[InlineKeyboardButton(text=f"{'🟢' if effective_stock(p)>0 else '🔴'} {html.escape(p['name'])} • {float(p['price']):g} {currency()}",callback_data=f"product:{p['id']}")] for p in rows]
    buttons.append([InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")])
    return await c.message.edit_text(f"⭐ <b>Favorites</b> ({len(rows)})\n\nTap a product to view or buy.",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

async def render_support_callback(c: CallbackQuery):
    if not _feature_on("feature_support") and not is_admin(c.from_user.id):
        return await c.answer("Support is currently disabled by admin.",show_alert=True)
    return await c.message.edit_text(
      "🆘 <b>Support Center</b>\n\nChoose the issue type:",
      reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Payment",callback_data="support_new:payment"),
         InlineKeyboardButton(text="📦 Delivery",callback_data="support_new:delivery")],
        [InlineKeyboardButton(text="👤 Account",callback_data="support_new:account"),
         InlineKeyboardButton(text="❓ Other",callback_data="support_new:other")],
        [InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")]
      ]))


@router.callback_query(F.data.startswith("home:"))
async def premium_home_callback(c: CallbackQuery, state: FSMContext):
    if user_blocked(c.from_user.id) and not is_admin(c.from_user.id):
        return await c.answer("🚫 Your account is blocked.", show_alert=True)
    action=c.data.split(":",1)[1]
    feature_for_action={
        "shop":"feature_quick_shop",
        "search":"feature_search",
        "rewards":"feature_rewards",
        "refer":"feature_referral",
        "favorites":"feature_favorites",
        "support":"feature_support",
        "loyalty":"feature_vip",
        "offers":"feature_smart_offers",
    }.get(action)
    if feature_for_action and not _feature_on(feature_for_action) and not is_admin(c.from_user.id):
        return await c.answer("This feature is currently disabled by admin.", show_alert=True)
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


@router.callback_query(F.data.startswith("order_support:"))
async def secure_login_order_support(c:CallbackQuery):
    try:
        oid=int(c.data.split(":",1)[1])
    except Exception:
        return await c.answer("Invalid order.",show_alert=True)
    row=await adb_execute(
        """SELECT o.id,o.status,o.total,p.name,p.category,u.tg_id
           FROM orders o JOIN products p ON p.id=o.product_id
           JOIN users u ON u.id=o.user_id
           WHERE o.id=%s""",
        (oid,),"one")
    if not row or (int(row["tg_id"])!=c.from_user.id and not is_admin(c.from_user.id)):
        return await c.answer("Order not found.",show_alert=True)
    if not is_secure_login_support_product({"category":row["category"],"delivery_type":"manual"}):
        return await c.answer("External support handoff is not required for this order.",show_alert=True)
    msg=secure_login_support_message(oid)
    await c.answer()
    return await c.message.answer(
        f"🎮 <b>Secure Order Support</b>\\n\\n"
        f"🧾 Order ID: <b>#{oid}</b>\\n"
        f"🎮 Product: <b>{html.escape(row['name'])}</b>\\n"
        f"📍 Status: <b>{html.escape(buyer_status_text(row['status']))}</b>\\n\\n"
        f"📩 Send this message to our support team:\\n"
        f"<code>{html.escape(msg)}</code>\\n\\n"
        f"🔒 Never send your password inside the bot.",
        reply_markup=secure_login_support_markup(oid)
    )


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
    total_row = await adb_execute(f"SELECT COUNT(*) AS c FROM products p WHERE {where}", (category, category), "one")
    total = int(total_row["c"]) if total_row else 0
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages - 1)
    rows = await adb_execute(
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




def recent_viewed_products(user_id,limit=10):
    return db_execute("""
      SELECT DISTINCT ON (p.id) p.id,p.name,p.price,p.delivery_type,p.stock,v.viewed_at,
        CASE WHEN p.delivery_type='code'
          THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available')
          ELSE p.stock END AS effective_stock
      FROM product_views v JOIN products p ON p.id=v.product_id
      WHERE v.user_id=%s AND p.active=1
      ORDER BY p.id,v.viewed_at DESC
      LIMIT %s
    """,(user_id,max(1,min(20,int(limit)))),"all") or []


@router.callback_query(F.data=="home:recent")
async def buyer_recently_viewed(c:CallbackQuery):
    u=await aget_user(c.from_user)
    rows=await asyncio.to_thread(recent_viewed_products,u["id"],10)
    if not rows:
        await c.answer()
        return await c.message.edit_text(
          "🕘 <b>Recently Viewed</b>\n\nNo products viewed yet.",
          reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎮 Browse Shop",callback_data="home:shop")],
            [InlineKeyboardButton(text="🏠 Home",callback_data="main_menu")]
          ]))
    kb=[]
    for p in rows:
        stock=int(p["effective_stock"] or 0)
        kb.append([
          InlineKeyboardButton(text=f"🎮 {str(p['name'])[:22]} • {fmt_money(p['price'])}",callback_data=f"product:{p['id']}"),
          InlineKeyboardButton(text="🛒 Buy" if stock>0 else "⛔",callback_data=f"buy:{p['id']}" if stock>0 else f"soldout:{p['id']}")
        ])
    kb.append([InlineKeyboardButton(text="🏠 Home",callback_data="main_menu")])
    await c.answer()
    await c.message.edit_text("🕘 <b>Recently Viewed</b>\n\nYour latest product views.",reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))



def merch_product_rows(mode,limit=12):
    if mode=="featured":
        where="p.featured=1"; order="p.merch_rank,p.id DESC"
    elif mode=="popular":
        where="(p.best_seller=1 OR p.hot=1)"; order="p.best_seller DESC,p.hot DESC,p.merch_rank,p.id DESC"
    else:
        return []
    return db_execute(
        f"""SELECT p.*,CASE WHEN p.delivery_type='code' THEN
              (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available')
              ELSE p.stock END AS effective_stock
            FROM products p WHERE p.active=1 AND {where}
            ORDER BY {order} LIMIT %s""",(max(1,min(24,int(limit))),),"all") or []


def merch_kb(rows):
    kb=[]
    for p in rows:
        stock=int(p["effective_stock"] or 0)
        badges=product_merch_badges(p)
        label=str(p["name"]); label=label[:22]+"…" if len(label)>23 else label
        kb.append([
            InlineKeyboardButton(text=f"{badges} {label} • {product_button_price(p)}".strip(),callback_data=f"product:{p['id']}"),
            InlineKeyboardButton(text="🛒 Buy" if stock>0 else "⛔",callback_data=f"buy:{p['id']}" if stock>0 else f"soldout:{p['id']}")
        ])
    kb.append([InlineKeyboardButton(text="🎮 Games",callback_data="shop"),InlineKeyboardButton(text="🏠 Home",callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


@router.callback_query(F.data=="shop:featured")
async def buyer_featured(c:CallbackQuery):
    rows=await asyncio.to_thread(merch_product_rows,"featured",12)
    await c.answer()
    if not rows:
        return await c.message.edit_text("⭐ <b>Featured Products</b>\n\nNo featured products right now.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎮 Browse Games",callback_data="shop")]]))
    await c.message.edit_text("⭐ <b>Featured Products</b>\n\nHand-picked products and offers.",reply_markup=merch_kb(rows))


@router.callback_query(F.data=="shop:popular")
async def buyer_popular(c:CallbackQuery):
    rows=await asyncio.to_thread(merch_product_rows,"popular",12)
    await c.answer()
    if not rows:
        return await c.message.edit_text("🏆 <b>Popular Products</b>\n\nNo popular products marked yet.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎮 Browse Games",callback_data="shop")]]))
    await c.message.edit_text("🏆 <b>Popular / Hot</b>\n\nBest sellers and hot picks.",reply_markup=merch_kb(rows))

@router.callback_query(F.data.startswith("product:"))
async def product_callback(c:CallbackQuery):
    if maintenance_active() and not is_admin(c.from_user.id):
        return await c.answer("Shop is under maintenance.",show_alert=True)
    pid=int(c.data.split(":")[1])
    p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1 AND COALESCE(archived,0)=0",(pid,),"one")
    if not p:
        return await c.answer("Product unavailable.",show_alert=True)
    try:
        u=await aget_user(c.from_user)
        await asyncio.to_thread(record_product_view,u["id"],pid)
    except Exception:
        pass
    stock=effective_stock(p)
    badges=product_merch_badges(p)
    if is_auto_code_product(p):
        delivery_line="⚡ <b>INSTANT CODE DELIVERY</b>"
        credential_line="🎁 Code delivered automatically after payment"
    elif is_secure_login_support_product(p):
        delivery_line="🎮 <b>SECURE SUPPORT DELIVERY</b>"
        credential_line="🔒 No Gmail, Facebook ID, UID, or password is collected by the bot"
    elif is_uid_only_manual_product(p):
        delivery_line="🆔 <b>UID TOP-UP</b>"
        credential_line="🔒 Player UID only • No password required"
    else:
        delivery_line="🔐 <b>SECURE MANUAL DELIVERY</b>"
        credential_line="🛡 Login details handled through protected checkout"
    availability="🟢 READY TO ORDER" if stock>0 else "🔴 TEMPORARILY SOLD OUT"
    sale_until=p.get("sale_until")
    offer_line=""
    if product_sale_price(p)<float(p["price"]) and sale_until:
        offer_line=f"\n⏳ <b>Elite offer ends:</b> {html.escape(str(sale_until)[:16])} UTC"
    body=(
        f"💎 <b>NEXT LEVEL ELITE</b> {badges}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎮 <b>{html.escape(p['name'])}</b>\n"
        f"<i>{html.escape(p['category'])}</i>\n\n"
        f"💰 <b>PRICE</b>  {product_price_display(p)}\n"
        f"📦 <b>AVAILABILITY</b>  {stock} ready{offer_line}\n"
        f"🚀 <b>DELIVERY</b>  {delivery_line}\n"
        f"{credential_line}\n\n"
        f"{availability}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 {html.escape(p['description'] or 'Premium gaming product with secure order processing.')}\n\n"
        f"{'✨ <b>Secure your package now.</b>' if stock>0 else '🔔 Check back shortly for restock.'}"
    )
    buttons=[]
    if stock>0:
        buttons.append([InlineKeyboardButton(text=f"💳 BUY NOW • {product_button_price(p)}",callback_data=f"buy:{pid}")])
        if cart_enabled():
            buttons.append([InlineKeyboardButton(text="🛒 Add to Cart",callback_data=f"cart:add:{pid}")])
    fav_label=setting("button_favorite_remove","💔 Remove Favorite") if user_favorite(pid,c.from_user.id) else setting("button_favorite_add","⭐ Save to Favorites")
    buttons.append([InlineKeyboardButton(text=fav_label,callback_data=f"fav:{pid}")])
    buttons.append([InlineKeyboardButton(text="⭐ Elite Picks",callback_data="shop:featured"),
                    InlineKeyboardButton(text=setting("button_back","⬅️ Back"),callback_data=f"cat:{p['category']}")])
    markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    await c.answer()
    if p["image_file_id"]:
        try:
            await c.message.answer_photo(p["image_file_id"],caption=body,reply_markup=markup)
        except Exception:
            pass
        else:
            try: await c.message.delete()
            except Exception: pass
            return
    await c.message.edit_text(body,reply_markup=markup)

@router.callback_query(F.data.startswith("fav:"))
async def favorite_toggle(c: CallbackQuery):
    if not _feature_on("feature_favorites") and not is_admin(c.from_user.id):
        return await c.answer("Favorites are currently disabled by admin.",show_alert=True)
    if user_blocked(c.from_user.id) and not is_admin(c.from_user.id):
        return await c.answer("Account blocked.",show_alert=True)
    pid=int(c.data.split(":",1)[1]); u=await aget_user(c.from_user)
    p=await adb_execute("SELECT id,name,category,price,stock,delivery_type,active,description,image_file_id,quantity FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    exists=await adb_execute("SELECT 1 FROM favorites WHERE user_id=%s AND product_id=%s",(u["id"],pid),"one")
    if exists:
        await adb_execute("DELETE FROM favorites WHERE user_id=%s AND product_id=%s",(u["id"],pid))
        msg="💔 Removed from favorites."
    else:
        await adb_execute("INSERT INTO favorites(user_id,product_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",(u["id"],pid))
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
    if not _feature_on("feature_favorites") and not is_admin(m.from_user.id):
        return await m.answer("❤️ Favorites are currently disabled by admin.",reply_markup=premium_home_kb())
    if user_access_denied(m.from_user.id) and not is_admin(m.from_user.id):
        return await m.answer("🔧 Shop is temporarily unavailable. Please try again later.")
    u=await aget_user(m.from_user)
    rows=await adb_execute("SELECT p.* FROM favorites f JOIN products p ON p.id=f.product_id WHERE f.user_id=%s AND p.active=1 ORDER BY f.created_at DESC LIMIT 30",(u["id"],),"all")
    if not rows: return await m.answer("⭐ <b>Your Favorites</b>\n\nNo saved products yet. Open a product and tap ⭐ Add to Favorites.")
    buttons=[[InlineKeyboardButton(text=f"{'🟢' if effective_stock(p)>0 else '🔴'} {p['name']} • {float(p['price']):g} {currency()}",callback_data=f"product:{p['id']}")] for p in rows]
    await m.answer(f"⭐ <b>Your Favorites</b> ({len(rows)})\n\nTap a product to view or buy.",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("buy:"))
async def buy(c: CallbackQuery,state:FSMContext):
    if maintenance_active() and not is_admin(c.from_user.id): return await c.answer("Shop is under maintenance.",show_alert=True)
    pid=int(c.data.split(":")[1]); p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one"); u=await aget_user(c.from_user)
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    if u["blocked"] and not is_admin(c.from_user.id): return await c.answer("Account blocked.",show_alert=True)
    if effective_stock(p)<1: return await c.answer("Out of stock.",show_alert=True)
    await state.update_data(pid=pid,qty=1,game_uid="",account_password="",origin_message_id=c.message.message_id,origin_is_photo=bool(getattr(c.message,"photo",None)))
    if is_auto_code_product(p):
        await state.set_state(Buy.confirm); return await order_confirm(c,state)
    if is_secure_login_support_product(p):
        await state.update_data(game_uid="", account_password="", qty=1)
        await state.set_state(Buy.confirm)
        return await order_confirm(c,state)
    if is_uid_only_manual_product(p):
        await state.set_state(Buy.uid); await c.answer()
        return await c.message.answer("🆔 <b>Enter Player UID</b>\n\n"+f"🎮 Product: <b>{html.escape(str(p['name']))}</b>\n"+"Send only the Player UID required for this top-up.\nNo password is required for this product.\nSend /cancel to cancel.")
    await state.set_state(None); await c.answer()
    await c.message.answer("🔐 <b>Manual Delivery Login</b>\n\nUse the buttons below to enter the login details required for this product.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📧 Enter Gmail / FB ID",callback_data="manual_cred:enter_id")],[InlineKeyboardButton(text="🔒 Enter Password (after ID)",callback_data="manual_cred:password_locked")],[InlineKeyboardButton(text="❌ Cancel",callback_data="manual_cred:cancel")]]))


@router.callback_query(F.data=="manual_cred:cancel")
async def manual_credential_cancel(c:CallbackQuery,state:FSMContext):
    # This callback is intentionally state-independent: Cancel must work from
    # the ID prompt, password prompt, or the initial manual-delivery screen.
    await state.clear()
    try:
        await c.answer("Cancelled")
    except Exception:
        pass
    text="❌ <b>Manual delivery checkout cancelled.</b>"
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=setting("inline_shop","🛍️ Shop"),callback_data="home:shop")],
        [InlineKeyboardButton(text=setting("button_main_menu","🏠 Main Menu"),callback_data="main_menu")],
    ])
    msg=getattr(c,"message",None)
    if msg is None:
        return
    # Telegram may reject edit_text for media messages and edit_caption for text
    # messages. Try both, then fall back to a fresh navigation message.
    try:
        await msg.edit_text(text,reply_markup=kb)
        return
    except Exception:
        pass
    try:
        await msg.edit_caption(caption=text,reply_markup=kb)
        return
    except Exception:
        pass
    await msg.answer(text,reply_markup=kb)


@router.callback_query(F.data=="manual_cred:enter_id")
async def manual_credential_enter_id(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); pid=d.get("pid")
    if not pid: return await c.answer("Checkout expired. Please tap Buy again.",show_alert=True)
    p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p or is_auto_code_product(p): await state.clear(); return await c.answer("Credential entry is not available for this product.",show_alert=True)
    if effective_stock(p)<1: await state.clear(); return await c.answer("Product is out of stock.",show_alert=True)
    await state.set_state(Buy.uid); await c.answer()
    if is_uid_only_manual_product(p): return await c.message.answer("🆔 <b>Enter Player UID</b>\n\nSend only the Player UID required for this top-up.\nNo password is required.\nSend /cancel to cancel.")
    await c.message.answer("📧 <b>Enter your Gmail / Facebook ID</b>\n\nSend the account email / Facebook ID required for delivery.\nSend /cancel to cancel.")


@router.callback_query(F.data=="manual_cred:password_locked")
async def manual_credential_password_locked(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); pid=d.get("pid"); p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one") if pid else None
    if p and is_uid_only_manual_product(p): return await c.answer("This product only needs Player UID. No password is required.",show_alert=True)
    if not (d.get("game_uid") or "").strip(): return await c.answer("Enter your Gmail / FB ID first.",show_alert=True)
    c.data="manual_cred:enter_password"; return await manual_credential_enter_password(c,state)


@router.callback_query(F.data=="manual_cred:enter_password")
async def manual_credential_enter_password(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); pid=d.get("pid"); p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one") if pid else None
    if p and is_uid_only_manual_product(p): return await c.answer("This product only needs Player UID. No password is required.",show_alert=True)
    if not (d.get("game_uid") or "").strip(): return await c.answer("Enter your Gmail / FB ID first.",show_alert=True)
    if not p or is_auto_code_product(p): await state.clear(); return await c.answer("Checkout expired. Please tap Buy again.",show_alert=True)
    if effective_stock(p)<1: await state.clear(); return await c.answer("Product is out of stock.",show_alert=True)
    await state.set_state(Buy.password); await c.answer()
    prompt=await c.message.answer("🔐 <b>Enter your Password</b>\n\nSend only the password required to complete this order.\nYour password message will be removed from chat after submission.\nSend /cancel to cancel.")
    await state.update_data(credential_prompt_message_id=prompt.message_id)


@router.message(Buy.uid)
async def buy_uid(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id): await state.clear(); return await m.answer(custom_text("maintenance_message","🔧 Shop is temporarily under maintenance. Please try again later."),reply_markup=inline_home_kb())
    uid=(m.text or "").strip()
    if uid.lower()=="/cancel": await state.clear(); return await m.answer("❌ Cancelled.")
    d=await state.get_data(); p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(d["pid"],),"one")
    if not p or effective_stock(p)<1: await state.clear(); return await m.answer("❌ Product is out of stock.")
    uid_only=is_uid_only_manual_product(p)
    if len(uid)<2 or len(uid)>128:
        return await m.answer(f"❌ Please send a valid {'Player UID' if uid_only else 'Gmail / Facebook ID'} (2–128 characters).")
    await state.update_data(game_uid=uid,qty=1)
    if uid_only:
        await state.update_data(account_password=""); await state.set_state(Buy.confirm)
        return await m.answer("✅ <b>Player UID received</b>\n\n"+f"🆔 UID: <code>{html.escape(uid)}</code>\nNo password is required for this product.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💳 Continue to Payment",callback_data="order:confirm")],[InlineKeyboardButton(text="✏️ Change UID",callback_data="manual_cred:enter_id")],[InlineKeyboardButton(text="❌ Cancel",callback_data="manual_cred:cancel")]]))
    if not is_auto_code_product(p):
        await state.set_state(None)
        await m.answer("✅ <b>Gmail / FB ID received</b>\n\nNow continue with your password.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔐 Enter Password",callback_data="manual_cred:enter_password")],[InlineKeyboardButton(text="✏️ Change Gmail / FB ID",callback_data="manual_cred:enter_id")],[InlineKeyboardButton(text="❌ Cancel",callback_data="manual_cred:cancel")]])); return
    await state.set_state(Buy.confirm); return await m.answer("Continue checkout from the product screen.")

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
    p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(d["pid"],),"one"); u=await aget_user(m.from_user)
    if not p or is_auto_code_product(p):
        await state.clear(); return await m.answer("❌ Credential step is no longer required.")
    if effective_stock(p)<1:
        await state.clear(); return await m.answer("❌ Product is out of stock.")
    await state.update_data(account_password=password, qty=1)
    await state.set_state(Buy.confirm)

    # Manual flow: ID -> password -> wallet/direct payment choice.
    # The password message was deleted for credential privacy, so render the next
    # checkout screen as a fresh message and surface callback-style errors to buyer.
    class _FreshMessageProxy:
        def __init__(self, original):
            self._original = original
        async def edit_text(self, text, reply_markup=None, **kwargs):
            return await self._original.answer(text, reply_markup=reply_markup, **kwargs)
        async def edit_caption(self, caption=None, reply_markup=None, **kwargs):
            return await self._original.answer(caption or "", reply_markup=reply_markup, **kwargs)
        async def answer(self, text, reply_markup=None, **kwargs):
            return await self._original.answer(text, reply_markup=reply_markup, **kwargs)

    class _MessageCallbackAdapter:
        def __init__(self, message):
            self.message = _FreshMessageProxy(message)
            self.from_user = message.from_user
            self.bot = message.bot
        async def answer(self, text=None, *args, **kwargs):
            if text:
                kwargs.pop("show_alert", None)
                return await self.message._original.answer(text, **kwargs)
            return None

    await order_confirm(_MessageCallbackAdapter(m), state)




@router.callback_query(F.data=="order:change_qty")
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
    unit_price=product_sale_price(p)
    total=unit_price*qty
    text=(f"📦 <b>Choose Quantity</b>\n\n🎮 Product: <b>{html.escape(p['name'])}</b>\n"
          f"💰 Unit Price: <b>{fmt_money(unit_price)}</b>\n📦 Quantity: <b>{qty}</b>\n"
          f"⭐ Total: <b>{fmt_money(total)}</b>\n\n👛 Wallet Balance: <b>{fmt_money(u['balance'])}</b>\n\n"
          "Use − / +, then continue to payment.")
    markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➖",callback_data="order:qty:-1"),
         InlineKeyboardButton(text=f"📦 {qty}",callback_data="order:noop"),
         InlineKeyboardButton(text="➕",callback_data="order:qty:1")],
        [InlineKeyboardButton(text="💳 Continue to Payment",callback_data="order:confirm")],
        [InlineKeyboardButton(text="❌ Cancel",callback_data="order:cancel"),
         InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")],
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
    d=await state.get_data(); pid=int(d["pid"]); p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    current=int(d.get("qty",1)); delta=int(c.data.rsplit(":",1)[1]); max_qty=1 if not is_auto_code_product(p) else max(1,min(10,effective_stock(p))); qty=max(1,min(max_qty,current+delta))
    if qty==current: return await c.answer("Maximum available quantity reached." if delta>0 else "Minimum quantity is 1.",show_alert=True)
    await state.update_data(qty=qty)
    unit_price=product_sale_price(p); total=unit_price*qty; uid=html.escape(d.get("game_uid","")); balance=float((await aget_user(c.from_user))["balance"])
    cred_line = "\n🔑 Password: <code>••••••••</code>" if not is_auto_code_product(p) else ""
    delivery_line = "🛠️ Manual — Admin processing" if not is_auto_code_product(p) else "⚡ Instant Delivery"
    text=(f"🛒 <b>Purchase Confirmation</b>\n\n🎮 Product: <b>{html.escape(p['name'])}</b>\n🆔 UID: <code>{uid}</code>{cred_line}\n💰 Unit Price: <b>{fmt_money(unit_price)}</b>\n📦 Quantity: <b>{qty}</b>\n⭐ Total: <b>{fmt_money(total)}</b>\n\n💳 Your balance: <b>{fmt_money(balance)}</b>\n{delivery_line}\n\n{html.escape(custom_text('buy_prompt','Confirm your purchase:'))}")
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


_LOYALTY_TIERS=[
    ("Bronze",0,0.00),
    ("Silver",3,1000.00),
    ("Gold",10,5000.00),
    ("VIP",25,15000.00),
]


def loyalty_tier_for(completed_orders,lifetime_spend):
    completed_orders=int(completed_orders or 0)
    lifetime_spend=float(lifetime_spend or 0)
    tier="Bronze"
    for name,min_orders,min_spend in _LOYALTY_TIERS:
        if completed_orders>=min_orders and lifetime_spend>=min_spend:
            tier=name
    return tier


def loyalty_points_for(amount):
    return max(0,int(math.floor(float(amount or 0))))


def sync_loyalty_profile(user_id):
    stats=db_execute(
        """SELECT COUNT(*) AS completed_orders,COALESCE(SUM(total),0) AS lifetime_spend
           FROM orders WHERE user_id=%s AND status='completed'""",
        (user_id,),"one") or {}
    completed=int(stats.get("completed_orders") or 0)
    spend=float(stats.get("lifetime_spend") or 0)
    tier=loyalty_tier_for(completed,spend)
    points=loyalty_points_for(spend)
    db_execute(
        """INSERT INTO loyalty_profiles(user_id,tier,points,lifetime_spend,completed_orders,updated_at)
           VALUES(%s,%s,%s,%s,%s,NOW())
           ON CONFLICT(user_id) DO UPDATE SET
             tier=EXCLUDED.tier,points=EXCLUDED.points,lifetime_spend=EXCLUDED.lifetime_spend,
             completed_orders=EXCLUDED.completed_orders,updated_at=NOW()""",
        (user_id,tier,points,spend,completed))
    return {"tier":tier,"points":points,"lifetime_spend":spend,"completed_orders":completed}


def loyalty_snapshot(user_id):
    row=db_execute(
        "SELECT tier,points,lifetime_spend,completed_orders FROM loyalty_profiles WHERE user_id=%s",
        (user_id,),"one")
    return row or sync_loyalty_profile(user_id)


def loyalty_next_progress(profile):
    orders=int(profile.get("completed_orders") or 0)
    spend=float(profile.get("lifetime_spend") or 0)
    current=profile.get("tier") or "Bronze"
    names=[x[0] for x in _LOYALTY_TIERS]
    try: idx=names.index(current)
    except ValueError: idx=0
    if idx>=len(_LOYALTY_TIERS)-1:
        return {"next":"MAX","orders_needed":0,"spend_needed":0}
    name,min_orders,min_spend=_LOYALTY_TIERS[idx+1]
    return {"next":name,"orders_needed":max(0,min_orders-orders),"spend_needed":max(0,float(min_spend)-spend)}


def record_order_event(order_id,event_type,status=None,message="",actor_tg_id=None):
    db_execute(
        """INSERT INTO order_events(order_id,event_type,status,message,actor_tg_id)
           VALUES(%s,%s,%s,%s,%s)""",
        (int(order_id),str(event_type)[:60],status,str(message or "")[:1000],actor_tg_id))


def order_timeline(order_id):
    return db_execute(
        """SELECT event_type,status,message,actor_tg_id,created_at
           FROM order_events WHERE order_id=%s ORDER BY created_at,id""",
        (int(order_id),),"all") or []


def order_status_label(status):
    labels={
        "awaiting_payment":"🟡 Awaiting Payment",
        "pending":"🟠 Processing",
        "completed":"✅ Delivered",
        "refund_pending":"💸 Refund Pending",
        "refunded":"↩️ Refunded",
        "rejected":"❌ Rejected",
        "cancelled":"⚫ Cancelled",
    }
    return labels.get(str(status),f"ℹ️ {str(status).replace('_',' ').title()}")


def build_order_tracking_text(order,events):
    lines=[
        f"📦 <b>Order #{order['id']}</b>",
        f"🎮 {html.escape(order.get('product_name') or 'Product')}",
        f"💰 {fmt_money(order.get('total',0))}",
        f"📍 Status: <b>{order_status_label(order.get('status'))}</b>",
    ]
    if order.get("created_at"):
        lines.append(f"🕒 Created: {html.escape(str(order['created_at'])[:16])}")
    lines += ["","🧭 <b>Timeline</b>"]
    if not events:
        lines.append("• Order created.")
    else:
        for e in events[-12:]:
            ts=html.escape(str(e["created_at"])[:16])
            msg=html.escape(e.get("message") or e.get("event_type") or "Update")
            lines.append(f"• {ts} — {msg}")
    return "\n".join(lines)


def notify_order_status_change(tg_id,order_id,status,message):
    text=(
        f"📦 <b>Order #{order_id} Update</b>\n\n"
        f"Status: <b>{order_status_label(status)}</b>\n"
        f"{html.escape(message)}"
    )
    enqueue_notification(tg_id,text,[[["📦 Track Order",f"order_track:{order_id}"],["🏠 Main Menu","main_menu"]]])


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
    cur.execute(
        "INSERT INTO order_events(order_id,event_type,status,message) VALUES(%s,'completed','completed','Order completed and rewards processed')",
        (order_id,))


@router.callback_query(Buy.confirm,F.data=="order:confirm")
async def order_confirm(c:CallbackQuery,state:FSMContext):
    """Show secure payment choice. Money/stock is touched only after final confirmation."""
    if maintenance_active() and not is_admin(c.from_user.id): await state.clear(); return await c.answer("Shop is under maintenance.",show_alert=True)
    d=await state.get_data(); pid=int(d["pid"]); p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one"); u=await aget_user(c.from_user)
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    qty=max(1,min(10,int(d.get("qty",1)))); qty=1 if not is_auto_code_product(p) else qty
    stock=effective_stock(p); unit_price=product_sale_price(p); total=unit_price*qty
    if stock<qty: return await c.answer("Stock changed. Please retry.",show_alert=True)
    wallet_label=f"💰 Pay from Wallet • {fmt_money(u['balance'])}" if float(u["balance"])>=total else f"💰 Wallet • Need {fmt_money(total-float(u['balance']))} more"
    if is_auto_code_product(p):
        cred_line=""
        delivery_line="⚡ Instant Delivery"
        next_step=""
    elif is_secure_login_support_product(p):
        cred_line=""
        delivery_line="🎮 Manual Top-Up • Support Assisted"
        next_step="\n\n📩 <b>After payment:</b> Contact us with your Order ID.\n🔒 The bot will not ask for your Gmail/Facebook login or password."
    elif is_uid_only_manual_product(p):
        cred_line=f"\n🆔 Player UID: <code>{html.escape(str(d.get('game_uid','')))}</code>"
        delivery_line="🛠️ Manual — Admin processing"
        next_step=""
    else:
        cred_line=f"\n🆔 ID / UID: <code>{html.escape(str(d.get('game_uid','')))}</code>\n🔑 Password: <code>••••••••</code>"
        delivery_line="🛠️ Manual — Admin processing"
        next_step=""
    text=(f"🧾 <b>Order Review</b>\n\n🎮 Product: <b>{html.escape(p['name'])}</b>{cred_line}\n📦 Quantity: <b>{qty}</b>\n💰 Unit Price: <b>{fmt_money(unit_price)}</b>\n⭐ Total: <b>{fmt_money(total)}</b>\n\n👛 Wallet Balance: <b>{fmt_money(u['balance'])}</b>\n{delivery_line}{next_step}\n\n💳 <b>Choose Payment Method</b>")
    rows=[[InlineKeyboardButton(text=wallet_label,callback_data="order:pay_wallet")]]
    if uddoktapay_enabled():
        rows.append([InlineKeyboardButton(text="⚡ Auto Pay • UddoktaPay",callback_data="order:pay_uddoktapay")])
    rows.append([InlineKeyboardButton(text="🌐 Manual Direct Payment",callback_data="order:pay_direct")])
    if is_auto_code_product(p): rows.append([InlineKeyboardButton(text="📦 Change Quantity",callback_data="order:change_qty")])
    rows.append([InlineKeyboardButton(text="❌ Cancel",callback_data="order:cancel"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")])
    markup=InlineKeyboardMarkup(inline_keyboard=rows); await c.answer()
    try: await c.message.edit_text(text,reply_markup=markup)
    except Exception:
        try: await c.message.edit_caption(caption=text,reply_markup=markup)
        except Exception: await c.message.answer(text,reply_markup=markup)


def _order_payment_methods_kb(prefix="orderpay:method:"):
    active=[x for x in payment_method_specs() if payment_method_enabled(x[0])]
    rows=[]
    for i in range(0,len(active),2):
        rows.append([InlineKeyboardButton(text=f"{x[2]} {x[1]}",callback_data=f"{prefix}{x[0]}") for x in active[i:i+2]])
    rows.append([InlineKeyboardButton(text="⬅️ Back",callback_data="order:confirm")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _start_direct_order_payment(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); pid=int(d["pid"]); p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    qty=max(1,min(10,int(d.get("qty",1)))); qty=1 if not is_auto_code_product(p) else qty
    # V10.1 safety guard: direct-payment fulfillment currently reserves one order/code.
    # Block multi-quantity before the buyer is shown an amount or asked to send money.
    if qty != 1:
        return await c.answer("Direct Payment currently supports 1 item per transaction. Please set quantity to 1 or use Wallet.", show_alert=True)
    if effective_stock(p)<qty: return await c.answer("Out of stock.",show_alert=True)
    total=product_sale_price(p)*qty
    await state.update_data(direct_amount=total,direct_qty=qty)
    await state.set_state(DirectPaymentState.method)
    await c.answer()
    await c.message.edit_text(
        f"🌐 <b>Direct Payment</b>\n\n💰 Amount: <b>{fmt_money(total)}</b>\n\nChoose a live payment method below. Your order will remain <b>awaiting payment</b> until the payment is verified by an admin.",
        reply_markup=_order_payment_methods_kb())



@router.callback_query(Buy.confirm,F.data=="order:pay_uddoktapay")
async def order_pay_uddoktapay(c:CallbackQuery,state:FSMContext):
    if not uddoktapay_enabled():
        return await c.answer("Auto payment is not available right now.",show_alert=True)
    async with _buyer_checkout_lock(c.from_user.id):
        d=await state.get_data()
        try:
            pid=int(d["pid"]); qty=max(1,min(10,int(d.get("qty",1))))
        except Exception:
            await state.clear()
            return await c.answer("Checkout session expired.",show_alert=True)
        p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
        if not p:
            await state.clear()
            return await c.answer("Product unavailable.",show_alert=True)
        qty=1 if not is_auto_code_product(p) else qty
        if qty != 1:
            return await c.answer("Auto payment currently supports one item per direct checkout.",show_alert=True)
        amount=product_sale_price(p)*qty
        placeholder="UP-"+uuid.uuid4().hex.upper()
        normalized=normalize_trx_id(placeholder)
        try:
            result=await asyncio.to_thread(
                _create_direct_payment_tx,
                c.from_user.id,pid,qty,float(amount),"uddoktapay",placeholder,normalized,
                d.get("game_uid",""),d.get("account_password","")
            )
            oid=result["oid"]; payid=result["payid"]
            user=await aget_user(c.from_user)
            checkout_url=await asyncio.to_thread(
                uddoktapay_create_checkout,payid,c.from_user.id,user.get("name") or c.from_user.full_name,
                float(result["amount"]),"direct_order",oid
            )
        except Exception as exc:
            # If API checkout creation failed after reservation, release safely.
            try:
                if 'payid' in locals() and 'oid' in locals():
                    await asyncio.to_thread(_direct_payment_cancel_tx,payid,oid)
            except Exception as release_exc:
                record_runtime_error("uddoktapay_checkout_release",release_exc,{"payment_id":locals().get("payid"),"order_id":locals().get("oid")})
            error_id=record_runtime_error("uddoktapay_order_checkout",exc,{"user_id":c.from_user.id,"product_id":pid})
            await state.clear()
            return await c.answer(f"Auto-payment checkout failed safely. Ref: {error_id}",show_alert=True)
        await state.update_data(direct_order_id=oid,direct_payment_id=payid,direct_trx=placeholder)
        await state.set_state(DirectPaymentState.receipt)
        await c.answer()
        await c.message.edit_text(
            f"⚡ <b>UddoktaPay Auto Payment</b>\n\n🧾 Order: <b>#{oid}</b>\n💰 Amount: <b>{fmt_money(amount)}</b>\n📦 Stock: <b>Reserved while payment is pending</b>\n\n"
            "Tap <b>Pay Now</b> and complete payment on the secure checkout page. The bot will verify the payment from UddoktaPay automatically.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Pay Now",url=checkout_url)],
                [InlineKeyboardButton(text="🔄 Check Payment",callback_data=f"uddoktapay:check:{payid}")],
                [InlineKeyboardButton(text="❌ Cancel Pending Order",callback_data="orderpay:cancel")],
            ])
        )


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
        if float(u["balance"]) < product_sale_price(p)*qty: return await c.answer("Insufficient wallet balance. Choose Direct Payment instead.",show_alert=True)
        return await _fulfill_wallet_order(c,state,d,p,u,qty)


def _fulfill_wallet_order_tx(tg_id,d,p,qty):
    delivered=[]; pending=[]; order_ids=[]
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users WHERE tg_id=%s FOR UPDATE",(tg_id,)); u=cur.fetchone()
                cur.execute("SELECT * FROM products WHERE id=%s AND active=1 FOR UPDATE",(p["id"],)); product=cur.fetchone()
                if not u or not product: raise RuntimeError("Order unavailable.")
                unit_price=product_sale_price(product); available=effective_stock(product)
                if available<qty: raise RuntimeError(f"Only {available} item(s) available.")
                total=unit_price*qty
                if float(u["balance"])<total: raise RuntimeError("Balance changed. Please retry.")
                stored_account_password = encrypt_credential_cursor(cur, d.get("account_password", "")) if product["delivery_type"] != "code" else ""
                for _ in range(qty):
                    delivered_code=None; auto_code=False
                    cur.execute("SELECT * FROM product_codes WHERE product_id=%s AND status='available' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",(product["id"],)); code_row=cur.fetchone()
                    auto_code=(product["delivery_type"]=="code")
                    if auto_code:
                        if not code_row: raise RuntimeError("Code stock changed. Please retry.")
                        cur.execute("UPDATE product_codes SET status='sold',sold_to=%s,sold_at=NOW() WHERE id=%s AND status='available'",(u["id"],code_row["id"]))
                        delivered_code=code_row["code"]; status="completed"
                    else:
                        cur.execute("UPDATE products SET stock=stock-1,updated_at=NOW() WHERE id=%s AND stock>0",(product["id"],))
                        if cur.rowcount!=1: raise RuntimeError("Stock changed. Please retry.")
                        status="pending"
                    cur.execute("UPDATE users SET balance=balance-%s,updated_at=NOW() WHERE id=%s AND balance>=%s",(unit_price,u["id"],unit_price))
                    if cur.rowcount!=1: raise RuntimeError("Balance changed. Please retry.")
                    cur.execute("INSERT INTO orders(user_id,product_id,game_uid,account_password,total,delivered_code,status,payment_mode) VALUES(%s,%s,%s,%s,%s,%s,%s,'wallet') RETURNING id",(u["id"],product["id"],d.get("game_uid",""),stored_account_password if not auto_code else "",unit_price,delivered_code,status)); oid=cur.fetchone()["id"]; order_ids.append(oid)
                    if delivered_code:
                        cur.execute("UPDATE product_codes SET order_id=%s WHERE id=%s",(oid,code_row["id"])); delivered.append((oid,product["name"],delivered_code,unit_price))
                    else: pending.append((oid,product["name"],unit_price))
                    cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(u["id"],-unit_price,"purchase",f"Order #{oid}"))
                    if status=="completed": award_completed_order_rewards(cur,oid,u["id"],unit_price)
                sync_code_product_stock(product["id"],conn)
    return product,u,order_ids,delivered,pending,total


async def _fulfill_wallet_order(c,state,d,p,u,qty):
    await state.clear()
    try:
        p,u,order_ids,delivered,pending,total=await asyncio.to_thread(_fulfill_wallet_order_tx,c.from_user.id,d,p,qty)
    except Exception as exc:
        error_id = record_runtime_error("wallet_order_transaction", exc, {"user_id": c.from_user.id, "product_id": p.get("id") if p else None, "quantity": qty})
        return await c.answer(f"Order failed safely. Nothing was charged. Ref: {error_id}", show_alert=True)
    for oid,_,_ in list(pending):
        try:
            result=await asyncio.to_thread(auto_topup_try_fulfill_order,oid)
            if result.get("status")=="success":
                await notify_user(c.bot,c.from_user.id,f"✅ <b>Order #{oid} Auto Top-Up Completed</b>\n\nYour provider top-up was confirmed successfully.")
        except Exception as exc:
            record_runtime_error("wallet_auto_topup_hook",exc,{"order_id":oid})
    await _send_order_result(c,p,u,qty,order_ids,delivered,pending,total=total,payment_label="Wallet")

async def _send_order_result(c,p,u,qty,order_ids,delivered,pending,total,payment_label="Wallet"):
    await c.answer("✅ Payment successful")
    msg=["🎉 <b>ORDER CONFIRMED</b>","━━━━━━━━━━━━━━━━━━",f"🎮 <b>{html.escape(p['name'])}</b>",f"📦 Quantity: <b>{qty}</b>   •   💰 <b>{fmt_money(total)}</b>",f"💳 {html.escape(payment_label)}   •   🧾 {len(order_ids)} order(s)","", "✅ Payment secured successfully"]
    if delivered: msg.append("\n🎁 <b>Instant Delivery</b>\n"+"\n".join(f"#{o} • <code>{code}</code>" for o,n,code,a in delivered))
    if pending: msg.append("\n⏳ <b>Manual Delivery</b>\n"+"\n".join(f"#{o} • {fmt_money(a)}" for o,n,a in pending))
    if is_secure_login_support_product(p) and pending:
        oid=int(pending[0][0])
        msg.append(
            f"\n🎮 <b>NEXT STEP</b>\n"
            f"Contact our support team and send your Order ID: <b>#{oid}</b>.\n"
            f"🔒 Do not send your password inside the bot."
        )
        buyer_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Contact Support",callback_data=f"order_support:{oid}")],
            [InlineKeyboardButton(text="📦 Track Order",callback_data=f"order_track:{oid}")],
            [InlineKeyboardButton(text="🛒 Buy More",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]
        ])
    else:
        buyer_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 View My Orders",callback_data="home:orders")],[InlineKeyboardButton(text="🛒 Buy More",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]])
    await c.message.answer("\n".join(msg),reply_markup=buyer_markup)
    for oid in order_ids:
        if not any(x[0]==oid for x in pending):
            continue
        try:
            await asyncio.to_thread(
                record_order_event,oid,"processing","pending",
                "Payment received; manual delivery is processing.",c.from_user.id)
        except Exception as exc:
            record_runtime_error("order_event_record",exc,{"order_id":oid})
    try:
        await asyncio.to_thread(sync_loyalty_profile,u["id"])
    except Exception as exc:
        record_runtime_error("loyalty_sync_order_result",exc,{"user_id":u["id"]})
    if pending:
        for oid in order_ids:
            if not any(x[0] == oid for x in pending):
                continue
            for admin_id in ADMIN_IDS:
                try:
                    order_row=await adb_execute("SELECT game_uid,CASE WHEN COALESCE(account_password,'')<>'' THEN 1 ELSE 0 END AS has_credential FROM orders WHERE id=%s",(oid,),"one")
                    game_uid=(order_row["game_uid"] if order_row else "") or ""
                    credential_line = "🔐 Credential: <b>Encrypted — reveal only when needed</b>" if order_row and order_row.get("has_credential") else "🔐 Credential: <b>Not required</b>"
                    if is_secure_login_support_product(p):
                        admin_text=f"🎮 <b>New Secure Login Order #{oid}</b>\n\n👤 User: <code>{u['tg_id']}</code>\n🎮 Product: {html.escape(p['name'])}\n📦 Qty: 1\n💰 Total: {fmt_money(next((x[2] for x in pending if x[0]==oid), total))}\n💳 Paid via: <b>{html.escape(payment_label)}</b>\n🔐 Bot credentials: <b>Not collected</b>\n\n⏳ <b>Waiting for buyer/support-assisted delivery.</b>"
                    else:
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
    await asyncio.to_thread(ensure_buyer_account,c.from_user.id)
    d=await state.get_data()
    if int(d.get("qty",1)) != 1:
        return await c.answer("Direct live payment currently supports quantity 1. Reduce quantity to 1 first.",show_alert=True)
    return await _start_direct_order_payment(c,state)

@router.callback_query(DirectPaymentState.method,F.data.startswith("orderpay:method:"))
async def direct_payment_method(c:CallbackQuery,state:FSMContext):
    await asyncio.to_thread(ensure_buyer_account,c.from_user.id)
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

def _direct_payment_cancel_tx(payid, oid):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                payment=None
                if payid:
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
    return True


@router.callback_query(DirectPaymentState.trx,F.data=="orderpay:cancel")
@router.callback_query(DirectPaymentState.receipt,F.data=="orderpay:cancel")
async def direct_payment_cancel(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); payid=d.get("direct_payment_id"); oid=d.get("direct_order_id")
    try:
        if payid or oid:
            await asyncio.to_thread(_direct_payment_cancel_tx,payid,oid)
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

def _create_direct_payment_tx(tg_id, pid, qty, amount, method, trx, normalized, game_uid, account_password):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT payment_id FROM payment_trx_claims WHERE normalized_trx_id=%s",(normalized,))
                if cur.fetchone(): raise ValueError("duplicate_trx")
                cur.execute("SELECT * FROM users WHERE tg_id=%s FOR UPDATE",(tg_id,)); user=cur.fetchone()
                cur.execute("SELECT * FROM products WHERE id=%s AND active=1 FOR UPDATE",(pid,)); p=cur.fetchone()
                if not user or not p: raise ValueError("product_unavailable")
                current_amount=float(p["price"])*qty
                amount_ok,_=direct_order_amount_ok(current_amount)
                if not amount_ok: raise ValueError("invalid_amount")
                if abs(current_amount-amount)>0.009: raise ValueError(f"price_changed:{current_amount}")
                code_row=None; reservation_kind="manual"
                if is_auto_code_product(p):
                    reservation_kind="code"
                    cur.execute("SELECT * FROM product_codes WHERE product_id=%s AND status='available' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",(pid,)); code_row=cur.fetchone()
                    if not code_row: raise ValueError("out_of_stock")
                elif int(p.get("stock") or 0) < 1:
                    raise ValueError("out_of_stock")
                stored_account_password = encrypt_credential_cursor(cur, account_password or "") if reservation_kind=="manual" else ""
                cur.execute("INSERT INTO orders(user_id,product_id,game_uid,account_password,total,status,payment_mode,stock_reserved,reservation_kind) VALUES(%s,%s,%s,%s,%s,'awaiting_payment','direct',TRUE,%s) RETURNING id",(user["id"],pid,game_uid or "",stored_account_password,current_amount,reservation_kind))
                oid=cur.fetchone()["id"]
                if reservation_kind=="code":
                    cur.execute("UPDATE product_codes SET status='reserved',sold_to=%s,order_id=%s,sold_at=NULL WHERE id=%s AND status='available'",(user["id"],oid,code_row["id"]))
                    if cur.rowcount != 1: raise RuntimeError("Code reservation changed. Please retry.")
                    sync_code_product_stock(pid,conn)
                else:
                    cur.execute("UPDATE products SET stock=stock-1,updated_at=NOW() WHERE id=%s AND stock>0",(pid,))
                    if cur.rowcount != 1: raise ValueError("out_of_stock")
                cur.execute("INSERT INTO payments(user_id,amount,method,trx_id,status,order_id) VALUES(%s,%s,%s,%s,'pending',%s) RETURNING id",(user["id"],current_amount,method,trx,oid))
                payid=cur.fetchone()["id"]
                assess_payment_fraud(cur,payid,user["id"],current_amount,method,trx)
                cur.execute("INSERT INTO payment_trx_claims(normalized_trx_id,payment_id) VALUES(%s,%s)",(normalized,payid))
                cur.execute("UPDATE orders SET payment_id=%s WHERE id=%s",(payid,oid))
                record_payment_audit(cur,payid,None,"submitted","","pending",current_amount,method,trx,f"Direct payment for Order #{oid}; stock reserved")
                return {"oid":oid,"payid":payid,"amount":current_amount}


@router.message(DirectPaymentState.trx)
async def direct_payment_trx(m:Message,state:FSMContext):
    trx=(m.text or "").strip(); d=await state.get_data()
    if trx.lower()=="/cancel":
        await state.clear(); return await m.answer("❌ Cancelled.")
    if len(trx)<3 or len(trx)>255: return await m.answer("❌ Please send a valid transaction ID.")
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
    if len(normalized)<3: return await m.answer("❌ Please send a valid transaction ID.")
    try:
        result=await asyncio.to_thread(_create_direct_payment_tx,m.from_user.id,pid,qty,amount,method,trx,normalized,d.get("game_uid",""),d.get("account_password",""))
        oid=result["oid"]; payid=result["payid"]; amount=result["amount"]
    except errors.UniqueViolation:
        logging.warning("Duplicate normalized TrxID rejected for user %s", m.from_user.id)
        return await m.answer("❌ This transaction ID has already been submitted.")
    except ValueError as exc:
        reason=str(exc)
        if reason=="duplicate_trx": return await m.answer("❌ This transaction ID has already been submitted.")
        if reason=="product_unavailable": await state.clear(); return await m.answer("❌ Product unavailable. Please restart checkout.")
        if reason=="out_of_stock": await state.clear(); return await m.answer("❌ Product is out of stock. No order was created.")
        if reason=="invalid_amount": await state.clear(); return await m.answer("❌ Direct payment amount is outside the configured limits. Please restart checkout or contact support.")
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
    await m.answer(f"📸 <b>Payment Receipt</b>\n\nOrder: <b>#{oid}</b>\nPayment: <b>#{payid}</b>\n💰 Amount: <b>{fmt_money(amount)}</b>\n📦 Stock: <b>Reserved pending verification</b>\n\nSend a screenshot/photo if available, or use /skip.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭️ Skip Screenshot",callback_data=f"orderpay:skip:{payid}")],[InlineKeyboardButton(text="❌ Cancel",callback_data="orderpay:cancel")]]))

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
    row=await adb_execute("SELECT p.*,o.product_id,o.total AS order_total,o.status AS order_status FROM payments p JOIN orders o ON o.payment_id=p.id WHERE p.id=%s",(payid,),"one")
    if not row: await state.clear(); return await m.answer("❌ Payment request not found.")
    await state.clear()
    try:
        await asyncio.to_thread(record_order_event,oid,"payment_submitted","awaiting_payment","Direct payment submitted; waiting for admin verification.",m.from_user.id)
    except Exception as exc:
        record_runtime_error("order_event_payment_submitted",exc,{"order_id":oid})
    receipt_text="📸 Receipt received" if receipt else "📸 No receipt attached"
    await m.answer(f"⏳ <b>Direct Payment Submitted</b>\n\n🧾 Order: <b>#{oid}</b>\n💳 Payment: <b>#{payid}</b>\n💰 Amount: <b>{fmt_money(row['amount'])}</b>\n💳 Method: <b>{html.escape(row['method'].title())}</b>\n{receipt_text}\n\nWaiting for admin verification.",reply_markup=inline_home_kb())
    receipt=await adb_execute("SELECT 1 FROM payment_receipts WHERE payment_id=%s",(payid,),"one")
    for admin_id in ADMIN_IDS:
        try:
            p=await adb_execute("SELECT name FROM products WHERE id=%s",(row['product_id'],),"one")
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
    if not is_admin(m.from_user.id):
        return await m.answer(
            f"🎮 <b>{html.escape(shop_name())}</b>\n"
            "✅ Service online\n⚡ Fast delivery • 🛡️ Secure checkout"
        )
    await m.answer(
        f"🚀 <b>{html.escape(shop_name())}</b>\n"
        f"🧩 Internal build: <code>{html.escape(APP_VERSION)}</code>\n"
        "☁️ PostgreSQL • 🚆 Railway • 🩺 Live monitoring enabled"
    )

@router.message(Command("track"))
async def track_order_command(m:Message):
    if user_blocked(m.from_user.id) and not is_admin(m.from_user.id):
        return await m.answer("🚫 Your account is blocked.")
    parts=(m.text or "").strip().split(maxsplit=1)
    if len(parts)<2 or not parts[1].strip().lstrip("#").isdigit():
        return await m.answer("🧭 <b>Track an order</b>\n\nUse: <code>/track 123</code>")
    oid=int(parts[1].strip().lstrip("#"))
    u=await aget_user(m.from_user)
    order=await adb_execute(
        """SELECT o.id,o.user_id,o.total,o.status,o.created_at,p.name AS product_name
           FROM orders o JOIN products p ON p.id=o.product_id
           WHERE o.id=%s AND o.user_id=%s""",(oid,u["id"]),"one")
    if not order:
        return await m.answer("❌ Order not found in your account.")
    events=await asyncio.to_thread(order_timeline,oid)
    await m.answer(build_order_tracking_text(order,events),reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh",callback_data=f"order_track:{oid}"),InlineKeyboardButton(text="📦 Details",callback_data=f"order_detail:{oid}")],
        [InlineKeyboardButton(text="📦 Order Center",callback_data="orders_view:all:0")]
    ]))

@router.message(Command("profile"))
@router.message(F.text=="👤 Profile")
@router.message(F.text=="👤 My Account")
async def profile(m:Message):
    u=await aget_user(m.from_user); row=await adb_execute("SELECT COUNT(*) AS c FROM orders WHERE user_id=%s",(u["id"],),"one")
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


@router.callback_query(F.data.startswith("order_track:"))
async def buyer_order_track(c:CallbackQuery):
    u=await aget_user(c.from_user)
    oid=int(c.data.split(":",1)[1])
    order=await adb_execute(
        """SELECT o.id,o.user_id,o.total,o.status,o.created_at,p.name AS product_name
           FROM orders o JOIN products p ON p.id=o.product_id
           WHERE o.id=%s AND o.user_id=%s""",(oid,u["id"]),"one")
    if not order:
        return await c.answer("Order not found.",show_alert=True)
    events=await asyncio.to_thread(order_timeline,oid)
    await c.answer()
    await c.message.edit_text(
        build_order_tracking_text(order,events),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh",callback_data=f"order_track:{oid}")],
            [InlineKeyboardButton(text="📦 Order Details",callback_data=f"order_detail:{oid}")],
            [InlineKeyboardButton(text="⬅️ My Orders",callback_data="home:orders")]
        ]))

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
        rows.append([InlineKeyboardButton(text="🔁 Buy Again",callback_data=f"reorder:{o['id']}")])
    rows.append([InlineKeyboardButton(text="🧭 Track Order",callback_data=f"order_track:{oid}")])
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
            (SELECT COUNT(*) FROM error_events
             WHERE created_at>=NOW()-INTERVAL '24 hours'
               AND resolved=FALSE
               AND COALESCE(severity,'error')<>'benign') errors24,
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


def _safe_public_base_url():
    raw=(os.getenv("PUBLIC_BASE_URL") or "").strip()
    if not raw:
        domain=(os.getenv("RAILWAY_PUBLIC_DOMAIN") or "").strip()
        if domain:
            raw="https://"+domain
    return raw.rstrip("/")


def uddoktapay_config():
    api_key=(os.getenv("UDDOKTAPAY_API_KEY") or "").strip()
    raw_url=(os.getenv("UDDOKTAPAY_API_URL") or os.getenv("UDDOKTAPAY_BASE_URL") or "").strip().rstrip("/")
    # Accept either a base URL or the full checkout endpoint.
    for suffix in ("/api/checkout-v2", "/api/checkout"):
        if raw_url.endswith(suffix):
            raw_url=raw_url[:-len(suffix)]
            break
    enabled_default="1" if (os.getenv("UDDOKTAPAY_ENABLED") or "0").strip().lower() in {"1","true","yes","on"} else "0"
    public_base=_safe_public_base_url()
    email=(os.getenv("UDDOKTAPAY_CUSTOMER_EMAIL") or os.getenv("PAYMENT_CONTACT_EMAIL") or "").strip()
    return {
        "api_key": api_key,
        "base_url": raw_url,
        "public_base": public_base,
        "customer_email": email,
        "enabled": setting("uddoktapay_enabled", enabled_default) == "1",
        "timeout": max(5, min(60, int(os.getenv("UDDOKTAPAY_TIMEOUT_SECONDS", "25") or "25"))),
    }


def uddoktapay_ready():
    cfg=uddoktapay_config()
    return bool(cfg["api_key"] and cfg["base_url"] and cfg["public_base"] and cfg["customer_email"])


def uddoktapay_enabled():
    cfg=uddoktapay_config()
    return bool(cfg["enabled"] and uddoktapay_ready())


def _uddoktapay_json_post(path, payload):
    cfg=uddoktapay_config()
    if not cfg["api_key"] or not cfg["base_url"]:
        raise RuntimeError("UddoktaPay API is not configured")
    endpoint=cfg["base_url"].rstrip("/") + "/" + str(path).lstrip("/")
    body=json.dumps(payload, ensure_ascii=False, separators=(",",":")).encode("utf-8")
    req=Request(
        endpoint,
        data=body,
        method="POST",
        headers={
            "Accept":"application/json",
            "Content-Type":"application/json",
            "RT-UDDOKTAPAY-API-KEY":cfg["api_key"],
            "User-Agent":"NextLevelGamingShopBot/6I",
        },
    )
    try:
        with urlopen(req, timeout=cfg["timeout"]) as resp:
            raw=resp.read(1048576)
            status=getattr(resp, "status", 200)
    except HTTPError as exc:
        raw=exc.read(262144)
        raise RuntimeError(f"UddoktaPay HTTP {exc.code}: {raw.decode('utf-8','replace')[:300]}")
    except URLError as exc:
        raise RuntimeError(f"UddoktaPay connection failed: {exc}")
    if status < 200 or status >= 300:
        raise RuntimeError(f"UddoktaPay HTTP {status}")
    try:
        data=json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError("UddoktaPay returned invalid JSON") from exc
    if not isinstance(data, dict):
        raise RuntimeError("UddoktaPay returned an invalid response")
    return data


def uddoktapay_create_checkout(payment_id, tg_id, full_name, amount, purpose, order_id=None):
    cfg=uddoktapay_config()
    if not uddoktapay_enabled():
        raise RuntimeError("UddoktaPay auto payment is not enabled or not fully configured")
    base=cfg["public_base"]
    payload={
        "full_name": (full_name or f"Telegram {tg_id}")[:120],
        "email": cfg["customer_email"],
        "amount": f"{float(amount):.2f}",
        "metadata": {
            "payment_id": int(payment_id),
            "tg_id": int(tg_id),
            "purpose": str(purpose),
            "order_id": int(order_id) if order_id else None,
            "source": "next_level_gaming_shop_bot",
        },
        "redirect_url": base + "/payments/uddoktapay/return",
        "return_type": "GET",
        "cancel_url": base + "/payments/uddoktapay/cancel",
        "webhook_url": base + "/webhooks/uddoktapay",
    }
    data=_uddoktapay_json_post("/api/checkout-v2", payload)
    if data.get("status") is not True or not data.get("payment_url"):
        raise RuntimeError(str(data.get("message") or "UddoktaPay checkout creation failed")[:300])
    payment_url=str(data["payment_url"]).strip()
    db_execute(
        "UPDATE payments SET provider_name='uddoktapay',provider_checkout_url=%s,provider_payload=%s::jsonb,updated_at=NOW() WHERE id=%s AND status='pending'",
        (payment_url, json.dumps({"checkout_response": data}, ensure_ascii=False), payment_id),
    )
    return payment_url


def uddoktapay_verify_invoice(invoice_id):
    invoice_id=str(invoice_id or "").strip()
    if not invoice_id or len(invoice_id) > 255:
        raise ValueError("invalid invoice id")
    data=_uddoktapay_json_post("/api/verify-payment", {"invoice_id": invoice_id})
    data["_requested_invoice_id"]=invoice_id
    return data


def _uddoktapay_extract_invoice(payload):
    if not isinstance(payload, dict):
        return ""
    direct=payload.get("invoice_id") or payload.get("invoice") or payload.get("payment_id")
    if isinstance(direct, (str,int)) and str(direct).strip():
        return str(direct).strip()
    for key in ("data","payment","payload"):
        nested=payload.get(key)
        if isinstance(nested, dict):
            found=_uddoktapay_extract_invoice(nested)
            if found:
                return found
    return ""


def _uddoktapay_prepare_verified_payment(verified):
    if not isinstance(verified, dict):
        raise ValueError("invalid verification response")
    status=str(verified.get("status") or "").upper()
    if status != "COMPLETED":
        return {"ok":False,"status":status or "UNKNOWN","message":"Payment is not completed yet."}
    metadata=verified.get("metadata") or {}
    if not isinstance(metadata, dict):
        raise ValueError("missing verification metadata")
    try:
        payment_id=int(metadata.get("payment_id"))
    except Exception as exc:
        raise ValueError("missing payment id in provider metadata") from exc
    invoice_id=str(verified.get("invoice_id") or verified.get("_requested_invoice_id") or "").strip()
    provider_trx=str(verified.get("transaction_id") or "").strip()
    if not invoice_id or not provider_trx:
        raise ValueError("provider verification is missing invoice/transaction id")
    try:
        provider_amount=round(float(verified.get("amount")),2)
    except Exception as exc:
        raise ValueError("provider verification has invalid amount") from exc

    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(payment_id,))
                p=cur.fetchone()
                if not p:
                    raise ValueError("payment not found")
                if (p.get("method") or "").lower() != "uddoktapay":
                    raise ValueError("provider/payment method mismatch")
                if round(float(p["amount"]),2) != provider_amount:
                    raise ValueError("provider amount mismatch")
                if p["status"] == "credited":
                    return {"ok":True,"already":True,"payment_id":payment_id,"payment":p}
                if p["status"] != "pending":
                    raise ValueError(f"payment is {p['status']}")
                cur.execute(
                    "SELECT id FROM payments WHERE provider_name='uddoktapay' AND provider_transaction_id=%s AND id<>%s LIMIT 1",
                    (provider_trx,payment_id),
                )
                if cur.fetchone():
                    raise ValueError("provider transaction id already used")
                cur.execute(
                    """UPDATE payments
                       SET provider_name='uddoktapay',provider_invoice_id=%s,provider_transaction_id=%s,
                           provider_verified_at=NOW(),provider_payload=%s::jsonb,
                           review_required=FALSE,review_cleared_at=NOW(),review_cleared_by=NULL,updated_at=NOW()
                       WHERE id=%s AND status='pending'""",
                    (invoice_id,provider_trx,json.dumps(verified,ensure_ascii=False,default=str),payment_id),
                )
                if cur.rowcount != 1:
                    raise RuntimeError("payment state changed during provider verification")
    return {"ok":True,"already":False,"payment_id":payment_id}


def _uddoktapay_notify_credit(result):
    if not result.get("ok"):
        return result
    if result.get("already"):
        return result
    payment_id=int(result["payment_id"])
    credited=_payment_credit_tx(payment_id, None)
    if not credited.get("ok"):
        # Another concurrent webhook/return may already have completed it.
        row=db_execute("SELECT status FROM payments WHERE id=%s",(payment_id,),fetch="one")
        if row and row.get("status")=="credited":
            return {"ok":True,"already":True,"payment_id":payment_id}
        return {"ok":False,"message":credited.get("message","Provider credit failed"),"payment_id":payment_id}

    p=credited["payment"]; direct=credited.get("direct_order"); u=credited.get("user")
    if not p.get("order_id"):
        enqueue_notification(
            u["tg_id"],
            f"✅ <b>Auto Payment Verified</b>\n\n💰 <b>{fmt_money(p['amount'])}</b> added to your wallet automatically.\n💳 Gateway: <b>UddoktaPay</b>\n🧾 Payment: <b>#{payment_id}</b>",
            [[["💰 Wallet","profile"],["🛍️ Shop","home:shop"]]],
        )
        return {"ok":True,"payment_id":payment_id,"kind":"wallet"}

    o,prod,delivered_code,status,u,prod_name=direct
    try:
        if status=="pending":
            record_order_event(o["id"],"payment_verified","pending","UddoktaPay verified payment automatically.",None)
        sync_loyalty_profile(o["user_id"])
    except Exception as exc:
        record_runtime_error("uddoktapay_order_events",exc,{"order_id":o["id"],"payment_id":payment_id})

    provider_result=None
    if status=="pending":
        try:
            provider_result=auto_topup_try_fulfill_order(o["id"])
            if provider_result and provider_result.get("status")=="success":
                status="completed"
        except Exception as exc:
            record_runtime_error("uddoktapay_auto_topup_hook",exc,{"order_id":o["id"],"payment_id":payment_id})

    product_name=prod_name["name"] if prod_name else "Product"
    if status=="completed":
        delivery=(f"🎁 <b>Your Code</b>\n<code>{html.escape(delivered_code or '')}</code>" if delivered_code else "⚡ <b>Order completed.</b>")
        enqueue_notification(
            u["tg_id"],
            f"🎉 <b>Order #{o['id']} Completed</b>\n\n📦 Product: <b>{html.escape(product_name)}</b>\n💳 Auto-paid via <b>UddoktaPay</b>: <b>{fmt_money(o['total'])}</b>\n\n{delivery}",
            [[["📦 Track Order",f"order_track:{o['id']}"],["🛍️ Buy More","home:shop"]]],
        )
    else:
        if is_secure_login_support_product(prod):
            enqueue_notification(
                u["tg_id"],
                f"✅ <b>Order #{o['id']} Payment Verified</b>\n\n"
                f"🎮 Product: <b>{html.escape(product_name)}</b>\n"
                f"💳 UddoktaPay verified: <b>{fmt_money(o['total'])}</b>\n\n"
                f"📩 <b>NEXT STEP:</b> Contact support and send Order ID <b>#{o['id']}</b>.\n"
                f"🔒 No login/password was collected by the bot.",
                [[["💬 Contact Support",f"order_support:{o['id']}"],["📦 Track Order",f"order_track:{o['id']}"]]],
            )
        else:
            enqueue_notification(
                u["tg_id"],
                f"✅ <b>Order #{o['id']} Payment Verified</b>\n\nUddoktaPay verified your payment automatically. Your order is now waiting for manual delivery.",
                [[["📦 Track Order",f"order_track:{o['id']}"]]],
            )
        for admin_id in ADMIN_IDS:
            admin_body=(
                f"⚡ <b>UddoktaPay Auto-Paid Order #{o['id']}</b>\n\n👤 User: <code>{u['tg_id']}</code>\n"
                f"🎮 Product: <b>{html.escape(product_name)}</b>\n💰 Paid: <b>{fmt_money(o['total'])}</b>\n"
                f"🧾 Payment: <b>#{payment_id}</b>\n\n"
                + ("🎮 Buyer must contact support with the Order ID. No bot login credentials were collected."
                   if is_secure_login_support_product(prod)
                   else "Open Orders for manual delivery.")
            )
            enqueue_notification(
                admin_id,
                admin_body,
                [[["🧾 Orders","admin:orders"],["💳 Payments","admin:payments"]]],
            )
    return {"ok":True,"payment_id":payment_id,"order_id":o["id"],"kind":"order","status":status}


def uddoktapay_process_invoice(invoice_id):
    verified=uddoktapay_verify_invoice(invoice_id)
    prepared=_uddoktapay_prepare_verified_payment(verified)
    if not prepared.get("ok"):
        return prepared
    return _uddoktapay_notify_credit(prepared)


def _create_uddoktapay_wallet_payment_tx(tg_id, amount):
    nonce="UP-"+uuid.uuid4().hex.upper()
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users WHERE tg_id=%s FOR UPDATE",(tg_id,))
                u=cur.fetchone()
                if not u:
                    raise ValueError("user_not_found")
                cur.execute(
                    "INSERT INTO payments(user_id,amount,method,trx_id,status,provider_name) VALUES(%s,%s,'uddoktapay',%s,'pending','uddoktapay') RETURNING id",
                    (u["id"],amount,nonce),
                )
                pid=cur.fetchone()["id"]
                record_payment_audit(cur,pid,None,"provider_checkout_created","","pending",amount,"uddoktapay",nonce,"UddoktaPay wallet checkout created")
                return {"payment_id":pid,"user":u}


def payment_method_choice_keyboard():
    rows=[]
    if uddoktapay_enabled():
        rows.append([InlineKeyboardButton(text="⚡ Auto Pay • UddoktaPay",callback_data="payauto:uddoktapay")])
    active=[x for x in payment_method_specs() if payment_method_enabled(x[0])]
    for i in range(0,len(active),2):
        rows.append([InlineKeyboardButton(text=f"{icon} {label}",callback_data=f"paymethod:{code}") for code,label,icon in active[i:i+2]])
    rows.append([InlineKeyboardButton(text="⬅️ Amount Options",callback_data="payamount:back")])
    rows.append([InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def payment_methods_admin_keyboard():
    rows=[]
    for code,label,icon in payment_method_specs():
        state="✅" if payment_method_enabled(code) else "❌"
        rows.append([
            InlineKeyboardButton(text=f"{state} {icon} {label}",callback_data=f"admin:paytoggle:{code}"),
            InlineKeyboardButton(text="✏️",callback_data=f"admin:payedit:{code}"),
        ])
    auto_state="✅" if uddoktapay_enabled() else ("⚠️" if uddoktapay_ready() else "❌")
    rows.append([InlineKeyboardButton(text=f"{auto_state} ⚡ UddoktaPay Auto Gateway",callback_data="admin:uddoktapay")])
    rows.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
    return await c.message.edit_text(custom_text("payment_method_prompt", "💳 <b>Choose a payment method</b>:")+"\n\nUse <b>Auto Pay</b> for provider-verified payment, or choose a manual method.",reply_markup=payment_method_choice_keyboard())

@router.message(PaymentState.amount)
async def payment_amount(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear(); return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."), reply_markup=inline_home_kb())
    amount,error=await validate_deposit_amount(m.text)
    if error: return await m.answer(error,reply_markup=payment_amount_keyboard())
    await state.update_data(amount=amount)
    await state.set_state(PaymentState.method)
    await m.answer(custom_text("payment_method_prompt", "💳 <b>Choose a payment method</b>:")+"\n\nUse <b>Auto Pay</b> for provider-verified payment, or choose a manual method.",reply_markup=payment_method_choice_keyboard())


@router.callback_query(PaymentState.method,F.data=="payauto:uddoktapay")
async def wallet_pay_uddoktapay(c:CallbackQuery,state:FSMContext):
    if not uddoktapay_enabled():
        return await c.answer("Auto payment is not available right now.",show_alert=True)
    d=await state.get_data()
    amount,error=await validate_deposit_amount(d.get("amount"))
    if error:
        await state.clear()
        return await c.answer(error,show_alert=True)
    user=await aget_user(c.from_user)
    try:
        created=await asyncio.to_thread(_create_uddoktapay_wallet_payment_tx,c.from_user.id,amount)
        payid=created["payment_id"]
        checkout_url=await asyncio.to_thread(
            uddoktapay_create_checkout,payid,c.from_user.id,user.get("name") or c.from_user.full_name,
            amount,"wallet_deposit",None
        )
    except Exception as exc:
        if 'payid' in locals():
            try:
                await adb_execute("UPDATE payments SET status='cancelled',updated_at=NOW() WHERE id=%s AND status='pending'",(payid,))
            except Exception:
                pass
        error_id=record_runtime_error("uddoktapay_wallet_checkout",exc,{"user_id":c.from_user.id,"amount":amount})
        await state.clear()
        return await c.answer(f"Auto-payment checkout failed. Ref: {error_id}",show_alert=True)
    await state.clear()
    await c.answer()
    await c.message.edit_text(
        f"⚡ <b>UddoktaPay Auto Payment</b>\n\n💰 Wallet top-up: <b>{fmt_money(amount)}</b>\n🧾 Payment: <b>#{payid}</b>\n\n"
        "Complete payment on the checkout page. After UddoktaPay confirms it, your bot wallet will be credited automatically.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Pay Now",url=checkout_url)],
            [InlineKeyboardButton(text="🔄 Check Payment",callback_data=f"uddoktapay:check:{payid}")],
            [InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")],
        ])
    )


@router.callback_query(F.data.startswith("uddoktapay:check:"))
async def uddoktapay_check_payment(c:CallbackQuery):
    try:
        pid=int(c.data.rsplit(":",1)[1])
    except Exception:
        return await c.answer("Invalid payment.",show_alert=True)
    row=await adb_execute(
        "SELECT p.*,u.tg_id FROM payments p JOIN users u ON u.id=p.user_id WHERE p.id=%s",
        (pid,),"one"
    )
    if not row or int(row["tg_id"])!=c.from_user.id:
        return await c.answer("Payment not found.",show_alert=True)
    if row["status"]=="credited":
        return await c.answer("✅ Payment already verified and credited.",show_alert=True)
    invoice=(row.get("provider_invoice_id") or "").strip()
    if not invoice:
        return await c.answer("Payment is still waiting for UddoktaPay confirmation. Complete checkout first.",show_alert=True)
    try:
        result=await asyncio.to_thread(uddoktapay_process_invoice,invoice)
    except Exception as exc:
        error_id=record_runtime_error("uddoktapay_manual_check",exc,{"payment_id":pid,"user_id":c.from_user.id})
        return await c.answer(f"Could not verify yet. Ref: {error_id}",show_alert=True)
    if result.get("ok"):
        return await c.answer("✅ Payment verified.",show_alert=True)
    return await c.answer("⏳ Payment is not completed yet.",show_alert=True)


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

def _create_wallet_payment_tx(user_id, amount, method, trx, normalized):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id,status FROM payments WHERE lower(regexp_replace(trx_id,'\\s','','g'))=%s LIMIT 1",(normalized,))
                duplicate=cur.fetchone()
                if duplicate:
                    return {"duplicate": duplicate}
                cur.execute("INSERT INTO payments(user_id,amount,method,trx_id) VALUES(%s,%s,%s,%s) RETURNING id",(user_id,amount,method,trx))
                row=cur.fetchone(); payment_id=row["id"]
                assess_payment_fraud(cur,payment_id,user_id,amount,method,trx)
                record_payment_audit(cur,payment_id,None,"submitted","", "pending",amount,method,trx,"User submitted payment")
                return {"payment_id": payment_id}


@router.message(PaymentState.trx)
async def payment_trx(m:Message,state:FSMContext):
    if maintenance_active() and not is_admin(m.from_user.id):
        await state.clear(); return await m.answer(custom_text("maintenance_message", "🔧 Shop is temporarily under maintenance. Please try again later."), reply_markup=inline_home_kb())
    trx=(m.text or "").strip(); normalized=normalize_trx_id(trx)
    if len(normalized)<3 or len(normalized)>120: return await m.answer("❌ Invalid transaction ID / hash.")
    d=await state.get_data(); u=await aget_user(m.from_user); method=d.get("method"); amount=d.get("amount")
    if not method or not amount or not payment_method_enabled(method):
        await state.clear(); return await m.answer("❌ Payment session expired or method unavailable. Please start again.",reply_markup=inline_home_kb())
    ok, limit_error=payment_amount_limits_ok(amount)
    if not ok:
        await state.clear(); return await m.answer(f"❌ {limit_error}",reply_markup=inline_home_kb())
    try:
        result=await asyncio.to_thread(_create_wallet_payment_tx,u["id"],amount,method,trx,normalized)
        if result.get("duplicate"):
            duplicate=result["duplicate"]
            return await m.answer(f"❌ This transaction ID was already submitted (Payment #{duplicate['id']}, status: {duplicate['status']}). Please verify the TxID/Hash.")
        payment_id=result["payment_id"]
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
    row=await adb_execute("SELECT * FROM payments WHERE id=%s",(payment_id,),"one")
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
                receipt_row=await adb_execute("SELECT file_id FROM payment_receipts WHERE payment_id=%s",(payment_id,),"one")
                if receipt_row: await m.bot.send_document(admin_id,receipt_row["file_id"],caption=f"📸 Payment #{payment_id} receipt",reply_markup=kb)
        except Exception as exc:
            record_runtime_error("payment_admin_notification", exc, {"payment_id": payment_id, "admin_id": admin_id})
            logging.exception("Failed to send payment #%s notification to admin %s", payment_id, admin_id)

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
    if not _feature_on("feature_rewards") and not is_admin(m.from_user.id):
        return await m.answer("⭐ Rewards are currently disabled by admin.",reply_markup=premium_home_kb())
    u=await aget_user(m.from_user)
    refs=await adb_execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=%s",(u["id"],),"one")
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
    if not _feature_on("feature_referral") and not is_admin(m.from_user.id):
        return await m.answer("🤝 Referral is currently disabled by admin.",reply_markup=premium_home_kb())
    u=await aget_user(m.from_user)
    me=await m.bot.get_me()
    link=f"https://t.me/{me.username}?start=ref_{m.from_user.id}" if me.username else f"Use /start ref_{m.from_user.id}"
    refs=await adb_execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=%s",(u["id"],),"one")
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
async def support(m:Message):
    if not _feature_on("feature_support") and not is_admin(m.from_user.id):
        return await m.answer("🆘 Support is currently disabled by admin.",reply_markup=premium_home_kb())
    await m.answer(
      "🆘 <b>Support Center</b>\n\nChoose the issue type:",
      reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Payment",callback_data="support_new:payment"),
         InlineKeyboardButton(text="📦 Delivery",callback_data="support_new:delivery")],
        [InlineKeyboardButton(text="👤 Account",callback_data="support_new:account"),
         InlineKeyboardButton(text="❓ Other",callback_data="support_new:other")],
        [InlineKeyboardButton(text="🎧 Contact Support",url=f"https://t.me/{setting('support',SUPPORT).lstrip('@')}" if setting('support',SUPPORT).startswith('@') else "https://t.me/Telegram")]
      ]))


@router.callback_query(F.data.startswith("support_new:"))
async def support_ticket_start(c:CallbackQuery,state:FSMContext):
    if not _feature_on("feature_support") and not is_admin(c.from_user.id):
        return await c.answer("Support is currently disabled by admin.",show_alert=True)
    if user_blocked(c.from_user.id) and not is_admin(c.from_user.id):
        return await c.answer("Account blocked.",show_alert=True)
    category=c.data.rsplit(":",1)[1]
    if category not in {"payment","delivery","account","other"}:
        return await c.answer("Invalid support category.",show_alert=True)
    priority="high" if category=="payment" else "normal"
    await state.update_data(support_category=category,support_priority=priority)
    await state.set_state(AdminState.support_ticket_message)
    await c.answer()
    await c.message.answer(
      f"🆘 <b>{html.escape(category.title())} Support</b>\n\n"
      "Describe the problem in one message (max 1500 characters).\nSend /cancel to stop.")


@router.message(AdminState.support_ticket_message)
async def support_ticket_receive(m:Message,state:FSMContext):
    if m.text and m.text.casefold()=="/cancel":
        await state.clear(); return await m.answer("Support request cancelled.")
    body=(m.text or "").strip()
    if not body or len(body)>1500:
        return await m.answer("❌ Please send 1–1500 characters.")
    u=await aget_user(m.from_user); d=await state.get_data()
    row=await adb_execute(
      """INSERT INTO customer_support_tickets(user_id,category,priority,message)
         VALUES(%s,%s,%s,%s) RETURNING id""",
      (u["id"],d["support_category"],d["support_priority"],body),"one")
    await state.clear()
    if not row: return await m.answer("Could not create support ticket. Please retry.")
    tid=int(row["id"])
    for admin_id in ADMIN_IDS:
        try:
            await m.bot.send_message(
              admin_id,
              f"🆘 <b>New Support Ticket #{tid}</b>\n"
              f"Category: <b>{html.escape(d['support_category'].title())}</b> • Priority: <b>{html.escape(d['support_priority'].upper())}</b>\n"
              f"Buyer: <code>{m.from_user.id}</code>",
              reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🆘 Open Ticket",callback_data=f"customer_ticket:{tid}")
              ]]))
        except Exception as exc:
            record_runtime_error("customer_support_notify",exc,{"ticket_id":tid})
    await m.answer(f"✅ <b>Support Ticket #{tid} Created</b>\nWe will review your request.")


def customer_support_snapshot():
    summary=db_execute("""SELECT
      COUNT(*) FILTER(WHERE status='open') AS open_count,
      COUNT(*) FILTER(WHERE status='open' AND priority='high') AS high_count,
      COUNT(*) FILTER(WHERE status='open' AND created_at<NOW()-INTERVAL '30 minutes') AS aged_count
      FROM customer_support_tickets""",fetch="one") or {}
    rows=db_execute("""SELECT st.*,u.tg_id FROM customer_support_tickets st
      JOIN users u ON u.id=st.user_id
      ORDER BY CASE WHEN st.status='open' THEN 0 ELSE 1 END,
               CASE st.priority WHEN 'high' THEN 0 ELSE 1 END,
               st.updated_at DESC LIMIT 20""",fetch="all") or []
    return summary,rows


@router.callback_query(F.data=="admin:customer_support")
async def admin_customer_support(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    summary,rows=await asyncio.to_thread(customer_support_snapshot)
    msg=(f"🎧 <b>Customer Support Tickets</b>\n\n"
         f"🔴 Open: <b>{summary.get('open_count',0)}</b> • 🔥 High: <b>{summary.get('high_count',0)}</b> • "
         f"⏳ Aged: <b>{summary.get('aged_count',0)}</b>")
    kb=[]
    for r in rows:
        icon="🔴" if r["status"]=="open" else "✅"
        kb.append([InlineKeyboardButton(
          text=f"{icon} #{r['id']} • {r['category'].title()} • {r['priority'].upper()}",
          callback_data=f"customer_ticket:{r['id']}")])
    kb.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")])
    await c.answer(); await c.message.edit_text(msg,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("customer_ticket:"))
async def admin_customer_ticket(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    tid=int(c.data.split(":",1)[1])
    r=await asyncio.to_thread(db_execute,
      """SELECT st.*,u.tg_id FROM customer_support_tickets st JOIN users u ON u.id=st.user_id WHERE st.id=%s""",
      (tid,),"one")
    if not r: return await c.answer("Ticket not found.",show_alert=True)
    msg=(f"🆘 <b>Ticket #{tid}</b>\n"
         f"Status: <b>{html.escape(r['status'].upper())}</b> • Priority: <b>{html.escape(r['priority'].upper())}</b>\n"
         f"Category: <b>{html.escape(r['category'].title())}</b>\nBuyer: <code>{r['tg_id']}</code>\n\n"
         f"📝 {html.escape(r['message'])}\n\n"
         f"👑 Admin note: {html.escape(r.get('admin_note') or 'None')}")
    kb=[[InlineKeyboardButton(text="📝 Admin Note",callback_data=f"customer_ticket_note:{tid}")],
        [InlineKeyboardButton(text="🧩 Customer CRM",callback_data=f"crm:user:{r['user_id']}"),InlineKeyboardButton(text="🧭 Timeline",callback_data=f"crm:timeline:{r['user_id']}")]]
    if r["status"]=="open":
        kb.append([InlineKeyboardButton(text="✅ Resolve + Notify",callback_data=f"customer_ticket_resolve:{tid}")])
    kb.append([InlineKeyboardButton(text="⬅️ Tickets",callback_data="admin:customer_support")])
    await c.answer(); await c.message.edit_text(msg,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("customer_ticket_note:"))
async def customer_ticket_note_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    tid=int(c.data.rsplit(":",1)[1])
    await state.update_data(customer_ticket_id=tid)
    await state.set_state(AdminState.support_ticket_note)
    await c.answer(); await c.message.answer(f"📝 Send admin note for ticket <b>#{tid}</b>.")


@router.message(AdminState.support_ticket_note)
async def customer_ticket_note_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return await state.clear()
    note=(m.text or "").strip()
    if not note or len(note)>1500: return await m.answer("❌ Note must be 1–1500 characters.")
    d=await state.get_data(); tid=int(d["customer_ticket_id"])
    row=await adb_execute("""UPDATE customer_support_tickets
      SET admin_note=%s,assigned_admin=%s,updated_at=NOW() WHERE id=%s RETURNING id""",
      (note,m.from_user.id,tid),"one")
    await state.clear()
    if not row: return await m.answer("Ticket not found.")
    await asyncio.to_thread(admin_log,m.from_user.id,"customer_support_note",f"ticket={tid}")
    await m.answer("✅ Note saved.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
      InlineKeyboardButton(text="🆘 Open Ticket",callback_data=f"customer_ticket:{tid}")]]))


@router.callback_query(F.data.startswith("customer_ticket_resolve:"))
async def customer_ticket_resolve(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    tid=int(c.data.rsplit(":",1)[1])
    row=await adb_execute("""UPDATE customer_support_tickets SET status='resolved',
      resolved_at=NOW(),resolved_by=%s,assigned_admin=COALESCE(assigned_admin,%s),updated_at=NOW()
      WHERE id=%s AND status='open' RETURNING user_id,admin_note""",
      (c.from_user.id,c.from_user.id,tid),"one")
    if not row: return await c.answer("Ticket already resolved or not found.",show_alert=True)
    u=await adb_execute("SELECT tg_id FROM users WHERE id=%s",(row["user_id"],),"one")
    if u:
        msg=f"✅ <b>Support Ticket #{tid} Resolved</b>\nYour support request has been reviewed."
        if row.get("admin_note"): msg+=f"\n\n📝 Admin note: {html.escape(row['admin_note'])}"
        await asyncio.to_thread(enqueue_notification,u["tg_id"],msg,[[["🏠 Main Menu","main_menu"]]])
    await asyncio.to_thread(admin_log,c.from_user.id,"customer_support_resolved",f"ticket={tid}")
    await c.answer("Resolved.")
    c.data=f"customer_ticket:{tid}"
    return await admin_customer_ticket(c)


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
    await aadmin_log(m.from_user.id,"announcement_update",value)
    await m.answer("✅ Premium home announcement updated. Users will see it on /start.")

# ---------------- Admin ----------------
@router.message(Command("admin"))
async def admin_command(m:Message):
    if not is_admin(m.from_user.id): return await m.answer("❌ Access denied.")
    await m.answer(f"👑 <b>{html.escape(shop_name())} {APP_VERSION}</b>\nAdmin Control Center",reply_markup=admin_menu())


def customer_segment_counts():
    return db_execute("""
      SELECT
        COUNT(*) FILTER (WHERE created_at>=NOW()-INTERVAL '30 days') AS new_users,
        COUNT(*) FILTER (WHERE id IN (SELECT user_id FROM orders WHERE status='completed' GROUP BY user_id HAVING COUNT(*)>=3)) AS regular,
        COUNT(*) FILTER (WHERE id IN (SELECT user_id FROM orders WHERE status='completed' GROUP BY user_id HAVING COALESCE(SUM(total),0)>=5000)) AS vip,
        COUNT(*) FILTER (WHERE id IN (SELECT user_id FROM orders WHERE status='completed' GROUP BY user_id HAVING COALESCE(SUM(total),0)>=10000)) AS high_spend,
        COUNT(*) FILTER (WHERE id NOT IN (SELECT DISTINCT user_id FROM orders WHERE created_at>=NOW()-INTERVAL '60 days')) AS inactive
      FROM users
    """,fetch="one") or {}


def customer_segment_tg_ids(segment):
    segment=(segment or "").casefold()
    sql_map={
      "new": "SELECT tg_id FROM users WHERE created_at>=NOW()-INTERVAL '30 days'",
      "regular": "SELECT u.tg_id FROM users u WHERE u.id IN (SELECT user_id FROM orders WHERE status='completed' GROUP BY user_id HAVING COUNT(*)>=3)",
      "vip": "SELECT u.tg_id FROM users u WHERE u.id IN (SELECT user_id FROM orders WHERE status='completed' GROUP BY user_id HAVING COALESCE(SUM(total),0)>=5000)",
      "high": "SELECT u.tg_id FROM users u WHERE u.id IN (SELECT user_id FROM orders WHERE status='completed' GROUP BY user_id HAVING COALESCE(SUM(total),0)>=10000)",
      "inactive": "SELECT tg_id FROM users WHERE id NOT IN (SELECT DISTINCT user_id FROM orders WHERE created_at>=NOW()-INTERVAL '60 days')",
    }
    if segment not in sql_map: return []
    return [int(r["tg_id"]) for r in (db_execute(sql_map[segment],fetch="all") or [])]


@router.callback_query(F.data=="admin:segments")
async def admin_segments(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    s=await asyncio.to_thread(customer_segment_counts)
    msg=("🎯 <b>Customer Segments</b>\n\n"
         f"🆕 New 30d: <b>{s.get('new_users',0)}</b>\n"
         f"🔁 Regular 3+ orders: <b>{s.get('regular',0)}</b>\n"
         f"⭐ VIP spend 5,000+: <b>{s.get('vip',0)}</b>\n"
         f"💎 High spend 10,000+: <b>{s.get('high_spend',0)}</b>\n"
         f"🌙 Inactive 60d: <b>{s.get('inactive',0)}</b>")
    kb=InlineKeyboardMarkup(inline_keyboard=[
      [InlineKeyboardButton(text="🆕 New",callback_data="admin:seg:new"),InlineKeyboardButton(text="🔁 Regular",callback_data="admin:seg:regular")],
      [InlineKeyboardButton(text="⭐ VIP",callback_data="admin:seg:vip"),InlineKeyboardButton(text="💎 High Spend",callback_data="admin:seg:high")],
      [InlineKeyboardButton(text="🌙 Inactive",callback_data="admin:seg:inactive")],
      [InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]
    ])
    await c.answer(); await c.message.edit_text(msg,reply_markup=kb)


@router.callback_query(F.data.startswith("admin:seg:"))
async def admin_segment_prepare(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    seg=c.data.rsplit(":",1)[1]
    ids=await asyncio.to_thread(customer_segment_tg_ids,seg)
    if not ids: return await c.answer("No users in this segment.",show_alert=True)
    await state.update_data(segment=seg)
    await state.set_state(AdminState.segment_broadcast)
    await c.answer()
    await c.message.answer(f"📣 Target: <b>{html.escape(seg.title())}</b> • <b>{len(ids)}</b> user(s)\n\nSend offer text.")


@router.message(AdminState.segment_broadcast)
async def admin_segment_broadcast_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return
    body=(m.text or "").strip()
    if not body or len(body)>3000: return await m.answer("❌ Message must be 1–3000 characters.")
    d=await state.get_data()
    tg_ids=await asyncio.to_thread(customer_segment_tg_ids,d.get("segment"))
    queued=0
    for tg_id in tg_ids[:5000]:
        try:
            await asyncio.to_thread(enqueue_notification,tg_id,body,None)
            queued+=1
        except Exception as exc:
            record_runtime_error("segment_offer_queue",exc,{"tg_id":tg_id})
    await asyncio.to_thread(admin_log,m.from_user.id,"segment_offer_queued",f"segment={d.get('segment')} queued={queued}")
    await state.clear()
    await m.answer(f"✅ Offer queued for <b>{queued}</b> user(s).")


def active_flash_price(product_id):
    return db_execute("""SELECT sale_price,ends_at FROM flash_sales
                         WHERE product_id=%s AND active=1 AND starts_at<=NOW() AND ends_at>NOW()
                         ORDER BY sale_price ASC,id DESC LIMIT 1""",(product_id,),"one")


@router.callback_query(F.data=="admin:flash_sales")
async def admin_flash_sales(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    c.data="admin:offers_v2"
    return await admin_offers_v2(c)


def quick_reorder_candidate(user_id,order_id):
    return db_execute("""SELECT o.id,o.product_id,o.status,p.active,
                         CASE WHEN p.delivery_type='code'
                              THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available')
                              ELSE p.stock END AS effective_stock
                         FROM orders o JOIN products p ON p.id=o.product_id
                         WHERE o.id=%s AND o.user_id=%s""",(order_id,user_id),"one")


@router.callback_query(F.data.startswith("reorder:"))
async def buyer_quick_reorder(c:CallbackQuery,state:FSMContext):
    u=await aget_user(c.from_user)
    oid=int(c.data.split(":",1)[1])
    row=await asyncio.to_thread(quick_reorder_candidate,u["id"],oid)
    if not row or row["status"]!="completed": return await c.answer("This order cannot be reordered.",show_alert=True)
    if not int(row["active"] or 0) or int(row["effective_stock"] or 0)<=0: return await c.answer("This product is currently unavailable.",show_alert=True)
    await c.answer()
    c.data=f"buy:{row['product_id']}"
    return await buy(c,state)




def daily_business_summary_snapshot():
    totals=db_execute("""SELECT
      COUNT(*) FILTER(WHERE status='completed' AND created_at>=NOW()-INTERVAL '24 hours') completed_orders,
      COALESCE(SUM(total) FILTER(WHERE status='completed' AND created_at>=NOW()-INTERVAL '24 hours'),0) revenue24,
      COUNT(*) FILTER(WHERE status='refund_pending') refund_pending,
      COUNT(*) FILTER(WHERE status='awaiting_payment') awaiting_payment
      FROM orders""",fetch="one") or {}

    users=db_execute("""SELECT
      COUNT(*) FILTER(WHERE created_at>=NOW()-INTERVAL '24 hours') new_users24,
      COUNT(*) total_users
      FROM users""",fetch="one") or {}

    payments=db_execute("""SELECT
      COUNT(*) FILTER(WHERE status='pending') pending_payments,
      COUNT(*) FILTER(WHERE status='rejected' AND created_at>=NOW()-INTERVAL '24 hours') rejected24
      FROM payments""",fetch="one") or {}

    stock=db_execute("""SELECT COUNT(*) AS low_stock FROM (
      SELECT p.id,
        CASE WHEN p.delivery_type='code'
          THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available')
          ELSE p.stock END AS effective_stock
      FROM products p WHERE p.active=1
    ) x WHERE effective_stock<=3""",fetch="one") or {}

    top=db_execute("""SELECT p.name,COUNT(*) qty,COALESCE(SUM(o.total),0) revenue
      FROM orders o JOIN products p ON p.id=o.product_id
      WHERE o.status='completed' AND o.created_at>=NOW()-INTERVAL '24 hours'
      GROUP BY p.id,p.name ORDER BY revenue DESC,qty DESC LIMIT 5""",fetch="all") or []

    errors=db_execute("""SELECT COUNT(*) open_errors FROM error_events
      WHERE resolved=FALSE AND COALESCE(severity,'error')<>'benign'""",fetch="one") or {}

    backup=backup_health_snapshot()
    return {
      "totals":totals,"users":users,"payments":payments,"stock":stock,
      "top":top,"errors":errors,"backup":backup,
    }


def render_daily_business_summary(snap):
    t=snap["totals"]; u=snap["users"]; p=snap["payments"]; st=snap["stock"]; er=snap["errors"]
    top=snap["top"]; b=snap["backup"]
    top_text="No completed sales in last 24h." if not top else "\n".join(
      f"• {html.escape(r['name'])}: {r['qty']} order(s) • {fmt_money(r['revenue'])}" for r in top)
    return (
      "📊 <b>Daily Business Summary</b>\n\n"
      f"💰 Revenue 24h: <b>{fmt_money(t.get('revenue24',0))}</b>\n"
      f"✅ Completed orders: <b>{t.get('completed_orders',0)}</b>\n"
      f"👤 New users: <b>{u.get('new_users24',0)}</b> • Total: <b>{u.get('total_users',0)}</b>\n"
      f"💳 Pending payments: <b>{p.get('pending_payments',0)}</b> • Rejected 24h: <b>{p.get('rejected24',0)}</b>\n"
      f"⏳ Awaiting payment orders: <b>{t.get('awaiting_payment',0)}</b>\n"
      f"💸 Refund pending: <b>{t.get('refund_pending',0)}</b>\n"
      f"📦 Low stock ≤3: <b>{st.get('low_stock',0)}</b>\n"
      f"🚨 Open actionable errors: <b>{er.get('open_errors',0)}</b>\n"
      f"💾 Backup: <b>{html.escape(str(b.get('status','unknown')).upper())}</b> • {html.escape(str(b.get('last_at','never')))}\n\n"
      f"🏆 <b>Top Products</b>\n{top_text}"
    )


@router.callback_query(F.data=="admin:daily_summary")
async def admin_daily_summary(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    snap=await asyncio.to_thread(daily_business_summary_snapshot)
    await c.answer()
    await c.message.edit_text(
      render_daily_business_summary(snap),
      reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:daily_summary"),
         InlineKeyboardButton(text="🚆 Railway Live",callback_data="admin:live_status")],
        [InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]
      ]))


async def daily_business_summary_loop(bot):
    """Leader-only once-per-day owner summary; de-duplicated via runtime setting date."""
    await asyncio.sleep(60)
    while True:
        try:
            if runtime_state_snapshot().get("role")=="leader":
                today=datetime.now(timezone.utc).date().isoformat()
                last=setting("ops_daily_summary_last_date","")
                if last!=today:
                    snap=await asyncio.to_thread(daily_business_summary_snapshot)
                    msg=render_daily_business_summary(snap)
                    sent=False
                    for admin_id in ADMIN_IDS:
                        try:
                            await bot.send_message(admin_id,msg,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                              InlineKeyboardButton(text="📊 Open Summary",callback_data="admin:daily_summary"),
                              InlineKeyboardButton(text="🩺 Diagnostics",callback_data="admin:diagnostics")
                            ]]))
                            sent=True
                        except Exception as exc:
                            record_runtime_error("daily_business_summary_delivery",exc,{"admin_id":admin_id})
                    if sent:
                        await asyncio.to_thread(set_setting,"ops_daily_summary_last_date",today)
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            record_runtime_error("daily_business_summary_loop",exc,{"instance_id":INSTANCE_ID})
            await asyncio.sleep(3600)


@router.callback_query(F.data=="admin:monitoring_history")
async def admin_monitoring_history(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    s=await asyncio.to_thread(ops_health_history_snapshot,24)
    await c.answer()
    await c.message.edit_text(
      "📈 <b>24h Monitoring History</b>\n\n"
      f"Samples: <b>{s.get('samples',0)}</b>\n"
      f"Avg health: <b>{s.get('avg_health') or 0}</b> • Min: <b>{s.get('min_health') or 0}</b>\n"
      f"Avg DB latency: <b>{s.get('avg_db_ms') or 0} ms</b> • Max: <b>{s.get('max_db_ms') or 0} ms</b>\n"
      f"Max queue pending: <b>{s.get('max_queue_pending') or 0}</b>\n"
      f"Max queue failed: <b>{s.get('max_queue_failed') or 0}</b>\n"
      f"Max errors/15m: <b>{s.get('max_errors15') or 0}</b>",
      reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚆 Railway Live",callback_data="admin:live_status"),
         InlineKeyboardButton(text="🩺 Diagnostics",callback_data="admin:diagnostics")],
        [InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]
      ]))



def loyalty_admin_snapshot():
    return db_execute(
        """SELECT lp.user_id,lp.tier,lp.points,lp.lifetime_spend,lp.completed_orders,u.tg_id,u.name
           FROM loyalty_profiles lp JOIN users u ON u.id=lp.user_id
           ORDER BY CASE lp.tier WHEN 'VIP' THEN 0 WHEN 'Gold' THEN 1 WHEN 'Silver' THEN 2 ELSE 3 END,
                    lp.lifetime_spend DESC LIMIT 50""",fetch="all") or []


@router.callback_query(F.data=="admin:loyalty")
async def admin_loyalty(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    rows=await asyncio.to_thread(loyalty_admin_snapshot)
    counts=collections.Counter(r["tier"] for r in rows)
    msg=(
        "🎁 <b>Loyalty / VIP Center</b>\n\n"
        f"VIP: <b>{counts.get('VIP',0)}</b> • Gold: <b>{counts.get('Gold',0)}</b> • "
        f"Silver: <b>{counts.get('Silver',0)}</b> • Bronze: <b>{counts.get('Bronze',0)}</b>\n\nTop customers:\n"
    )
    msg += "\n".join(
        f"• {html.escape(r.get('name') or str(r['tg_id']))[:24]} — <b>{r['tier']}</b> • "
        f"{fmt_money(r['lifetime_spend'])} • {r['completed_orders']} orders"
        for r in rows[:15]) if rows else "No loyalty profiles yet."
    await c.answer()
    await c.message.edit_text(msg,reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Rebuild Profiles",callback_data="admin:loyalty_rebuild")],
        [InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]
    ]))


@router.callback_query(F.data=="admin:loyalty_rebuild")
async def admin_loyalty_rebuild(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    ids=await adb_execute("SELECT id FROM users",(),"all") or []
    rebuilt=0
    for r in ids[:10000]:
        try:
            await asyncio.to_thread(sync_loyalty_profile,int(r["id"]))
            rebuilt+=1
        except Exception as exc:
            record_runtime_error("loyalty_rebuild",exc,{"user_id":r["id"]})
    await asyncio.to_thread(admin_log,c.from_user.id,"loyalty_rebuild",f"profiles={rebuilt}")
    await c.answer(f"Rebuilt {rebuilt} profile(s).")
    c.data="admin:loyalty"
    return await admin_loyalty(c)


def business_analytics_snapshot(days=7):
    days=max(1,min(365,int(days)))
    interval=f"{days} days"

    totals=db_execute(
        """SELECT
             COUNT(*) FILTER(WHERE status='completed') AS completed_orders,
             COALESCE(SUM(total) FILTER(WHERE status='completed'),0) AS revenue,
             COALESCE(AVG(total) FILTER(WHERE status='completed'),0) AS aov,
             COUNT(*) FILTER(WHERE status='pending') AS pending,
             COUNT(*) FILTER(WHERE status='awaiting_payment') AS awaiting_payment,
             COUNT(*) FILTER(WHERE status='refund_pending') AS refund_pending,
             COUNT(*) FILTER(WHERE status='refunded') AS refunded,
             COUNT(*) FILTER(WHERE status IN ('rejected','cancelled')) AS rejected_cancelled
           FROM orders
           WHERE created_at>=NOW()-(%s::interval)""",
        (interval,),"one") or {}

    buyers=db_execute(
        """WITH period_buyers AS (
             SELECT DISTINCT user_id
             FROM orders
             WHERE status='completed' AND created_at>=NOW()-(%s::interval)
           ),
           returning_buyers_cte AS (
             SELECT pb.user_id
             FROM period_buyers pb
             WHERE EXISTS(
               SELECT 1 FROM orders old
               WHERE old.user_id=pb.user_id
                 AND old.status='completed'
                 AND old.created_at<NOW()-(%s::interval)
             )
           )
           SELECT
             (SELECT COUNT(*) FROM period_buyers) AS unique_buyers,
             (SELECT COUNT(*) FROM returning_buyers_cte) AS returning_buyers""",
        (interval,interval),"one") or {}
    unique_buyers=int(buyers.get("unique_buyers") or 0)
    returning=int(buyers.get("returning_buyers") or 0)
    buyers["new_buyers"]=max(0,unique_buyers-returning)

    users=db_execute(
        """SELECT COUNT(*) FILTER(WHERE created_at>=NOW()-(%s::interval)) AS new_users,
                  COUNT(*) AS total_users
           FROM users""",(interval,),"one") or {}

    top_products=db_execute(
        """SELECT p.name,p.category,COUNT(*) AS orders,COALESCE(SUM(o.total),0) AS revenue
           FROM orders o JOIN products p ON p.id=o.product_id
           WHERE o.status='completed' AND o.created_at>=NOW()-(%s::interval)
           GROUP BY p.id,p.name,p.category
           ORDER BY revenue DESC,orders DESC LIMIT 8""",
        (interval,),"all") or []

    top_games=db_execute(
        """SELECT split_part(p.category,' > ',1) AS game,
                  COUNT(*) AS orders,COALESCE(SUM(o.total),0) AS revenue
           FROM orders o JOIN products p ON p.id=o.product_id
           WHERE o.status='completed' AND o.created_at>=NOW()-(%s::interval)
           GROUP BY split_part(p.category,' > ',1)
           ORDER BY revenue DESC,orders DESC LIMIT 8""",
        (interval,),"all") or []

    vip=db_execute(
        """SELECT tier,COUNT(*) AS c,COALESCE(SUM(lifetime_spend),0) AS spend
           FROM loyalty_profiles GROUP BY tier""",fetch="all") or []

    offers=db_execute(
        """SELECT COUNT(*) FILTER(WHERE active=1 AND sale_price IS NOT NULL AND sale_until>NOW()) AS active_offers,
                  COUNT(*) FILTER(WHERE active=1 AND featured=1) AS featured,
                  COUNT(*) FILTER(WHERE active=1 AND hot=1) AS hot,
                  COUNT(*) FILTER(WHERE active=1 AND best_seller=1) AS best_sellers
           FROM products""",fetch="one") or {}

    active_offer_sales=db_execute(
        """SELECT COUNT(*) AS orders,COALESCE(SUM(o.total),0) AS revenue
           FROM orders o JOIN products p ON p.id=o.product_id
           WHERE o.status='completed'
             AND o.created_at>=NOW()-(%s::interval)
             AND p.sale_price IS NOT NULL AND p.sale_until>NOW()""",
        (interval,),"one") or {}

    return {
        "days":days,"totals":totals,"buyers":buyers,"users":users,
        "top_products":top_products,"top_games":top_games,
        "vip":vip,"offers":offers,"active_offer_sales":active_offer_sales,
    }


def render_business_analytics(snap):
    d=int(snap["days"]); t=snap["totals"]; b=snap["buyers"]; u=snap["users"]
    vip={r["tier"]:r for r in snap["vip"]}
    tops=snap["top_products"]; games=snap["top_games"]; offers=snap["offers"]; offer_sales=snap["active_offer_sales"]

    product_text="• No completed sales." if not tops else "\n".join(
        f"• {html.escape(r['name'])}: {r['orders']} order(s) • {fmt_money(r['revenue'])}"
        for r in tops[:5])
    game_text="• No completed game sales." if not games else "\n".join(
        f"• {html.escape(r['game'] or 'Other')}: {r['orders']} order(s) • {fmt_money(r['revenue'])}"
        for r in games[:5])

    return (
        f"📊 <b>Business Analytics — {d} Day{'s' if d!=1 else ''}</b>\n\n"
        f"💰 Revenue: <b>{fmt_money(t.get('revenue',0))}</b>\n"
        f"✅ Completed orders: <b>{int(t.get('completed_orders') or 0)}</b>\n"
        f"🧮 Avg order value: <b>{fmt_money(t.get('aov',0))}</b>\n"
        f"👥 Unique buyers: <b>{int(b.get('unique_buyers') or 0)}</b>\n"
        f"🆕 New buyers: <b>{int(b.get('new_buyers') or 0)}</b> • "
        f"🔁 Returning: <b>{int(b.get('returning_buyers') or 0)}</b>\n"
        f"👤 New users: <b>{int(u.get('new_users') or 0)}</b> • Total users: <b>{int(u.get('total_users') or 0)}</b>\n"
        f"⏳ Pending: <b>{int(t.get('pending') or 0)}</b> • Awaiting payment: <b>{int(t.get('awaiting_payment') or 0)}</b>\n"
        f"💸 Refund pending: <b>{int(t.get('refund_pending') or 0)}</b> • Refunded: <b>{int(t.get('refunded') or 0)}</b>\n"
        f"❌ Rejected/cancelled: <b>{int(t.get('rejected_cancelled') or 0)}</b>\n\n"
        f"🎁 <b>Loyalty Mix</b>\n"
        f"VIP: <b>{int((vip.get('VIP') or {}).get('c',0))}</b> • "
        f"Gold: <b>{int((vip.get('Gold') or {}).get('c',0))}</b> • "
        f"Silver: <b>{int((vip.get('Silver') or {}).get('c',0))}</b> • "
        f"Bronze: <b>{int((vip.get('Bronze') or {}).get('c',0))}</b>\n\n"
        f"🔥 <b>Current Merchandising</b>\n"
        f"Active offers: <b>{int(offers.get('active_offers') or 0)}</b> • Featured: <b>{int(offers.get('featured') or 0)}</b>\n"
        f"Hot: <b>{int(offers.get('hot') or 0)}</b> • Best sellers: <b>{int(offers.get('best_sellers') or 0)}</b>\n"
        f"Current-offer products sold in period: <b>{int(offer_sales.get('orders') or 0)}</b> • "
        f"{fmt_money(offer_sales.get('revenue',0))}\n\n"
        f"🎮 <b>Top Games</b>\n{game_text}\n\n"
        f"🏆 <b>Top Products</b>\n{product_text}"
    )



# ===================== PHASE 6C ANALYTICS V3 + SMART OFFER ENGINE =====================
def analytics_v3_snapshot(days=7):
    """Read-only business/funnel snapshot. No financial or product mutations."""
    days=max(1,min(365,int(days)))
    interval=f"{days} days"
    core=business_analytics_snapshot(days)
    funnel=db_execute(
        """SELECT
          (SELECT COUNT(*) FROM product_views WHERE viewed_at>=NOW()-(%s::interval)) AS product_views,
          (SELECT COUNT(*) FROM orders WHERE created_at>=NOW()-(%s::interval)) AS orders_created,
          (SELECT COUNT(*) FROM orders WHERE status='completed' AND created_at>=NOW()-(%s::interval)) AS orders_completed,
          (SELECT COUNT(*) FROM payments WHERE created_at>=NOW()-(%s::interval)) AS payments_created,
          (SELECT COUNT(*) FROM payments WHERE status='credited' AND created_at>=NOW()-(%s::interval)) AS payments_credited
        """,(interval,interval,interval,interval,interval),"one") or {}
    trend=db_execute(
        """SELECT DATE(created_at) AS day, COUNT(*) FILTER(WHERE status='completed') AS orders,
                  COALESCE(SUM(total) FILTER(WHERE status='completed'),0) AS revenue
           FROM orders WHERE created_at>=NOW()-(%s::interval)
           GROUP BY DATE(created_at) ORDER BY day DESC LIMIT 14""",(interval,),"all") or []
    segments=db_execute(
        """WITH spend AS (
             SELECT u.id, COUNT(o.id) FILTER(WHERE o.status='completed') completed_orders,
                    COALESCE(SUM(o.total) FILTER(WHERE o.status='completed'),0) spend,
                    MAX(o.created_at) FILTER(WHERE o.status='completed') last_order
             FROM users u LEFT JOIN orders o ON o.user_id=u.id GROUP BY u.id
           ) SELECT
             COUNT(*) FILTER(WHERE completed_orders=0) AS prospects,
             COUNT(*) FILTER(WHERE completed_orders=1) AS one_time,
             COUNT(*) FILTER(WHERE completed_orders>=2) AS repeat_buyers,
             COUNT(*) FILTER(WHERE spend>=5000) AS high_value,
             COUNT(*) FILTER(WHERE last_order IS NOT NULL AND last_order<NOW()-INTERVAL '30 days') AS dormant
           FROM spend""",fetch="one") or {}
    core.update({"funnel":funnel,"trend":trend,"segments":segments})
    return core


def render_analytics_v3(snap):
    base=render_business_analytics(snap)
    f=snap.get("funnel") or {}; seg=snap.get("segments") or {}; trend=snap.get("trend") or []
    views=int(f.get('product_views') or 0); created=int(f.get('orders_created') or 0); completed=int(f.get('orders_completed') or 0)
    payments=int(f.get('payments_created') or 0); credited=int(f.get('payments_credited') or 0)
    view_to_order=(created/views*100.0) if views else 0.0
    completion=(completed/created*100.0) if created else 0.0
    pay_success=(credited/payments*100.0) if payments else 0.0
    trend_text="• No daily sales yet." if not trend else "\n".join(
        f"• {r['day']}: {int(r['orders'] or 0)} order(s) • {fmt_money(r['revenue'])}" for r in trend[:7])
    return (base +
        f"\n\n🧭 <b>Funnel</b>\n"
        f"👀 Product views: <b>{views}</b>\n"
        f"🧾 Orders created: <b>{created}</b> • View→Order: <b>{view_to_order:.1f}%</b>\n"
        f"✅ Completion rate: <b>{completion:.1f}%</b>\n"
        f"💳 Payment success: <b>{pay_success:.1f}%</b>\n\n"
        f"👥 <b>Customer Segments</b>\n"
        f"Prospects: <b>{int(seg.get('prospects') or 0)}</b> • One-time: <b>{int(seg.get('one_time') or 0)}</b>\n"
        f"Repeat: <b>{int(seg.get('repeat_buyers') or 0)}</b> • High value: <b>{int(seg.get('high_value') or 0)}</b>\n"
        f"Dormant 30d+: <b>{int(seg.get('dormant') or 0)}</b>\n\n"
        f"📈 <b>Recent Daily Trend</b>\n{trend_text}")


def smart_offer_engine_snapshot(days=30, limit=12):
    """Generate read-only offer candidates; never changes price or posts a checkout."""
    days=max(7,min(90,int(days))); limit=max(1,min(30,int(limit))); interval=f"{days} days"
    rows=db_execute(
        """WITH perf AS (
             SELECT p.id, COUNT(o.id) FILTER(WHERE o.status='completed' AND o.created_at>=NOW()-(%s::interval)) AS sales,
                    COALESCE(SUM(o.total) FILTER(WHERE o.status='completed' AND o.created_at>=NOW()-(%s::interval)),0) AS revenue
             FROM products p LEFT JOIN orders o ON o.product_id=p.id GROUP BY p.id
           ), views AS (
             SELECT product_id,COUNT(*) AS views FROM product_views
             WHERE viewed_at>=NOW()-(%s::interval) GROUP BY product_id
           ), stock AS (
             SELECT product_id,COUNT(*)::int AS code_stock FROM product_codes WHERE status='available' GROUP BY product_id
           )
           SELECT p.id,p.name,p.category,p.price,p.sale_price,p.sale_until,p.delivery_type,p.stock,
                  CASE WHEN p.delivery_type='code' THEN COALESCE(st.code_stock,0) ELSE p.stock END AS effective_stock,
                  COALESCE(v.views,0) AS views,COALESCE(perf.sales,0) AS sales,COALESCE(perf.revenue,0) AS revenue
           FROM products p LEFT JOIN perf ON perf.id=p.id LEFT JOIN views v ON v.product_id=p.id LEFT JOIN stock st ON st.product_id=p.id
           WHERE p.active=1 ORDER BY COALESCE(v.views,0) DESC,COALESCE(perf.sales,0) DESC,p.id DESC""",
        (interval,interval,interval),"all") or []
    out=[]
    for r in rows:
        stock=int(r.get('effective_stock') or 0); views=int(r.get('views') or 0); sales=int(r.get('sales') or 0)
        if stock<=0: continue
        active_sale=bool(r.get('sale_price') is not None and r.get('sale_until') is not None)
        reasons=[]; score=0; pct=0
        if views>=10 and sales==0:
            score+=45; pct=max(pct,8); reasons.append('high views, no conversion')
        elif views>=10 and sales/max(views,1)<0.05:
            score+=32; pct=max(pct,6); reasons.append('low view-to-sale conversion')
        if stock>=20 and sales<=2:
            score+=25; pct=max(pct,5); reasons.append('high stock, slow sales')
        if sales>=5:
            score+=12; reasons.append('proven demand')
        if active_sale:
            score-=40; reasons.append('already on active sale')
        if score>0:
            out.append({**dict(r),'offer_score':score,'suggested_discount_pct':pct,'reason':', '.join(reasons)})
    out.sort(key=lambda x:(x['offer_score'],x['views'],x['effective_stock']),reverse=True)
    return {'days':days,'candidates':out[:limit]}


def smart_offer_target_audience(candidate):
    reason=(candidate.get("reason") or "").casefold()
    sales=int(candidate.get("sales") or 0)
    views=int(candidate.get("views") or 0)
    if "high stock" in reason and sales<=2:
        return "inactive"
    if sales>=5:
        return "buyers"
    if views>=10 and sales==0:
        return "new"
    return "all"


def stage_smart_offer_proposal(product_id, admin_id):
    product_id=int(product_id)
    snap=smart_offer_engine_snapshot(30,30)
    candidate=next((r for r in snap.get("candidates",[]) if int(r["id"])==product_id),None)
    if not candidate:
        raise ValueError("Product is no longer a strong Smart Offer candidate. Refresh first.")
    pct=int(candidate.get("suggested_discount_pct") or 0)
    if pct<=0:
        raise ValueError("Engine recommends monitor-only for this product.")
    duration=48 if int(candidate.get("offer_score") or 0)>=60 else 24
    audience=smart_offer_target_audience(candidate)
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id,sale_price,sale_until FROM products WHERE id=%s AND active=1 FOR UPDATE",(product_id,))
                product=cur.fetchone()
                if not product:
                    raise ValueError("Product unavailable.")
                if product.get("sale_price") is not None and product.get("sale_until") is not None:
                    raise ValueError("Product already has an offer. Clear/finish it first.")
                cur.execute("SELECT id FROM smart_offer_proposals WHERE product_id=%s AND status='pending' ORDER BY id DESC LIMIT 1",(product_id,))
                existing=cur.fetchone()
                if existing:
                    return int(existing["id"])
                cur.execute("""INSERT INTO smart_offer_proposals(product_id,discount_pct,duration_hours,audience,reason,score,status,created_by)
                               VALUES(%s,%s,%s,%s,%s,%s,'pending',%s) RETURNING id""",
                            (product_id,pct,duration,audience,(candidate.get("reason") or "")[:500],int(candidate.get("offer_score") or 0),int(admin_id)))
                return int(cur.fetchone()["id"])


def smart_offer_proposal_row(proposal_id):
    return db_execute("""SELECT sp.*,p.name,p.price,p.sale_price,p.sale_until,p.active
                         FROM smart_offer_proposals sp JOIN products p ON p.id=sp.product_id
                         WHERE sp.id=%s""",(int(proposal_id),),"one")


def smart_offer_pending_proposals(limit=15):
    return db_execute("""SELECT sp.*,p.name,p.price FROM smart_offer_proposals sp
                         JOIN products p ON p.id=sp.product_id WHERE sp.status='pending'
                         ORDER BY sp.score DESC,sp.id DESC LIMIT %s""",(max(1,min(30,int(limit))),),"all") or []


def _approve_smart_offer_worker(proposal_id, admin_id, with_campaign=False):
    proposal_id=int(proposal_id); admin_id=int(admin_id)
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT sp.*,p.name,p.price,p.sale_price,p.sale_until,p.active
                               FROM smart_offer_proposals sp JOIN products p ON p.id=sp.product_id
                               WHERE sp.id=%s FOR UPDATE OF sp,p""",(proposal_id,))
                row=cur.fetchone()
                if not row: raise ValueError("Proposal not found.")
                if row["status"]!="pending": raise ValueError(f"Proposal already {row['status']}.")
                if not row["active"]: raise ValueError("Product is disabled.")
                if row.get("sale_price") is not None and row.get("sale_until") is not None:
                    raise ValueError("Product already has an active offer.")
                base=float(row["price"]); pct=float(row["discount_pct"])
                sale=round(base*(1-(pct/100.0)),2)
                if sale<=0 or sale>=base: raise ValueError("Calculated offer price is invalid.")
                seconds=int(row["duration_hours"])*3600
                cur.execute("UPDATE products SET sale_price=%s,sale_until=NOW()+(%s*INTERVAL '1 second'),updated_at=NOW() WHERE id=%s",(sale,seconds,row["product_id"]))
                cur.execute("UPDATE smart_offer_proposals SET status='approved',approved_by=%s,decided_at=NOW() WHERE id=%s",(admin_id,proposal_id))
                result={"id":proposal_id,"product_id":int(row["product_id"]),"name":row["name"],"sale_price":sale,"discount_pct":pct,"duration_hours":int(row["duration_hours"]),"audience":row["audience"],"campaign_id":None}
    result["campaign_error"]=None
    if with_campaign:
        try:
            title=f"{int(round(result['discount_pct']))}% Off • {result['name']}"
            message=f"Limited-time {int(round(result['discount_pct']))}% offer on {result['name']}. Open the shop to view the live price."
            campaign_id=marketing_create_campaign(admin_id,title,message,result["audience"],"",0,result["duration_hours"])
            db_execute("UPDATE smart_offer_proposals SET campaign_id=%s WHERE id=%s",(campaign_id,proposal_id))
            result["campaign_id"]=int(campaign_id)
        except Exception as exc:
            result["campaign_error"]=record_runtime_error("smart_offer_campaign_create",exc,{"proposal_id":proposal_id,"product_id":result["product_id"],"audience":result["audience"]})
    admin_log(admin_id,"smart_offer_approved",f"proposal={proposal_id} product={result['product_id']} discount={result['discount_pct']} audience={result['audience']} campaign={result['campaign_id']} campaign_error={result['campaign_error']}")
    return result


def reject_smart_offer_proposal(proposal_id, admin_id):
    row=db_execute("""UPDATE smart_offer_proposals SET status='rejected',approved_by=%s,decided_at=NOW()
                      WHERE id=%s AND status='pending' RETURNING id,product_id""",(int(admin_id),int(proposal_id)),"one")
    if not row: raise ValueError("Proposal not found or already decided.")
    admin_log(admin_id,"smart_offer_rejected",f"proposal={proposal_id} product={row['product_id']}")
    return row


def render_smart_offer_proposal(row):
    if not row: return "🧠 Proposal not found."
    sale=round(float(row["price"])*(1-(float(row["discount_pct"])/100.0)),2)
    return (f"🧠 <b>Smart Offer Proposal #{row['id']}</b>\n\n"
            f"🎮 <b>{html.escape(row['name'])}</b>\n"
            f"💰 {fmt_money(row['price'])} → <b>{fmt_money(sale)}</b> ({float(row['discount_pct']):g}% off)\n"
            f"⏱ Duration: <b>{int(row['duration_hours'])}h</b>\n"
            f"🎯 Suggested audience: <b>{html.escape(str(row['audience']).title())}</b>\n"
            f"📈 Engine score: <b>{int(row['score'])}</b>\n"
            f"📝 {html.escape((row.get('reason') or '')[:300])}\n\n"
            f"Status: <b>{html.escape(str(row['status']).title())}</b>\n"
            "Nothing changes until an authorized admin approves it.")


def render_smart_offer_engine(snap):
    rows=snap.get('candidates') or []
    lines=["🧠 <b>Smart Offer Engine — Recommendation Mode</b>","",
           "Read-only suggestions. Nothing is discounted automatically.",""]
    if not rows:
        lines.append('✅ No strong offer candidates right now.')
    else:
        for r in rows[:12]:
            pct=int(r.get('suggested_discount_pct') or 0)
            suggestion=f" • suggest ~{pct}%" if pct else " • monitor only"
            lines.append(f"• #{r['id']} <b>{html.escape(r['name'][:34])}</b> — score {r['offer_score']}{suggestion}")
            lines.append(f"  views {int(r['views'] or 0)} • sales {int(r['sales'] or 0)} • stock {int(r['effective_stock'] or 0)} • {html.escape(r['reason'][:90])}")
    return '\n'.join(lines)

@router.callback_query(F.data.startswith("admin:analytics_v3"))
async def admin_analytics_v3(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    try: days=int(c.data.rsplit(':',1)[1])
    except Exception: days=7
    days=days if days in (1,7,30) else 7
    snap=await asyncio.to_thread(analytics_v3_snapshot,days)
    await c.answer()
    await c.message.edit_text(render_analytics_v3(snap),reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1D",callback_data="admin:analytics_v3:1"),InlineKeyboardButton(text="7D",callback_data="admin:analytics_v3:7"),InlineKeyboardButton(text="30D",callback_data="admin:analytics_v3:30")],
        [InlineKeyboardButton(text="🧠 Smart Offer Engine",callback_data="admin:smart_offer_engine"),InlineKeyboardButton(text="📦 Stock Intelligence",callback_data="admin:stock_intel")],
        [InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]
    ]))

@router.callback_query(F.data=="admin:smart_offer_engine")
async def admin_smart_offer_engine(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    snap=await asyncio.to_thread(smart_offer_engine_snapshot,30,12)
    buttons=[]
    if not admin_is_readonly(c.from_user.id):
        for r in (snap.get("candidates") or [])[:5]:
            if int(r.get("suggested_discount_pct") or 0)>0:
                buttons.append([InlineKeyboardButton(text=f"➕ Stage #{r['id']} • ~{int(r['suggested_discount_pct'])}%",callback_data=f"admin:smart_offer_stage:{r['id']}")])
    buttons.append([InlineKeyboardButton(text="📋 Pending Approvals",callback_data="admin:smart_offer_queue"),InlineKeyboardButton(text="🔥 Offers Manager",callback_data="admin:offers_v2")])
    buttons.append([InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:smart_offer_engine"),InlineKeyboardButton(text="📊 Analytics V3",callback_data="admin:analytics_v3:7")])
    buttons.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")])
    await c.answer()
    await c.message.edit_text(render_smart_offer_engine(snap)+"\n\n🛡️ Phase 6D: stage → review → admin approval. No automatic activation.",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("admin:smart_offer_stage:"))
async def admin_smart_offer_stage(c:CallbackQuery):
    if not admin_can(c.from_user.id,"mutate"):
        return await c.answer("Read-only admin cannot stage offers.",show_alert=True)
    try:
        pid=int(c.data.rsplit(":",1)[1])
        proposal_id=await asyncio.to_thread(stage_smart_offer_proposal,pid,c.from_user.id)
        row=await asyncio.to_thread(smart_offer_proposal_row,proposal_id)
    except Exception as exc:
        return await c.answer(str(exc)[:180],show_alert=True)
    await c.answer("Proposal staged")
    await c.message.edit_text(render_smart_offer_proposal(row),reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve Offer",callback_data=f"admin:smart_offer_approve:{proposal_id}"),InlineKeyboardButton(text="📣 Approve + Campaign",callback_data=f"admin:smart_offer_campaign:{proposal_id}")],
        [InlineKeyboardButton(text="❌ Reject",callback_data=f"admin:smart_offer_reject:{proposal_id}")],
        [InlineKeyboardButton(text="⬅️ Smart Engine",callback_data="admin:smart_offer_engine")]
    ]))

@router.callback_query(F.data=="admin:smart_offer_queue")
async def admin_smart_offer_queue(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=await asyncio.to_thread(smart_offer_pending_proposals,15)
    text="📋 <b>Smart Offer Approval Queue</b>\n\n" + ("No pending proposals." if not rows else "\n".join(f"• #{r['id']} • product #{r['product_id']} {html.escape(r['name'][:28])} • {float(r['discount_pct']):g}% • {html.escape(str(r['audience']).title())} • score {r['score']}" for r in rows))
    kb=[[InlineKeyboardButton(text=f"Review Proposal #{r['id']}",callback_data=f"admin:smart_offer_review:{r['id']}")] for r in rows[:8]]
    kb.append([InlineKeyboardButton(text="⬅️ Smart Engine",callback_data="admin:smart_offer_engine")])
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("admin:smart_offer_review:"))
async def admin_smart_offer_review(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    row=await asyncio.to_thread(smart_offer_proposal_row,int(c.data.rsplit(":",1)[1]))
    if not row: return await c.answer("Proposal not found.",show_alert=True)
    kb=[]
    if row['status']=='pending' and not admin_is_readonly(c.from_user.id):
        kb.append([InlineKeyboardButton(text="✅ Approve Offer",callback_data=f"admin:smart_offer_approve:{row['id']}"),InlineKeyboardButton(text="📣 Approve + Campaign",callback_data=f"admin:smart_offer_campaign:{row['id']}")])
        kb.append([InlineKeyboardButton(text="❌ Reject",callback_data=f"admin:smart_offer_reject:{row['id']}")])
    kb.append([InlineKeyboardButton(text="⬅️ Approval Queue",callback_data="admin:smart_offer_queue")])
    await c.answer(); await c.message.edit_text(render_smart_offer_proposal(row),reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("admin:smart_offer_approve:"))
async def admin_smart_offer_approve(c:CallbackQuery):
    if not admin_can(c.from_user.id,"mutate"): return await c.answer("Read-only admin cannot approve offers.",show_alert=True)
    try: result=await asyncio.to_thread(_approve_smart_offer_worker,int(c.data.rsplit(":",1)[1]),c.from_user.id,False)
    except Exception as exc: return await c.answer(str(exc)[:180],show_alert=True)
    await c.answer("Offer activated")
    await c.message.edit_text(f"✅ <b>Smart Offer Approved</b>\n\n🎮 {html.escape(result['name'])}\n💰 Live price: <b>{fmt_money(result['sale_price'])}</b>\n⏱ {result['duration_hours']}h\n\nNo campaign was sent.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Approval Queue",callback_data="admin:smart_offer_queue")]]))

@router.callback_query(F.data.startswith("admin:smart_offer_campaign:"))
async def admin_smart_offer_campaign(c:CallbackQuery):
    if not admin_can(c.from_user.id,"mutate"): return await c.answer("Read-only admin cannot approve campaigns.",show_alert=True)
    try: result=await asyncio.to_thread(_approve_smart_offer_worker,int(c.data.rsplit(":",1)[1]),c.from_user.id,True)
    except Exception as exc: return await c.answer(str(exc)[:180],show_alert=True)
    if result.get("campaign_error"):
        await c.answer("Offer approved; campaign creation failed",show_alert=True)
        body=(f"⚠️ <b>Offer Approved — Campaign Not Created</b>\n\n🎮 {html.escape(result['name'])}\n💰 Live price: <b>{fmt_money(result['sale_price'])}</b>\n🎯 Intended audience: <b>{html.escape(result['audience'].title())}</b>\n\nThe offer is already live. Do not approve it again. Campaign error ref: <code>{html.escape(str(result['campaign_error']))}</code>")
    else:
        await c.answer("Offer + campaign approved")
        body=(f"📣 <b>Offer + Target Campaign Approved</b>\n\n🎮 {html.escape(result['name'])}\n💰 Live price: <b>{fmt_money(result['sale_price'])}</b>\n🎯 Audience: <b>{html.escape(result['audience'].title())}</b>\n🧾 Campaign: <b>#{result['campaign_id']}</b>\n\nCampaign uses the existing queued marketing worker and daily limits.")
    await c.message.edit_text(body,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Approval Queue",callback_data="admin:smart_offer_queue")]]))

@router.callback_query(F.data.startswith("admin:smart_offer_reject:"))
async def admin_smart_offer_reject(c:CallbackQuery):
    if not admin_can(c.from_user.id,"mutate"): return await c.answer("Read-only admin cannot reject proposals.",show_alert=True)
    try: await asyncio.to_thread(reject_smart_offer_proposal,int(c.data.rsplit(":",1)[1]),c.from_user.id)
    except Exception as exc: return await c.answer(str(exc)[:180],show_alert=True)
    await c.answer("Proposal rejected")
    c.data="admin:smart_offer_queue"
    return await admin_smart_offer_queue(c)

def stock_intelligence_snapshot():
    threshold=low_stock_threshold()
    rows=db_execute(
        """SELECT p.id,p.name,p.category,p.delivery_type,p.active,p.stock,
                  CASE WHEN p.delivery_type='code'
                    THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=p.id AND pc.status='available')
                    ELSE p.stock END AS effective_stock
           FROM products p
           WHERE p.active=1
           ORDER BY effective_stock ASC,p.category,p.name""",fetch="all") or []
    out_of_stock=[r for r in rows if int(r["effective_stock"] or 0)<=0]
    low_stock=[r for r in rows if 0<int(r["effective_stock"] or 0)<=threshold]
    healthy=[r for r in rows if int(r["effective_stock"] or 0)>threshold]
    return {
        "threshold":threshold,"total":len(rows),
        "out":out_of_stock,"low":low_stock,"healthy":healthy,
    }


def render_stock_intelligence(snap):
    lines=[
        "📦 <b>Stock Intelligence</b>",
        "",
        f"Active products: <b>{snap['total']}</b>",
        f"⛔ Out of stock: <b>{len(snap['out'])}</b>",
        f"⚠️ Low stock ≤{snap['threshold']}: <b>{len(snap['low'])}</b>",
        f"✅ Healthy: <b>{len(snap['healthy'])}</b>",
    ]
    critical=(snap["out"]+snap["low"])[:20]
    if critical:
        lines += ["","🚨 <b>Needs Attention</b>"]
        for r in critical:
            stock=int(r["effective_stock"] or 0)
            marker="⛔" if stock<=0 else "⚠️"
            lines.append(
                f"• {marker} #{r['id']} {html.escape(r['name'][:28])} • "
                f"{html.escape(r['category'][:28])} • stock <b>{stock}</b>"
            )
    else:
        lines += ["","✅ No low-stock alerts."]
    return "\n".join(lines)


@router.callback_query(F.data.startswith("admin:analytics_v2"))
async def admin_analytics_v2(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    days=7
    if c.data.count(":")>=2:
        try:
            days=int(c.data.rsplit(":",1)[1])
        except ValueError:
            days=7
    days=days if days in (1,7,30) else 7
    snap=await asyncio.to_thread(business_analytics_snapshot,days)
    await c.answer()
    await c.message.edit_text(
        render_business_analytics(snap),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1D",callback_data="admin:analytics_v2:1"),
             InlineKeyboardButton(text="7D",callback_data="admin:analytics_v2:7"),
             InlineKeyboardButton(text="30D",callback_data="admin:analytics_v2:30")],
            [InlineKeyboardButton(text="📦 Stock Intelligence",callback_data="admin:stock_intel"),
             InlineKeyboardButton(text="🎁 Loyalty",callback_data="admin:loyalty")],
            [InlineKeyboardButton(text="🔥 Offers",callback_data="admin:offers_v2"),
             InlineKeyboardButton(text="📊 Daily Summary",callback_data="admin:daily_summary")],
            [InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]
        ]))


@router.callback_query(F.data=="admin:stock_intel")
async def admin_stock_intelligence(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    snap=await asyncio.to_thread(stock_intelligence_snapshot)
    await c.answer()
    await c.message.edit_text(
        render_stock_intelligence(snap),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:stock_intel")],
            [InlineKeyboardButton(text="🛍 Products",callback_data="admin:products"),
             InlineKeyboardButton(text="📥 Bulk Import",callback_data="admin:bulk_products")],
            [InlineKeyboardButton(text="📊 Analytics",callback_data="admin:analytics_v2:7")],
            [InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]
        ]))


AUTO_TOPUP_PROVIDER=(_auto_provider_raw if (_auto_provider_raw:=(os.getenv("AUTO_TOPUP_PROVIDER","BANGJEFF") or "BANGJEFF").strip().upper()) else "BANGJEFF")


def _auto_topup_env(name,default=""):
    return (os.getenv(name,default) or "").strip()


def auto_topup_config():
    # BANGJEFF API v4: signed requests using X-Client-Id / X-Request-Time / X-Signature.
    # Defaults target production; set BANGJEFF_ENV=sandbox while testing.
    env=_auto_topup_env("BANGJEFF_ENV","production").lower()
    default_base=("https://sandbox-api.bangjeff.com" if env=="sandbox"
                  else "https://distribution-api.bangjeff.com")
    base=_auto_topup_env("BANGJEFF_API_BASE",default_base).rstrip("/")
    api_key=_auto_topup_env("BANGJEFF_API_KEY")
    return {
        "provider":AUTO_TOPUP_PROVIDER,
        "base":base,
        "api_key":api_key,
        # Most BANGJEFF accounts use the Client ID/API key itself for HMAC.
        # Keep a separate optional secret hook for accounts where support issues one.
        "signature_secret":_auto_topup_env("BANGJEFF_SIGNATURE_SECRET",api_key),
        "region":_auto_topup_env("BANGJEFF_REGION","ID"),
        "balance_path":_auto_topup_env("BANGJEFF_BALANCE_PATH","/api/v4/balance"),
        "products_path":_auto_topup_env("BANGJEFF_PRODUCTS_PATH","/api/v4/product"),
        "product_detail_path":_auto_topup_env("BANGJEFF_PRODUCT_DETAIL_PATH","/api/v4/product/detail"),
        "variant_path":_auto_topup_env("BANGJEFF_VARIANT_PATH","/api/v4/variant"),
        "order_path":_auto_topup_env("BANGJEFF_ORDER_PATH","/api/v4/checkout"),
        "order_invoice_path":_auto_topup_env("BANGJEFF_ORDER_INVOICE_PATH","/api/v4/order/invoice"),
        "order_reference_path":_auto_topup_env("BANGJEFF_ORDER_REFERENCE_PATH","/api/v4/order/reference"),
        "order_status_path_template":_auto_topup_env("BANGJEFF_ORDER_STATUS_PATH_TEMPLATE",""),
        "webhook_token":_auto_topup_env("BANGJEFF_WEBHOOK_TOKEN"),
        "status_poll_seconds":max(30,min(900,int(_auto_topup_env("BANGJEFF_STATUS_POLL_SECONDS","60") or 60))),
        # Inquiry/UID validation is a separately subscribed BANGJEFF service.
        "uid_path":_auto_topup_env("BANGJEFF_UID_PATH"),
        "uid_field":_auto_topup_env("BANGJEFF_UID_FIELD","uid"),
        "zone_field":_auto_topup_env("BANGJEFF_ZONE_FIELD","zone"),
        "game_field":_auto_topup_env("BANGJEFF_GAME_FIELD","productCode"),
        "free_fire_code":_auto_topup_env("BANGJEFF_FREE_FIRE_CODE","FREEFIRE"),
        "products_list_path":"data",
        "product_id_path":"code",
        "product_name_path":"name",
        "timeout":max(3,min(30,int(_auto_topup_env("BANGJEFF_TIMEOUT_SECONDS","12") or 12))),
    }


def _bangjeff_request_time():
    # Official docs use moment(timestamp).format("YYYY-MM-DDTHH:mm:ssZ").
    # Moment renders UTC with the numeric offset +00:00 (not a literal Z).
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _bangjeff_payload_bytes(payload):
    """Match JavaScript JSON.stringify(payload) used by the BANGJEFF v4 example."""
    obj={} if payload is None else payload
    text=json.dumps(obj,separators=(",",":"),ensure_ascii=False)
    return text,text.encode("utf-8")


def _bangjeff_signature(client_id,method,pathname,payload,request_time):
    """BANGJEFF V4: sign the pathname WITH its leading slash."""
    payload_string,_=_bangjeff_payload_bytes(payload)
    payload_md5=hashlib.md5(payload_string.encode("utf-8")).hexdigest()
    canonical_path=str(pathname).strip()
    if not canonical_path.startswith("/"):
        canonical_path="/"+canonical_path
    signature_payload=f"{str(method).upper()}:{canonical_path}:{payload_md5}:{request_time}"
    signature=hmac.new(
        str(client_id).encode("utf-8"),
        signature_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return signature,payload_md5


def auto_topup_live_armed():
    """Live provider payments are impossible unless Railway explicitly arms them."""
    return _auto_topup_env("AUTO_TOPUP_LIVE_ALLOWED","0").strip().lower() in {"1","true","yes","on"}

def auto_topup_master_requested():
    """New production master switch; legacy experimental setting is intentionally ignored."""
    return setting("autotopup_live_enabled","0")=="1"

def auto_topup_master_enabled():
    return auto_topup_live_armed() and auto_topup_master_requested()


def auto_topup_free_fire_enabled():
    return setting("autotopup_free_fire_enabled","0")=="1"


def auto_topup_connection_ready():
    cfg=auto_topup_config()
    return bool(cfg["base"] and cfg["api_key"])


def auto_topup_live_order_ready():
    cfg=auto_topup_config()
    return bool(
        auto_topup_master_enabled()
        and auto_topup_free_fire_enabled()
        and auto_topup_connection_ready()
        and cfg["order_path"]
        and cfg.get("order_status_path_template")
    )


def _json_path(data,path,default=None):
    if not path:
        return data
    cur=data
    for part in path.split("."):
        if isinstance(cur,dict) and part in cur:
            cur=cur[part]
        elif isinstance(cur,list) and part.isdigit() and int(part)<len(cur):
            cur=cur[int(part)]
        else:
            return default
    return cur


def _provider_url(base,path):
    if not path:
        raise ValueError("Provider endpoint is not configured.")
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not base:
        raise ValueError("BANGJEFF_API_BASE is not configured.")
    return base + "/" + path.lstrip("/")


def _bangjeff_http_json(method,path,payload,request_time,signature):
    cfg=auto_topup_config()
    url=_provider_url(cfg["base"],path)
    headers={
        "Accept":"application/json",
        "Content-Type":"application/json",
        "X-Client-Id":cfg["api_key"],
        "X-Request-Time":request_time,
        "X-Signature":signature,
    }
    _,body=_bangjeff_payload_bytes(payload)
    req=Request(url,data=body,headers=headers,method=str(method).upper())
    try:
        with urlopen(req,timeout=cfg["timeout"]) as resp:
            raw=resp.read(2_000_000)
            status=int(getattr(resp,"status",200) or 200)
    except HTTPError as exc:
        raw=exc.read(200_000)
        text=raw.decode("utf-8","replace")[:500]
        raise RuntimeError(f"BANGJEFF HTTP {exc.code}: {text}") from exc
    except URLError as exc:
        raise RuntimeError(f"BANGJEFF connection failed: {exc.reason}") from exc
    try:
        data=json.loads(raw.decode("utf-8")) if raw else {}
    except Exception as exc:
        raise RuntimeError(f"BANGJEFF returned non-JSON response (HTTP {status}).") from exc
    if status<200 or status>=300:
        raise RuntimeError(f"BANGJEFF HTTP {status}: {str(data)[:500]}")
    return data


def bangjeff_request(method,path,payload=None):
    cfg=auto_topup_config()
    if not cfg["api_key"]:
        raise ValueError("BANGJEFF_API_KEY is not configured.")
    method=str(method).upper()
    clean_path=str(path).strip()
    if not clean_path.startswith("/"):
        clean_path="/"+clean_path
    request_time=_bangjeff_request_time()
    signature,payload_md5=_bangjeff_signature(
        cfg["api_key"],method,clean_path,payload,request_time
    )
    data=_bangjeff_http_json(method,clean_path,payload,request_time,signature)
    if isinstance(data,dict) and str(data.get("rc","00"))!="00":
        msg=str(data.get("message","Request failed"))
        raise RuntimeError(
            f"BANGJEFF {data.get('rc')}: {msg} | method={method} signed_path={clean_path} "
            f"request_time={request_time} payload_md5={payload_md5}"
        )
    return data

def bangjeff_check_balance():
    cfg=auto_topup_config()
    if not auto_topup_connection_ready():
        raise ValueError("Set BANGJEFF_API_KEY in Railway first.")
    data=bangjeff_request("POST",cfg["balance_path"],{"region":cfg["region"]})
    obj=data.get("data") if isinstance(data,dict) else None
    if not isinstance(obj,dict):
        raise ValueError("BANGJEFF balance response has no data object.")
    balance_field=obj.get("balance",obj.get("value",obj.get("credit")))
    if isinstance(balance_field,dict):
        balance=balance_field.get("value")
        currency=balance_field.get("currency") or obj.get("currency") or cfg["region"]
    else:
        balance=balance_field
        currency=obj.get("currency") or cfg["region"]
    if balance is None:
        raise ValueError("Balance field not found in BANGJEFF response.")
    cache={"balance":balance,"currency":str(currency or ""),"checked_at":datetime.now(timezone.utc).isoformat()}
    set_setting("autotopup_balance_cache",json.dumps(cache,separators=(",",":")))
    return cache

def auto_topup_balance_cache():
    raw=setting("autotopup_balance_cache","")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _safe_decimal(value):
    if value in (None,""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def bangjeff_product_detail(product_code):
    cfg=auto_topup_config()
    return bangjeff_request("POST",cfg["product_detail_path"],{
        "region":cfg["region"],"productCode":str(product_code)})


def bangjeff_get_variants(product_code):
    cfg=auto_topup_config()
    return bangjeff_request("POST",cfg["variant_path"],{
        "region":cfg["region"],"productCode":str(product_code)})


def bangjeff_sync_products():
    cfg=auto_topup_config()
    if not auto_topup_connection_ready():
        raise ValueError("Set BANGJEFF_API_KEY in Railway first.")
    data=bangjeff_request("POST",cfg["products_path"],{"region":cfg["region"]})
    products=data.get("data") if isinstance(data,dict) else None
    if not isinstance(products,list):
        raise ValueError("BANGJEFF product response has no data array.")
    parsed=[]
    for product in products[:10000]:
        if not isinstance(product,dict) or product.get("status")!="ACTIVE" or not product.get("code"):
            continue
        code=str(product["code"]); name=str(product.get("name") or code)
        # Variants carry the actual sellable SKU, price, currency, duration and region.
        vresp=bangjeff_get_variants(code)
        variants=vresp.get("data") if isinstance(vresp,dict) else []
        if not isinstance(variants,list): variants=[]
        for v in variants:
            if not isinstance(v,dict) or v.get("status")!="ACTIVE" or not v.get("code"):
                continue
            price=v.get("price") if isinstance(v.get("price"),dict) else {}
            raw={"product":product,"variant":v}
            parsed.append((str(v["code"]),code,str(v.get("name") or name),
                           _safe_decimal(price.get("value")),str(price.get("currency") or ""),raw))
    if not parsed:
        raise ValueError("No ACTIVE BANGJEFF variants found for this region.")
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE autotopup_provider_products SET active=0 WHERE provider=%s",(AUTO_TOPUP_PROVIDER,))
                for pid,game,name,cost,currency,item in parsed:
                    cur.execute(
                        """INSERT INTO autotopup_provider_products(
                             provider,provider_product_id,game_key,name,cost,currency,active,raw_json,synced_at)
                           VALUES(%s,%s,%s,%s,%s,%s,1,%s::jsonb,NOW())
                           ON CONFLICT(provider,provider_product_id) DO UPDATE SET
                             game_key=EXCLUDED.game_key,name=EXCLUDED.name,cost=EXCLUDED.cost,
                             currency=EXCLUDED.currency,active=1,raw_json=EXCLUDED.raw_json,synced_at=NOW()""",
                        (AUTO_TOPUP_PROVIDER,pid,game,name,cost,currency,json.dumps(item,separators=(",",":"))))
    set_setting("autotopup_last_sync",datetime.now(timezone.utc).isoformat())
    return len(parsed)

def bangjeff_test_uid(uid,zone=""):
    cfg=auto_topup_config()
    if not auto_topup_connection_ready():
        raise ValueError("Set BANGJEFF API credentials in Railway first.")
    if not cfg["uid_path"]:
        raise ValueError("BANGJEFF UID inquiry is not configured. BJAPI Inquiry is a separate subscribed service; set BANGJEFF_UID_PATH only after BANGJEFF gives you that endpoint.")
    payload={
        cfg["uid_field"]:str(uid).strip(),
        cfg["game_field"]:cfg["free_fire_code"],
    }
    if zone and cfg["zone_field"]:
        payload[cfg["zone_field"]]=str(zone).strip()
    return bangjeff_request("POST",cfg["uid_path"],payload)



# ---- Phase 4L provider adapter boundary ---------------------------------
# Core bot/admin/order code calls these functions only. Each provider keeps
# its own credentials, signing rules and response parsing inside an adapter.
def auto_topup_provider_name():
    return AUTO_TOPUP_PROVIDER

def _require_auto_topup_provider():
    provider=auto_topup_provider_name()
    if provider not in {"BANGJEFF"}:
        raise ValueError(
            f"AUTO_TOPUP_PROVIDER={provider} has no installed adapter yet. "
            "The main bot remains available; add/register that provider adapter before enabling auto top-up."
        )
    return provider

def provider_config():
    provider=_require_auto_topup_provider()
    if provider=="BANGJEFF":
        return auto_topup_config()
    raise ValueError(f"Unsupported auto top-up provider: {provider}")

def provider_check_balance():
    provider=_require_auto_topup_provider()
    if provider=="BANGJEFF":
        return bangjeff_check_balance()

def provider_sync_products():
    provider=_require_auto_topup_provider()
    if provider=="BANGJEFF":
        return bangjeff_sync_products()

def provider_test_uid(uid,zone=""):
    provider=_require_auto_topup_provider()
    if provider=="BANGJEFF":
        return bangjeff_test_uid(uid,zone)

def provider_connection_ready():
    try:
        provider=_require_auto_topup_provider()
    except Exception:
        return False
    if provider=="BANGJEFF":
        return auto_topup_connection_ready()
    return False

def provider_live_order_ready():
    try:
        provider=_require_auto_topup_provider()
    except Exception:
        return False
    if provider=="BANGJEFF":
        return auto_topup_live_order_ready()
    return False


def auto_topup_synced_counts():
    return db_execute(
        """SELECT COUNT(*) AS total,
                  COUNT(*) FILTER(WHERE LOWER(game_key) LIKE '%%free%%fire%%') AS free_fire
           FROM autotopup_provider_products
           WHERE provider=%s AND active=1""",
        (AUTO_TOPUP_PROVIDER,),"one") or {"total":0,"free_fire":0}



def auto_topup_mapping(product_id):
    return db_execute("""SELECT m.*,pp.cost,pp.currency,pp.raw_json,pp.active AS provider_active FROM autotopup_product_map m LEFT JOIN autotopup_provider_products pp ON pp.provider=m.provider AND pp.provider_product_id=m.provider_variant_code WHERE m.product_id=%s AND m.enabled=1""",(int(product_id),),"one")

def auto_topup_mapping_count():
    row=db_execute("SELECT COUNT(*) AS c FROM autotopup_product_map WHERE enabled=1",fetch="one")
    return int(row["c"] or 0) if row else 0

def auto_topup_save_mapping(local_product_id,variant_code):
    local=db_execute("SELECT id,name FROM products WHERE id=%s",(int(local_product_id),),"one")
    if not local: raise ValueError("Local product not found.")
    pp=db_execute("SELECT * FROM autotopup_provider_products WHERE provider=%s AND provider_product_id=%s AND active=1",(AUTO_TOPUP_PROVIDER,str(variant_code).strip()),"one")
    if not pp: raise ValueError("Provider variant not found. Run Sync Products first.")
    raw=pp.get("raw_json") or {}
    if isinstance(raw,str):
        try: raw=json.loads(raw)
        except Exception: raw={}
    product=(raw.get("product") or {}) if isinstance(raw,dict) else {}
    product_code=str(product.get("code") or pp.get("game_key") or "")
    schema=[]
    if product_code:
        detail=bangjeff_product_detail(product_code); d=detail.get("data") if isinstance(detail,dict) else None
        if isinstance(d,dict) and isinstance(d.get("inputs"),list): schema=d["inputs"]
    db_execute("""INSERT INTO autotopup_product_map(product_id,provider,provider_variant_code,provider_product_code,enabled,input_schema,updated_at) VALUES(%s,%s,%s,%s,1,%s::jsonb,NOW()) ON CONFLICT(product_id) DO UPDATE SET provider=EXCLUDED.provider,provider_variant_code=EXCLUDED.provider_variant_code,provider_product_code=EXCLUDED.provider_product_code,enabled=1,input_schema=EXCLUDED.input_schema,updated_at=NOW()""",(int(local_product_id),AUTO_TOPUP_PROVIDER,str(variant_code).strip(),product_code,json.dumps(schema,separators=(",",":"))))
    return {"local":local,"provider":pp,"inputs":schema}

def auto_topup_build_inputs(mapping,order):
    schema=mapping.get("input_schema") or []
    if isinstance(schema,str):
        try: schema=json.loads(schema)
        except Exception: schema=[]
    if not isinstance(schema,list): schema=[]
    if len(schema)!=1: raise ValueError(f"Auto Top-Up requires exactly 1 provider input for this mapping; provider requires {len(schema)}.")
    field=schema[0] if isinstance(schema[0],dict) else {}; name=str(field.get("name") or "").strip(); uid=str(order.get("game_uid") or "").strip()
    if not name: raise ValueError("Provider input name is missing.")
    if not uid: raise ValueError("Order has no game UID.")
    return [{"name":name,"value":uid}]

def _autotopup_reference(order_id): return f"NLG-{int(order_id)}-{uuid.uuid4().hex[:10].upper()}"


def bangjeff_checkout_payload(mapping,order,reference_number):
    cfg=auto_topup_config()
    cost=mapping.get("cost")
    currency_code=str(mapping.get("currency") or "").strip()
    if cost is None or not currency_code:
        raise ValueError("Synced provider price/currency missing.")
    inputs=auto_topup_build_inputs(mapping,order)
    return {
        "region":cfg["region"],
        "variantCode":str(mapping["provider_variant_code"]),
        "referenceNumber":str(reference_number),
        "qty":1,
        "price":{"currency":currency_code,"value":float(cost)},
        "inputs":inputs,
    }


def bangjeff_get_order(invoice_number):
    cfg=auto_topup_config()
    invoice=str(invoice_number or "").strip()
    if not invoice:
        raise ValueError("Provider invoice number is missing.")
    template=str(cfg.get("order_status_path_template") or "").strip()
    if template:
        path=template.replace("{invoiceNumber}",invoice)
        return bangjeff_request("POST",path,{})
    path=str(cfg.get("order_invoice_path") or "").strip()
    if not path:
        raise ValueError("No BANGJEFF invoice lookup endpoint is configured.")
    return bangjeff_request("POST",path,{"invoiceNumber":invoice})


def bangjeff_get_order_by_reference(reference_number):
    """Best-effort reconciliation for an ambiguous checkout using our idempotency reference."""
    cfg=auto_topup_config()
    reference=str(reference_number or "").strip()
    if not reference:
        raise ValueError("Provider reference number is missing.")
    path=str(cfg.get("order_reference_path") or "").strip()
    if not path:
        raise ValueError("BANGJEFF_ORDER_REFERENCE_PATH is not configured.")
    return bangjeff_request("POST",path,{"referenceNumber":reference})


def _auto_topup_reverse_rewards_cursor(cur,order):
    if not bool(order.get("rewards_awarded")):
        return
    total=float(order.get("total") or 0)
    earned=max(1,int(total//10))
    uid=int(order["user_id"])
    cur.execute(
        """UPDATE users SET loyalty_points=GREATEST(0,loyalty_points-%s),
           lifetime_spend=GREATEST(0,lifetime_spend-%s),updated_at=NOW() WHERE id=%s""",
        (earned,total,uid))
    cur.execute("SELECT referred_by FROM users WHERE id=%s FOR UPDATE",(uid,))
    ref=cur.fetchone()
    # Referral bonus is awarded on the buyer's first completed order. If this
    # is now the only completed order, reverse that one-time bonus as well.
    cur.execute("SELECT COUNT(*) AS c FROM orders WHERE user_id=%s AND status='completed' AND id<>%s",(uid,order["id"]))
    other_completed=int(cur.fetchone()["c"] or 0)
    if ref and ref.get("referred_by") and other_completed==0:
        cur.execute("UPDATE users SET loyalty_points=GREATEST(0,loyalty_points-50),updated_at=NOW() WHERE id=%s",(ref["referred_by"],))
        cur.execute("UPDATE users SET loyalty_points=GREATEST(0,loyalty_points-50),updated_at=NOW() WHERE id=%s",(uid,))
    cur.execute("UPDATE orders SET rewards_awarded=FALSE,updated_at=NOW() WHERE id=%s",(order["id"],))


def auto_topup_handle_provider_refund(order_id,reason="Provider reported REFUNDED"):
    """Idempotent money/stock recovery.

    Wallet orders are refunded automatically to the bot wallet.
    Direct-payment orders are moved to refund_pending; an admin must actually
    send the external refund and confirm it using the existing safe flow.
    """
    oid=int(order_id)
    result={"changed":False,"mode":"","tg_id":None,"amount":0.0}
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,))
                o=cur.fetchone()
                if not o:
                    raise ValueError("Order not found.")
                if o["status"] in {"refunded","refund_pending"}:
                    return result
                if o["status"] not in {"pending","completed"}:
                    raise ValueError(f"Order status {o['status']} cannot enter provider refund recovery.")
                # Restore the one manual unit consumed when the order was created.
                if o.get("delivered_code"):
                    cur.execute(
                        "UPDATE product_codes SET status='available',sold_to=NULL,order_id=NULL,sold_at=NULL "
                        "WHERE order_id=%s AND status='sold'",(oid,))
                    sync_code_product_stock(o["product_id"],conn)
                else:
                    cur.execute("UPDATE products SET stock=stock+1,updated_at=NOW() WHERE id=%s",(o["product_id"],))
                _auto_topup_reverse_rewards_cursor(cur,o)
                payment_mode=(o.get("payment_mode") or "wallet").strip().lower()
                if payment_mode=="direct":
                    cur.execute(
                        "UPDATE orders SET status='refund_pending',refund_amount=0,account_password='',processed_at=NULL,updated_at=NOW() WHERE id=%s",
                        (oid,))
                    if o.get("payment_id"):
                        cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(o["payment_id"],))
                        pay=cur.fetchone()
                        if pay and pay["status"]=="credited":
                            cur.execute("UPDATE payments SET status='refund_pending',updated_at=NOW() WHERE id=%s",(pay["id"],))
                            record_payment_audit(
                                cur,pay["id"],None,"provider_refund_requested","credited","refund_pending",
                                pay["amount"],pay["method"],pay["trx_id"],
                                f"Order #{oid}: provider refunded; external refund required")
                    mode="external"
                else:
                    cur.execute(
                        "UPDATE orders SET status='refunded',refund_amount=total,account_password='',processed_at=NOW(),updated_at=NOW() WHERE id=%s",
                        (oid,))
                    cur.execute("UPDATE users SET balance=balance+%s,updated_at=NOW() WHERE id=%s",(o["total"],o["user_id"]))
                    cur.execute(
                        "INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",
                        (o["user_id"],o["total"],"provider_refund",f"Order #{oid} provider refund"))
                    mode="wallet"
                cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],))
                u=cur.fetchone()
                result={"changed":True,"mode":mode,"tg_id":u["tg_id"] if u else None,"amount":float(o["total"] or 0)}
    try:
        record_order_event(
            oid,"auto_topup_refunded","refund_pending" if result["mode"]=="external" else "refunded",
            str(reason)[:1000],None)
        sync_loyalty_profile(o["user_id"])
    except Exception as exc:
        record_runtime_error("auto_topup_refund_post",exc,{"order_id":oid})
    return result


def auto_topup_apply_provider_response(auto_id,response,source="status"):
    """Normalize a provider response and apply exactly one local transition."""
    aid=int(auto_id)
    data=response.get("data") if isinstance(response,dict) and isinstance(response.get("data"),dict) else response
    data=data if isinstance(data,dict) else {}
    provider_status=_autotopup_status(data.get("statusCode"))
    invoice=str(data.get("invoiceNumber") or "")
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM autotopup_orders WHERE id=%s FOR UPDATE",(aid,))
                ao=cur.fetchone()
                if not ao:
                    raise ValueError("Auto Top-Up record not found.")
                old=str(ao.get("status") or "")
                cur.execute(
                    """UPDATE autotopup_orders SET provider_order_id=CASE WHEN %s<>'' THEN %s ELSE provider_order_id END,
                       status=%s,response_json=%s::jsonb,updated_at=NOW() WHERE id=%s""",
                    (invoice,invoice,provider_status,json.dumps(response,separators=(",",":")),aid))
                order_id=ao.get("order_id")
    transition={"auto_id":aid,"order_id":order_id,"status":provider_status,"changed":old!=provider_status,"source":source}
    if order_id and provider_status=="success":
        try:
            transition["local_changed"]=bool(auto_topup_mark_success(order_id,response))
        except Exception as exc:
            transition["conflict"]=str(exc)
            record_runtime_error("auto_topup_success_transition",exc,{"auto_id":aid,"order_id":order_id,"source":source})
    elif order_id and provider_status=="refunded":
        try:
            transition["refund"]=auto_topup_handle_provider_refund(order_id,data.get("statusDesc") or "Provider reported REFUNDED")
        except Exception as exc:
            transition["conflict"]=str(exc)
            record_runtime_error("auto_topup_refund_transition",exc,{"auto_id":aid,"order_id":order_id,"source":source})
    return transition


def auto_topup_refresh_provider_order(auto_id):
    """Reconcile without ever replaying a paid checkout request.

    Prefer provider invoice lookup. If checkout timed out before an invoice was
    persisted, recover by the unique referenceNumber stored in request_json.
    """
    ao=db_execute("SELECT * FROM autotopup_orders WHERE id=%s",(int(auto_id),),"one")
    if not ao:
        raise ValueError("Auto Top-Up record not found.")
    if ao["status"] not in {"creating","processing","uncertain"}:
        return {"auto_id":int(auto_id),"status":ao["status"],"changed":False}
    invoice=str(ao.get("provider_order_id") or "").strip()
    if invoice:
        response=bangjeff_get_order(invoice)
        return auto_topup_apply_provider_response(auto_id,response,"poll_invoice")
    request_json=ao.get("request_json") or {}
    if isinstance(request_json,str):
        try: request_json=json.loads(request_json)
        except Exception: request_json={}
    reference=str(request_json.get("referenceNumber") or "").strip() if isinstance(request_json,dict) else ""
    if not reference:
        return {"auto_id":int(auto_id),"status":ao["status"],"changed":False,"reason":"invoice_and_reference_missing"}
    response=bangjeff_get_order_by_reference(reference)
    return auto_topup_apply_provider_response(auto_id,response,"poll_reference")


def auto_topup_process_webhook(payload):
    """Reconcile BANGJEFF transaction webhook by invoice or our referenceNumber."""
    root=payload.get("data") if isinstance(payload,dict) and isinstance(payload.get("data"),dict) else payload
    root=root if isinstance(root,dict) else {}
    invoice=str(root.get("invoiceNumber") or "").strip()
    reference=str(root.get("referenceNumber") or "").strip()
    if not invoice and not reference:
        raise ValueError("Webhook has no invoiceNumber/referenceNumber.")
    if invoice:
        ao=db_execute(
            "SELECT * FROM autotopup_orders WHERE provider=%s AND provider_order_id=%s ORDER BY id DESC LIMIT 1",
            (AUTO_TOPUP_PROVIDER,invoice),"one")
    else:
        ao=None
    if not ao and reference:
        ao=db_execute(
            """SELECT * FROM autotopup_orders WHERE provider=%s
               AND request_json->>'referenceNumber'=%s ORDER BY id DESC LIMIT 1""",
            (AUTO_TOPUP_PROVIDER,reference),"one")
    if not ao:
        raise ValueError("Webhook order not found locally.")
    return auto_topup_apply_provider_response(ao["id"],payload,"webhook")


def _auto_topup_queue_transition_notice(result):
    oid=result.get("order_id")
    if not oid:
        return
    order=db_execute(
        """SELECT o.id,o.total,o.status,o.payment_mode,u.tg_id,p.name
           FROM orders o JOIN users u ON u.id=o.user_id
           JOIN products p ON p.id=o.product_id WHERE o.id=%s""",(int(oid),),"one")
    if not order:
        return
    if result.get("status")=="success" and result.get("local_changed"):
        enqueue_notification(
            order["tg_id"],
            f"✅ <b>Order #{oid} Auto Top-Up Completed</b>\\n\\n"
            f"🎮 {html.escape(order['name'])}\\nProvider confirmed the top-up successfully.",
            [[{"text":"📦 My Orders","callback_data":"home:orders"}]])
    elif result.get("status")=="refunded":
        refund=result.get("refund") or {}
        if refund.get("changed") and refund.get("mode")=="wallet":
            enqueue_notification(
                order["tg_id"],
                f"↩️ <b>Order #{oid} Refunded</b>\\n\\n"
                f"{fmt_money(order['total'])} was returned to your bot wallet.",
                [[{"text":"💰 Wallet / Profile","callback_data":"home:profile"}]])
        elif refund.get("changed") and refund.get("mode")=="external":
            enqueue_notification(
                order["tg_id"],
                f"↩️ <b>Order #{oid} Refund Pending</b>\\n\\n"
                "The provider refunded the top-up. Your original direct-payment refund now requires admin processing.",
                [[{"text":"📦 My Orders","callback_data":"home:orders"}]])
            for admin_id in ADMIN_IDS:
                enqueue_notification(
                    admin_id,
                    f"⚠️ <b>Provider Refund Requires Action</b>\\n\\nOrder #{oid} • {fmt_money(order['total'])}\\n"
                    "BANGJEFF reported REFUNDED. Send the refund through the original payment method, then confirm it in Refunds.",
                    [[{"text":"↩️ Refunds","callback_data":"admin:refunds"}]])


async def auto_topup_status_loop(bot):
    """Reconcile provider orders only when live Auto Top-Up is explicitly enabled."""
    while True:
        try:
            if auto_topup_master_enabled() and provider_connection_ready():
                cfg=auto_topup_config()
                if cfg.get("order_status_path_template"):
                    rows=await asyncio.to_thread(
                        db_execute,
                        """SELECT id FROM autotopup_orders
                           WHERE provider=%s AND status IN ('creating','processing','uncertain')
                           ORDER BY updated_at ASC LIMIT 50""",
                        (AUTO_TOPUP_PROVIDER,),"all")
                    for row in rows or []:
                        try:
                            result=await asyncio.to_thread(auto_topup_refresh_provider_order,row["id"])
                            if result.get("changed") or result.get("local_changed") or result.get("refund",{}).get("changed"):
                                await asyncio.to_thread(_auto_topup_queue_transition_notice,result)
                        except Exception as exc:
                            record_runtime_error("auto_topup_status_poll",exc,{"auto_id":row["id"]})
                        await asyncio.sleep(0.15)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            record_runtime_error("auto_topup_status_loop",exc,{"instance_id":INSTANCE_ID})
        await asyncio.sleep(auto_topup_config().get("status_poll_seconds",60))


@router.callback_query(F.data.startswith("at:refresh_order:"))
async def admin_auto_topup_refresh_order(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    aid=int(c.data.rsplit(":",1)[1])
    try:
        result=await asyncio.to_thread(auto_topup_refresh_provider_order,aid)
        if result.get("changed") or result.get("local_changed") or result.get("refund",{}).get("changed"):
            await asyncio.to_thread(_auto_topup_queue_transition_notice,result)
    except Exception as exc:
        return await c.answer(str(exc)[:190],show_alert=True)
    await c.answer(f"Status: {result.get('status','unknown')}",show_alert=True)
    c.data="at:orders"
    return await admin_auto_topup_orders(c)

def bangjeff_checkout(mapping,order,reference_number):
    cfg=auto_topup_config()
    payload=bangjeff_checkout_payload(mapping,order,reference_number)
    return payload,bangjeff_request("POST",cfg["order_path"],payload)

def _autotopup_status(value):
    s=str(value or "").strip().upper()
    if s in {"SUCCESS","COMPLETED"}: return "success"
    if s in {"REFUNDED","REFUND"}: return "refunded"
    if s in {"FAILED","ERROR","CANCELLED"}: return "failed"
    if s in {"PROCESSING","PENDING","IN_PROGRESS","WAITING"}: return "processing"
    # Missing/new provider states are ambiguous. Never treat them as success or
    # replay checkout; keep them reconcilable until webhook/poll resolves them.
    return "uncertain"

def auto_topup_mark_success(order_id,provider_response=None):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(int(order_id),)); o=cur.fetchone()
                if not o: raise ValueError("Order not found.")
                if o["status"]=="completed": return False
                if o["status"] not in {"pending","awaiting_payment"}: raise ValueError(f"Order status {o['status']} cannot be auto-completed.")
                cur.execute("UPDATE orders SET status='completed',account_password='',processed_at=NOW(),updated_at=NOW() WHERE id=%s",(int(order_id),))
                award_completed_order_rewards(cur,int(order_id),o["user_id"],o["total"])
    try:
        sync_loyalty_profile(o["user_id"])
    except Exception as exc:
        record_runtime_error("auto_topup_loyalty_sync", exc, {"order_id": int(order_id), "user_id": o["user_id"]})
    try:
        record_order_event(int(order_id),"auto_topup_success","completed","Provider confirmed successful top-up.",None)
    except Exception as exc:
        record_runtime_error("auto_topup_order_event", exc, {"order_id": int(order_id)})
    return True

def auto_topup_try_fulfill_order(order_id):
    """Idempotent paid provider checkout with fail-closed network recovery."""
    if not auto_topup_master_enabled():
        return {"handled":False,"reason":"master_off"}
    order=db_execute("SELECT * FROM orders WHERE id=%s",(int(order_id),),"one")
    if not order or order["status"] not in {"pending","awaiting_payment"}:
        return {"handled":False,"reason":"order_not_pending"}
    mapping=auto_topup_mapping(order["product_id"])
    if not mapping:
        return {"handled":False,"reason":"not_mapped"}
    if not int(mapping.get("provider_active") or 0):
        return {"handled":False,"reason":"provider_variant_inactive"}
    existing=db_execute(
        "SELECT * FROM autotopup_orders WHERE provider=%s AND order_id=%s",
        (AUTO_TOPUP_PROVIDER,int(order_id)),"one")
    if existing:
        return {"handled":True,"status":existing["status"],"existing":True}
    reference=_autotopup_reference(order_id)
    payload=bangjeff_checkout_payload(mapping,order,reference)
    try:
        row=db_execute(
            """INSERT INTO autotopup_orders(provider,order_id,provider_product_id,game_uid,status,request_json,response_json)
               VALUES(%s,%s,%s,%s,'creating',%s::jsonb,'{}'::jsonb) RETURNING id""",
            (AUTO_TOPUP_PROVIDER,int(order_id),mapping["provider_variant_code"],order.get("game_uid") or "",
             json.dumps(payload,separators=(",",":"))),"one")
    except Exception:
        existing=db_execute(
            "SELECT * FROM autotopup_orders WHERE provider=%s AND order_id=%s",
            (AUTO_TOPUP_PROVIDER,int(order_id)),"one")
        return {"handled":True,"status":existing["status"] if existing else "claimed","existing":True}
    auto_id=row["id"]
    try:
        response=bangjeff_request("POST",auto_topup_config()["order_path"],payload)
        result=auto_topup_apply_provider_response(auto_id,response,"checkout")
        data=response.get("data") if isinstance(response,dict) and isinstance(response.get("data"),dict) else {}
        total_obj=data.get("totalAmount") if isinstance(data.get("totalAmount"),dict) else {}
        db_execute(
            "UPDATE autotopup_orders SET amount=%s,currency=%s,updated_at=NOW() WHERE id=%s",
            (total_obj.get("value",mapping.get("cost")),total_obj.get("currency",mapping.get("currency")),auto_id))
        return {"handled":True,**result,"response":response}
    except Exception as exc:
        # A connection failure after sending Checkout is ambiguous: never retry the
        # paid request automatically. Keep the exact reference for webhook recovery.
        message=str(exc)
        uncertain=any(x in message.lower() for x in ("timed out","timeout","connection","reset","remote end","temporarily"))
        status="uncertain" if uncertain else "failed"
        db_execute(
            "UPDATE autotopup_orders SET status=%s,response_json=%s::jsonb,updated_at=NOW() WHERE id=%s",
            (status,json.dumps({"error":message[:1500]},separators=(",",":")),auto_id))
        record_runtime_error("auto_topup_checkout",exc,{"order_id":int(order_id),"auto_id":auto_id,"status":status})
        return {"handled":True,"status":status,"error":message}

@router.callback_query(F.data=="at:mapping")
async def admin_auto_topup_mapping(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    count=await asyncio.to_thread(auto_topup_mapping_count); await state.set_state(AdminState.auto_topup_map); await c.answer()
    await c.message.edit_text(f"🔗 <b>PRODUCT MAPPING</b>\n\nActive mappings: <b>{count}</b>\n\nRun 📦 Sync Products first, then send:\n<code>LOCAL_PRODUCT_ID | PROVIDER_VARIANT_CODE</code>\n\nExample:\n<code>25 | FF_ID_100</code>\n\nSafety: automatic checkout currently supports provider mappings that require exactly one input (UID). Multi-input games stay manual.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel",callback_data="at:mapping_cancel")]]))

@router.callback_query(F.data=="at:mapping_cancel")
async def admin_auto_topup_mapping_cancel(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await state.clear(); await c.answer("Cancelled."); c.data="at:settings"; return await admin_auto_topup_settings(c)

@router.message(AdminState.auto_topup_map)
async def admin_auto_topup_mapping_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return await state.clear()
    raw=(m.text or "").strip()
    if raw.lower()=="/cancel": await state.clear(); return await m.answer("Cancelled.",reply_markup=admin_menu())
    parts=[x.strip() for x in raw.split("|",1)]
    if len(parts)!=2 or not parts[0].isdigit() or not parts[1]: return await m.answer("❌ Use: <code>LOCAL_PRODUCT_ID | PROVIDER_VARIANT_CODE</code>")
    try: result=await asyncio.to_thread(auto_topup_save_mapping,int(parts[0]),parts[1])
    except Exception as exc: return await m.answer(f"❌ Mapping failed:\n<code>{html.escape(str(exc)[:1200])}</code>")
    await state.clear(); await m.answer(f"✅ Mapped local product <b>#{result['local']['id']} {html.escape(result['local']['name'])}</b>\n→ <code>{html.escape(parts[1])}</code>\nProvider inputs: <b>{len(result['inputs'])}</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🤖 Auto Top-Up",callback_data="admin:auto_topup")]]))

def auto_topup_status_text():
    cfg=auto_topup_config()
    armed=auto_topup_live_armed()
    requested=auto_topup_master_requested()
    master=auto_topup_master_enabled()
    ff=auto_topup_free_fire_enabled()
    ready=provider_connection_ready()
    if master and ready:
        status="🟢 Enabled"
    elif not armed:
        status="🔒 OFF • Railway safety lock"
    elif not requested:
        status="🔴 OFF • Admin switch"
    else:
        status="🟡 Armed • API/lifecycle incomplete"
    balance=auto_topup_balance_cache()
    b=f"{balance.get('balance','—')} {balance.get('currency','')}".strip() if balance else "—"
    counts=auto_topup_synced_counts()
    live="🟢 Ready" if provider_live_order_ready() else "🔒 Not ready"
    mapped=auto_topup_mapping_count()
    webhook="🟢 Secured" if cfg.get("webhook_token") else "⚪ Not configured"
    return (
        "🤖 <b>AUTO TOP-UP</b>\n\n"
        f"Status: <b>{status}</b>\n"
        f"Provider: <b>{html.escape(auto_topup_provider_name())}</b>\n"
        f"Balance: <b>{html.escape(str(b))}</b>\n"
        f"Free Fire profile: <b>{'🟢 Enabled' if ff else '🔴 Disabled'}</b>\n"
        f"API connection: <b>{'🟢 Configured' if ready else '🔴 Not configured'}</b>\n"
        f"Live checkout: <b>{live}</b>\n"
        f"Webhook: <b>{webhook}</b>\n"
        f"Mapped shop products: <b>{mapped}</b>\n"
        f"Synced provider variants: <b>{int(counts.get('total') or 0)}</b>\n\n"
        "🛡 Default mode is MANUAL DELIVERY. Provider paid calls stay blocked until Railway + Admin are both explicitly enabled."
    )


def auto_topup_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Check Balance",callback_data="at:balance"),
         InlineKeyboardButton(text="📦 Sync Products",callback_data="at:sync")],
        [InlineKeyboardButton(text="🧪 Test UID",callback_data="at:test_uid"),
         InlineKeyboardButton(text="📋 Top-Up Orders",callback_data="at:orders")],
        [InlineKeyboardButton(text="⚙️ Settings",callback_data="at:settings")],
        [InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]
    ])


@router.callback_query(F.data=="admin:auto_topup")
async def admin_auto_topup(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await c.answer()
    await c.message.edit_text(
        await asyncio.to_thread(auto_topup_status_text),
        reply_markup=auto_topup_admin_kb())


@router.callback_query(F.data=="at:balance")
async def admin_auto_topup_balance(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    try:
        cache=await asyncio.to_thread(provider_check_balance)
    except Exception as exc:
        cfg=auto_topup_config()
        record_runtime_error("autotopup_balance",exc,{"admin":c.from_user.id,"env":_auto_topup_env("BANGJEFF_ENV","production"),"base":cfg.get("base"),"region":cfg.get("region")})
        await c.answer("Check Balance failed — details sent below.",show_alert=True)
        return await c.message.answer(
            f"❌ <b>{html.escape(auto_topup_provider_name())} Check Balance failed</b>\n\n"
            f"<code>{html.escape(str(exc)[:1200])}</code>\n\n"
            f"Environment: <b>{html.escape(_auto_topup_env('BANGJEFF_ENV','production'))}</b>\n"
            f"Region: <b>{html.escape(str(cfg.get('region') or ''))}</b>\n"
            f"Endpoint: <code>{html.escape(str(cfg.get('base') or '') + str(cfg.get('balance_path') or ''))}</code>\n\n"
            "API key/signature are intentionally hidden."
        )
    await c.answer(f"Balance: {cache.get('balance')} {cache.get('currency','')}".strip(),show_alert=True)
    c.data="admin:auto_topup"
    return await admin_auto_topup(c)


@router.callback_query(F.data=="at:sync")
async def admin_auto_topup_sync(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await c.answer("Syncing…")
    try:
        count=await asyncio.to_thread(provider_sync_products)
    except Exception as exc:
        record_runtime_error("autotopup_product_sync",exc,{"admin":c.from_user.id})
        return await c.message.answer(f"❌ Product sync failed:\n<code>{html.escape(str(exc)[:900])}</code>")
    await c.message.answer(f"✅ Synced <b>{count}</b> provider products.")
    c.data="admin:auto_topup"
    return await admin_auto_topup(c)


@router.callback_query(F.data=="at:test_uid")
async def admin_auto_topup_uid_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await state.set_state(AdminState.auto_topup_uid_test)
    await c.answer()
    await c.message.answer(
        "🧪 <b>Test Free Fire UID</b>\n\n"
        "Send:\n<code>UID</code>\n"
        "or, if the provider requires a zone/server:\n<code>UID | ZONE</code>\n\n"
        "Send /cancel to stop.")


@router.message(AdminState.auto_topup_uid_test)
async def admin_auto_topup_uid_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return await state.clear()
    raw=(m.text or "").strip()
    if raw.lower()=="/cancel":
        await state.clear()
        return await m.answer("Cancelled.")
    parts=[x.strip() for x in raw.split("|",1)]
    uid=parts[0] if parts else ""
    zone=parts[1] if len(parts)>1 else ""
    if not uid:
        return await m.answer("Send a UID, or UID | ZONE.")
    try:
        result=await asyncio.to_thread(provider_test_uid,uid,zone)
    except Exception as exc:
        record_runtime_error("autotopup_uid_test",exc,{"admin":m.from_user.id})
        return await m.answer(f"❌ UID test failed:\n<code>{html.escape(str(exc)[:900])}</code>")
    await state.clear()
    preview=json.dumps(result,ensure_ascii=False,indent=2)[:3000]
    await m.answer(
        f"✅ <b>{html.escape(auto_topup_provider_name())} UID response</b>\n\n"
        f"<pre>{html.escape(preview)}</pre>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🤖 Auto Top-Up",callback_data="admin:auto_topup")
        ]]))


@router.callback_query(F.data=="at:orders")
async def admin_auto_topup_orders(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    rows=await adb_execute(
        """SELECT id,order_id,provider_order_id,provider_product_id,game_uid,status,amount,currency,created_at
           FROM autotopup_orders WHERE provider=%s ORDER BY id DESC LIMIT 20""",
        (AUTO_TOPUP_PROVIDER,),"all") or []
    if rows:
        body="\n".join(
            f"• #{r['id']} • order {r.get('order_id') or '—'} • <b>{html.escape(r.get('status') or '')}</b> • "
            f"{html.escape(r.get('provider_order_id') or 'no provider invoice')}"
            for r in rows)
    else:
        body="No provider top-up orders yet."
    buttons=[]
    for r in rows[:8]:
        if r.get("status") in {"creating","processing","uncertain"}:
            buttons.append([InlineKeyboardButton(
                text=f"🔄 Refresh #{r['id']} ({r['status']})",
                callback_data=f"at:refresh_order:{r['id']}")])
    buttons += [
        [InlineKeyboardButton(text="🔄 Refresh List",callback_data="at:orders")],
        [InlineKeyboardButton(text="⬅️ Auto Top-Up",callback_data="admin:auto_topup")]
    ]
    await c.answer()
    await c.message.edit_text(
        "📋 <b>Top-Up Orders</b>\n\n"+body,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data=="at:settings")
async def admin_auto_topup_settings(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    armed=auto_topup_live_armed()
    requested=auto_topup_master_requested()
    master=auto_topup_master_enabled()
    ff=auto_topup_free_fire_enabled()
    cfg=auto_topup_config()
    await c.answer()
    await c.message.edit_text(
        "⚙️ <b>AUTO TOP-UP SETTINGS</b>\n\n"
        f"Railway safety gate: <b>{'🟢 ARMED' if armed else '🔒 LOCKED'}</b>\n"
        f"Admin master switch: <b>{'🟢 ON' if requested else '🔴 OFF'}</b>\n"
        f"Effective Auto Top-Up: <b>{'🟢 ON' if master else '🔴 OFF'}</b>\n"
        f"Free Fire profile: <b>{'🟢 ON' if ff else '🔴 OFF'}</b>\n"
        f"API credentials: <b>{'🟢 Configured' if cfg['api_key'] else '🔴 Missing'}</b>\n"
        f"Checkout endpoint: <b>{html.escape(cfg['order_path'] or 'Missing')}</b>\n"
        f"Order-status endpoint: <b>{html.escape(cfg.get('order_status_path_template') or 'Not configured')}</b>\n"
        f"Webhook token: <b>{'🟢 Configured' if cfg.get('webhook_token') else '⚪ Not configured'}</b>\n"
        f"Mappings: <b>{auto_topup_mapping_count()}</b>\n\n"
        "Current safe setup: keep <code>AUTO_TOPUP_LIVE_ALLOWED=0</code>.\n"
        "Later, after a real provider/API is verified, configure credentials + lifecycle endpoints, set the Railway gate to 1, then turn the Admin master switch ON.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{'🟢' if requested else '🔴'} Master ON/OFF",callback_data="at:toggle:master")],
            [InlineKeyboardButton(text=f"{'🟢' if ff else '🔴'} Free Fire ON/OFF",callback_data="at:toggle:ff")],
            [InlineKeyboardButton(text="🔗 Product Mapping",callback_data="at:mapping")],
            [InlineKeyboardButton(text="📋 Top-Up Orders",callback_data="at:orders")],
            [InlineKeyboardButton(text="⬅️ Auto Top-Up",callback_data="admin:auto_topup")]
        ]))


@router.callback_query(F.data.startswith("at:toggle:"))
async def admin_auto_topup_toggle(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    target=c.data.rsplit(":",1)[1]
    if target=="master":
        requested=auto_topup_master_requested()
        if not requested:
            if not auto_topup_live_armed():
                await asyncio.to_thread(set_setting,"autotopup_live_enabled","0")
                return await c.answer(
                    "🔒 Auto Top-Up is safety-locked. Keep AUTO_TOPUP_LIVE_ALLOWED=0 until a real API is ready.",
                    show_alert=True)
            if not provider_connection_ready():
                return await c.answer("API credentials are not configured yet.",show_alert=True)
            if not auto_topup_config().get("order_status_path_template"):
                return await c.answer("Order-status endpoint is not configured. Live Auto Top-Up stays OFF.",show_alert=True)
            new="1"
        else:
            new="0"
        await asyncio.to_thread(set_setting,"autotopup_live_enabled",new)
    elif target=="ff":
        new="0" if auto_topup_free_fire_enabled() else "1"
        await asyncio.to_thread(set_setting,"autotopup_free_fire_enabled",new)
    else:
        return await c.answer("Unknown setting.",show_alert=True)
    await asyncio.to_thread(admin_log,c.from_user.id,"auto_topup_toggle",f"{target}={new}")
    await c.answer("Updated.")
    c.data="at:settings"
    return await admin_auto_topup_settings(c)

def order_recovery_snapshot(limit=20):
    """Read-only recovery view. It never replays checkout or mutates money/stock."""
    limit=max(1,min(50,int(limit or 20)))
    summary=db_execute(
        """SELECT
          (SELECT COUNT(*) FROM orders WHERE status='pending' AND updated_at < NOW()-(%s * INTERVAL '1 minute')) AS stale_pending,
          (SELECT COUNT(*) FROM orders WHERE status='refund_pending' AND updated_at < NOW()-(%s * INTERVAL '1 minute')) AS stale_refunds,
          (SELECT COUNT(*) FROM autotopup_orders WHERE provider=%s AND status IN ('creating','processing','uncertain') AND updated_at < NOW()-(%s * INTERVAL '1 minute')) AS stale_provider,
          (SELECT COUNT(*) FROM autotopup_orders WHERE provider=%s AND status='failed') AS provider_failed""",
        (ORDER_RECOVERY_PENDING_MINUTES,ORDER_RECOVERY_REFUND_MINUTES,AUTO_TOPUP_PROVIDER,ORDER_RECOVERY_PROVIDER_MINUTES,AUTO_TOPUP_PROVIDER),"one") or {}
    rows=db_execute(
        """SELECT o.id AS order_id,o.status AS order_status,o.total,o.payment_mode,o.updated_at,
                  u.tg_id,p.name AS product_name,
                  ao.id AS auto_id,ao.status AS auto_status,ao.provider_order_id,ao.updated_at AS auto_updated_at
           FROM orders o
           JOIN users u ON u.id=o.user_id
           JOIN products p ON p.id=o.product_id
           LEFT JOIN autotopup_orders ao ON ao.order_id=o.id AND ao.provider=%s
           WHERE (o.status='pending' AND o.updated_at < NOW()-(%s * INTERVAL '1 minute'))
              OR (o.status='refund_pending' AND o.updated_at < NOW()-(%s * INTERVAL '1 minute'))
              OR (ao.status IN ('creating','processing','uncertain') AND ao.updated_at < NOW()-(%s * INTERVAL '1 minute'))
           ORDER BY GREATEST(o.updated_at,COALESCE(ao.updated_at,o.updated_at)) ASC
           LIMIT %s""",
        (AUTO_TOPUP_PROVIDER,ORDER_RECOVERY_PENDING_MINUTES,ORDER_RECOVERY_REFUND_MINUTES,ORDER_RECOVERY_PROVIDER_MINUTES,limit),"all") or []
    return {"summary":summary,"rows":rows}


def reconcile_stale_provider_orders(limit=10):
    """Status-only reconciliation. Never calls provider checkout POST again."""
    ids=db_execute(
        """SELECT id FROM autotopup_orders
           WHERE provider=%s AND status IN ('creating','processing','uncertain')
             AND updated_at < NOW()-(%s * INTERVAL '1 minute')
           ORDER BY updated_at ASC LIMIT %s""",
        (AUTO_TOPUP_PROVIDER,ORDER_RECOVERY_PROVIDER_MINUTES,max(1,min(20,int(limit or 10)))),"all") or []
    results=[]
    for row in ids:
        aid=int(row["id"])
        try:
            results.append(auto_topup_refresh_provider_order(aid))
        except Exception as exc:
            record_runtime_error("recovery_reconcile_provider",exc,{"auto_id":aid})
            results.append({"auto_id":aid,"status":"error","error":str(exc)[:300]})
    return results


@router.callback_query(F.data=="admin:recovery")
async def admin_recovery_center(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    snap=await asyncio.to_thread(order_recovery_snapshot,20)
    sm=snap["summary"]; rows=snap["rows"]
    text=(
        "🧭 <b>Order Recovery Center</b>\n\n"
        f"🟠 Stale pending &gt;{ORDER_RECOVERY_PENDING_MINUTES}m: <b>{int(sm.get('stale_pending') or 0)}</b>\n"
        f"🌐 Provider unresolved &gt;{ORDER_RECOVERY_PROVIDER_MINUTES}m: <b>{int(sm.get('stale_provider') or 0)}</b>\n"
        f"💸 Refund pending &gt;{ORDER_RECOVERY_REFUND_MINUTES}m: <b>{int(sm.get('stale_refunds') or 0)}</b>\n"
        f"❌ Provider failed: <b>{int(sm.get('provider_failed') or 0)}</b>\n\n"
        "Recovery is fail-closed: provider refresh checks status only and never replays a paid checkout."
    )
    kb=[]
    for r in rows[:12]:
        oid=int(r["order_id"]); ast=(r.get("auto_status") or "").strip().lower()
        label=f"#{oid} • {ast or r['order_status']} • {str(r['product_name'])[:18]}"
        if r.get("auto_id") and ast in {"creating","processing","uncertain"}:
            kb.append([InlineKeyboardButton(text=f"🔄 {label}",callback_data=f"recovery:refresh:{int(r['auto_id'])}")])
        else:
            kb.append([InlineKeyboardButton(text=f"🧾 {label}",callback_data=f"admin_order:{oid}")])
    kb.append([InlineKeyboardButton(text="🔄 Reconcile Stale Provider Orders",callback_data="recovery:reconcile")])
    kb.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")])
    await c.answer()
    await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("recovery:refresh:"))
async def admin_recovery_refresh_one(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    aid=int(c.data.rsplit(":",1)[1])
    try:
        result=await asyncio.to_thread(auto_topup_refresh_provider_order,aid)
        if result.get("changed") or result.get("local_changed") or result.get("refund",{}).get("changed"):
            await asyncio.to_thread(_auto_topup_queue_transition_notice,result)
    except Exception as exc:
        record_runtime_error("recovery_refresh_one",exc,{"auto_id":aid,"admin_id":c.from_user.id})
        return await c.answer(str(exc)[:190],show_alert=True)
    await asyncio.to_thread(admin_log,c.from_user.id,"recovery_refresh",f"auto_id={aid} status={result.get('status','unknown')}")
    await c.answer(f"Status: {result.get('status','unknown')}",show_alert=True)
    c.data="admin:recovery"
    return await admin_recovery_center(c)


@router.callback_query(F.data=="recovery:reconcile")
async def admin_recovery_reconcile(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    results=await asyncio.to_thread(reconcile_stale_provider_orders,10)
    changed=0; errors=0
    for result in results:
        if result.get("status")=="error": errors+=1
        if result.get("changed") or result.get("local_changed") or result.get("refund",{}).get("changed"):
            changed+=1
            try:
                await asyncio.to_thread(_auto_topup_queue_transition_notice,result)
            except Exception as exc:
                record_runtime_error("recovery_transition_notice",exc,{"result":str(result)[:1000]})
    await asyncio.to_thread(admin_log,c.from_user.id,"recovery_reconcile",f"checked={len(results)} changed={changed} errors={errors}")
    await c.answer(f"Checked {len(results)} • changed {changed} • errors {errors}",show_alert=True)
    c.data="admin:recovery"
    return await admin_recovery_center(c)


_RECOVERY_ALERT_STATE={"signature":None,"last":0.0}


async def order_recovery_alert_loop(bot):
    """Periodic stale-state alert; read-only except safe provider GET reconciliation remains admin-triggered."""
    while True:
        try:
            snap=await asyncio.to_thread(order_recovery_snapshot,5)
            sm=snap["summary"]
            counts=(int(sm.get("stale_pending") or 0),int(sm.get("stale_provider") or 0),int(sm.get("stale_refunds") or 0))
            signature=counts
            now_m=time.monotonic()
            if any(counts) and (signature!=_RECOVERY_ALERT_STATE.get("signature") or now_m-float(_RECOVERY_ALERT_STATE.get("last") or 0)>=ORDER_RECOVERY_ALERT_COOLDOWN_SECONDS):
                msg=("🧭 <b>Recovery Alert</b>\n\n"
                     f"🟠 Stale pending: <b>{counts[0]}</b>\n"
                     f"🌐 Provider unresolved: <b>{counts[1]}</b>\n"
                     f"💸 Refund pending: <b>{counts[2]}</b>\n\n"
                     "Open Recovery Center to review. No paid checkout is retried automatically.")
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(admin_id,msg,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🧭 Recovery Center",callback_data="admin:recovery")]]))
                    except Exception as exc:
                        record_runtime_error("recovery_alert_notify",exc,{"admin_id":admin_id})
                _RECOVERY_ALERT_STATE.update(signature=signature,last=now_m)
            elif not any(counts):
                _RECOVERY_ALERT_STATE["signature"]=None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            record_runtime_error("order_recovery_alert_loop",exc,{"instance_id":INSTANCE_ID})
        await asyncio.sleep(300)


def admin_command_center_snapshot():
    """Read-only cross-system snapshot for the unified admin command center."""
    row=db_execute("""SELECT
      (SELECT COUNT(*) FROM orders WHERE status='pending') AS pending_orders,
      (SELECT COUNT(*) FROM orders WHERE status='refund_pending') AS refund_pending,
      (SELECT COUNT(*) FROM payments WHERE status='pending') AS pending_payments,
      (SELECT COUNT(*) FROM customer_support_tickets WHERE status='open') AS open_customer_support,
      (SELECT COUNT(*) FROM payment_support_cases WHERE status='open') AS open_payment_support,
      (SELECT COUNT(*) FROM error_events WHERE resolved=FALSE) AS open_errors,
      (SELECT COUNT(*) FROM notification_queue WHERE status='failed') AS failed_notifications,
      (SELECT COUNT(*) FROM autotopup_orders WHERE provider=%s AND status IN ('creating','processing','uncertain')) AS provider_unresolved,
      (SELECT COUNT(*) FROM products WHERE active=1 AND (CASE WHEN delivery_type='code' THEN
          (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=products.id AND pc.status='available') ELSE stock END)<=%s) AS low_stock,
      (SELECT COALESCE(SUM(total),0) FROM orders WHERE status='completed' AND created_at::date=CURRENT_DATE) AS today_sales,
      (SELECT COUNT(*) FROM orders WHERE status='completed' AND created_at::date=CURRENT_DATE) AS today_orders
    """,(AUTO_TOPUP_PROVIDER,low_stock_threshold()),"one") or {}
    recovery=order_recovery_snapshot(5).get("summary",{})
    return {**row,
      "stale_pending":int(recovery.get("stale_pending") or 0),
      "stale_provider":int(recovery.get("stale_provider") or 0),
      "stale_refunds":int(recovery.get("stale_refunds") or 0),
    }


def render_admin_command_center(snap):
    ops_risk=(int(snap.get("open_errors") or 0)+int(snap.get("failed_notifications") or 0)
              +int(snap.get("stale_provider") or 0)+int(snap.get("stale_refunds") or 0))
    health="🟢 Stable" if ops_risk==0 else ("🟡 Attention" if ops_risk<5 else "🔴 Action Needed")
    return (
      "🎛 <b>Unified Admin Command Center</b>\n"
      f"System: <b>{health}</b>\n\n"
      "💼 <b>Business Today</b>\n"
      f"💵 Sales: <b>{fmt_money(snap.get('today_sales') or 0)}</b> • Orders: <b>{int(snap.get('today_orders') or 0)}</b>\n\n"
      "💳 <b>Money & Orders</b>\n"
      f"🧾 Pending orders: <b>{int(snap.get('pending_orders') or 0)}</b>\n"
      f"💳 Pending payments: <b>{int(snap.get('pending_payments') or 0)}</b>\n"
      f"💸 Refund pending: <b>{int(snap.get('refund_pending') or 0)}</b>\n\n"
      "🎧 <b>Customer Operations</b>\n"
      f"🎫 Customer support: <b>{int(snap.get('open_customer_support') or 0)}</b>\n"
      f"🛡 Payment support: <b>{int(snap.get('open_payment_support') or 0)}</b>\n"
      f"📦 Low stock: <b>{int(snap.get('low_stock') or 0)}</b>\n\n"
      "⚙️ <b>Reliability</b>\n"
      f"🌐 Provider unresolved: <b>{int(snap.get('provider_unresolved') or 0)}</b>\n"
      f"🧭 Stale pending/provider/refund: <b>{int(snap.get('stale_pending') or 0)}/{int(snap.get('stale_provider') or 0)}/{int(snap.get('stale_refunds') or 0)}</b>\n"
      f"🚨 Open runtime errors: <b>{int(snap.get('open_errors') or 0)}</b>\n"
      f"🔔 Failed notifications: <b>{int(snap.get('failed_notifications') or 0)}</b>\n\n"
      "ℹ️ This screen is read-only. Financial and provider mutations remain behind their existing guarded workflows."
    )


def admin_command_center_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
      [InlineKeyboardButton(text="🧾 Orders",callback_data="admin:orders"),InlineKeyboardButton(text="💳 Payments",callback_data="admin:payments")],
      [InlineKeyboardButton(text="💸 Refunds",callback_data="admin:refunds"),InlineKeyboardButton(text="🧾 Financial Audit",callback_data="admin:financial_audit")],
      [InlineKeyboardButton(text="🎧 Customer Support",callback_data="admin:customer_support"),InlineKeyboardButton(text="💳 Payment Support",callback_data="admin:support")],
      [InlineKeyboardButton(text="🧩 CRM",callback_data="admin:crm"),InlineKeyboardButton(text="🧭 Recovery",callback_data="admin:recovery")],
      [InlineKeyboardButton(text="📡 Production Ops",callback_data="admin:ops_overview"),InlineKeyboardButton(text="🛡 Risk Radar",callback_data="admin:risk")],
      [InlineKeyboardButton(text="🤖 Auto Top-Up",callback_data="admin:auto_topup"),InlineKeyboardButton(text="🛍 Products",callback_data="admin:products")],
      [InlineKeyboardButton(text="📊 Analytics V3",callback_data="admin:analytics_v3:7"),InlineKeyboardButton(text="🧠 Smart Offers",callback_data="admin:smart_offer_engine")],
      [InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:command_center"),InlineKeyboardButton(text="📊 Dashboard",callback_data="admin:dashboard")],
    ])


@router.callback_query(F.data=="admin:command_center")
async def admin_command_center(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    try:
        snap=await asyncio.to_thread(admin_command_center_snapshot)
    except Exception as exc:
        error_id=record_runtime_error("admin_command_center",exc,{"admin_id":c.from_user.id})
        return await c.answer(f"Command Center unavailable. Ref: {error_id}",show_alert=True)
    await c.answer()
    await c.message.edit_text(render_admin_command_center(snap),reply_markup=admin_command_center_kb())


@router.callback_query(F.data=="admin:dashboard")
async def admin_dashboard(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    row=await adb_execute("""SELECT COUNT(*) AS users,
        (SELECT COUNT(*) FROM products WHERE active=1) AS products,
        (SELECT COUNT(*) FROM products WHERE active=1 AND (CASE WHEN delivery_type='code' THEN
          (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=products.id AND pc.status='available') ELSE stock END)<=%s) AS low_stock,
        (SELECT COUNT(*) FROM orders WHERE status='pending') AS pending_orders,
        (SELECT COUNT(*) FROM orders WHERE status='completed') AS completed,
        (SELECT COUNT(*) FROM payments WHERE status='pending') AS pending_payments,
        (SELECT COALESCE(SUM(total),0) FROM orders WHERE status='completed') AS sales,
        (SELECT COALESCE(SUM(total),0) FROM orders WHERE status='completed' AND created_at::date=CURRENT_DATE) AS today_sales,
        (SELECT COALESCE(SUM(total),0) FROM orders WHERE status='completed' AND created_at>=NOW()-INTERVAL '7 days') AS week_sales,
        (SELECT COUNT(*) FROM orders WHERE status='completed' AND created_at>=NOW()-INTERVAL '7 days') AS week_orders,
        (SELECT COALESCE(SUM(balance),0) FROM users) AS balance,
        (SELECT COUNT(*) FROM product_codes WHERE status='available') AS codes
        FROM users""",(low_stock_threshold(),),fetch="one")
    week_orders=int(row.get("week_orders") or 0)
    week_sales=float(row.get("week_sales") or 0)
    aov=(week_sales/week_orders) if week_orders else 0
    body=(
        f"📊 <b>{shop_name()} — Admin Dashboard</b>\n\n"
        f"👥 Users: <b>{row['users']}</b>\n"
        f"🛍 Active Products: <b>{row['products']}</b>\n"
        f"⚠️ Low-stock Products: <b>{row['low_stock']}</b>\n"
        f"🎫 Available Codes: <b>{row['codes']}</b>\n"
        f"🧾 Pending Orders: <b>{row['pending_orders']}</b>\n"
        f"💳 Pending Payments: <b>{row['pending_payments']}</b>\n"
        f"✅ Completed Orders: <b>{row['completed']}</b>\n\n"
        f"💵 Today Sales: <b>{fmt_money(row['today_sales'])}</b>\n"
        f"📆 7-Day Sales: <b>{fmt_money(week_sales)}</b> • {week_orders} orders\n"
        f"🧮 7-Day AOV: <b>{fmt_money(aov)}</b>\n"
        f"💰 All-time Sales: <b>{fmt_money(row['sales'])}</b>\n"
        f"👛 User Wallet Total: <b>{fmt_money(row['balance'])}</b>"
    )
    await c.answer()
    await c.message.edit_text(body,reply_markup=admin_menu())

@router.callback_query(F.data=="admin:premium")
async def admin_premium(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    row=await adb_execute("""SELECT
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
    row=await adb_execute("""SELECT
        COALESCE(SUM(total) FILTER (WHERE status='completed' AND created_at::date=CURRENT_DATE),0) AS today_sales,
        COUNT(*) FILTER (WHERE created_at::date=CURRENT_DATE) AS today_orders,
        COALESCE(SUM(total) FILTER (WHERE status='completed' AND created_at >= NOW()-INTERVAL '7 days'),0) AS week_sales,
        COUNT(*) FILTER (WHERE created_at >= NOW()-INTERVAL '7 days') AS week_orders,
        COALESCE(SUM(total) FILTER (WHERE status='completed'),0) AS all_sales,
        COUNT(*) FILTER (WHERE status='pending') AS pending,
        COUNT(*) FILTER (WHERE status='refunded') AS refunded
        FROM orders""",fetch="one")
    users=(await adb_execute("SELECT COUNT(*) AS c FROM users",fetch="one"))["c"]
    top=await adb_execute("""SELECT p.name,COUNT(*) AS orders,COALESCE(SUM(o.total),0) AS sales
        FROM orders o JOIN products p ON p.id=o.product_id
        WHERE o.status='completed' GROUP BY p.id,p.name ORDER BY orders DESC,sales DESC LIMIT 5""",fetch="all")
    top_text="\n".join(f"• {html.escape(r['name'])}: {r['orders']} orders / {fmt_money(r['sales'])}" for r in top) or "• No completed sales yet"
    text=(f"📊 <b>{APP_VERSION} Sales Report</b>\n\n📅 Today sales: <b>{fmt_money(row['today_sales'])}</b>\n🧾 Today orders: <b>{row['today_orders']}</b>\n📆 7-day sales: <b>{fmt_money(row['week_sales'])}</b>\n🧾 7-day orders: <b>{row['week_orders']}</b>\n💰 All-time sales: <b>{fmt_money(row['all_sales'])}</b>\n👥 Users: <b>{users}</b>\n⏳ Pending orders: <b>{row['pending']}</b>\n↩️ Refunded orders: <b>{row['refunded']}</b>\n\n🏆 <b>Top Products</b>\n{top_text}")
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]))


@router.callback_query(F.data=="admin:games")
async def admin_games(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    meta_rows=await asyncio.to_thread(game_catalog_rows)
    counts=await asyncio.to_thread(
        db_execute,
        "SELECT category,COUNT(*) AS c FROM products GROUP BY category ORDER BY category",
        (),
        "all",
    ) or []
    product_counts={}
    for r in counts:
        game,_=_split_category(r["category"])
        product_counts[game]=product_counts.get(game,0)+int(r["c"])
    meta={r["game_key"]:r for r in meta_rows}
    games=sorted(set(product_counts)|set(meta),key=lambda g:(
        int(meta.get(g,{}).get("sort_order",100)),
        str(meta.get(g,{}).get("display_name",g)).casefold(),
        g.casefold()
    ))
    kb=[]
    for game in games[:40]:
        m=meta.get(game,{})
        active=int(m.get("active",1))
        emoji=m.get("emoji","🎮")
        display=m.get("display_name",game)
        count=product_counts.get(game,0)
        kb.append([InlineKeyboardButton(
            text=f"{'🟢' if active else '⚫'} {emoji} {display[:24]} • {count}",
            callback_data=f"admin:game:{game}")])
    kb += [
        [InlineKeyboardButton(text="➕ Add / Preconfigure Game",callback_data="admin:game_add")],
        [InlineKeyboardButton(text="⬅️ Products",callback_data="admin:products")]
    ]
    await c.answer()
    await c.message.edit_text(
        "🎮 <b>Game Management</b>\n\n"
        "Control buyer display name, emoji, order and visibility without renaming product categories.\n"
        "Hiding a game never deletes its products.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("admin:game:"))
async def admin_game_detail(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    game=c.data.split(":",2)[2]
    row=await adb_execute(
        "SELECT game_key,display_name,emoji,image_file_id,sort_order,active FROM game_catalog WHERE game_key=%s",
        (game,),"one")
    if not row:
        row={"game_key":game,"display_name":game,"emoji":"🎮","image_file_id":"","sort_order":100,"active":1}
    count=await adb_execute(
        "SELECT COUNT(*) AS c FROM products WHERE category=%s OR category LIKE %s",
        (game,game+" > %"),"one")
    msg=(
        f"🎮 <b>Game Settings</b>\n\n"
        f"Internal key: <code>{html.escape(game)}</code>\n"
        f"Buyer name: <b>{html.escape(str(row['display_name']))}</b>\n"
        f"Emoji: {html.escape(str(row['emoji']))}\n"
        f"Logo/Banner: <b>{'✅ Set' if str(row.get('image_file_id') or '').strip() else '❌ Not set'}</b>\n"
        f"Order: <b>{int(row['sort_order'])}</b>\n"
        f"Visible: <b>{'Yes' if int(row['active']) else 'No'}</b>\n"
        f"Products: <b>{int((count or {}).get('c',0))}</b>\n\n"
        "Internal key stays stable, so existing products/orders are safe."
    )
    logo_set=bool(str(row.get("image_file_id") or "").strip())
    kb_rows=[
        [InlineKeyboardButton(text="✏️ Edit Display",callback_data=f"admin:game_edit:{game}")],
        [InlineKeyboardButton(text="🖼️ Change Logo/Banner" if logo_set else "🖼️ Set Logo/Banner",callback_data=f"agl:{game}")],
    ]
    if logo_set:
        kb_rows.append([InlineKeyboardButton(text="🗑 Remove Logo/Banner",callback_data=f"aglr:{game}")])
    kb_rows += [
        [InlineKeyboardButton(text="🙈 Hide Game" if int(row["active"]) else "👁 Show Game",callback_data=f"agt:{game}")],
        [InlineKeyboardButton(text="⬆️ Move Up",callback_data=f"agm:{game}:-10"),
         InlineKeyboardButton(text="⬇️ Move Down",callback_data=f"agm:{game}:10")],
        [InlineKeyboardButton(text="⬅️ Games",callback_data="admin:games")]
    ]
    kb=InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await c.answer()
    await c.message.edit_text(msg,reply_markup=kb)



@router.callback_query(F.data.startswith("agl:"))
async def admin_game_logo_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    game=c.data.split(":",1)[1]
    await state.update_data(game_logo_key=game)
    await state.set_state(AdminState.game_logo)
    await c.answer()
    await c.message.answer(
        "🖼️ <b>Set Game Logo / Banner</b>\n\n"
        f"Game: <b>{html.escape(game)}</b>\n\n"
        "Send the image as a normal Telegram <b>Photo</b>.\n"
        "Recommended: landscape/banner image, under Telegram's normal photo limits.\n\n"
        "The bot stores Telegram's file_id, so Railway does not need local image storage.\n"
        "Send /cancel to cancel."
    )


@router.message(AdminState.game_logo)
async def admin_game_logo_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return await state.clear()
    if (m.text or "").strip().casefold()=="/cancel":
        await state.clear()
        return await m.answer("❌ Logo update cancelled.")
    if not m.photo:
        return await m.answer("❌ Please send the logo/banner as a Telegram <b>Photo</b>, not as text or a document.")
    d=await state.get_data()
    game=(d.get("game_logo_key") or "").strip()
    if not game:
        await state.clear()
        return await m.answer("❌ Game selection expired. Open Game Management and try again.")
    file_id=m.photo[-1].file_id
    row=await adb_execute("SELECT game_key FROM game_catalog WHERE game_key=%s",(game,),"one")
    if not row:
        await asyncio.to_thread(upsert_game_catalog,game,game,"🎮",100,1)
    await adb_execute(
        "UPDATE game_catalog SET image_file_id=%s,updated_at=NOW() WHERE game_key=%s",
        (file_id,game),
    )
    await asyncio.to_thread(admin_log,m.from_user.id,"game_logo_set",f"{game} logo configured")
    await state.clear()
    await m.answer(
        f"✅ <b>{html.escape(game)}</b> logo/banner saved.\n\n"
        "Buyers will now see this image above that game's packages/products."
    )


@router.callback_query(F.data.startswith("aglr:"))
async def admin_game_logo_remove(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    game=c.data.split(":",1)[1]
    await adb_execute(
        "UPDATE game_catalog SET image_file_id='',updated_at=NOW() WHERE game_key=%s",
        (game,),
    )
    await asyncio.to_thread(admin_log,c.from_user.id,"game_logo_remove",f"{game} logo removed")
    await c.answer("Logo removed.")
    c.data=f"admin:game:{game}"
    return await admin_game_detail(c)


@router.callback_query(F.data=="admin:game_add")
async def admin_game_add_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await state.set_state(AdminState.game_add)
    await c.answer()
    await c.message.answer(
        "➕ <b>Add / Preconfigure Game</b>\n\n"
        "Send:\n<code>Internal Key | Buyer Display Name | Emoji | Order</code>\n\n"
        "Example:\n<code>eFootball | eFootball | ⚽ | 10</code>\n\n"
        "Then bulk import with <code>GAME: eFootball</code>."
    )


@router.message(AdminState.game_add)
async def admin_game_add_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return await state.clear()
    parts=[x.strip() for x in (m.text or "").split("|")]
    if len(parts)!=4:
        return await m.answer("❌ Use: <code>Internal Key | Buyer Display Name | Emoji | Order</code>")
    try:
        await asyncio.to_thread(upsert_game_catalog,parts[0],parts[1],parts[2],int(parts[3]),1)
    except Exception as exc:
        return await m.answer(f"❌ Could not save game: {html.escape(str(exc)[:250])}")
    await asyncio.to_thread(admin_log,m.from_user.id,"game_catalog_upsert",f"{parts[0]} display={parts[1]}")
    await state.clear()
    await m.answer("✅ Game saved. It appears in Shop after active products exist under that game.")


@router.callback_query(F.data.startswith("admin:game_edit:"))
async def admin_game_edit_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    game=c.data.split(":",2)[2]
    row=await adb_execute(
        "SELECT game_key,display_name,emoji,sort_order FROM game_catalog WHERE game_key=%s",
        (game,),"one")
    if not row:
        row={"game_key":game,"display_name":game,"emoji":"🎮","sort_order":100}
    await state.update_data(game_edit_key=game)
    await state.set_state(AdminState.game_edit)
    await c.answer()
    await c.message.answer(
        "✏️ <b>Edit Game Display</b>\n\n"
        "Send:\n<code>Buyer Display Name | Emoji | Order</code>\n\n"
        f"Current:\n<code>{html.escape(str(row['display_name']))} | {html.escape(str(row['emoji']))} | {int(row['sort_order'])}</code>"
    )


@router.message(AdminState.game_edit)
async def admin_game_edit_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return await state.clear()
    d=await state.get_data()
    game=d.get("game_edit_key")
    parts=[x.strip() for x in (m.text or "").split("|")]
    if len(parts)!=3:
        return await m.answer("❌ Use: <code>Buyer Display Name | Emoji | Order</code>")
    current=await adb_execute("SELECT active FROM game_catalog WHERE game_key=%s",(game,),"one")
    try:
        await asyncio.to_thread(
            upsert_game_catalog,game,parts[0],parts[1],int(parts[2]),int((current or {}).get("active",1)))
    except Exception as exc:
        return await m.answer(f"❌ Could not update game: {html.escape(str(exc)[:250])}")
    await asyncio.to_thread(admin_log,m.from_user.id,"game_catalog_edit",f"{game} display={parts[0]}")
    await state.clear()
    await m.answer("✅ Game display updated.")


@router.callback_query(F.data.startswith("agt:"))
async def admin_game_toggle(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    game=c.data.split(":",1)[1]
    row=await adb_execute(
        "SELECT active,display_name,emoji,sort_order FROM game_catalog WHERE game_key=%s",
        (game,),"one")
    if row:
        new=0 if int(row["active"]) else 1
        await asyncio.to_thread(upsert_game_catalog,game,row["display_name"],row["emoji"],row["sort_order"],new)
    else:
        await asyncio.to_thread(upsert_game_catalog,game,game,"🎮",100,0)
        new=0
    await asyncio.to_thread(admin_log,c.from_user.id,"game_visibility",f"{game}={new}")
    await c.answer("Shown" if new else "Hidden")
    c.data=f"admin:game:{game}"
    return await admin_game_detail(c)


@router.callback_query(F.data.startswith("agm:"))
async def admin_game_move(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    _,game,delta_s=c.data.split(":",2)
    delta=int(delta_s)
    row=await adb_execute(
        "SELECT display_name,emoji,sort_order,active FROM game_catalog WHERE game_key=%s",
        (game,),"one")
    if not row:
        row={"display_name":game,"emoji":"🎮","sort_order":100,"active":1}
    new_order=max(-10000,min(10000,int(row["sort_order"])+delta))
    await asyncio.to_thread(upsert_game_catalog,game,row["display_name"],row["emoji"],new_order,row["active"])
    await c.answer("Order updated.")
    c.data=f"admin:game:{game}"
    return await admin_game_detail(c)

@router.callback_query(F.data=="admin:products")
@router.callback_query(F.data.startswith("admin:products:"))
@router.callback_query(F.data.startswith("apf:"))
async def admin_products(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)

    page=0; view="all"
    if c.data.startswith("apf:"):
        parts=c.data.split(":",2)
        view=parts[1] if len(parts)>1 else "all"
        try: page=max(0,int(parts[2])) if len(parts)>2 else 0
        except (TypeError,ValueError): page=0
    elif c.data.startswith("admin:products:"):
        try: page=max(0,int(c.data.rsplit(":",1)[1]))
        except (TypeError,ValueError): page=0

    allowed={"all","live","disabled","low","code","manual"}
    if view not in allowed: view="all"
    where="COALESCE(archived,0)=0"
    params=[]
    if view=="live": where+=" AND active=1"
    elif view=="disabled": where+=" AND active=0"
    elif view=="code": where+=" AND delivery_type='code'"
    elif view=="manual": where+=" AND delivery_type='manual'"
    elif view=="low":
        where+=" AND ((delivery_type='code' AND (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=products.id AND pc.status='available')<=%s) OR (delivery_type<>'code' AND stock<=%s))"
        threshold=low_stock_threshold(); params.extend([threshold,threshold])

    per_page=12
    total_row=await adb_execute(f"SELECT COUNT(*) AS c FROM products WHERE {where}",tuple(params),"one")
    total=int((total_row or {}).get("c") or 0)
    total_pages=max(1,(total+per_page-1)//per_page); page=min(page,total_pages-1); offset=page*per_page
    rows=await adb_execute(
        f"SELECT * FROM products WHERE {where} ORDER BY featured DESC,hot DESC,best_seller DESC,merch_rank,id DESC LIMIT %s OFFSET %s",
        tuple(params+[per_page,offset]),"all") or []

    buttons=[]
    for p in rows:
        status='🟢' if p['active'] else '⚫'; badges=product_merch_badges(p)
        icon="⚡" if is_auto_code_product(p) else ("🆔" if is_uid_only_manual_product(p) else "🔐")
        name=str(p['name']); name=name[:24]+"…" if len(name)>25 else name
        buttons.append([InlineKeyboardButton(text=f"{status} {badges}{icon} {name} • {product_button_price(p)}",callback_data=f"p:{p['id']}")])
    buttons.append([
        InlineKeyboardButton(text=("✅ " if view=="all" else "")+"All",callback_data="apf:all:0"),
        InlineKeyboardButton(text=("✅ " if view=="live" else "")+"Live",callback_data="apf:live:0"),
        InlineKeyboardButton(text=("✅ " if view=="disabled" else "")+"Off",callback_data="apf:disabled:0")])
    buttons.append([
        InlineKeyboardButton(text=("✅ " if view=="low" else "")+"Low",callback_data="apf:low:0"),
        InlineKeyboardButton(text=("✅ " if view=="code" else "")+"Codes",callback_data="apf:code:0"),
        InlineKeyboardButton(text=("✅ " if view=="manual" else "")+"Manual",callback_data="apf:manual:0")])
    if total_pages>1:
        buttons.append([
            InlineKeyboardButton(text="⏮",callback_data=f"apf:{view}:0"),
            InlineKeyboardButton(text="◀️",callback_data=f"apf:{view}:{max(0,page-1)}"),
            InlineKeyboardButton(text=f"{page+1}/{total_pages}",callback_data=f"apf:{view}:{page}"),
            InlineKeyboardButton(text="▶️",callback_data=f"apf:{view}:{min(total_pages-1,page+1)}"),
            InlineKeyboardButton(text="⏭",callback_data=f"apf:{view}:{total_pages-1}")])
    buttons += [
        [InlineKeyboardButton(text="🔎 Search Products",callback_data="admin:product_search"),InlineKeyboardButton(text="🗂 Categories",callback_data="admin:categories_v3")],
        [InlineKeyboardButton(text=setting("admin_add_product","➕ Add Product"),callback_data="admin:add_product"),InlineKeyboardButton(text="📥 Bulk Import",callback_data="admin:bulk_products")],
        [InlineKeyboardButton(text="🧰 Bulk Edit",callback_data="admin:bulk_edit_products"),InlineKeyboardButton(text="🗂 Templates",callback_data="admin:product_templates")],
        [InlineKeyboardButton(text="🎮 Game Manager",callback_data="admin:games"),InlineKeyboardButton(text="🔥 Offers",callback_data="admin:offers_v2")],
        [InlineKeyboardButton(text=setting("admin_back","⬅️ Admin"),callback_data="admin:dashboard")]]
    await c.answer()
    await c.message.edit_text(
        f"💎 <b>PRODUCT MANAGER V3</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🔎 View: <b>{html.escape(view.title())}</b>\n🛍 Records: <b>{total}</b>\n📄 Page: <b>{page+1}/{total_pages}</b>\n\n"
        "🟢 Live • ⚫ Disabled • ⚡ Instant • 🆔 UID • 🔐 ID/Pass",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data=="admin:product_search")
async def admin_product_search_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await state.set_state(AdminState.admin_product_search); await c.answer()
    await c.message.answer("🔎 <b>Admin Product Search</b>\n\nSend product ID, name, game or category.\nExample: <code>eFootball 550</code>")


@router.message(AdminState.admin_product_search)
async def admin_product_search_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return await state.clear()
    q=(m.text or "").strip()
    if not q: return await m.answer("❌ Send a search term.")
    if q.isdigit():
        rows=await adb_execute("SELECT * FROM products WHERE id=%s AND COALESCE(archived,0)=0",(int(q),),"all") or []
    else:
        like=f"%{q}%"
        rows=await adb_execute("SELECT * FROM products WHERE COALESCE(archived,0)=0 AND (name ILIKE %s OR category ILIKE %s OR description ILIKE %s) ORDER BY active DESC,featured DESC,id DESC LIMIT 30",(like,like,like),"all") or []
    await state.clear()
    kb=[]
    for p in rows[:30]:
        status='🟢' if p['active'] else '⚫'; name=str(p['name']); name=name[:27]+"…" if len(name)>28 else name
        kb.append([InlineKeyboardButton(text=f"{status} #{p['id']} {name} • {product_button_price(p)}",callback_data=f"p:{p['id']}")])
    kb.append([InlineKeyboardButton(text="⬅️ Products",callback_data="admin:products")])
    await m.answer(f"🔎 <b>Search Results</b>\nQuery: <code>{html.escape(q)}</code>\nFound: <b>{len(rows)}</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data=="admin:categories_v3")
async def admin_categories_v3(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=await adb_execute("SELECT category,COUNT(*) AS total,COUNT(*) FILTER (WHERE active=1) AS live,COALESCE(SUM(stock) FILTER (WHERE delivery_type<>'code'),0) AS manual_stock FROM products WHERE COALESCE(archived,0)=0 GROUP BY category ORDER BY category",(),"all") or []
    kb=[]
    for r in rows[:45]:
        cat=str(r['category']); label=cat[:27]+"…" if len(cat)>28 else cat
        cb=f"acat:{cat}"
        if len(cb.encode('utf-8'))<=64:
            kb.append([InlineKeyboardButton(text=f"📁 {label} • {r['live']}/{r['total']}",callback_data=cb)])
    kb.append([InlineKeyboardButton(text="🎮 Game Manager",callback_data="admin:games"),InlineKeyboardButton(text="⬅️ Products",callback_data="admin:products")])
    await c.answer(); await c.message.edit_text(
        f"🗂 <b>CATEGORY MANAGER V3</b>\n\nCategories: <b>{len(rows)}</b>\nSelect a category to inspect its products.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("acat:"))
async def admin_category_v3_detail(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    cat=c.data.split(":",1)[1]
    rows=await adb_execute("SELECT * FROM products WHERE category=%s AND COALESCE(archived,0)=0 ORDER BY active DESC,id DESC LIMIT 25",(cat,),"all") or []
    kb=[]
    for p in rows:
        status='🟢' if p['active'] else '⚫'; name=str(p['name']); name=name[:25]+"…" if len(name)>26 else name
        kb.append([InlineKeyboardButton(text=f"{status} #{p['id']} {name} • {product_button_price(p)}",callback_data=f"p:{p['id']}")])
    kb.append([InlineKeyboardButton(text="⬅️ Categories",callback_data="admin:categories_v3")])
    await c.answer(); await c.message.edit_text(
        f"📁 <b>{html.escape(cat)}</b>\nProducts: <b>{len(rows)}</b>"+("\n\nShowing first 25." if len(rows)>=25 else ""),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


def parse_bulk_product_edits(payload):
    """
    One product per line:
      PRODUCT_ID | price=150 | stock=20 | active=on | category=PUBG Mobile > UC

    Allowed keys: price, stock, active, category.
    Stock cannot be manually changed for code-delivery products.
    """
    edits = []
    errors = []
    seen = set()

    for lineno, raw in enumerate((payload or "").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [x.strip() for x in line.split("|") if x.strip()]
        if len(parts) < 2:
            errors.append(f"Line {lineno}: expected Product ID plus at least one key=value edit")
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            errors.append(f"Line {lineno}: invalid Product ID")
            continue
        if pid <= 0 or pid in seen:
            errors.append(f"Line {lineno}: invalid or duplicate Product ID")
            continue
        seen.add(pid)

        changes = {}
        for token in parts[1:]:
            if "=" not in token:
                errors.append(f"Line {lineno}: '{token}' must be key=value")
                continue
            key, value = [x.strip() for x in token.split("=", 1)]
            key = key.lower()
            if key not in {"price","stock","active","category"}:
                errors.append(f"Line {lineno}: unsupported field '{key}'")
                continue
            if key == "price":
                try:
                    value = float(value)
                    if value <= 0:
                        raise ValueError
                except ValueError:
                    errors.append(f"Line {lineno}: price must be > 0")
                    continue
            elif key == "stock":
                try:
                    value = int(value)
                    if value < 0:
                        raise ValueError
                except ValueError:
                    errors.append(f"Line {lineno}: stock must be 0 or higher")
                    continue
            elif key == "active":
                val = value.casefold()
                if val in {"1","on","yes","true","active"}:
                    value = 1
                elif val in {"0","off","no","false","inactive"}:
                    value = 0
                else:
                    errors.append(f"Line {lineno}: active must be on/off")
                    continue
            elif key == "category":
                if not value or len(value.encode("utf-8")) > 48:
                    errors.append(f"Line {lineno}: category must be 1–48 UTF-8 bytes")
                    continue
            changes[key] = value

        if changes:
            edits.append({"line":lineno,"id":pid,"changes":changes})

    if len(edits) > 200:
        errors.append("Maximum 200 products per bulk edit")
    if not edits and not errors:
        errors.append("No edit rows found")
    return edits, errors


def apply_bulk_product_edits(edits):
    """Atomic product edits. Any invalid target aborts the whole transaction."""
    updated = []
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                ids = [e["id"] for e in edits]
                cur.execute(
                    "SELECT id,delivery_type FROM products WHERE id = ANY(%s) FOR UPDATE",
                    (ids,),
                )
                rows = cur.fetchall() or []
                by_id = {int(r["id"]): r for r in rows}
                missing = [pid for pid in ids if pid not in by_id]
                if missing:
                    raise ValueError("Product ID(s) not found: " + ", ".join(map(str, missing)))

                for edit in edits:
                    pid = edit["id"]
                    changes = dict(edit["changes"])
                    if "stock" in changes and by_id[pid]["delivery_type"] == "code":
                        raise ValueError(
                            f"Product #{pid} uses code delivery; upload/remove codes instead of editing stock."
                        )

                for edit in edits:
                    pid = edit["id"]
                    changes = edit["changes"]
                    fields = []
                    params = []
                    for key in ("price","stock","active","category"):
                        if key in changes:
                            fields.append(f"{key}=%s")
                            params.append(changes[key])
                    fields.append("updated_at=NOW()")
                    params.append(pid)
                    cur.execute(
                        f"UPDATE products SET {', '.join(fields)} WHERE id=%s RETURNING id",
                        tuple(params),
                    )
                    updated.append(int(cur.fetchone()["id"]))
    return updated



@router.callback_query(F.data=="admin:bulk_products_confirm")
async def admin_bulk_products_confirm(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    d=await state.get_data()
    products=d.get("bulk_products_preview") or []
    if not products:
        return await c.answer("Bulk preview expired. Start import again.",show_alert=True)
    try:
        created,skipped=await asyncio.to_thread(bulk_insert_products,products)
    except Exception as exc:
        error_id=record_runtime_error("bulk_product_import",exc,{"admin_id":c.from_user.id,"rows":len(products)})
        return await c.answer(f"Import failed safely. Ref {error_id}",show_alert=True)
    await asyncio.to_thread(admin_log,c.from_user.id,"bulk_product_import",f"requested={len(products)} created={len(created)} skipped={len(skipped)}")
    await state.clear()
    await c.answer("Import complete.")
    await c.message.edit_text(
        f"✅ <b>Bulk import complete</b>\n\n📥 Parsed: <b>{len(products)}</b>\n✅ Created: <b>{len(created)}</b>\n⏭ Skipped existing: <b>{len(skipped)}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍 Products",callback_data="admin:products")],
            [InlineKeyboardButton(text="🎮 Game Manager",callback_data="admin:games")]
        ]))


@router.callback_query(F.data=="admin:bulk_products_cancel")
async def admin_bulk_products_cancel(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await state.clear()
    await c.answer("Cancelled.")
    await c.message.edit_text("❌ Bulk import cancelled. Nothing was created.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="⬅️ Products",callback_data="admin:products")
        ]]))

@router.callback_query(F.data=="admin:bulk_edit_products")
async def admin_bulk_edit_products_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await state.set_state(AdminState.bulk_edit_products)
    await c.answer()
    await c.message.answer(
        "🧰 <b>Bulk Product Manager V2</b>\n\n"
        "One product per line:\n"
        "<code>123 | price=150 | stock=20 | active=on | category=PUBG Mobile &gt; UC</code>\n"
        "<code>124 | price=300 | active=off</code>\n\n"
        "Allowed: <b>price, stock, active, category</b>.\n"
        "Use <code>active=off</code> / <code>active=on</code> for bulk disable/enable.\n"
        "⚠️ Code-product stock cannot be edited here.\n"
        "✅ Preview before commit.\n✅ Final update is atomic."
    )


@router.message(AdminState.bulk_edit_products)
async def admin_bulk_edit_products_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return
    edits,errors=parse_bulk_product_edits(m.text or "")
    if errors:
        preview="\n".join(f"• {html.escape(x)}" for x in errors[:12])
        extra=f"\n• ...and {len(errors)-12} more" if len(errors)>12 else ""
        return await m.answer(f"❌ <b>Bulk edit validation failed</b>\n\n{preview}{extra}\n\nNothing was changed.")
    ids=[e["id"] for e in edits]
    rows=await adb_execute("SELECT id,name,delivery_type FROM products WHERE id = ANY(%s)",(ids,),"all") or []
    by_id={int(r["id"]):r for r in rows}
    missing=[pid for pid in ids if pid not in by_id]
    if missing:
        return await m.answer("❌ Missing Product ID(s): <code>"+html.escape(",".join(map(str,missing)))+"</code>\nNothing was changed.")
    for e in edits:
        if "stock" in e["changes"] and by_id[e["id"]]["delivery_type"]=="code":
            return await m.answer(f"❌ Product #{e['id']} uses code delivery; code stock cannot be manually edited.\nNothing was changed.")
    await state.update_data(bulk_edits_preview=edits)
    preview=[]
    for e in edits[:15]:
        changes=", ".join(f"{k}={v}" for k,v in e["changes"].items())
        preview.append(f"• #{e['id']} {html.escape(by_id[e['id']]['name'][:22])} → <code>{html.escape(changes)}</code>")
    extra=f"\n• ...and {len(edits)-15} more" if len(edits)>15 else ""
    await m.answer(
        f"👀 <b>Bulk Edit Preview</b>\n\nRows: <b>{len(edits)}</b>\n\n"
        + "\n".join(preview)+extra+
        "\n\nNothing has been changed yet.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Confirm Changes",callback_data="admin:bulk_edit_confirm")],
            [InlineKeyboardButton(text="❌ Cancel",callback_data="admin:bulk_edit_cancel")]
        ])
    )


def parse_bulk_products(payload):
    """
    Supported grouped formats:
      GAME: eFootball
      80 Coins | 80 | 120 | manual | 999 | eFootball 80 Coins

      CATEGORY: PUBG Mobile > UC
      60 UC | 60 | 150 | manual | 999 | Description

    Or full legacy line:
      60 UC | PUBG Mobile > UC | 60 | 150 | manual | 999 | Description
    """
    current_category = None
    parsed = []
    errors = []
    seen = set()

    for lineno, raw in enumerate((payload or "").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        upper = line.upper()
        if upper.startswith("CATEGORY:") or upper.startswith("GAME:"):
            current_category = line.split(":", 1)[1].strip()
            if not current_category:
                errors.append(f"Line {lineno}: game/category is empty")
                current_category = None
            elif len(current_category.encode("utf-8")) > 48:
                errors.append(f"Line {lineno}: game/category exceeds 48 UTF-8 bytes")
            continue

        parts = [x.strip() for x in line.split("|", 6)]
        if len(parts) == 6 and current_category:
            name, quantity, price, delivery, stock, description = parts
            category = current_category
        elif len(parts) == 7:
            name, category, quantity, price, delivery, stock, description = parts
        else:
            errors.append(
                f"Line {lineno}: expected 6 fields under CATEGORY or 7 fields in full format"
            )
            continue

        if not name or not category:
            errors.append(f"Line {lineno}: name/category cannot be empty")
            continue
        if len(category.encode("utf-8")) > 48:
            errors.append(f"Line {lineno}: category exceeds 48 UTF-8 bytes")
            continue

        try:
            quantity_i = int(quantity)
            price_f = float(price)
            stock_i = int(stock)
        except ValueError:
            errors.append(f"Line {lineno}: quantity, price and stock must be numbers")
            continue

        delivery = delivery.lower()
        if delivery not in {"code", "manual"}:
            errors.append(f"Line {lineno}: delivery must be code or manual")
            continue
        if price_f <= 0 or quantity_i < 0 or stock_i < 0:
            errors.append(f"Line {lineno}: invalid quantity/price/stock")
            continue

        # Code products get stock from product_codes, never from a typed stock number.
        if delivery == "code":
            stock_i = 0

        key = (name.casefold(), category.casefold())
        if key in seen:
            errors.append(f"Line {lineno}: duplicate Name + Category inside this import")
            continue
        seen.add(key)

        parsed.append({
            "line": lineno,
            "name": name,
            "category": category,
            "quantity": quantity_i,
            "price": price_f,
            "delivery": delivery,
            "stock": stock_i,
            "description": description,
        })

    if len(parsed) > 200:
        errors.append("Maximum 200 products per import")
    if not parsed and not errors:
        errors.append("No product rows found")
    return parsed, errors


def bulk_insert_products(products):
    """Atomic create-only import. Existing live same Name+Category rows are skipped; archived rows do not block re-import."""
    created = []
    skipped = []
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                for item in products:
                    cur.execute(
                        """SELECT id FROM products
                           WHERE COALESCE(archived,0)=0
                             AND lower(name)=lower(%s) AND lower(category)=lower(%s)
                           ORDER BY id LIMIT 1""",
                        (item["name"], item["category"]),
                    )
                    existing = cur.fetchone()
                    if existing:
                        skipped.append((item["name"], item["category"], int(existing["id"])))
                        continue
                    cur.execute(
                        """INSERT INTO products
                           (name,category,quantity,price,delivery_type,stock,description,archived)
                           VALUES(%s,%s,%s,%s,%s,%s,%s,0)
                           RETURNING id""",
                        (
                            item["name"], item["category"], item["quantity"], item["price"],
                            item["delivery"], item["stock"], item["description"],
                        ),
                    )
                    created.append(int(cur.fetchone()["id"]))
    return created, skipped


@router.callback_query(F.data=="admin:bulk_products")
async def admin_bulk_products_start(c:CallbackQuery, state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    await state.set_state(AdminState.bulk_products)
    await c.answer()
    await c.message.answer(
        "📥 <b>Bulk Product Import</b>\n\n"
        "📎 Upload a <b>.txt</b> file OR paste products in one message.\n"
        "Maximum: 200 parsed rows; TXT file maximum 512 KB.\n\n"
        "🎮 <b>Recommended GAME format:</b>\n"
        "<code>GAME: eFootball\n"
        "80 Coins | 80 | 120 | manual | 999 | eFootball 80 Coins\n"
        "130 Coins | 130 | 180 | manual | 999 | eFootball 130 Coins\n"
        "550 Coins | 550 | 650 | manual | 999 | eFootball 550 Coins\n\n"
        "GAME: PUBG Mobile\n"
        "60 UC | 60 | 150 | manual | 999 | PUBG 60 UC\n"
        "325 UC | 325 | 700 | manual | 999 | PUBG 325 UC</code>\n\n"
        "Buyer flow: <b>Game → package list → 🛒 Buy</b>.\n\n"
        "📂 <b>Advanced category format still works:</b>\n"
        "<code>CATEGORY: PUBG Mobile &gt; UC\n"
        "60 UC | 60 | 150 | manual | 999 | PUBG UC</code>\n\n"
        "<b>Full format also works:</b>\n"
        "<code>Name | Category | Quantity | Price | Delivery | Stock | Description</code>\n\n"
        "✅ Import is atomic: invalid input creates nothing.\n"
        "✅ Existing same Name + Category is skipped.\n"
        "ℹ️ For <code>code</code> delivery, stock is controlled by uploaded codes."
    )


async def _show_bulk_products_preview(m:Message,state:FSMContext,payload:str):
    products,errors=parse_bulk_products(payload or "")
    if errors:
        preview="\n".join(f"• {html.escape(x)}" for x in errors[:12])
        extra=f"\n• ...and {len(errors)-12} more" if len(errors)>12 else ""
        return await m.answer(
            f"❌ <b>Bulk import validation failed</b>\n\n{preview}{extra}\n\nNothing was created."
        )
    await state.update_data(bulk_products_preview=products)
    preview_rows=[
        f"• {html.escape(p['category'])} → {html.escape(p['name'])} • {fmt_money(p['price'])} • {html.escape(p['delivery'])}"
        for p in products[:12]
    ]
    extra=f"\n• ...and {len(products)-12} more" if len(products)>12 else ""
    await m.answer(
        f"👀 <b>Bulk Import Preview</b>\n\nParsed: <b>{len(products)}</b>\n\n"
        + "\n".join(preview_rows)+extra+
        "\n\nNothing has been created yet.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Confirm Import",callback_data="admin:bulk_products_confirm")],
            [InlineKeyboardButton(text="❌ Cancel",callback_data="admin:bulk_products_cancel")]
        ])
    )


@router.message(AdminState.bulk_products, F.document)
async def admin_bulk_products_file_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return
    doc=m.document
    filename=(doc.file_name or "").strip()
    if not filename.lower().endswith(".txt"):
        return await m.answer("❌ Please upload a .txt file only. Nothing was created.")
    if int(doc.file_size or 0) > 512*1024:
        return await m.answer("❌ TXT file is too large. Maximum size is 512 KB. Nothing was created.")
    try:
        tg_file=await m.bot.get_file(doc.file_id)
        buf=io.BytesIO()
        await m.bot.download_file(tg_file.file_path,destination=buf)
        raw=buf.getvalue()
        if raw.startswith(b"\xef\xbb\xbf"):
            raw=raw[3:]
        payload=raw.decode("utf-8")
    except UnicodeDecodeError:
        return await m.answer("❌ TXT must be UTF-8 encoded. Nothing was created.")
    except Exception as exc:
        error_id=record_runtime_error("bulk_product_txt_upload",exc,{"admin_id":m.from_user.id,"file_name":filename})
        return await m.answer(f"❌ Could not read the TXT file safely. Ref <code>{error_id}</code>")
    if not payload.strip():
        return await m.answer("❌ TXT file is empty. Nothing was created.")
    await _show_bulk_products_preview(m,state,payload)


@router.message(AdminState.bulk_products)
async def admin_bulk_products_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return
    if not (m.text or "").strip():
        return await m.answer("📎 Upload a .txt file or paste the bulk product text here.")
    return await _show_bulk_products_preview(m,state,m.text or "")


def save_product_template(template_name, source_category, admin_id):
    template_name = (template_name or "").strip()
    source_category = (source_category or "").strip()
    if not template_name or len(template_name) > 60:
        raise ValueError("Template name must be 1–60 characters.")
    if not source_category or len(source_category.encode("utf-8")) > 48:
        raise ValueError("Source category is invalid.")

    rows = db_execute(
        """SELECT name,quantity,price,delivery_type,stock,description,active
           FROM products WHERE category=%s ORDER BY id""",
        (source_category,),
        "all",
    ) or []
    if not rows:
        raise ValueError("No products found in that category.")

    items = []
    for r in rows:
        items.append({
            "name": r["name"],
            "quantity": int(r["quantity"] or 0),
            "price": float(r["price"]),
            "delivery_type": r["delivery_type"],
            "stock": 0 if r["delivery_type"]=="code" else int(r["stock"] or 0),
            "description": r["description"] or "",
            "active": int(r["active"] or 0),
        })

    db_execute(
        """INSERT INTO product_templates(name,source_category,items_json,created_by)
           VALUES(%s,%s,%s,%s)
           ON CONFLICT(name) DO UPDATE SET
             source_category=EXCLUDED.source_category,
             items_json=EXCLUDED.items_json,
             created_by=EXCLUDED.created_by,
             created_at=NOW()""",
        (template_name,source_category,json.dumps(items,ensure_ascii=False),admin_id),
    )
    return len(items)


def apply_product_template(template_id, target_category):
    target_category = (target_category or "").strip()
    if not target_category or len(target_category.encode("utf-8")) > 48:
        raise ValueError("Target category must be 1–48 UTF-8 bytes.")

    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM product_templates WHERE id=%s FOR UPDATE",
                    (template_id,),
                )
                template = cur.fetchone()
                if not template:
                    raise ValueError("Template not found.")
                try:
                    items = json.loads(template["items_json"])
                except Exception as exc:
                    raise ValueError("Template data is invalid.") from exc
                if not isinstance(items,list) or not items:
                    raise ValueError("Template contains no products.")

                created = []
                skipped = []
                for item in items:
                    name = str(item.get("name") or "").strip()
                    if not name:
                        raise ValueError("Template contains an invalid product name.")
                    cur.execute(
                        """SELECT id FROM products
                           WHERE lower(name)=lower(%s) AND lower(category)=lower(%s)
                           ORDER BY id LIMIT 1""",
                        (name,target_category),
                    )
                    existing = cur.fetchone()
                    if existing:
                        skipped.append(int(existing["id"]))
                        continue

                    delivery = str(item.get("delivery_type") or "manual").lower()
                    if delivery not in {"manual","code"}:
                        delivery = "manual"
                    stock = 0 if delivery=="code" else max(0,int(item.get("stock") or 0))
                    price = float(item.get("price") or 0)
                    if price <= 0:
                        raise ValueError(f"Template product '{name}' has invalid price.")

                    cur.execute(
                        """INSERT INTO products
                           (name,category,quantity,price,delivery_type,stock,description,active)
                           VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                           RETURNING id""",
                        (
                            name,target_category,max(0,int(item.get("quantity") or 0)),
                            price,delivery,stock,str(item.get("description") or ""),
                            1 if int(item.get("active") or 0) else 0,
                        ),
                    )
                    created.append(int(cur.fetchone()["id"]))
    return created, skipped



@router.callback_query(F.data=="admin:bulk_edit_confirm")
async def admin_bulk_edit_confirm(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    d=await state.get_data()
    edits=d.get("bulk_edits_preview") or []
    if not edits:
        return await c.answer("Bulk edit preview expired. Start again.",show_alert=True)
    try:
        updated=await asyncio.to_thread(apply_bulk_product_edits,edits)
    except Exception as exc:
        error_id=record_runtime_error("bulk_product_edit",exc,{"admin_id":c.from_user.id,"rows":len(edits)})
        return await c.answer(f"Bulk edit failed safely. Ref {error_id}",show_alert=True)
    await asyncio.to_thread(admin_log,c.from_user.id,"bulk_product_edit",f"updated={len(updated)}")
    await state.clear()
    await c.answer("Changes applied.")
    await c.message.edit_text(
        f"✅ <b>Bulk edit complete</b>\nUpdated: <b>{len(updated)}</b> product(s).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🛍 Products",callback_data="admin:products")
        ]]))


@router.callback_query(F.data=="admin:bulk_edit_cancel")
async def admin_bulk_edit_cancel(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await state.clear()
    await c.answer("Cancelled.")
    await c.message.edit_text("❌ Bulk edit cancelled. Nothing was changed.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="⬅️ Products",callback_data="admin:products")
        ]]))

@router.callback_query(F.data=="admin:product_templates")
async def admin_product_templates(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    rows = await asyncio.to_thread(
        db_execute,
        "SELECT id,name,source_category,created_at FROM product_templates ORDER BY id DESC LIMIT 20",
        (),
        "all",
    ) or []
    buttons = [[
        InlineKeyboardButton(
            text=f"🗂 {r['name'][:22]} • {r['source_category'][:18]}",
            callback_data=f"admin:template_apply:{r['id']}",
        ),
        InlineKeyboardButton(text="🗑",callback_data=f"admin:template_delete:{r['id']}"),
    ] for r in rows]
    buttons += [
        [InlineKeyboardButton(text="➕ Save Category as Template",callback_data="admin:template_create")],
        [InlineKeyboardButton(text="⬅️ Products",callback_data="admin:products")],
    ]
    await c.answer()
    await c.message.edit_text(
        "🗂 <b>Game / Product Templates</b>\n\n"
        "Save an existing category as a reusable template. Tap a template to apply it to a target category.\n"
        "Existing same Name + Category products are skipped.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data=="admin:template_create")
async def admin_template_create_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await state.set_state(AdminState.template_create)
    await c.answer()
    await c.message.answer(
        "➕ <b>Save Category as Template</b>\n\n"
        "Send:\n<code>Template Name | Existing Category</code>\n\n"
        "Example:\n<code>PUBG UC Standard | PUBG Mobile &gt; UC</code>"
    )


@router.message(AdminState.template_create)
async def admin_template_create_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return
    parts = [x.strip() for x in (m.text or "").split("|",1)]
    if len(parts)!=2:
        return await m.answer("❌ Use: <code>Template Name | Existing Category</code>")
    try:
        count = await asyncio.to_thread(save_product_template,parts[0],parts[1],m.from_user.id)
    except Exception as exc:
        return await m.answer(f"❌ Could not save template: {html.escape(str(exc)[:250])}")
    await asyncio.to_thread(
        admin_log,m.from_user.id,"product_template_saved",f"name={parts[0]} items={count}"
    )
    await state.clear()
    await m.answer(f"✅ Template saved with <b>{count}</b> product(s).")


@router.callback_query(F.data.startswith("admin:template_apply:"))
async def admin_template_apply_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    tid = int(c.data.rsplit(":",1)[1])
    row = await asyncio.to_thread(
        db_execute,"SELECT id,name,source_category FROM product_templates WHERE id=%s",(tid,),"one"
    )
    if not row:
        return await c.answer("Template not found.",show_alert=True)
    await state.update_data(template_id=tid,template_source=row["source_category"])
    await state.set_state(AdminState.template_target)
    await c.answer()
    await c.message.answer(
        f"🗂 <b>{html.escape(row['name'])}</b>\n\n"
        f"Source: <code>{html.escape(row['source_category'])}</code>\n"
        "Send the <b>target category</b> where these products should be created.\n"
        "Send <code>/same</code> to use the original category."
    )


@router.message(AdminState.template_target)
async def admin_template_target_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return
    d = await state.get_data()
    target = (m.text or "").strip()
    if target.casefold()=="/same":
        target = d.get("template_source") or ""
    try:
        created, skipped = await asyncio.to_thread(
            apply_product_template,d["template_id"],target
        )
    except Exception as exc:
        return await m.answer(f"❌ Template apply failed safely: {html.escape(str(exc)[:250])}")
    await asyncio.to_thread(
        admin_log,m.from_user.id,"product_template_applied",
        f"template={d['template_id']} target={target} created={len(created)} skipped={len(skipped)}"
    )
    await state.clear()
    await m.answer(
        f"✅ <b>Template applied</b>\n"
        f"Created: <b>{len(created)}</b>\n"
        f"Skipped existing: <b>{len(skipped)}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🛍 Products",callback_data="admin:products")
        ]])
    )


@router.callback_query(F.data.startswith("admin:template_delete:"))
async def admin_template_delete(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    tid = int(c.data.rsplit(":",1)[1])
    row = await asyncio.to_thread(
        db_execute,
        "DELETE FROM product_templates WHERE id=%s RETURNING name",
        (tid,),
        "one",
    )
    if not row:
        return await c.answer("Template not found.",show_alert=True)
    await asyncio.to_thread(
        admin_log,c.from_user.id,"product_template_deleted",f"template={tid} name={row['name']}"
    )
    await c.answer("Template deleted.")
    c.data = "admin:product_templates"
    return await admin_product_templates(c)



def set_product_merch_flag(product_id,flag):
    if flag not in {"featured","hot","best_seller"}:
        raise ValueError("Invalid merchandising flag.")
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {flag} AS v FROM products WHERE id=%s FOR UPDATE",(product_id,))
                row=cur.fetchone()
                if not row:
                    raise ValueError("Product not found.")
                new=0 if int(row["v"] or 0) else 1
                cur.execute(f"UPDATE products SET {flag}=%s,updated_at=NOW() WHERE id=%s",(new,product_id))
    return new


@router.callback_query(F.data.startswith("admin:merch:"))
async def admin_product_merch_toggle(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    _,_,flag,pid_s=c.data.split(":",3)
    try:
        new=await asyncio.to_thread(set_product_merch_flag,int(pid_s),flag)
    except Exception as exc:
        return await c.answer(str(exc)[:180],show_alert=True)
    await asyncio.to_thread(admin_log,c.from_user.id,"product_merch_toggle",f"product={pid_s} {flag}={new}")
    await c.answer("Enabled" if new else "Disabled")
    c.data=f"p:{pid_s}"
    return await product_manage(c)


@router.callback_query(F.data=="admin:offers_v2")
async def admin_offers_v2(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    rows=await adb_execute(
        """SELECT id,name,category,price,sale_price,sale_until
           FROM products WHERE sale_price IS NOT NULL AND sale_until>NOW()
           ORDER BY sale_until,id DESC LIMIT 25""",(),"all") or []
    body="\n".join(
        f"• #{r['id']} {html.escape(r['name'][:22])} • {fmt_money(r['sale_price'])} <s>{fmt_money(r['price'])}</s> • until {html.escape(str(r['sale_until'])[:16])}"
        for r in rows
    ) if rows else "No active offers."
    await c.answer()
    await c.message.edit_text(
        "🔥 <b>Offer Manager V2</b>\n\n"+body+
        "\n\nProduct fixed-price and game-wide percentage offers use the same checkout price.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Create / Clear Offer",callback_data="admin:offer_create")],
            [InlineKeyboardButton(text="⬅️ Products",callback_data="admin:products")]
        ]))


@router.callback_query(F.data=="admin:offer_create")
async def admin_offer_create_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await state.set_state(AdminState.offer_create)
    await c.answer()
    await c.message.answer(
        "🔥 <b>Create Offer V2</b>\n\n"
        "Product:\n<code>PRODUCT | 123 | 120 | 24h</code>\n\n"
        "Game-wide:\n<code>GAME | eFootball | 10% | 2d</code>\n\n"
        "Clear:\n<code>CLEAR PRODUCT | 123</code>\n"
        "<code>CLEAR GAME | eFootball</code>\n\n"
        "Duration supports hours/days, maximum 30 days."
    )


@router.message(AdminState.offer_create)
async def admin_offer_create_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return await state.clear()
    parts=[x.strip() for x in (m.text or "").split("|")]
    try:
        if len(parts)==4 and parts[0].casefold()=="product":
            pid=int(parts[1]); sale=float(parts[2]); seconds=offer_seconds(parts[3])
            await asyncio.to_thread(apply_product_offer,pid,sale,seconds)
            action=f"product={pid} sale={sale} duration={parts[3]}"
            result=f"✅ Product #{pid} offer activated at <b>{fmt_money(sale)}</b>."
        elif len(parts)==4 and parts[0].casefold()=="game":
            game=parts[1]; discount=float(parts[2].rstrip("%")); seconds=offer_seconds(parts[3])
            count=await asyncio.to_thread(apply_game_offer,game,discount,seconds)
            action=f"game={game} discount={discount}% duration={parts[3]} products={count}"
            result=f"✅ <b>{count}</b> product(s) under {html.escape(game)} now have <b>{discount:g}% off</b>."
        elif len(parts)==2 and parts[0].casefold()=="clear product":
            count=await asyncio.to_thread(clear_offer_scope,"product",int(parts[1]))
            action=f"clear product={parts[1]} count={count}"
            result=f"✅ Cleared offer from <b>{count}</b> product."
        elif len(parts)==2 and parts[0].casefold()=="clear game":
            count=await asyncio.to_thread(clear_offer_scope,"game",parts[1])
            action=f"clear game={parts[1]} count={count}"
            result=f"✅ Cleared offers from <b>{count}</b> product(s)."
        else:
            raise ValueError("Use PRODUCT / GAME / CLEAR format.")
    except Exception as exc:
        return await m.answer(f"❌ Offer not changed: {html.escape(str(exc)[:250])}")
    await asyncio.to_thread(admin_log,m.from_user.id,"offer_v2",action)
    await state.clear()
    await m.answer(result,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔥 Offer Manager",callback_data="admin:offers_v2")
    ]]))

@router.callback_query(F.data.startswith("p:"))
async def product_manage(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1])
    p=await adb_execute("SELECT * FROM products WHERE id=%s AND COALESCE(archived,0)=0",(pid,),"one")
    if not p:
        return await c.answer("Product not found or already removed.",show_alert=True)
    badges=product_merch_badges(p) or "—"
    flow="Instant Code" if is_auto_code_product(p) else ("UID Only" if is_uid_only_manual_product(p) else "ID + Password")
    body=(
        f"💎 <b>ELITE PRODUCT CONTROL</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🎮 <b>{html.escape(p['name'])}</b>\n"
        f"🏷 {html.escape(p['category'])}\n\n"
        f"💰 Price: {product_price_display(p)}\n"
        f"📦 Stock: <b>{effective_stock(p)}</b>\n"
        f"🚀 Flow: <b>{html.escape(flow)}</b>\n"
        f"🔘 Status: <b>{'LIVE' if p['active'] else 'DISABLED'}</b>\n"
        f"🏅 Merch: {badges}\n\n"
        f"📝 {html.escape(p['description'] or 'No description.')}"
    )
    kb=[
        [InlineKeyboardButton(text=setting("admin_edit","✏️ Edit"),callback_data=f"pedit:{pid}"),
         InlineKeyboardButton(text=setting("admin_toggle_product","🔄 Enable/Disable"),callback_data=f"ptoggle:{pid}")],
        [InlineKeyboardButton(text="💰 Quick Price",callback_data=f"pqp:{pid}"),
         InlineKeyboardButton(text="📦 Quick Stock",callback_data=f"pqs:{pid}")],
        [InlineKeyboardButton(text=f"{'✅' if p.get('featured') else '⬜'} ⭐ Featured",callback_data=f"admin:merch:featured:{pid}"),
         InlineKeyboardButton(text=f"{'✅' if p.get('hot') else '⬜'} 🔥 Hot",callback_data=f"admin:merch:hot:{pid}")],
        [InlineKeyboardButton(text=f"{'✅' if p.get('best_seller') else '⬜'} 🏆 Best Seller",callback_data=f"admin:merch:best_seller:{pid}")],
        [InlineKeyboardButton(text="🔥 Offer Manager V2",callback_data="admin:offers_v2")],
        [InlineKeyboardButton(text=setting("admin_add_codes","🎫 Add Codes"),callback_data=f"codes_add:{pid}")],
        [InlineKeyboardButton(text="🗑 Remove Product",callback_data=f"pdelete:{pid}")],
        [InlineKeyboardButton(text=setting("admin_products_back","⬅️ Products"),callback_data="admin:products")]
    ]
    await c.answer()
    await c.message.edit_text(body,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("pqp:"))
async def admin_product_quick_price_start(c:CallbackQuery,state:FSMContext):
    if not admin_can(c.from_user.id,"mutate"): return await c.answer("Read-only admin cannot change products.",show_alert=True)
    pid=int(c.data.split(":",1)[1]); p=await adb_execute("SELECT id,name,price FROM products WHERE id=%s AND COALESCE(archived,0)=0",(pid,),"one")
    if not p: return await c.answer("Product not found.",show_alert=True)
    await state.update_data(quick_product_id=pid); await state.set_state(AdminState.admin_product_quick_price); await c.answer()
    await c.message.answer(f"💰 <b>Quick Price</b>\n{html.escape(p['name'])}\nCurrent: <b>{fmt_money(p['price'])}</b>\n\nSend the new price only.")


@router.message(AdminState.admin_product_quick_price)
async def admin_product_quick_price_receive(m:Message,state:FSMContext):
    if not admin_can(m.from_user.id,"mutate"): await state.clear(); return await m.answer("❌ Read-only admin cannot change products.")
    d=await state.get_data()
    try: price=float((m.text or "").strip()); assert price>0
    except Exception: return await m.answer("❌ Send a valid price greater than 0.")
    pid=int(d['quick_product_id']); row=await adb_execute("UPDATE products SET price=%s,updated_at=NOW() WHERE id=%s AND COALESCE(archived,0)=0 RETURNING name",(price,pid),"one")
    if not row: await state.clear(); return await m.answer("❌ Product not found.")
    await aadmin_log(m.from_user.id,"quick_product_price",f"product #{pid} price={price}"); await state.clear()
    await m.answer(f"✅ #{pid} <b>{html.escape(row['name'])}</b> price updated to <b>{fmt_money(price)}</b>.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Product",callback_data=f"p:{pid}")]]))


@router.callback_query(F.data.startswith("pqs:"))
async def admin_product_quick_stock_start(c:CallbackQuery,state:FSMContext):
    if not admin_can(c.from_user.id,"mutate"): return await c.answer("Read-only admin cannot change products.",show_alert=True)
    pid=int(c.data.split(":",1)[1]); p=await adb_execute("SELECT id,name,delivery_type,stock FROM products WHERE id=%s AND COALESCE(archived,0)=0",(pid,),"one")
    if not p: return await c.answer("Product not found.",show_alert=True)
    if p['delivery_type']=='code': return await c.answer("Code-product stock is controlled by code inventory. Add/remove codes instead.",show_alert=True)
    await state.update_data(quick_product_id=pid); await state.set_state(AdminState.admin_product_quick_stock); await c.answer()
    await c.message.answer(f"📦 <b>Quick Stock</b>\n{html.escape(p['name'])}\nCurrent: <b>{int(p['stock'] or 0)}</b>\n\nSend the new stock quantity only.")


@router.message(AdminState.admin_product_quick_stock)
async def admin_product_quick_stock_receive(m:Message,state:FSMContext):
    if not admin_can(m.from_user.id,"mutate"): await state.clear(); return await m.answer("❌ Read-only admin cannot change products.")
    d=await state.get_data()
    try: stock=int((m.text or "").strip()); assert stock>=0
    except Exception: return await m.answer("❌ Send a whole number 0 or higher.")
    pid=int(d['quick_product_id']); row=await adb_execute("UPDATE products SET stock=%s,updated_at=NOW() WHERE id=%s AND delivery_type<>'code' AND COALESCE(archived,0)=0 RETURNING name",(stock,pid),"one")
    if not row: await state.clear(); return await m.answer("❌ Product not found or stock is code-managed.")
    await aadmin_log(m.from_user.id,"quick_product_stock",f"product #{pid} stock={stock}"); await state.clear()
    await m.answer(f"✅ #{pid} <b>{html.escape(row['name'])}</b> stock updated to <b>{stock}</b>.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Product",callback_data=f"p:{pid}")]]))


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
    row=await adb_insert_returning("INSERT INTO products(name,category,quantity,price,delivery_type,stock,description) VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id",(name,category or "Gaming",quantity,price,delivery,stock,description)); pid=row["id"]
    await aadmin_log(m.from_user.id,"add_product",f"product #{pid}"); await state.clear(); await m.answer(f"✅ Product #{pid} created.",reply_markup=admin_menu())

@router.callback_query(F.data.startswith("pedit:"))
async def product_edit_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1])
    p=await adb_execute("SELECT * FROM products WHERE id=%s",(pid,),"one")
    if not p: return await c.answer("Not found",show_alert=True)
    await state.update_data(pid=pid)
    await state.set_state(AdminState.edit_product)
    await c.answer()
    await c.message.answer(
        "✏️ <b>Edit Product</b>\n\n"
        "Send exactly: <code>Name | Category | Quantity | Price | Delivery | Stock | Description</code>\n\n"
        f"Current: <code>{html.escape(p['name'])} | {html.escape(p['category'])} | {p['quantity']} | {p['price']} | {p['delivery_type']} | {effective_stock(p)} | {html.escape(p['description'] or '')}</code>"
    )

def _product_edit_tx(pid,name,category,quantity,price,delivery,stock,description):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM products WHERE id=%s FOR UPDATE",(pid,)); current=cur.fetchone()
                if not current: return {"error":"not_found"}
                cur.execute("SELECT COUNT(*) AS c FROM product_codes WHERE product_id=%s AND status IN ('available','reserved')",(pid,)); code_inventory=int(cur.fetchone()["c"])
                cur.execute("SELECT COUNT(*) AS c FROM orders WHERE product_id=%s AND status IN ('awaiting_payment','pending')",(pid,)); active_orders=int(cur.fetchone()["c"])
                if delivery != current["delivery_type"] and active_orders: return {"error":"active_orders"}
                if delivery=="manual" and code_inventory: return {"error":"code_inventory"}
                if delivery=="code":
                    cur.execute("SELECT COUNT(*) AS c FROM product_codes WHERE product_id=%s AND status='available'",(pid,)); stock=int(cur.fetchone()["c"])
                cur.execute("UPDATE products SET name=%s,category=%s,quantity=%s,price=%s,delivery_type=%s,stock=%s,description=%s,updated_at=NOW() WHERE id=%s",(name,category,quantity,price,delivery,stock,description,pid))
                return {"ok":True}


@router.message(AdminState.edit_product)
async def product_edit_save(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return
    d=await state.get_data(); pid=d.get("pid")
    parts=[x.strip() for x in (m.text or "").split("|",6)]
    if len(parts)!=7: return await m.answer("❌ Invalid format. Use 7 fields separated by |.")
    name,category,quantity,price,delivery,stock,description=parts
    if not name or not category: return await m.answer("❌ Name and category cannot be empty.")
    try: quantity=int(quantity); price=float(price); stock=int(stock)
    except ValueError: return await m.answer("❌ Quantity, price and stock must be numbers.")
    delivery=delivery.lower()
    if delivery not in {"code","manual"} or price<=0 or quantity<0 or stock<0: return await m.answer("❌ Invalid values.")
    if len(category.encode("utf-8")) > 48: return await m.answer("❌ Category is too long for Telegram navigation. Keep it within 48 UTF-8 bytes.")
    result=await asyncio.to_thread(_product_edit_tx,pid,name,category,quantity,price,delivery,stock,description)
    if result.get("error")=="not_found": await state.clear(); return await m.answer("❌ Product not found.")
    if result.get("error")=="active_orders": return await m.answer("❌ Delivery type cannot be changed while this product has awaiting/pending orders.")
    if result.get("error")=="code_inventory": return await m.answer("❌ This product still has available/reserved codes. Resolve those codes before switching delivery to manual.")
    await aadmin_log(m.from_user.id,"edit_product",f"product #{pid}")
    await state.clear(); await m.answer(f"✅ Product #{pid} updated.",reply_markup=admin_menu())

@router.callback_query(F.data.startswith("ptoggle:"))
async def product_toggle(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1]); await adb_execute("UPDATE products SET active=CASE WHEN active=1 THEN 0 ELSE 1 END,updated_at=NOW() WHERE id=%s",(pid,)); await aadmin_log(c.from_user.id,"toggle_product",f"product #{pid}"); await c.answer("Updated"); await product_manage(c)

@router.callback_query(F.data.startswith("pdelete:"))
async def product_delete(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1])
    p=await adb_execute("SELECT id,name,category FROM products WHERE id=%s AND COALESCE(archived,0)=0",(pid,),"one")
    if not p:
        return await c.answer("Product already removed or not found.",show_alert=True)
    row=await adb_execute("SELECT COUNT(*) AS c FROM orders WHERE product_id=%s",(pid,),"one")
    orders=int((row or {}).get("c") or 0)
    await c.answer()
    await c.message.edit_text(
        f"🗑 <b>REMOVE PRODUCT?</b>\n\n"
        f"🎮 <b>{html.escape(p['name'])}</b>\n"
        f"🏷 {html.escape(p['category'])}\n"
        f"🧾 Linked order history: <b>{orders}</b>\n\n"
        f"The product will disappear from Shop and Product Management. "
        f"Existing order history will remain safe.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Yes, Remove",callback_data=f"pdelete_confirm:{pid}")],
            [InlineKeyboardButton(text="❌ Keep Product",callback_data=f"p:{pid}")]
        ])
    )

@router.callback_query(F.data.startswith("pdelete_confirm:"))
async def product_delete_confirm(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1])
    p=await adb_execute("SELECT id,name FROM products WHERE id=%s AND COALESCE(archived,0)=0",(pid,),"one")
    if not p:
        return await c.answer("Product already removed.",show_alert=True)
    # Soft archive is intentional: orders keep their product FK and historical name.
    await adb_execute(
        "UPDATE products SET archived=1,active=0,featured=0,hot=0,best_seller=0,sale_price=NULL,sale_until=NULL,updated_at=NOW() WHERE id=%s",
        (pid,)
    )
    await asyncio.to_thread(admin_log,c.from_user.id,"archive_product",f"product #{pid} {p['name']}")
    await c.answer("Product removed.")
    c.data="admin:products"
    return await admin_products(c)

@router.callback_query(F.data=="admin:codes")
async def admin_codes(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=await adb_execute("SELECT p.id,p.name,COUNT(pc.id) FILTER (WHERE pc.status='available') AS available,COUNT(pc.id) FILTER (WHERE pc.status='reserved') AS reserved,COUNT(pc.id) FILTER (WHERE pc.status='sold') AS sold FROM products p LEFT JOIN product_codes pc ON pc.product_id=p.id WHERE p.delivery_type='code' AND COALESCE(p.archived,0)=0 GROUP BY p.id ORDER BY p.id DESC",fetch="all")
    buttons=[[InlineKeyboardButton(text=f"🎫 {r['name'][:18]} • {r['available'] or 0} avail • {r['reserved'] or 0} held",callback_data=f"codes_add:{r['id']}")] for r in rows]
    buttons.append([InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]); await c.answer(); await c.message.edit_text("🎫 <b>Code Inventory</b>\nSelect a product:",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("codes_add:"))
async def codes_add(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return
    d=await state.get_data(); lines=list(dict.fromkeys(x.strip() for x in (m.text or "").splitlines() if x.strip()))
    if not lines: return await m.answer("❌ No codes found.")
    result=await asyncio.to_thread(_codes_add_tx,d["pid"],lines)
    if result.get("error")=="not_found": await state.clear(); return await m.answer("❌ Product not found.")
    if result.get("error")=="active_orders": return await m.answer("❌ Cannot switch this product to code delivery while it has awaiting/pending orders.")
    added=result["added"]; duplicates=result["duplicates"]
    await aadmin_log(m.from_user.id,"add_codes",f"product #{d['pid']} added={added} duplicates={duplicates}")
    await state.clear(); await m.answer(f"✅ Added: <b>{added}</b>\n♻️ Duplicates skipped: <b>{duplicates}</b>",reply_markup=admin_menu())


def admin_order_search_rows(query, limit=20):
    """Search admin orders by order id, Telegram id, username/name, product, or status."""
    q=(query or "").strip()
    if not q:
        return []
    limit=max(1,min(50,int(limit or 20)))
    raw=q[1:] if q.startswith("#") else q
    like=f"%{q.lstrip('@#')}%"
    numeric=int(raw) if raw.isdigit() else None
    conditions=["u.username ILIKE %s","u.name ILIKE %s","p.name ILIKE %s","o.status ILIKE %s"]
    params=[like,like,like,like]
    if numeric is not None:
        conditions[0:0]=["o.id=%s","u.tg_id=%s"]
        params[0:0]=[numeric,numeric]
    sql=f"""SELECT o.id,o.total,o.status,o.created_at,p.name,u.tg_id,u.username
            FROM orders o
            JOIN users u ON u.id=o.user_id
            JOIN products p ON p.id=o.product_id
            WHERE {' OR '.join(conditions)}
            ORDER BY o.id DESC LIMIT %s"""
    params.append(limit)
    return db_execute(sql,tuple(params),"all") or []


def order_search_markup(rows):
    buttons=[]
    for r in rows or []:
        label=f"#{r['id']} • {str(r['status']).replace('_',' ').title()} • {str(r['name'])[:18]}"
        buttons.append([InlineKeyboardButton(text=label,callback_data=f"admin:order_track:{r['id']}")])
    buttons.append([
        InlineKeyboardButton(text="🔎 Search Again",callback_data="admin:order_search"),
        InlineKeyboardButton(text="⬅️ Orders",callback_data="admin:orders"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.callback_query(F.data=="admin:order_search")
async def admin_order_search_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await state.set_state(AdminState.order_search)
    await c.answer()
    await c.message.answer(
        "🔎 <b>Order Search</b>\n\n"
        "Send Order ID, Telegram ID, username, product name or status.\n\n"
        "Examples:\n<code>#123</code>\n<code>987654321</code>\n"
        "<code>pending</code>\n<code>PUBG</code>"
    )


@router.message(AdminState.order_search)
async def admin_order_search_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return
    q=(m.text or "").strip()
    if q.casefold()=="/cancel":
        await state.clear()
        return await m.answer("Search cancelled.",reply_markup=admin_menu())
    rows=await asyncio.to_thread(admin_order_search_rows,q,20)
    if not rows:
        return await m.answer(
            f"🔎 No orders found for <code>{html.escape(q)}</code>.\n"
            "Try Order ID, Telegram ID, product or status."
        )
    await state.clear()
    await m.answer(
        f"🔎 <b>Order Search Results</b>\nFound: <b>{len(rows)}</b>",
        reply_markup=order_search_markup(rows),
    )



@router.callback_query(F.data.startswith("admin:order_track:"))
async def admin_order_track(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    oid=int(c.data.rsplit(":",1)[1])
    order=await adb_execute(
        """SELECT o.id,o.total,o.status,o.created_at,u.tg_id,p.name AS product_name
           FROM orders o JOIN users u ON u.id=o.user_id JOIN products p ON p.id=o.product_id
           WHERE o.id=%s""",(oid,),"one")
    if not order:
        return await c.answer("Order not found.",show_alert=True)
    events=await asyncio.to_thread(order_timeline,oid)
    await c.answer()
    await c.message.edit_text(
        build_order_tracking_text(order,events)+f"\n\n👤 Buyer: <code>{order['tg_id']}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🧾 Open Order",callback_data=f"admin_order:{oid}")],
            [InlineKeyboardButton(text="⬅️ Orders",callback_data="admin:orders")]
        ]))

@router.callback_query(F.data=="admin:orders")
async def admin_orders(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    rows=await asyncio.to_thread(
        db_execute,
        """SELECT o.id,o.total,o.status,o.created_at,p.name,u.tg_id
           FROM orders o JOIN products p ON p.id=o.product_id
           JOIN users u ON u.id=o.user_id
           ORDER BY o.id DESC LIMIT 20""",
        (),
        "all",
    )
    text_msg="🧾 No orders yet." if not rows else (
        "🧾 <b>Recent Orders</b>\n\n"+
        "\n".join(
            f"#{r['id']} • {html.escape(r['name'][:18])}\n"
            f"👤 <code>{r['tg_id']}</code> • {fmt_money(r['total'])}\n"
            f"{status_emoji(r['status'])} {r['status'].replace('_',' ').title()}\n"
            for r in rows
        )
    )
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔎 Search Orders",callback_data="admin:order_search"),
         InlineKeyboardButton(text="💸 Refund Queue",callback_data="admin:refunds")],
        [InlineKeyboardButton(text=setting("admin_back","⬅️ Admin"),callback_data="admin:dashboard")]
    ])
    await c.answer()
    await c.message.edit_text(text_msg,reply_markup=kb)

@router.callback_query(F.data.startswith("order_credential:"))
async def order_credential_reveal(c:CallbackQuery):
    if not admin_can(c.from_user.id, "sensitive"):
        if is_admin(c.from_user.id):
            await asyncio.to_thread(security_log, "readonly_sensitive_denied", c.from_user.id, None, "Credential reveal denied")
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




def delivery_template_rows():
    return db_execute("SELECT id,name,body FROM delivery_templates WHERE active=1 ORDER BY name LIMIT 20",fetch="all") or []


@router.callback_query(F.data=="admin:delivery_templates")
async def admin_delivery_templates(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=await asyncio.to_thread(delivery_template_rows)
    kb=[[InlineKeyboardButton(text=f"📝 {r['name'][:28]}",callback_data=f"delivery_tpl_view:{r['id']}")] for r in rows]
    kb.append([InlineKeyboardButton(text="➕ New Template",callback_data="delivery_tpl_create")])
    kb.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")])
    await c.answer(); await c.message.edit_text(
      "📝 <b>Manual Delivery Templates</b>\n\nReusable delivery-note text for manual orders.",
      reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data=="delivery_tpl_create")
async def delivery_template_create_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await state.set_state(AdminState.delivery_template_create)
    await c.answer()
    await c.message.answer("➕ Send:\n<code>Template Name | Delivery text</code>")


@router.message(AdminState.delivery_template_create)
async def delivery_template_create_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return await state.clear()
    parts=[x.strip() for x in (m.text or "").split("|",1)]
    if len(parts)!=2 or not parts[0] or not parts[1] or len(parts[0])>60 or len(parts[1])>4000:
        return await m.answer("❌ Use: <code>Template Name | Delivery text</code> (name ≤60, text ≤4000).")
    await adb_execute("""INSERT INTO delivery_templates(name,body,created_by) VALUES(%s,%s,%s)
      ON CONFLICT(name) DO UPDATE SET body=EXCLUDED.body,created_by=EXCLUDED.created_by,updated_at=NOW(),active=1""",
      (parts[0],parts[1],m.from_user.id))
    await asyncio.to_thread(admin_log,m.from_user.id,"delivery_template_saved",parts[0])
    await state.clear(); await m.answer("✅ Delivery template saved.")


@router.callback_query(F.data.startswith("delivery_tpl_view:"))
async def delivery_template_view(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    tid=int(c.data.rsplit(":",1)[1])
    r=await asyncio.to_thread(db_execute,"SELECT id,name,body FROM delivery_templates WHERE id=%s",(tid,),"one")
    if not r: return await c.answer("Template not found.",show_alert=True)
    await c.answer(); await c.message.edit_text(
      f"📝 <b>{html.escape(r['name'])}</b>\n\n{html.escape(r['body'])}",
      reply_markup=InlineKeyboardMarkup(inline_keyboard=[
       [InlineKeyboardButton(text="🗑 Delete",callback_data=f"delivery_tpl_delete:{tid}")],
       [InlineKeyboardButton(text="⬅️ Templates",callback_data="admin:delivery_templates")]
      ]))


@router.callback_query(F.data.startswith("delivery_tpl_delete:"))
async def delivery_template_delete(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    tid=int(c.data.rsplit(":",1)[1])
    await asyncio.to_thread(db_execute,"UPDATE delivery_templates SET active=0,updated_at=NOW() WHERE id=%s",(tid,))
    await asyncio.to_thread(admin_log,c.from_user.id,"delivery_template_deleted",f"id={tid}")
    await c.answer("Template removed.")
    c.data="admin:delivery_templates"; return await admin_delivery_templates(c)


@router.callback_query(F.data.startswith("order_note_template:"))
async def manual_order_template_preview(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    _,oid,tid=c.data.split(":")
    o,r=await asyncio.gather(
      adb_execute("SELECT id,status FROM orders WHERE id=%s",(int(oid),),"one"),
      adb_execute("SELECT id,name,body FROM delivery_templates WHERE id=%s AND active=1",(int(tid),),"one"))
    if not o or o["status"]!="pending": return await c.answer("Order already processed.",show_alert=True)
    if not r: return await c.answer("Template unavailable.",show_alert=True)
    await c.answer()
    await c.message.answer(
      f"📝 <b>Template Preview — Order #{oid}</b>\n\n"
      f"<b>{html.escape(r['name'])}</b>\n{html.escape(r['body'])}\n\nConfirm before delivery.",
      reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Use & Deliver",callback_data=f"order_note_template_confirm:{oid}:{tid}")],
        [InlineKeyboardButton(text="❌ Cancel",callback_data=f"order_note_cancel:{oid}")]
      ]))


def _complete_manual_order_with_note_tx(oid,note):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,)); o=cur.fetchone()
                if not o or o["status"]!="pending": return None,None,None
                cur.execute("UPDATE orders SET admin_note=%s,delivery_note=%s,account_password='',status='completed',processed_at=NOW(),updated_at=NOW() WHERE id=%s",(note,note,oid))
                award_completed_order_rewards(cur,oid,o["user_id"],o["total"])
                cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
                cur.execute("SELECT name FROM products WHERE id=%s",(o["product_id"],)); p=cur.fetchone()
                return o,u,p

async def complete_manual_order_with_note(bot,admin_id,oid,note):
    result=await asyncio.to_thread(_complete_manual_order_with_note_tx,oid,note)
    if result[0]:
        await aadmin_log(admin_id,"manual_delivery_template",f"order #{oid}")
    return result


@router.callback_query(F.data.startswith("order_note_template_confirm:"))
async def manual_order_template_confirm(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    _,oid,tid=c.data.split(":")
    r=await adb_execute("SELECT body FROM delivery_templates WHERE id=%s AND active=1",(int(tid),),"one")
    if not r: return await c.answer("Template unavailable.",show_alert=True)
    o,u,p=await complete_manual_order_with_note(c.bot,c.from_user.id,int(oid),r["body"])
    if not o: return await c.answer("Order already processed.",show_alert=True)
    try:
        await asyncio.to_thread(sync_loyalty_profile,o["user_id"])
    except Exception as exc:
        record_runtime_error("phase3_template_loyalty_sync",exc,{"order_id":oid})
    markup=InlineKeyboardMarkup(inline_keyboard=[
      [InlineKeyboardButton(text="📦 Order Details",callback_data=f"order_detail:{oid}")],
      [InlineKeyboardButton(text="🛍️ Buy More",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]])
    delivered=await notify_user(c.bot,u["tg_id"],
      f"🎉 <b>Order Delivered</b>\n\n🧾 Order: <b>#{oid}</b>\n📦 Product: <b>{html.escape(p['name'] if p else 'Product')}</b>\n\n"
      f"📝 <b>Delivery Information</b>\n{html.escape(r['body'])}",reply_markup=markup)
    await c.answer("Delivered.")
    await c.message.answer(
      f"✅ Order #{oid} completed." + ("" if delivered else " Buyer notification queued for retry."),
      reply_markup=admin_menu())


@router.callback_query(F.data.startswith("order_note:"))
async def manual_order_note_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":")[1])
    o=await adb_execute("SELECT id,status FROM orders WHERE id=%s",(oid,),"one")
    if not o or o["status"]!="pending":
        return await c.answer("Order already processed.",show_alert=True)
    templates=await asyncio.to_thread(delivery_template_rows)
    await state.update_data(order_id=oid)
    await state.set_state(AdminState.manual_delivery_note)
    await c.answer()
    kb=[[InlineKeyboardButton(text=f"📝 {r['name'][:28]}",callback_data=f"order_note_template:{oid}:{r['id']}")] for r in templates[:10]]
    kb.append([InlineKeyboardButton(text="✍️ Write Custom Delivery",callback_data=f"order_note_custom:{oid}")])
    kb.append([InlineKeyboardButton(text="❌ Cancel",callback_data=f"order_note_cancel:{oid}")])
    await c.message.answer(
        f"✍️ <b>Delivery note for Order #{oid}</b>\n\n"
        "Choose a saved template or write a custom delivery note.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))



@router.callback_query(F.data.startswith("order_note_custom:"))
async def manual_order_custom_prompt(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    oid=int(c.data.rsplit(":",1)[1])
    o=await adb_execute("SELECT id,status FROM orders WHERE id=%s",(oid,),"one")
    if not o or o["status"]!="pending":
        await state.clear()
        return await c.answer("Order already processed.",show_alert=True)
    await state.update_data(order_id=oid)
    await state.set_state(AdminState.manual_delivery_note)
    await c.answer()
    await c.message.answer(
        f"✍️ Send the custom delivery note for Order <b>#{oid}</b>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel",callback_data=f"order_note_cancel:{oid}")]
        ]))

@router.callback_query(F.data.startswith("order_note_cancel:"))
async def manual_order_note_cancel(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    oid=int(c.data.rsplit(":",1)[1])
    await state.clear()
    await c.answer("Delivery cancelled.")
    # Cancel must always leave the manual-delivery FSM. Keep navigation on a known-good callback.
    try:
        await c.message.edit_text(
            f"❌ Delivery entry cancelled for Order <b>#{oid}</b>.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Orders",callback_data="admin:orders")],
                [InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]
            ]))
    except Exception:
        await c.message.answer(
            f"❌ Delivery entry cancelled for Order <b>#{oid}</b>.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Orders",callback_data="admin:orders")]
            ]))

def _manual_delivery_complete_tx(oid, note):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,)); o=cur.fetchone()
                if not o or o["status"]!="pending":
                    return {"ok":False,"reason":"processed"}
                cur.execute("UPDATE orders SET admin_note=%s,delivery_note=%s,account_password='',status='completed',processed_at=NOW(),updated_at=NOW() WHERE id=%s",(note,note,oid))
                award_completed_order_rewards(cur,oid,o["user_id"],o["total"])
                cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
                cur.execute("SELECT name FROM products WHERE id=%s",(o["product_id"],)); p=cur.fetchone()
                return {"ok":True,"order":o,"user":u,"product":p}

def _manual_order_reject_tx(oid, admin_id):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,)); o=cur.fetchone()
                if not o or o["status"]!="pending":
                    return {"ok":False,"reason":"processed"}
                payment_mode=(o.get("payment_mode") or "wallet").strip().lower()
                if o.get("delivered_code"):
                    cur.execute("UPDATE product_codes SET status='available',sold_to=NULL,order_id=NULL,sold_at=NULL WHERE order_id=%s",(oid,))
                    sync_code_product_stock(o["product_id"],conn)
                else:
                    cur.execute("UPDATE products SET stock=stock+1,updated_at=NOW() WHERE id=%s",(o["product_id"],))
                cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
                if payment_mode=="direct":
                    cur.execute("UPDATE orders SET status='refund_pending',refund_amount=0,account_password='',processed_at=NULL,updated_at=NOW() WHERE id=%s",(oid,))
                    if o.get("payment_id"):
                        cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(o["payment_id"],)); payment=cur.fetchone()
                        if payment and payment["status"]=="credited":
                            cur.execute("UPDATE payments SET status='refund_pending',updated_at=NOW() WHERE id=%s",(payment["id"],))
                            record_payment_audit(cur,payment["id"],admin_id,"refund_requested","credited","refund_pending",payment["amount"],payment["method"],payment["trx_id"],f"Order #{oid} rejected after direct payment; external refund required")
                    refund_mode="external"
                else:
                    cur.execute("UPDATE orders SET status='refunded',refund_amount=total,account_password='',processed_at=NOW(),updated_at=NOW() WHERE id=%s",(oid,))
                    cur.execute("UPDATE users SET balance=balance+%s,updated_at=NOW() WHERE id=%s",(o["total"],o["user_id"]))
                    cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(o["user_id"],o["total"],"refund",f"Order #{oid} rejected"))
                    refund_mode="wallet"
                return {"ok":True,"order":o,"user":u,"refund_mode":refund_mode}

def _admin_balance_tx(tg_id, amount, action, note):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users WHERE tg_id=%s FOR UPDATE",(tg_id,)); u=cur.fetchone()
                if not u:
                    return {"ok":False,"reason":"not_found"}
                delta=amount if action=="add" else -amount
                cur.execute("UPDATE users SET balance=balance+%s,updated_at=NOW() WHERE id=%s AND (%s >= 0 OR balance >= %s)",(delta,u["id"],delta,amount))
                if cur.rowcount!=1:
                    return {"ok":False,"reason":"low_balance"}
                cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(u["id"],delta,f"admin_{action}",note))
                return {"ok":True,"user_id":u["id"],"delta":delta}

@router.message(AdminState.manual_delivery_note)
async def manual_order_note_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return await state.clear()
    note=(m.text or "").strip()
    if note.lower()=="/cancel":
        await state.clear(); return await m.answer("❌ Delivery note cancelled.",reply_markup=admin_menu())
    if not note or len(note)>4000: return await m.answer("❌ Delivery note must be 1–4000 characters.")
    d=await state.get_data(); oid=int(d["order_id"])
    result=await asyncio.to_thread(_manual_delivery_complete_tx,oid,note)
    if not result.get("ok"):
        await state.clear(); return await m.answer("❌ Order already processed.")
    o,u,p=result["order"],result["user"],result["product"]
    await aadmin_log(m.from_user.id,"manual_delivery_note",f"order #{oid} delivered with admin note")
    try:
        await asyncio.to_thread(sync_loyalty_profile,o["user_id"])
    except Exception as exc:
        record_runtime_error("phase3_manual_loyalty_sync",exc,{"order_id":oid})
    await state.clear()
    delivery_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 Order Details",callback_data=f"order_detail:{oid}")],[InlineKeyboardButton(text="🛍️ Buy More",callback_data="home:shop"),InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]])
    delivered_to_buyer = await notify_user(m.bot,u["tg_id"],f"🎉 <b>Order Delivered</b>\n\n🧾 Order: <b>#{oid}</b>\n📦 Product: <b>{html.escape(p['name'] if p else 'Product')}</b>\n\n📝 <b>Delivery Information</b>\n{html.escape(note)}",reply_markup=delivery_markup)
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
        result=await asyncio.to_thread(_manual_order_reject_tx,oid,c.from_user.id)
    except Exception as exc:
        error_id=record_runtime_error("order_reject_refund",exc,{"admin_id":c.from_user.id,"order_id":oid})
        return await c.answer(f"Refund transition failed safely. Ref: {error_id}",show_alert=True)
    if not result.get("ok"):
        return await c.answer("Already processed.",show_alert=True)
    o,u,refund_mode=result["order"],result["user"],result["refund_mode"]
    await aadmin_log(c.from_user.id,"reject_refund",f"order #{oid} refund_mode={refund_mode}")
    try:
        event_status="refund_pending" if refund_mode=="external" else "refunded"
        event_message="Order rejected; external refund is pending." if refund_mode=="external" else "Order rejected and refunded to wallet."
        await asyncio.to_thread(record_order_event,oid,"order_rejected",event_status,event_message,c.from_user.id)
        await asyncio.to_thread(sync_loyalty_profile,o["user_id"])
    except Exception as exc:
        record_runtime_error("phase3_order_reject_event",exc,{"order_id":oid})
    if refund_mode=="external":
        await c.answer("Rejected — external refund required")
        await c.message.edit_text(f"↩️ Order #{oid} rejected. Stock restored.\n\n💳 <b>Refund Pending:</b> send the refund through the original payment method, then confirm below.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Mark External Refund Sent",callback_data=f"order_refund_confirm:{oid}")]]))
        await notify_user(c.bot,u["tg_id"],f"↩️ <b>Order #{oid} rejected</b>\n\nYour refund of <b>{fmt_money(o['total'])}</b> is being returned through the original payment method.")
    else:
        await c.answer("Rejected + refunded")
        await c.message.edit_text(f"↩️ Order #{oid} rejected and refunded to buyer wallet.")
        await notify_user(c.bot,u["tg_id"],f"↩️ <b>Order #{oid} refunded</b>\nRefunded to your bot wallet: {fmt_money(o['total'])}")



def financial_audit_snapshot():
    """Read-only reconciliation checks for payment/order/wallet/refund invariants."""
    checks={}
    checks["credited_wallet_missing_log"]=db_execute("""
        SELECT COUNT(*) AS c FROM payments p
        WHERE p.status='credited' AND p.order_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM balance_logs bl
            WHERE bl.user_id=p.user_id AND bl.amount=p.amount AND bl.action='payment'
              AND bl.note=('Payment #' || p.id::text)
          )
    """,(),"one")["c"]
    checks["direct_refund_payment_mismatch"]=db_execute("""
        SELECT COUNT(*) AS c FROM orders o
        LEFT JOIN payments p ON p.id=o.payment_id
        WHERE o.payment_mode='direct' AND o.status IN ('refund_pending','refunded')
          AND (p.id IS NULL OR p.status<>o.status)
    """,(),"one")["c"]
    checks["credited_direct_stuck_awaiting"]=db_execute("""
        SELECT COUNT(*) AS c FROM payments p JOIN orders o ON o.id=p.order_id
        WHERE p.status='credited' AND o.status='awaiting_payment'
    """,(),"one")["c"]
    checks["wallet_refund_missing_log"]=db_execute("""
        SELECT COUNT(*) AS c FROM orders o
        WHERE o.payment_mode='wallet' AND o.status='refunded'
          AND NOT EXISTS (
            SELECT 1 FROM balance_logs bl
            WHERE bl.user_id=o.user_id AND bl.amount=o.total
              AND bl.action IN ('refund','provider_refund')
              AND bl.note LIKE ('%Order #' || o.id::text || '%')
          )
    """,(),"one")["c"]
    checks["duplicate_refund_logs"]=db_execute("""
        SELECT COUNT(*) AS c FROM (
            SELECT o.id
            FROM orders o JOIN balance_logs bl ON bl.user_id=o.user_id AND bl.amount=o.total
            WHERE o.status='refunded' AND o.payment_mode='wallet'
              AND bl.action IN ('refund','provider_refund')
              AND bl.note LIKE ('%Order #' || o.id::text || '%')
            GROUP BY o.id HAVING COUNT(*)>1
        ) x
    """,(),"one")["c"]
    checks["credited_payment_without_audit"]=db_execute("""
        SELECT COUNT(*) AS c FROM payments p
        WHERE p.status='credited'
          AND NOT EXISTS (SELECT 1 FROM payment_audit a WHERE a.payment_id=p.id AND a.new_status='credited')
    """,(),"one")["c"]
    return {k:int(v or 0) for k,v in checks.items()}


def financial_audit_anomalies(limit=20):
    return db_execute("""
        SELECT kind,ref_id,detail FROM (
          SELECT 'direct_refund_mismatch'::text kind,o.id ref_id,
                 ('order='||o.status||', payment='||COALESCE(p.status,'missing'))::text detail,
                 o.updated_at ts
          FROM orders o LEFT JOIN payments p ON p.id=o.payment_id
          WHERE o.payment_mode='direct' AND o.status IN ('refund_pending','refunded')
            AND (p.id IS NULL OR p.status<>o.status)
          UNION ALL
          SELECT 'credited_direct_awaiting',o.id,('payment #'||p.id||' credited but order awaiting_payment'),o.updated_at
          FROM payments p JOIN orders o ON o.id=p.order_id
          WHERE p.status='credited' AND o.status='awaiting_payment'
          UNION ALL
          SELECT 'wallet_refund_log_missing',o.id,'refunded wallet order has no matching positive refund log',o.updated_at
          FROM orders o
          WHERE o.payment_mode='wallet' AND o.status='refunded'
            AND NOT EXISTS (
              SELECT 1 FROM balance_logs bl WHERE bl.user_id=o.user_id AND bl.amount=o.total
                AND bl.action IN ('refund','provider_refund') AND bl.note LIKE ('%Order #'||o.id::text||'%')
            )
        ) q ORDER BY ts DESC LIMIT %s
    """,(max(1,min(100,int(limit))),),"all") or []

@router.callback_query(F.data=="admin:financial_audit")
async def admin_financial_audit(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    try:
        snap, rows = await asyncio.gather(
            asyncio.to_thread(financial_audit_snapshot),
            asyncio.to_thread(financial_audit_anomalies,20),
        )
    except Exception as exc:
        error_id=record_runtime_error("financial_audit_dashboard",exc,{"admin_id":c.from_user.id})
        return await c.answer(f"Audit failed. Ref: {error_id}",show_alert=True)
    total=sum(snap.values())
    lines=[
        "🧾 <b>Financial Reconciliation Audit</b>","━━━━━━━━━━━━━━━━━━",
        f"Overall anomalies: <b>{total}</b>","",
        f"💳 Credited wallet payment missing credit log: <b>{snap['credited_wallet_missing_log']}</b>",
        f"↩️ Direct refund/payment state mismatch: <b>{snap['direct_refund_payment_mismatch']}</b>",
        f"⏳ Credited direct payment still awaiting: <b>{snap['credited_direct_stuck_awaiting']}</b>",
        f"💰 Refunded wallet order missing refund log: <b>{snap['wallet_refund_missing_log']}</b>",
        f"⚠️ Duplicate wallet refund logs: <b>{snap['duplicate_refund_logs']}</b>",
        f"🧩 Credited payment missing audit trail: <b>{snap['credited_payment_without_audit']}</b>","",
        "<b>Recent actionable mismatches</b>"
    ]
    if rows:
        for r in rows[:20]:
            lines.append(f"• <code>{html.escape(str(r['kind']))}</code> #{int(r['ref_id'])} — {html.escape(str(r['detail']))}")
    else:
        lines.append("✅ No actionable state mismatch found.")
    lines += ["", "ℹ️ This screen is read-only. It never credits, refunds, or replays a provider checkout."]
    await c.answer()
    await c.message.edit_text("\n".join(lines),reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Re-run Audit",callback_data="admin:financial_audit")],
        [InlineKeyboardButton(text="💸 Refund Queue",callback_data="admin:refunds"),InlineKeyboardButton(text="💳 Payments",callback_data="admin:payments")],
        [InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:menu")],
    ]))

@router.callback_query(F.data.startswith("order_refund_confirm:"))
async def direct_refund_confirm(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":",1)[1])
    row=await asyncio.to_thread(
        db_execute,
        """SELECT o.id,o.total,o.status,o.payment_mode,p.name,u.tg_id
           FROM orders o JOIN products p ON p.id=o.product_id
           JOIN users u ON u.id=o.user_id WHERE o.id=%s""",
        (oid,),
        "one",
    )
    if not row or row["status"]!="refund_pending" or (row.get("payment_mode") or "")!="direct":
        return await c.answer("Refund is no longer pending.",show_alert=True)
    await c.answer()
    await c.message.edit_text(
        f"⚠️ <b>Confirm External Refund</b>\n\n"
        f"Order: <b>#{oid}</b>\n"
        f"Product: <b>{html.escape(row['name'])}</b>\n"
        f"Buyer: <code>{row['tg_id']}</code>\n"
        f"Amount: <b>{fmt_money(row['total'])}</b>\n\n"
        "Only confirm after you have actually sent the refund through the original payment method.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Yes, Refund Was Sent",callback_data=f"order_refund_complete:{oid}")],
            [InlineKeyboardButton(text="❌ Cancel",callback_data="admin:refunds")]
        ])
    )

def _direct_refund_complete_tx(oid:int, admin_id:int):
    """Finalize an external direct-payment refund without holding DB locks across Telegram awaits."""
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(oid,)); o=cur.fetchone()
                if not o or o["status"]!="refund_pending" or (o.get("payment_mode") or "")!="direct":
                    return {"ok":False,"message":"Refund is not pending for this order."}
                if not o.get("payment_id"):
                    raise RuntimeError("Direct refund order has no linked payment; confirmation aborted.")
                cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(o["payment_id"],)); payment=cur.fetchone()
                if not payment:
                    raise RuntimeError("Linked payment is missing; confirmation aborted.")
                if payment["status"]!="refund_pending":
                    raise RuntimeError(f"Linked payment status is {payment['status']}; expected refund_pending.")
                cur.execute("UPDATE orders SET status='refunded',refund_amount=total,processed_at=NOW(),updated_at=NOW() WHERE id=%s AND status='refund_pending'",(oid,))
                if cur.rowcount != 1:
                    raise RuntimeError("Order refund state changed")
                cur.execute("UPDATE payments SET status='refunded',updated_at=NOW() WHERE id=%s AND status='refund_pending'",(payment["id"],))
                if cur.rowcount != 1:
                    raise RuntimeError("Payment refund state changed")
                record_payment_audit(cur,payment["id"],admin_id,"refund_completed","refund_pending","refunded",payment["amount"],payment["method"],payment["trx_id"],f"Admin confirmed external refund for Order #{oid}")
                cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
                return {"ok":True,"order":o,"payment":payment,"user":u}

@router.callback_query(F.data.startswith("order_refund_complete:"))
async def direct_refund_complete(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    oid=int(c.data.split(":",1)[1])
    try:
        result=await asyncio.to_thread(_direct_refund_complete_tx,oid,c.from_user.id)
        if not result.get("ok"):
            return await c.answer(result.get("message","Refund is not pending for this order."),show_alert=True)
        o=result["order"]; u=result["user"]
    except Exception as exc:
        error_id=record_runtime_error("external_refund_complete",exc,{"admin_id":c.from_user.id,"order_id":oid})
        return await c.answer(f"Could not confirm refund safely. Ref: {error_id}",show_alert=True)
    await aadmin_log(c.from_user.id,"external_refund_complete",f"order #{oid}")
    try:
        await asyncio.to_thread(record_order_event,oid,"refund_completed","refunded","Refund completed through the original payment method.",c.from_user.id)
        await asyncio.to_thread(sync_loyalty_profile,o["user_id"])
    except Exception as exc:
        record_runtime_error("phase3_refund_event",exc,{"order_id":oid})
    await c.answer("Refund marked sent")
    await c.message.edit_text(f"✅ External refund for Order #{oid} marked completed.")
    if u:
        await notify_user(c.bot,u["tg_id"],f"✅ <b>Refund Completed</b>\n\nOrder #{oid}: <b>{fmt_money(o['total'])}</b> was marked refunded through the original payment method.")

@router.callback_query(F.data=="admin:refunds")
async def admin_refund_queue(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=await adb_execute(
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
        kb=[[InlineKeyboardButton(text=f"✅ Refund Sent #{r['id']}",callback_data=f"order_refund_confirm:{r['id']}")] for r in rows]
        kb.append([InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")])
    await c.answer()
    await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data=="admin:payment_methods")
async def admin_payment_methods(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    active=sum(1 for code,_,_ in payment_method_specs() if payment_method_enabled(code))
    text=("💳 <b>Payment Methods — Payment 2.0</b>\n\n"
          f"Manual methods active: <b>{active}/4</b>\n"
          f"UddoktaPay Auto Gateway: <b>{'🟢 Enabled' if uddoktapay_enabled() else ('🟡 Ready / Off' if uddoktapay_ready() else '🔴 Not Ready')}</b>\n\n"
          "Manual methods use account/TxID verification. UddoktaPay uses provider API verification and can auto-credit wallet/orders.")
    await c.answer(); await c.message.edit_text(text,reply_markup=payment_methods_admin_keyboard())


@router.callback_query(F.data=="admin:uddoktapay")
async def admin_uddoktapay_gateway(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    cfg=uddoktapay_config()
    ready=uddoktapay_ready()
    enabled=uddoktapay_enabled()
    host=""
    try:
        host=urlparse(cfg["base_url"]).netloc
    except Exception:
        host=""
    public_host=""
    try:
        public_host=urlparse(cfg["public_base"]).netloc
    except Exception:
        public_host=""
    text=(
        "⚡ <b>UddoktaPay Auto Payment</b>\n\n"
        f"Status: <b>{'🟢 ENABLED' if enabled else ('🟡 READY / OFF' if ready else '🔴 NOT READY')}</b>\n"
        f"API Key: <b>{'✅ Configured' if cfg['api_key'] else '❌ Missing'}</b>\n"
        f"API Host: <b>{html.escape(host or 'Missing')}</b>\n"
        f"Public Callback: <b>{html.escape(public_host or 'Missing')}</b>\n"
        f"Customer Email: <b>{'✅ Configured' if cfg['customer_email'] else '❌ Missing'}</b>\n"
        f"Deposit limits: <b>{setting('deposit_min','10')} – {setting('deposit_max','0') or 'Unlimited'} {currency()}</b>\n\n"
        "🔐 The API key is read only from Railway environment variables and is never displayed in Telegram.\n"
        "✅ Provider status, amount and transaction ID are server-verified before wallet/order credit."
    )
    kb=[
        [InlineKeyboardButton(text=("🔴 Disable Gateway" if enabled else "🟢 Enable Gateway"),callback_data="admin:uddoktapay_toggle")],
        [InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:uddoktapay"),InlineKeyboardButton(text="⬅️ Payment Methods",callback_data="admin:payment_methods")],
    ]
    await c.answer()
    await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data=="admin:uddoktapay_toggle")
async def admin_uddoktapay_toggle(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    if admin_is_readonly(c.from_user.id):
        return await c.answer("Read-only admin cannot change gateway settings.",show_alert=True)
    current=uddoktapay_config()["enabled"]
    if not current and not uddoktapay_ready():
        return await c.answer("Configure Railway UDDOKTAPAY_API_KEY, UDDOKTAPAY_API_URL, PUBLIC_BASE_URL and UDDOKTAPAY_CUSTOMER_EMAIL first.",show_alert=True)
    new="0" if current else "1"
    await adb_execute("INSERT INTO settings(key,value) VALUES('uddoktapay_enabled',%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",(new,))
    _load_settings_cache()
    await aadmin_log(c.from_user.id,"uddoktapay_toggle",f"enabled={new}")
    await c.answer("UddoktaPay enabled" if new=="1" else "UddoktaPay disabled")
    return await admin_uddoktapay_gateway(c)


@router.callback_query(F.data.startswith("admin:paytoggle:"))
async def admin_payment_toggle(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    code=c.data.rsplit(":",1)[1]
    if code not in {x[0] for x in payment_method_specs()}: return await c.answer("Invalid method",show_alert=True)
    new="0" if payment_method_enabled(code) else "1"
    await adb_execute("INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",(f"payment_{code}_enabled",new))
    _load_settings_cache()
    await aadmin_log(c.from_user.id,"payment_method_toggle",f"{code}={new}")
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
    row=await adb_execute("""SELECT
        (SELECT COUNT(*) FROM products WHERE active=1) products,
        (SELECT COUNT(*) FROM products WHERE active=1 AND (CASE WHEN delivery_type='code' THEN (SELECT COUNT(*) FROM product_codes pc WHERE pc.product_id=products.id AND pc.status='available') ELSE stock END)<=%s) low_stock,
        (SELECT COUNT(*) FROM orders WHERE status='completed' AND created_at>=NOW()-INTERVAL '7 days') orders7,
        (SELECT COALESCE(SUM(total),0) FROM orders WHERE status='completed' AND created_at>=NOW()-INTERVAL '7 days') sales7,
        (SELECT COUNT(*) FROM cart_items WHERE updated_at<=NOW()-INTERVAL '6 hours' AND updated_at>=NOW()-INTERVAL '30 days') abandoned_carts,
        (SELECT COUNT(*) FROM users WHERE updated_at<=NOW()-INTERVAL '30 days') inactive_users,
        (SELECT COUNT(*) FROM payments WHERE status='pending') pending_payments
    """,(low_stock_threshold(),),fetch="one")
    top=await adb_execute("""SELECT p.name,COUNT(*) c FROM orders o JOIN products p ON p.id=o.product_id WHERE o.status='completed' AND o.created_at>=NOW()-INTERVAL '7 days' GROUP BY p.id,p.name ORDER BY c DESC LIMIT 3""",fetch="all") or []
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
        [InlineKeyboardButton(text="🧩 Customer CRM",callback_data=f"crm:user:{r['user_id']}"),InlineKeyboardButton(text="🧭 Timeline",callback_data=f"crm:timeline:{r['user_id']}")],
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



def payment_match_assist_snapshot(payment_id):
    """Internal consistency assist only. Never verifies an external payment."""
    row=db_execute(
        """SELECT py.*,o.total AS order_total,o.status AS order_status,
                  u.tg_id,
                  EXISTS(SELECT 1 FROM payment_receipts pr WHERE pr.payment_id=py.id) AS has_receipt,
                  (SELECT COUNT(*) FROM payments x
                   WHERE x.id<>py.id
                     AND regexp_replace(lower(COALESCE(x.trx_id,'')),'[[:space:]]+','','g')
                         = regexp_replace(lower(COALESCE(py.trx_id,'')),'[[:space:]]+','','g')
                     AND COALESCE(py.trx_id,'')<>'') AS same_trx_count
           FROM payments py
           JOIN users u ON u.id=py.user_id
           LEFT JOIN orders o ON o.id=py.order_id
           WHERE py.id=%s""",
        (payment_id,),
        "one",
    )
    if not row:
        return None

    score=0
    checks=[]
    trx=(row.get("trx_id") or "").strip()

    if trx:
        score+=25; checks.append(("✅","TxID present"))
    else:
        checks.append(("❌","TxID missing"))

    if int(row.get("same_trx_count") or 0)==0 and trx:
        score+=25; checks.append(("✅","TxID unique in bot database"))
    elif trx:
        checks.append(("❌","Same TxID appears on another payment"))

    if row.get("order_id"):
        try:
            amount=float(row.get("amount") or 0)
            total=float(row.get("order_total") or 0)
            if abs(amount-total)<0.005:
                score+=25; checks.append(("✅","Payment amount matches linked order"))
            else:
                checks.append(("❌",f"Amount mismatch: payment {amount:g} vs order {total:g}"))
        except Exception:
            checks.append(("⚠️","Could not compare order amount"))
    else:
        score+=15; checks.append(("ℹ️","Wallet deposit; no linked order total"))

    if row.get("has_receipt"):
        score+=15; checks.append(("✅","Receipt attached"))
    else:
        checks.append(("ℹ️","No receipt attached"))

    fraud=int(row.get("fraud_score") or 0)
    if fraud < 30:
        score+=10; checks.append(("✅",f"Low internal fraud score ({fraud}/100)"))
    elif fraud < 60:
        checks.append(("⚠️",f"Moderate internal fraud score ({fraud}/100)"))
    else:
        checks.append(("🛡",f"High internal fraud score ({fraud}/100)"))

    return {"payment":row,"score":min(100,score),"checks":checks}


@router.callback_query(F.data.startswith("pay_match:"))
async def payment_match_assist_view(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":",1)[1])
    snap=await asyncio.to_thread(payment_match_assist_snapshot,pid)
    if not snap:
        return await c.answer("Payment not found.",show_alert=True)
    p=snap["payment"]
    lines="\n".join(f"{icon} {html.escape(msg)}" for icon,msg in snap["checks"])
    text_msg=(
        f"🧩 <b>Payment Match Assist #{pid}</b>\n\n"
        f"Internal consistency: <b>{snap['score']}/100</b>\n"
        f"Amount: <b>{fmt_money(p['amount'])}</b> • Method: <b>{html.escape(str(p['method']).title())}</b>\n"
        f"TxID: <code>{html.escape(str(p.get('trx_id') or ''))}</code>\n\n"
        f"{lines}\n\n"
        "⚠️ <b>This is not payment verification.</b> It only checks data stored inside the bot. "
        "Admin must still verify the external payment account/receipt before approving."
    )
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔎 Evidence",callback_data=f"pay_evidence:{pid}"),
         InlineKeyboardButton(text="🧾 Audit",callback_data=f"pay_audit:{pid}")],
        [InlineKeyboardButton(text="⬅️ Payments",callback_data="admin:payments")]
    ])
    await c.answer()
    await c.message.edit_text(text_msg,reply_markup=kb)


def admin_payment_search_rows(query,limit=20):
    q=(query or "").strip()
    if not q:
        return []
    clauses=[]
    params=[]
    numeric=q[1:] if q.startswith("#") else q
    if numeric.isdigit():
        num=int(numeric)
        clauses.extend(["py.id=%s","u.tg_id=%s","py.order_id=%s"])
        params.extend([num,num,num])
    status=q.casefold().replace(" ","_")
    if status in {"pending","credited","rejected","refunded","refund_pending","expired"}:
        clauses.append("py.status=%s"); params.append(status)
    pattern=f"%{q}%"
    clauses.extend([
        "COALESCE(py.trx_id,'') ILIKE %s",
        "COALESCE(py.method,'') ILIKE %s",
    ])
    params.extend([pattern,pattern])
    sql=f"""SELECT py.id,py.amount,py.status,py.method,py.trx_id,py.order_id,py.created_at,u.tg_id
            FROM payments py JOIN users u ON u.id=py.user_id
            WHERE {' OR '.join(clauses)}
            ORDER BY py.id DESC LIMIT {max(1,min(50,int(limit)))}"""
    return db_execute(sql,tuple(params),"all") or []


@router.callback_query(F.data=="admin:payment_search")
async def admin_payment_search_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await state.set_state(AdminState.payment_search)
    await c.answer()
    await c.message.answer(
        "🔎 <b>Payment Search</b>\n\n"
        "Send Payment ID, Order ID, Telegram ID, TxID, method or status."
    )


@router.message(AdminState.payment_search)
async def admin_payment_search_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id):
        return
    q=(m.text or "").strip()
    if q.casefold()=="/cancel":
        await state.clear()
        return await m.answer("Search cancelled.",reply_markup=admin_menu())
    rows=await asyncio.to_thread(admin_payment_search_rows,q,20)
    if not rows:
        return await m.answer(f"🔎 No payments found for <code>{html.escape(q)}</code>.")
    await state.clear()
    buttons=[]
    for r in rows[:15]:
        buttons.append([
            InlineKeyboardButton(
                text=f"#{r['id']} • {str(r['method']).title()} • {str(r['status']).replace('_',' ').title()}",
                callback_data=f"pay_match:{r['id']}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="⬅️ Payments",callback_data="admin:payments")])
    await m.answer(
        f"🔎 <b>Payment Search Results</b>\nFound: <b>{len(rows)}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


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
            evidence_row=[InlineKeyboardButton(text=f"🧩 Match #{r['id']}",callback_data=f"pay_match:{r['id']}"), InlineKeyboardButton(text=f"🔎 Evidence #{r['id']}",callback_data=f"pay_evidence:{r['id']}")]
            if receipt: evidence_row.append(InlineKeyboardButton(text=f"📸 Receipt #{r['id']}",callback_data=f"pay_receipt:{r['id']}"))
            kb.append(evidence_row)
        kb.append([InlineKeyboardButton(text="🔎 Search Payments",callback_data="admin:payment_search")])
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

def _payment_review_clear_tx(pid:int, admin_id:int):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(pid,)); p=cur.fetchone()
                if not p or p["status"]!="pending": return {"ok":False,"message":"Payment is no longer pending."}
                if not p.get("review_required"):
                    return {"ok":False,"message":"This payment does not require manual review."}
                cur.execute("UPDATE payments SET review_cleared_at=NOW(),review_cleared_by=%s,updated_at=NOW() WHERE id=%s AND status='pending'",(admin_id,pid))
                if cur.rowcount != 1:
                    return {"ok":False,"message":"Payment changed while clearing review."}
                record_payment_audit(cur,pid,admin_id,"fraud_review_cleared","pending","pending",p["amount"],p["method"],p["trx_id"],f"Manual fraud review cleared; score={p.get('fraud_score',0)} flags={p.get('fraud_flags','')}")
                return {"ok":True}

@router.callback_query(F.data.startswith("pay_review_clear:"))
async def payment_review_clear(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":",1)[1])
    try:
        result=await asyncio.to_thread(_payment_review_clear_tx,pid,c.from_user.id)
        if not result.get("ok"):
            return await c.answer(result.get("message","Could not clear review."),show_alert=True)
        await asyncio.to_thread(admin_log,c.from_user.id,"fraud_review_cleared",f"payment #{pid}")
        await c.answer("Manual review cleared.",show_alert=True)
        return await admin_payments(c)
    except Exception as exc:
        error_id=record_runtime_error("fraud_review_clear",exc,{"admin_id":c.from_user.id,"payment_id":pid})
        return await c.answer(f"Could not clear review. Ref: {error_id}",show_alert=True)


def _payment_credit_tx(pid:int, admin_id:int):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(pid,)); p=cur.fetchone()
                if not p or p["status"]!="pending":
                    return {"ok":False,"message":"Already processed."}
                if p.get("review_required") and not p.get("review_cleared_at"):
                    return {"ok":False,"message":f"🛡 Manual review required first. Risk {int(p.get('fraud_score') or 0)}/100."}
                direct_order=None
                if p.get("order_id"):
                    cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(p["order_id"],)); o=cur.fetchone()
                    if not o or o["status"]!="awaiting_payment": raise RuntimeError("Linked order is no longer awaiting payment.")
                    cur.execute("SELECT * FROM products WHERE id=%s FOR UPDATE",(o["product_id"],)); prod=cur.fetchone()
                    if not prod: raise RuntimeError("Product record is unavailable.")
                    delivered_code=None; status="pending"
                    reservation_kind=(o.get("reservation_kind") or "").strip().lower()
                    if bool(o.get("stock_reserved")):
                        if reservation_kind=="code":
                            cur.execute("SELECT * FROM product_codes WHERE order_id=%s AND status='reserved' FOR UPDATE",(o["id"],)); code_row=cur.fetchone()
                            if not code_row: raise RuntimeError("Reserved code is missing; approval aborted.")
                            cur.execute("UPDATE product_codes SET status='sold',sold_at=NOW() WHERE id=%s AND status='reserved'",(code_row["id"],))
                            if cur.rowcount != 1: raise RuntimeError("Reserved code changed; approval aborted.")
                            delivered_code=code_row["code"]; status="completed"; sync_code_product_stock(o["product_id"],conn)
                        elif reservation_kind=="manual": status="pending"
                        else: raise RuntimeError("Unknown stock reservation type; approval aborted.")
                    else:
                        if prod["delivery_type"]=="code":
                            cur.execute("SELECT * FROM product_codes WHERE product_id=%s AND status='available' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",(o["product_id"],)); code_row=cur.fetchone()
                            if not code_row: raise RuntimeError("Code stock unavailable.")
                            cur.execute("UPDATE product_codes SET status='sold',sold_to=%s,sold_at=NOW(),order_id=%s WHERE id=%s AND status='available'",(o["user_id"],o["id"],code_row["id"]))
                            if cur.rowcount != 1: raise RuntimeError("Code stock changed; approval aborted.")
                            delivered_code=code_row["code"]; status="completed"; sync_code_product_stock(o["product_id"],conn)
                        else:
                            cur.execute("UPDATE products SET stock=stock-1,updated_at=NOW() WHERE id=%s AND stock>0",(o["product_id"],))
                            if cur.rowcount!=1: raise RuntimeError("Stock changed; approval aborted.")
                            status="pending"
                    cur.execute("UPDATE payments SET status='credited',updated_at=NOW() WHERE id=%s AND status='pending'",(pid,))
                    if cur.rowcount != 1: raise RuntimeError("Payment status changed; approval aborted.")
                    clear_password = "" if status=="completed" else (o.get("account_password") or "")
                    cur.execute("UPDATE orders SET status=%s,delivered_code=%s,account_password=%s,stock_reserved=FALSE,reservation_kind='',processed_at=%s,updated_at=NOW() WHERE id=%s",(status,delivered_code,clear_password,None if status=="pending" else datetime.now(timezone.utc),o["id"]))
                    if status=="completed": award_completed_order_rewards(cur,o["id"],o["user_id"],o["total"])
                    record_payment_audit(cur,pid,admin_id,"order_approved","pending","credited",p["amount"],p["method"],p["trx_id"],f"Direct payment approved; Order #{o['id']} fulfilled from reserved stock")
                    cur.execute("SELECT tg_id FROM users WHERE id=%s",(o["user_id"],)); u=cur.fetchone()
                    cur.execute("SELECT name FROM products WHERE id=%s",(o["product_id"],)); prod_name=cur.fetchone()
                    direct_order=(o,prod,delivered_code,status,u,prod_name)
                else:
                    cur.execute("UPDATE payments SET status='credited',updated_at=NOW() WHERE id=%s AND status='pending'",(pid,))
                    if cur.rowcount != 1: return {"ok":False,"message":"Already processed."}
                    cur.execute("UPDATE users SET balance=balance+%s,updated_at=NOW() WHERE id=%s",(p["amount"],p["user_id"]))
                    cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(p["user_id"],p["amount"],"payment",f"Payment #{pid}"))
                    record_payment_audit(cur,pid,admin_id,"credited","pending","credited",p["amount"],p["method"],p["trx_id"],"Admin approved payment and credited wallet")
                    cur.execute("SELECT tg_id FROM users WHERE id=%s",(p["user_id"],)); u=cur.fetchone()
                return {"ok":True,"payment":p,"direct_order":direct_order,"user":u}

@router.callback_query(F.data.startswith("pay_credit:"))
async def payment_credit(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1]); direct_order=None
    try:
        result=await asyncio.to_thread(_payment_credit_tx,pid,c.from_user.id)
        if not result.get("ok"):
            return await c.answer(result.get("message","Already processed."),show_alert=True)
        p=result["payment"]; direct_order=result.get("direct_order"); u=result.get("user")
    except Exception as exc:
        error_id=record_runtime_error("payment_approval",exc,{"admin_id":c.from_user.id,"payment_id":pid})
        try: await c.answer(f"Approval failed safely. Ref: {error_id}",show_alert=True)
        except Exception: pass
        return
    await asyncio.to_thread(admin_log,c.from_user.id,"credit_payment",f"payment #{pid}")
    await c.answer("Approved")
    if p.get("order_id"):
        o,prod,delivered_code,status,u,prod_name=direct_order
        try:
            if status=="pending":
                await asyncio.to_thread(
                    record_order_event,o["id"],"payment_verified","pending",
                    "Payment verified; order moved to manual processing.",c.from_user.id)
            await asyncio.to_thread(sync_loyalty_profile,o["user_id"])
        except Exception as exc:
            record_runtime_error("phase3_payment_approval_events",exc,{"order_id":o["id"]})
        provider_result=None
        if status=="pending":
            try:
                provider_result=await asyncio.to_thread(auto_topup_try_fulfill_order,o["id"])
                if provider_result.get("status")=="success": status="completed"
            except Exception as exc:
                record_runtime_error("direct_payment_auto_topup_hook",exc,{"order_id":o["id"],"payment_id":pid})
        await c.message.edit_text(f"✅ Direct payment #{pid} approved. Order #{o['id']} " + ("completed by Auto Top-Up." if status=="completed" and provider_result else ("completed." if status=="completed" else "sent to manual delivery.")))
        if status=="completed":
            delivery=(f"🎁 <b>Your Code</b>\n<code>{html.escape(delivered_code or '')}</code>" if delivered_code else "⚡ <b>Auto Top-Up confirmed by provider.</b>")
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
                    if is_secure_login_support_product(prod):
                        direct_admin_text = (
                            f"🎮 <b>Direct Payment Secure Login Order #{o['id']}</b>\n\n"
                            f"👤 User: <code>{u['tg_id']}</code>\n"
                            f"🎮 Product: <b>{html.escape(prod_name['name'] if prod_name else 'Product')}</b>\n"
                            f"💰 Paid: <b>{fmt_money(o['total'])}</b>\n"
                            f"💳 Method: <b>{html.escape(p['method'])}</b>\n"
                            f"🧾 TrxID: <code>{html.escape(p['trx_id'] or '')}</code>\n"
                            f"🔐 Bot credentials: <b>Not collected</b>\n\n"
                            "📩 Buyer should contact support with the Order ID. Complete delivery after support processing."
                        )
                    else:
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
                    if has_credential and not is_secure_login_support_product(prod):
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
            if is_secure_login_support_product(prod):
                await notify_user(
                    c.bot,u["tg_id"],
                    f"✅ <b>Order #{o['id']} Payment Verified</b>\n\n"
                    f"🎮 Product: <b>{html.escape(prod_name['name'] if prod_name else 'Product')}</b>\n"
                    f"💰 Paid: <b>{fmt_money(o['total'])}</b>\n\n"
                    f"📩 <b>NEXT STEP:</b> Contact support and send Order ID <b>#{o['id']}</b>.\n"
                    f"🔒 No password was collected by the bot.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="💬 Contact Support",callback_data=f"order_support:{o['id']}")],
                        [InlineKeyboardButton(text="📦 Track Order",callback_data=f"order_track:{o['id']}")]
                    ])
                )
            else:
                await notify_user(c.bot,u["tg_id"],f"⏳ <b>Order #{o['id']} Payment Verified</b>\n\nYour payment is verified. The order is now waiting for manual delivery.")
    else:
        await c.message.edit_text(f"✅ Payment #{pid} credited.")
        await notify_user(c.bot,u["tg_id"],f"💰 <b>Balance Added</b>\n\nPayment: #{pid}\nAmount: <b>{fmt_money(p['amount'])}</b>\n\nYour wallet is ready for your next purchase. 🛒")

def _payment_reject_tx(pid:int, admin_id:int):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM payments WHERE id=%s FOR UPDATE",(pid,)); p=cur.fetchone()
                if not p or p["status"]!="pending": return {"ok":False,"message":"Already processed."}
                if p.get("order_id"):
                    cur.execute("SELECT * FROM orders WHERE id=%s FOR UPDATE",(p["order_id"],)); order=cur.fetchone()
                    if not order: raise RuntimeError("Linked order is missing; rejection aborted.")
                    if order["status"] != "awaiting_payment": raise RuntimeError(f"Linked order state is {order['status']}; rejection aborted to prevent payment/order mismatch.")
                    release_direct_order_reservation(cur,order,conn)
                    cur.execute("UPDATE orders SET status='cancelled',account_password='',processed_at=NOW(),updated_at=NOW() WHERE id=%s AND status='awaiting_payment'",(order["id"],))
                    if cur.rowcount != 1: raise RuntimeError("Linked order state changed during rejection.")
                cur.execute("UPDATE payments SET status='rejected',updated_at=NOW() WHERE id=%s AND status='pending'",(pid,))
                if cur.rowcount != 1: raise RuntimeError("Payment status changed during rejection.")
                record_payment_audit(cur,pid,admin_id,"rejected","pending","rejected",p["amount"],p["method"],p["trx_id"],"Admin rejected payment; any pending stock reservation was released")
                cur.execute("SELECT tg_id FROM users WHERE id=%s",(p["user_id"],)); u=cur.fetchone()
                return {"ok":True,"payment":p,"user":u}

@router.callback_query(F.data.startswith("pay_reject:"))
async def payment_reject(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    pid=int(c.data.split(":")[1])
    try:
        result=await asyncio.to_thread(_payment_reject_tx,pid,c.from_user.id)
        if not result.get("ok"): return await c.answer(result.get("message","Already processed."),show_alert=True)
        p=result["payment"]; u=result["user"]
    except Exception as exc:
        error_id=record_runtime_error("payment_rejection",exc,{"admin_id":c.from_user.id,"payment_id":pid})
        try: await c.answer(f"Rejection failed safely. Ref: {error_id}",show_alert=True)
        except Exception: pass
        return
    await asyncio.to_thread(admin_log,c.from_user.id,"reject_payment",f"payment #{pid}")
    if p.get("order_id"):
        try:
            await asyncio.to_thread(record_order_event,p["order_id"],"payment_rejected","cancelled","Payment was rejected; reserved stock was released.",c.from_user.id)
        except Exception as exc:
            record_runtime_error("phase3_payment_reject_event",exc,{"order_id":p.get("order_id")})
    await c.answer("Rejected")
    await c.message.edit_text(f"❌ Payment #{pid} rejected. Any reserved stock was released.")
    await notify_user(c.bot,u["tg_id"],f"❌ <b>Payment Rejected</b>\n\nPayment: #{pid}\nAmount: <b>{fmt_money(p['amount'])}</b>\n\nYour payment could not be verified. Any reserved stock was released. You can try the purchase again or contact support if you believe this is an error.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔁 Buy Again",callback_data="home:shop")],[InlineKeyboardButton(text=setting("button_support","🆘 Support"),callback_data="home:support"),InlineKeyboardButton(text=setting("button_orders","📦 My Orders"),callback_data="home:orders")]]))



def crm_customer_segment(row):
    completed=int(row.get("completed_orders") or 0)
    spend=float(row.get("lifetime_value") or 0)
    days=row.get("days_since_order")
    if spend >= 10000: return "💎 High Value"
    if completed >= 3 and (days is None or days <= 60): return "🔁 Repeat"
    if completed >= 1 and days is not None and days >= 60: return "🌙 Inactive"
    if completed == 1: return "🛍 One-time"
    return "🌱 Prospect"


def crm_customer_snapshot(user_id):
    row=db_execute("""SELECT u.id,u.tg_id,u.username,u.name,u.balance,u.blocked,u.created_at,u.updated_at,
        COUNT(o.id) FILTER (WHERE o.status='completed') AS completed_orders,
        COUNT(o.id) AS total_orders,
        COALESCE(SUM(o.total) FILTER (WHERE o.status='completed'),0) AS lifetime_value,
        MAX(o.created_at) FILTER (WHERE o.status='completed') AS last_order_at,
        CASE WHEN MAX(o.created_at) FILTER (WHERE o.status='completed') IS NULL THEN NULL
             ELSE EXTRACT(DAY FROM NOW()-MAX(o.created_at) FILTER (WHERE o.status='completed'))::INT END AS days_since_order,
        COALESCE((SELECT COUNT(*) FROM payment_support_cases psc WHERE psc.user_id=u.id AND psc.status='open'),0) AS open_payment_cases,
        COALESCE((SELECT COUNT(*) FROM customer_support_tickets st WHERE st.user_id=u.id AND st.status='open'),0) AS open_support_tickets,
        COALESCE((SELECT COUNT(*) FROM cart_items ci WHERE ci.user_id=u.id),0) AS cart_items
      FROM users u LEFT JOIN orders o ON o.user_id=u.id
      WHERE u.id=%s GROUP BY u.id""",(int(user_id),),"one")
    if not row: return None
    row=dict(row)
    row["segment"]=crm_customer_segment(row)
    row["notes"]=db_execute("SELECT id,note,created_by,created_at FROM crm_customer_notes WHERE user_id=%s ORDER BY id DESC LIMIT 5",(int(user_id),),"all") or []
    row["followups"]=db_execute("SELECT id,message,status,created_at,queued_at FROM crm_followups WHERE user_id=%s ORDER BY id DESC LIMIT 5",(int(user_id),),"all") or []
    row["top_products"]=db_execute("""SELECT p.name,COUNT(*) AS c,COALESCE(SUM(o.total),0) AS spend
       FROM orders o JOIN products p ON p.id=o.product_id
       WHERE o.user_id=%s AND o.status='completed' GROUP BY p.id,p.name ORDER BY c DESC,spend DESC LIMIT 3""",(int(user_id),),"all") or []
    return row


def crm_retention_snapshot(limit=20):
    return db_execute("""WITH stats AS (
      SELECT u.id,u.tg_id,u.name,u.username,u.blocked,u.created_at,
        COUNT(o.id) FILTER (WHERE o.status='completed') AS completed_orders,
        COALESCE(SUM(o.total) FILTER (WHERE o.status='completed'),0) AS lifetime_value,
        MAX(o.created_at) FILTER (WHERE o.status='completed') AS last_order_at,
        CASE WHEN MAX(o.created_at) FILTER (WHERE o.status='completed') IS NULL THEN NULL
             ELSE EXTRACT(DAY FROM NOW()-MAX(o.created_at) FILTER (WHERE o.status='completed'))::INT END AS days_since_order,
        COALESCE((SELECT COUNT(*) FROM cart_items ci WHERE ci.user_id=u.id),0) AS cart_items
      FROM users u LEFT JOIN orders o ON o.user_id=u.id GROUP BY u.id
    ) SELECT * FROM stats
      WHERE blocked=0 AND (
        (completed_orders>=1 AND days_since_order>=45) OR
        (completed_orders=0 AND created_at<=NOW()-INTERVAL '14 days') OR
        cart_items>0)
      ORDER BY
        CASE WHEN lifetime_value>=10000 AND days_since_order>=45 THEN 0
             WHEN completed_orders>=3 AND days_since_order>=45 THEN 1
             WHEN cart_items>0 THEN 2 ELSE 3 END,
        lifetime_value DESC, id DESC LIMIT %s""",(max(1,min(100,int(limit))),),"all") or []


def crm_search_customers(query,limit=20):
    q=(query or "").strip()
    if not q: return []
    if q.isdigit():
        return db_execute("SELECT id,tg_id,name,username,balance,blocked FROM users WHERE id=%s OR tg_id=%s ORDER BY id DESC LIMIT %s",(int(q),int(q),limit),"all") or []
    like=f"%{q}%"
    return db_execute("SELECT id,tg_id,name,username,balance,blocked FROM users WHERE COALESCE(name,'') ILIKE %s OR COALESCE(username,'') ILIKE %s ORDER BY id DESC LIMIT %s",(like,like,limit),"all") or []


def crm_retention_message(row):
    segment=crm_customer_segment(row)
    name=(row.get("name") or "there").split()[0]
    if "High Value" in segment or "Repeat" in segment:
        return f"Hi {name}! 👋 We miss you at {shop_name()}. We’ve got fresh offers available—open the shop anytime to see what’s new."
    if int(row.get("cart_items") or 0)>0:
        return f"Hi {name}! 👋 You still have item(s) waiting in your cart at {shop_name()}. Come back anytime to finish your order."
    if int(row.get("completed_orders") or 0)==0:
        return f"Hi {name}! 👋 Welcome back to {shop_name()}. Browse the latest products and offers whenever you’re ready."
    return f"Hi {name}! 👋 It’s been a while since your last order at {shop_name()}. Check the latest products and offers whenever you’re ready."


def crm_stage_followup(user_id,admin_id,message):
    message=(message or "").strip()
    if not message or len(message)>3000: raise ValueError("Message must be 1–3000 characters.")
    user=db_execute("SELECT id FROM users WHERE id=%s AND blocked=0",(int(user_id),),"one")
    if not user: raise ValueError("Customer not found or blocked.")
    row=db_insert_returning("INSERT INTO crm_followups(user_id,message,status,created_by) VALUES(%s,%s,'draft',%s) RETURNING id",(int(user_id),message,int(admin_id)))
    admin_log(admin_id,"crm_followup_staged",f"followup={row['id']} user={user_id}")
    return int(row["id"])


def crm_queue_followup(followup_id,admin_id):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT f.*,u.tg_id,u.blocked FROM crm_followups f JOIN users u ON u.id=f.user_id WHERE f.id=%s FOR UPDATE",(int(followup_id),))
            row=cur.fetchone()
            if not row: return {"ok":False,"reason":"missing"}
            if row["status"]!="draft": return {"ok":False,"reason":"already_decided","status":row["status"]}
            if row["blocked"]: return {"ok":False,"reason":"blocked"}
            cur.execute("INSERT INTO notification_queue(tg_id,text,buttons_json) VALUES(%s,%s,%s)",(row["tg_id"],row["message"],json.dumps([],ensure_ascii=False)))
            cur.execute("UPDATE crm_followups SET status='queued',queued_at=NOW() WHERE id=%s AND status='draft'",(int(followup_id),))
    admin_log(admin_id,"crm_followup_queued",f"followup={followup_id} user={row['user_id']}")
    return {"ok":True,"user_id":int(row["user_id"])}


def crm_customer_timeline_snapshot(user_id,limit=40):
    """Read-only unified customer history across orders, support, CRM notes and retention."""
    uid=int(user_id); lim=max(10,min(80,int(limit)))
    user=db_execute("SELECT id,tg_id,name,username FROM users WHERE id=%s",(uid,),"one")
    if not user: return None,[]
    rows=db_execute("""
      SELECT event_time,event_type,title,detail,ref_id,status FROM (
        SELECT o.created_at AS event_time,'order'::text AS event_type,
               ('Order #'||o.id)::text AS title,
               (COALESCE(p.name,'Product')||' • '||COALESCE(o.payment_mode,'')||' • '||COALESCE(o.total,0)::text)::text AS detail,
               o.id::bigint AS ref_id,COALESCE(o.status,'')::text AS status
          FROM orders o LEFT JOIN products p ON p.id=o.product_id WHERE o.user_id=%s
        UNION ALL
        SELECT oe.created_at,'order_event',('Order #'||oe.order_id||' • '||COALESCE(oe.event_type,'event')),
               COALESCE(oe.message,''),oe.order_id,COALESCE(oe.status,'')
          FROM order_events oe JOIN orders o ON o.id=oe.order_id WHERE o.user_id=%s
        UNION ALL
        SELECT st.created_at,'support_ticket',('Support Ticket #'||st.id||' • '||COALESCE(st.category,'')),
               LEFT(COALESCE(st.message,''),500),st.id,COALESCE(st.status,'')
          FROM customer_support_tickets st WHERE st.user_id=%s
        UNION ALL
        SELECT pc.created_at,'payment_case',('Payment Case #'||pc.id||' • Payment #'||pc.payment_id),
               LEFT(CASE WHEN COALESCE(pc.admin_note,'')<>'' THEN pc.admin_note ELSE COALESCE(pc.reason,'') END,500),pc.id,COALESCE(pc.status,'')
          FROM payment_support_cases pc WHERE pc.user_id=%s
        UNION ALL
        SELECT n.created_at,'crm_note',('CRM Note #'||n.id),LEFT(COALESCE(n.note,''),500),n.id,'note'
          FROM crm_customer_notes n WHERE n.user_id=%s
        UNION ALL
        SELECT f.created_at,'followup',('Retention Follow-up #'||f.id),LEFT(COALESCE(f.message,''),500),f.id,COALESCE(f.status,'')
          FROM crm_followups f WHERE f.user_id=%s
      ) t ORDER BY event_time DESC NULLS LAST LIMIT %s
    """,(uid,uid,uid,uid,uid,uid,lim),"all") or []
    return user,rows


def render_crm_customer_timeline(user,rows):
    if not user: return "🧭 Customer timeline not found."
    icons={"order":"📦","order_event":"🧾","support_ticket":"🎧","payment_case":"💳","crm_note":"📝","followup":"📨"}
    lines=[f"🧭 <b>Customer Timeline #{user['id']}</b>",f"👤 {html.escape(user.get('name') or 'User')} • <code>{user['tg_id']}</code>",""]
    if not rows: lines.append("No customer activity recorded yet.")
    for r in rows:
        icon=icons.get(str(r.get('event_type') or ''),"•")
        when=r.get('event_time'); when_text=when.strftime('%Y-%m-%d %H:%M') if hasattr(when,'strftime') else str(when or '')
        title=html.escape(str(r.get('title') or 'Event')); status=html.escape(str(r.get('status') or '').replace('_',' ').title())
        detail=html.escape(str(r.get('detail') or '')[:260])
        line=f"{icon} <b>{title}</b> • {status}\n<code>{when_text}</code>"
        if detail: line+=f"\n{detail}"
        lines.append(line)
    return "\n\n".join(lines)


@router.callback_query(F.data.startswith("crm:timeline:"))
async def admin_crm_timeline(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    uid=int(c.data.rsplit(":",1)[1]); user,rows=await asyncio.to_thread(crm_customer_timeline_snapshot,uid,50)
    if not user: return await c.answer("Customer not found.",show_alert=True)
    kb=[]; seen=set()
    for r in rows[:20]:
        typ=str(r.get('event_type') or ''); rid=int(r.get('ref_id') or 0); key=(typ,rid)
        if key in seen: continue
        seen.add(key)
        if typ in {'order','order_event'} and rid: kb.append([InlineKeyboardButton(text=f"📦 Open Order #{rid}",callback_data=f"admin:order_track:{rid}")])
        elif typ=='support_ticket' and rid: kb.append([InlineKeyboardButton(text=f"🎧 Open Ticket #{rid}",callback_data=f"customer_ticket:{rid}")])
        elif typ=='payment_case' and rid: kb.append([InlineKeyboardButton(text=f"💳 Open Payment Case #{rid}",callback_data=f"support_case:{rid}")])
        if len(kb)>=5: break
    kb.append([InlineKeyboardButton(text="🔄 Refresh",callback_data=f"crm:timeline:{uid}"),InlineKeyboardButton(text="⬅️ CRM Profile",callback_data=f"crm:user:{uid}")])
    await c.answer(); await c.message.edit_text(render_crm_customer_timeline(user,rows),reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


def render_crm_customer(row):
    if not row: return "🧩 Customer not found."
    last=row.get("last_order_at")
    last_text="Never" if not last else f"{last:%Y-%m-%d} ({row.get('days_since_order',0)}d ago)"
    tops="\n".join(f"• {html.escape(r['name'])} ×{r['c']} • {fmt_money(r['spend'])}" for r in row.get("top_products") or []) or "• No completed purchases"
    notes="\n".join(f"• {html.escape(str(n['note'])[:180])}" for n in row.get("notes") or []) or "• No notes"
    return (f"🧩 <b>Customer CRM #{row['id']}</b>\n\n"
            f"👤 <b>{html.escape(row.get('name') or 'User')}</b> • @{html.escape(row.get('username') or '-')}\n"
            f"🆔 <code>{row['tg_id']}</code>\n"
            f"🏷 Segment: <b>{html.escape(row['segment'])}</b>\n"
            f"💰 LTV: <b>{fmt_money(row['lifetime_value'])}</b> • {row['completed_orders']} completed\n"
            f"🧾 Total orders: <b>{row['total_orders']}</b> • Last: <b>{last_text}</b>\n"
            f"🛒 Cart items: <b>{row['cart_items']}</b>\n"
            f"🎧 Open support: <b>{int(row['open_support_tickets'])+int(row['open_payment_cases'])}</b>\n"
            f"👛 Wallet: <b>{fmt_money(row['balance'])}</b> • {'🚫 Blocked' if row['blocked'] else '🟢 Active'}\n\n"
            f"🎮 <b>Top products</b>\n{tops}\n\n"
            f"📝 <b>Recent notes</b>\n{notes}")


@router.callback_query(F.data=="admin:crm")
async def admin_crm(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=await asyncio.to_thread(crm_retention_snapshot,10)
    lines=["🧩 <b>Customer CRM + Retention</b>","", "Priority customers needing attention:"]
    if rows:
        for r in rows[:10]:
            seg=crm_customer_segment(dict(r))
            days=r.get("days_since_order")
            age="never ordered" if days is None else f"{days}d since order"
            lines.append(f"• <b>{html.escape(r.get('name') or str(r['tg_id']))[:24]}</b> — {html.escape(seg)} • {fmt_money(r['lifetime_value'])} • {age}")
    else: lines.append("• No priority retention candidates right now.")
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔎 Search Customer",callback_data="admin:crm_search")],
        [InlineKeyboardButton(text="🌙 Retention Queue",callback_data="admin:crm_retention")],
        [InlineKeyboardButton(text="🎯 Segments",callback_data="admin:segments")],
        [InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]])
    await c.answer(); await c.message.edit_text("\n".join(lines),reply_markup=kb)


@router.callback_query(F.data=="admin:crm_search")
async def admin_crm_search_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await state.set_state(AdminState.crm_search)
    await c.answer(); await c.message.answer("🔎 Send customer name, username, Telegram ID, or internal user ID.")


@router.message(AdminState.crm_search)
async def admin_crm_search_receive(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): await state.clear(); return
    rows=await asyncio.to_thread(crm_search_customers,m.text or "",20)
    await state.clear()
    if not rows: return await m.answer("No matching customers found.",reply_markup=admin_menu())
    kb=[[InlineKeyboardButton(text=f"{'🚫' if r['blocked'] else '🟢'} {(r['name'] or r['username'] or str(r['tg_id']))[:28]}",callback_data=f"crm:user:{r['id']}")] for r in rows]
    kb.append([InlineKeyboardButton(text="⬅️ CRM",callback_data="admin:crm")])
    await m.answer("🔎 <b>CRM Search Results</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("crm:user:"))
async def admin_crm_customer(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    uid=int(c.data.rsplit(":",1)[1]); row=await asyncio.to_thread(crm_customer_snapshot,uid)
    if not row: return await c.answer("Customer not found.",show_alert=True)
    kb=[[InlineKeyboardButton(text="📝 Add Note",callback_data=f"crm:note:{uid}"),InlineKeyboardButton(text="💬 Follow-up",callback_data=f"crm:follow:{uid}")],
        [InlineKeyboardButton(text="🧭 Unified Timeline",callback_data=f"crm:timeline:{uid}")],
        [InlineKeyboardButton(text="⬅️ CRM",callback_data="admin:crm")]]
    await c.answer(); await c.message.edit_text(render_crm_customer(row),reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("crm:note:"))
async def admin_crm_note_start(c:CallbackQuery,state:FSMContext):
    if not admin_can(c.from_user.id,"sensitive"):
        return await c.answer("Denied",show_alert=True)
    uid=int(c.data.rsplit(":",1)[1])
    await state.update_data(crm_user_id=uid); await state.set_state(AdminState.crm_note)
    await c.answer(); await c.message.answer("📝 Send an internal CRM note (max 1000 characters). This is not sent to the customer.")


@router.message(AdminState.crm_note)
async def admin_crm_note_receive(m:Message,state:FSMContext):
    if not admin_can(m.from_user.id,"sensitive"): await state.clear(); return
    d=await state.get_data(); uid=int(d.get("crm_user_id") or 0); note=(m.text or "").strip()
    if not uid or not note or len(note)>1000: return await m.answer("❌ Note must be 1–1000 characters.")
    await adb_execute("INSERT INTO crm_customer_notes(user_id,note,created_by) VALUES(%s,%s,%s)",(uid,note,m.from_user.id))
    await aadmin_log(m.from_user.id,"crm_note_added",f"user={uid}")
    await state.clear(); await m.answer("✅ CRM note saved.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🧩 Open Customer",callback_data=f"crm:user:{uid}")]]))


@router.callback_query(F.data.startswith("crm:follow:"))
async def admin_crm_followup_start(c:CallbackQuery,state:FSMContext):
    if not admin_can(c.from_user.id,"sensitive"):
        return await c.answer("Denied",show_alert=True)
    uid=int(c.data.rsplit(":",1)[1]); row=await asyncio.to_thread(crm_customer_snapshot,uid)
    if not row or row.get("blocked"): return await c.answer("Customer unavailable or blocked.",show_alert=True)
    suggested=crm_retention_message(row)
    await state.update_data(crm_user_id=uid); await state.set_state(AdminState.crm_followup)
    await c.answer(); await c.message.answer(f"💬 <b>Draft follow-up</b>\n\nSuggested:\n{html.escape(suggested)}\n\nSend your final message. It will be staged for approval, not sent immediately.")


@router.message(AdminState.crm_followup)
async def admin_crm_followup_receive(m:Message,state:FSMContext):
    if not admin_can(m.from_user.id,"sensitive"): await state.clear(); return
    d=await state.get_data(); uid=int(d.get("crm_user_id") or 0); body=(m.text or "").strip()
    if not uid or not body or len(body)>3000: return await m.answer("❌ Message must be 1–3000 characters.")
    try: fid=await asyncio.to_thread(crm_stage_followup,uid,m.from_user.id,body)
    except Exception as exc:
        await state.clear(); return await m.answer(f"❌ Could not stage follow-up: {html.escape(str(exc))}")
    await state.clear()
    await m.answer(f"✅ Follow-up #{fid} staged. Nothing has been sent yet.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Approve & Queue",callback_data=f"crm:queue:{fid}"),InlineKeyboardButton(text="❌ Cancel",callback_data=f"crm:cancel:{fid}")],[InlineKeyboardButton(text="🧩 CRM",callback_data="admin:crm")]]))


@router.callback_query(F.data.startswith("crm:queue:"))
async def admin_crm_followup_queue(c:CallbackQuery):
    if not admin_can(c.from_user.id,"sensitive"): return await c.answer("Denied",show_alert=True)
    fid=int(c.data.rsplit(":",1)[1]); result=await asyncio.to_thread(crm_queue_followup,fid,c.from_user.id)
    if not result.get("ok"): return await c.answer(f"Cannot queue: {result.get('reason','unknown')}",show_alert=True)
    await c.answer("Queued")
    await c.message.edit_text("✅ <b>CRM Follow-up Queued</b>\n\nThe approved message is now in the notification queue.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🧩 CRM",callback_data="admin:crm")]]))


@router.callback_query(F.data.startswith("crm:cancel:"))
async def admin_crm_followup_cancel(c:CallbackQuery):
    if not admin_can(c.from_user.id,"sensitive"): return await c.answer("Denied",show_alert=True)
    fid=int(c.data.rsplit(":",1)[1])
    row=await adb_execute("UPDATE crm_followups SET status='cancelled' WHERE id=%s AND status='draft' RETURNING user_id",(fid,),"one")
    if not row: return await c.answer("Follow-up already decided or missing.",show_alert=True)
    await aadmin_log(c.from_user.id,"crm_followup_cancelled",f"followup={fid}")
    await c.answer("Cancelled"); await c.message.edit_text("❌ CRM follow-up cancelled.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🧩 CRM",callback_data="admin:crm")]]))


@router.callback_query(F.data=="admin:crm_retention")
async def admin_crm_retention(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=await asyncio.to_thread(crm_retention_snapshot,30)
    if not rows: return await c.answer("No retention candidates right now.",show_alert=True)
    kb=[]
    for r in rows[:20]:
        seg=crm_customer_segment(dict(r)); days=r.get("days_since_order")
        label=f"{(r.get('name') or str(r['tg_id']))[:18]} • {int(days) if days is not None else '—'}d • {float(r['lifetime_value']):.0f}"
        kb.append([InlineKeyboardButton(text=label,callback_data=f"crm:user:{r['id']}")])
    kb.append([InlineKeyboardButton(text="⬅️ CRM",callback_data="admin:crm")])
    await c.answer(); await c.message.edit_text("🌙 <b>Retention Queue</b>\n\nPrioritized by customer value, inactivity and cart intent. No message is sent automatically.",reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data=="admin:users")
async def admin_users(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=await adb_execute("SELECT * FROM users ORDER BY id DESC LIMIT 20",fetch="all"); buttons=[[InlineKeyboardButton(text=f"{'🚫' if u['blocked'] else '🟢'} {(u['name'] or 'User')[:18]} • {float(u['balance']):.0f}",callback_data=f"user:{u['id']}")] for u in rows]; buttons.append([InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]); await c.answer(); await c.message.edit_text("👥 <b>User Management</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("user:"))
async def user_detail(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    uid=int(c.data.split(":")[1]); u=await adb_execute("SELECT * FROM users WHERE id=%s",(uid,),"one")
    if not u: return await c.answer("Not found.",show_alert=True)
    orders=(await adb_execute("SELECT COUNT(*) AS c FROM orders WHERE user_id=%s",(uid,),"one"))["c"]
    await c.answer(); await c.message.edit_text(f"👤 <b>{html.escape(u['name'] or 'User')}</b>\n\nTelegram ID: <code>{u['tg_id']}</code>\nUsername: @{html.escape(u['username'] or '-')}\nBalance: <b>{fmt_money(u['balance'])}</b>\nOrders: <b>{orders}</b>\nStatus: {'🚫 Blocked' if u['blocked'] else '🟢 Active'}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_unblock", "🔓 Unblock") if u['blocked'] else setting("admin_block", "🚫 Block"),callback_data=f"user_toggle:{uid}")],[InlineKeyboardButton(text="🧩 Open CRM",callback_data=f"crm:user:{uid}")],[InlineKeyboardButton(text=setting("admin_users_back", "⬅️ Users"),callback_data="admin:users")]]))

@router.callback_query(F.data.startswith("user_toggle:"))
async def user_toggle(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    uid=int(c.data.split(":")[1]); row=await adb_execute("SELECT blocked FROM users WHERE id=%s",(uid,),"one")
    if not row: return await c.answer("Not found.",show_alert=True)
    new=0 if row["blocked"] else 1; await adb_execute("UPDATE users SET blocked=%s,updated_at=NOW() WHERE id=%s",(new,uid)); await aadmin_log(c.from_user.id,"toggle_user",f"user #{uid} -> {new}"); await c.answer("Updated"); await user_detail(c)

@router.callback_query(F.data=="admin:balance")
async def admin_balance_start(c:CallbackQuery,state:FSMContext):
    if not admin_can(c.from_user.id, "financial"):
        if is_admin(c.from_user.id): await asyncio.to_thread(security_log,"readonly_admin_financial_denied",c.from_user.id,None,"admin_balance_start")
        return await c.answer("Denied",show_alert=True)
    await c.answer(); await state.set_state(AdminState.balance); await c.message.answer("💰 <b>Manual Balance</b>\n\nSend:\n<code>TelegramID | amount | add/deduct | note</code>")

@router.message(AdminState.balance)
async def admin_balance(m:Message,state:FSMContext):
    if not admin_can(m.from_user.id, "financial"):
        if is_admin(m.from_user.id): await asyncio.to_thread(security_log,"readonly_admin_financial_denied",m.from_user.id,None,"admin_balance")
        await state.clear(); return

    parts=[x.strip() for x in (m.text or "").split("|",3)]
    if len(parts)!=4: return await m.answer("❌ Invalid format.")
    try: tg_id=int(parts[0]); amount=float(parts[1])
    except ValueError: return await m.answer("❌ Invalid ID or amount.")
    action=parts[2].lower(); note=parts[3]
    if amount<=0 or action not in {"add","deduct"}: return await m.answer("❌ Invalid amount/action.")
    result=await asyncio.to_thread(_admin_balance_tx,tg_id,amount,action,note)
    if not result.get("ok"):
        await state.clear()
        if result.get("reason")=="not_found": return await m.answer("❌ User not found.")
        if result.get("reason")=="low_balance": return await m.answer("❌ User balance is too low.")
        return await m.answer("❌ Balance update failed safely.")
    await aadmin_log(m.from_user.id,f"balance_{action}",f"user {tg_id}, amount {amount}")
    await state.clear(); await m.answer("✅ Balance updated.",reply_markup=admin_menu())

@router.callback_query(F.data=="admin:broadcast")
async def admin_broadcast_start(c:CallbackQuery,state:FSMContext):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    await c.answer(); await state.set_state(AdminState.broadcast); await c.message.answer("📢 Send the broadcast message. HTML supported.")

@router.message(AdminState.broadcast)
async def admin_broadcast(m:Message,state:FSMContext):
    if not is_admin(m.from_user.id): return
    text=m.text or ""; users=await adb_execute("SELECT tg_id FROM users WHERE blocked=0",fetch="all"); sent=failed=0
    for u in users:
        try: await m.bot.send_message(u["tg_id"],text); sent+=1; await asyncio.sleep(0.04)
        except Exception: failed+=1
    await aadmin_log(m.from_user.id,"broadcast",f"sent={sent},failed={failed}"); await state.clear(); await m.answer(f"📢 Broadcast finished.\n✅ Sent: {sent}\n❌ Failed: {failed}",reply_markup=admin_menu())



def store_ops_health_sample(snapshot):
    db_execute(
      """INSERT INTO ops_health_history
         (instance_id,role,health_score,db_latency_ms,queue_pending,queue_failed,errors15)
         VALUES(%s,%s,%s,%s,%s,%s,%s)""",
      (
        INSTANCE_ID,
        str(snapshot.get("role","unknown"))[:40],
        int(snapshot.get("health_score") or 0),
        float(snapshot.get("db_latency_ms") or 0),
        int(snapshot.get("queue_pending") or 0),
        int(snapshot.get("queue_failed") or 0),
        int(snapshot.get("errors15") or 0),
      ),
    )


def ops_health_history_snapshot(hours=24):
    hours=max(1,min(168,int(hours)))
    return db_execute(
      f"""SELECT
            COUNT(*) AS samples,
            ROUND(AVG(health_score)::numeric,1) AS avg_health,
            MIN(health_score) AS min_health,
            ROUND(AVG(db_latency_ms)::numeric,1) AS avg_db_ms,
            MAX(db_latency_ms) AS max_db_ms,
            MAX(queue_pending) AS max_queue_pending,
            MAX(queue_failed) AS max_queue_failed,
            MAX(errors15) AS max_errors15
          FROM ops_health_history
          WHERE created_at>=NOW()-INTERVAL '{hours} hours'""",
      fetch="one",
    ) or {}



def railway_live_snapshot():
    """Compact production snapshot for Railway logs/admin; read-only except runtime timestamp update."""
    started = time.monotonic()
    row = db_execute("""SELECT
        (SELECT COUNT(*) FROM notification_queue WHERE status='pending') queue_pending,
        (SELECT COUNT(*) FROM notification_queue WHERE status='failed') queue_failed,
        (SELECT COUNT(*) FROM payments WHERE status='pending') pending_payments,
        (SELECT COUNT(*) FROM orders WHERE status='pending') pending_orders,
        (SELECT COUNT(*) FROM orders WHERE status='refund_pending') refund_pending,
        (SELECT COUNT(*) FROM error_events
           WHERE created_at>=NOW()-INTERVAL '15 minutes'
             AND resolved=FALSE
             AND COALESCE(severity,'error')<>'benign') errors15
    """, fetch="one") or {}
    db_latency_ms = round((time.monotonic() - started) * 1000.0, 2)
    runtime = runtime_state_snapshot()
    pool = DB_POOL.stats()
    return {
        "event": "railway_heartbeat",
        "app_version": APP_VERSION,
        "instance_id": INSTANCE_ID,
        "role": runtime.get("role", "unknown"),
        "health_score": int(runtime.get("health_score", 100) or 0),
        "health_status": runtime.get("health_status", "starting"),
        "db_latency_ms": db_latency_ms,
        "db_pool": {
            "created": pool.get("created", 0),
            "max": pool.get("max", 0),
            "idle": pool.get("idle", 0),
            "waits": pool.get("waits", 0),
        },
        "queue_pending": int(row.get("queue_pending") or 0),
        "queue_failed": int(row.get("queue_failed") or 0),
        "pending_payments": int(row.get("pending_payments") or 0),
        "pending_orders": int(row.get("pending_orders") or 0),
        "refund_pending": int(row.get("refund_pending") or 0),
        "errors15": int(row.get("errors15") or 0),
        "last_telegram_activity": runtime.get("last_telegram_activity"),
        "last_leader_heartbeat": runtime.get("last_leader_heartbeat"),
        "checked_at": now_text(),
    }


async def railway_live_monitor_loop():
    """Non-spammy Railway heartbeat log. Default: once every 5 minutes."""
    await asyncio.sleep(15)
    while True:
        try:
            snap = await asyncio.to_thread(railway_live_snapshot)
            await asyncio.to_thread(store_ops_health_sample,snap)
            runtime_state_update(
                last_railway_heartbeat=snap["checked_at"],
                last_db_latency_ms=snap["db_latency_ms"],
            )
            print(json.dumps(snap, ensure_ascii=False, sort_keys=True), flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            record_runtime_error("railway_live_monitor", exc, {"instance_id": INSTANCE_ID})
        await asyncio.sleep(RAILWAY_HEARTBEAT_SECONDS)


@router.callback_query(F.data=="admin:live_status")
async def admin_live_status(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    snap = await asyncio.to_thread(railway_live_snapshot)
    runtime_state_update(
        last_railway_heartbeat=snap["checked_at"],
        last_db_latency_ms=snap["db_latency_ms"],
    )
    last_tg = html.escape(str(snap.get("last_telegram_activity") or "n/a"))
    last_leader = html.escape(str(snap.get("last_leader_heartbeat") or "n/a"))
    text = (
        f"🚆 <b>Railway Live Status</b>\n\n"
        f"💚 Health: <b>{snap['health_score']}/100 • {html.escape(str(snap['health_status']).upper())}</b>\n"
        f"🌐 Role: <b>{html.escape(str(snap['role']).upper())}</b>\n"
        f"⚡ DB latency: <b>{snap['db_latency_ms']} ms</b>\n"
        f"🗄 DB pool: <b>{snap['db_pool']['created']}/{snap['db_pool']['max']}</b> • "
        f"idle {snap['db_pool']['idle']} • waits {snap['db_pool']['waits']}\n\n"
        f"🔔 Queue: <b>{snap['queue_pending']}</b> pending • <b>{snap['queue_failed']}</b> failed\n"
        f"💳 Pending payments: <b>{snap['pending_payments']}</b>\n"
        f"📦 Pending orders: <b>{snap['pending_orders']}</b>\n"
        f"💸 Refund pending: <b>{snap['refund_pending']}</b>\n"
        f"🚨 Errors 15m: <b>{snap['errors15']}</b>\n\n"
        f"🤖 Last Telegram activity: <code>{last_tg}</code>\n"
        f"👑 Last leader heartbeat: <code>{last_leader}</code>\n"
        f"🚆 Monitor heartbeat: <code>{html.escape(str(snap['checked_at']))}</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin:live_status"),
         InlineKeyboardButton(text="🩺 Diagnostics", callback_data="admin:diagnostics")],
        [InlineKeyboardButton(text="⬅️ Ops", callback_data="admin:ops")]
    ])
    await c.answer()
    await c.message.edit_text(text, reply_markup=kb)

def observability_snapshot():
    """Read-only health signals; never mutates financial state."""
    row = db_execute("""SELECT
        (SELECT COUNT(*) FROM notification_queue WHERE status='failed') failed_notifications,
        (SELECT COUNT(*) FROM notification_queue WHERE status='sending' AND claimed_at < NOW()-(%s * INTERVAL '1 minute')) stale_notification_leases,
        (SELECT COUNT(*) FROM notification_queue WHERE status='pending' AND next_attempt_at <= NOW()) due_notifications,
        (SELECT COALESCE(EXTRACT(EPOCH FROM (NOW()-MIN(created_at))),0) FROM notification_queue WHERE status='pending') oldest_pending_notification_seconds,
        (SELECT COUNT(*) FROM payments WHERE status='pending' AND created_at < NOW()-(%s * INTERVAL '1 minute')) aged_pending_payments,
        (SELECT COUNT(*) FROM orders WHERE status='refund_pending') refund_pending,
        (SELECT COUNT(*) FROM error_events
             WHERE created_at >= NOW()-INTERVAL '15 minutes'
               AND resolved=FALSE
               AND COALESCE(severity,'error')<>'benign') errors15
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


_TRANSIENT_NOTIFICATION_ERROR_HINTS=(
    "timeout", "timed out", "temporar", "connection", "network", "server error",
    "bad gateway", "gateway timeout", "service unavailable", "retry after",
    "too many requests", "connection reset", "remote protocol", "httpx", "aiohttp"
)
_PERMANENT_NOTIFICATION_ERROR_HINTS=(
    "bot was blocked", "user is deactivated", "chat not found", "forbidden",
    "bot can't initiate", "bot cannot initiate", "not enough rights"
)

def _notification_error_is_retryable(message):
    text=str(message or "").casefold()
    if not text:
        return False
    if any(x in text for x in _PERMANENT_NOTIFICATION_ERROR_HINTS):
        return False
    return any(x in text for x in _TRANSIENT_NOTIFICATION_ERROR_HINTS)

def requeue_retryable_failed_notifications(limit=None):
    """Requeue only clearly transient failed Telegram deliveries; never touches orders/payments."""
    limit=max(1,min(100,int(limit or SELF_HEAL_NOTIFICATION_BATCH)))
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id,last_error,attempts FROM notification_queue
                   WHERE status='failed' AND attempts < %s
                   ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED""",
                (SELF_HEAL_NOTIFICATION_MAX_ATTEMPTS,limit),
            )
            candidates=cur.fetchall() or []
            ids=[int(r["id"]) for r in candidates if _notification_error_is_retryable(r.get("last_error"))]
            if not ids:
                return 0
            cur.execute(
                """UPDATE notification_queue
                   SET status='pending',next_attempt_at=NOW()+INTERVAL '60 seconds',claimed_at=NULL,claimed_by=''
                   WHERE id = ANY(%s) AND status='failed'
                   RETURNING id""",
                (ids,),
            )
            return len(cur.fetchall() or [])

def self_heal_safe_operations():
    """Repair deterministic operational failures only; never approve/reject/refund/checkout money."""
    rows=db_execute("""UPDATE notification_queue
        SET status='pending',claimed_at=NULL,claimed_by='',next_attempt_at=NOW(),
            last_error=CASE WHEN last_error='' THEN 'Phase 5K stale lease self-heal' ELSE last_error END
        WHERE status='sending' AND claimed_at < NOW()-(%s * INTERVAL '1 minute')
        RETURNING id""",(WORKER_LEASE_MINUTES,),fetch="all") or []
    retried=requeue_retryable_failed_notifications()
    return {
        "notification_leases_requeued":len(rows),
        "transient_notifications_requeued":int(retried),
    }

def self_heal_guardian_snapshot():
    """Read-only signals for states that require human review rather than automatic financial mutation."""
    row=db_execute("""SELECT
        (SELECT COUNT(*) FROM notification_queue WHERE status='failed') AS failed_notifications,
        (SELECT COUNT(*) FROM orders WHERE status='refund_pending' AND updated_at < NOW()-(%s * INTERVAL '1 minute')) AS stale_refunds,
        (SELECT COUNT(*) FROM autotopup_orders WHERE status IN ('creating','processing','uncertain') AND updated_at < NOW()-(%s * INTERVAL '1 minute')) AS stale_provider
    """,(ORDER_RECOVERY_REFUND_MINUTES,ORDER_RECOVERY_PROVIDER_MINUTES),fetch="one") or {}
    backup=backup_health_snapshot()
    backup_stale=False
    backup_age_seconds=None
    last_at=str(backup.get("last_at") or "").strip()
    if AUTO_DB_BACKUP_HOURS>0 and last_at and last_at not in {"never","unknown"}:
        try:
            dt=datetime.strptime(last_at,"%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            backup_age_seconds=max(0.0,(datetime.now(timezone.utc)-dt).total_seconds())
            backup_stale=backup_age_seconds > AUTO_DB_BACKUP_HOURS*3600*SELF_HEAL_BACKUP_STALE_FACTOR
        except Exception:
            backup_stale=True
    elif AUTO_DB_BACKUP_HOURS>0:
        backup_stale=True
    return {**row,"backup":backup,"backup_stale":backup_stale,"backup_age_seconds":backup_age_seconds}

_SELF_HEAL_ALERT_STATE={"signature":None,"last":0.0}

async def self_heal_guardian_loop(bot):
    """Alert on unresolved operational states; no provider checkout or financial transition is replayed."""
    while True:
        try:
            healed=await asyncio.to_thread(self_heal_safe_operations)
            snap=await asyncio.to_thread(self_heal_guardian_snapshot)
            signature=(
                int(snap.get("failed_notifications") or 0),
                int(snap.get("stale_refunds") or 0),
                int(snap.get("stale_provider") or 0),
                bool(snap.get("backup_stale")),
            )
            now_m=time.monotonic()
            needs_alert=any(signature)
            if needs_alert and (signature!=_SELF_HEAL_ALERT_STATE.get("signature") or now_m-float(_SELF_HEAL_ALERT_STATE.get("last") or 0)>=SELF_HEAL_ALERT_COOLDOWN_SECONDS):
                msg=(
                    "🛟 <b>Phase 5K Recovery Guardian</b>\n\n"
                    f"🔔 Failed notifications: <b>{signature[0]}</b>\n"
                    f"💸 Stale refunds: <b>{signature[1]}</b>\n"
                    f"🌐 Stale provider states: <b>{signature[2]}</b>\n"
                    f"💾 Backup stale/unknown: <b>{'Yes' if signature[3] else 'No'}</b>\n"
                    f"♻️ Auto-healed leases: <b>{int(healed.get('notification_leases_requeued') or 0)}</b> • transient retries: <b>{int(healed.get('transient_notifications_requeued') or 0)}</b>\n\n"
                    "Financial/provider checkout state remains fail-closed; review Recovery Center for ambiguous states."
                )
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(admin_id,msg,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                            InlineKeyboardButton(text="🧭 Recovery Center",callback_data="admin:recovery"),
                            InlineKeyboardButton(text="🩺 Diagnostics",callback_data="admin:diagnostics")
                        ]]))
                    except Exception as exc:
                        record_runtime_error("self_heal_guardian_notify",exc,{"admin_id":admin_id})
                _SELF_HEAL_ALERT_STATE.update(signature=signature,last=now_m)
            elif not needs_alert:
                _SELF_HEAL_ALERT_STATE["signature"]=None
            runtime_state_update(last_self_heal=now_text(),last_self_heal_result=healed)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            record_runtime_error("self_heal_guardian_loop",exc,{"instance_id":INSTANCE_ID})
        await asyncio.sleep(max(60,OBSERVABILITY_INTERVAL_SECONDS))


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


def admin_ops_overview_snapshot():
    """Read-only consolidated production snapshot for admins. No financial/provider mutations."""
    live=railway_live_snapshot()
    obs=observability_snapshot()
    recovery=order_recovery_snapshot(1).get("summary",{})
    backup=backup_health_snapshot()
    cfg=auto_topup_config()
    provider=db_execute(
        """SELECT
          COUNT(*) FILTER (WHERE status IN ('creating','processing','uncertain')) AS unresolved,
          COUNT(*) FILTER (WHERE status='failed') AS failed,
          COUNT(*) FILTER (WHERE status='success') AS success,
          MAX(updated_at) AS last_update
        FROM autotopup_orders WHERE provider=%s""",
        (AUTO_TOPUP_PROVIDER,),"one") or {}
    errors=db_execute(
        """SELECT error_id,scope,error_type,severity,message,created_at
           FROM error_events
           WHERE resolved=FALSE AND COALESCE(severity,'error')<>'benign'
           ORDER BY CASE COALESCE(severity,'error')
             WHEN 'critical' THEN 1 WHEN 'error' THEN 2 WHEN 'warning' THEN 3 ELSE 4 END,
             created_at DESC LIMIT 5""",(),"all") or []
    runtime=runtime_state_snapshot()
    restarts=runtime.get("worker_restarts") or {}
    return {
        "live":live,"obs":obs,"recovery":recovery,"backup":backup,
        "provider":provider,"provider_configured":bool(cfg.get("api_key")),
        "provider_env":_auto_topup_env("BANGJEFF_ENV","production").lower(),
        "provider_name":cfg.get("provider") or AUTO_TOPUP_PROVIDER,
        "errors":errors,"worker_restarts":restarts,
        "bootstrap_complete":bool(runtime.get("bootstrap_complete")),
        "deployment_ok":bool(runtime.get("deployment_ok")),
        "recovery_complete":bool(runtime.get("recovery_complete",False)),
        "last_observability_check":runtime.get("last_observability_check"),
        "last_self_heal":runtime.get("last_self_heal"),
    }


def _ops_flag(ok):
    return "✅" if ok else "❌"


@router.callback_query(F.data=="admin:ops_overview")
async def admin_ops_overview(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    snap=await asyncio.to_thread(admin_ops_overview_snapshot)
    live=snap["live"]; obs=snap["obs"]; rec=snap["recovery"]; prv=snap["provider"]; backup=snap["backup"]
    restarts=snap.get("worker_restarts") or {}
    restart_text=", ".join(f"{html.escape(str(k))}:{int(v)}" for k,v in sorted(restarts.items())) or "none"
    error_lines=[]
    for r in snap.get("errors",[]):
        sev=str(r.get("severity") or "error").upper()
        error_lines.append(f"• <b>{html.escape(sev)}</b> {html.escape(str(r.get('scope') or '-'))}: {html.escape(str(r.get('message') or '')[:90])}")
    errors_text="\n".join(error_lines) or "• No unresolved actionable errors"
    provider_cfg=_ops_flag(snap.get("provider_configured"))
    backup_ok=str(backup.get("status") or "unknown").lower()=="ok"
    text=(
        "📡 <b>Phase 5H • Production Ops Overview</b>\n\n"
        f"{_ops_flag(snap.get('bootstrap_complete'))} Bootstrap • {_ops_flag(snap.get('deployment_ok'))} Deployment • {_ops_flag(snap.get('recovery_complete'))} Recovery\n"
        f"💚 Health: <b>{live['health_score']}/100 • {html.escape(str(live['health_status']).upper())}</b> • DB <b>{live['db_latency_ms']} ms</b>\n"
        f"🗄 Pool: <b>{live['db_pool']['created']}/{live['db_pool']['max']}</b> created • idle {live['db_pool']['idle']} • waits {live['db_pool']['waits']}\n"
        f"🤖 Worker restarts: <code>{restart_text}</code>\n\n"
        f"🌐 <b>Provider</b>\n"
        f"{provider_cfg} {html.escape(str(snap.get('provider_name')))} configured • env <b>{html.escape(str(snap.get('provider_env')).upper())}</b>\n"
        f"⏳ Unresolved: <b>{int(prv.get('unresolved') or 0)}</b> • ❌ Failed: <b>{int(prv.get('failed') or 0)}</b> • ✅ Success: <b>{int(prv.get('success') or 0)}</b>\n"
        f"🕒 Last provider update: <code>{html.escape(str(prv.get('last_update') or 'n/a'))}</code>\n\n"
        f"🧭 <b>Recovery / Queue</b>\n"
        f"Stale pending: <b>{int(rec.get('stale_pending') or 0)}</b> • stale provider: <b>{int(rec.get('stale_provider') or 0)}</b> • stale refunds: <b>{int(rec.get('stale_refunds') or 0)}</b>\n"
        f"🔔 Notifications: <b>{int(live.get('queue_pending') or 0)}</b> pending • <b>{int(live.get('queue_failed') or 0)}</b> failed • stale leases <b>{int(obs.get('stale_notification_leases') or 0)}</b>\n"
        f"💾 Backup: <b>{html.escape(str(backup.get('status') or 'unknown').upper())}</b> {_ops_flag(backup_ok)}\n\n"
        f"🚨 <b>Recent Actionable Errors</b>\n{errors_text}\n\n"
        f"🔎 Last observability: <code>{html.escape(str(snap.get('last_observability_check') or 'n/a'))}</code>\n"
        f"🛠 Last safe self-heal: <code>{html.escape(str(snap.get('last_self_heal') or 'n/a'))}</code>"
    )
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:ops_overview"),InlineKeyboardButton(text="🚆 Railway",callback_data="admin:live_status")],
        [InlineKeyboardButton(text="🧭 Recovery",callback_data="admin:recovery"),InlineKeyboardButton(text="🌐 Auto Top-Up",callback_data="admin:auto_topup")],
        [InlineKeyboardButton(text="🚨 Errors",callback_data="admin:ops_errors"),InlineKeyboardButton(text="🩺 Diagnostics",callback_data="admin:diagnostics")],
        [InlineKeyboardButton(text="💾 Backup",callback_data="admin:backup_health"),InlineKeyboardButton(text="⬅️ Ops",callback_data="admin:ops")],
    ])
    await c.answer()
    await c.message.edit_text(text,reply_markup=kb)


@router.callback_query(F.data=="admin:ops")
async def admin_ops_center(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    row = await asyncio.to_thread(db_execute, """SELECT
        (SELECT COUNT(*) FROM payments WHERE status='pending') pending_payments,
        (SELECT COUNT(*) FROM orders WHERE status='pending') pending_orders,
        (SELECT COUNT(*) FROM orders WHERE status='refund_pending') refund_pending,
        (SELECT COUNT(*) FROM notification_queue WHERE status='pending') queue_pending,
        (SELECT COUNT(*) FROM notification_queue WHERE status='failed') queue_failed,
        (SELECT COUNT(*) FROM error_events
             WHERE created_at>=NOW()-INTERVAL '24 hours'
               AND NOT (error_type='TelegramBadRequest' AND position('message is not modified' in lower(message)) > 0)) errors24,
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
        [InlineKeyboardButton(text="📡 Production Overview",callback_data="admin:ops_overview")],
        [InlineKeyboardButton(text="🚨 Recent Errors",callback_data="admin:ops_errors"),InlineKeyboardButton(text="🔄 Order Audit",callback_data="admin:ops_orders")],
        [InlineKeyboardButton(text="🩺 Ultra Diagnostics",callback_data="admin:diagnostics"),InlineKeyboardButton(text="🚆 Railway Live",callback_data="admin:live_status")],
        [InlineKeyboardButton(text="🧪 Deployment Check",callback_data="admin:deploy_check"),InlineKeyboardButton(text="💾 Backup Health",callback_data="admin:backup_health")],
        [InlineKeyboardButton(text="🗄 Run Safe Archive",callback_data="admin:ops_archive")],
        [InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:ops"),InlineKeyboardButton(text="⬅️ Admin",callback_data="admin:dashboard")]
    ])
    await c.answer(); await c.message.edit_text(text,reply_markup=kb)


@router.callback_query(F.data=="admin:ops_errors")
async def admin_ops_errors(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    rows = await asyncio.to_thread(
        db_execute,
        """SELECT error_id,scope,error_type,message,severity,created_at
           FROM error_events
           WHERE resolved=FALSE AND COALESCE(severity,'error')<>'benign'
           ORDER BY
             CASE COALESCE(severity,'error')
               WHEN 'critical' THEN 1 WHEN 'error' THEN 2 WHEN 'warning' THEN 3 ELSE 4 END,
             created_at DESC
           LIMIT 8""",
        (),
        "all",
    ) or []
    stats = await asyncio.to_thread(
        db_execute,
        """SELECT
             COUNT(*) FILTER (WHERE resolved=FALSE AND severity='critical') critical,
             COUNT(*) FILTER (WHERE resolved=FALSE AND severity='error') errors,
             COUNT(*) FILTER (WHERE resolved=FALSE AND severity='warning') warnings,
             COUNT(*) FILTER (WHERE resolved=TRUE) resolved
           FROM error_events""",
        (),
        "one",
    ) or {}
    header = (
        "🚨 <b>Actionable Error Center</b>\n\n"
        f"🟥 Critical: <b>{stats.get('critical',0)}</b> • "
        f"🟠 Error: <b>{stats.get('errors',0)}</b> • "
        f"🟡 Warning: <b>{stats.get('warnings',0)}</b>\n"
        f"✅ Resolved history: <b>{stats.get('resolved',0)}</b>\n\n"
    )
    if not rows:
        body = "✅ No unresolved actionable runtime errors."
        buttons = [[InlineKeyboardButton(text="⬅️ Ops Center",callback_data="admin:ops")]]
    else:
        body = "\n\n".join(
            f"<code>{html.escape(r['error_id'])}</code> • <b>{html.escape(str(r['severity']).upper())}</b>\n"
            f"📍 {html.escape(r['scope'])} • {html.escape(r['error_type'])}\n"
            f"{html.escape((r['message'] or '')[:150])}\n🕒 {r['created_at']}"
            for r in rows
        )
        buttons = [
            [InlineKeyboardButton(text=f"✅ Resolve {r['error_id'][-8:]}", callback_data=f"admin:err_resolve:{r['error_id']}")]
            for r in rows[:5]
        ]
        buttons.append([InlineKeyboardButton(text="⬅️ Ops Center",callback_data="admin:ops")])
    await c.answer()
    await c.message.edit_text(header+body, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("admin:err_resolve:"))
async def admin_error_resolve(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    error_id = c.data.split(":",2)[2]
    row = await asyncio.to_thread(
        db_execute,
        """UPDATE error_events
           SET resolved=TRUE,resolved_at=NOW(),resolved_by=%s
           WHERE error_id=%s AND resolved=FALSE
           RETURNING error_id,severity,scope""",
        (c.from_user.id,error_id),
        "one",
    )
    if not row:
        return await c.answer("Already resolved or not found.",show_alert=True)
    await asyncio.to_thread(
        admin_log,
        c.from_user.id,
        "runtime_error_resolved",
        f"{error_id} severity={row['severity']} scope={row['scope']}",
    )
    await c.answer("Resolved.")
    c.data = "admin:ops_errors"
    return await admin_ops_errors(c)


@router.callback_query(F.data=="admin:ops_orders")
async def admin_ops_order_audit(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    rows=await asyncio.to_thread(db_execute,"SELECT order_id,old_status,new_status,payment_mode,operation,changed_at FROM order_status_audit ORDER BY id DESC LIMIT 20",(),"all") or []
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


@router.callback_query(F.data=="admin:backup_health")
async def admin_backup_health(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    snap = await asyncio.to_thread(backup_health_snapshot)
    status = str(snap.get("status","unknown")).lower()
    icon = "✅" if status == "ok" else ("❌" if status == "failed" else "⚪")
    text = (
        "💾 <b>Backup Health</b>\n\n"
        f"{icon} Last status: <b>{html.escape(status.upper())}</b>\n"
        f"🕒 Last attempt: <code>{html.escape(str(snap.get('last_at','never')))}</code>\n"
        f"📄 Last file: <code>{html.escape(str(snap.get('last_file') or 'n/a'))}</code>\n"
        f"⏱ Auto backup: <b>{snap.get('auto_hours',0)}h</b> • Keep: <b>{snap.get('keep_count',0)}</b>\n"
    )
    if snap.get("last_error"):
        text += f"\n❌ Last error: <code>{html.escape(str(snap['last_error'])[:300])}</code>"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Create Backup",callback_data="admin:backup_now"),
         InlineKeyboardButton(text="🔄 Refresh",callback_data="admin:backup_health")],
        [InlineKeyboardButton(text="📈 Monitoring History",callback_data="admin:monitoring_history")],
        [InlineKeyboardButton(text="⬅️ Ops Center",callback_data="admin:ops")]
    ])
    await c.answer()
    await c.message.edit_text(text,reply_markup=kb)


@router.callback_query(F.data=="admin:ops_archive")
async def admin_ops_archive_confirm(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await c.answer()
    await c.message.edit_text(
        "🗄 <b>Run Safe Archive?</b>\n\n"
        "This archives old operational logs/notifications only. Financial orders, payments, "
        "balance logs and payment audits are not deleted.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Yes, Run Archive",callback_data="admin:ops_archive_run")],
            [InlineKeyboardButton(text="❌ Cancel",callback_data="admin:ops")]
        ])
    )


@router.callback_query(F.data=="admin:ops_archive_run")
async def admin_ops_archive_run(c:CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
    await c.answer("Running safe archive...")
    try:
        counts = await asyncio.to_thread(operational_archive_cleanup)
        await asyncio.to_thread(admin_log,c.from_user.id,"ops_archive",json.dumps(counts,sort_keys=True))
        text=("🗄 <b>Safe Archive Complete</b>\n\n"
              f"🔔 Notification rows: <b>{counts.get('notification_queue',0)}</b>\n"
              f"📝 Admin logs: <b>{counts.get('admin_logs',0)}</b>\n"
              f"🔐 Security events: <b>{counts.get('security_events',0)}</b>\n"
              f"🚨 Error events: <b>{counts.get('error_events',0)}</b>\n\n"
              "Financial orders, payments, balance logs and payment audits were not deleted.")
    except Exception as exc:
        error_id=record_runtime_error("admin_ops_archive",exc,{"admin_id":c.from_user.id})
        text=f"❌ Archive maintenance failed. Ref: <code>{html.escape(error_id)}</code>"
    await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Ops Center",callback_data="admin:ops")]
    ]))


@router.message(Command("error"))
async def admin_error_lookup(m:Message):
    if not is_admin(m.from_user.id): return await m.answer("Denied")
    parts=(m.text or "").split(maxsplit=1)
    if len(parts)!=2 or not parts[1].strip().upper().startswith("ERR-"):
        return await m.answer("Usage: <code>/error ERR-...</code>")
    error_id=parts[1].strip().upper()[:80]
    row=await adb_execute("SELECT error_id,instance_id,scope,error_type,message,context_json,created_at FROM error_events WHERE error_id=%s",(error_id,),"one")
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
    rows=await asyncio.to_thread(db_execute,"SELECT * FROM admin_logs ORDER BY id DESC LIMIT 20",(),"all"); text="📝 No admin logs." if not rows else "📝 <b>Recent Admin Logs</b>\n\n"+"\n".join(f"#{r['id']} • {html.escape(str(r['action']))}\n{html.escape(str(r['details'] or ''))}\n🕒 {r['created_at']}\n" for r in rows); await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]))

@router.callback_query(F.data=="admin:ultra")
async def admin_ultra(c:CallbackQuery, answer_callback=True):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied",show_alert=True)
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
        rows.append([InlineKeyboardButton(text=f"{label} • {state}",callback_data=f"feature:{key}")])
    rows.append([InlineKeyboardButton(text="📊 System Status",callback_data="admin:status"),InlineKeyboardButton(text="💾 Backup Now",callback_data="admin:backup_now")])
    rows.append([InlineKeyboardButton(text=setting("admin_back","⬅️ Admin"),callback_data="admin:dashboard")])
    body=(f"🚀 <b>{html.escape(APP_VERSION)} — Ultra Control</b>\n\n"
          "Each switch controls both visibility and direct access for buyer features.\n"
          "Changes are stored in PostgreSQL and survive redeploys.\n\n"
          "🟢 ON = buyer can use it • 🔴 OFF = buyer access blocked")
    if answer_callback:
        try: await c.answer()
        except Exception: pass
    try:
        await c.message.edit_text(body,reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower(): raise

@router.callback_query(F.data.startswith("feature:"))
async def toggle_feature(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    key=c.data.split(":",1)[1]
    allowed={"feature_quick_shop","feature_search","feature_favorites","feature_rewards","feature_referral","feature_support","feature_announcements","feature_vip","feature_smart_offers"}
    if key not in allowed: return await c.answer("Invalid feature",show_alert=True)
    new="0" if _feature_on(key) else "1"
    try:
        await asyncio.to_thread(set_setting,key,new)
        await asyncio.to_thread(admin_log,c.from_user.id,"feature_toggle",f"{key}={new}")
    except Exception as exc:
        record_runtime_error("ultra_feature_toggle",exc,{"admin":c.from_user.id,"key":key})
        return await c.answer("Toggle failed safely. Check Ops errors.",show_alert=True)
    if _feature_on(key)!=(new=="1"):
        return await c.answer("Toggle verification failed.",show_alert=True)
    await c.answer("🟢 Enabled" if new=="1" else "🔴 Disabled")
    await admin_ultra(c,answer_callback=False)

@router.callback_query(F.data=="admin:status")
async def admin_status(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied", show_alert=True)
    try:
        db=await adb_execute("SELECT current_database() AS db", fetch="one")
        users=(await adb_execute("SELECT COUNT(*) AS c FROM users", fetch="one"))["c"]
        products=(await adb_execute("SELECT COUNT(*) AS c FROM products WHERE active=1", fetch="one"))["c"]
        pending=(await adb_execute("SELECT COUNT(*) AS c FROM orders WHERE status='pending'", fetch="one"))["c"]
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
        await aadmin_log(c.from_user.id,"manual_backup","ultra_control")
        await c.message.answer_document(FSInputFile(str(path)), caption=f"💾 <b>V8 Ultra backup created</b>\n<code>{html.escape(str(path))}</code>")
    except Exception as e:
        await c.message.answer(f"❌ Backup failed: <code>{html.escape(str(e))}</code>")



@router.callback_query(F.data=="admin:themes")
async def admin_theme_control(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    current=setting("theme_preset","blue").casefold()
    rows=[]
    for key,data in _THEME_PRESETS.items():
        mark="✅ " if key==current else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{data['accent']} {data['name']}",callback_data=f"admin:theme_set:{key}")])
    rows.append([InlineKeyboardButton(text="⬅️ Settings",callback_data="admin:settings")])
    await c.answer()
    await c.message.edit_text(
      "🎨 <b>Buyer Theme Presets</b>\n\n"
      "Changes buyer-facing icons/header/progress style.\n"
      "Telegram controls native button background colors, so those cannot be recolored by the bot.",
      reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data.startswith("admin:theme_set:"))
async def admin_theme_set(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    key=c.data.rsplit(":",1)[1]
    if key not in _THEME_PRESETS: return await c.answer("Unknown theme.",show_alert=True)
    await asyncio.to_thread(set_setting,"theme_preset",key)
    await asyncio.to_thread(admin_log,c.from_user.id,"buyer_theme_changed",key)
    await c.answer("Theme updated.")
    c.data="admin:themes"
    return await admin_theme_control(c)


@router.callback_query(F.data=="admin:settings")
async def settings(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    text=(f"⚙️ <b>{APP_VERSION} — Custom Control Center</b>\n\n"
          f"🏪 Shop: <code>{html.escape(shop_name())}</code>\n"
          f"💱 Currency: <code>BDT + USDT</code>\n"
          f"💵 1 USDT = <code>{usdt_bdt_rate():g} BDT</code> • USDT: <b>{'ON' if usdt_enabled() else 'OFF'}</b>\n"
          f"🎧 Support: <code>{html.escape(setting('support',SUPPORT))}</code>\n"
          f"🔧 Maintenance: <b>{'ON' if maintenance_active() else 'OFF'}</b>\n\n"
          "Choose a section to customize without editing code.")
    kb=[[InlineKeyboardButton(text=setting("custom_shop", "🏪 Shop & Branding"),callback_data="custom:shop"),InlineKeyboardButton(text=setting("custom_ui", "🎨 Buttons & UI"),callback_data="custom:ui")],
        [InlineKeyboardButton(text=setting("custom_payments", "💳 Payments"),callback_data="custom:payments"),InlineKeyboardButton(text=setting("custom_money", "💰 Money & Rewards"),callback_data="custom:money")],
        [InlineKeyboardButton(text=setting("custom_orders", "📦 Orders"),callback_data="custom:orders"),InlineKeyboardButton(text=setting("custom_messages", "📝 Messages"),callback_data="custom:messages")],
        [InlineKeyboardButton(text=setting("custom_system", "🔧 System"),callback_data="custom:system")],
        [InlineKeyboardButton(text="🎨 Buyer Theme Presets",callback_data="admin:themes")],
        [InlineKeyboardButton(text=setting("admin_toggle_maintenance", "🔧 Toggle Maintenance"),callback_data="set:maintenance")],
        [InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]
    await c.answer(); await c.message.edit_text(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

CUSTOM_GROUPS={
 "shop":[("shop_name","🏪 Shop Name"),("currency","💱 Base Currency (keep BDT)"),("support","🎧 Support"),("announcement","📢 Announcement"),("footer_text","🔻 Footer")],
 "ui":[("button_shop","🛍 Shop Button"),("button_search","🔍 Search Button"),("button_orders","📦 Orders Button"),("button_favorites","❤️ Favorites Button"),("button_profile","👤 Profile Button"),("button_deposit","💰 Deposit Button"),("button_rewards","⭐ Rewards Button"),("button_refer","🤝 Refer Button"),("button_support","🆘 Support Button"),("button_buy","🛒 Buy Button"),("button_purchase","🛒 Purchase Button"),("button_confirm","✅ Confirm Button"),("button_back","⬅️ Back Button"),("button_back_listings","⬅️ Back Listings Button"),("button_main_menu","🏠 Main Menu Button"),("button_favorite_add","⭐ Add Favorite Button"),("button_favorite_remove","💔 Remove Favorite Button"),("button_sold_out","⛔ Sold Out Button"), ("inline_rewards","⭐ Rewards Button"),("inline_referral","🤝 Referral Button"),("inline_shop","🛍️ Shop Button"),("inline_view_orders","📦 View My Orders Button"),("admin_block","🚫 Block Button"), ("inline_all_products","✨ All Products Button"),("inline_games_back","⬅️ Games Button"),("inline_first","⏮ First Button"),("inline_back","◀️ Back Button"),("inline_next","▶️ Next Button"),("inline_last","⏭ Last Button"),("inline_refresh","🔄 Refresh Button"),("inline_under5","💵 Under 5 Button"),("inline_categories","📂 Categories Button"),("inline_main_menu","🏠 Main Menu Button"),("inline_my_orders","⬅️ My Orders Button"),("inline_admin_back","⬅️ Admin Button"),("admin_add_product","➕ Add Product Button"),("admin_edit","✏️ Edit Button"),("admin_toggle_product","🔄 Enable/Disable Button"),("admin_add_codes","🎫 Add Codes Button"),("admin_delete","🗑 Delete Button"),("admin_order_complete","✅ Complete Button"),("admin_order_reject_refund","❌ Reject + Refund Button"),("admin_credit","✅ Credit Button"),("admin_reject","❌ Reject Button"),("admin_unblock","🔓 Unblock Button"),("admin_block","🚫 Block Button"),("admin_users_back","⬅️ Users Button"),("admin_products_back","⬅️ Products Button"),("admin_settings_back","⬅️ Settings Button"),("admin_back","⬅️ Admin Button"),("admin_database","📊 Database Button"),("admin_logs","📝 Logs Button"),("admin_dashboard","📊 Dashboard Button"),("admin_reports","📈 Reports Button"),("admin_premium","💎 Premium Analytics Button"),("admin_orders","🧾 Orders Button"),("admin_payments","💳 Payments Button"),("admin_users","👥 Users Button"),("admin_products","🛍 Products Button"),("admin_codes","🎫 Codes Button"),("admin_balance","💰 Balance Button"),("admin_broadcast","📢 Broadcast Button"),("admin_settings","⚙️ Settings Button"),("admin_toggle_maintenance","🔧 Toggle Maintenance Button"),("custom_shop","🏪 Shop & Branding Button"),("custom_ui","🎨 Buttons & UI Button"),("custom_payments","💳 Payments Button"),("custom_money","💰 Money & Rewards Button"),("custom_orders","📦 Orders Button"),("custom_messages","📝 Messages Button"),("custom_system","🔧 System Button"),("home_shop","🏠 Home Shop Button"),("home_orders","🏠 Home Orders Button"),("home_deposit","🏠 Home Deposit Button"),("home_profile","🏠 Home Profile Button"),("home_rewards","🏠 Home Rewards Button"),("home_refer","🏠 Home Referral Button"),("home_favorites","🏠 Home Favorites Button"),("home_support","🏠 Home Support Button"),("home_search","🏠 Home Search Button"),("admin_dashboard","📊 Admin Dashboard Button"),("admin_reports","📈 Admin Reports Button"),("admin_premium","💎 Admin Premium Button"),("admin_orders","🧾 Admin Orders Button"),("admin_payments","💳 Admin Payments Button"),("admin_users","👥 Admin Users Button"),("admin_products","🛍 Admin Products Button"),("admin_codes","🎫 Admin Codes Button"),("admin_balance","💰 Admin Balance Button"),("admin_broadcast","📢 Admin Broadcast Button"),("admin_settings","⚙️ Admin Settings Button"),("admin_database","📊 Admin Database Button"),("admin_logs","📝 Admin Logs Button")],
 "payments":[("payment_info","💳 Payment Instructions"),("payment_bkash_label","bKash Button Label"),("payment_nagad_label","Nagad Button Label"),("payment_rocket_label","Rocket Button Label"),("payment_binance_label","Binance Button Label"),("payment_bkash_account","bKash Account"),("payment_nagad_account","Nagad Account"),("payment_rocket_account","Rocket Account"),("payment_binance_account","Binance Account/Wallet"),("payment_bkash_instruction","bKash Instructions"),("payment_nagad_instruction","Nagad Instructions"),("payment_rocket_instruction","Rocket Instructions"),("payment_binance_instruction","Binance Instructions"),("payment_binance_network","Binance Network"),("payment_bkash_icon","bKash Logo/Icon"),("payment_nagad_icon","Nagad Logo/Icon"),("payment_rocket_icon","Rocket Logo/Icon"),("payment_binance_icon","Binance Logo/Icon"),("payment_receipt_required","Receipt Required 1/0"),("payment_presets","Quick Amounts CSV"),("payment_method_prompt","Payment Method Prompt"),("deposit_min","⬇️ Minimum Deposit"),("deposit_max","⬆️ Maximum Deposit (0=unlimited)"),("payment_timeout_minutes","⏱ Payment Timeout")],
 "money":[("signup_bonus","🎁 Signup Bonus"),("referral_reward","🤝 Referral Reward"),("usdt_bdt_rate","💵 1 USDT = ? BDT"),("usdt_enabled","🪙 USDT Display ON/OFF 1/0")],
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
    val="0" if setting("maintenance","0")=="1" else "1"; set_setting("maintenance",val); await aadmin_log(c.from_user.id,"maintenance",val); await c.answer("Updated"); await settings(c)

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
    set_setting(key,value); await state.clear(); await aadmin_log(m.from_user.id,"custom_setting_changed",f"{key}={value[:300]}")
    await m.answer(f"✅ <b>{html.escape(key)}</b> updated.",reply_markup=admin_menu())

@router.callback_query(F.data=="admin:dbinfo")
async def dbinfo(c:CallbackQuery):
    if not is_admin(c.from_user.id): return await c.answer("Denied",show_alert=True)
    row=await adb_execute("SELECT current_database() AS db,current_schema() AS schema",fetch="one")
    await c.answer(); await c.message.edit_text(f"☁️ <b>Database</b>\n\nEngine: <b>PostgreSQL</b>\nDatabase: <code>{row['db']}</code>\nSchema: <code>{row['schema']}</code>\n\n✅ Data is stored in PostgreSQL, outside the Railway service filesystem.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=setting("admin_back", "⬅️ Admin"),callback_data="admin:dashboard")]]))

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



_SEARCH_ALIASES = {
    "ff": "free fire",
    "freefire": "free fire",
    "pubgm": "pubg mobile",
    "pubg": "pubg mobile",
    "codm": "cod mobile",
    "cod": "cod mobile",
    "pes": "efootball",
    "e football": "efootball",
    "coins": "coin",
    "diamonds": "diamond",
}


def normalize_product_search(value):
    value = re.sub(r"[^a-z0-9]+"," ",str(value or "").casefold()).strip()
    tokens = []
    for token in value.split():
        tokens.append(_SEARCH_ALIASES.get(token,token))
    normalized = " ".join(tokens)
    return _SEARCH_ALIASES.get(normalized,normalized)



def product_search_tokens(value):
    normalized=normalize_product_search(value)
    tokens=normalized.split()
    numeric=[t for t in tokens if t.isdigit()]
    words=[t for t in tokens if not t.isdigit()]
    return normalized,words,numeric


def game_search_aliases():
    rows=game_catalog_rows()
    aliases={}
    for r in rows:
        aliases[normalize_product_search(r["display_name"])]=normalize_product_search(r["game_key"])
        aliases[normalize_product_search(r["game_key"])]=normalize_product_search(r["game_key"])
    return aliases

def fuzzy_product_search(term, limit=12):
    """Bounded Python fallback; no pg_trgm dependency."""
    needle = normalize_product_search(term)
    if not needle:
        return []
    rows = db_execute("""
        SELECT p.*, CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock
        FROM products p
        LEFT JOIN (
          SELECT product_id,COUNT(*) AS available
          FROM product_codes WHERE status='available'
          GROUP BY product_id
        ) pc ON pc.product_id=p.id
        WHERE p.active=1
        ORDER BY p.id DESC
        LIMIT 500
    """, fetch="all") or []

    scored = []
    needle_tokens = set(needle.split())
    for p in rows:
        name = normalize_product_search(p.get("name"))
        category = normalize_product_search(p.get("category"))
        desc = normalize_product_search(p.get("description"))
        hay = f"{name} {category} {desc}".strip()
        ratio = max(
            difflib.SequenceMatcher(None,needle,name).ratio() if name else 0,
            difflib.SequenceMatcher(None,needle,category).ratio() if category else 0,
            difflib.SequenceMatcher(None,needle,hay[:160]).ratio() if hay else 0,
        )
        hay_tokens = set(hay.split())
        overlap = len(needle_tokens & hay_tokens) / max(1,len(needle_tokens))
        score = max(ratio,overlap)
        if score >= 0.48:
            scored.append((score, int(p.get("effective_stock") or 0)>0, int(p["id"]), p))
    scored.sort(key=lambda x:(-x[0],-int(x[1]),-x[2]))
    return [x[3] for x in scored[:max(1,min(24,int(limit)))]]


async def run_product_search(m:Message,term:str):
    term=(term or "").strip()
    normalized,words,numbers=product_search_tokens(term)
    aliases=await asyncio.to_thread(game_search_aliases)
    if normalized in aliases:
        normalized=aliases[normalized]
    else:
        for alias,key in sorted(aliases.items(),key=lambda x:len(x[0]),reverse=True):
            if alias and normalized.startswith(alias+" "):
                normalized=key+normalized[len(alias):]
                break
    search_terms=list(dict.fromkeys(x for x in (term,normalized) if x))
    patterns=[f"%{x}%" for x in search_terms]
    where_parts=[]; params=[]
    for pat in patterns:
        where_parts.append("(p.name ILIKE %s OR p.category ILIKE %s OR COALESCE(p.description,'') ILIKE %s)")
        params.extend((pat,pat,pat))
    for number in numbers:
        where_parts.append("(p.name ILIKE %s OR CAST(p.quantity AS TEXT)=%s)")
        params.extend((f"%{number}%",number))
    where_sql=" OR ".join(where_parts) or "FALSE"
    prefix=f"{normalized or term}%"
    params.extend((normalized or term,prefix,prefix,f"%{normalized or term}%"))
    rows=await asyncio.to_thread(
        db_execute,
        f"""SELECT p.*,CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END AS effective_stock
            FROM products p
            LEFT JOIN (
              SELECT product_id,COUNT(*) AS available FROM product_codes
              WHERE status='available' GROUP BY product_id
            ) pc ON pc.product_id=p.id
            WHERE p.active=1 AND ({where_sql})
            ORDER BY CASE WHEN LOWER(p.name)=LOWER(%s) THEN 0
                          WHEN p.name ILIKE %s THEN 1
                          WHEN p.category ILIKE %s THEN 2
                          WHEN p.name ILIKE %s THEN 3 ELSE 4 END,
                     CASE WHEN (CASE WHEN p.delivery_type='code' THEN COALESCE(pc.available,0) ELSE p.stock END)>0 THEN 0 ELSE 1 END,
                     p.id DESC
            LIMIT 24""",
        tuple(params),"all")
    if not rows:
        rows=await asyncio.to_thread(fuzzy_product_search,normalized or term,12)
    if not rows:
        try:
            u=await aget_user(m.from_user)
            picks=await asyncio.to_thread(smart_recommendations,u["id"],4)
        except Exception:
            picks=[]
        if picks:
            return await m.answer(
                f"🔎 No close match for <b>{html.escape(term)}</b>.\n\n🎯 <b>You may like these instead:</b>",
                reply_markup=recommendations_kb(picks))
        return await m.answer(
            f"🔎 No products found for <b>{html.escape(term)}</b>.\n\n"
            "Try game + amount, e.g. <code>pubg 325</code> or <code>efootball 80</code>.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🛍 Browse Shop",callback_data="shop")
            ]]))
    kb=[]
    for p in rows:
        stock=int(p["effective_stock"] or 0)
        badge="⚡" if p.get("delivery_type")=="code" and stock>0 else ("🟢" if stock>0 else "🔴")
        label=str(p["name"])
        if len(label)>26: label=label[:25]+"…"
        kb.append([
            InlineKeyboardButton(text=f"{badge} {label} • {product_button_price(p)}",callback_data=f"product:{p['id']}"),
            InlineKeyboardButton(text="🛒 Buy" if stock>0 else "⛔",callback_data=f"buy:{p['id']}" if stock>0 else f"soldout:{p['id']}")
        ])
    kb.append([InlineKeyboardButton(text="🛍 Browse All",callback_data="shop"),InlineKeyboardButton(text="🏠 Home",callback_data="main_menu")])
    await m.answer(
        f"🔎 <b>Smart Search V2</b>\n\nFound <b>{len(rows)}</b> result(s) for <code>{html.escape(term)}</code>.\n"
        "Game aliases, close spelling and package amounts are supported.",
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
                except Exception as exc:
                    record_runtime_error("automatic_backup_notify", exc, {"instance_id": INSTANCE_ID, "admin_id": admin_id})
        except Exception as exc:
            record_runtime_error("automatic_backup_loop", exc, {"instance_id": INSTANCE_ID})
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
                await adb_execute("UPDATE marketing_campaigns SET status='expired',claimed_at=NULL,claimed_by='' WHERE status IN ('scheduled','sending') AND ends_at IS NOT NULL AND ends_at<NOW()")
                campaigns = claim_due_campaigns(5)
                for campaign in campaigns:
                    recipients = marketing_recipients(campaign["audience"])
                    buttons = marketing_campaign_markup(campaign["id"])
                    coupon_line = f"\n\n🏷️ Coupon: <code>{html.escape(campaign['coupon_code'])}</code>" if campaign["coupon_code"] else ""
                    text = f"📣 <b>{html.escape(campaign['title'])}</b>\n\n{campaign['message']}{coupon_line}"
                    for user in recipients:
                        enqueue_campaign_notification(campaign, user, text, buttons)
                    row = await adb_execute("SELECT COUNT(*) AS c FROM marketing_delivery_claims WHERE campaign_id=%s", (campaign["id"],), "one")
                    sent = int(row["c"] or 0) if row else 0
                    await adb_execute(
                        "UPDATE marketing_campaigns SET status='sent',sent_count=%s,sent_at=NOW(),claimed_at=NULL,claimed_by='' WHERE id=%s AND status='sending' AND claimed_by=%s",
                        (sent, campaign["id"], INSTANCE_ID),
                    )
                    await aadmin_log(campaign["created_by"], "marketing_campaign_sent", f"campaign #{campaign['id']} recipients={sent}")
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
    row = await asyncio.to_thread(
        db_execute,
        """SELECT COUNT(*) campaigns, COALESCE(SUM(sent_count),0) sent,\n                              COALESCE(SUM(clicked_count),0) clicks, COALESCE(SUM(converted_count),0) conversions\n                       FROM marketing_campaigns WHERE created_at>=NOW()-INTERVAL '30 days'""",
        (),
        "one",
    )
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
    await aadmin_log(m.from_user.id, "marketing_campaign_create", f"campaign #{campaign_id} audience={audience}")
    await m.answer(f"✅ Campaign <b>#{campaign_id}</b> scheduled.\nAudience: <b>{html.escape(audience)}</b>\nStart: <b>{html.escape(start_minutes)} min</b>", reply_markup=admin_menu())


@router.callback_query(F.data == "mkt:list")
async def marketing_list(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Denied", show_alert=True)
    rows = await adb_execute("SELECT id,title,audience,status,sent_count,clicked_count,converted_count,starts_at FROM marketing_campaigns ORDER BY id DESC LIMIT 10", fetch="all") or []
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
    u = await aget_user(c.from_user)
    campaign = await adb_execute("SELECT * FROM marketing_campaigns WHERE id=%s", (campaign_id,), "one")
    if not campaign:
        return await c.answer("Offer unavailable.", show_alert=True)
    marketing_record_event(campaign_id, u["id"], "click")
    await adb_execute("UPDATE marketing_campaigns SET clicked_count=clicked_count+1 WHERE id=%s", (campaign_id,))
    buttons = [[InlineKeyboardButton(text="🛍️ Shop Now", callback_data="home:shop")], [InlineKeyboardButton(text="🏠 Main Menu", callback_data="main_menu")]]
    coupon = f"\n\n🏷️ Coupon: <code>{html.escape(campaign['coupon_code'])}</code>" if campaign["coupon_code"] else ""
    await c.answer("Offer opened")
    await c.message.answer(f"🎁 <b>{html.escape(campaign['title'])}</b>\n\n{campaign['message']}{coupon}", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

# ---------------- V8.3 Ultra Commerce ----------------

def product_merch_badges(product):
    badges=[]
    if int(product.get("featured") or 0): badges.append("⭐")
    if int(product.get("hot") or 0): badges.append("🔥")
    if int(product.get("best_seller") or 0): badges.append("🏆")
    return "".join(badges)


def product_price_display(product):
    base=float(product["price"])
    effective=product_sale_price(product)
    if effective < base:
        return f"🔥 {fmt_money(effective)}  <s>{fmt_money(base)}</s>"
    return fmt_money(base)


def product_button_price(product):
    base=float(product["price"])
    effective=product_sale_price(product)
    return f"🔥 {fmt_money(effective)}" if effective < base else fmt_money(base)


def offer_seconds(value):
    v=(value or "").strip().casefold()
    m=re.fullmatch(r"(\d+)\s*(h|hr|hrs|hour|hours|d|day|days)",v)
    if not m:
        raise ValueError("Duration must look like 6h, 24h, 2d.")
    amount=int(m.group(1))
    if amount<1:
        raise ValueError("Duration must be positive.")
    seconds=amount*(86400 if m.group(2).startswith("d") else 3600)
    if seconds>30*86400:
        raise ValueError("Maximum offer duration is 30 days.")
    return seconds


def apply_product_offer(product_id,sale_price,duration_seconds):
    sale_price=float(sale_price)
    duration_seconds=int(duration_seconds)
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id,price FROM products WHERE id=%s FOR UPDATE",(product_id,))
                p=cur.fetchone()
                if not p:
                    raise ValueError("Product not found.")
                base=float(p["price"])
                if sale_price<=0 or sale_price>=base:
                    raise ValueError("Sale price must be >0 and lower than regular price.")
                cur.execute(
                    "UPDATE products SET sale_price=%s,sale_until=NOW()+(%s * INTERVAL '1 second'),updated_at=NOW() WHERE id=%s",
                    (sale_price,duration_seconds,product_id))


def apply_game_offer(game_key,discount_percent,duration_seconds):
    discount=float(discount_percent)
    if discount<=0 or discount>=90:
        raise ValueError("Game discount must be greater than 0% and below 90%.")
    duration_seconds=int(duration_seconds)
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE products
                       SET sale_price=ROUND((price*(1-(%s/100.0)))::numeric,2),
                           sale_until=NOW()+(%s * INTERVAL '1 second'),
                           updated_at=NOW()
                       WHERE active=1 AND (category=%s OR category LIKE %s)
                       RETURNING id""",
                    (discount,duration_seconds,game_key,game_key+" > %"))
                rows=cur.fetchall() or []
    if not rows:
        raise ValueError("No active products found under that game.")
    return len(rows)


def clear_offer_scope(scope,target):
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                if scope=="product":
                    cur.execute(
                        "UPDATE products SET sale_price=NULL,sale_until=NULL,updated_at=NOW() WHERE id=%s RETURNING id",
                        (int(target),))
                elif scope=="game":
                    cur.execute(
                        """UPDATE products SET sale_price=NULL,sale_until=NULL,updated_at=NOW()
                           WHERE category=%s OR category LIKE %s RETURNING id""",
                        (target,target+" > %"))
                else:
                    raise ValueError("Unknown offer scope.")
                return len(cur.fetchall() or [])



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

async def cart_count(user_id):
    row=await adb_execute("SELECT COALESCE(SUM(quantity),0) AS c FROM cart_items WHERE user_id=%s",(user_id,),"one")
    return int(row["c"] or 0) if row else 0

def coupon_discount(coupon, subtotal):
    if not coupon: return 0.0
    value=float(coupon["value"] or 0)
    discount=value if coupon["discount_type"]=="fixed" else subtotal*value/100
    cap=float(coupon["max_discount"] or 0)
    if cap>0: discount=min(discount,cap)
    return max(0.0,min(discount,subtotal))

async def get_coupon(code,user_id,subtotal):
    if not coupons_enabled(): return None,"Coupons are disabled."
    code=(code or "").strip().upper()
    row=await adb_execute("""SELECT * FROM coupons WHERE UPPER(code)=UPPER(%s) AND active=1
                      AND (starts_at IS NULL OR starts_at<=NOW())
                      AND (ends_at IS NULL OR ends_at>=NOW()) LIMIT 1""",(code,),"one")
    if not row: return None,"Invalid or expired coupon."
    if int(row["usage_limit"] or 0)>0 and int(row["used_count"] or 0)>=int(row["usage_limit"]):
        return None,"Coupon usage limit reached."
    if subtotal<float(row["min_order"] or 0): return None,f"Minimum order is {fmt_money(row['min_order'])}."
    used=await adb_execute("SELECT 1 FROM coupon_uses WHERE coupon_id=%s AND user_id=%s LIMIT 1",(row["id"],user_id),"one")
    if used: return None,"You already used this coupon."
    return row,None

async def cart_rows(user_id):
    return await adb_execute("""SELECT ci.product_id,ci.quantity,p.name,p.category,p.price,p.sale_price,p.sale_until,p.delivery_type,p.stock,p.active
                         FROM cart_items ci JOIN products p ON p.id=ci.product_id
                         WHERE ci.user_id=%s AND p.active=1 ORDER BY ci.updated_at DESC""",(user_id,),"all")

async def cart_markup(user_id):
    rows=await cart_rows(user_id); buttons=[]
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
    rows=await cart_rows(user_id)
    if not rows:
        return await c.message.edit_text("🛒 <b>Your Cart is Empty</b>\n\nAdd products from the shop.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛍️ Shop",callback_data="home:shop")],[InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]]))
    subtotal=sum(int(r["quantity"])*product_sale_price(r) for r in rows)
    lines=["🛒 <b>SMART CART</b>","━━━━━━━━━━━━━━━━━━"]
    for r in rows:
        lines.append(f"• <b>{html.escape(r['name'])}</b> × {r['quantity']} = {fmt_money(int(r['quantity'])*product_sale_price(r))}")
    lines.append(f"\n💰 Subtotal: <b>{fmt_money(subtotal)}</b>")
    await c.message.edit_text("\n".join(lines),reply_markup=await cart_markup(user_id))

@router.callback_query(F.data=="cart:view")
async def cart_view_callback(c:CallbackQuery):
    if not cart_enabled(): return await c.answer("Cart is disabled.",show_alert=True)
    u=await aget_user(c.from_user); await c.answer(); await render_cart(c,u["id"])

@router.callback_query(F.data.startswith("cart:add:"))
async def cart_add_callback(c:CallbackQuery):
    if not cart_enabled(): return await c.answer("Cart is disabled.",show_alert=True)
    pid=int(c.data.split(":")[2]); u=await aget_user(c.from_user)
    p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not p: return await c.answer("Product unavailable.",show_alert=True)
    if effective_stock(p)<1: return await c.answer("Out of stock.",show_alert=True)
    max_qty=max(1,int(setting("cart_max_quantity","10") or 10))
    await adb_execute("""INSERT INTO cart_items(user_id,product_id,quantity,updated_at) VALUES(%s,%s,1,NOW())
                 ON CONFLICT(user_id,product_id) DO UPDATE SET quantity=LEAST(cart_items.quantity+1,%s),updated_at=NOW()""",(u["id"],pid,max_qty))
    await c.answer("🛒 Added to cart"); await render_cart(c,u["id"])

@router.callback_query(F.data.startswith("cart:inc:"))
async def cart_inc_callback(c:CallbackQuery):
    pid=int(c.data.split(":")[2]); u=await aget_user(c.from_user)
    row=await adb_execute("SELECT quantity FROM cart_items WHERE user_id=%s AND product_id=%s",(u["id"],pid),"one")
    p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(pid,),"one")
    if not row or not p: return await c.answer("Item unavailable.",show_alert=True)
    limit=min(max(1,int(setting("cart_max_quantity","10") or 10)),max(1,effective_stock(p)))
    if int(row["quantity"])>=limit: return await c.answer("Maximum available quantity reached.",show_alert=True)
    await adb_execute("UPDATE cart_items SET quantity=quantity+1,updated_at=NOW() WHERE user_id=%s AND product_id=%s",(u["id"],pid))
    await c.answer("Quantity increased"); await render_cart(c,u["id"])

@router.callback_query(F.data.startswith("cart:dec:"))
async def cart_dec_callback(c:CallbackQuery):
    pid=int(c.data.split(":")[2]); u=await aget_user(c.from_user)
    await adb_execute("UPDATE cart_items SET quantity=quantity-1,updated_at=NOW() WHERE user_id=%s AND product_id=%s AND quantity>1",(u["id"],pid))
    await adb_execute("DELETE FROM cart_items WHERE user_id=%s AND product_id=%s AND quantity<=1",(u["id"],pid))
    await c.answer("Quantity updated"); await render_cart(c,u["id"])

@router.callback_query(F.data=="cart:clear")
async def cart_clear_callback(c:CallbackQuery):
    u=await aget_user(c.from_user); await adb_execute("DELETE FROM cart_items WHERE user_id=%s",(u["id"],))
    await c.answer("Cart cleared"); await render_cart(c,u["id"])

@router.callback_query(F.data=="cart:checkout")
async def cart_checkout_callback(c:CallbackQuery,state:FSMContext):
    u=await aget_user(c.from_user)
    if not cart_enabled() or await cart_count(u["id"])<1: return await c.answer("Cart is empty.",show_alert=True)
    await state.set_state(CartState.uid); await c.answer()
    await c.message.answer("🆔 <b>Cart checkout</b>\n\nSend your game/player UID.\n\nSend /cancel to cancel.")

@router.message(CartState.uid)
async def cart_uid(m:Message,state:FSMContext):
    uid=(m.text or "").strip()
    if uid.lower()=="/cancel": await state.clear(); return await m.answer("❌ Cancelled.",reply_markup=inline_home_kb())
    if len(uid)<2 or len(uid)>64: return await m.answer("❌ Please send a valid UID.")
    u=await aget_user(m.from_user); rows=await cart_rows(u["id"])
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
        coupon,err=await get_coupon(code,u["id"],subtotal)
        if err: return await m.answer(f"❌ {err}\n\nTry again or send <code>SKIP</code>.")
    discount=coupon_discount(coupon,subtotal); total=max(0,subtotal-discount)
    if float(u["balance"])<total:
        await state.clear(); return await m.answer(f"❌ Insufficient balance.\nNeed: {fmt_money(total-float(u['balance']))}",reply_markup=inline_home_kb())
    await state.update_data(coupon_id=coupon["id"] if coupon else None,discount=discount,total=total)
    await m.answer(f"🛒 <b>Checkout Confirmation</b>\n\nSubtotal: <b>{fmt_money(subtotal)}</b>\nDiscount: <b>-{fmt_money(discount)}</b>\nTotal: <b>{fmt_money(total)}</b>\nUID: <code>{html.escape(d['game_uid'])}</code>",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Confirm Payment",callback_data="cart:confirm")],[InlineKeyboardButton(text="❌ Cancel",callback_data="cart:cancel")]]))

@router.callback_query(CartState.coupon,F.data=="cart:cancel")
async def cart_cancel(c:CallbackQuery,state:FSMContext):
    await state.clear(); await c.answer("Cancelled"); await c.message.edit_text("❌ Cart checkout cancelled.",reply_markup=await cart_markup((await aget_user(c.from_user))["id"]))

def _cart_confirm_tx(user_id,uid,coupon_id):
    delivered=[]; pending=[]; order_ids=[]
    with DB_LOCK:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users WHERE id=%s FOR UPDATE",(user_id,)); u=cur.fetchone()
                cur.execute("SELECT ci.product_id,ci.quantity,p.* FROM cart_items ci JOIN products p ON p.id=ci.product_id WHERE ci.user_id=%s AND p.active=1 ORDER BY ci.product_id FOR UPDATE",(user_id,)); rows=cur.fetchall()
                if not rows: raise RuntimeError("Cart is empty.")
                subtotal=sum(int(r["quantity"])*product_sale_price(r) for r in rows)
                coupon=None
                if coupon_id:
                    cur.execute("SELECT * FROM coupons WHERE id=%s AND active=1 FOR UPDATE",(coupon_id,)); coupon=cur.fetchone()
                    if not coupon: raise RuntimeError("Coupon invalid.")
                discount=coupon_discount(coupon,subtotal); grand=max(0,subtotal-discount)
                if float(u["balance"])<grand: raise RuntimeError("Insufficient balance.")
                for r in rows:
                    p=r; qty=int(r["quantity"]); unit=product_sale_price(p); line=unit*qty
                    line_discount=discount*(line/subtotal) if subtotal else 0
                    for _ in range(qty):
                        cur.execute("SELECT * FROM products WHERE id=%s AND active=1 FOR UPDATE",(p["id"],)); prod=cur.fetchone()
                        if not prod: raise RuntimeError("Product unavailable.")
                        price=max(0,unit-line_discount/qty); delivered_code=None
                        if prod["delivery_type"]=="code":
                            cur.execute("SELECT * FROM product_codes WHERE product_id=%s AND status='available' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",(prod["id"],)); code_row=cur.fetchone()
                            if not code_row: raise RuntimeError(f"Out of stock: {prod['name']}")
                            cur.execute("UPDATE product_codes SET status='sold',sold_to=%s,sold_at=NOW() WHERE id=%s AND status='available'",(user_id,code_row["id"]))
                            delivered_code=code_row["code"]; status="completed"
                        else:
                            cur.execute("UPDATE products SET stock=stock-1,updated_at=NOW() WHERE id=%s AND stock>0",(prod["id"],))
                            if cur.rowcount!=1: raise RuntimeError(f"Out of stock: {prod['name']}")
                            status="pending"
                        cur.execute("INSERT INTO orders(user_id,product_id,game_uid,total,delivered_code,status) VALUES(%s,%s,%s,%s,%s,%s) RETURNING id",(user_id,prod["id"],uid,price,delivered_code,status)); oid=cur.fetchone()["id"]; order_ids.append(oid)
                        cur.execute("INSERT INTO balance_logs(user_id,amount,action,note) VALUES(%s,%s,%s,%s)",(user_id,-price,"purchase",f"Cart Order #{oid}"))
                        if delivered_code:
                            cur.execute("UPDATE product_codes SET order_id=%s WHERE id=%s",(oid,code_row["id"])); award_completed_order_rewards(cur,oid,user_id,price); delivered.append((oid,prod["name"],delivered_code,price))
                        else: pending.append((oid,prod["name"],price))
                        if prod["delivery_type"]=="code": sync_code_product_stock(prod["id"],conn)
                cur.execute("UPDATE users SET balance=balance-%s,updated_at=NOW() WHERE id=%s AND balance>=%s",(grand,user_id,grand))
                if cur.rowcount!=1: raise RuntimeError("Balance changed.")
                if coupon:
                    cur.execute("UPDATE coupons SET used_count=used_count+1 WHERE id=%s",(coupon["id"],))
                    cur.execute("INSERT INTO coupon_uses(coupon_id,user_id,order_id) VALUES(%s,%s,%s)",(coupon["id"],user_id,order_ids[0]))
                cur.execute("DELETE FROM cart_items WHERE user_id=%s",(user_id,))
    return {"order_ids":order_ids,"delivered":delivered,"pending":pending,"grand":grand}


@router.callback_query(CartState.coupon,F.data=="cart:confirm")
async def cart_confirm(c:CallbackQuery,state:FSMContext):
    d=await state.get_data(); await state.clear(); uid=d["game_uid"]; coupon_id=d.get("coupon_id"); user_id=(await aget_user(c.from_user))["id"]
    try:
        result=await asyncio.to_thread(_cart_confirm_tx,user_id,uid,coupon_id)
    except Exception as exc:
        error_id=record_runtime_error("cart_confirm",exc,{"user_id":user_id,"coupon_id":coupon_id})
        return await c.answer(f"Checkout failed safely. No balance or stock was charged. Ref: {error_id}",show_alert=True)
    order_ids=result["order_ids"]; delivered=result["delivered"]; pending=result["pending"]; grand=result["grand"]
    await c.answer("✅ Checkout complete")
    msg=[f"🛒 <b>Checkout Complete</b>",f"🧾 Orders: <b>{len(order_ids)}</b>",f"💰 Paid: <b>{fmt_money(grand)}</b>"]
    if coupon_id: msg.append("🏷️ Coupon applied.")
    if delivered: msg.append("\n🎁 <b>Instant Delivery</b>\n" + "\n".join(f"#{o} • {html.escape(n)} • <code>{code}</code>" for o,n,code,_ in delivered))
    if pending: msg.append("\n⏳ <b>Manual Delivery</b>\n" + "\n".join(f"#{o} • {html.escape(n)} • {fmt_money(a)}" for o,n,a in pending))
    await c.message.answer("\n".join(msg),reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 My Orders",callback_data="home:orders")],[InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]]))

@router.message(Command("cart"))
async def cart_command(m:Message):
    u=await aget_user(m.from_user)
    if not cart_enabled(): return await m.answer("🛒 Cart is disabled.")
    rows=await cart_rows(u["id"]); subtotal=sum(int(r["quantity"])*product_sale_price(r) for r in rows)
    lines=["🛒 <b>SMART CART</b>"]+[f"• {html.escape(r['name'])} × {r['quantity']} = {fmt_money(int(r['quantity'])*product_sale_price(r))}" for r in rows]
    if rows: lines.append(f"\n💰 Subtotal: <b>{fmt_money(subtotal)}</b>")
    else: lines.append("\nYour cart is empty.")
    await m.answer("\n".join(lines),reply_markup=await cart_markup(u["id"]))

@router.message(Command("coupon"))
async def coupon_command(m:Message):
    await m.answer("🏷️ <b>Coupons</b>\n\nApply your coupon during Smart Cart checkout.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛒 Open Cart",callback_data="cart:view")]]))

@router.message(Command("reorder"))
async def reorder_command(m:Message):
    u=await aget_user(m.from_user)
    row=await adb_execute("SELECT product_id FROM orders WHERE user_id=%s AND status='completed' ORDER BY id DESC LIMIT 1",(u["id"],),"one")
    if not row: return await m.answer("📦 No completed order found.",reply_markup=inline_home_kb())
    p=await adb_execute("SELECT * FROM products WHERE id=%s AND active=1",(row["product_id"],),"one")
    if not p or effective_stock(p)<1: return await m.answer("⚠️ Your latest product is unavailable.",reply_markup=inline_home_kb())
    await adb_execute("""INSERT INTO cart_items(user_id,product_id,quantity,updated_at) VALUES(%s,%s,1,NOW())
                  ON CONFLICT(user_id,product_id) DO UPDATE SET quantity=cart_items.quantity+1,updated_at=NOW()""",(u["id"],p["id"]))
    await m.answer(f"🔄 <b>Added to Cart</b>\n\n{html.escape(p['name'])}\n💰 {fmt_money(product_sale_price(p))}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛒 Open Cart",callback_data="cart:view")],[InlineKeyboardButton(text="🏠 Main Menu",callback_data="main_menu")]]))

@router.callback_query(F.data=="home:cart")
async def home_cart(c:CallbackQuery):
    if not cart_enabled(): return await c.answer("Cart is disabled.",show_alert=True)
    await c.answer(); await render_cart(c,(await aget_user(c.from_user))["id"])

USER_BOT_COMMANDS = [
    BotCommand(command="start", description="Launch the bot and open the main menu"),
    BotCommand(command="shop", description="Browse available products"),
    BotCommand(command="listings", description="Browse products (shortcut)"),
    BotCommand(command="search", description="Search products by name"),
    BotCommand(command="orders", description="View your recent orders"),
    BotCommand(command="track", description="Track an order by ID"),
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
        except Exception as exc:
            record_runtime_error("setup_admin_bot_commands", exc, {"admin_id": admin_id})



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
    db_execute("""CREATE TABLE IF NOT EXISTS smart_offer_proposals (
        id BIGSERIAL PRIMARY KEY,
        product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        discount_pct NUMERIC(6,2) NOT NULL CHECK(discount_pct>0 AND discount_pct<90),
        duration_hours INTEGER NOT NULL DEFAULT 24 CHECK(duration_hours>0 AND duration_hours<=720),
        audience TEXT NOT NULL DEFAULT 'all',
        reason TEXT NOT NULL DEFAULT '',
        score INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending',
        created_by BIGINT, approved_by BIGINT, campaign_id BIGINT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        decided_at TIMESTAMPTZ
    )""")
    db_execute("CREATE INDEX IF NOT EXISTS idx_smart_offer_proposals_status_time ON smart_offer_proposals(status,created_at DESC)")
    db_execute("CREATE INDEX IF NOT EXISTS idx_smart_offer_proposals_product ON smart_offer_proposals(product_id,status)")


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
    if not _feature_on("feature_smart_offers"):
        return await m.answer("🧠 Smart Offers are currently disabled.",reply_markup=premium_home_kb())
    u=await aget_user(m.from_user)
    rows=await asyncio.to_thread(smart_recommendations,u["id"],4)
    if not rows:
        return await m.answer("🎁 <b>Smart Offers</b>\n\nNo personalized offer is available right now.",reply_markup=premium_home_kb())
    await m.answer("🎯 <b>Smart Picks For You</b>\n\nBased on your activity and current stock:",reply_markup=recommendations_kb(rows))


@router.callback_query(F.data == "home:offers")
async def smart_offers_callback(c: CallbackQuery):
    if not _feature_on("feature_smart_offers"):
        return await c.answer("Smart Offers are currently disabled.",show_alert=True)
    u=await aget_user(c.from_user)
    rows=await asyncio.to_thread(smart_recommendations,u["id"],4)
    await c.answer()
    if not rows:
        return await c.message.edit_text(
            "🎁 <b>Smart Offers</b>\n\nNo personalized offer is available right now.",
            reply_markup=premium_home_kb())
    await c.message.edit_text(
        "🎯 <b>Smart Picks For You</b>\n\nRecommended from your activity and current stock:",
        reply_markup=recommendations_kb(rows))


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
    db_execute("DELETE FROM ops_health_history WHERE created_at < NOW()-INTERVAL '30 days'")


# V9.0 Preflight MAX: strict startup integrity checks before polling.
def startup_preflight():
    """Phase 6J deploy preflight: structural checks only; no brittle source-text assertions."""
    version=str(APP_VERSION or "").upper()
    if "V11 PHASE 6J" not in version:
        raise RuntimeError(f"Preflight failed: unexpected APP_VERSION {APP_VERSION!r}")

    required_callables = (
        "db_conn", "db_execute", "adb_execute",
        "product_sale_price", "discounted_price",
        "order_confirm", "order_pay_wallet", "_fulfill_wallet_order",
        "_start_direct_order_payment", "_finish_direct_payment", "_finish_payment_submission",
        "payment_credit", "cart_confirm",
        "admin_order_search_rows", "order_search_markup", "admin_order_search_receive",
        "uddoktapay_config", "uddoktapay_create_checkout", "uddoktapay_verify_invoice", "uddoktapay_process_invoice",
        "startup_preflight",
    )
    missing=[name for name in required_callables if not callable(globals().get(name))]
    if missing:
        raise RuntimeError("Preflight failed; missing callables: " + ", ".join(missing))

    # Configuration sanity. Keep compatibility with existing deployments while warning
    # when a dedicated credential secret is not configured.
    # Read deploy configuration from the environment. The bot does not expose
    # module globals named BOT_TOKEN/DATABASE_URL in every deployment layout.
    _bot_token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TOKEN")
    _database_url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or os.getenv("PG_URL")
    if not _bot_token:
        raise RuntimeError("Preflight failed: Telegram bot token environment variable is missing")
    if not _database_url:
        raise RuntimeError("Preflight failed: PostgreSQL database URL environment variable is missing")

    # Security hardening from Phase 5J: strict mode may require a dedicated secret.
    if globals().get("CREDENTIAL_SECRET_REQUIRED") and not os.getenv("CREDENTIAL_SECRET"):
        raise RuntimeError("Preflight failed: CREDENTIAL_SECRET is required")

    logging.info("Startup preflight PASS: %s", APP_VERSION)
    return True


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
    except Exception as exc:
        logging.warning("Failed to release leader advisory lock: %s", exc)
    try:
        conn.close()
    except Exception as exc:
        logging.warning("Failed to close leader connection cleanly: %s", exc)


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
                except Exception as exc:
                    perf_inc("errors")
                    record_runtime_error("low_stock_loop", exc, {"instance_id": INSTANCE_ID})
                await asyncio.sleep(600)

        async def autopilot_health_loop():
            while True:
                try:
                    await asyncio.to_thread(db_execute, "SELECT 1")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    perf_inc("errors")
                    record_runtime_error("autopilot_health_loop", exc, {"instance_id": INSTANCE_ID})
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
            "railway-live-monitor": railway_live_monitor_loop,
            "daily-business-summary": lambda: daily_business_summary_loop(bot),
            "auto-topup-status": lambda: auto_topup_status_loop(bot),
            "order-recovery-alerts": lambda: order_recovery_alert_loop(bot),
            "self-heal-guardian": lambda: self_heal_guardian_loop(bot),
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
                    snap=await asyncio.to_thread(observability_snapshot)
                    score=int(snap.get("health_score") or 0); reasons=list(snap.get("health_reasons") or []); status=_health_status(score)
                    runtime_state_update(health_score=score,health_status=status,health_reasons=reasons,last_observability_check=now_text())
                    signature=(status,tuple(reasons),tuple(sorted((runtime_state_snapshot().get("worker_restarts") or {}).items()))); now_m=time.monotonic()
                    if score<HEALTH_DEGRADED_SCORE and (signature!=alert_signature or now_m-last_alert>=OBSERVABILITY_ALERT_COOLDOWN_SECONDS):
                        reason_text="\n".join(f"• {html.escape(str(x))}" for x in reasons[:6]) or "• Health score degraded"
                        for admin_id in ADMIN_IDS:
                            try:
                                await bot.send_message(admin_id,f"🩺 <b>V10.19 Health Alert</b>\n\nScore: <b>{score}/100 • {status.upper()}</b>\n{reason_text}",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🩺 Diagnostics",callback_data="admin:diagnostics")]]))
                            except Exception as exc:
                                record_runtime_error("health_alert_notify", exc, {"instance_id": INSTANCE_ID, "admin_id": admin_id, "health_score": score})
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
                        "Open: <code>/admin?token=YOUR_ADMIN_WEB_TOKEN</code> on your Railway service URL.\n"
                        "💾 Use /backup for a manual database backup."
                    )
                except Exception as exc:
                    record_runtime_error("web_admin_startup_notify", exc, {"instance_id": INSTANCE_ID, "admin_id": admin_id})

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
