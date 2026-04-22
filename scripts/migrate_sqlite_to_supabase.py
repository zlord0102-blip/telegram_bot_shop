import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

from supabase import create_client

DB_PATH = os.getenv("SQLITE_DB_PATH", "data/shop.db")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BATCH_SIZE = int(os.getenv("MIGRATION_BATCH_SIZE", "500"))


def _parse_dt(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return datetime.fromisoformat(value).isoformat()
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
                try:
                    return datetime.strptime(value, fmt).isoformat()
                except ValueError:
                    continue
    return None


def _batch_insert(table: str, rows: List[Dict[str, Any]], supabase):
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i:i + BATCH_SIZE]
        if not chunk:
            continue
        supabase.table(table).insert(chunk).execute()


def migrate_users(conn, supabase):
    rows = conn.execute("SELECT user_id, username, balance, balance_usdt, language, created_at FROM users").fetchall()
    payload = [
        {
            "user_id": r[0],
            "username": r[1],
            "balance": r[2] or 0,
            "balance_usdt": r[3] or 0,
            "language": r[4],
            "created_at": _parse_dt(r[5]),
        }
        for r in rows
    ]
    _batch_insert("users", payload, supabase)


def migrate_products(conn, supabase):
    rows = conn.execute("SELECT id, name, price, price_usdt, description FROM products").fetchall()
    payload = [
        {
            "id": r[0],
            "name": r[1],
            "price": r[2],
            "price_usdt": r[3] or 0,
            "description": r[4],
        }
        for r in rows
    ]
    _batch_insert("products", payload, supabase)


def migrate_stock(conn, supabase):
    rows = conn.execute("SELECT id, product_id, content, sold FROM stock").fetchall()
    payload = [
        {
            "id": r[0],
            "product_id": r[1],
            "content": r[2],
            "sold": bool(r[3]),
        }
        for r in rows
    ]
    _batch_insert("stock", payload, supabase)


def migrate_orders(conn, supabase):
    rows = conn.execute(
        "SELECT id, user_id, product_id, content, price, quantity, order_group, created_at FROM orders"
    ).fetchall()
    payload = [
        {
            "id": r[0],
            "user_id": r[1],
            "product_id": r[2],
            "content": r[3],
            "price": r[4],
            "quantity": r[5],
            "order_group": r[6],
            "created_at": _parse_dt(r[7]),
        }
        for r in rows
    ]
    _batch_insert("orders", payload, supabase)


def migrate_deposits(conn, supabase):
    rows = conn.execute(
        "SELECT id, user_id, amount, code, status, created_at FROM deposits"
    ).fetchall()
    payload = [
        {
            "id": r[0],
            "user_id": r[1],
            "amount": r[2],
            "code": r[3],
            "status": r[4],
            "created_at": _parse_dt(r[5]),
        }
        for r in rows
    ]
    _batch_insert("deposits", payload, supabase)


def migrate_withdrawals(conn, supabase):
    rows = conn.execute(
        "SELECT id, user_id, amount, momo_phone, status, created_at FROM withdrawals"
    ).fetchall()
    payload = [
        {
            "id": r[0],
            "user_id": r[1],
            "amount": r[2],
            "momo_phone": r[3],
            "status": r[4],
            "created_at": _parse_dt(r[5]),
        }
        for r in rows
    ]
    _batch_insert("withdrawals", payload, supabase)


def migrate_settings(conn, supabase):
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    payload = [{"key": r[0], "value": r[1]} for r in rows]
    _batch_insert("settings", payload, supabase)


def migrate_usdt_withdrawals(conn, supabase):
    rows = conn.execute(
        "SELECT id, user_id, usdt_amount, wallet_address, network, status, created_at FROM usdt_withdrawals"
    ).fetchall()
    payload = [
        {
            "id": r[0],
            "user_id": r[1],
            "usdt_amount": r[2],
            "wallet_address": r[3],
            "network": r[4],
            "status": r[5],
            "created_at": _parse_dt(r[6]),
        }
        for r in rows
    ]
    _batch_insert("usdt_withdrawals", payload, supabase)


def migrate_processed_transactions(conn, supabase):
    rows = conn.execute(
        "SELECT tx_id, processed_at FROM processed_transactions"
    ).fetchall()
    payload = [
        {
            "tx_id": r[0],
            "processed_at": _parse_dt(r[1]) or r[1],
        }
        for r in rows
    ]
    if payload:
        _batch_insert("processed_transactions", payload, supabase)


def main():
    if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_SECRET_KEY/SUPABASE_SERVICE_ROLE_KEY in environment.")

    if not os.path.exists(DB_PATH):
        raise SystemExit(f"SQLite database not found at {DB_PATH}")

    supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)
    conn = sqlite3.connect(DB_PATH)

    try:
        migrate_users(conn, supabase)
        migrate_products(conn, supabase)
        migrate_stock(conn, supabase)
        migrate_orders(conn, supabase)
        migrate_deposits(conn, supabase)
        migrate_withdrawals(conn, supabase)
        migrate_settings(conn, supabase)
        migrate_usdt_withdrawals(conn, supabase)
        migrate_processed_transactions(conn, supabase)
    finally:
        conn.close()

    try:
        supabase.rpc("reset_sequences").execute()
    except Exception:
        pass

    print("✅ Migration complete.")


if __name__ == "__main__":
    main()
