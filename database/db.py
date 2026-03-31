import aiosqlite
import json
import os
from datetime import datetime, timedelta

def _parse_bool(value, default=True):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("0", "false", "off", "no"):
        return False
    if text in ("1", "true", "on", "yes"):
        return True
    return default


def _parse_json_list(value):
    if not value:
        return []
    try:
        data = json.loads(value)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _parse_json_object(value):
    if not value:
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

DB_PATH = "data/shop.db"


class DirectOrderFulfillmentError(RuntimeError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


class BinanceDirectOrderError(RuntimeError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code

async def init_db():
    # Ensure data directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                balance INTEGER DEFAULT 0,
                balance_usdt REAL DEFAULT 0,
                language TEXT DEFAULT 'vi',
                created_at TEXT
            )
        """)
        # Add balance_usdt column if not exists (migration)
        try:
            await db.execute("ALTER TABLE users ADD COLUMN balance_usdt REAL DEFAULT 0")
        except:
            pass
        # Add language column if not exists (migration)
        try:
            await db.execute("ALTER TABLE users ADD COLUMN language TEXT DEFAULT 'vi'")
        except:
            pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN first_name TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN last_name TEXT")
        except:
            pass
        await db.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price INTEGER NOT NULL,
                price_usdt REAL DEFAULT 0,
                price_tiers TEXT,
                promo_buy_quantity INTEGER DEFAULT 0,
                promo_bonus_quantity INTEGER DEFAULT 0,
                description TEXT,
                format_data TEXT
            )
        """)
        # Add price_usdt column if not exists (migration)
        try:
            await db.execute("ALTER TABLE products ADD COLUMN price_usdt REAL DEFAULT 0")
        except:
            pass
        # Add format_data column if not exists (migration)
        try:
            await db.execute("ALTER TABLE products ADD COLUMN format_data TEXT")
        except:
            pass
        # Add quantity pricing / promotion columns if not exists (migration)
        try:
            await db.execute("ALTER TABLE products ADD COLUMN price_tiers TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE products ADD COLUMN promo_buy_quantity INTEGER DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE products ADD COLUMN promo_bonus_quantity INTEGER DEFAULT 0")
        except:
            pass
        # Product visibility / soft-delete columns
        try:
            await db.execute("ALTER TABLE products ADD COLUMN is_hidden INTEGER DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE products ADD COLUMN is_deleted INTEGER DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE products ADD COLUMN deleted_at TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE products ADD COLUMN sort_position INTEGER")
        except:
            pass
        await db.execute("""
            CREATE TABLE IF NOT EXISTS format_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                pattern TEXT NOT NULL,
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS stock (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER,
                content TEXT NOT NULL,
                sold INTEGER DEFAULT 0,
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                product_id INTEGER,
                content TEXT,
                price INTEGER,
                quantity INTEGER DEFAULT 1,
                order_group TEXT,
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS deposits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                code TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                momo_phone TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS telegram_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                direction TEXT NOT NULL,
                message_type TEXT DEFAULT 'text',
                text TEXT,
                payload TEXT,
                sent_at TEXT,
                created_at TEXT,
                UNIQUE(chat_id, message_id)
            )
        """)
        # USDT withdrawals table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS usdt_withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                usdt_amount REAL,
                wallet_address TEXT,
                network TEXT DEFAULT 'TRC20',
                status TEXT DEFAULT 'pending',
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS direct_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                product_id INTEGER,
                quantity INTEGER DEFAULT 1,
                bonus_quantity INTEGER DEFAULT 0,
                unit_price INTEGER,
                amount INTEGER,
                code TEXT,
                payment_channel TEXT DEFAULT 'vietqr',
                payment_asset TEXT,
                payment_network TEXT,
                payment_amount_asset REAL,
                payment_rate_vnd REAL,
                payment_address TEXT,
                payment_address_tag TEXT,
                external_payment_id TEXT,
                external_tx_id TEXT,
                external_paid_at TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS bot_delivery_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                direct_order_id INTEGER NOT NULL UNIQUE,
                user_id INTEGER NOT NULL,
                channel TEXT NOT NULL DEFAULT 'telegram_bot',
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                next_retry_at TEXT,
                last_error TEXT,
                sent_at TEXT,
                last_attempt_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (direct_order_id) REFERENCES direct_orders(id)
            )
        """)
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN bonus_quantity INTEGER DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN payment_channel TEXT DEFAULT 'vietqr'")
        except:
            pass
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN payment_asset TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN payment_network TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN payment_amount_asset REAL")
        except:
            pass
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN payment_rate_vnd REAL")
        except:
            pass
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN payment_address TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN payment_address_tag TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN external_payment_id TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN external_tx_id TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN external_paid_at TEXT")
        except:
            pass
        await db.execute("UPDATE direct_orders SET payment_channel = 'vietqr' WHERE payment_channel IS NULL")
        await db.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS direct_orders_pending_binance_amount_idx
            ON direct_orders (payment_channel, payment_asset, payment_network, payment_amount_asset)
            WHERE status = 'pending'
              AND payment_channel = 'binance_onchain'
              AND payment_amount_asset IS NOT NULL
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS binance_processed_deposits (
                payment_id TEXT PRIMARY KEY,
                tx_id TEXT,
                direct_order_id INTEGER,
                amount_asset REAL,
                payment_asset TEXT,
                payment_network TEXT,
                processed_at TEXT
            )
        """)
        await db.execute("DROP TABLE IF EXISTS binance_deposits")
        await db.commit()


async def log_telegram_message(
    chat_id: int,
    message_id: int,
    direction: str,
    message_type: str = "text",
    text: str = None,
    payload=None,
    sent_at=None,
):
    """
    SQLite fallback for chat history logging (used when Supabase is disabled).
    Best-effort: errors are swallowed to avoid breaking the bot.
    """
    if not chat_id or not message_id:
        return

    try:
        payload_text = json.dumps(payload, ensure_ascii=False) if payload is not None else None
        sent_at_text = (
            sent_at.isoformat()
            if isinstance(sent_at, datetime)
            else (str(sent_at) if sent_at else datetime.utcnow().isoformat())
        )
        created_at = datetime.utcnow().isoformat()

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT OR IGNORE INTO telegram_messages
                (chat_id, message_id, direction, message_type, text, payload, sent_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (chat_id, message_id, str(direction), str(message_type), text, payload_text, sent_at_text, created_at),
            )
            await db.commit()
    except Exception:
        return

# User functions
async def get_or_create_user(
    user_id: int,
    username: str = None,
    first_name: str = None,
    last_name: str = None
):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_id, username, first_name, last_name, balance, balance_usdt, language FROM users WHERE user_id = ?",
            (user_id,)
        )
        user = await cursor.fetchone()
        if not user:
            await db.execute(
                "INSERT INTO users (user_id, username, first_name, last_name, language, created_at) VALUES (?, ?, ?, ?, NULL, ?)",
                (user_id, username, first_name, last_name, datetime.now().isoformat())
            )
            await db.commit()
            return {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "balance": 0,
                "balance_usdt": 0,
                "language": None
            }

        await db.execute(
            """
            UPDATE users
            SET
                username = COALESCE(?, username),
                first_name = COALESCE(?, first_name),
                last_name = COALESCE(?, last_name)
            WHERE user_id = ?
            """,
            (username, first_name, last_name, user_id)
        )
        await db.commit()
        return {
            "user_id": user[0],
            "username": username or user[1],
            "first_name": first_name or user[2],
            "last_name": last_name or user[3],
            "balance": user[4],
            "balance_usdt": user[5] or 0,
            "language": user[6]
        }

async def get_user_language(user_id: int) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT language FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        return row[0] if row and row[0] else "vi"

async def set_user_language(user_id: int, language: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET language = ? WHERE user_id = ?", (language, user_id))
        await db.commit()

async def get_balance(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        return row[0] if row else 0

async def get_balance_usdt(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT balance_usdt FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        return row[0] if row and row[0] else 0

async def update_balance(user_id: int, amount: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
        await db.commit()

async def update_balance_usdt(user_id: int, amount: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance_usdt = balance_usdt + ? WHERE user_id = ?", (amount, user_id))
        await db.commit()


# Product functions
async def get_products():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, name, price, description, price_usdt, format_data,
                price_tiers, promo_buy_quantity, promo_bonus_quantity, sort_position
            FROM products
            WHERE COALESCE(is_deleted, 0) = 0 AND COALESCE(is_hidden, 0) = 0
            ORDER BY
                CASE WHEN sort_position IS NULL THEN 1 ELSE 0 END ASC,
                sort_position ASC,
                id ASC
            """
        )
        rows = await cursor.fetchall()
        products = []
        for row in rows:
            stock_cursor = await db.execute(
                "SELECT COUNT(*) FROM stock WHERE product_id = ? AND sold = 0", (row[0],)
            )
            stock_count = (await stock_cursor.fetchone())[0]
            products.append({
                "id": row[0], "name": row[1], "price": row[2],
                "description": row[3], "stock": stock_count, "price_usdt": row[4] or 0,
                "format_data": row[5],
                "price_tiers": _parse_json_list(row[6]),
                "promo_buy_quantity": row[7] or 0,
                "promo_bonus_quantity": row[8] or 0,
                "sort_position": row[9] if row[9] is not None else None,
            })
        return products

async def get_product(product_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, name, price, description, price_usdt, format_data,
                price_tiers, promo_buy_quantity, promo_bonus_quantity, sort_position
            FROM products
            WHERE id = ? AND COALESCE(is_deleted, 0) = 0 AND COALESCE(is_hidden, 0) = 0
            """,
            (product_id,)
        )
        row = await cursor.fetchone()
        if row:
            stock_cursor = await db.execute(
                "SELECT COUNT(*) FROM stock WHERE product_id = ? AND sold = 0", (row[0],)
            )
            stock_count = (await stock_cursor.fetchone())[0]
            return {
                "id": row[0],
                "name": row[1],
                "price": row[2],
                "description": row[3],
                "stock": stock_count,
                "price_usdt": row[4] or 0,
                "format_data": row[5],
                "price_tiers": _parse_json_list(row[6]),
                "promo_buy_quantity": row[7] or 0,
                "promo_bonus_quantity": row[8] or 0,
                "sort_position": row[9] if row[9] is not None else None,
            }
        return None

async def add_product(
    name: str,
    price: int,
    description: str = "",
    price_usdt: float = 0,
    format_data: str = "",
    price_tiers=None,
    promo_buy_quantity: int = 0,
    promo_bonus_quantity: int = 0,
    sort_position: int = None,
):
    async with aiosqlite.connect(DB_PATH) as db:
        if sort_position is not None:
            await db.execute(
                """
                UPDATE products
                SET sort_position = sort_position + 1
                WHERE sort_position IS NOT NULL AND sort_position >= ?
                """,
                (int(sort_position),),
            )
        cursor = await db.execute(
            """
            INSERT INTO products
            (name, price, description, price_usdt, format_data, price_tiers, promo_buy_quantity, promo_bonus_quantity, sort_position)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                price,
                description,
                price_usdt,
                format_data,
                json.dumps(price_tiers) if price_tiers else None,
                promo_buy_quantity,
                promo_bonus_quantity,
                int(sort_position) if sort_position is not None else None,
            ),
        )
        await db.commit()
        return cursor.lastrowid

async def update_product_price_usdt(product_id: int, price_usdt: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE products SET price_usdt = ? WHERE id = ?", (price_usdt, product_id))
        await db.commit()

async def delete_product(product_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE products SET is_hidden = 1, is_deleted = 1, deleted_at = ? WHERE id = ?",
            (datetime.now().isoformat(), product_id)
        )
        await db.commit()

async def add_stock(product_id: int, content: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO stock (product_id, content) VALUES (?, ?)", (product_id, content))
        await db.commit()

async def add_stock_bulk(product_id: int, contents: list):
    """Thêm nhiều stock cùng lúc - tối ưu cho vài trăm items"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            "INSERT INTO stock (product_id, content) VALUES (?, ?)",
            [(product_id, content) for content in contents]
        )
        await db.commit()

async def get_available_stock(product_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, content FROM stock WHERE product_id = ? AND sold = 0 LIMIT 1", (product_id,)
        )
        return await cursor.fetchone()

async def get_available_stock_batch(product_id: int, quantity: int):
    """Lấy nhiều stock cùng lúc - tối ưu cho mua số lượng lớn"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, content FROM stock WHERE product_id = ? AND sold = 0 LIMIT ?",
            (product_id, quantity)
        )
        return await cursor.fetchall()

async def mark_stock_sold(stock_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE stock SET sold = 1 WHERE id = ?", (stock_id,))
        await db.commit()

async def mark_stock_sold_batch(stock_ids: list):
    """Mark nhiều stock sold cùng lúc"""
    if not stock_ids:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        placeholders = ",".join("?" * len(stock_ids))
        await db.execute(f"UPDATE stock SET sold = 1 WHERE id IN ({placeholders})", stock_ids)
        await db.commit()

async def get_stock_by_product(product_id: int):
    """Lấy tất cả stock của sản phẩm"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, content, sold FROM stock WHERE product_id = ? ORDER BY sold ASC, id DESC",
            (product_id,)
        )
        return await cursor.fetchall()

async def get_stock_detail(stock_id: int):
    """Lấy chi tiết một stock"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, product_id, content, sold FROM stock WHERE id = ?",
            (stock_id,)
        )
        return await cursor.fetchone()

async def update_stock_content(stock_id: int, new_content: str):
    """Cập nhật nội dung stock"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE stock SET content = ? WHERE id = ?", (new_content, stock_id))
        await db.commit()

async def delete_stock(stock_id: int):
    """Xóa một stock"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM stock WHERE id = ?", (stock_id,))
        await db.commit()

async def delete_all_stock(product_id: int, only_unsold: bool = False):
    """Xóa tất cả stock của sản phẩm"""
    async with aiosqlite.connect(DB_PATH) as db:
        if only_unsold:
            await db.execute("DELETE FROM stock WHERE product_id = ? AND sold = 0", (product_id,))
        else:
            await db.execute("DELETE FROM stock WHERE product_id = ?", (product_id,))
        await db.commit()

async def export_stock(product_id: int, only_unsold: bool = True):
    """Export stock ra list để tải file"""
    async with aiosqlite.connect(DB_PATH) as db:
        if only_unsold:
            cursor = await db.execute(
                "SELECT content FROM stock WHERE product_id = ? AND sold = 0 ORDER BY id",
                (product_id,)
            )
        else:
            cursor = await db.execute(
                "SELECT content FROM stock WHERE product_id = ? ORDER BY id",
                (product_id,)
            )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]
        await db.commit()

# Order functions
async def create_order_bulk(
    user_id: int,
    product_id: int,
    contents: list,
    price_per_item: int,
    order_group: str,
    total_price: int = None,
    quantity: int = None,
):
    """Tạo đơn hàng với nhiều items cùng lúc"""
    async with aiosqlite.connect(DB_PATH) as db:
        created_at = datetime.now().isoformat()
        final_quantity = quantity if quantity is not None else len(contents)
        final_total = total_price if total_price is not None else price_per_item * len(contents)
        # Lưu tất cả items vào 1 record với content là JSON
        import json
        await db.execute(
            "INSERT INTO orders (user_id, product_id, content, price, quantity, order_group, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, product_id, json.dumps(contents), int(final_total), int(final_quantity), order_group, created_at)
        )
        await db.commit()

async def create_order(user_id: int, product_id: int, content: str, price: int):
    """Legacy - tạo đơn hàng 1 item"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO orders (user_id, product_id, content, price, quantity, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, product_id, content, price, 1, datetime.now().isoformat())
        )
        await db.commit()


async def fulfill_bot_balance_purchase(
    user_id: int,
    product_id: int,
    quantity: int,
    bonus_quantity: int,
    order_price_per_item: int,
    order_total_price: int,
    charge_balance: int = 0,
    charge_balance_usdt: float = 0.0,
    order_group: str | None = None,
):
    required_stock = max(1, int(quantity) + max(0, int(bonus_quantity or 0)))
    stocks = await get_available_stock_batch(product_id, required_stock)
    if not stocks or len(stocks) < required_stock:
        raise DirectOrderFulfillmentError("not_enough_stock")

    current_balance = None
    if charge_balance:
        current_balance = await get_balance(user_id)
        if current_balance < int(charge_balance):
            raise DirectOrderFulfillmentError("insufficient_balance")

    current_balance_usdt = None
    if charge_balance_usdt:
        current_balance_usdt = await get_balance_usdt(user_id)
        if current_balance_usdt + 1e-9 < float(charge_balance_usdt):
            raise DirectOrderFulfillmentError("insufficient_usdt_balance")

    stock_ids = [stock[0] for stock in stocks]
    items = [stock[1] for stock in stocks]
    await mark_stock_sold_batch(stock_ids)

    final_order_group = (order_group or "").strip() or f"ORD{user_id}{datetime.now().strftime('%Y%m%d%H%M%S')}"
    await create_order_bulk(
        user_id,
        product_id,
        items,
        int(order_price_per_item),
        final_order_group,
        total_price=int(order_total_price),
        quantity=len(items),
    )

    new_balance = None
    if charge_balance:
        await update_balance(user_id, -int(charge_balance))
        new_balance = await get_balance(user_id)
    elif current_balance is not None:
        new_balance = current_balance

    new_balance_usdt = None
    if charge_balance_usdt:
        await update_balance_usdt(user_id, -float(charge_balance_usdt))
        new_balance_usdt = await get_balance_usdt(user_id)
    elif current_balance_usdt is not None:
        new_balance_usdt = current_balance_usdt

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT name, description, format_data FROM products WHERE id = ?",
            (product_id,),
        )
        product_row = await cursor.fetchone()

    return {
        "user_id": user_id,
        "product_id": product_id,
        "product_name": (product_row[0] if product_row else f"#{product_id}") or f"#{product_id}",
        "description": (product_row[1] if product_row else "") or "",
        "format_data": (product_row[2] if product_row else "") or "",
        "quantity": int(quantity),
        "bonus_quantity": int(bonus_quantity or 0),
        "delivered_quantity": len(items),
        "order_group": final_order_group,
        "items": items,
        "new_balance": new_balance,
        "new_balance_usdt": new_balance_usdt,
        "order_total_price": int(order_total_price),
        "charged_balance": int(charge_balance or 0),
        "charged_balance_usdt": float(charge_balance_usdt or 0.0),
    }

async def get_user_orders(user_id: int):
    """Lấy lịch sử đơn hàng - gom theo order_group hoặc từng đơn"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT o.id, p.name, o.content, o.price, o.created_at, o.quantity
               FROM orders o JOIN products p ON o.product_id = p.id 
               WHERE o.user_id = ? ORDER BY o.created_at DESC LIMIT 20""",
            (user_id,)
        )
        return await cursor.fetchall()

async def get_order_detail(order_id: int):
    """Lấy chi tiết 1 đơn hàng"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT o.id, p.name, o.content, o.price, o.created_at, o.quantity, p.description, p.format_data
               FROM orders o JOIN products p ON o.product_id = p.id 
               WHERE o.id = ?""",
            (order_id,)
        )
        return await cursor.fetchone()

async def get_sold_codes_by_product(product_id: int, limit: int = 100):
    """Lấy danh sách code đã bán theo sản phẩm"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT o.id, o.user_id, o.content, o.price, o.quantity, o.created_at
               FROM orders o
               WHERE o.product_id = ?
               ORDER BY o.created_at DESC
               LIMIT ?""",
            (product_id, limit)
        )
        return await cursor.fetchall()

async def get_sold_codes_by_user(user_id: int, limit: int = 50):
    """Lấy danh sách code đã bán cho 1 user"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT o.id, p.name, o.content, o.price, o.quantity, o.created_at
               FROM orders o JOIN products p ON o.product_id = p.id
               WHERE o.user_id = ?
               ORDER BY o.created_at DESC
               LIMIT ?""",
            (user_id, limit)
        )
        return await cursor.fetchall()

async def search_user_by_id(user_id: int):
    """Tìm user theo ID"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_id, username, balance, created_at FROM users WHERE user_id = ?",
            (user_id,)
        )
        return await cursor.fetchone()

# Deposit functions
async def create_deposit_with_settings(user_id: int, amount: int, code: str):
    await create_deposit(user_id, amount, code)
    return await get_bank_settings()

async def create_deposit(user_id: int, amount: int, code: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO deposits (user_id, amount, code, created_at) VALUES (?, ?, ?, ?)",
            (user_id, amount, code, datetime.now().isoformat())
        )
    await db.commit()

# Direct order functions
async def create_direct_order_with_settings(
    user_id: int,
    product_id: int,
    quantity: int,
    unit_price: int,
    amount: int,
    code: str,
    bonus_quantity: int = 0,
):
    await create_direct_order(user_id, product_id, quantity, unit_price, amount, code, bonus_quantity=bonus_quantity)
    return await get_bank_settings()

async def create_direct_order(
    user_id: int,
    product_id: int,
    quantity: int,
    unit_price: int,
    amount: int,
    code: str,
    bonus_quantity: int = 0,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO direct_orders (user_id, product_id, quantity, bonus_quantity, unit_price, amount, code, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, product_id, quantity, bonus_quantity, unit_price, amount, code, datetime.now().isoformat())
        )
        await db.commit()

async def get_pending_direct_orders():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT id, user_id, product_id, quantity, bonus_quantity, unit_price, amount, code, created_at
               FROM direct_orders
               WHERE status = 'pending'
                 AND COALESCE(payment_channel, 'vietqr') != 'binance_onchain'"""
        )
        return await cursor.fetchall()

async def set_direct_order_status(order_id: int, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE direct_orders SET status = ? WHERE id = ?", (status, order_id))
        await db.commit()


def _normalize_bot_delivery_outbox_row(row):
    if not row:
        return None
    payload = _parse_json_object(row["payload"])
    return {
        "id": int(row["id"] or 0),
        "direct_order_id": int(row["direct_order_id"] or 0),
        "user_id": int(row["user_id"] or 0),
        "channel": str(row["channel"] or "telegram_bot"),
        "payload": payload,
        "status": str(row["status"] or "pending"),
        "attempt_count": int(row["attempt_count"] or 0),
        "next_retry_at": row["next_retry_at"],
        "last_error": row["last_error"],
        "sent_at": row["sent_at"],
        "last_attempt_at": row["last_attempt_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


async def get_bot_delivery_outbox(direct_order_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM bot_delivery_outbox WHERE direct_order_id = ? LIMIT 1",
            (direct_order_id,),
        )
        row = await cursor.fetchone()
        return _normalize_bot_delivery_outbox_row(row)


async def ensure_bot_delivery_outbox(
    direct_order_id: int,
    user_id: int,
    payload: dict,
    reset_status: bool = False,
):
    now_iso = datetime.now().isoformat()
    payload_json = json.dumps(payload, ensure_ascii=False)
    existing = await get_bot_delivery_outbox(direct_order_id)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if existing:
            updates = [
                "user_id = ?",
                "payload = ?",
                "channel = ?",
                "updated_at = ?",
            ]
            params = [user_id, payload_json, "telegram_bot", now_iso]
            if reset_status:
                updates.extend([
                    "status = ?",
                    "last_error = NULL",
                    "next_retry_at = ?",
                    "sent_at = NULL",
                    "last_attempt_at = NULL",
                ])
                params.extend(["pending", now_iso])
            params.append(existing["id"])
            await db.execute(
                f"UPDATE bot_delivery_outbox SET {', '.join(updates)} WHERE id = ?",
                params,
            )
        else:
            await db.execute(
                """
                INSERT INTO bot_delivery_outbox (
                    direct_order_id, user_id, channel, payload, status, attempt_count,
                    next_retry_at, last_error, sent_at, last_attempt_at, created_at, updated_at
                ) VALUES (?, ?, 'telegram_bot', ?, 'pending', 0, ?, NULL, NULL, NULL, ?, ?)
                """,
                (direct_order_id, user_id, payload_json, now_iso, now_iso, now_iso),
            )
        await db.commit()

    return await get_bot_delivery_outbox(direct_order_id)


async def get_due_bot_delivery_outbox(limit: int = 20):
    now_iso = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT *
            FROM bot_delivery_outbox
            WHERE status = 'pending'
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY COALESCE(next_retry_at, created_at) ASC, id ASC
            LIMIT ?
            """,
            (now_iso, max(1, int(limit or 20))),
        )
        rows = await cursor.fetchall()
        return [_normalize_bot_delivery_outbox_row(row) for row in rows if row]


async def mark_bot_delivery_outbox_sending(outbox_id: int, attempt_count: int):
    now_iso = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE bot_delivery_outbox
            SET status = 'sending',
                attempt_count = ?,
                last_attempt_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (attempt_count, now_iso, now_iso, outbox_id),
        )
        await db.commit()


async def mark_bot_delivery_outbox_sent(outbox_id: int, attempt_count: int):
    now_iso = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE bot_delivery_outbox
            SET status = 'sent',
                attempt_count = ?,
                last_error = NULL,
                next_retry_at = NULL,
                sent_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (attempt_count, now_iso, now_iso, outbox_id),
        )
        await db.commit()


async def schedule_bot_delivery_outbox_retry(
    outbox_id: int,
    attempt_count: int,
    last_error: str,
    next_retry_at: str | None,
):
    now_iso = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE bot_delivery_outbox
            SET status = 'pending',
                attempt_count = ?,
                last_error = ?,
                next_retry_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (attempt_count, str(last_error or "")[:2000], next_retry_at, now_iso, outbox_id),
        )
        await db.commit()


async def mark_bot_delivery_outbox_failed(outbox_id: int, attempt_count: int, last_error: str):
    now_iso = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE bot_delivery_outbox
            SET status = 'failed',
                attempt_count = ?,
                last_error = ?,
                next_retry_at = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (attempt_count, str(last_error or "")[:2000], now_iso, outbox_id),
        )
        await db.commit()


async def get_recent_confirmed_direct_orders_missing_delivery(limit: int = 50, hours: int = 48):
    cutoff_iso = (datetime.now() - timedelta(hours=max(1, int(hours or 48)))).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT d.*
            FROM direct_orders d
            LEFT JOIN bot_delivery_outbox o ON o.direct_order_id = d.id
            WHERE d.status = 'confirmed'
              AND d.created_at >= ?
              AND o.id IS NULL
            ORDER BY d.created_at DESC, d.id DESC
            LIMIT ?
            """,
            (cutoff_iso, max(1, int(limit or 50))),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def build_bot_delivery_payload_for_direct_order(direct_order_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT id, user_id, product_id, quantity, bonus_quantity, amount, code, created_at, status
            FROM direct_orders
            WHERE id = ?
            LIMIT 1
            """,
            (direct_order_id,),
        )
        direct_order = await cursor.fetchone()
        if not direct_order or str(direct_order["status"] or "") != "confirmed":
            return None

        cursor = await db.execute(
            "SELECT id, name, description, format_data FROM products WHERE id = ? LIMIT 1",
            (direct_order["product_id"],),
        )
        product = await cursor.fetchone()

        expected_delivered_quantity = max(1, int(direct_order["quantity"] or 1)) + max(0, int(direct_order["bonus_quantity"] or 0))
        cursor = await db.execute(
            """
            SELECT id, content, price, quantity, order_group, created_at
            FROM orders
            WHERE user_id = ? AND product_id = ? AND created_at >= ?
            ORDER BY created_at ASC, id ASC
            LIMIT 20
            """,
            (direct_order["user_id"], direct_order["product_id"], direct_order["created_at"]),
        )
        order_rows = await cursor.fetchall()
        if not order_rows:
            return None

        selected = None
        for row in order_rows:
            items = _parse_json_list(row["content"])
            delivered_quantity = max(len(items), int(row["quantity"] or 0))
            if delivered_quantity == expected_delivered_quantity and int(row["price"] or 0) == int(direct_order["amount"] or 0):
                selected = (row, items)
                break
        if selected is None:
            for row in order_rows:
                items = _parse_json_list(row["content"])
                if len(items) == expected_delivered_quantity:
                    selected = (row, items)
                    break
        if selected is None:
            for row in order_rows:
                items = _parse_json_list(row["content"])
                if items:
                    selected = (row, items)
                    break
        if selected is None:
            return None

        order_row, items = selected
        return {
            "directOrderId": int(direct_order["id"] or 0),
            "orderId": int(order_row["id"] or 0) or None,
            "userId": int(direct_order["user_id"] or 0),
            "productId": int(direct_order["product_id"] or 0),
            "productName": (product["name"] if product else f"#{int(direct_order['product_id'] or 0)}") or f"#{int(direct_order['product_id'] or 0)}",
            "description": (product["description"] if product else "") or "",
            "formatData": (product["format_data"] if product else "") or "",
            "quantity": max(1, int(direct_order["quantity"] or 1)),
            "bonusQuantity": max(0, int(direct_order["bonus_quantity"] or 0)),
            "deliveredQuantity": max(1, len(items) or expected_delivered_quantity),
            "amount": max(0, int(direct_order["amount"] or 0)),
            "code": str(direct_order["code"] or ""),
            "orderGroup": str(order_row["order_group"] or ""),
            "items": [str(item or "") for item in items],
        }


async def fulfill_bot_direct_order(
    order_id: int,
    order_group: str | None = None,
    expire_minutes: int = 10,
):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT id, user_id, product_id, quantity, bonus_quantity, unit_price, amount, code, status, created_at
               FROM direct_orders WHERE id = ?""",
            (order_id,),
        )
        row = await cursor.fetchone()
        if not row:
            raise DirectOrderFulfillmentError("direct_order_not_found")
        if str(row[8] or "") != "pending":
            raise DirectOrderFulfillmentError("direct_order_not_pending")

        created_at = row[9]
        try:
            created_dt = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
        except Exception:
            created_dt = None
        if created_dt and (datetime.now(created_dt.tzinfo) if created_dt.tzinfo else datetime.now()) - created_dt >= timedelta(minutes=max(1, int(expire_minutes or 10))):
            await db.execute("UPDATE direct_orders SET status = 'cancelled' WHERE id = ?", (order_id,))
            await db.commit()
            raise DirectOrderFulfillmentError("direct_order_expired")

        product_id = int(row[2] or 0)
        quantity = int(row[3] or 1)
        bonus_quantity = int(row[4] or 0)
        deliver_quantity = max(1, quantity + max(0, bonus_quantity))

        cursor = await db.execute(
            "SELECT id, content FROM stock WHERE product_id = ? AND sold = 0 ORDER BY id LIMIT ?",
            (product_id, deliver_quantity),
        )
        stocks = await cursor.fetchall()
        if not stocks or len(stocks) < deliver_quantity:
            await db.execute("UPDATE direct_orders SET status = 'failed' WHERE id = ?", (order_id,))
            await db.commit()
            raise DirectOrderFulfillmentError("not_enough_stock")

        stock_ids = [stock[0] for stock in stocks]
        items = [stock[1] for stock in stocks]
        placeholders = ",".join("?" * len(stock_ids))
        await db.execute(f"UPDATE stock SET sold = 1 WHERE id IN ({placeholders})", stock_ids)

        final_order_group = (order_group or "").strip() or f"PAY{row[1]}{datetime.now().strftime('%Y%m%d%H%M%S')}"
        total_price = int(row[6] or 0) or (int(row[5] or 0) * max(1, quantity))
        await db.execute(
            "INSERT INTO orders (user_id, product_id, content, price, quantity, order_group, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row[1], product_id, json.dumps(items), total_price, len(items), final_order_group, datetime.now().isoformat()),
        )
        await db.execute("UPDATE direct_orders SET status = 'confirmed' WHERE id = ?", (order_id,))
        await db.commit()

        cursor = await db.execute(
            "SELECT name, description, format_data FROM products WHERE id = ?",
            (product_id,),
        )
        product_row = await cursor.fetchone()

        return {
            "direct_order_id": int(row[0]),
            "order_id": None,
            "user_id": int(row[1]),
            "product_id": product_id,
            "product_name": (product_row[0] if product_row else f"#{product_id}") or f"#{product_id}",
            "description": (product_row[1] if product_row else "") or "",
            "format_data": (product_row[2] if product_row else "") or "",
            "quantity": quantity,
            "bonus_quantity": bonus_quantity,
            "delivered_quantity": len(items),
            "unit_price": int(row[5] or 0),
            "amount": total_price,
            "code": str(row[7] or ""),
            "order_group": final_order_group,
            "items": items,
        }


async def fulfill_website_direct_order(
    website_direct_order_id: int,
    order_group: str | None = None,
    expire_minutes: int = 10,
):
    raise DirectOrderFulfillmentError(
        "website_direct_order_not_supported",
        "website_direct_order_not_supported_in_sqlite",
    )

async def get_pending_deposits():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, user_id, amount, code, created_at FROM deposits WHERE status = 'pending'"
        )
        return await cursor.fetchall()

async def confirm_deposit(deposit_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, amount FROM deposits WHERE id = ?", (deposit_id,))
        row = await cursor.fetchone()
        if row:
            await db.execute("UPDATE deposits SET status = 'confirmed' WHERE id = ?", (deposit_id,))
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (row[1], row[0]))
            await db.commit()
            return row
        return None

async def cancel_deposit(deposit_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deposits SET status = 'cancelled' WHERE id = ?", (deposit_id,))
        await db.commit()

async def set_deposit_status(deposit_id: int, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deposits SET status = ? WHERE id = ?", (status, deposit_id))
        await db.commit()

# Stats
async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        users = (await (await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0]
        orders = (await (await db.execute("SELECT COUNT(*) FROM orders")).fetchone())[0]
        revenue = (await (await db.execute("SELECT COALESCE(SUM(price), 0) FROM orders")).fetchone())[0]
        return {"users": users, "orders": orders, "revenue": revenue}

async def get_all_user_ids():
    """Lấy tất cả user_id để gửi thông báo"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users")
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

# Withdrawal functions
async def create_withdrawal(user_id: int, amount: int, momo_phone: str):
    async with aiosqlite.connect(DB_PATH) as db:
        # Chỉ tạo yêu cầu, KHÔNG trừ tiền - sẽ trừ khi admin duyệt
        await db.execute(
            "INSERT INTO withdrawals (user_id, amount, momo_phone, created_at) VALUES (?, ?, ?, ?)",
            (user_id, amount, momo_phone, datetime.now().isoformat())
        )
        await db.commit()

async def get_pending_withdrawals():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, user_id, amount, momo_phone, created_at FROM withdrawals WHERE status = 'pending'"
        )
        return await cursor.fetchall()

async def get_withdrawal_detail(withdrawal_id: int):
    """Lấy chi tiết một yêu cầu rút tiền"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, user_id, amount, momo_phone, status, created_at FROM withdrawals WHERE id = ?",
            (withdrawal_id,)
        )
        return await cursor.fetchone()

async def get_user_pending_withdrawal(user_id: int):
    """Kiểm tra user có yêu cầu rút tiền đang pending không, trả về số tiền pending"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT SUM(amount) FROM withdrawals WHERE user_id = ? AND status = 'pending'",
            (user_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row and row[0] else 0

async def confirm_withdrawal(withdrawal_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, amount, momo_phone FROM withdrawals WHERE id = ?", (withdrawal_id,))
        row = await cursor.fetchone()
        if row:
            user_id, amount, bank_info = row
            
            # Check số dư trước khi trừ
            cursor = await db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
            balance_row = await cursor.fetchone()
            if not balance_row or balance_row[0] < amount:
                return None  # Không đủ tiền
            
            # Trừ tiền khi admin duyệt
            await db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ? AND balance >= ?", (amount, user_id, amount))
            await db.execute("UPDATE withdrawals SET status = 'confirmed' WHERE id = ?", (withdrawal_id,))
            await db.commit()
            return row  # (user_id, amount, bank_info)
        return None

async def cancel_withdrawal(withdrawal_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, amount FROM withdrawals WHERE id = ?", (withdrawal_id,))
        row = await cursor.fetchone()
        if row:
            # Không cần hoàn tiền vì chưa trừ
            await db.execute("UPDATE withdrawals SET status = 'cancelled' WHERE id = ?", (withdrawal_id,))
            await db.commit()
            return row
        return None

# Settings functions
async def get_setting(key: str, default: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row[0] if row else default

async def set_setting(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value)
        )
        await db.commit()

async def get_bank_settings():
    return {
        "bank_name": await get_setting("bank_name", ""),
        "account_number": await get_setting("account_number", ""),
        "account_name": await get_setting("account_name", ""),
        "sepay_token": await get_setting("sepay_token", ""),
    }

# Binance on-chain direct-order helpers
async def create_binance_direct_order(
    user_id: int,
    product_id: int,
    quantity: int,
    unit_price: int,
    amount: int,
    code: str,
    *,
    bonus_quantity: int = 0,
    payment_asset: str,
    payment_network: str,
    payment_amount_asset: float,
    payment_rate_vnd: float,
    payment_address: str,
    payment_address_tag: str = "",
):
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            cursor = await db.execute(
                """INSERT INTO direct_orders (
                       user_id, product_id, quantity, bonus_quantity, unit_price, amount, code,
                       payment_channel, payment_asset, payment_network, payment_amount_asset,
                       payment_rate_vnd, payment_address, payment_address_tag, created_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, 'binance_onchain', ?, ?, ?, ?, ?, ?, ?)""",
                (
                    user_id,
                    product_id,
                    quantity,
                    bonus_quantity,
                    unit_price,
                    amount,
                    code,
                    payment_asset,
                    payment_network,
                    payment_amount_asset,
                    payment_rate_vnd,
                    payment_address,
                    payment_address_tag or "",
                    datetime.now().isoformat(),
                ),
            )
        except aiosqlite.IntegrityError as exc:
            message = str(exc).lower()
            if "direct_orders_pending_binance_amount_idx" in message or "unique" in message:
                raise BinanceDirectOrderError("duplicate_binance_amount", str(exc))
            raise
        await db.commit()
        return {
            "direct_order_id": cursor.lastrowid,
            "code": code,
            "payment_asset": payment_asset,
            "payment_network": payment_network,
            "payment_amount_asset": payment_amount_asset,
            "payment_address": payment_address,
            "payment_address_tag": payment_address_tag or "",
            "created_at": datetime.now().isoformat(),
        }


async def get_pending_binance_direct_orders():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT
                   id, user_id, product_id, quantity, bonus_quantity, unit_price, amount, code, created_at,
                   payment_asset, payment_network, payment_amount_asset, payment_rate_vnd,
                   payment_address, payment_address_tag, external_payment_id, external_tx_id, external_paid_at
               FROM direct_orders
               WHERE status = 'pending' AND payment_channel = 'binance_onchain'
               ORDER BY created_at ASC"""
        )
        rows = await cursor.fetchall()
    return [
        {
            "id": row[0],
            "user_id": row[1],
            "product_id": row[2],
            "quantity": row[3] or 1,
            "bonus_quantity": row[4] or 0,
            "unit_price": row[5] or 0,
            "amount": row[6] or 0,
            "code": row[7] or "",
            "created_at": row[8],
            "payment_asset": row[9] or "",
            "payment_network": row[10] or "",
            "payment_amount_asset": row[11] or 0,
            "payment_rate_vnd": row[12] or 0,
            "payment_address": row[13] or "",
            "payment_address_tag": row[14] or "",
            "external_payment_id": row[15] or "",
            "external_tx_id": row[16] or "",
            "external_paid_at": row[17],
        }
        for row in rows
    ]


async def record_direct_order_external_payment(
    order_id: int,
    *,
    payment_id: str,
    tx_id: str,
    paid_at: str | None,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE direct_orders
               SET external_payment_id = ?, external_tx_id = ?, external_paid_at = ?
               WHERE id = ?""",
            (payment_id, tx_id, paid_at, order_id),
        )
        await db.commit()


async def is_processed_binance_deposit(payment_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM binance_processed_deposits WHERE payment_id = ?",
            (str(payment_id or "").strip(),),
        )
        return await cursor.fetchone() is not None


async def mark_processed_binance_deposit(
    payment_id: str,
    *,
    tx_id: str,
    direct_order_id: int | None,
    amount_asset: float,
    payment_asset: str,
    payment_network: str,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT OR REPLACE INTO binance_processed_deposits (
                   payment_id, tx_id, direct_order_id, amount_asset, payment_asset, payment_network, processed_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                str(payment_id or "").strip(),
                str(tx_id or "").strip(),
                direct_order_id,
                amount_asset,
                payment_asset,
                payment_network,
                datetime.now().isoformat(),
            ),
        )
        await db.commit()


# USDT Withdrawal functions
async def create_usdt_withdrawal(user_id: int, usdt_amount: float, wallet_address: str, network: str = "TRC20"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO usdt_withdrawals (user_id, usdt_amount, wallet_address, network, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, usdt_amount, wallet_address, network, datetime.now().isoformat())
        )
        await db.commit()

async def get_pending_usdt_withdrawals():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, user_id, usdt_amount, wallet_address, network, created_at FROM usdt_withdrawals WHERE status = 'pending'"
        )
        return await cursor.fetchall()

async def get_usdt_withdrawal_detail(withdrawal_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, user_id, usdt_amount, wallet_address, network, status, created_at FROM usdt_withdrawals WHERE id = ?",
            (withdrawal_id,)
        )
        return await cursor.fetchone()

async def get_user_pending_usdt_withdrawal(user_id: int):
    """Kiểm tra user có yêu cầu rút USDT đang pending không"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT SUM(usdt_amount) FROM usdt_withdrawals WHERE user_id = ? AND status = 'pending'",
            (user_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row and row[0] else 0

async def confirm_usdt_withdrawal(withdrawal_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, usdt_amount, wallet_address FROM usdt_withdrawals WHERE id = ?", (withdrawal_id,))
        row = await cursor.fetchone()
        if row:
            user_id, usdt_amount, wallet_address = row
            
            # Check số dư USDT trước khi trừ
            cursor = await db.execute("SELECT balance_usdt FROM users WHERE user_id = ?", (user_id,))
            balance_row = await cursor.fetchone()
            if not balance_row or (balance_row[0] or 0) < usdt_amount:
                return None  # Không đủ tiền
            
            # Trừ tiền khi admin duyệt
            await db.execute("UPDATE users SET balance_usdt = balance_usdt - ? WHERE user_id = ? AND balance_usdt >= ?", (usdt_amount, user_id, usdt_amount))
            await db.execute("UPDATE usdt_withdrawals SET status = 'confirmed' WHERE id = ?", (withdrawal_id,))
            await db.commit()
            return row  # (user_id, usdt_amount, wallet_address)
        return None

async def cancel_usdt_withdrawal(withdrawal_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, usdt_amount FROM usdt_withdrawals WHERE id = ?", (withdrawal_id,))
        row = await cursor.fetchone()
        if row:
            await db.execute("UPDATE usdt_withdrawals SET status = 'cancelled' WHERE id = ?", (withdrawal_id,))
            await db.commit()
            return row
        return None

# SePay processed transactions
async def is_processed_transaction(tx_id: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM processed_transactions WHERE tx_id = ?", (tx_id,))
        return await cursor.fetchone() is not None

async def mark_processed_transaction(tx_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO processed_transactions (tx_id) VALUES (?)", (tx_id,))
        await db.commit()

async def get_ui_flags():
    return {
        "show_shop": _parse_bool(await get_setting("show_shop", "true")),
        "show_balance": _parse_bool(await get_setting("show_balance", "true")),
        "show_deposit": _parse_bool(await get_setting("show_deposit", "true")),
        "show_withdraw": _parse_bool(await get_setting("show_withdraw", "true")),
        "show_usdt": _parse_bool(await get_setting("show_usdt", "true")),
        "show_history": _parse_bool(await get_setting("show_history", "true")),
        "show_language": _parse_bool(await get_setting("show_language", "true")),
        "show_support": _parse_bool(await get_setting("show_support", "true")),
    }
