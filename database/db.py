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

DB_PATH = "data/shop.db"


class DirectOrderFulfillmentError(RuntimeError):
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
        # Binance deposits table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS binance_deposits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                usdt_amount REAL,
                vnd_amount INTEGER,
                code TEXT,
                screenshot_file_id TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT
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
                status TEXT DEFAULT 'pending',
                created_at TEXT
            )
        """)
        try:
            await db.execute("ALTER TABLE direct_orders ADD COLUMN bonus_quantity INTEGER DEFAULT 0")
        except:
            pass
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
               FROM direct_orders WHERE status = 'pending'"""
        )
        return await cursor.fetchall()

async def set_direct_order_status(order_id: int, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE direct_orders SET status = ? WHERE id = ?", (status, order_id))
        await db.commit()


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

# Binance deposit functions
async def create_binance_deposit(user_id: int, usdt_amount: float, vnd_amount: int, code: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO binance_deposits (user_id, usdt_amount, vnd_amount, code, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, usdt_amount, vnd_amount, code, datetime.now().isoformat())
        )
        await db.commit()

async def update_binance_deposit_screenshot(user_id: int, code: str, file_id: str):
    """Cập nhật screenshot cho deposit"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE binance_deposits SET screenshot_file_id = ? WHERE user_id = ? AND code = ? AND status = 'pending'",
            (file_id, user_id, code)
        )
        await db.commit()

async def get_pending_binance_deposits():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, user_id, usdt_amount, vnd_amount, code, screenshot_file_id, created_at FROM binance_deposits WHERE status = 'pending' AND screenshot_file_id IS NOT NULL"
        )
        return await cursor.fetchall()

async def get_binance_deposit_detail(deposit_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, user_id, usdt_amount, vnd_amount, code, screenshot_file_id, status, created_at FROM binance_deposits WHERE id = ?",
            (deposit_id,)
        )
        return await cursor.fetchone()

async def confirm_binance_deposit(deposit_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id, usdt_amount FROM binance_deposits WHERE id = ?", (deposit_id,))
        row = await cursor.fetchone()
        if row:
            await db.execute("UPDATE binance_deposits SET status = 'confirmed' WHERE id = ?", (deposit_id,))
            await db.execute("UPDATE users SET balance_usdt = balance_usdt + ? WHERE user_id = ?", (row[1], row[0]))
            await db.commit()
            return row  # (user_id, usdt_amount)
        return None

async def cancel_binance_deposit(deposit_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE binance_deposits SET status = 'cancelled' WHERE id = ?", (deposit_id,))
        await db.commit()

async def get_user_pending_binance_deposit(user_id: int):
    """Lấy deposit binance đang pending của user"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, usdt_amount, vnd_amount, code FROM binance_deposits WHERE user_id = ? AND status = 'pending' ORDER BY id DESC LIMIT 1",
            (user_id,)
        )
        return await cursor.fetchone()


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
