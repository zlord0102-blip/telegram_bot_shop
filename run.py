import asyncio
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from telegram import BotCommand, BotCommandScopeChat, BotCommandScopeDefault
from telegram.error import BadRequest, NetworkError, TimedOut
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters
)
from config import ADMIN_IDS, BOT_TOKEN
from database import init_db
from handlers.chat_logger import log_incoming_message
from handlers.start import (
    start_command,
    help_command,
    settings_command,
    back_to_main,
    handle_history_text,
    handle_balance,
    handle_support_text,
    handle_support_callback,
    set_language,
    handle_change_language,
    delete_message,
)
from handlers.shop import (
    show_shop, show_shop_folder, show_product, confirm_buy, show_account,
    show_sale_catalog, show_sale_product, sale_command,
    show_history, show_deposit, process_deposit, handle_deposit_text,
    handle_shop_text, process_deposit_amount,
    handle_withdraw_text, process_withdraw_amount, process_withdraw_bank, process_withdraw_account,
    handle_buy_quantity, show_order_detail,
    select_payment_vnd, select_payment_usdt, select_sale_payment_vnd, select_sale_payment_usdt,
    select_quick_quantity, prompt_manual_quantity, prompt_quick_quantity,
    select_sale_quick_quantity, prompt_sale_manual_quantity, prompt_sale_quick_quantity,
    select_direct_payment_vietqr, select_direct_payment_binance,
    select_sale_direct_payment_vietqr, select_sale_direct_payment_binance,
    show_direct_order_status,
    search_products_command,
    WAITING_DEPOSIT_AMOUNT, WAITING_WITHDRAW_AMOUNT, WAITING_WITHDRAW_BANK, WAITING_WITHDRAW_ACCOUNT
)
from handlers.admin import (
    admin_command, admin_callback, admin_products, admin_delete_product, admin_confirm_delete_product,
    admin_add_product_start, admin_add_product_name, admin_add_product_price, admin_add_product_price_usdt,
    admin_add_stock_menu, admin_select_stock_product, admin_add_stock_content,
    admin_deposits, admin_confirm_deposit, admin_cancel_deposit,
    admin_withdrawals, admin_view_withdrawal, admin_confirm_withdrawal, admin_cancel_withdrawal,
    admin_bank_settings, refresh_bank_info, set_bank_name_start, set_bank_name_done,
    set_account_number_start, set_account_number_done,
    set_account_name_start, set_account_name_done,
    set_sepay_token_start, set_sepay_token_done,
    set_admin_contact_start, set_admin_contact_done,
    cancel_conversation, ADD_PRODUCT_NAME, ADD_PRODUCT_PRICE, ADD_PRODUCT_PRICE_USDT, ADD_STOCK_CONTENT,
    BANK_NAME, ACCOUNT_NUMBER, ACCOUNT_NAME, SEPAY_TOKEN, ADMIN_CONTACT, NOTIFICATION_MESSAGE, EDIT_STOCK_CONTENT, SEARCH_USER_ID,
    handle_admin_products_text, handle_admin_stock_text, handle_admin_transactions_text,
    handle_admin_bank_text, handle_exit_admin, notification_command, notification_send,
    status_command, emoji_id_command,
    admin_manage_stock, admin_view_stock, admin_stock_page, admin_stock_detail,
    admin_edit_stock_start, admin_edit_stock_done, admin_delete_stock, handle_admin_manage_stock_text,
    admin_export_stock, admin_clear_unsold_stock, admin_clear_all_stock,
    admin_sold_codes_menu, admin_sold_by_product, admin_export_sold_codes,
    admin_sold_by_user_start, admin_sold_by_user_search, handle_admin_sold_codes_text,
)
from sepay_checker import run_checker, init_checker_db
from helpers.telegram_resilience import is_stale_callback_query_error

def _env_positive_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        return max(1, int(str(raw_value).strip()))
    except (TypeError, ValueError):
        return default


LOG_FILE_MAX_BYTES = _env_positive_int("BOT_LOG_MAX_BYTES", 5 * 1024 * 1024)
LOG_FILE_BACKUP_COUNT = _env_positive_int("BOT_LOG_BACKUP_COUNT", 7)

logger = logging.getLogger(__name__)
_LOGGING_CONFIGURED = False


def setup_logging():
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(sys.stdout),
            RotatingFileHandler(
                'bot.log',
                maxBytes=LOG_FILE_MAX_BYTES,
                backupCount=LOG_FILE_BACKUP_COUNT,
                encoding='utf-8'
            )
        ]
    )
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('telegram').setLevel(logging.WARNING)
    _LOGGING_CONFIGURED = True

async def post_init(application):
    if application.bot_data.get("_shop_post_init_done"):
        return
    application.bot_data["_shop_post_init_done"] = True

    default_commands = [
        BotCommand("start", "Open bot"),
        BotCommand("shop", "Shop"),
        BotCommand("sale", "Sale deals"),
        BotCommand("search", "Search products"),
        BotCommand("balance", "Check balance"),
        BotCommand("deposit", "Deposit funds"),
        BotCommand("history", "Order history"),
        BotCommand("support", "Get support"),
        BotCommand("settings", "Settings"),
        BotCommand("help", "Help"),
    ]
    admin_commands = default_commands + [
        BotCommand("admin", "Admin panel"),
        BotCommand("status", "Bot status"),
        BotCommand("emojiid", "Get custom emoji ID"),
        BotCommand("notification", "Broadcast message"),
    ]

    try:
        await application.bot.set_my_commands(default_commands, scope=BotCommandScopeDefault())
        registered_admin_scopes = 0
        for admin_id in ADMIN_IDS:
            try:
                await application.bot.set_my_commands(
                    admin_commands,
                    scope=BotCommandScopeChat(chat_id=admin_id),
                )
                registered_admin_scopes += 1
            except Exception:
                logger.exception("Failed to set admin command menu for chat_id=%s", admin_id)
        logger.info(
            "✅ Telegram command menu registered (%s default commands, %s admin chat scopes)",
            len(default_commands),
            registered_admin_scopes,
        )
    except Exception:
        logger.exception("Failed to set Telegram command menu")


async def handle_application_error(update, context):
    error = context.error
    if is_stale_callback_query_error(error):
        logger.info("Ignored stale callback query from application error handler: %s", error)
        return

    if isinstance(error, BadRequest) and "message is not modified" in str(error).lower():
        logger.info("Ignored Telegram message-not-modified response")
        return

    if isinstance(error, (NetworkError, TimedOut)):
        logger.warning("Telegram network error while processing update: %s", error)
        return

    exc_info = (type(error), error, error.__traceback__) if error else None
    logger.error("Unhandled exception while processing update", exc_info=exc_info)


def setup_bot():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .connect_timeout(_env_positive_int("BOT_TELEGRAM_CONNECT_TIMEOUT", 30))
        .read_timeout(_env_positive_int("BOT_TELEGRAM_READ_TIMEOUT", 45))
        .write_timeout(_env_positive_int("BOT_TELEGRAM_WRITE_TIMEOUT", 45))
        .pool_timeout(_env_positive_int("BOT_TELEGRAM_POOL_TIMEOUT", 30))
        .build()
    )
    app.add_error_handler(handle_application_error)

    # Log incoming updates before other handlers (conversation handlers may stop processing).
    app.add_handler(MessageHandler(filters.ALL, log_incoming_message), group=-1)

    # Add product conversation
    add_product_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_add_product_start, pattern="^admin_add_product$")],
        states={
            ADD_PRODUCT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_product_name)],
            ADD_PRODUCT_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_product_price)],
            ADD_PRODUCT_PRICE_USDT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_product_price_usdt)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    # Add stock conversation (hỗ trợ cả text và file .txt)
    add_stock_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_select_stock_product, pattern="^admin_stock_\\d+$")],
        states={
            ADD_STOCK_CONTENT: [MessageHandler((filters.TEXT | filters.Document.TXT) & ~filters.COMMAND, admin_add_stock_content)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    # Edit stock conversation
    edit_stock_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_edit_stock_start, pattern="^admin_editstock_\\d+$")],
        states={
            EDIT_STOCK_CONTENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_stock_done)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    # Search sold codes by user conversation
    search_user_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_sold_by_user_start, pattern="^admin_soldby_user$")],
        states={
            SEARCH_USER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_sold_by_user_search)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    # Deposit conversation - support both languages
    deposit_conv = ConversationHandler(
        entry_points=[
            CommandHandler("deposit", handle_deposit_text),
            MessageHandler(filters.Regex("^(➕ Nạp tiền|➕ Deposit)$"), handle_deposit_text),
        ],
        states={
            WAITING_DEPOSIT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_deposit_amount)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    # Withdraw conversation (VND only for Vietnamese users)
    withdraw_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^💸 Rút tiền$"), handle_withdraw_text)],
        states={
            WAITING_WITHDRAW_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_withdraw_amount)],
            WAITING_WITHDRAW_BANK: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_withdraw_bank)],
            WAITING_WITHDRAW_ACCOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_withdraw_account)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    # Bank settings conversations
    bank_name_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(set_bank_name_start, pattern="^set_bank_name$")],
        states={BANK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_bank_name_done)]},
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )
    account_number_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(set_account_number_start, pattern="^set_account_number$")],
        states={ACCOUNT_NUMBER: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_account_number_done)]},
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )
    account_name_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(set_account_name_start, pattern="^set_account_name$")],
        states={ACCOUNT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_account_name_done)]},
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )
    sepay_token_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(set_sepay_token_start, pattern="^set_sepay_token$")],
        states={SEPAY_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_sepay_token_done)]},
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    admin_contact_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(set_admin_contact_start, pattern="^set_admin_contact$")],
        states={ADMIN_CONTACT: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_admin_contact_done)]},
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    # Commands
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("shop", handle_shop_text))
    app.add_handler(CommandHandler("sale", sale_command))
    app.add_handler(CommandHandler("search", search_products_command))
    app.add_handler(CommandHandler("balance", handle_balance))
    app.add_handler(CommandHandler("history", handle_history_text))
    app.add_handler(CommandHandler("support", handle_support_text))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("emojiid", emoji_id_command))

    # Notification conversation
    notification_conv = ConversationHandler(
        entry_points=[CommandHandler("notification", notification_command)],
        states={
            NOTIFICATION_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, notification_send)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )
    app.add_handler(notification_conv)

    # Conversations
    app.add_handler(add_product_conv)
    app.add_handler(add_stock_conv)
    app.add_handler(edit_stock_conv)
    app.add_handler(search_user_conv)
    app.add_handler(deposit_conv)
    app.add_handler(withdraw_conv)
    app.add_handler(bank_name_conv)
    app.add_handler(account_number_conv)
    app.add_handler(account_name_conv)
    app.add_handler(sepay_token_conv)
    app.add_handler(admin_contact_conv)

    # Reply keyboard handlers (user) - support both languages
    app.add_handler(MessageHandler(filters.Regex("^(📜 Lịch sử mua|📜 Lịch sử|📜 History)$"), handle_history_text))
    app.add_handler(MessageHandler(filters.Regex("^(💰 Số dư|💰 Balance)$"), handle_balance))
    app.add_handler(MessageHandler(filters.Regex("^(🛒 Mua hàng|🛒 Danh mục|🛒 Shop)$"), handle_shop_text))
    app.add_handler(MessageHandler(filters.Regex("^(💬 Hỗ trợ|💬 Support|🆘 Hỗ trợ|🆘 Support)$"), handle_support_text))
    app.add_handler(MessageHandler(filters.Regex("^(🌐 Ngôn ngữ|🌐 Language)$"), handle_change_language))

    # Admin reply keyboard handlers
    app.add_handler(MessageHandler(filters.Regex("^📦 Quản lý SP$"), handle_admin_products_text))
    app.add_handler(MessageHandler(filters.Regex("^📥 Thêm stock$"), handle_admin_stock_text))
    app.add_handler(MessageHandler(filters.Regex("^📋 Xem stock$"), handle_admin_manage_stock_text))
    app.add_handler(MessageHandler(filters.Regex("^📜 Code đã bán$"), handle_admin_sold_codes_text))
    app.add_handler(MessageHandler(filters.Regex("^✅ Duyệt giao dịch$"), handle_admin_transactions_text))
    app.add_handler(MessageHandler(filters.Regex("^🏦 Cài đặt NH$"), handle_admin_bank_text))
    app.add_handler(MessageHandler(filters.Regex("^🚪 Thoát Admin$"), handle_exit_admin))

    # Handler nhập số lượng mua: nhận cả số hợp lệ lẫn text sai để bot có thể nhắc lại trong cùng flow
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buy_quantity))

    # User callbacks
    app.add_handler(CallbackQueryHandler(set_language, pattern="^set_lang_(vi|en)$"))
    app.add_handler(CallbackQueryHandler(delete_message, pattern="^delete_msg$"))
    app.add_handler(CallbackQueryHandler(back_to_main, pattern="^back_main$"))
    app.add_handler(CallbackQueryHandler(handle_support_callback, pattern="^support$"))
    app.add_handler(CallbackQueryHandler(show_shop, pattern="^shop(?:_\\d+)?$"))
    app.add_handler(CallbackQueryHandler(show_shop_folder, pattern="^shopfolder_\\d+_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(show_sale_catalog, pattern="^sale(?:_\\d+)?$"))
    app.add_handler(CallbackQueryHandler(show_sale_product, pattern="^salebuy_\\d+$"))
    app.add_handler(CallbackQueryHandler(show_product, pattern="^buy_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_payment_vnd, pattern="^pay_vnd_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_payment_usdt, pattern="^pay_usdt_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_sale_payment_vnd, pattern="^salepay_vnd_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_sale_payment_usdt, pattern="^salepay_usdt_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_quick_quantity, pattern="^buyqty_(?:vnd|usdt)_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(prompt_manual_quantity, pattern="^buyqtymanual_(?:vnd|usdt)_\\d+$"))
    app.add_handler(CallbackQueryHandler(prompt_quick_quantity, pattern="^buyqtyquick_(?:vnd|usdt)_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_sale_quick_quantity, pattern="^salebuyqty_(?:vnd|usdt)_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(prompt_sale_manual_quantity, pattern="^salebuyqtymanual_(?:vnd|usdt)_\\d+$"))
    app.add_handler(CallbackQueryHandler(prompt_sale_quick_quantity, pattern="^salebuyqtyquick_(?:vnd|usdt)_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_direct_payment_vietqr, pattern="^directpay_vietqr_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_direct_payment_binance, pattern="^directpay_binance_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_sale_direct_payment_vietqr, pattern="^saledirectpay_vietqr_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_sale_direct_payment_binance, pattern="^saledirectpay_binance_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(show_direct_order_status, pattern="^directstatus:.+$"))
    app.add_handler(CallbackQueryHandler(show_account, pattern="^account$"))
    app.add_handler(CallbackQueryHandler(show_history, pattern="^history(?:_page_\\d+)?$"))
    app.add_handler(CallbackQueryHandler(show_order_detail, pattern="^order_detail_\\d+$"))
    app.add_handler(CallbackQueryHandler(show_deposit, pattern="^deposit$"))
    app.add_handler(CallbackQueryHandler(process_deposit, pattern="^deposit_\\d+$"))

    # Admin callbacks
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^admin$"))
    app.add_handler(CallbackQueryHandler(admin_products, pattern="^admin_products$"))
    app.add_handler(CallbackQueryHandler(admin_delete_product, pattern="^admin_del_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_confirm_delete_product, pattern="^admin_confirmdel_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_add_stock_menu, pattern="^admin_add_stock$"))
    app.add_handler(CallbackQueryHandler(admin_deposits, pattern="^admin_deposits$"))
    app.add_handler(CallbackQueryHandler(admin_confirm_deposit, pattern="^admin_confirm_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_cancel_deposit, pattern="^admin_cancel_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_withdrawals, pattern="^admin_withdraws$"))
    app.add_handler(CallbackQueryHandler(admin_view_withdrawal, pattern="^admin_view_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_confirm_withdrawal, pattern="^admin_confirm_withdraw_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_cancel_withdrawal, pattern="^admin_cancel_withdraw_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_bank_settings, pattern="^admin_bank_settings$"))
    app.add_handler(CallbackQueryHandler(refresh_bank_info, pattern="^refresh_bank_info$"))

    # Stock management callbacks
    app.add_handler(CallbackQueryHandler(admin_manage_stock, pattern="^admin_manage_stock$"))
    app.add_handler(CallbackQueryHandler(admin_view_stock, pattern="^admin_viewstock_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_stock_page, pattern="^admin_stockpage_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_stock_detail, pattern="^admin_stockdetail_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_delete_stock, pattern="^admin_delstock_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_export_stock, pattern="^admin_export_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_clear_unsold_stock, pattern="^admin_clearunsold_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_clear_all_stock, pattern="^admin_clearall_\\d+$"))

    # Sold codes management callbacks
    app.add_handler(CallbackQueryHandler(admin_sold_codes_menu, pattern="^admin_sold_codes$"))
    app.add_handler(CallbackQueryHandler(admin_sold_by_product, pattern="^admin_soldby_product_\\d+$"))
    app.add_handler(CallbackQueryHandler(admin_export_sold_codes, pattern="^admin_export_sold_\\d+$"))

    # Binance deposits callbacks

    return app

async def main():
    setup_logging()
    # Init database FIRST
    os.makedirs("data", exist_ok=True)
    logger.info("📁 Data directory ready")
    await init_db()
    logger.info("✅ Main database initialized!")
    await init_checker_db()
    logger.info("✅ Checker database initialized!")

    bot_app = setup_bot()

    logger.info("🤖 Bot is starting...")
    await bot_app.initialize()
    await post_init(bot_app)
    await bot_app.start()
    await bot_app.updater.start_polling(drop_pending_updates=True)

    # Start SePay checker
    asyncio.create_task(run_checker(bot_app, interval=30))
    logger.info("🔄 SePay auto-checker enabled (30s interval)")

    # Keep running
    stop_event = asyncio.Event()

    def signal_handler():
        stop_event.set()

    try:
        import signal
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_event_loop().add_signal_handler(sig, signal_handler)
    except (NotImplementedError, AttributeError):
        pass  # Windows doesn't support signals

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("🛑 Shutting down...")
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()
        logger.info("👋 Bot stopped!")

if __name__ == "__main__":
    setup_logging()
    # Use uvloop on Linux for better performance
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("Using uvloop")
    except ImportError:
        pass

    asyncio.run(main())
