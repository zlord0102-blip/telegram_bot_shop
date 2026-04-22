import asyncio
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters
)
from config import BOT_TOKEN
from database import init_db, get_setting, log_telegram_message
from handlers.chat_logger import log_incoming_message
from handlers.start import (
    start_command,
    back_to_main,
    handle_history_text,
    handle_balance,
    handle_support_text,
    set_language,
    handle_change_language,
    delete_message,
)
from handlers.shop import (
show_shop, show_shop_folder, show_product, confirm_buy, show_account,
    show_history, show_deposit, process_deposit, handle_deposit_text,
    handle_shop_text, process_deposit_amount,
    handle_withdraw_text, process_withdraw_amount, process_withdraw_bank, process_withdraw_account,
    handle_buy_quantity, show_order_detail,
    select_payment_vnd, select_payment_usdt,
    select_quick_quantity, prompt_manual_quantity, prompt_quick_quantity,
    select_direct_payment_vietqr, select_direct_payment_binance,
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
    admin_manage_stock, admin_view_stock, admin_stock_page, admin_stock_detail,
    admin_edit_stock_start, admin_edit_stock_done, admin_delete_stock, handle_admin_manage_stock_text,
    admin_export_stock, admin_clear_unsold_stock, admin_clear_all_stock,
    admin_sold_codes_menu, admin_sold_by_product, admin_export_sold_codes,
    admin_sold_by_user_start, admin_sold_by_user_search, handle_admin_sold_codes_text,
)
from sepay_checker import run_checker, init_checker_db

def _env_positive_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        return max(1, int(str(raw_value).strip()))
    except (TypeError, ValueError):
        return default


LOG_FILE_MAX_BYTES = _env_positive_int("BOT_LOG_MAX_BYTES", 5 * 1024 * 1024)
LOG_FILE_BACKUP_COUNT = _env_positive_int("BOT_LOG_BACKUP_COUNT", 7)

# Setup logging
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
logger = logging.getLogger(__name__)

# Disable noisy loggers
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)

async def post_init(application):
    # Wrap Telegram API send methods to capture outgoing messages for admin chat history.
    original_send_message = application.bot.send_message
    original_send_document = application.bot.send_document
    original_send_photo = application.bot.send_photo

    async def send_message_logged(*args, **kwargs):
        result = await original_send_message(*args, **kwargs)
        try:
            if getattr(result.chat, "type", None) == "private":
                await log_telegram_message(
                    chat_id=result.chat.id,
                    message_id=result.message_id,
                    direction="out",
                    message_type="text",
                    text=getattr(result, "text", None),
                    payload=None,
                    sent_at=getattr(result, "date", None),
                )
        except Exception:
            logger.exception("Failed to log outgoing send_message")
        return result

    async def send_document_logged(*args, **kwargs):
        result = await original_send_document(*args, **kwargs)
        try:
            if getattr(result.chat, "type", None) == "private":
                doc = getattr(result, "document", None)
                payload = None
                if doc:
                    payload = {
                        "file_id": getattr(doc, "file_id", None),
                        "file_name": getattr(doc, "file_name", None),
                        "mime_type": getattr(doc, "mime_type", None),
                    }
                await log_telegram_message(
                    chat_id=result.chat.id,
                    message_id=result.message_id,
                    direction="out",
                    message_type="document",
                    text=getattr(result, "caption", None),
                    payload=payload,
                    sent_at=getattr(result, "date", None),
                )
        except Exception:
            logger.exception("Failed to log outgoing send_document")
        return result

    async def send_photo_logged(*args, **kwargs):
        result = await original_send_photo(*args, **kwargs)
        try:
            if getattr(result.chat, "type", None) == "private":
                photos = getattr(result, "photo", None) or []
                payload = None
                if photos:
                    payload = {"file_id": getattr(photos[-1], "file_id", None)}
                await log_telegram_message(
                    chat_id=result.chat.id,
                    message_id=result.message_id,
                    direction="out",
                    message_type="photo",
                    text=getattr(result, "caption", None),
                    payload=payload,
                    sent_at=getattr(result, "date", None),
                )
        except Exception:
            logger.exception("Failed to log outgoing send_photo")
        return result

    application.bot.send_message = send_message_logged  # type: ignore[assignment]
    application.bot.send_document = send_document_logged  # type: ignore[assignment]
    application.bot.send_photo = send_photo_logged  # type: ignore[assignment]

def setup_bot():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .build()
    )

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
        entry_points=[MessageHandler(filters.Regex("^(➕ Nạp tiền|➕ Deposit)$"), handle_deposit_text)],
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
    app.add_handler(CommandHandler("admin", admin_command))
    
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
    app.add_handler(CallbackQueryHandler(show_shop, pattern="^shop(?:_\\d+)?$"))
    app.add_handler(CallbackQueryHandler(show_shop_folder, pattern="^shopfolder_\\d+_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(show_product, pattern="^buy_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_payment_vnd, pattern="^pay_vnd_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_payment_usdt, pattern="^pay_usdt_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_quick_quantity, pattern="^buyqty_(?:vnd|usdt)_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(prompt_manual_quantity, pattern="^buyqtymanual_(?:vnd|usdt)_\\d+$"))
    app.add_handler(CallbackQueryHandler(prompt_quick_quantity, pattern="^buyqtyquick_(?:vnd|usdt)_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_direct_payment_vietqr, pattern="^directpay_vietqr_\\d+_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_direct_payment_binance, pattern="^directpay_binance_\\d+_\\d+$"))
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
    # Use uvloop on Linux for better performance
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("Using uvloop")
    except ImportError:
        pass
    
    asyncio.run(main())
