# Cấu trúc Project Đề Xuất

Mục tiêu: giữ root repository dễ đọc, tách rõ runtime Bot, Dashboard, Storefront, SQL và tài liệu vận hành.

## Cấu trúc hiện tại nên giữ

- `run.py`: entrypoint Telegram Bot.
- `handlers/`: luồng người dùng/admin Telegram.
- `helpers/`: logic phụ trợ dùng lại nhiều nơi, ví dụ pricing, menu, delivery text, Binance.
- `keyboards/`: reply/inline keyboard.
- `database/`: Supabase backend và client database dùng chung cho Bot/checker.
- `locales/`: text đa ngôn ngữ.
- `scripts/`: script thao tác một lần hoặc migration tooling.
- `admin_dashboard_telegram_bot/`: project Dashboard riêng.
- `storefront-web/`: project Website riêng.

## Quy ước mới

- Không commit artifact build/cache/log:
  - `build/`
  - `dist/`
  - `dist_pyc/`
  - `__pycache__/`
  - `.codex_pycache/`
  - `*.log`
  - `*.tsbuildinfo`
- SQL tổng để apply production một lần vẫn giữ ở root:
  - `supabase_schema_all_in_one.sql`
- SQL lẻ vẫn có thể giữ ở root để review/rollback, nhưng khi thêm SQL mới phải mirror vào `supabase_schema_all_in_one.sql`.
- Báo cáo/ghi chú nên gom dần vào `docs/` ở các batch sau; hiện chưa move hàng loạt để tránh phá các link/tabs đang mở.

## Cấu trúc nên hướng tới sau này

```text
telegram_bot_shop/
  run.py
  config.py
  database/
  handlers/
  helpers/
  keyboards/
  locales/
  scripts/
  sql/
    all_in_one/
    migrations/
  docs/
    reports/
    runbooks/
  admin_dashboard_telegram_bot/
  storefront-web/
```

## Nguyên tắc cleanup

- Chỉ xóa artifact có thể build/generate lại.
- Không xóa `.env`, database runtime trong `data/`, hoặc source đang được import.
- Khi muốn move SQL sang `sql/`, cần cập nhật checklist/deploy docs cùng lúc để tránh apply nhầm file.
