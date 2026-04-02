from helpers.formatting import format_stock_items


_PURCHASE_COPY = {
    "vi": {
        "success_title": "✅ Mua hàng thành công",
        "product": "📦 Sản phẩm",
        "quantity": "🔢 Số lượng",
        "total": "💰 Tổng thanh toán",
        "bonus": "🎁 Tặng thêm",
        "balance": "💳 Số dư còn lại",
        "description": "📝 Ghi chú",
        "account": "📦 Dữ liệu nhận",
    },
    "en": {
        "success_title": "✅ Purchase successful",
        "product": "📦 Product",
        "quantity": "🔢 Quantity",
        "total": "💰 Total paid",
        "bonus": "🎁 Bonus",
        "balance": "💳 Remaining balance",
        "description": "📝 Note",
        "account": "📦 Delivered data",
    },
}


def get_purchase_copy(lang: str | None = "vi") -> dict[str, str]:
    key = str(lang or "vi").strip().lower()
    return _PURCHASE_COPY.get(key, _PURCHASE_COPY["vi"])


def build_display_name(
    first_name: str | None = None,
    last_name: str | None = None,
    username: str | None = None,
    fallback: str = "-",
) -> str:
    full_name = " ".join(
        part.strip() for part in (str(first_name or ""), str(last_name or "")) if part and part.strip()
    ).strip()
    if full_name:
        return full_name

    clean_username = str(username or "").strip().lstrip("@")
    if clean_username:
        return f"@{clean_username}"
    return fallback


def format_description_block(description: str | None, lang: str | None = "vi") -> str:
    if not description:
        return ""
    cleaned = str(description).strip()
    if not cleaned:
        return ""
    copy = get_purchase_copy(lang)
    return f"{copy['description']}:\n{cleaned}\n\n"


def build_purchase_summary_text(
    *,
    product_name: str,
    delivered_quantity: int,
    total_text: str,
    bonus_quantity: int = 0,
    balance_text: str | None = None,
    lang: str | None = "vi",
    title: str | None = None,
    extra_lines: list[str] | None = None,
) -> str:
    copy = get_purchase_copy(lang)
    lines = [
        title or copy["success_title"],
        "",
        f"{copy['product']}: {product_name}",
        f"{copy['quantity']}: {int(delivered_quantity)}",
        f"{copy['total']}: {total_text}",
    ]

    if int(bonus_quantity or 0) > 0:
        lines.append(f"{copy['bonus']}: {int(bonus_quantity)}")
    if balance_text:
        lines.append(f"{copy['balance']}: {balance_text}")
    if extra_lines:
        lines.extend([str(line) for line in extra_lines if str(line or "").strip()])
    return "\n".join(lines)


def build_delivery_message(
    *,
    summary_text: str,
    purchased_items: list[str],
    format_data: str | None,
    description: str | None = "",
    lang: str | None = "vi",
    html: bool = True,
) -> str:
    copy = get_purchase_copy(lang)
    description_block = format_description_block(description, lang=lang)
    items_formatted = "\n\n".join(format_stock_items(purchased_items, format_data, html=html))
    return f"{summary_text}\n\n{description_block}{copy['account']}:\n{items_formatted}"
