export type PriceTier = {
  min_quantity: number;
  unit_price: number;
};

export type PricingSnapshot = {
  quantity: number;
  unitPrice: number;
  totalPrice: number;
  bonusQuantity: number;
  deliveredQuantity: number;
};

const toPositiveInteger = (value: unknown, fallback = 0) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(0, Math.trunc(parsed));
};

export function normalizePriceTiers(value: unknown): PriceTier[] {
  if (!Array.isArray(value)) return [];

  const normalized: PriceTier[] = [];
  for (const raw of value) {
    if (!raw || typeof raw !== "object") continue;
    const tier = raw as Record<string, unknown>;
    const minQty = toPositiveInteger(tier.min_quantity);
    const unitPrice = toPositiveInteger(tier.unit_price);
    if (minQty < 1 || unitPrice < 1) continue;
    normalized.push({ min_quantity: minQty, unit_price: unitPrice });
  }

  normalized.sort((a, b) => a.min_quantity - b.min_quantity);

  const deduped: PriceTier[] = [];
  for (const tier of normalized) {
    const last = deduped[deduped.length - 1];
    if (last && last.min_quantity === tier.min_quantity) {
      deduped[deduped.length - 1] = tier;
    } else {
      deduped.push(tier);
    }
  }

  return deduped;
}

export function resolveUnitPrice(basePrice: number, tiers: PriceTier[], quantity: number) {
  let unitPrice = toPositiveInteger(basePrice);
  const safeQuantity = Math.max(1, toPositiveInteger(quantity, 1));

  for (const tier of tiers) {
    if (tier.min_quantity <= safeQuantity) {
      unitPrice = tier.unit_price;
    }
  }

  return unitPrice;
}

export function computeBonusQuantity(quantity: number, buyQuantity: number, bonusQuantity: number) {
  const safeQty = Math.max(1, toPositiveInteger(quantity, 1));
  const buyX = toPositiveInteger(buyQuantity);
  const bonusY = toPositiveInteger(bonusQuantity);

  if (buyX < 1 || bonusY < 1) return 0;
  return Math.floor(safeQty / buyX) * bonusY;
}

export function getPricingSnapshot(input: {
  basePrice: number;
  priceTiers: unknown;
  quantity: number;
  promoBuyQuantity?: number | null;
  promoBonusQuantity?: number | null;
}): PricingSnapshot {
  const quantity = Math.max(1, toPositiveInteger(input.quantity, 1));
  const tiers = normalizePriceTiers(input.priceTiers);
  const unitPrice = resolveUnitPrice(input.basePrice, tiers, quantity);
  const totalPrice = quantity * unitPrice;
  const bonusQuantity = computeBonusQuantity(
    quantity,
    Number(input.promoBuyQuantity || 0),
    Number(input.promoBonusQuantity || 0)
  );

  return {
    quantity,
    unitPrice,
    totalPrice,
    bonusQuantity,
    deliveredQuantity: quantity + bonusQuantity
  };
}

export function getPriceRangeLabel(basePrice: number, priceTiers: unknown) {
  const tiers = normalizePriceTiers(priceTiers);
  const values = [toPositiveInteger(basePrice), ...tiers.map((tier) => tier.unit_price)].filter((v) => v > 0);
  if (!values.length) return "Liên hệ";

  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min === max) return `${min.toLocaleString("vi-VN")}đ`;
  return `${min.toLocaleString("vi-VN")}đ - ${max.toLocaleString("vi-VN")}đ`;
}
