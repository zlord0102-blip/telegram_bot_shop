import { getPricingSnapshot } from "@/lib/pricing";
import { getSupabaseAdminClient } from "@/lib/supabaseAdmin";
import { generateVietQrUrl } from "@/lib/vietqr";

export type ShopProduct = {
  id: number;
  name: string;
  price: number;
  price_usdt: number;
  price_tiers: unknown;
  promo_buy_quantity: number;
  promo_bonus_quantity: number;
  website_sort_position: number | null;
  website_banner_url: string;
  website_logo_url: string;
  website_enabled: boolean;
  description: string;
  format_data: string;
  stock: number;
  sold_count: number;
};

export type SupportContact = {
  label: string;
  url: string;
};

export type AppBannerConfig = {
  image_url: string;
  title: string;
  subtitle: string;
  link: string;
};

export type MiddleBannerConfig = {
  image_url: string;
  link: string;
};

export type FaqItemConfig = {
  question: string;
  answer: string;
  enabled: boolean;
};

export type StorefrontSettings = {
  bank_name: string;
  account_number: string;
  account_name: string;
  admin_contact: string;
  support_contacts: string;
  payment_mode: "direct" | "hybrid" | "balance";
  shop_page_size: number;
  show_shop: boolean;
  show_support: boolean;
  show_history: boolean;
  show_deposit: boolean;
  show_balance: boolean;
  show_withdraw: boolean;
  show_usdt: boolean;
  show_language: boolean;
  show_app_banners: boolean;
  show_stats_section: boolean;
  show_stats_feedback: boolean;
  show_stats_sold: boolean;
  show_stats_customers: boolean;
  hero_banner_url: string;
  middle_banners: MiddleBannerConfig[];
  side_banner_left_url: string;
  side_banner_left_link: string;
  side_banner_right_url: string;
  side_banner_right_link: string;
  app_banners: AppBannerConfig[];
  faq_items: FaqItemConfig[];
};

export type StorefrontBootstrap = {
  products: ShopProduct[];
  settings: StorefrontSettings;
  supportContacts: SupportContact[];
};

export type WebsiteSessionUser = {
  id: string;
  email: string;
  displayName: string;
  lastSignInAt: string | null;
};

export type DirectOrderCheckoutInput = {
  productId: number;
  quantity: number;
  websiteUser: WebsiteSessionUser;
};

export type DirectOrderCheckoutResult = {
  code: string;
  status: string;
  amount: number;
  bankName: string;
  accountNumber: string;
  accountName: string;
  qrUrl: string;
  expiresAt: string;
  paymentMode: "direct" | "hybrid" | "balance";
  pricing: {
    quantity: number;
    bonusQuantity: number;
    deliveredQuantity: number;
    unitPrice: number;
    totalPrice: number;
  };
  product: {
    id: number;
    name: string;
    stock: number;
  };
};

export type DirectOrderRecord = {
  id: number;
  code: string;
  status: string;
  amount: number;
  quantity: number;
  bonus_quantity: number;
  unit_price: number;
  created_at: string;
  product_id: number;
  product_name: string;
  expires_at: string;
};

export type UserOrderRecord = {
  id: number;
  product_id: number;
  product_name: string;
  quantity: number;
  price: number;
  created_at: string;
};

export type UserOrdersSummary = {
  telegramUserId: number;
  directOrders: DirectOrderRecord[];
  orders: UserOrderRecord[];
};

export type WebsiteUserOrdersSummary = {
  authUserId: string;
  email: string;
  directOrders: DirectOrderRecord[];
  orders: UserOrderRecord[];
};

const DEFAULT_PAGE_SIZE = 10;
const DIRECT_ORDER_EXPIRE_MINUTES = (() => {
  const raw = Number(process.env.DIRECT_ORDER_PENDING_EXPIRE_MINUTES || "10");
  if (!Number.isFinite(raw)) return 10;
  return Math.max(1, Math.trunc(raw));
})();

const toInt = (value: unknown, fallback = 0) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.trunc(parsed);
};

const parseBooleanSetting = (value: string | undefined, fallback = true) => {
  if (value === undefined || value === null || value === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (["false", "0", "no", "off"].includes(normalized)) return false;
  if (["true", "1", "yes", "on"].includes(normalized)) return true;
  return fallback;
};

const parsePageSize = (value: string | undefined) => {
  const parsed = Number.parseInt(String(value || DEFAULT_PAGE_SIZE), 10);
  if (!Number.isFinite(parsed)) return DEFAULT_PAGE_SIZE;
  return Math.max(1, Math.min(50, parsed));
};

const parsePaymentMode = (value: string | undefined): "direct" | "hybrid" | "balance" => {
  const normalized = String(value || process.env.PAYMENT_MODE || "hybrid").trim().toLowerCase();
  if (normalized === "direct" || normalized === "hybrid" || normalized === "balance") {
    return normalized;
  }
  return "hybrid";
};

const STORAGE_URI_PREFIX = "storage://";
const STORAGE_SIGNED_TTL_SECONDS = 60 * 60 * 6;

const parseStorageUri = (value: string | undefined): { bucket: string; path: string } | null => {
  const trimmed = String(value || "").trim();
  if (!trimmed.startsWith(STORAGE_URI_PREFIX)) return null;
  const raw = trimmed.slice(STORAGE_URI_PREFIX.length);
  const slashIndex = raw.indexOf("/");
  if (slashIndex <= 0) return null;
  const bucket = raw.slice(0, slashIndex).trim();
  const path = raw.slice(slashIndex + 1).trim();
  if (!bucket || !path) return null;
  return { bucket, path };
};

const parseBannerUrl = (value: string | undefined) => {
  const trimmed = String(value || "").trim();
  if (!trimmed) return "";
  if (trimmed.startsWith("https://") || trimmed.startsWith("http://") || trimmed.startsWith("/")) {
    return trimmed;
  }
  return "";
};

const parseProductAssetUrl = (value: string | undefined) => {
  const trimmed = String(value || "").trim();
  if (!trimmed) return "";
  if (parseStorageUri(trimmed)) return trimmed;
  return parseBannerUrl(trimmed);
};

const DEFAULT_APP_BANNERS: AppBannerConfig[] = [
  { image_url: "", title: "Microsoft Office", subtitle: "Bản quyền", link: "" },
  { image_url: "", title: "Khám phá thế giới AI", subtitle: "Siêu tối ưu", link: "" },
  { image_url: "", title: "Ứng dụng VPN", subtitle: "Tốc độ - bảo mật", link: "" },
  { image_url: "", title: "Steam Wallet", subtitle: "Siêu tiết kiệm", link: "" }
];

const DEFAULT_FAQ_ITEMS: FaqItemConfig[] = [
  {
    question: "Shop này chạy theo logic nào?",
    answer: "Đồng bộ với Bot Telegram và Dashboard hiện tại: giá, tồn kho, direct order, SePay checker.",
    enabled: true
  },
  {
    question: "Có hỗ trợ sau thanh toán không?",
    answer: "Đơn confirmed được xử lý theo tồn kho. Bạn có thể tra mã thanh toán ở mục Status hoặc liên hệ hỗ trợ.",
    enabled: true
  }
];

const parseMiddleBanners = (rawValue: string | undefined, map: Record<string, string>): MiddleBannerConfig[] => {
  const jsonValue = String(rawValue || "").trim();
  if (jsonValue) {
    try {
      const parsed = JSON.parse(jsonValue);
      if (Array.isArray(parsed)) {
        const rows = parsed
          .map((item: any) => ({
            image_url: parseBannerUrl(item?.image_url),
            link: parseLinkUrl(item?.link)
          }))
          .filter((item) => item.image_url || item.link);
        if (rows.length) return rows;
      }
    } catch {
      // fallback to legacy single banner setting
    }
  }

  const legacyImage = parseBannerUrl(
    map.website_banner_middle_url || map.storefront_hero_banner_url || ""
  );
  if (!legacyImage) return [];
  return [{ image_url: legacyImage, link: "" }];
};

const normalizeBannerText = (value: unknown) => String(value || "").trim();

const parseLinkUrl = (value: string | undefined) => {
  const trimmed = String(value || "").trim();
  if (!trimmed) return "";
  if (trimmed.startsWith("https://") || trimmed.startsWith("http://") || trimmed.startsWith("/")) return trimmed;
  if (trimmed.startsWith("@")) return `https://t.me/${trimmed.slice(1)}`;
  if (trimmed.startsWith("t.me/")) return `https://${trimmed}`;
  return "";
};

const parseAppBanners = (rawValue: string | undefined, map: Record<string, string>): AppBannerConfig[] => {
  const jsonValue = String(rawValue || "").trim();

  if (jsonValue) {
    try {
      const parsed = JSON.parse(jsonValue);
      if (Array.isArray(parsed)) {
        const rows = parsed
          .map((item: any) => ({
            image_url: parseBannerUrl(item?.image_url),
            title: normalizeBannerText(item?.title),
            subtitle: normalizeBannerText(item?.subtitle),
            link: parseLinkUrl(item?.link)
          }))
          .filter((item) => item.image_url || item.title || item.subtitle || item.link);
        if (rows.length) return rows;
      }
    } catch {
      // keep fallback behavior
    }
  }

  const legacyRows = DEFAULT_APP_BANNERS
    .map((item, index) => ({
      ...item,
      image_url: parseBannerUrl(map[`website_banner_app_${index + 1}_url`] || map[`storefront_mini_banner_${index + 1}_url`] || "")
    }))
    .filter((item) => item.image_url || item.title || item.subtitle || item.link);

  if (legacyRows.length) return legacyRows;
  return DEFAULT_APP_BANNERS;
};

const parseFaqItems = (rawValue: string | undefined): FaqItemConfig[] => {
  const jsonValue = String(rawValue || "").trim();
  if (jsonValue) {
    try {
      const parsed = JSON.parse(jsonValue);
      if (Array.isArray(parsed)) {
        return parsed
          .map((item: any) => ({
            question: String(item?.question || "").trim(),
            answer: String(item?.answer || "").trim(),
            enabled: item?.enabled !== false
          }))
          .filter((item) => item.question && item.answer);
      }
    } catch {
      // fallback to defaults
    }
  }
  return DEFAULT_FAQ_ITEMS;
};

type WebsiteProductOverrides = {
  website_name?: string | null;
  website_price?: number | null;
  website_sort_position?: number | null;
  website_price_tiers?: unknown;
  website_promo_buy_quantity?: number | null;
  website_promo_bonus_quantity?: number | null;
  website_description?: string | null;
  website_format_data?: string | null;
  website_banner_url?: string | null;
  website_logo_url?: string | null;
  website_enabled?: boolean | null;
  website_deleted?: boolean | null;
};

const normalizePriceTiers = (value: unknown) => {
  if (!Array.isArray(value)) return null;
  const normalized = value
    .map((row) => ({
      min_quantity: toInt((row as any)?.min_quantity, 0),
      unit_price: toInt((row as any)?.unit_price, 0)
    }))
    .filter((row) => row.min_quantity > 0 && row.unit_price > 0)
    .sort((a, b) => a.min_quantity - b.min_quantity);
  return normalized.length ? normalized : null;
};

const toProduct = (row: any, override?: WebsiteProductOverrides): ShopProduct => {
  const rawName = String(row?.name || "Sản phẩm");
  const websiteName = String(override?.website_name || row?.website_name || "").trim();
  const name = websiteName || rawName;
  const websiteDescription = String(override?.website_description ?? row?.website_description ?? "").trim();

  const basePrice = Math.max(0, toInt(row?.price));
  const websitePriceRaw = override?.website_price ?? row?.website_price;
  const hasWebsitePriceField = websitePriceRaw !== null && websitePriceRaw !== undefined;
  const websitePrice = Math.max(0, toInt(websitePriceRaw ?? 0));
  const price = hasWebsitePriceField ? websitePrice : basePrice;

  const hasWebsiteTierField =
    (!!override && Object.prototype.hasOwnProperty.call(override, "website_price_tiers")) ||
    Object.prototype.hasOwnProperty.call(row || {}, "website_price_tiers");
  const websiteTiers = normalizePriceTiers(override?.website_price_tiers ?? row?.website_price_tiers);
  const fallbackTiers = normalizePriceTiers(row?.price_tiers);
  const priceTiers = hasWebsiteTierField ? websiteTiers : fallbackTiers;

  const hasWebsitePromoField =
    (!!override &&
      (Object.prototype.hasOwnProperty.call(override, "website_promo_buy_quantity") ||
        Object.prototype.hasOwnProperty.call(override, "website_promo_bonus_quantity"))) ||
    Object.prototype.hasOwnProperty.call(row || {}, "website_promo_buy_quantity") ||
    Object.prototype.hasOwnProperty.call(row || {}, "website_promo_bonus_quantity");

  const websitePromoBuy = Math.max(0, toInt(override?.website_promo_buy_quantity ?? row?.website_promo_buy_quantity ?? 0));
  const websitePromoBonus = Math.max(0, toInt(override?.website_promo_bonus_quantity ?? row?.website_promo_bonus_quantity ?? 0));
  const fallbackPromoBuy = Math.max(0, toInt(row?.promo_buy_quantity));
  const fallbackPromoBonus = Math.max(0, toInt(row?.promo_bonus_quantity));
  const promoBuy = hasWebsitePromoField ? websitePromoBuy : fallbackPromoBuy;
  const promoBonus = hasWebsitePromoField ? websitePromoBonus : fallbackPromoBonus;

  const websiteSortPositionRaw = override?.website_sort_position ?? row?.website_sort_position;
  const websiteSortPosition = Number.isFinite(Number(websiteSortPositionRaw))
    ? Number(websiteSortPositionRaw)
    : null;
  const websiteFormatData = String(override?.website_format_data ?? row?.website_format_data ?? "").trim();

  return {
    id: toInt(row?.id),
    name,
    price,
    price_usdt: Number(row?.price_usdt || 0),
    price_tiers: priceTiers,
    promo_buy_quantity: promoBuy,
    promo_bonus_quantity: promoBonus,
    website_sort_position: websiteSortPosition,
    website_banner_url: parseProductAssetUrl(String(override?.website_banner_url ?? row?.website_banner_url ?? "")),
    website_logo_url: parseProductAssetUrl(String(override?.website_logo_url ?? row?.website_logo_url ?? "")),
    website_enabled: (override?.website_enabled ?? row?.website_enabled) !== false,
    description: websiteDescription || String(row?.description || ""),
    format_data: websiteFormatData || String(row?.format_data || ""),
    stock: Math.max(0, toInt(row?.stock)),
    sold_count: Math.max(0, toInt(row?.sold_count))
  };
};

const toWebsiteSortRank = (value: number | null | undefined) =>
  Number.isFinite(value) ? Number(value) : Number.POSITIVE_INFINITY;

const sortWebsiteProducts = (products: ShopProduct[]) =>
  products
    .slice()
    .sort((a, b) => {
      const rankA = toWebsiteSortRank(a.website_sort_position);
      const rankB = toWebsiteSortRank(b.website_sort_position);
      if (rankA !== rankB) return rankA - rankB;
      return a.id - b.id;
    });

function normalizeTelegramContact(value: string) {
  const cleaned = value.trim();
  if (!cleaned) return "";

  if (cleaned.startsWith("https://") || cleaned.startsWith("http://")) {
    return cleaned;
  }

  if (cleaned.startsWith("@")) {
    return `https://t.me/${cleaned.slice(1)}`;
  }

  if (cleaned.startsWith("t.me/")) {
    return `https://${cleaned}`;
  }

  if (/^[A-Za-z0-9_]{5,}$/.test(cleaned)) {
    return `https://t.me/${cleaned}`;
  }

  if (cleaned.startsWith("www.") || /^[A-Za-z0-9.-]+\.[A-Za-z]{2,}([/].*)?$/.test(cleaned)) {
    return `https://${cleaned}`;
  }

  return "";
}

function formatSupportLabel(label: string, icon: string, fallback: string) {
  const text = (label || "").trim() || fallback;
  if (text.startsWith("💬") || text.startsWith("📘") || text.startsWith("💠") || text.startsWith("🔗")) {
    return text;
  }
  return `${icon} ${text}`;
}

export function parseSupportContacts(rawValue: string, adminContact: string) {
  const contacts = new Map<string, string>();

  const addContact = (label: string, url: string) => {
    const finalLabel = label.trim();
    const finalUrl = url.trim();
    if (!finalLabel || !finalUrl) return;

    const key = finalUrl.toLowerCase();
    if (contacts.has(key)) return;
    contacts.set(key, `${finalLabel}|${finalUrl}`);
  };

  for (const line of String(rawValue || "").split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const [rawLabel = "", rawTarget = ""] = trimmed.includes("|")
      ? trimmed.split("|", 2)
      : ["", trimmed];

    const label = rawLabel.trim();
    const target = rawTarget.trim();
    if (!target) continue;

    const lowerLabel = label.toLowerCase();
    const lowerTarget = target.toLowerCase();

    const isTelegram =
      lowerLabel.includes("telegram") ||
      target.startsWith("@") ||
      lowerTarget.startsWith("t.me/") ||
      lowerTarget.includes("t.me/");
    const isFacebook = lowerLabel.includes("facebook") || lowerTarget.includes("facebook.com") || lowerTarget.includes("fb.com");
    const isZalo = lowerLabel.includes("zalo") || lowerTarget.includes("zalo.me") || lowerTarget.includes("zaloapp.com");

    if (isTelegram) {
      const normalized = normalizeTelegramContact(target);
      if (normalized) {
        addContact(formatSupportLabel(label, "💬", "Telegram"), normalized);
      }
      continue;
    }

    if (isFacebook) {
      const normalized = normalizeTelegramContact(target);
      if (normalized) {
        addContact(formatSupportLabel(label, "📘", "Facebook"), normalized);
      }
      continue;
    }

    if (isZalo) {
      const normalized = normalizeTelegramContact(target);
      if (normalized) {
        addContact(formatSupportLabel(label, "💠", "Zalo"), normalized);
      }
      continue;
    }

    const normalized = normalizeTelegramContact(target);
    if (normalized) {
      addContact(formatSupportLabel(label, "🔗", "Liên hệ"), normalized);
    }
  }

  const fallbackTelegram = normalizeTelegramContact(adminContact);
  if (fallbackTelegram) {
    addContact("💬 Telegram", fallbackTelegram);
  }

  return Array.from(contacts.values()).map((raw) => {
    const [label, url] = raw.split("|", 2);
    return { label, url };
  });
}

const getExpireAtFromCreatedAt = (createdAt: string) => {
  const timestamp = Date.parse(createdAt);
  if (Number.isNaN(timestamp)) {
    return new Date(Date.now() + DIRECT_ORDER_EXPIRE_MINUTES * 60_000).toISOString();
  }
  return new Date(timestamp + DIRECT_ORDER_EXPIRE_MINUTES * 60_000).toISOString();
};

const isDirectOrderExpired = (createdAt: string, status: string) => {
  if (status !== "pending") return false;
  const timestamp = Date.parse(createdAt);
  if (Number.isNaN(timestamp)) return false;
  return Date.now() - timestamp >= DIRECT_ORDER_EXPIRE_MINUTES * 60_000;
};

export async function getStorefrontSettings(): Promise<StorefrontSettings> {
  const supabase = getSupabaseAdminClient();
  const keys = [
    "website_bank_name",
    "website_account_number",
    "website_account_name",
    "website_admin_contact",
    "website_support_contacts",
    "website_payment_mode",
    "website_shop_page_size",
    "website_show_shop",
    "website_show_support",
    "website_show_history",
    "website_show_deposit",
    "website_show_balance",
    "website_show_withdraw",
    "website_show_usdt",
    "website_show_language",
    "website_show_app_banners",
    "website_show_stats_section",
    "website_show_stats_feedback",
    "website_show_stats_sold",
    "website_show_stats_customers",
    "website_faq_items",
    "website_banner_middle_url",
    "website_banner_middles",
    "website_banner_ads_left_url",
    "website_banner_ads_right_url",
    "website_banner_ads_left_link",
    "website_banner_ads_right_link",
    "website_banner_apps",
    "website_banner_app_1_url",
    "website_banner_app_2_url",
    "website_banner_app_3_url",
    "website_banner_app_4_url",
    "bank_name",
    "account_number",
    "account_name",
    "admin_contact",
    "support_contacts",
    "payment_mode",
    "shop_page_size",
    "show_shop",
    "show_support",
    "show_history",
    "show_deposit",
    "show_balance",
    "show_withdraw",
    "show_usdt",
    "show_language",
    "storefront_hero_banner_url",
    "storefront_side_left_banner_url",
    "storefront_side_right_banner_url",
    "storefront_side_left_banner_link",
    "storefront_side_right_banner_link",
    "storefront_mini_banner_1_url",
    "storefront_mini_banner_2_url",
    "storefront_mini_banner_3_url",
    "storefront_mini_banner_4_url",
    "storefront_mini_banners_json"
  ];

  const { data, error } = await supabase
    .from("settings")
    .select("key, value")
    .in("key", keys);

  if (error) {
    throw new Error(error.message);
  }

  const map: Record<string, string> = {};
  for (const row of data || []) {
    map[row.key] = row.value || "";
  }

  const pickSetting = (...keysToCheck: string[]) => {
    for (const key of keysToCheck) {
      const value = String(map[key] || "").trim();
      if (value) return value;
    }
    return "";
  };

  return {
    bank_name: pickSetting("website_bank_name", "bank_name") || process.env.SEPAY_BANK_NAME || "",
    account_number: pickSetting("website_account_number", "account_number") || process.env.SEPAY_ACCOUNT_NUMBER || "",
    account_name: pickSetting("website_account_name", "account_name") || process.env.SEPAY_ACCOUNT_NAME || "",
    admin_contact: pickSetting("website_admin_contact", "admin_contact"),
    support_contacts: pickSetting("website_support_contacts", "support_contacts"),
    payment_mode: parsePaymentMode(pickSetting("website_payment_mode", "payment_mode")),
    shop_page_size: parsePageSize(pickSetting("website_shop_page_size", "shop_page_size")),
    show_shop: parseBooleanSetting(pickSetting("website_show_shop", "show_shop"), true),
    show_support: parseBooleanSetting(pickSetting("website_show_support", "show_support"), true),
    show_history: parseBooleanSetting(pickSetting("website_show_history", "show_history"), true),
    show_deposit: parseBooleanSetting(pickSetting("website_show_deposit", "show_deposit"), true),
    show_balance: parseBooleanSetting(pickSetting("website_show_balance", "show_balance"), true),
    show_withdraw: parseBooleanSetting(pickSetting("website_show_withdraw", "show_withdraw"), true),
    show_usdt: parseBooleanSetting(pickSetting("website_show_usdt", "show_usdt"), true),
    show_language: parseBooleanSetting(pickSetting("website_show_language", "show_language"), true),
    show_app_banners: parseBooleanSetting(pickSetting("website_show_app_banners"), true),
    show_stats_section: parseBooleanSetting(pickSetting("website_show_stats_section"), true),
    show_stats_feedback: parseBooleanSetting(pickSetting("website_show_stats_feedback"), true),
    show_stats_sold: parseBooleanSetting(pickSetting("website_show_stats_sold"), true),
    show_stats_customers: parseBooleanSetting(pickSetting("website_show_stats_customers"), true),
    hero_banner_url: parseBannerUrl(pickSetting("website_banner_middle_url", "storefront_hero_banner_url")),
    middle_banners: parseMiddleBanners(pickSetting("website_banner_middles"), map),
    side_banner_left_url: parseBannerUrl(pickSetting("website_banner_ads_left_url", "storefront_side_left_banner_url")),
    side_banner_left_link: parseLinkUrl(pickSetting("website_banner_ads_left_link", "storefront_side_left_banner_link")),
    side_banner_right_url: parseBannerUrl(pickSetting("website_banner_ads_right_url", "storefront_side_right_banner_url")),
    side_banner_right_link: parseLinkUrl(pickSetting("website_banner_ads_right_link", "storefront_side_right_banner_link")),
    app_banners: parseAppBanners(
      pickSetting("website_banner_apps", "storefront_mini_banners_json"),
      map
    ),
    faq_items: parseFaqItems(pickSetting("website_faq_items"))
  };
}

async function getSalesMap(productIds: number[]) {
  const ids = Array.from(new Set(productIds.filter((id) => id > 0)));
  if (!ids.length) return new Map<number, number>();

  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("orders")
    .select("product_id, quantity")
    .in("product_id", ids);

  if (error) {
    return new Map<number, number>();
  }

  const map = new Map<number, number>();
  for (const row of data || []) {
    const productId = toInt(row.product_id);
    if (!productId) continue;
    const quantity = Math.max(0, toInt(row.quantity, 1));
    map.set(productId, (map.get(productId) || 0) + quantity);
  }

  return map;
}

async function hydrateSoldCounts(products: ShopProduct[]) {
  const salesMap = await getSalesMap(products.map((product) => product.id));
  return products.map((product) => ({
    ...product,
    sold_count: salesMap.get(product.id) || 0
  }));
}

async function resolveStorageAssetUrl(rawUrl: string, cache: Map<string, string>) {
  const source = String(rawUrl || "").trim();
  if (!source) return "";

  if (cache.has(source)) {
    return cache.get(source) || "";
  }

  const parsed = parseStorageUri(source);
  if (!parsed) {
    cache.set(source, source);
    return source;
  }

  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase.storage
    .from(parsed.bucket)
    .createSignedUrl(parsed.path, STORAGE_SIGNED_TTL_SECONDS);

  if (error || !data?.signedUrl) {
    cache.set(source, "");
    return "";
  }

  cache.set(source, data.signedUrl);
  return data.signedUrl;
}

async function hydrateWebsiteAssetUrls(products: ShopProduct[]) {
  if (!products.length) return products;
  const cache = new Map<string, string>();

  return Promise.all(
    products.map(async (product) => ({
      ...product,
      website_banner_url: await resolveStorageAssetUrl(product.website_banner_url, cache),
      website_logo_url: await resolveStorageAssetUrl(product.website_logo_url, cache)
    }))
  );
}

async function getWebsiteOverridesByIds(productIds: number[]) {
  const ids = Array.from(new Set(productIds.filter((id) => id > 0)));
  if (!ids.length) return new Map<number, WebsiteProductOverrides>();

  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("products")
    .select(
      "id, website_name, website_price, website_sort_position, website_price_tiers, website_promo_buy_quantity, website_promo_bonus_quantity, website_description, website_format_data, website_banner_url, website_logo_url, website_enabled, website_deleted"
    )
    .in("id", ids);

  if (error) {
    return new Map<number, WebsiteProductOverrides>();
  }

  const map = new Map<number, WebsiteProductOverrides>();
  for (const row of (data as any[]) || []) {
    const id = toInt(row?.id);
    if (!id) continue;
    map.set(id, {
      website_name: row?.website_name ?? null,
      website_price: row?.website_price ?? null,
      website_sort_position: row?.website_sort_position ?? null,
      website_price_tiers: row?.website_price_tiers ?? null,
      website_promo_buy_quantity: row?.website_promo_buy_quantity ?? null,
      website_promo_bonus_quantity: row?.website_promo_bonus_quantity ?? null,
      website_description: row?.website_description ?? null,
      website_format_data: row?.website_format_data ?? null,
      website_banner_url: row?.website_banner_url ?? null,
      website_logo_url: row?.website_logo_url ?? null,
      website_enabled: row?.website_enabled ?? null,
      website_deleted: row?.website_deleted ?? null
    });
  }

  return map;
}

async function mapRowsToWebsiteProducts(rows: any[]) {
  const overrides = await getWebsiteOverridesByIds(rows.map((row) => toInt(row?.id)));
  const mapped = rows
    .map((row) => {
      const id = toInt(row?.id);
      const override = overrides.get(id);
      const websiteDeleted = Boolean(override?.website_deleted ?? row?.website_deleted);
      if (websiteDeleted) return null;
      return toProduct(row, override);
    })
    .filter((product): product is ShopProduct => Boolean(product && product.website_enabled));
  return sortWebsiteProducts(mapped);
}

export async function getProductsWithStock() {
  const supabase = getSupabaseAdminClient();
  const { data: productsData, error: productsError } = await supabase
    .from("products")
    .select(
      "id, name, price, price_usdt, price_tiers, promo_buy_quantity, promo_bonus_quantity, description, format_data, website_name, website_price, website_sort_position, website_price_tiers, website_promo_buy_quantity, website_promo_bonus_quantity, website_description, website_format_data, website_banner_url, website_logo_url, website_enabled, website_deleted"
    )
    .order("id", { ascending: true });

  let baseRows: any[] = (productsData as any[]) || [];
  if (productsError) {
    const { data: legacyData, error: legacyError } = await supabase
      .from("products")
      .select("id, name, price, price_usdt, price_tiers, promo_buy_quantity, promo_bonus_quantity, description, format_data")
      .order("id", { ascending: true });

    if (legacyError) {
      throw new Error(legacyError.message);
    }
    baseRows = (legacyData as any[]) || [];
  }

  const products = await mapRowsToWebsiteProducts(baseRows);

  const productsWithStock = await Promise.all(
    products.map(async (product) => {
      const { count, error: stockError } = await supabase
        .from("stock")
        .select("id", { count: "exact", head: true })
        .eq("product_id", product.id)
        .eq("sold", false);

      if (stockError) {
        return product;
      }

      return { ...product, stock: Math.max(0, Number(count) || 0) };
    })
  );

  const withSold = await hydrateSoldCounts(productsWithStock);
  return hydrateWebsiteAssetUrls(withSold);
}

export async function getProductWithStock(productId: number) {
  const supabase = getSupabaseAdminClient();
  const { data: productData, error: productError } = await supabase
    .from("products")
    .select(
      "id, name, price, price_usdt, price_tiers, promo_buy_quantity, promo_bonus_quantity, description, format_data, website_name, website_price, website_sort_position, website_price_tiers, website_promo_buy_quantity, website_promo_bonus_quantity, website_description, website_format_data, website_banner_url, website_logo_url, website_enabled, website_deleted"
    )
    .eq("id", productId)
    .maybeSingle();

  let baseRow: any = productData;
  if (productError) {
    const { data: legacyRow, error: legacyError } = await supabase
      .from("products")
      .select("id, name, price, price_usdt, price_tiers, promo_buy_quantity, promo_bonus_quantity, description, format_data")
      .eq("id", productId)
      .maybeSingle();
    if (legacyError) {
      throw new Error(legacyError.message);
    }
    baseRow = legacyRow;
  }

  if (!baseRow) return null;

  const { count, error: stockError } = await supabase
    .from("stock")
    .select("id", { count: "exact", head: true })
    .eq("product_id", productId)
    .eq("sold", false);

  if (stockError) {
    throw new Error(stockError.message);
  }

  const mapped = await mapRowsToWebsiteProducts([{ ...baseRow, stock: Math.max(0, Number(count) || 0) }]);
  if (!mapped.length) return null;
  const withStats = await hydrateSoldCounts(mapped);
  const withAssets = await hydrateWebsiteAssetUrls(withStats);
  return withAssets[0] || withStats[0] || mapped[0];
}

const WEBSITE_USER_ID_SENTINEL_BASE = 8_000_000_000_000;
const WEBSITE_USER_ID_SENTINEL_RANGE = 900_000_000_000;

const mapWebsiteAuthIdToSentinelUserId = (authUserId: string) => {
  let hash = 0;
  for (let index = 0; index < authUserId.length; index += 1) {
    hash = (hash * 31 + authUserId.charCodeAt(index)) >>> 0;
  }
  return WEBSITE_USER_ID_SENTINEL_BASE + (hash % WEBSITE_USER_ID_SENTINEL_RANGE);
};

const buildWebsiteShadowUsername = (authUserId: string) => {
  const normalized = String(authUserId || "").replace(/[^a-zA-Z0-9]/g, "").toLowerCase();
  const token = normalized.slice(0, 16) || "websiteuser";
  return `web_${token}`;
};

async function ensureWebsiteShadowUser(params: {
  authUserId: string;
  email: string;
  displayName?: string | null;
}) {
  const supabase = getSupabaseAdminClient();
  const preferredUsername = buildWebsiteShadowUsername(params.authUserId);
  const nowIso = new Date().toISOString();
  let candidate = mapWebsiteAuthIdToSentinelUserId(params.authUserId);

  for (let retry = 0; retry < 1024; retry += 1) {
    const { data: existed, error: existedError } = await supabase
      .from("users")
      .select("user_id, username")
      .eq("user_id", candidate)
      .maybeSingle();

    if (existedError) {
      throw new Error(`Không thể kiểm tra user mapping cho website order: ${existedError.message}`);
    }

    const existedUsername = String((existed as any)?.username || "").trim().toLowerCase();
    const isOwner = existedUsername === preferredUsername;
    const isEmpty = !existed;

    if (isEmpty || isOwner) {
      const usernameFromName =
        String(params.displayName || "")
          .trim()
          .replace(/\s+/g, "_")
          .replace(/[^a-zA-Z0-9_.-]/g, "")
          .slice(0, 24) || "";
      const username = preferredUsername || usernameFromName || `web_${String(params.email || "").split("@")[0] || "user"}`;
      const payload = {
        user_id: candidate,
        username: username.slice(0, 32),
        language: "vi",
        created_at: nowIso
      };
      const { error: upsertError } = await supabase.from("users").upsert(payload, { onConflict: "user_id" });
      if (upsertError) {
        throw new Error(`Không thể tạo user mapping cho website order: ${upsertError.message}`);
      }
      return candidate;
    }

    candidate += 1;
  }

  throw new Error("Không thể cấp user_id mapping cho website order. Vui lòng thử lại.");
}

async function ensureWebsiteUser(user: WebsiteSessionUser) {
  const supabase = getSupabaseAdminClient();
  const email = String(user.email || "").trim().toLowerCase();
  if (!email) {
    throw new Error("Tài khoản Website chưa có email hợp lệ.");
  }

  const payload = {
    auth_user_id: user.id,
    email,
    display_name: user.displayName || null,
    last_sign_in_at: user.lastSignInAt || new Date().toISOString(),
    updated_at: new Date().toISOString()
  };

  const { error } = await supabase
    .from("website_users")
    .upsert(payload, { onConflict: "auth_user_id" });

  if (error) {
    throw new Error(`Không thể đồng bộ website user: ${error.message}`);
  }
}

async function createWebsiteDirectOrderRecord(params: {
  websiteUser: WebsiteSessionUser;
  productId: number;
  quantity: number;
  bonusQuantity: number;
  unitPrice: number;
  amount: number;
  code: string;
  createdAt: string;
}) {
  const supabase = getSupabaseAdminClient();
  const payload = {
    auth_user_id: params.websiteUser.id,
    user_email: params.websiteUser.email,
    product_id: params.productId,
    quantity: params.quantity,
    bonus_quantity: params.bonusQuantity,
    unit_price: params.unitPrice,
    amount: params.amount,
    code: params.code,
    status: "pending",
    created_at: params.createdAt
  };

  const { error } = await supabase
    .from("website_direct_orders")
    .upsert(payload, { onConflict: "code" });

  if (error) {
    throw new Error(`Không thể tạo website direct order: ${error.message}`);
  }
}

async function createDirectOrderWithSettings(params: {
  userId: number;
  productId: number;
  quantity: number;
  bonusQuantity: number;
  unitPrice: number;
  amount: number;
  code: string;
}) {
  const supabase = getSupabaseAdminClient();

  const rpcPayload = {
    p_user_id: params.userId,
    p_product_id: params.productId,
    p_quantity: params.quantity,
    p_bonus_quantity: params.bonusQuantity,
    p_unit_price: params.unitPrice,
    p_amount: params.amount,
    p_code: params.code
  };

  const rpcResult = await supabase.rpc("create_direct_order_and_get_bank_settings", rpcPayload);

  if (!rpcResult.error) {
    return;
  }

  const payload = {
    user_id: params.userId,
    product_id: params.productId,
    quantity: params.quantity,
    bonus_quantity: params.bonusQuantity,
    unit_price: params.unitPrice,
    amount: params.amount,
    code: params.code,
    created_at: new Date().toISOString(),
    status: "pending"
  };

  const { error: insertError } = await supabase.from("direct_orders").insert(payload);

  if (insertError) {
    const legacyPayload = {
      user_id: params.userId,
      product_id: params.productId,
      quantity: params.quantity,
      unit_price: params.unitPrice,
      amount: params.amount,
      code: params.code,
      created_at: new Date().toISOString(),
      status: "pending"
    };

    const { error: legacyInsertError } = await supabase.from("direct_orders").insert(legacyPayload);
    if (legacyInsertError) {
      throw new Error(legacyInsertError.message);
    }
  }
}

function makeOrderCode(userId: number) {
  const random = Math.floor(1000 + Math.random() * 9000);
  return `SEBUY ${userId}${random}`;
}

export async function createDirectOrderCheckout(input: DirectOrderCheckoutInput): Promise<DirectOrderCheckoutResult> {
  const settings = await getStorefrontSettings();

  if (!settings.show_shop) {
    throw new Error("Shop đang tạm tắt trong cài đặt Dashboard.");
  }

  if (settings.payment_mode === "balance") {
    throw new Error("Website hiện hỗ trợ thanh toán VietQR/SePay. Chế độ chỉ balance đang được dùng trong bot Telegram.");
  }

  const product = await getProductWithStock(input.productId);
  if (!product) {
    throw new Error("Sản phẩm không tồn tại hoặc không thể truy cập.");
  }

  const quantity = Math.max(1, Math.trunc(Number(input.quantity) || 1));
  const pricing = getPricingSnapshot({
    basePrice: product.price,
    priceTiers: product.price_tiers,
    quantity,
    promoBuyQuantity: product.promo_buy_quantity,
    promoBonusQuantity: product.promo_bonus_quantity
  });

  if (product.stock < pricing.deliveredQuantity) {
    throw new Error(`Không đủ tồn kho. Cần ${pricing.deliveredQuantity}, hiện còn ${product.stock}.`);
  }

  const websiteUser = input.websiteUser;
  const authUserId = String(websiteUser?.id || "").trim();
  const authUserEmail = String(websiteUser?.email || "").trim().toLowerCase();
  if (!authUserId || !authUserEmail) {
    throw new Error("Tài khoản đăng nhập không hợp lệ. Vui lòng đăng nhập lại.");
  }

  await ensureWebsiteUser({
    id: authUserId,
    email: authUserEmail,
    displayName: String(websiteUser.displayName || "").trim(),
    lastSignInAt: websiteUser.lastSignInAt || null
  });

  const sentinelUserId = await ensureWebsiteShadowUser({
    authUserId,
    email: authUserEmail,
    displayName: String(websiteUser.displayName || "").trim()
  });
  const code = makeOrderCode(sentinelUserId);
  const createdAt = new Date().toISOString();

  await createWebsiteDirectOrderRecord({
    websiteUser: {
      id: authUserId,
      email: authUserEmail,
      displayName: String(websiteUser.displayName || "").trim(),
      lastSignInAt: websiteUser.lastSignInAt || null
    },
    productId: product.id,
    quantity: pricing.quantity,
    bonusQuantity: pricing.bonusQuantity,
    unitPrice: pricing.unitPrice,
    amount: pricing.totalPrice,
    code,
    createdAt
  });

  try {
    await createDirectOrderWithSettings({
      userId: sentinelUserId,
      productId: product.id,
      quantity: pricing.quantity,
      bonusQuantity: pricing.bonusQuantity,
      unitPrice: pricing.unitPrice,
      amount: pricing.totalPrice,
      code
    });
  } catch (error) {
    const supabase = getSupabaseAdminClient();
    await supabase
      .from("website_direct_orders")
      .update({ status: "failed", updated_at: new Date().toISOString() })
      .eq("code", code);
    throw error;
  }

  const bankName = settings.bank_name;
  const accountNumber = settings.account_number;
  const accountName = settings.account_name;

  if (!accountNumber) {
    throw new Error("Chưa cấu hình tài khoản ngân hàng nhận tiền trong Dashboard settings.");
  }

  const qrUrl = generateVietQrUrl({
    bankName,
    accountNumber,
    accountName,
    amount: pricing.totalPrice,
    content: code
  });

  const expiresAt = new Date(new Date(createdAt).getTime() + DIRECT_ORDER_EXPIRE_MINUTES * 60_000).toISOString();

  return {
    code,
    status: "pending",
    amount: pricing.totalPrice,
    bankName,
    accountNumber,
    accountName,
    qrUrl,
    expiresAt,
    paymentMode: settings.payment_mode,
    pricing: {
      quantity: pricing.quantity,
      bonusQuantity: pricing.bonusQuantity,
      deliveredQuantity: pricing.deliveredQuantity,
      unitPrice: pricing.unitPrice,
      totalPrice: pricing.totalPrice
    },
    product: {
      id: product.id,
      name: product.name,
      stock: product.stock
    }
  };
}

async function applyDirectOrderExpiryIfNeeded(order: DirectOrderRecord) {
  if (!isDirectOrderExpired(order.created_at, order.status)) {
    return order;
  }

  const supabase = getSupabaseAdminClient();
  const { error } = await supabase
    .from("direct_orders")
    .update({ status: "cancelled" })
    .eq("id", order.id)
    .eq("status", "pending");

  if (error) {
    return order;
  }

  return { ...order, status: "cancelled" };
}

async function syncWebsiteDirectOrderStatus(code: string, status: string) {
  const safeCode = code.trim();
  if (!safeCode) return;
  const supabase = getSupabaseAdminClient();
  const { data: row, error } = await supabase
    .from("website_direct_orders")
    .select("id, status")
    .eq("code", safeCode)
    .maybeSingle();

  if (error || !row) return;
  if (String(row.status || "") === status) return;

  await supabase
    .from("website_direct_orders")
    .update({ status, updated_at: new Date().toISOString() })
    .eq("id", row.id);
}

function mapDirectOrderRow(row: any): DirectOrderRecord {
  const createdAt = String(row?.created_at || "");
  return {
    id: toInt(row?.id),
    code: String(row?.code || ""),
    status: String(row?.status || "pending"),
    amount: Math.max(0, toInt(row?.amount)),
    quantity: Math.max(1, toInt(row?.quantity, 1)),
    bonus_quantity: Math.max(0, toInt(row?.bonus_quantity, 0)),
    unit_price: Math.max(0, toInt(row?.unit_price)),
    created_at: createdAt,
    product_id: Math.max(0, toInt(row?.product_id)),
    product_name: String((row?.products || {}).name || "Sản phẩm"),
    expires_at: getExpireAtFromCreatedAt(createdAt)
  };
}

export async function getDirectOrderByCode(code: string) {
  const safeCode = code.trim();
  if (!safeCode) return null;

  const supabase = getSupabaseAdminClient();

  const { data, error } = await supabase
    .from("direct_orders")
    .select("id, code, status, amount, quantity, bonus_quantity, unit_price, created_at, product_id, products(name)")
    .eq("code", safeCode)
    .order("id", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(error.message);
  }

  if (!data) return null;

  const mapped = await applyDirectOrderExpiryIfNeeded(mapDirectOrderRow(data));
  await syncWebsiteDirectOrderStatus(mapped.code, mapped.status);
  return mapped;
}

export async function getDirectOrderByCodeForAuth(code: string, authUserId: string) {
  const safeCode = code.trim();
  const safeAuthUserId = String(authUserId || "").trim();
  if (!safeCode || !safeAuthUserId) return null;

  const supabase = getSupabaseAdminClient();
  const { data, error } = await supabase
    .from("website_direct_orders")
    .select("id")
    .eq("code", safeCode)
    .eq("auth_user_id", safeAuthUserId)
    .maybeSingle();

  if (error) {
    throw new Error(error.message);
  }
  if (!data) {
    return null;
  }

  return getDirectOrderByCode(safeCode);
}

export async function getUserOrdersSummary(telegramUserId: number, limit = 20): Promise<UserOrdersSummary> {
  const userId = Math.max(1, Math.trunc(Number(telegramUserId) || 0));
  if (!Number.isFinite(userId) || userId <= 0) {
    throw new Error("Telegram ID không hợp lệ.");
  }

  const supabase = getSupabaseAdminClient();

  const [{ data: directRows, error: directError }, { data: orderRows, error: orderError }] = await Promise.all([
    supabase
      .from("direct_orders")
      .select("id, code, status, amount, quantity, bonus_quantity, unit_price, created_at, product_id, products(name)")
      .eq("user_id", userId)
      .order("created_at", { ascending: false })
      .limit(limit),
    supabase
      .from("orders")
      .select("id, product_id, price, quantity, created_at, products(name)")
      .eq("user_id", userId)
      .order("created_at", { ascending: false })
      .limit(limit)
  ]);

  if (directError) {
    throw new Error(directError.message);
  }

  if (orderError) {
    throw new Error(orderError.message);
  }

  const mappedDirect = await Promise.all(((directRows as any[]) || []).map((row) => applyDirectOrderExpiryIfNeeded(mapDirectOrderRow(row))));
  const mappedOrders: UserOrderRecord[] = ((orderRows as any[]) || []).map((row) => ({
    id: toInt(row?.id),
    product_id: Math.max(0, toInt(row?.product_id)),
    product_name: String((row?.products || {}).name || "Sản phẩm"),
    quantity: Math.max(1, toInt(row?.quantity, 1)),
    price: Math.max(0, toInt(row?.price)),
    created_at: String(row?.created_at || "")
  }));

  return {
    telegramUserId: userId,
    directOrders: mappedDirect,
    orders: mappedOrders
  };
}

export async function getWebsiteUserOrdersSummary(authUserId: string, limit = 20): Promise<WebsiteUserOrdersSummary> {
  const userId = String(authUserId || "").trim();
  if (!userId) {
    throw new Error("Auth user id không hợp lệ.");
  }

  const supabase = getSupabaseAdminClient();
  const [{ data: directRows, error: directError }, { data: orderRows, error: orderError }, { data: userRow }] =
    await Promise.all([
      supabase
        .from("website_direct_orders")
        .select("id, code, status, amount, quantity, bonus_quantity, unit_price, created_at, product_id, products(name)")
        .eq("auth_user_id", userId)
        .order("created_at", { ascending: false })
        .limit(limit),
      supabase
        .from("website_orders")
        .select("id, product_id, price, quantity, created_at, products(name)")
        .eq("auth_user_id", userId)
        .order("created_at", { ascending: false })
        .limit(limit),
      supabase
        .from("website_users")
        .select("email")
        .eq("auth_user_id", userId)
        .maybeSingle()
    ]);

  if (directError) throw new Error(directError.message);
  if (orderError) throw new Error(orderError.message);

  const mappedDirect = await Promise.all(
    ((directRows as any[]) || []).map((row) => applyDirectOrderExpiryIfNeeded(mapDirectOrderRow(row)))
  );
  const mappedOrders: UserOrderRecord[] = ((orderRows as any[]) || []).map((row) => ({
    id: toInt(row?.id),
    product_id: Math.max(0, toInt(row?.product_id)),
    product_name: String((row?.products || {}).name || "Sản phẩm"),
    quantity: Math.max(1, toInt(row?.quantity, 1)),
    price: Math.max(0, toInt(row?.price)),
    created_at: String(row?.created_at || "")
  }));

  return {
    authUserId: userId,
    email: String((userRow as any)?.email || ""),
    directOrders: mappedDirect,
    orders: mappedOrders
  };
}

export async function getStorefrontBootstrap(): Promise<StorefrontBootstrap> {
  const [products, settings] = await Promise.all([getProductsWithStock(), getStorefrontSettings()]);
  const supportContacts = settings.show_support
    ? parseSupportContacts(settings.support_contacts, settings.admin_contact)
    : [];

  return {
    products,
    settings,
    supportContacts
  };
}
