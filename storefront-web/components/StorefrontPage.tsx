"use client";

import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import type { Session } from "@supabase/supabase-js";
import { useRouter } from "next/navigation";
import Image from "next/image";
import { getPriceRangeLabel, getPricingSnapshot, normalizePriceTiers } from "@/lib/pricing";
import type {
  DirectOrderCheckoutResult,
  DirectOrderRecord,
  ShopProduct,
  StorefrontSettings,
  SupportContact
} from "@/lib/shop";
import { supabaseBrowser } from "@/lib/supabaseBrowser";
import brandLogo from "@/logo.png";

type StorefrontPageProps = {
  initialProducts: ShopProduct[];
  settings: StorefrontSettings;
  supportContacts: SupportContact[];
  bootstrapError: string | null;
  pageMode?: "home" | "search" | "checkout";
  initialSearchQuery?: string;
  initialCheckoutProductId?: number | null;
};

type WebsiteDirectOrderRow = {
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

type WebsiteOrderRow = {
  id: number;
  product_id: number;
  product_name: string;
  quantity: number;
  price: number;
  created_at: string;
};

type WebsiteMyOrdersSummary = {
  email: string;
  directOrders: WebsiteDirectOrderRow[];
  orders: WebsiteOrderRow[];
};

type CategoryPreset = {
  id: string;
  label: string;
  icon: string;
  keywords: string[];
};

const CATEGORY_PRESETS: CategoryPreset[] = [
  { id: "all", label: "Tất cả", icon: "📦", keywords: [] },
  { id: "ai", label: "Tài khoản AI", icon: "✨", keywords: ["ai", "chatgpt", "grok", "claude", "copilot"] },
  { id: "stream", label: "Giải trí", icon: "🎬", keywords: ["netflix", "spotify", "youtube", "disney"] },
  { id: "design", label: "Thiết kế", icon: "🎨", keywords: ["adobe", "figma", "canva"] },
  { id: "utility", label: "Tiện ích", icon: "🧰", keywords: ["vpn", "office", "premium", "lastpass", "camscanner"] },
  { id: "game", label: "Game", icon: "🎮", keywords: ["steam", "game", "wallet", "top-up"] }
];

const STATUS_META: Record<string, { label: string; className: string }> = {
  pending: { label: "Đang chờ thanh toán", className: "pending" },
  confirmed: { label: "Thanh toán thành công", className: "confirmed" },
  cancelled: { label: "Đơn đã hủy", className: "cancelled" },
  failed: { label: "Thanh toán lỗi", className: "failed" }
};

type CardVisualPreset = {
  coverClass: string;
  logoText: string;
};

const getLogoMonogram = (name: string) => {
  const tokens = name
    .replace(/[^A-Za-z0-9 ]/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  if (!tokens.length) return "SP";
  if (tokens.length === 1) return tokens[0].slice(0, 2).toUpperCase();
  return `${tokens[0][0] || ""}${tokens[1][0] || ""}`.toUpperCase();
};

const getCardVisual = (product: ShopProduct): CardVisualPreset => {
  const lowered = product.name.toLowerCase();

  if (lowered.includes("grok")) {
    return { coverClass: "preset-grok", logoText: "Ø" };
  }
  if (lowered.includes("netflix")) {
    return { coverClass: "preset-netflix", logoText: "N" };
  }
  if (lowered.includes("adobe")) {
    return { coverClass: "preset-adobe", logoText: "CC" };
  }
  if (lowered.includes("figma")) {
    return { coverClass: "preset-figma", logoText: "F" };
  }
  if (lowered.includes("chatgpt")) {
    return { coverClass: "preset-chatgpt", logoText: "◎" };
  }
  if (lowered.includes("freepik")) {
    return { coverClass: "preset-freepik", logoText: "F" };
  }
  if (lowered.includes("lastpass")) {
    return { coverClass: "preset-lastpass", logoText: "•••" };
  }
  if (lowered.includes("camscanner")) {
    return { coverClass: "preset-camscanner", logoText: "CS" };
  }

  const fallback = ["preset-grok", "preset-netflix", "preset-freepik", "preset-camscanner"];
  return {
    coverClass: fallback[Math.abs(product.id) % fallback.length],
    logoText: getLogoMonogram(product.name)
  };
};

const getRatingMeta = (product: ShopProduct) => {
  const value = Math.min(5, 4 + ((product.id % 12) / 10));
  const votes = Math.max(1, Math.min(99, Math.round(product.sold_count / 3) + 1));
  return {
    value: value.toFixed(1),
    votes
  };
};

const detectCategory = (name: string) => {
  const lowered = name.toLowerCase();
  for (const category of CATEGORY_PRESETS) {
    if (category.id === "all") continue;
    if (category.keywords.some((keyword) => lowered.includes(keyword))) {
      return category.id;
    }
  }
  return "utility";
};

const formatDateTime = (value: string) => {
  if (!value) return "-";
  const time = Date.parse(value);
  if (Number.isNaN(time)) return value;
  return new Date(time).toLocaleString("vi-VN", { hour12: false });
};

const formatCountDown = (targetIso: string, nowMs: number) => {
  const target = Date.parse(targetIso);
  if (Number.isNaN(target)) return "--:--";
  const remaining = Math.max(0, target - nowMs);
  const totalSeconds = Math.floor(remaining / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
};

const buildBannerStyle = (imageUrl: string): CSSProperties | undefined => {
  const trimmed = (imageUrl || "").trim();
  if (!trimmed) return undefined;
  const safeUrl = trimmed.replace(/"/g, "%22");
  return {
    backgroundImage: `url("${safeUrl}")`,
    backgroundSize: "cover",
    backgroundPosition: "center",
    backgroundRepeat: "no-repeat"
  };
};

const AuthNavIcon = ({ signedIn }: { signedIn: boolean }) => {
  if (signedIn) {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M14 3h5a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-5" />
        <polyline points="10 17 15 12 10 7" />
        <line x1="15" y1="12" x2="3" y2="12" />
      </svg>
    );
  }

  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M10 3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h5" />
      <polyline points="14 17 19 12 14 7" />
      <line x1="19" y1="12" x2="7" y2="12" />
    </svg>
  );
};

const CartNavIcon = () => (
  <svg viewBox="0 0 24 24" aria-hidden="true">
    <circle cx="9" cy="20" r="1.25" />
    <circle cx="19" cy="20" r="1.25" />
    <path d="M2 3h3l2.4 11.2a2 2 0 0 0 2 1.6h8.2a2 2 0 0 0 1.9-1.4l1.7-6.4H7.1" />
  </svg>
);

const parsePriceNumber = (raw: string) => {
  const normalized = (raw || "").replace(/[^\d]/g, "");
  if (!normalized) return null;
  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isFinite(parsed) || parsed < 0) return null;
  return parsed;
};

const getComparablePrice = (product: ShopProduct) => {
  const tiers = normalizePriceTiers(product.price_tiers);
  if (!tiers.length) return product.price;
  const minTierPrice = Math.min(...tiers.map((tier) => tier.unit_price));
  return Math.min(product.price, minTierPrice);
};

export default function StorefrontPage({
  initialProducts,
  settings,
  supportContacts,
  bootstrapError,
  pageMode = "home",
  initialSearchQuery = "",
  initialCheckoutProductId = null
}: StorefrontPageProps) {
  const router = useRouter();
  const isSearchPage = pageMode === "search";
  const isCheckoutPage = pageMode === "checkout";
  const initialSearchValue = (initialSearchQuery || "").trim();
  const searchBoxRef = useRef<HTMLDivElement | null>(null);
  const [activePanel, setActivePanel] = useState<"shop" | "orders">("shop");
  const [search, setSearch] = useState(initialSearchValue);
  const [searchFocused, setSearchFocused] = useState(false);
  const [category, setCategory] = useState("all");
  const [subCategory, setSubCategory] = useState("all");
  const [sortBy, setSortBy] = useState<"hot" | "priceAsc" | "priceDesc" | "stockDesc">("hot");
  const [showOutOfStock, setShowOutOfStock] = useState(true);
  const [priceFromInput, setPriceFromInput] = useState("");
  const [priceToInput, setPriceToInput] = useState("");
  const [priceFrom, setPriceFrom] = useState<number | null>(null);
  const [priceTo, setPriceTo] = useState<number | null>(null);
  const [visibleCount, setVisibleCount] = useState(Math.max(1, settings.shop_page_size || 10));

  const [quantity, setQuantity] = useState("1");
  const [authSession, setAuthSession] = useState<Session | null>(null);
  const [authOpen, setAuthOpen] = useState(false);
  const [authMode, setAuthMode] = useState<"login" | "signup">("login");
  const [authEmail, setAuthEmail] = useState("");
  const [authPassword, setAuthPassword] = useState("");
  const [authPasswordConfirm, setAuthPasswordConfirm] = useState("");
  const [authLoading, setAuthLoading] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [authVerifyEmail, setAuthVerifyEmail] = useState<string | null>(null);
  const [checkout, setCheckout] = useState<DirectOrderCheckoutResult | null>(null);
  const [checkoutLoading, setCheckoutLoading] = useState(false);
  const [checkoutError, setCheckoutError] = useState<string | null>(null);
  const [statusNote, setStatusNote] = useState<string | null>(null);
  const [nowMs, setNowMs] = useState(Date.now());

  const [lookupLoading, setLookupLoading] = useState(false);
  const [lookupError, setLookupError] = useState<string | null>(null);
  const [lookupData, setLookupData] = useState<WebsiteMyOrdersSummary | null>(null);

  const [codeLookup, setCodeLookup] = useState("");
  const [codeLookupLoading, setCodeLookupLoading] = useState(false);
  const [codeLookupError, setCodeLookupError] = useState<string | null>(null);
  const [codeLookupData, setCodeLookupData] = useState<DirectOrderRecord | null>(null);
  const pageStep = Math.max(1, settings.shop_page_size || 10);
  const hasSupport = settings.show_support && supportContacts.length > 0;
  const authUser = authSession?.user || null;
  const authEmailDisplay = String(authUser?.email || "");
  const checkoutProduct = useMemo(() => {
    if (!isCheckoutPage || !initialCheckoutProductId) return null;
    return initialProducts.find((item) => item.id === initialCheckoutProductId) || null;
  }, [initialProducts, initialCheckoutProductId, isCheckoutPage]);
  useEffect(() => {
    setSearch(initialSearchValue);
  }, [initialSearchValue]);

  useEffect(() => {
    if (isSearchPage) {
      setActivePanel("shop");
    }
  }, [isSearchPage]);

  useEffect(() => {
    if (isCheckoutPage) {
      setActivePanel("shop");
    }
  }, [isCheckoutPage]);

  useEffect(() => {
    if (!isCheckoutPage) return;
    setQuantity("1");
    setCheckout(null);
    setCheckoutError(null);
    setStatusNote(null);
  }, [checkoutProduct?.id, isCheckoutPage]);

  useEffect(() => {
    let mounted = true;

    supabaseBrowser.auth.getSession().then(({ data }) => {
      if (!mounted) return;
      setAuthSession(data.session || null);
    });

    const {
      data: { subscription }
    } = supabaseBrowser.auth.onAuthStateChange((_event, session) => {
      setAuthSession(session);
    });

    return () => {
      mounted = false;
      subscription.unsubscribe();
    };
  }, []);

  useEffect(() => {
    if (authMode === "login") {
      setAuthPasswordConfirm("");
    }
  }, [authMode]);

  const categories = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const product of initialProducts) {
      const key = detectCategory(product.name);
      counts[key] = (counts[key] || 0) + 1;
    }

    return CATEGORY_PRESETS.map((item) => {
      if (item.id === "all") {
        return { ...item, count: initialProducts.length };
      }
      return { ...item, count: counts[item.id] || 0 };
    });
  }, [initialProducts]);

  const searchMatches = useMemo(() => {
    const searchText = search.trim().toLowerCase();
    if (!searchText) return [];

    return initialProducts
      .filter((product) => {
        return (
          product.name.toLowerCase().includes(searchText) ||
          (product.description || "").toLowerCase().includes(searchText)
        );
      })
      .slice(0, 6);
  }, [initialProducts, search]);

  const openSearchResults = (keyword: string) => {
    const term = keyword.trim();
    router.push(term ? `/products?q=${encodeURIComponent(term)}` : "/products");
    setSearchFocused(false);
  };

  const applySearchFilters = () => {
    const parsedFrom = parsePriceNumber(priceFromInput);
    const parsedTo = parsePriceNumber(priceToInput);
    setPriceFrom(parsedFrom);
    setPriceTo(parsedTo);
  };

  const resetSearchFilters = () => {
    setCategory("all");
    setSubCategory("all");
    setSortBy("hot");
    setShowOutOfStock(true);
    setPriceFromInput("");
    setPriceToInput("");
    setPriceFrom(null);
    setPriceTo(null);
  };

  const filteredProducts = useMemo(() => {
    const searchText = search.trim().toLowerCase();
    const filterCategory = isSearchPage ? category : "all";
    const filterSubCategory = isSearchPage ? subCategory : "all";
    const filterPriceFrom = isSearchPage ? priceFrom : null;
    const filterPriceTo = isSearchPage ? priceTo : null;

    const filtered = initialProducts.filter((product) => {
      if (!showOutOfStock && product.stock <= 0) return false;
      const inCategory = filterCategory === "all" || detectCategory(product.name) === filterCategory;
      const inSubCategory = filterSubCategory === "all" || detectCategory(product.name) === filterSubCategory;
      const inSearch =
        !searchText ||
        product.name.toLowerCase().includes(searchText) ||
        (product.description || "").toLowerCase().includes(searchText);
      const comparablePrice = getComparablePrice(product);
      const inPriceFrom = filterPriceFrom == null || comparablePrice >= filterPriceFrom;
      const inPriceTo = filterPriceTo == null || comparablePrice <= filterPriceTo;
      return inCategory && inSubCategory && inSearch && inPriceFrom && inPriceTo;
    });

    const sorted = filtered.slice();
    sorted.sort((a, b) => {
      if (sortBy === "priceAsc") return a.price - b.price;
      if (sortBy === "priceDesc") return b.price - a.price;
      if (sortBy === "stockDesc") return b.stock - a.stock;
      // hot: sold count desc then stock desc
      if (b.sold_count !== a.sold_count) return b.sold_count - a.sold_count;
      return b.stock - a.stock;
    });

    return sorted;
  }, [initialProducts, search, category, subCategory, showOutOfStock, sortBy, priceFrom, priceTo, isSearchPage]);

  const visibleProducts = filteredProducts.slice(0, visibleCount);
  const productsForGrid = isSearchPage ? filteredProducts : visibleProducts;

  useEffect(() => {
    setVisibleCount(pageStep);
  }, [search, category, subCategory, sortBy, showOutOfStock, priceFrom, priceTo, pageStep]);

  const preview = useMemo(() => {
    if (!checkoutProduct) return null;
    const parsedQuantity = Math.max(1, Math.trunc(Number(quantity) || 1));
    return getPricingSnapshot({
      basePrice: checkoutProduct.price,
      priceTiers: checkoutProduct.price_tiers,
      quantity: parsedQuantity,
      promoBuyQuantity: checkoutProduct.promo_buy_quantity,
      promoBonusQuantity: checkoutProduct.promo_bonus_quantity
    });
  }, [checkoutProduct, quantity]);

  useEffect(() => {
    if (!checkout?.code || checkout.status !== "pending") return;

    const timer = setInterval(async () => {
      try {
        const response = await fetch(`/api/orders/status?code=${encodeURIComponent(checkout.code)}`, {
          cache: "no-store"
        });

        if (!response.ok) return;

        const payload = await response.json();
        const latest = payload?.order as DirectOrderRecord | undefined;
        if (!latest) return;

        if (latest.status !== checkout.status) {
          setCheckout((prev) => {
            if (!prev) return prev;
            return {
              ...prev,
              status: latest.status,
              amount: latest.amount,
              expiresAt: latest.expires_at || prev.expiresAt
            };
          });
          setStatusNote("Đơn hàng đã cập nhật trạng thái mới từ hệ thống SePay.");
        }
      } catch {
        // ignore polling failures
      }
    }, 5000);

    return () => clearInterval(timer);
  }, [checkout]);

  useEffect(() => {
    if (!checkout || checkout.status !== "pending") return;
    const timer = setInterval(() => setNowMs(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [checkout]);

  const openSupportLink = (index = 0) => {
    const target = supportContacts[index] || supportContacts[0];
    if (!target?.url) return;
    window.open(target.url, "_blank", "noopener,noreferrer");
  };

  const openCheckout = (product: ShopProduct) => {
    router.push(`/checkout?product=${product.id}`);
  };

  const toggleAuthMode = () => {
    setAuthMode((prev) => (prev === "signup" ? "login" : "signup"));
    setAuthPasswordConfirm("");
    setAuthError(null);
    setAuthVerifyEmail(null);
  };

  const submitAuth = async (event: React.FormEvent) => {
    event.preventDefault();
    const email = authEmail.trim().toLowerCase();
    const password = authPassword.trim();
    if (!email || !password) {
      setAuthError("Vui lòng nhập email và mật khẩu.");
      return;
    }
    if (authMode === "signup") {
      if (!authPasswordConfirm.trim()) {
        setAuthError("Vui lòng nhập xác nhận mật khẩu.");
        return;
      }
      if (password !== authPasswordConfirm.trim()) {
        setAuthError("Mật khẩu xác nhận không khớp.");
        return;
      }
    }

    setAuthLoading(true);
    setAuthError(null);
    try {
      if (authMode === "signup") {
        const { data, error } = await supabaseBrowser.auth.signUp({ email, password });
        if (error) throw error;
        setAuthPassword("");
        setAuthPasswordConfirm("");
        if (data.session) {
          setAuthOpen(false);
          setAuthMode("login");
          setAuthVerifyEmail(null);
        } else {
          setAuthMode("login");
          setAuthError(null);
          setAuthVerifyEmail(email);
        }
      } else {
        const { error } = await supabaseBrowser.auth.signInWithPassword({ email, password });
        if (error) throw error;
        setAuthOpen(false);
        setAuthVerifyEmail(null);
      }
    } catch (error) {
      setAuthError(error instanceof Error ? error.message : "Không thể xử lý đăng nhập.");
    } finally {
      setAuthLoading(false);
    }
  };

  const handleSignOut = async () => {
    await supabaseBrowser.auth.signOut();
    setLookupData(null);
    setLookupError(null);
  };

  const loadMyOrders = async () => {
    if (!authSession?.access_token) {
      setLookupError("Vui lòng đăng nhập để xem đơn hàng.");
      setLookupData(null);
      return;
    }

    setLookupLoading(true);
    setLookupError(null);
    try {
      const response = await fetch("/api/orders/me", {
        cache: "no-store",
        headers: {
          Authorization: `Bearer ${authSession.access_token}`
        }
      });
      const payload = await response.json();
      if (!response.ok || !payload?.ok) {
        throw new Error(payload?.error || "Không thể tải lịch sử đơn.");
      }
      setLookupData(payload.data as WebsiteMyOrdersSummary);
    } catch (error) {
      setLookupError(error instanceof Error ? error.message : "Lỗi tải đơn hàng.");
      setLookupData(null);
    } finally {
      setLookupLoading(false);
    }
  };

  useEffect(() => {
    if (!authSession?.user?.id) {
      setLookupData(null);
      setLookupError(null);
      return;
    }
    void loadMyOrders();
  }, [authSession?.user?.id]);

  const submitCheckout = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!checkoutProduct || !preview) return;
    if (!authSession?.access_token) {
      setCheckoutError("Vui lòng đăng nhập tài khoản Website trước khi thanh toán.");
      return;
    }

    if (checkoutProduct.stock < preview.deliveredQuantity) {
      setCheckoutError(`Không đủ tồn kho. Cần ${preview.deliveredQuantity}, hiện còn ${checkoutProduct.stock}.`);
      return;
    }

    setCheckoutError(null);
    setCheckoutLoading(true);

    try {
      const response = await fetch("/api/checkout", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${authSession.access_token}`
        },
        body: JSON.stringify({
          productId: checkoutProduct.id,
          quantity: preview.quantity
        })
      });

      const payload = await response.json();
      if (!response.ok || !payload?.ok) {
        throw new Error(payload?.error || "Không thể tạo đơn thanh toán.");
      }

      setCheckout(payload.data as DirectOrderCheckoutResult);
      setStatusNote("Đơn đã được tạo. Hệ thống SePay sẽ cập nhật trạng thái khi nhận đúng nội dung chuyển khoản.");
      setCodeLookup((payload.data as DirectOrderCheckoutResult).code || "");
      setCodeLookupData(null);
      await loadMyOrders();
    } catch (error) {
      setCheckoutError(error instanceof Error ? error.message : "Đã có lỗi khi tạo đơn.");
    } finally {
      setCheckoutLoading(false);
    }
  };

  const lookupByCode = async (event: React.FormEvent) => {
    event.preventDefault();
    setCodeLookupError(null);
    setCodeLookupData(null);

    const code = codeLookup.trim();
    if (!code) {
      setCodeLookupError("Vui lòng nhập mã thanh toán.");
      return;
    }

    setCodeLookupLoading(true);
    try {
      const response = await fetch(`/api/orders/status?code=${encodeURIComponent(code)}`, { cache: "no-store" });
      const payload = await response.json();
      if (!response.ok || !payload?.ok) {
        throw new Error(payload?.error || "Không tìm thấy đơn với mã này.");
      }
      setCodeLookupData(payload.order as DirectOrderRecord);
    } catch (error) {
      setCodeLookupError(error instanceof Error ? error.message : "Lỗi tra cứu mã đơn.");
    } finally {
      setCodeLookupLoading(false);
    }
  };

  const statusMeta = checkout ? STATUS_META[checkout.status] || STATUS_META.pending : STATUS_META.pending;
  const checkoutCountDown = checkout?.expiresAt ? formatCountDown(checkout.expiresAt, nowMs) : "--:--";
  const checkoutBlockedByMode = settings.payment_mode === "balance";
  const authSubmitLabel = authLoading ? "Đang xử lý..." : authMode === "signup" ? "Đăng ký" : "Đăng nhập";
  const authSwitchLabel = authMode === "signup" ? "Đã có tài khoản? Đăng nhập" : "Chưa có tài khoản? Đăng ký";

  const authForm = (
    <form className="checkout-form auth-form" onSubmit={submitAuth}>
      <div className="form-row">
        <label>Email</label>
        <input
          type="email"
          value={authEmail}
          onChange={(event) => setAuthEmail(event.target.value)}
          placeholder="you@example.com"
          required
        />
      </div>
      <div className="form-row">
        <label>Mật khẩu</label>
        <input
          type="password"
          value={authPassword}
          onChange={(event) => setAuthPassword(event.target.value)}
          placeholder="••••••••"
          minLength={6}
          required
        />
      </div>
      {authMode === "signup" && (
        <div className="form-row">
          <label>Xác nhận mật khẩu</label>
          <input
            type="password"
            value={authPasswordConfirm}
            onChange={(event) => setAuthPasswordConfirm(event.target.value)}
            placeholder="••••••••"
            minLength={6}
            required
          />
        </div>
      )}
      {authError && <p className="form-error">{authError}</p>}
      {authVerifyEmail && (
        <div className="auth-verify-box">
          <strong>Đăng ký thành công</strong>
          <p>
            Vui lòng vào email <b>{authVerifyEmail}</b> để xác thực tài khoản trước khi đăng nhập.
          </p>
          <button
            type="button"
            className="auth-switch"
            onClick={() => {
              setAuthMode("login");
              setAuthVerifyEmail(null);
            }}
          >
            Tôi đã xác thực, chuyển sang đăng nhập
          </button>
        </div>
      )}
      <div className="auth-actions">
        <button className="auth-submit" type="submit" disabled={authLoading}>
          {authSubmitLabel}
        </button>
        <button
          type="button"
          className="auth-switch"
          onClick={toggleAuthMode}
        >
          {authSwitchLabel}
        </button>
      </div>
    </form>
  );

  const totalSoldCount = useMemo(
    () => initialProducts.reduce((sum, product) => sum + Math.max(0, Number(product.sold_count) || 0), 0),
    [initialProducts]
  );
  const averageRating = useMemo(() => {
    if (!initialProducts.length) return "0.0";
    const total = initialProducts.reduce((sum, product) => sum + Number(getRatingMeta(product).value), 0);
    return (total / initialProducts.length).toFixed(1);
  }, [initialProducts]);
  const totalCustomers = useMemo(
    () => initialProducts.filter((product) => product.sold_count > 0).length,
    [initialProducts]
  );
  const visibleStatItems = useMemo(() => {
    const items = [
      { key: "feedback", label: "FEEDBACK RATING", value: averageRating, visible: settings.show_stats_feedback },
      { key: "sold", label: "PRODUCTS SOLD", value: String(totalSoldCount), visible: settings.show_stats_sold },
      { key: "customers", label: "TOTAL CUSTOMERS", value: String(totalCustomers), visible: settings.show_stats_customers }
    ];
    return items.filter((item) => item.visible);
  }, [
    averageRating,
    totalCustomers,
    totalSoldCount,
    settings.show_stats_customers,
    settings.show_stats_feedback,
    settings.show_stats_sold
  ]);
  const faqItems = useMemo(
    () =>
      (settings.faq_items || []).filter(
        (item) => item && item.enabled !== false && String(item.question || "").trim() && String(item.answer || "").trim()
      ),
    [settings.faq_items]
  );

  const scrollToSection = (id: string) => {
    if (typeof document === "undefined") return;
    const target = document.getElementById(id);
    if (!target) return;
    target.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  return (
    <div className="storefront-root pp-root">
      <header className="pp-navbar">
        <div className="pp-navbar-inner">
          <button
            type="button"
            className="pp-brand"
            onClick={() => {
              if (isCheckoutPage || isSearchPage) {
                router.push("/");
                return;
              }
              setActivePanel("shop");
              scrollToSection("home");
            }}
          >
            <Image src={brandLogo} alt="Destiny Store" className="pp-brand-logo" priority />
            <span className="pp-brand-name">Destiny Store</span>
          </button>

          <nav className="pp-nav-links" aria-label="Điều hướng">
            <button
              type="button"
              className={!isCheckoutPage && !isSearchPage && activePanel === "shop" ? "active" : ""}
              onClick={() => {
                if (isCheckoutPage || isSearchPage) {
                  router.push("/");
                  return;
                }
                setActivePanel("shop");
                scrollToSection("home");
              }}
            >
              Home
            </button>
            <button
              type="button"
              className={!isCheckoutPage && activePanel === "shop" ? "active" : ""}
              onClick={() => {
                if (isCheckoutPage) {
                  router.push("/products");
                  return;
                }
                setActivePanel("shop");
                if (isSearchPage) {
                  router.push("/products");
                  return;
                }
                scrollToSection("products");
              }}
            >
              Products
            </button>
            <button
              type="button"
              className={!isCheckoutPage && activePanel === "orders" ? "active" : ""}
              onClick={() => {
                if (isCheckoutPage || isSearchPage) {
                  router.push("/");
                  return;
                }
                setActivePanel("orders");
                scrollToSection("status");
              }}
            >
              Status
            </button>
            <button
              type="button"
              onClick={() => {
                if (hasSupport) {
                  openSupportLink(0);
                  return;
                }
                if (isCheckoutPage || isSearchPage) {
                  router.push("/");
                  return;
                }
                scrollToSection("feedback");
              }}
            >
              Feedback
            </button>
          </nav>

          <div className="pp-nav-actions">
            <button
              type="button"
              className="pp-nav-icon"
              title={authUser ? "Đăng xuất" : "Đăng nhập"}
              onClick={() => (authUser ? handleSignOut() : setAuthOpen(true))}
            >
              <AuthNavIcon signedIn={Boolean(authUser)} />
            </button>
            <button
              type="button"
              className="pp-nav-icon"
              title="Danh sách sản phẩm"
              onClick={() => {
                if (isCheckoutPage || isSearchPage) {
                  router.push("/products");
                  return;
                }
                setActivePanel("shop");
                scrollToSection("products");
              }}
            >
              <CartNavIcon />
            </button>
          </div>
        </div>
      </header>

      <main className="pp-main">
        <section className="pp-container">
          {bootstrapError && (
            <div className="notice warning">
              <strong>Lỗi kết nối dữ liệu:</strong> {bootstrapError}
            </div>
          )}

          {activePanel === "shop" && (
            <>
              {settings.show_shop ? (
                <>
                  {isCheckoutPage ? (
                    <>
                      <section className="checkout-page-head">
                        <div className="search-breadcrumb">🏠 Trang chủ › Thanh toán</div>
                        <h2>🧾 Thanh toán đơn hàng</h2>
                        <p>Tạo đơn VietQR và theo dõi trạng thái thanh toán theo thời gian thực.</p>
                      </section>

                      {!checkoutProduct && (
                        <div className="empty-state">
                          Không tìm thấy sản phẩm để thanh toán.
                          <div style={{ marginTop: 12 }}>
                            <button type="button" className="auth-switch" onClick={() => router.push("/")}>
                              Quay về trang chủ
                            </button>
                          </div>
                        </div>
                      )}

                      {checkoutProduct && !authUser && (
                        <section className="checkout-inline-card">
                          <h3>Đăng nhập để thanh toán</h3>
                          <p className="muted-text">Vui lòng đăng nhập tài khoản Website trước khi tạo đơn hàng.</p>
                          {authForm}
                        </section>
                      )}

                      {checkoutProduct && authUser && (
                        <section className="checkout-inline-card">
                          {!checkout && (
                            <form className="checkout-form" onSubmit={submitCheckout}>
                              <div className="form-row">
                                <label>Sản phẩm</label>
                                <input value={checkoutProduct.name} disabled />
                              </div>

                              <div className="form-row">
                                <label>Tài khoản Website</label>
                                <input value={authEmailDisplay || "Chưa đăng nhập"} disabled />
                              </div>

                              <div className="form-row">
                                <label>Số lượng</label>
                                <input
                                  type="number"
                                  min={1}
                                  max={Math.max(1, checkoutProduct.stock)}
                                  value={quantity}
                                  onChange={(event) => setQuantity(event.target.value)}
                                  required
                                />
                              </div>

                              {preview && (
                                <div className="checkout-preview">
                                  <div><span>Đơn giá:</span> <strong>{preview.unitPrice.toLocaleString("vi-VN")}đ</strong></div>
                                  <div><span>Số lượng mua:</span> <strong>{preview.quantity}</strong></div>
                                  <div><span>Bonus:</span> <strong>{preview.bonusQuantity}</strong></div>
                                  <div><span>Số lượng nhận:</span> <strong>{preview.deliveredQuantity}</strong></div>
                                  <div><span>Tổng tiền:</span> <strong>{preview.totalPrice.toLocaleString("vi-VN")}đ</strong></div>
                                </div>
                              )}

                              {checkoutBlockedByMode && (
                                <p className="form-error">Website đang ở mode `balance`, không hỗ trợ tạo đơn VietQR.</p>
                              )}
                              {checkoutProduct.stock <= 0 && (
                                <p className="form-error">Sản phẩm hiện đã hết hàng.</p>
                              )}
                              {checkoutError && <p className="form-error">{checkoutError}</p>}

                              <button
                                className="create-order"
                                type="submit"
                                disabled={checkoutLoading || checkoutBlockedByMode || checkoutProduct.stock <= 0}
                              >
                                {checkoutLoading ? "Đang tạo đơn..." : "Tạo đơn và lấy QR"}
                              </button>
                            </form>
                          )}

                          {checkout && (
                            <div className="payment-result">
                              <div className={`status-pill ${statusMeta.className}`}>{statusMeta.label}</div>
                              <p className="status-note">Mã thanh toán: <strong>{checkout.code}</strong></p>

                              <img src={checkout.qrUrl} alt="VietQR" className="qr-image" />

                              <div className="payment-info">
                                <div><span>Ngân hàng:</span><strong>{checkout.bankName}</strong></div>
                                <div><span>Số tài khoản:</span><strong>{checkout.accountNumber}</strong></div>
                                <div><span>Chủ tài khoản:</span><strong>{checkout.accountName}</strong></div>
                                <div><span>Số tiền:</span><strong>{checkout.amount.toLocaleString("vi-VN")}đ</strong></div>
                                <div><span>Nội dung CK:</span><strong>{checkout.code}</strong></div>
                                <div><span>Hết hạn:</span><strong>{new Date(checkout.expiresAt).toLocaleString("vi-VN")}</strong></div>
                                <div><span>Đếm ngược:</span><strong>{checkoutCountDown}</strong></div>
                              </div>

                              <div className="status-note-wrap">
                                <p>
                                  Sau khi chuyển khoản đúng nội dung, hệ thống sẽ cập nhật trạng thái đơn sang <strong>confirmed</strong> để Dashboard Website xử lý giao hàng.
                                </p>
                                {statusNote && <p>{statusNote}</p>}
                              </div>
                            </div>
                          )}
                        </section>
                      )}
                    </>
                  ) : (
                    <>
                      {!isSearchPage && (
                        <section id="home" className="pp-hero">
                          <h1>
                            Welcome to <span>DESTINY STORE</span>
                          </h1>

                          <div
                            className="pp-hero-search-wrap"
                            ref={searchBoxRef}
                            onFocusCapture={() => setSearchFocused(true)}
                            onBlurCapture={() => {
                              requestAnimationFrame(() => {
                                if (searchBoxRef.current?.contains(document.activeElement)) return;
                                setSearchFocused(false);
                              });
                            }}
                          >
                            <form
                              className="pp-hero-search"
                              aria-label="Tìm kiếm sản phẩm"
                              onSubmit={(event) => {
                                event.preventDefault();
                                openSearchResults(search);
                              }}
                            >
                              <span>⌕</span>
                              <input
                                value={search}
                                onChange={(event) => setSearch(event.target.value)}
                                placeholder="Search for products..."
                                disabled={!settings.show_shop}
                              />
                            </form>

                            {settings.show_shop && search.trim() && searchFocused && (
                              <div className="search-suggest">
                                <div className="search-suggest-list">
                                  {searchMatches.map((product) => (
                                    <button
                                      key={`search-match-${product.id}`}
                                      type="button"
                                      className="search-suggest-item"
                                      onMouseDown={(event) => event.preventDefault()}
                                      onClick={() => openSearchResults(product.name)}
                                    >
                                      <span className="search-suggest-thumb" aria-hidden>
                                        {product.website_logo_url || product.website_banner_url ? (
                                          <img
                                            src={product.website_logo_url || product.website_banner_url}
                                            alt={product.name}
                                          />
                                        ) : (
                                          "🛍️"
                                        )}
                                      </span>
                                      <span className="search-suggest-content">
                                        <strong>{product.name}</strong>
                                        <em>{getPriceRangeLabel(product.price, product.price_tiers)}</em>
                                      </span>
                                    </button>
                                  ))}

                                  {!searchMatches.length && <div className="search-suggest-empty">Không tìm thấy sản phẩm phù hợp.</div>}
                                </div>
                                <button
                                  type="button"
                                  className="search-view-all"
                                  onMouseDown={(event) => event.preventDefault()}
                                  onClick={() => openSearchResults(search)}
                                >
                                  Xem tất cả kết quả →
                                </button>
                              </div>
                            )}
                          </div>

                          {settings.show_stats_section && visibleStatItems.length > 0 && (
                            <div
                              className="pp-stats"
                              style={{ gridTemplateColumns: `repeat(${visibleStatItems.length}, minmax(0, 1fr))` }}
                            >
                              {visibleStatItems.map((item) => (
                                <article key={item.key}>
                                  <strong>{item.value}</strong>
                                  <span>{item.label}</span>
                                </article>
                              ))}
                            </div>
                          )}
                        </section>
                      )}

                      {!isSearchPage && (
                        <section className="pp-features">
                          <div className="pp-section-title">Features 💡</div>
                          <div className="pp-feature-grid">
                            <article>
                              <span>☆</span>
                              <h3>Instant Delivery</h3>
                              <p>Đơn hàng được xử lý nhanh theo logic tồn kho và checker tự động.</p>
                            </article>
                            <article>
                              <span>☆</span>
                              <h3>Secure Payments</h3>
                              <p>Thanh toán VietQR + SePay, theo dõi trạng thái real-time và chống trùng giao dịch.</p>
                            </article>
                            <article>
                              <span>☆</span>
                              <h3>24/7 Support</h3>
                              <p>Hỗ trợ qua các kênh đã cấu hình trong Dashboard, bám sát quy trình bot hiện tại.</p>
                            </article>
                          </div>
                        </section>
                      )}

                      <section id="products" className="pp-shop">
                        <div className="pp-section-title">Shop 🛒</div>

                        {isSearchPage && (
                          <p className="pp-search-summary">
                            {search.trim()
                              ? `Kết quả cho "${search.trim()}" • ${filteredProducts.length} sản phẩm`
                              : `${filteredProducts.length} sản phẩm`}
                          </p>
                        )}

                        <div className="pp-shop-toolbar">
                          <label className="pp-sort-pill">
                            <span>Sắp xếp</span>
                            <select value={sortBy} onChange={(event) => setSortBy(event.target.value as "hot" | "priceAsc" | "priceDesc" | "stockDesc")}>
                              <option value="hot">Hot (đã bán)</option>
                              <option value="priceAsc">Giá tăng dần</option>
                              <option value="priceDesc">Giá giảm dần</option>
                              <option value="stockDesc">Tồn kho giảm dần</option>
                            </select>
                          </label>
                          <label className={`pp-stock-toggle${showOutOfStock ? " active" : ""}`}>
                            <input
                              type="checkbox"
                              checked={showOutOfStock}
                              onChange={(event) => setShowOutOfStock(event.target.checked)}
                            />
                            <span>Hiện cả hết hàng</span>
                          </label>
                        </div>

                        <section className="pp-product-grid">
                          {productsForGrid.map((product) => {
                            const priceLabel = getPriceRangeLabel(product.price, product.price_tiers);
                            const inStock = product.stock > 0;
                            const visual = getCardVisual(product);
                            const hasCoverImage = Boolean(product.website_banner_url);
                            const coverStyle = buildBannerStyle(product.website_banner_url);

                            return (
                              <article
                                key={product.id}
                                className={`pp-product-card${inStock ? "" : " is-unavailable"}`}
                                role="button"
                                tabIndex={0}
                                onClick={() => openCheckout(product)}
                                onKeyDown={(event) => {
                                  if (event.key !== "Enter" && event.key !== " ") return;
                                  event.preventDefault();
                                  openCheckout(product);
                                }}
                              >
                                <div
                                  className={`pp-product-cover ${visual.coverClass}${hasCoverImage ? " has-image" : ""}`}
                                  style={coverStyle}
                                  aria-label={`Banner hàng hóa ${product.name}`}
                                >
                                  {!hasCoverImage && <span>{visual.logoText}</span>}
                                </div>

                                <div className="pp-product-body">
                                  <div className="pp-product-row">
                                    <strong>{priceLabel}</strong>
                                    <em className={inStock ? "in" : "out"}>{inStock ? `${product.stock} In Stock` : "Out of Stock"}</em>
                                  </div>
                                  <h3>{product.name}</h3>
                                  <button
                                    type="button"
                                    onClick={(event) => {
                                      event.stopPropagation();
                                      openCheckout(product);
                                    }}
                                  >
                                    →
                                  </button>
                                </div>
                              </article>
                            );
                          })}
                        </section>

                        {!productsForGrid.length && <div className="empty-state">Không tìm thấy sản phẩm phù hợp.</div>}

                        {!isSearchPage && visibleCount < filteredProducts.length && (
                          <div className="load-more-wrap">
                            <button type="button" className="load-more" onClick={() => setVisibleCount((prev) => prev + pageStep)}>
                              Xem thêm sản phẩm
                            </button>
                          </div>
                        )}
                      </section>

                      {!isSearchPage && faqItems.length > 0 && (
                        <section id="feedback" className="pp-faq">
                          <div className="pp-section-title">
                            FAQ <span>?</span>
                          </div>
                          {faqItems.map((item, index) => (
                            <details key={`faq-${index}-${item.question}`} open={index === 0}>
                              <summary>{item.question}</summary>
                              <p>{item.answer}</p>
                            </details>
                          ))}
                        </section>
                      )}
                    </>
                  )}
                </>
              ) : (
                <div className="empty-state">Shop hiện đang tạm tắt trong Dashboard (`show_shop=false`).</div>
              )}
            </>
          )}

          {!isCheckoutPage && activePanel === "orders" && (
            <section id="status" className="orders-panel pp-orders-panel">
              <h3>Trạng thái đơn hàng</h3>
              <p className="muted-text">
                Dữ liệu được đồng bộ từ Website Dashboard (`website_direct_orders`, `website_orders`).
              </p>

              {!authUser ? (
                <section className="checkout-inline-card">
                  <h3>Đăng nhập để xem đơn hàng</h3>
                  <p className="muted-text">Trang này yêu cầu đăng nhập tài khoản Website trước khi tra cứu lịch sử đơn.</p>
                  {authForm}
                </section>
              ) : (
                <>
                  <div className="order-form" style={{ marginBottom: 10 }}>
                    <input value={authEmailDisplay} disabled />
                    <button type="button" onClick={() => void loadMyOrders()} disabled={lookupLoading}>
                      {lookupLoading ? "Đang tải..." : "Làm mới"}
                    </button>
                  </div>

                  {lookupError && <p className="form-error">{lookupError}</p>}

                  {lookupData && (
                    <div className="orders-result">
                      <h4>Direct Orders (VietQR)</h4>
                      <table className="order-table">
                        <thead>
                          <tr>
                            <th>Mã</th>
                            <th>Sản phẩm</th>
                            <th>Trạng thái</th>
                            <th>Số tiền</th>
                            <th>Mua/Nhận</th>
                            <th>Tạo lúc</th>
                          </tr>
                        </thead>
                        <tbody>
                          {lookupData.directOrders.map((order) => (
                            <tr key={`d-${order.id}`}>
                              <td>{order.code}</td>
                              <td>{order.product_name}</td>
                              <td>
                                <span className={`status-pill ${STATUS_META[order.status]?.className || "pending"}`}>
                                  {STATUS_META[order.status]?.label || order.status}
                                </span>
                              </td>
                              <td>{order.amount.toLocaleString("vi-VN")}đ</td>
                              <td>
                                {order.quantity} / {order.quantity + order.bonus_quantity}
                              </td>
                              <td>{formatDateTime(order.created_at)}</td>
                            </tr>
                          ))}
                          {!lookupData.directOrders.length && (
                            <tr>
                              <td colSpan={6}>Chưa có direct order.</td>
                            </tr>
                          )}
                        </tbody>
                      </table>

                      <h4 style={{ marginTop: 16 }}>Orders (đã giao)</h4>
                      <table className="order-table">
                        <thead>
                          <tr>
                            <th>ID</th>
                            <th>Sản phẩm</th>
                            <th>Số lượng</th>
                            <th>Giá</th>
                            <th>Thời gian</th>
                          </tr>
                        </thead>
                        <tbody>
                          {lookupData.orders.map((order) => (
                            <tr key={`o-${order.id}`}>
                              <td>#{order.id}</td>
                              <td>{order.product_name}</td>
                              <td>{order.quantity}</td>
                              <td>{order.price.toLocaleString("vi-VN")}đ</td>
                              <td>{formatDateTime(order.created_at)}</td>
                            </tr>
                          ))}
                          {!lookupData.orders.length && (
                            <tr>
                              <td colSpan={5}>Chưa có đơn đã giao.</td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  )}
                </>
              )}

              <div className="code-lookup-box">
                <h4>Tra cứu nhanh theo mã thanh toán</h4>
                <form className="order-form" onSubmit={lookupByCode}>
                  <input
                    value={codeLookup}
                    onChange={(event) => setCodeLookup(event.target.value)}
                    placeholder="VD: SEBUY 1234567891234"
                  />
                  <button type="submit" disabled={codeLookupLoading}>{codeLookupLoading ? "Đang tra cứu..." : "Tra mã"}</button>
                </form>
                {codeLookupError && <p className="form-error">{codeLookupError}</p>}
                {codeLookupData && (
                  <div className="code-result">
                    <p><strong>Mã:</strong> {codeLookupData.code}</p>
                    <p><strong>Trạng thái:</strong> {STATUS_META[codeLookupData.status]?.label || codeLookupData.status}</p>
                    <p><strong>Số tiền:</strong> {codeLookupData.amount.toLocaleString("vi-VN")}đ</p>
                    <p><strong>Tạo lúc:</strong> {formatDateTime(codeLookupData.created_at)}</p>
                    <p><strong>Hết hạn:</strong> {formatDateTime(codeLookupData.expires_at)}</p>
                  </div>
                )}
              </div>
            </section>
          )}

        </section>
      </main>

      {authOpen && (
        <div className="checkout-overlay" onClick={() => setAuthOpen(false)}>
          <div className="checkout-modal" onClick={(event) => event.stopPropagation()}>
            <div className="checkout-head">
              <h3>{authMode === "signup" ? "Tạo tài khoản Website" : "Đăng nhập Website"}</h3>
              <button type="button" onClick={() => setAuthOpen(false)}>✕</button>
            </div>
            {authForm}
          </div>
        </div>
      )}
    </div>
  );
}
