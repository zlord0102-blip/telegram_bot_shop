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

/* ===== Types ===== */
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

/* ===== Category Presets (chototmmo-style) ===== */
type CategoryPreset = {
  id: string;
  label: string;
  icon: string;
  iconUrl?: string;
  keywords: string[];
};

const CATEGORY_PRESETS: CategoryPreset[] = [
  { id: "all", label: "Tất cả", icon: "📦", keywords: [] },
  { id: "twitter", label: "Twitter", icon: "🐦", iconUrl: "https://chototmmo.com/assets/storage/images/iconE9V7.png", keywords: ["twitter"] },
  { id: "telegram", label: "Telegram", icon: "✈️", iconUrl: "https://chototmmo.com/assets/storage/images/category7NVU.png", keywords: ["telegram", "tdata"] },
  { id: "tiktok", label: "Tiktok", icon: "🎵", iconUrl: "https://chototmmo.com/assets/storage/images/icon5EMZ.png", keywords: ["tiktok"] },
  { id: "facebook", label: "Facebook", icon: "📘", iconUrl: "https://chototmmo.com/assets/storage/images/iconDYXU.png", keywords: ["facebook", "fb"] },
  { id: "instagram", label: "Instagram", icon: "📷", iconUrl: "https://chototmmo.com/assets/storage/images/icon03HJ.png", keywords: ["instagram"] },
  { id: "discord", label: "Discord", icon: "🎮", iconUrl: "https://chototmmo.com/assets/storage/images/iconDBVW.png", keywords: ["discord"] },
  { id: "email", label: "Email", icon: "✉️", iconUrl: "https://chototmmo.com/assets/storage/images/categoryP43H.png", keywords: ["gmail", "email", "outlook", "hotmail"] },
  { id: "ai", label: "AI", icon: "✨", iconUrl: "https://chototmmo.com/assets/storage/images/categoryU4LP.png", keywords: ["ai", "chatgpt", "grok", "claude", "copilot", "gemini", "cursor"] },
  { id: "vpn", label: "VPN", icon: "🌐", iconUrl: "https://chototmmo.com/assets/storage/images/iconBS4R.png", keywords: ["vpn", "cyberghost", "nordvpn", "express"] },
  { id: "proxy", label: "Proxy", icon: "🔒", iconUrl: "https://chototmmo.com/assets/storage/images/iconH91D.png", keywords: ["proxy", "resident"] },
  { id: "software", label: "Phần mềm", icon: "💿", iconUrl: "https://chototmmo.com/assets/storage/images/iconDGHB.png", keywords: ["adobe", "figma", "canva", "office", "premium", "netflix", "spotify"] },
  { id: "other", label: "Tài khoản khác", icon: "👤", iconUrl: "https://chototmmo.com/assets/storage/images/iconO43A.png", keywords: [] },
];

const STATUS_META: Record<string, { label: string; className: string }> = {
  pending: { label: "Đang chờ thanh toán", className: "pending" },
  confirmed: { label: "Thanh toán thành công", className: "confirmed" },
  cancelled: { label: "Đơn đã hủy", className: "cancelled" },
  failed: { label: "Thanh toán lỗi", className: "failed" }
};

/* ===== Helpers ===== */
const getLogoMonogram = (name: string) => {
  const tokens = name.replace(/[^A-Za-z0-9 ]/g, " ").trim().split(/\s+/).filter(Boolean);
  if (!tokens.length) return "SP";
  if (tokens.length === 1) return tokens[0].slice(0, 2).toUpperCase();
  return `${tokens[0][0] || ""}${tokens[1][0] || ""}`.toUpperCase();
};

const detectCategory = (name: string) => {
  const lowered = name.toLowerCase();
  for (const cat of CATEGORY_PRESETS) {
    if (cat.id === "all") continue;
    if (cat.keywords.some((kw) => lowered.includes(kw))) return cat.id;
  }
  return "software";
};

const getRatingMeta = (product: ShopProduct) => {
  const value = Math.min(5, 4 + ((product.id % 12) / 10));
  const votes = Math.max(1, Math.min(999, Math.round(product.sold_count / 3) + 1));
  return { value: value.toFixed(1), votes };
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
  const h = Math.floor(totalSeconds / 3600);
  const m = Math.floor((totalSeconds % 3600) / 60);
  const s = totalSeconds % 60;
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
};

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
  const minTierPrice = Math.min(...tiers.map((t) => t.unit_price));
  return Math.min(product.price, minTierPrice);
};

const getDiscountPercent = (product: ShopProduct) => {
  const tiers = normalizePriceTiers(product.price_tiers);
  if (!tiers.length) return 0;
  const minTier = Math.min(...tiers.map((t) => t.unit_price));
  if (minTier >= product.price || product.price <= 0) return 0;
  return Math.round(((product.price - minTier) / product.price) * 100);
};

const renderStars = (rating: number) => {
  const full = Math.floor(rating);
  const half = rating - full >= 0.5;
  let stars = "";
  for (let i = 0; i < full; i++) stars += "★";
  if (half) stars += "★";
  while (stars.length < 5) stars += "☆";
  return stars;
};

/* ===== Component ===== */
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

  /* State */
  const [activePanel, setActivePanel] = useState<"shop" | "orders">("shop");
  const [search, setSearch] = useState(initialSearchValue);
  const [searchFocused, setSearchFocused] = useState(false);
  const [category, setCategory] = useState("all");
  const [sortBy, setSortBy] = useState<"hot" | "new" | "priceAsc" | "priceDesc">("hot");
  const [showOutOfStock, setShowOutOfStock] = useState(true);
  const [currentPage, setCurrentPage] = useState(1);
  const [quantity, setQuantity] = useState("1");

  /* Auth state */
  const [authSession, setAuthSession] = useState<Session | null>(null);
  const [authOpen, setAuthOpen] = useState(false);
  const [authMode, setAuthMode] = useState<"login" | "signup">("login");
  const [authEmail, setAuthEmail] = useState("");
  const [authPassword, setAuthPassword] = useState("");
  const [authPasswordConfirm, setAuthPasswordConfirm] = useState("");
  const [authLoading, setAuthLoading] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [authVerifyEmail, setAuthVerifyEmail] = useState<string | null>(null);

  /* Checkout state */
  const [checkout, setCheckout] = useState<DirectOrderCheckoutResult | null>(null);
  const [checkoutLoading, setCheckoutLoading] = useState(false);
  const [checkoutError, setCheckoutError] = useState<string | null>(null);
  const [statusNote, setStatusNote] = useState<string | null>(null);
  const [nowMs, setNowMs] = useState(Date.now());

  /* Orders state */
  const [lookupLoading, setLookupLoading] = useState(false);
  const [lookupError, setLookupError] = useState<string | null>(null);
  const [lookupData, setLookupData] = useState<WebsiteMyOrdersSummary | null>(null);
  const [codeLookup, setCodeLookup] = useState("");
  const [codeLookupLoading, setCodeLookupLoading] = useState(false);
  const [codeLookupError, setCodeLookupError] = useState<string | null>(null);
  const [codeLookupData, setCodeLookupData] = useState<DirectOrderRecord | null>(null);

  const pageSize = Math.max(1, settings.shop_page_size || 20);
  const hasSupport = settings.show_support && supportContacts.length > 0;
  const authUser = authSession?.user || null;
  const authEmailDisplay = String(authUser?.email || "");

  const checkoutProduct = useMemo(() => {
    if (!isCheckoutPage || !initialCheckoutProductId) return null;
    return initialProducts.find((p) => p.id === initialCheckoutProductId) || null;
  }, [initialProducts, initialCheckoutProductId, isCheckoutPage]);

  /* ===== Effects ===== */
  useEffect(() => { setSearch(initialSearchValue); }, [initialSearchValue]);
  useEffect(() => { if (isSearchPage) setActivePanel("shop"); }, [isSearchPage]);
  useEffect(() => { if (isCheckoutPage) setActivePanel("shop"); }, [isCheckoutPage]);

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
    const { data: { subscription } } = supabaseBrowser.auth.onAuthStateChange((_event, session) => {
      setAuthSession(session);
    });
    return () => { mounted = false; subscription.unsubscribe(); };
  }, []);

  useEffect(() => { if (authMode === "login") setAuthPasswordConfirm(""); }, [authMode]);

  /* Search matches for dropdown */
  const searchMatches = useMemo(() => {
    const text = search.trim().toLowerCase();
    if (!text) return [];
    return initialProducts
      .filter((p) => p.name.toLowerCase().includes(text) || (p.description || "").toLowerCase().includes(text))
      .slice(0, 6);
  }, [initialProducts, search]);

  /* Filtered + sorted products */
  const filteredProducts = useMemo(() => {
    const searchText = search.trim().toLowerCase();
    const filtered = initialProducts.filter((product) => {
      if (!showOutOfStock && product.stock <= 0) return false;
      const inCategory = category === "all" || detectCategory(product.name) === category;
      const inSearch = !searchText || product.name.toLowerCase().includes(searchText) || (product.description || "").toLowerCase().includes(searchText);
      return inCategory && inSearch;
    });

    const sorted = filtered.slice();
    sorted.sort((a, b) => {
      if (sortBy === "priceAsc") return getComparablePrice(a) - getComparablePrice(b);
      if (sortBy === "priceDesc") return getComparablePrice(b) - getComparablePrice(a);
      if (sortBy === "new") return b.id - a.id;
      // hot: sold count desc
      if (b.sold_count !== a.sold_count) return b.sold_count - a.sold_count;
      return b.stock - a.stock;
    });
    return sorted;
  }, [initialProducts, search, category, showOutOfStock, sortBy]);

  /* Pagination */
  const totalPages = Math.max(1, Math.ceil(filteredProducts.length / pageSize));
  const safePage = Math.min(currentPage, totalPages);
  const pagedProducts = filteredProducts.slice((safePage - 1) * pageSize, safePage * pageSize);

  useEffect(() => { setCurrentPage(1); }, [search, category, sortBy, showOutOfStock]);

  /* Deal product = best seller */
  const dealProduct = useMemo(() => {
    const inStock = initialProducts.filter((p) => p.stock > 0);
    if (!inStock.length) return null;
    return inStock.reduce((best, p) => (p.sold_count > best.sold_count ? p : best), inStock[0]);
  }, [initialProducts]);

  /* Checkout preview */
  const preview = useMemo(() => {
    if (!checkoutProduct) return null;
    const parsedQty = Math.max(1, Math.trunc(Number(quantity) || 1));
    return getPricingSnapshot({
      basePrice: checkoutProduct.price,
      priceTiers: checkoutProduct.price_tiers,
      quantity: parsedQty,
      promoBuyQuantity: checkoutProduct.promo_buy_quantity,
      promoBonusQuantity: checkoutProduct.promo_bonus_quantity
    });
  }, [checkoutProduct, quantity]);

  /* Checkout polling */
  useEffect(() => {
    if (!checkout?.code || checkout.status !== "pending" || !authSession?.access_token) return;
    const timer = setInterval(async () => {
      try {
        const res = await fetch(`/api/orders/status?code=${encodeURIComponent(checkout.code)}`, {
          cache: "no-store",
          headers: { Authorization: `Bearer ${authSession.access_token}` }
        });
        if (!res.ok) return;
        const payload = await res.json();
        const latest = payload?.order as DirectOrderRecord | undefined;
        if (!latest) return;
        if (latest.status !== checkout.status) {
          setCheckout((prev) => {
            if (!prev) return prev;
            return { ...prev, status: latest.status, amount: latest.amount, expiresAt: latest.expires_at || prev.expiresAt };
          });
          setStatusNote("Đơn hàng đã cập nhật trạng thái mới từ hệ thống SePay.");
        }
      } catch { /* ignore */ }
    }, 5000);
    return () => clearInterval(timer);
  }, [checkout, authSession?.access_token]);

  useEffect(() => {
    if (!checkout || checkout.status !== "pending") return;
    const t = setInterval(() => setNowMs(Date.now()), 1000);
    return () => clearInterval(t);
  }, [checkout]);

  /* FAQ */
  const faqItems = useMemo(
    () => (settings.faq_items || []).filter((item) => item && item.enabled !== false && String(item.question || "").trim() && String(item.answer || "").trim()),
    [settings.faq_items]
  );

  /* ===== Handlers ===== */
  const openSupportLink = (index = 0) => {
    const target = supportContacts[index] || supportContacts[0];
    if (!target?.url) return;
    window.open(target.url, "_blank", "noopener,noreferrer");
  };

  const openSearchResults = (keyword: string) => {
    const term = keyword.trim();
    router.push(term ? `/products?q=${encodeURIComponent(term)}` : "/products");
    setSearchFocused(false);
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
    if (!email || !password) { setAuthError("Vui lòng nhập email và mật khẩu."); return; }
    if (authMode === "signup") {
      if (!authPasswordConfirm.trim()) { setAuthError("Vui lòng nhập xác nhận mật khẩu."); return; }
      if (password !== authPasswordConfirm.trim()) { setAuthError("Mật khẩu xác nhận không khớp."); return; }
    }
    setAuthLoading(true);
    setAuthError(null);
    try {
      if (authMode === "signup") {
        const { data, error } = await supabaseBrowser.auth.signUp({ email, password });
        if (error) throw error;
        setAuthPassword("");
        setAuthPasswordConfirm("");
        if (data.session) { setAuthOpen(false); setAuthMode("login"); setAuthVerifyEmail(null); }
        else { setAuthMode("login"); setAuthError(null); setAuthVerifyEmail(email); }
      } else {
        const { error } = await supabaseBrowser.auth.signInWithPassword({ email, password });
        if (error) throw error;
        setAuthOpen(false);
        setAuthVerifyEmail(null);
      }
    } catch (error) {
      setAuthError(error instanceof Error ? error.message : "Không thể xử lý đăng nhập.");
    } finally { setAuthLoading(false); }
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
      const res = await fetch("/api/orders/me", { cache: "no-store", headers: { Authorization: `Bearer ${authSession.access_token}` } });
      const payload = await res.json();
      if (!res.ok || !payload?.ok) throw new Error(payload?.error || "Không thể tải lịch sử đơn.");
      setLookupData(payload.data as WebsiteMyOrdersSummary);
    } catch (error) {
      setLookupError(error instanceof Error ? error.message : "Lỗi tải đơn hàng.");
      setLookupData(null);
    } finally { setLookupLoading(false); }
  };

  useEffect(() => {
    if (!authSession?.user?.id) { setLookupData(null); setLookupError(null); return; }
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
      const res = await fetch("/api/checkout", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${authSession.access_token}` },
        body: JSON.stringify({ productId: checkoutProduct.id, quantity: preview.quantity })
      });
      const payload = await res.json();
      if (!res.ok || !payload?.ok) throw new Error(payload?.error || "Không thể tạo đơn thanh toán.");
      setCheckout(payload.data as DirectOrderCheckoutResult);
      setStatusNote("Đơn đã được tạo. Hệ thống SePay sẽ cập nhật trạng thái khi nhận đúng nội dung chuyển khoản.");
      setCodeLookup((payload.data as DirectOrderCheckoutResult).code || "");
      setCodeLookupData(null);
      await loadMyOrders();
    } catch (error) {
      setCheckoutError(error instanceof Error ? error.message : "Đã có lỗi khi tạo đơn.");
    } finally { setCheckoutLoading(false); }
  };

  const lookupByCode = async (event: React.FormEvent) => {
    event.preventDefault();
    setCodeLookupError(null);
    setCodeLookupData(null);
    const code = codeLookup.trim();
    if (!code) { setCodeLookupError("Vui lòng nhập mã thanh toán."); return; }
    if (!authSession?.access_token) { setCodeLookupError("Vui lòng đăng nhập để tra cứu đơn hàng."); return; }
    setCodeLookupLoading(true);
    try {
      const res = await fetch(`/api/orders/status?code=${encodeURIComponent(code)}`, {
        cache: "no-store",
        headers: { Authorization: `Bearer ${authSession.access_token}` }
      });
      const payload = await res.json();
      if (!res.ok || !payload?.ok) throw new Error(payload?.error || "Không tìm thấy đơn với mã này.");
      setCodeLookupData(payload.order as DirectOrderRecord);
    } catch (error) {
      setCodeLookupError(error instanceof Error ? error.message : "Lỗi tra cứu mã đơn.");
    } finally { setCodeLookupLoading(false); }
  };

  const statusMeta = checkout ? STATUS_META[checkout.status] || STATUS_META.pending : STATUS_META.pending;
  const checkoutCountDown = checkout?.expiresAt ? formatCountDown(checkout.expiresAt, nowMs) : "--:--:--";
  const checkoutBlockedByMode = settings.payment_mode === "balance";
  const authSubmitLabel = authLoading ? "Đang xử lý..." : authMode === "signup" ? "Đăng ký" : "Đăng nhập";
  const authSwitchLabel = authMode === "signup" ? "Đã có tài khoản? Đăng nhập" : "Chưa có tài khoản? Đăng ký";

  /* Countdown for deal card */
  const [dealCountdown, setDealCountdown] = useState("09:59:59");
  useEffect(() => {
    const endOfDay = new Date();
    endOfDay.setHours(23, 59, 59, 999);
    const target = endOfDay.getTime();
    const update = () => {
      const diff = Math.max(0, target - Date.now());
      const h = Math.floor(diff / 3600000);
      const m = Math.floor((diff % 3600000) / 60000);
      const s = Math.floor((diff % 60000) / 1000);
      setDealCountdown(`${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`);
    };
    update();
    const t = setInterval(update, 1000);
    return () => clearInterval(t);
  }, []);

  /* Auth form fragment */
  const authForm = (
    <form onSubmit={submitAuth}>
      <div className="ct-form-row">
        <label>Email</label>
        <input type="email" value={authEmail} onChange={(e) => setAuthEmail(e.target.value)} placeholder="you@example.com" required />
      </div>
      <div className="ct-form-row">
        <label>Mật khẩu</label>
        <input type="password" value={authPassword} onChange={(e) => setAuthPassword(e.target.value)} placeholder="••••••••" minLength={6} required />
      </div>
      {authMode === "signup" && (
        <div className="ct-form-row">
          <label>Xác nhận mật khẩu</label>
          <input type="password" value={authPasswordConfirm} onChange={(e) => setAuthPasswordConfirm(e.target.value)} placeholder="••••••••" minLength={6} required />
        </div>
      )}
      {authError && <p className="ct-form-error">{authError}</p>}
      {authVerifyEmail && (
        <div className="ct-auth-verify-box">
          <strong>Đăng ký thành công</strong>
          <p>Vui lòng vào email <b>{authVerifyEmail}</b> để xác thực tài khoản trước khi đăng nhập.</p>
          <button type="button" className="ct-auth-switch" onClick={() => { setAuthMode("login"); setAuthVerifyEmail(null); }}>
            Tôi đã xác thực, chuyển sang đăng nhập
          </button>
        </div>
      )}
      <div className="ct-auth-actions">
        <button className="ct-auth-submit" type="submit" disabled={authLoading}>{authSubmitLabel}</button>
        <button type="button" className="ct-auth-switch" onClick={toggleAuthMode}>{authSwitchLabel}</button>
      </div>
    </form>
  );

  /* Pagination range */
  const paginationRange = useMemo(() => {
    const pages: number[] = [];
    const maxVisible = 5;
    let start = Math.max(1, safePage - Math.floor(maxVisible / 2));
    let end = Math.min(totalPages, start + maxVisible - 1);
    start = Math.max(1, end - maxVisible + 1);
    for (let i = start; i <= end; i++) pages.push(i);
    return pages;
  }, [safePage, totalPages]);

  /* ===== RENDER ===== */
  return (
    <div className="storefront-root">
      {/* ===== HEADER ===== */}
      <header className="ct-header">
        <div className="ct-header-inner">
          <button type="button" className="ct-logo" onClick={() => {
            if (isCheckoutPage || isSearchPage) { router.push("/"); return; }
            setActivePanel("shop");
            setCategory("all");
            setSearch("");
          }}>
            <Image src={brandLogo} alt="Destiny Store" width={30} height={30} priority />
            <span className="ct-logo-text"><span className="blue">Destiny</span><span className="orange">Store</span></span>
          </button>

          <div className="ct-search-wrap" ref={searchBoxRef}
            onFocusCapture={() => setSearchFocused(true)}
            onBlurCapture={() => { requestAnimationFrame(() => { if (searchBoxRef.current?.contains(document.activeElement)) return; setSearchFocused(false); }); }}
          >
            <form className="ct-search-bar" onSubmit={(e) => { e.preventDefault(); openSearchResults(search); }}>
              <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Tìm sản phẩm, gian hàng, dịch vụ hot..." disabled={!settings.show_shop} />
              <button type="submit" className="ct-search-btn">🔍</button>
            </form>

            {settings.show_shop && search.trim() && searchFocused && (
              <div className="ct-suggest">
                <div className="ct-suggest-list">
                  {searchMatches.map((product) => (
                    <button key={`s-${product.id}`} type="button" className="ct-suggest-item"
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => openSearchResults(product.name)}
                    >
                      <span className="ct-suggest-thumb">
                        {product.website_logo_url || product.website_banner_url
                          ? <img src={product.website_logo_url || product.website_banner_url} alt={product.name} />
                          : "🛍️"}
                      </span>
                      <span className="ct-suggest-info">
                        <strong>{product.name}</strong>
                        <em>{getPriceRangeLabel(product.price, product.price_tiers)}</em>
                      </span>
                    </button>
                  ))}
                  {!searchMatches.length && <div className="ct-suggest-empty">Không tìm thấy sản phẩm phù hợp.</div>}
                </div>
                <button type="button" className="ct-suggest-all" onMouseDown={(e) => e.preventDefault()} onClick={() => openSearchResults(search)}>
                  Xem tất cả kết quả →
                </button>
              </div>
            )}
          </div>

          <nav className="ct-nav-links">
            <button type="button" className="ct-nav-link" title="Tools">⚙️ Tools</button>
            <button type="button" className={`ct-nav-link${!isCheckoutPage && activePanel === "orders" ? " active" : ""}`}
              onClick={() => { if (isCheckoutPage || isSearchPage) { router.push("/"); } setActivePanel("orders"); }}
            >
              <svg viewBox="0 0 24 24"><path d="M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2" /><rect x="9" y="3" width="6" height="4" rx="1" /></svg>
              Đơn hàng
            </button>
            <button type="button" className="ct-nav-icon-btn" title="Tin nhắn">
              💬
            </button>
            <button type="button" className="ct-nav-icon-btn" title="Thông báo">
              🔔<span className="ct-nav-badge">2</span>
            </button>
            <button type="button" className="ct-deposit-btn">
              <svg viewBox="0 0 24 24"><path d="M17 9V7a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v6a2 2 0 0 0 2 2h2" /><rect x="9" y="9" width="13" height="10" rx="2" /></svg>
              Nạp tiền
            </button>
          </nav>

          <button type="button" className="ct-user-pill"
            onClick={() => authUser ? handleSignOut() : setAuthOpen(true)}
            title={authUser ? "Đăng xuất" : "Đăng nhập"}
          >
            <span className="ct-user-avatar">{authUser ? "👤" : "🔑"}</span>
            {authUser ? authEmailDisplay.split("@")[0] : "Đăng Nhập"}
          </button>
        </div>
      </header>

      {/* ===== MAIN ===== */}
      <main className="ct-main">
        {bootstrapError && (
          <div className="ct-notice warning">
            <strong>Lỗi kết nối dữ liệu:</strong> {bootstrapError}
          </div>
        )}

        {activePanel === "shop" && (
          <>
            {settings.show_shop ? (
              <>
                {isCheckoutPage ? (
                  /* ===== CHECKOUT PAGE ===== */
                  <div className="ct-checkout-section">
                    <div className="ct-breadcrumb">🏠 Trang chủ › Thanh toán</div>
                    <h2 className="ct-checkout-title">🧾 Thanh toán đơn hàng</h2>
                    <p className="ct-checkout-desc">Tạo đơn VietQR và theo dõi trạng thái thanh toán theo thời gian thực.</p>

                    {!checkoutProduct && (
                      <div className="ct-empty">
                        Không tìm thấy sản phẩm để thanh toán.
                        <div style={{ marginTop: 12 }}>
                          <button type="button" className="ct-auth-switch" onClick={() => router.push("/")}>Quay về trang chủ</button>
                        </div>
                      </div>
                    )}

                    {checkoutProduct && !authUser && (
                      <div className="ct-checkout-card">
                        <h3>Đăng nhập để thanh toán</h3>
                        <p className="muted-text">Vui lòng đăng nhập tài khoản Website trước khi tạo đơn hàng.</p>
                        {authForm}
                      </div>
                    )}

                    {checkoutProduct && authUser && (
                      <div className="ct-checkout-card">
                        {!checkout && (
                          <form onSubmit={submitCheckout}>
                            <div className="ct-form-row"><label>Sản phẩm</label><input value={checkoutProduct.name} disabled /></div>
                            <div className="ct-form-row"><label>Tài khoản Website</label><input value={authEmailDisplay || "Chưa đăng nhập"} disabled /></div>
                            <div className="ct-form-row">
                              <label>Số lượng</label>
                              <input type="number" min={1} max={Math.max(1, checkoutProduct.stock)} value={quantity} onChange={(e) => setQuantity(e.target.value)} required />
                            </div>
                            {preview && (
                              <div className="ct-checkout-preview">
                                <div><span>Đơn giá:</span><strong>{preview.unitPrice.toLocaleString("vi-VN")}đ</strong></div>
                                <div><span>Số lượng mua:</span><strong>{preview.quantity}</strong></div>
                                <div><span>Bonus:</span><strong>{preview.bonusQuantity}</strong></div>
                                <div><span>Số lượng nhận:</span><strong>{preview.deliveredQuantity}</strong></div>
                                <div><span>Tổng tiền:</span><strong>{preview.totalPrice.toLocaleString("vi-VN")}đ</strong></div>
                              </div>
                            )}
                            {checkoutBlockedByMode && <p className="ct-form-error">Website đang ở mode `balance`, không hỗ trợ tạo đơn VietQR.</p>}
                            {checkoutProduct.stock <= 0 && <p className="ct-form-error">Sản phẩm hiện đã hết hàng.</p>}
                            {checkoutError && <p className="ct-form-error">{checkoutError}</p>}
                            <button className="ct-submit-btn" type="submit" disabled={checkoutLoading || checkoutBlockedByMode || checkoutProduct.stock <= 0}>
                              {checkoutLoading ? "Đang tạo đơn..." : "Tạo đơn và lấy QR"}
                            </button>
                          </form>
                        )}
                        {checkout && (
                          <div className="ct-payment-result">
                            <div className={`ct-status-pill ${statusMeta.className}`}>{statusMeta.label}</div>
                            <p className="ct-payment-code">Mã thanh toán: <strong>{checkout.code}</strong></p>
                            <img src={checkout.qrUrl} alt="VietQR" className="ct-qr-image" />
                            <div className="ct-payment-info">
                              <div><span>Ngân hàng:</span><strong>{checkout.bankName}</strong></div>
                              <div><span>Số tài khoản:</span><strong>{checkout.accountNumber}</strong></div>
                              <div><span>Chủ tài khoản:</span><strong>{checkout.accountName}</strong></div>
                              <div><span>Số tiền:</span><strong>{checkout.amount.toLocaleString("vi-VN")}đ</strong></div>
                              <div><span>Nội dung CK:</span><strong>{checkout.code}</strong></div>
                              <div><span>Hết hạn:</span><strong>{new Date(checkout.expiresAt).toLocaleString("vi-VN")}</strong></div>
                              <div><span>Đếm ngược:</span><strong>{checkoutCountDown}</strong></div>
                            </div>
                            <p className="ct-payment-note">
                              Sau khi chuyển khoản đúng nội dung, hệ thống sẽ cập nhật trạng thái đơn sang <strong>confirmed</strong> để Dashboard Website xử lý giao hàng.
                              {statusNote && <><br />{statusNote}</>}
                            </p>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                ) : (
                  <>
                    {/* ===== HOME: HERO (Deal + Categories) ===== */}
                    {!isSearchPage && (
                      <div className="ct-hero-wrapper">
                      <section className="ct-hero">
                        {/* Deal Hot */}
                        <div className="ct-deal-card">
                          <div className="ct-deal-header">
                            <span className="ct-deal-icon">🔥</span>
                            <span className="ct-deal-title">DEAL HOT HÔM NAY</span>
                          </div>
                          <div className="ct-deal-countdown">
                            <span className="ct-deal-countdown-icon">🕐</span>
                            Kết thúc sau: {dealCountdown}
                          </div>
                          {dealProduct && (
                            <div className="ct-deal-product">
                              <div className="ct-deal-img">
                                {dealProduct.website_banner_url
                                  ? <img src={dealProduct.website_banner_url} alt={dealProduct.name} />
                                  : getLogoMonogram(dealProduct.name)}
                              </div>
                              <div className="ct-deal-info">
                                <div className="ct-deal-name">{dealProduct.name}</div>
                                <div className="ct-deal-pricing">
                                  {getDiscountPercent(dealProduct) > 0 && (
                                    <>
                                      <span className="ct-deal-old-price">{dealProduct.price.toLocaleString("vi-VN")}đ</span>
                                      <span className="ct-deal-discount">-{getDiscountPercent(dealProduct)}%</span>
                                    </>
                                  )}
                                  <span className="ct-deal-price">{getPriceRangeLabel(dealProduct.price, dealProduct.price_tiers)}</span>
                                </div>
                                <div className="ct-deal-bottom">
                                  <button type="button" className="ct-deal-cta" onClick={() => openCheckout(dealProduct)}>Xem ngay →</button>
                                </div>
                              </div>
                            </div>
                          )}
                        </div>

                        {/* Category Grid */}
                        <div className="ct-categories">
                          {CATEGORY_PRESETS.filter((c) => c.id !== "all").map((cat) => (
                            <button key={cat.id} type="button"
                              className={`ct-cat-item${category === cat.id ? " active" : ""}`}
                              onClick={() => setCategory(category === cat.id ? "all" : cat.id)}
                            >
                              <span className="ct-cat-icon">
                                {cat.iconUrl
                                  ? <img src={cat.iconUrl} alt={cat.label} style={{ width: 32, height: 32, objectFit: 'contain' }} />
                                  : cat.icon}
                              </span>
                              <span className="ct-cat-label">{cat.label}</span>
                            </button>
                          ))}
                        </div>
                      </section>
                      </div>
                    )}

                    {/* ===== SEARCH PAGE HEADER ===== */}
                    {isSearchPage && (
                      <p className="ct-search-summary">
                        {search.trim()
                          ? `Kết quả cho "${search.trim()}" • ${filteredProducts.length} sản phẩm`
                          : `${filteredProducts.length} sản phẩm`}
                      </p>
                    )}

                    {/* ===== FILTER / SORT TOOLBAR ===== */}
                    <div className="ct-toolbar">
                      <div className="ct-filter-row">
                        <span className="ct-filter-label">SẮP XẾP:</span>
                        <button type="button" className={`ct-filter-pill${sortBy === "new" ? " active" : ""}`}
                          onClick={() => setSortBy("new")}>⊕ Mới</button>
                        <button type="button" className={`ct-filter-pill${sortBy === "hot" ? " active" : ""}`}
                          onClick={() => setSortBy("hot")}>⊕ Giá ưu đãi</button>
                        <button type="button" className={`ct-filter-pill${sortBy === "hot" ? " active" : ""}`}
                          onClick={() => setSortBy("hot")}>⊕ Được mua nhiều</button>
                      </div>
                      <div className="ct-sort-row">
                        <button type="button" className={`ct-sort-btn${sortBy === "priceAsc" ? " active" : ""}`}
                          onClick={() => setSortBy("priceAsc")}>↕ Giá thấp nhất</button>
                        <button type="button" className={`ct-sort-btn${sortBy === "priceDesc" ? " active" : ""}`}
                          onClick={() => setSortBy("priceDesc")}>↕ Giá cao nhất</button>
                      </div>
                    </div>

                    {/* ===== PRODUCT GRID ===== */}
                    <section className="ct-products-grid">
                      {pagedProducts.map((product) => {
                        const priceLabel = getPriceRangeLabel(product.price, product.price_tiers);
                        const inStock = product.stock > 0;
                        const rating = getRatingMeta(product);
                        const discount = getDiscountPercent(product);
                        const hasBanner = Boolean(product.website_banner_url);
                        const isNew = product.sold_count === 0;

                        return (
                          <article key={product.id}
                            className={`ct-product-card${!inStock ? " out-of-stock" : ""}`}
                            role="button" tabIndex={0}
                            onClick={() => openCheckout(product)}
                            onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openCheckout(product); } }}
                          >
                            <div className="ct-product-banner">
                              {hasBanner
                                ? <img src={product.website_banner_url} alt={product.name} />
                                : <span className="ct-product-banner-fallback">{getLogoMonogram(product.name)}</span>
                              }
                              {inStock && <span className="ct-badge-instant">GIAO NGAY</span>}
                              {discount > 0 && <span className="ct-badge-discount">-{discount}%</span>}
                            </div>
                            <div className="ct-product-body">
                              <div className="ct-product-name">{product.name}</div>
                              <div className="ct-product-rating">
                                <span className="ct-stars">{renderStars(Number(rating.value))}</span>
                                <span className="ct-rating-value">{rating.value}</span>
                                <span className="ct-rating-count">({rating.votes})</span>
                              </div>
                              <div className="ct-product-bottom">
                                <div className="ct-product-price">{priceLabel}</div>
                                <div className="ct-product-sold">
                                  {isNew ? <span className="ct-badge-new">Mới</span> : <>🛒 Đã bán: {product.sold_count.toLocaleString("vi-VN")}</>}
                                </div>
                              </div>
                            </div>
                          </article>
                        );
                      })}
                    </section>

                    {!pagedProducts.length && <div className="ct-empty">Không tìm thấy sản phẩm phù hợp.</div>}

                    {/* ===== PAGINATION ===== */}
                    {filteredProducts.length > 0 && (
                      <div className="ct-pagination">
                        <span className="ct-pagination-info">
                          Hiện thị {Math.min(filteredProducts.length, (safePage - 1) * pageSize + 1)}-{Math.min(filteredProducts.length, safePage * pageSize)} trong số {filteredProducts.length} Sản phẩm
                        </span>
                        <div className="ct-pagination-pages">
                          <button type="button" className="ct-page-btn" disabled={safePage <= 1}
                            onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}>←</button>
                          {paginationRange.map((p) => (
                            <button key={p} type="button"
                              className={`ct-page-btn${p === safePage ? " active" : ""}`}
                              onClick={() => setCurrentPage(p)}>{p}</button>
                          ))}
                          <button type="button" className="ct-page-btn" disabled={safePage >= totalPages}
                            onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}>→</button>
                        </div>
                      </div>
                    )}

                    {/* ===== FAQ ===== */}
                    {!isSearchPage && faqItems.length > 0 && (
                      <section className="ct-faq">
                        <div className="ct-section-title">Câu hỏi thường gặp</div>
                        {faqItems.map((item, i) => (
                          <details key={`faq-${i}-${item.question}`} open={i === 0}>
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
              <div className="ct-empty">Shop hiện đang tạm tắt trong Dashboard (`show_shop=false`).</div>
            )}
          </>
        )}

        {/* ===== ORDERS PANEL ===== */}
        {!isCheckoutPage && activePanel === "orders" && (
          <section className="ct-orders-panel">
            <h3>Trạng thái đơn hàng</h3>
            <p style={{ fontSize: 13, color: "#5a6578", marginBottom: 16 }}>
              Dữ liệu được đồng bộ từ Website Dashboard.
            </p>

            {!authUser ? (
              <div className="ct-checkout-card">
                <h3>Đăng nhập để xem đơn hàng</h3>
                <p className="muted-text">Trang này yêu cầu đăng nhập tài khoản Website trước khi tra cứu lịch sử đơn.</p>
                {authForm}
              </div>
            ) : (
              <>
                <div className="ct-order-form" style={{ marginBottom: 10 }}>
                  <input value={authEmailDisplay} disabled />
                  <button type="button" onClick={() => void loadMyOrders()} disabled={lookupLoading}>
                    {lookupLoading ? "Đang tải..." : "Làm mới"}
                  </button>
                </div>

                {lookupError && <p className="ct-form-error">{lookupError}</p>}

                {lookupData && (
                  <div>
                    <h4 style={{ marginBottom: 8 }}>Direct Orders (VietQR)</h4>
                    <table className="ct-order-table">
                      <thead>
                        <tr><th>Mã</th><th>Sản phẩm</th><th>Trạng thái</th><th>Số tiền</th><th>Mua/Nhận</th><th>Tạo lúc</th></tr>
                      </thead>
                      <tbody>
                        {lookupData.directOrders.map((order) => (
                          <tr key={`d-${order.id}`}>
                            <td>{order.code}</td>
                            <td>{order.product_name}</td>
                            <td><span className={`ct-status-pill ${STATUS_META[order.status]?.className || "pending"}`}>{STATUS_META[order.status]?.label || order.status}</span></td>
                            <td>{order.amount.toLocaleString("vi-VN")}đ</td>
                            <td>{order.quantity} / {order.quantity + order.bonus_quantity}</td>
                            <td>{formatDateTime(order.created_at)}</td>
                          </tr>
                        ))}
                        {!lookupData.directOrders.length && <tr><td colSpan={6}>Chưa có direct order.</td></tr>}
                      </tbody>
                    </table>

                    <h4 style={{ marginTop: 16, marginBottom: 8 }}>Orders (đã giao)</h4>
                    <table className="ct-order-table">
                      <thead>
                        <tr><th>ID</th><th>Sản phẩm</th><th>Số lượng</th><th>Giá</th><th>Thời gian</th></tr>
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
                        {!lookupData.orders.length && <tr><td colSpan={5}>Chưa có đơn đã giao.</td></tr>}
                      </tbody>
                    </table>
                  </div>
                )}
              </>
            )}

            <div className="ct-code-lookup">
              <h4>Tra cứu nhanh theo mã thanh toán</h4>
              <form className="ct-order-form" onSubmit={lookupByCode}>
                <input value={codeLookup} onChange={(e) => setCodeLookup(e.target.value)} placeholder="VD: SEBUY 1234567891234" />
                <button type="submit" disabled={codeLookupLoading}>{codeLookupLoading ? "Đang tra cứu..." : "Tra mã"}</button>
              </form>
              {codeLookupError && <p className="ct-form-error">{codeLookupError}</p>}
              {codeLookupData && (
                <div className="ct-code-result">
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
      </main>

      {/* ===== FOOTER ===== */}
      <footer className="ct-footer">
        © {new Date().getFullYear()} Destiny Store. All rights reserved.
      </footer>

      {/* ===== FLOATING SUPPORT ===== */}
      {hasSupport && (
        <button type="button" className="ct-support-float" onClick={() => openSupportLink(0)}>
          <svg viewBox="0 0 24 24"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" /></svg>
          Hỗ trợ
        </button>
      )}

      {/* ===== AUTH MODAL ===== */}
      {authOpen && (
        <div className="ct-modal-overlay" onClick={() => setAuthOpen(false)}>
          <div className="ct-modal" onClick={(e) => e.stopPropagation()}>
            <div className="ct-modal-head">
              <h3>{authMode === "signup" ? "Tạo tài khoản Website" : "Đăng nhập Website"}</h3>
              <button type="button" className="ct-modal-close" onClick={() => setAuthOpen(false)}>✕</button>
            </div>
            <div className="ct-modal-body">
              {authForm}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
