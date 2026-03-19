import StorefrontPage from "@/components/StorefrontPage";
import type { StorefrontBootstrap, StorefrontSettings, SupportContact } from "@/lib/shop";
import { getStorefrontBootstrap } from "@/lib/shop";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const fallbackSettings: StorefrontSettings = {
  bank_name: process.env.SEPAY_BANK_NAME || "MBBank",
  account_number: process.env.SEPAY_ACCOUNT_NUMBER || "0123456789",
  account_name: process.env.SEPAY_ACCOUNT_NAME || "NGUYEN VAN A",
  admin_contact: "",
  support_contacts: "",
  payment_mode: "hybrid",
  shop_page_size: 10,
  show_shop: true,
  show_support: true,
  show_history: true,
  show_deposit: true,
  show_balance: true,
  show_withdraw: true,
  show_usdt: true,
  show_language: true,
  show_app_banners: true,
  show_stats_section: true,
  show_stats_feedback: true,
  show_stats_sold: true,
  show_stats_customers: true,
  hero_banner_url: "",
  middle_banners: [],
  side_banner_left_url: "",
  side_banner_left_link: "",
  side_banner_right_url: "",
  side_banner_right_link: "",
  app_banners: [
    { image_url: "", title: "Microsoft Office", subtitle: "Bản quyền", link: "" },
    { image_url: "", title: "Khám phá thế giới AI", subtitle: "Siêu tối ưu", link: "" },
    { image_url: "", title: "Ứng dụng VPN", subtitle: "Tốc độ - bảo mật", link: "" },
    { image_url: "", title: "Steam Wallet", subtitle: "Siêu tiết kiệm", link: "" }
  ],
  faq_items: [
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
  ]
};

const fallbackContacts: SupportContact[] = [
  { label: "💬 Telegram", url: "https://t.me/your_admin" }
];

const fallbackBootstrap: StorefrontBootstrap = {
  products: [],
  settings: fallbackSettings,
  supportContacts: fallbackContacts
};

export default async function HomePage() {
  let bootstrap = fallbackBootstrap;
  let bootstrapError: string | null = null;

  try {
    bootstrap = await getStorefrontBootstrap();
    if (!bootstrap.supportContacts.length) {
      bootstrap = {
        ...bootstrap,
        supportContacts: fallbackContacts
      };
    }
  } catch (error) {
    bootstrapError = error instanceof Error ? error.message : "Không thể tải dữ liệu Supabase";
  }

  return (
    <StorefrontPage
      initialProducts={bootstrap.products}
      settings={bootstrap.settings}
      supportContacts={bootstrap.supportContacts}
      bootstrapError={bootstrapError}
    />
  );
}
