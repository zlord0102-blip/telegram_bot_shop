import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Destiny Store",
  description: "Destiny Store - Storefront NextJS kết nối Supabase, thanh toán VietQR + SePay"
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="vi">
      <body>{children}</body>
    </html>
  );
}
