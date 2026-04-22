import { NextRequest, NextResponse } from "next/server";
import { createDirectOrderCheckout } from "@/lib/shop";
import { getSupabaseAdminClient } from "@/lib/supabaseAdmin";
import { checkRateLimit, getClientIp } from "@/lib/rateLimit";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const productId = Number(body?.productId);
    const quantity = Number(body?.quantity);
    const authHeader = request.headers.get("authorization") || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";

    if (!token) {
      return NextResponse.json({ ok: false, error: "Vui lòng đăng nhập để tạo đơn." }, { status: 401 });
    }

    const ipRateLimit = checkRateLimit(`checkout:ip:${getClientIp(request)}`, {
      windowMs: 60_000,
      max: 60
    });
    if (ipRateLimit.limited) {
      return NextResponse.json(
        { ok: false, error: "Bạn thao tác quá nhanh. Vui lòng thử lại sau." },
        { status: 429, headers: { "Retry-After": String(ipRateLimit.retryAfterSeconds) } }
      );
    }

    const supabase = getSupabaseAdminClient();
    const { data: userData, error: userError } = await supabase.auth.getUser(token);
    if (userError || !userData.user) {
      return NextResponse.json({ ok: false, error: "Phiên đăng nhập không hợp lệ." }, { status: 401 });
    }

    const userRateLimit = checkRateLimit(`checkout:user:${userData.user.id}`, {
      windowMs: 60_000,
      max: 20
    });
    if (userRateLimit.limited) {
      return NextResponse.json(
        { ok: false, error: "Bạn tạo đơn quá nhanh. Vui lòng thử lại sau." },
        { status: 429, headers: { "Retry-After": String(userRateLimit.retryAfterSeconds) } }
      );
    }

    const email = String(userData.user.email || "").trim().toLowerCase();
    if (!email) {
      return NextResponse.json({ ok: false, error: "Tài khoản chưa có email hợp lệ." }, { status: 400 });
    }

    if (!Number.isFinite(productId) || productId <= 0) {
      return NextResponse.json({ ok: false, error: "productId không hợp lệ." }, { status: 400 });
    }
    if (!Number.isFinite(quantity) || quantity <= 0) {
      return NextResponse.json({ ok: false, error: "quantity không hợp lệ." }, { status: 400 });
    }

    const result = await createDirectOrderCheckout({
      productId,
      quantity,
      websiteUser: {
        id: userData.user.id,
        email,
        displayName:
          String(userData.user.user_metadata?.full_name || userData.user.user_metadata?.name || "").trim() ||
          email.split("@")[0],
        lastSignInAt: userData.user.last_sign_in_at || null
      }
    });

    return NextResponse.json(
      { ok: true, data: result },
      {
        headers: {
          "Cache-Control": "no-store"
        }
      }
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ ok: false, error: message }, { status: 400 });
  }
}
