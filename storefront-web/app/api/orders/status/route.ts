import { NextRequest, NextResponse } from "next/server";
import { getDirectOrderByCodeForAuth } from "@/lib/shop";
import { getSupabaseAdminClient } from "@/lib/supabaseAdmin";
import { checkRateLimit, getClientIp } from "@/lib/rateLimit";

export async function GET(request: NextRequest) {
  try {
    const code = request.nextUrl.searchParams.get("code") || "";
    if (!code.trim()) {
      return NextResponse.json({ ok: false, error: "Thiếu mã đơn hàng." }, { status: 400 });
    }

    const authHeader = request.headers.get("authorization") || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
    if (!token) {
      return NextResponse.json({ ok: false, error: "Unauthorized." }, { status: 401 });
    }

    const rateLimit = checkRateLimit(`orders-status:ip:${getClientIp(request)}`, {
      windowMs: 60_000,
      max: 120
    });
    if (rateLimit.limited) {
      return NextResponse.json(
        { ok: false, error: "Bạn tra cứu quá nhanh. Vui lòng thử lại sau." },
        { status: 429, headers: { "Retry-After": String(rateLimit.retryAfterSeconds) } }
      );
    }

    const supabase = getSupabaseAdminClient();
    const { data: userData, error: userError } = await supabase.auth.getUser(token);
    if (userError || !userData.user) {
      return NextResponse.json({ ok: false, error: "Unauthorized." }, { status: 401 });
    }

    const order = await getDirectOrderByCodeForAuth(code, userData.user.id);
    if (!order) {
      return NextResponse.json({ ok: false, error: "Không tìm thấy đơn hàng." }, { status: 404 });
    }

    return NextResponse.json(
      { ok: true, order },
      {
        headers: {
          "Cache-Control": "no-store"
        }
      }
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
