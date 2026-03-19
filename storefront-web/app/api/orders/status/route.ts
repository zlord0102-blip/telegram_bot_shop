import { NextRequest, NextResponse } from "next/server";
import { getDirectOrderByCode } from "@/lib/shop";

export async function GET(request: NextRequest) {
  try {
    const code = request.nextUrl.searchParams.get("code") || "";
    if (!code.trim()) {
      return NextResponse.json({ ok: false, error: "Thiếu mã đơn hàng." }, { status: 400 });
    }

    const order = await getDirectOrderByCode(code);
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
