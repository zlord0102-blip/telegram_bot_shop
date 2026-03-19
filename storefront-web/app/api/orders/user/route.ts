import { NextRequest, NextResponse } from "next/server";
import { getUserOrdersSummary } from "@/lib/shop";

export async function GET(request: NextRequest) {
  try {
    const rawUserId = request.nextUrl.searchParams.get("userId") || "";
    const userId = Math.trunc(Number(rawUserId));

    if (!Number.isFinite(userId) || userId <= 0) {
      return NextResponse.json({ ok: false, error: "Telegram ID không hợp lệ." }, { status: 400 });
    }

    const summary = await getUserOrdersSummary(userId);
    return NextResponse.json(
      { ok: true, data: summary },
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
