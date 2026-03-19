import { NextRequest, NextResponse } from "next/server";
import { getProductsWithStock } from "@/lib/shop";

export async function GET(request: NextRequest) {
  try {
    const search = (request.nextUrl.searchParams.get("q") || "").trim().toLowerCase();
    const page = Math.max(1, Number.parseInt(request.nextUrl.searchParams.get("page") || "1", 10) || 1);
    const pageSize = Math.min(50, Math.max(1, Number.parseInt(request.nextUrl.searchParams.get("pageSize") || "10", 10) || 10));
    const inStockOnly = (request.nextUrl.searchParams.get("inStockOnly") || "false").toLowerCase() === "true";

    let products = await getProductsWithStock();
    if (inStockOnly) {
      products = products.filter((product) => product.stock > 0);
    }
    if (search) {
      products = products.filter((product) => {
        return (
          product.name.toLowerCase().includes(search) ||
          (product.description || "").toLowerCase().includes(search)
        );
      });
    }

    const total = products.length;
    const from = (page - 1) * pageSize;
    const paged = products.slice(from, from + pageSize);

    return NextResponse.json(
      {
        ok: true,
        products: paged,
        pagination: {
          page,
          pageSize,
          total
        }
      },
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
