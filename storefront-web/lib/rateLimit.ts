import type { NextRequest } from "next/server";

type RateLimitOptions = {
  windowMs: number;
  max: number;
};

type RateLimitBucket = {
  count: number;
  resetAt: number;
};

type RateLimitResult = {
  limited: boolean;
  retryAfterSeconds: number;
  resetAt: number;
  remaining: number;
};

const buckets = new Map<string, RateLimitBucket>();

export function getClientIp(request: NextRequest) {
  const forwardedFor = request.headers.get("x-forwarded-for") || "";
  const firstForwarded = forwardedFor
    .split(",")
    .map((value) => value.trim())
    .find(Boolean);
  return firstForwarded || request.headers.get("x-real-ip") || "unknown";
}

export function checkRateLimit(key: string, options: RateLimitOptions): RateLimitResult {
  const now = Date.now();
  const windowMs = Math.max(1_000, Math.trunc(options.windowMs));
  const max = Math.max(1, Math.trunc(options.max));
  const current = buckets.get(key);

  if (!current || current.resetAt <= now) {
    const resetAt = now + windowMs;
    buckets.set(key, { count: 1, resetAt });
    return {
      limited: false,
      retryAfterSeconds: 0,
      resetAt,
      remaining: Math.max(0, max - 1)
    };
  }

  current.count += 1;
  const retryAfterSeconds = Math.max(1, Math.ceil((current.resetAt - now) / 1000));
  const limited = current.count > max;

  return {
    limited,
    retryAfterSeconds: limited ? retryAfterSeconds : 0,
    resetAt: current.resetAt,
    remaining: Math.max(0, max - current.count)
  };
}
