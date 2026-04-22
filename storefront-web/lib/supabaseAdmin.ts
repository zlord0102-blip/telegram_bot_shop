import { createClient } from "@supabase/supabase-js";

let cachedClient: any = null;

export function getSupabaseAdminClient(): any {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL || "";
  const secretKey = process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY || "";

  if (!supabaseUrl || !secretKey) {
    throw new Error("Missing NEXT_PUBLIC_SUPABASE_URL/SUPABASE_URL or SUPABASE_SECRET_KEY/SUPABASE_SERVICE_ROLE_KEY");
  }

  if (!cachedClient) {
    cachedClient = createClient(supabaseUrl, secretKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false
      }
    });
  }

  return cachedClient;
}
