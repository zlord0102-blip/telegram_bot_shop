-- Website Product channel split:
-- Only stock is shared with Telegram/Bot side.
-- Website merchandising fields are managed independently via website_* columns.

ALTER TABLE public.products
  ADD COLUMN IF NOT EXISTS website_sort_position INTEGER;

ALTER TABLE public.products
  ADD COLUMN IF NOT EXISTS website_format_data TEXT;

ALTER TABLE public.products
  ADD COLUMN IF NOT EXISTS website_deleted BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE public.products
SET
  website_enabled = COALESCE(website_enabled, TRUE),
  website_deleted = COALESCE(website_deleted, FALSE);

-- One-time backfill so Website starts from existing values but can diverge later.
UPDATE public.products
SET website_name = COALESCE(NULLIF(TRIM(website_name), ''), name)
WHERE COALESCE(NULLIF(TRIM(website_name), ''), '') = '';

UPDATE public.products
SET website_price = COALESCE(website_price, price)
WHERE website_price IS NULL;

UPDATE public.products
SET website_description = COALESCE(NULLIF(TRIM(website_description), ''), description)
WHERE COALESCE(NULLIF(TRIM(website_description), ''), '') = ''
  AND COALESCE(NULLIF(TRIM(description), ''), '') <> '';

UPDATE public.products
SET website_format_data = COALESCE(NULLIF(TRIM(website_format_data), ''), format_data)
WHERE COALESCE(NULLIF(TRIM(website_format_data), ''), '') = ''
  AND COALESCE(NULLIF(TRIM(format_data), ''), '') <> '';

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'products'
      AND column_name = 'sort_position'
  ) THEN
    UPDATE public.products
    SET website_sort_position = COALESCE(website_sort_position, sort_position)
    WHERE website_sort_position IS NULL;
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_products_website_channel_visibility
  ON public.products (website_deleted, website_enabled, website_sort_position, id);

COMMENT ON COLUMN public.products.website_sort_position IS
  'Website-only product order position. Smaller values are shown first.';

COMMENT ON COLUMN public.products.website_format_data IS
  'Website-only stock output format template (independent from bot format_data).';

COMMENT ON COLUMN public.products.website_deleted IS
  'Website-only soft delete flag. Does not affect bot-side product visibility.';
