-- Telegram user profile name fields for Bot admin display.
-- New file on purpose: do not append this into older SQL files.

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS first_name TEXT;

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS last_name TEXT;

COMMENT ON COLUMN public.users.first_name IS
  'Latest Telegram first_name captured from user updates for admin display.';

COMMENT ON COLUMN public.users.last_name IS
  'Latest Telegram last_name captured from user updates for admin display.';
