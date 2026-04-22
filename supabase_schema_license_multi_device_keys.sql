-- Per-key device binding mode for license management

ALTER TABLE public.license_keys
  ADD COLUMN IF NOT EXISTS device_limit_mode TEXT;

UPDATE public.license_keys
SET device_limit_mode = 'single_device'
WHERE COALESCE(TRIM(device_limit_mode), '') = '';

ALTER TABLE public.license_keys
  ALTER COLUMN device_limit_mode SET DEFAULT 'single_device';

ALTER TABLE public.license_keys
  ALTER COLUMN device_limit_mode SET NOT NULL;

ALTER TABLE public.license_keys
  DROP CONSTRAINT IF EXISTS license_keys_device_limit_mode_chk;

ALTER TABLE public.license_keys
  ADD CONSTRAINT license_keys_device_limit_mode_chk
  CHECK (device_limit_mode IN ('single_device', 'unlimited_devices'));

DROP INDEX IF EXISTS public.idx_license_activations_active_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_license_activations_active_key_fingerprint
  ON public.license_activations (license_key_id, fingerprint)
  WHERE deactivated_at IS NULL;

DROP FUNCTION IF EXISTS public.activate_license_key(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT);
CREATE OR REPLACE FUNCTION public.activate_license_key(
  p_extension_code TEXT,
  p_key_hash TEXT,
  p_activation_token_hash TEXT,
  p_fingerprint TEXT,
  p_ip TEXT DEFAULT NULL,
  p_user_agent TEXT DEFAULT NULL,
  p_version TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_extension public.license_extensions%ROWTYPE;
  v_license_key public.license_keys%ROWTYPE;
  v_existing_activation public.license_activations%ROWTYPE;
  v_other_activation public.license_activations%ROWTYPE;
  v_extension_code TEXT := UPPER(TRIM(COALESCE(p_extension_code, '')));
  v_key_hash TEXT := TRIM(COALESCE(p_key_hash, ''));
  v_token_hash TEXT := TRIM(COALESCE(p_activation_token_hash, ''));
  v_fingerprint TEXT := TRIM(COALESCE(p_fingerprint, ''));
  v_effective_status TEXT;
  v_device_limit_mode TEXT;
BEGIN
  IF v_extension_code = '' OR v_key_hash = '' OR v_token_hash = '' OR v_fingerprint = '' THEN
    PERFORM public.log_license_check(NULL, v_extension_code, v_fingerprint, 'activate', 'not_found', p_ip, p_user_agent);
    RETURN jsonb_build_object('valid', FALSE, 'status', 'not_found', 'expires_at', NULL);
  END IF;

  SELECT *
  INTO v_extension
  FROM public.license_extensions
  WHERE code = v_extension_code
  LIMIT 1;

  IF NOT FOUND THEN
    PERFORM public.log_license_check(NULL, v_extension_code, v_fingerprint, 'activate', 'not_found', p_ip, p_user_agent);
    RETURN jsonb_build_object('valid', FALSE, 'status', 'not_found', 'expires_at', NULL);
  END IF;

  IF NOT v_extension.is_active THEN
    PERFORM public.log_license_check(NULL, v_extension_code, v_fingerprint, 'activate', 'extension_disabled', p_ip, p_user_agent);
    RETURN jsonb_build_object('valid', FALSE, 'status', 'extension_disabled', 'expires_at', NULL);
  END IF;

  SELECT *
  INTO v_license_key
  FROM public.license_keys
  WHERE extension_id = v_extension.id
    AND key_hash = v_key_hash
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    PERFORM public.log_license_check(NULL, v_extension_code, v_fingerprint, 'activate', 'not_found', p_ip, p_user_agent);
    RETURN jsonb_build_object('valid', FALSE, 'status', 'not_found', 'expires_at', NULL);
  END IF;

  v_effective_status := public.license_key_effective_status(v_license_key.status, v_license_key.expires_at);
  IF v_effective_status = 'expired' THEN
    PERFORM public.mark_license_key_expired(v_license_key.id);
    PERFORM public.log_license_check(v_license_key.id, v_extension_code, v_fingerprint, 'activate', 'expired', p_ip, p_user_agent);
    RETURN jsonb_build_object('valid', FALSE, 'status', 'expired', 'expires_at', v_license_key.expires_at);
  END IF;

  IF v_effective_status = 'revoked' THEN
    PERFORM public.log_license_check(v_license_key.id, v_extension_code, v_fingerprint, 'activate', 'revoked', p_ip, p_user_agent);
    RETURN jsonb_build_object('valid', FALSE, 'status', 'revoked', 'expires_at', v_license_key.expires_at);
  END IF;

  v_device_limit_mode := COALESCE(NULLIF(TRIM(COALESCE(v_license_key.device_limit_mode, '')), ''), 'single_device');

  SELECT *
  INTO v_existing_activation
  FROM public.license_activations
  WHERE license_key_id = v_license_key.id
    AND fingerprint = v_fingerprint
    AND deactivated_at IS NULL
  ORDER BY activated_at DESC
  LIMIT 1
  FOR UPDATE;

  IF FOUND THEN
    UPDATE public.license_activations
    SET activation_token_hash = v_token_hash,
        last_checked_at = NOW(),
        last_ip = NULLIF(TRIM(COALESCE(p_ip, '')), ''),
        last_user_agent = NULLIF(LEFT(TRIM(COALESCE(p_user_agent, '')), 512), ''),
        last_version = NULLIF(LEFT(TRIM(COALESCE(p_version, '')), 120), '')
    WHERE id = v_existing_activation.id;

    PERFORM public.log_license_check(v_license_key.id, v_extension_code, v_fingerprint, 'activate', 'active', p_ip, p_user_agent);
    RETURN jsonb_build_object('valid', TRUE, 'status', 'active', 'expires_at', v_license_key.expires_at);
  END IF;

  IF v_device_limit_mode <> 'unlimited_devices' THEN
    SELECT *
    INTO v_other_activation
    FROM public.license_activations
    WHERE license_key_id = v_license_key.id
      AND deactivated_at IS NULL
    ORDER BY activated_at DESC
    LIMIT 1
    FOR UPDATE;

    IF FOUND THEN
      PERFORM public.log_license_check(v_license_key.id, v_extension_code, v_fingerprint, 'activate', 'fingerprint_mismatch', p_ip, p_user_agent);
      RETURN jsonb_build_object('valid', FALSE, 'status', 'fingerprint_mismatch', 'expires_at', v_license_key.expires_at);
    END IF;
  END IF;

  BEGIN
    INSERT INTO public.license_activations (
      license_key_id,
      fingerprint,
      activation_token_hash,
      last_ip,
      last_user_agent,
      last_version
    )
    VALUES (
      v_license_key.id,
      v_fingerprint,
      v_token_hash,
      NULLIF(TRIM(COALESCE(p_ip, '')), ''),
      NULLIF(LEFT(TRIM(COALESCE(p_user_agent, '')), 512), ''),
      NULLIF(LEFT(TRIM(COALESCE(p_version, '')), 120), '')
    )
    RETURNING *
    INTO v_existing_activation;
  EXCEPTION
    WHEN unique_violation THEN
      SELECT *
      INTO v_existing_activation
      FROM public.license_activations
      WHERE license_key_id = v_license_key.id
        AND fingerprint = v_fingerprint
        AND deactivated_at IS NULL
      ORDER BY activated_at DESC
      LIMIT 1
      FOR UPDATE;

      IF NOT FOUND THEN
        RAISE;
      END IF;

      UPDATE public.license_activations
      SET activation_token_hash = v_token_hash,
          last_checked_at = NOW(),
          last_ip = NULLIF(TRIM(COALESCE(p_ip, '')), ''),
          last_user_agent = NULLIF(LEFT(TRIM(COALESCE(p_user_agent, '')), 512), ''),
          last_version = NULLIF(LEFT(TRIM(COALESCE(p_version, '')), 120), '')
      WHERE id = v_existing_activation.id;
  END;

  PERFORM public.log_license_check(v_license_key.id, v_extension_code, v_fingerprint, 'activate', 'active', p_ip, p_user_agent);
  RETURN jsonb_build_object('valid', TRUE, 'status', 'active', 'expires_at', v_license_key.expires_at);
END;
$$;

REVOKE ALL ON FUNCTION public.activate_license_key(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.activate_license_key(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT) TO service_role;
