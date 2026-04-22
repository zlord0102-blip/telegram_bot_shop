# License API Integration

## 1. `Code công khai` là gì?

`Code công khai` chính là `extensionCode`.

- Đây là mã định danh public của từng extension.
- Mã này không phải secret.
- Mỗi extension chỉ nên có 1 mã riêng, ví dụ:
  - `EMAIL_INBOX`
  - `WP_PLUGIN_PRO`
  - `CHROME_TOOL_A`
- Extension của bạn phải hard-code đúng mã này khi gọi API license.
- Một license key chỉ hợp lệ với đúng `extensionCode` đã được gán khi tạo key.

Quy tắc hiện tại:

- hệ thống tự normalize thành uppercase
- khoảng trắng sẽ bị đổi thành `-`
- chỉ giữ `A-Z`, `0-9`, `_`, `-`
- tối đa 48 ký tự
- sau khi tạo extension thì code này không đổi được

Ví dụ:

- Bạn tạo extension với code `EMAIL_INBOX`
- Các key sinh ra cho extension này chỉ dùng được với `EMAIL_INBOX`
- Nếu extension khác gửi `SEO_TOOL`, API sẽ trả invalid

## 2. Luồng tích hợp chuẩn

Luồng v1:

1. User nhập raw license key trong extension của bạn.
2. Extension gọi `POST /api/licenses/activate`.
3. Nếu hợp lệ, server trả `activationToken`.
4. Extension lưu `activationToken` local.
5. Sau đó extension không gửi raw key nữa.
6. Mỗi 6 giờ, extension gọi `POST /api/licenses/validate` bằng `activationToken`.

Lưu ý:

- Mặc định mỗi license key hoạt động theo kiểu `1 thiết bị`.
- Nếu admin bật chế độ `Không giới hạn thiết bị` cho key đó, cùng một raw key có thể activate trên nhiều fingerprint khác nhau.
- Mỗi fingerprint / cài đặt vẫn nhận `activationToken` riêng và phải tự lưu token của chính nó.

## 3. Base URL

Thay `YOUR_DOMAIN` bằng domain của `admin-dashboard`.

Ví dụ:

- `https://YOUR_DOMAIN/api/licenses/activate`
- `https://YOUR_DOMAIN/api/licenses/validate`

## 4. `fingerprint` nên là gì?

`fingerprint` là chuỗi ổn định đại diện cho 1 máy / 1 site / 1 cài đặt.

Ví dụ:

- Chrome extension: hash từ `chrome.runtime.id + browser profile marker + machine marker`
- WordPress plugin: hash từ `site_url + wp_salt`
- Desktop app: hash từ `machine id + app id`

Yêu cầu:

- ổn định theo cùng một cài đặt
- không quá 255 ký tự
- không thay đổi liên tục giữa các lần chạy

Nếu key đang ở chế độ `1 thiết bị` và đã bind với fingerprint A, mà extension khác gửi fingerprint B, API sẽ trả `fingerprint_mismatch`.

Nếu key được admin đặt là `Không giới hạn thiết bị` thì:

- fingerprint A, B, C... đều có thể activate độc lập
- mỗi fingerprint sẽ có `activationToken` riêng
- `validate` vẫn phải dùng đúng `activationToken` + đúng `fingerprint` đã bind với token đó

## 5. API Activate

### Request

`POST /api/licenses/activate`

```json
{
  "extensionCode": "EMAIL_INBOX",
  "licenseKey": "LIC-EMAIL-INBOX-ABCDE-FGHIJ-KLMNP-QRSTU",
  "fingerprint": "site:example.com|install:abc123",
  "version": "1.0.0"
}
```

### Success response

```json
{
  "success": true,
  "data": {
    "valid": true,
    "status": "active",
    "expiresAt": "2026-12-31T00:00:00.000Z",
    "nextCheckAfterSeconds": 21600,
    "activationToken": "act_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  }
}
```

### Invalid response example

```json
{
  "success": true,
  "data": {
    "valid": false,
    "status": "fingerprint_mismatch",
    "expiresAt": "2026-12-31T00:00:00.000Z",
    "nextCheckAfterSeconds": 21600
  }
}
```

### Service unavailable response

Nếu API trả HTTP `503` với `code="license_service_unavailable"`, extension không được báo `License Key không hợp lệ`.
Đây là lỗi hạ tầng/server license tạm thời, ví dụ Supabase key hết hiệu lực hoặc RPC/database chưa sẵn sàng.

```json
{
  "success": false,
  "code": "license_service_unavailable",
  "error": "Dịch vụ license tạm thời không khả dụng. Vui lòng thử lại sau."
}
```

## 6. API Validate

### Request

`POST /api/licenses/validate`

```json
{
  "extensionCode": "EMAIL_INBOX",
  "activationToken": "act_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "fingerprint": "site:example.com|install:abc123",
  "version": "1.0.1"
}
```

### Success response

```json
{
  "success": true,
  "data": {
    "valid": true,
    "status": "active",
    "expiresAt": "2026-12-31T00:00:00.000Z",
    "nextCheckAfterSeconds": 21600
  }
}
```

## 7. Ý nghĩa các trạng thái

- `active`: key/token hợp lệ
- `expired`: key đã hết hạn
- `revoked`: key bị admin thu hồi
- `extension_disabled`: extension này đã bị admin tắt
- `fingerprint_mismatch`: key đang ở chế độ `1 thiết bị` và đã bind với fingerprint khác
- `not_found`: không tìm thấy key/token hợp lệ

## 8. Cách extension nên xử lý

### Lần đầu user nhập key

- Gọi `activate`
- Nếu `valid=true`, lưu:
  - `activationToken`
  - `expiresAt`
  - `lastValidatedAt`
- Xóa raw key khỏi bộ nhớ nếu không cần giữ lại

### Mỗi lần app khởi động

- Nếu chưa có `activationToken`: yêu cầu user nhập key
- Nếu đã có `activationToken` nhưng đã quá 6 giờ kể từ lần check gần nhất: gọi `validate`
- Nếu chưa quá 6 giờ: có thể cho chạy tạm bằng cache local

### Khi validate fail

- `expired`: khóa tính năng premium và báo gia hạn
- `revoked`: khóa tính năng premium và báo license bị thu hồi
- `extension_disabled`: khóa tính năng và báo extension đang bị vô hiệu hóa
- `fingerprint_mismatch`: báo key đang được dùng ở nơi khác
- `not_found`: coi như token không còn hợp lệ, yêu cầu nhập key lại

## 9. Pseudocode tham khảo

```ts
const API_BASE = "https://YOUR_DOMAIN/api/licenses";
const EXTENSION_CODE = "EMAIL_INBOX";

async function activateLicense(rawKey: string, fingerprint: string) {
  const res = await fetch(`${API_BASE}/activate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      extensionCode: EXTENSION_CODE,
      licenseKey: rawKey,
      fingerprint,
      version: getExtensionVersion()
    })
  });

  const json = await res.json();
  if (!res.ok || !json?.success) {
    if (json?.code === "license_service_unavailable" || res.status >= 500) {
      throw new Error("License server unavailable. Please retry later.");
    }
    throw new Error(json?.error || "Activate failed");
  }

  if (json.data.valid) {
    saveLocal({
      activationToken: json.data.activationToken,
      expiresAt: json.data.expiresAt,
      lastValidatedAt: Date.now()
    });
  }

  return json.data;
}

async function validateLicense(activationToken: string, fingerprint: string) {
  const res = await fetch(`${API_BASE}/validate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      extensionCode: EXTENSION_CODE,
      activationToken,
      fingerprint,
      version: getExtensionVersion()
    })
  });

  const json = await res.json();
  if (!res.ok || !json?.success) {
    if (json?.code === "license_service_unavailable" || res.status >= 500) {
      throw new Error("License server unavailable. Please retry later.");
    }
    throw new Error(json?.error || "Validate failed");
  }

  if (json.data.valid) {
    saveLocal({ lastValidatedAt: Date.now(), expiresAt: json.data.expiresAt });
  }

  return json.data;
}
```

## 10. Checklist vận hành

Trên dashboard:

1. Tạo extension với `Code công khai`, ví dụ `EMAIL_INBOX`
2. Tạo license key cho extension đó
3. Nếu cần, chỉnh key đó sang chế độ `Không giới hạn thiết bị`
4. Gửi raw key cho khách

Trong extension:

1. Hard-code `extensionCode`
2. Tạo fingerprint ổn định
3. Gọi `activate` khi user nhập key
4. Lưu `activationToken`
5. Gọi `validate` định kỳ mỗi 6 giờ

## 11. Lưu ý bảo mật

- Không lưu plain raw license key trong database của bạn nếu không cần.
- Sau khi activate thành công, chỉ dùng `activationToken`.
- `extensionCode` là public, nhưng `SUPABASE_SECRET_KEY` hoặc legacy `SUPABASE_SERVICE_ROLE_KEY` phải chỉ nằm ở server.
- Public API route đã chạy server-side; extension chỉ gọi HTTP tới dashboard domain của bạn.
