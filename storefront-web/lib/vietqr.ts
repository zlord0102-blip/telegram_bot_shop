const BANK_CODES: Record<string, string> = {
  VietinBank: "970415",
  Vietcombank: "970436",
  BIDV: "970418",
  Agribank: "970405",
  MBBank: "970422",
  MB: "970422",
  Techcombank: "970407",
  ACB: "970416",
  VPBank: "970432",
  TPBank: "970423",
  Sacombank: "970403",
  HDBank: "970437",
  VIB: "970441",
  SHB: "970443",
  Eximbank: "970431",
  MSB: "970426",
  OCB: "970448",
  LienVietPostBank: "970449",
  SeABank: "970440",
  NamABank: "970428",
  PVcomBank: "970412",
  BacABank: "970409",
  VietABank: "970427",
  ABBank: "970425",
  BaoVietBank: "970438",
  NCB: "970419",
  Kienlongbank: "970452",
  VietBank: "970433"
};

export function resolveBankCode(bankName: string) {
  if (!bankName) return "970415";
  return BANK_CODES[bankName] || BANK_CODES[bankName.trim()] || "970415";
}

export function generateVietQrUrl(params: {
  bankName: string;
  accountNumber: string;
  accountName: string;
  amount: number;
  content: string;
}) {
  const bankCode = resolveBankCode(params.bankName);
  const accountNumber = encodeURIComponent(params.accountNumber.trim());
  const accountName = encodeURIComponent(params.accountName.trim());
  const amount = Math.max(0, Math.trunc(Number(params.amount) || 0));
  const content = encodeURIComponent(params.content.trim());

  return `https://img.vietqr.io/image/${bankCode}-${accountNumber}-compact2.png?amount=${amount}&addInfo=${content}&accountName=${accountName}`;
}
