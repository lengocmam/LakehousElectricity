"""
LLM-as-a-Judge & Deterministic Evidence Verifier (Chuẩn SalesNow).
Kiểm chứng độc lập các số liệu vận hành trích xuất từ văn bản bán cấu trúc (EVN News)
dựa trên 3 tiêu chí do con người đặt ra (Human-Defined Criteria):
1. Literal Substring Evidence: Đoạn trích dẫn bằng chứng (`evidence_quote`) phải xuất hiện
   nguyên văn trong văn bản gốc (`raw_text`).
2. Physical Domain Sanity: Giá trị công suất đỉnh (MW) và sản lượng điện ngày (triệu kWh)
   phải nằm trong ngưỡng vật lý hợp lý của Hệ thống điện Quốc gia Việt Nam.
3. Optional LLM Judge (Gemini / Claude): Khi có cấu hình GEMINI_API_KEY, gọi LLM-as-a-Judge
   để đối chiếu chéo con số với ngữ cảnh câu văn. Nếu bằng chứng không đủ rõ ràng,
   bắt buộc đánh dấu `UNVERIFIABLE` ("không thể xác định").
"""

import json
import os
import re
import requests


MIN_NATIONAL_PEAK_MW = 10000.0
MAX_NATIONAL_PEAK_MW = 65000.0
MIN_DAILY_ENERGY_MKWH = 200.0
MAX_DAILY_ENERGY_MKWH = 1500.0


def extract_sentence_around_match(text: str, start_idx: int, end_idx: int, window: int = 120) -> str:
    left = max(0, start_idx - window)
    right = min(len(text), end_idx + window)
    return re.sub(r"\s+", " ", text[left:right]).strip()


def verify_extraction_with_evidence(
    raw_text: str,
    peak_mw: float | None,
    daily_kwh: float | None,
    evidence_quote: str | None,
) -> tuple[str, str]:
    """
    Trả về `(evidence_status, audit_reason)`:
    - `evidence_status` nhận một trong hai giá trị: `'VERIFIED'` hoặc `'UNVERIFIABLE'`.
    """
    if not evidence_quote or evidence_quote not in raw_text:
        return (
            "UNVERIFIABLE",
            "Missing or non-verbatim evidence quote in article body",
        )

    if peak_mw is None and daily_kwh is None:
        return (
            "UNVERIFIABLE",
            "Neither peak_capacity_mw nor daily_energy_million_kwh could be extracted",
        )

    if peak_mw is not None and not (MIN_NATIONAL_PEAK_MW <= peak_mw <= MAX_NATIONAL_PEAK_MW):
        return (
            "UNVERIFIABLE",
            f"peak_capacity_mw={peak_mw} out of physical bounds [{MIN_NATIONAL_PEAK_MW}, {MAX_NATIONAL_PEAK_MW}]",
        )

    if daily_kwh is not None and not (MIN_DAILY_ENERGY_MKWH <= daily_kwh <= MAX_DAILY_ENERGY_MKWH):
        return (
            "UNVERIFIABLE",
            f"daily_energy_million_kwh={daily_kwh} out of physical bounds [{MIN_DAILY_ENERGY_MKWH}, {MAX_DAILY_ENERGY_MKWH}]",
        )

    gemini_key = os.getenv("GEMINI_API_KEY")
    if gemini_key:
        try:
            prompt = (
                "You are a strict Data Quality Judge. Verify whether the extracted numbers "
                f"(peak_capacity_mw={peak_mw}, daily_energy_million_kwh={daily_kwh}) are "
                f"directly supported by this exact quote from the Vietnam Electricity report:\n"
                f"QUOTE: \"{evidence_quote}\"\n"
                "Reply ONLY with JSON: {\"verified\": true/false, \"reason\": \"...\"}"
            )
            resp = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={gemini_key}",
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=15,
            )
            if resp.ok:
                candidates = resp.json().get("candidates") or []
                if candidates:
                    content_text = candidates[0]["content"]["parts"][0]["text"]
                    match = re.search(r"\{.*\}", content_text, re.DOTALL)
                    if match:
                        verdict = json.loads(match.group(0))
                        if not verdict.get("verified", False):
                            return (
                                "UNVERIFIABLE",
                                f"LLM Judge rejected: {verdict.get('reason', 'Insufficient evidence')}",
                            )
        except Exception:
            # Nếu LLM API timeout, giữ kết quả kiểm tra bằng chứng vật lý & nguyên văn
            pass

    return (
        "VERIFIED",
        f"Verified with verbatim quote: '{evidence_quote[:100]}'",
    )
