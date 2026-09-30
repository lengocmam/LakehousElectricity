import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import urllib3

from bronze.bronze_utils import (
    get_dates_to_crawl,
    merge_ingestion_log,
    write_raw_bronze,
)
from utils.spark import create_spark_session

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://www.nsmo.vn"
API_PATH = "/api/services/app/Pages/GetChartPhuTaiVM"
SOURCE_NAME = "nsmo"
NAMESPACE = "nessie.bronze"
TABLE_NAME = "nessie.bronze.nsmo"

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
START_DATE_DEFAULT = date(2023, 1, 1)
EXPECTED_ITEMS_PER_DAY = 24
RECENT_RECHECK_DAYS = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": f"{BASE_URL}/HeThongDien",
    "X-Requested-With": "XMLHttpRequest",
}


def create_nsmo_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    # Warm-up request để khởi tạo cookie session như trình duyệt
    session.get(f"{BASE_URL}/HeThongDien", verify=False, timeout=30)
    return session


def _count_valid_load_points(node) -> int:
    """
    Đệ quy đếm số điểm đo phụ tải có giá trị công suất hợp lệ (!= None và > 0)
    bên trong cấu trúc JSON `result` của API GetChartPhuTaiVM.
    """
    if isinstance(node, list):
        if not node:
            return 0
        # Nếu là mảng các số trực tiếp (ví dụ [35120.5, 34800.2, ...])
        if all(isinstance(x, (int, float)) or x is None for x in node):
            return sum(1 for x in node if isinstance(x, (int, float)) and x > 0)
        # Nếu là mảng các object điểm đo (ví dụ [{"hour": 1, "value": 35120.5}, ...])
        if all(isinstance(x, dict) for x in node):
            valid_objs = 0
            for item in node:
                numeric_vals = [
                    v
                    for k, v in item.items()
                    if isinstance(v, (int, float))
                    and not isinstance(v, bool)
                    and k.lower() not in {"id", "hour", "index", "stt", "order"}
                    and v > 0
                ]
                if numeric_vals:
                    valid_objs += 1
            if valid_objs > 0:
                return valid_objs
        return max((_count_valid_load_points(child) for child in node), default=0)

    if isinstance(node, dict):
        # Ưu tiên các trường chứa chuỗi dữ liệu biểu đồ
        for key in ("data", "items", "series", "chartData", "values", "phuTai", "listData"):
            if key in node:
                cnt = _count_valid_load_points(node[key])
                if cnt > 0:
                    return cnt
        return max((_count_valid_load_points(v) for v in node.values()), default=0)

    return 0


def validate_nsmo_raw(
    raw_text: str,
    data_date: str,
    bronze_key: str,
    batch_id: str,
    checked_at: str,
) -> dict:
    """
    Kiểm tra trực tiếp trên raw JSON của NSMO:
    - Parse JSON, lấy đối tượng `result` (hoặc gốc).
    - Đếm số điểm đo phụ tải hợp lệ (!= None và > 0) trong ngày.
    - Đánh giá trạng thái:
      * actual_items >= EXPECTED_ITEMS_PER_DAY (24) -> VALID
      * 0 < actual_items < EXPECTED_ITEMS_PER_DAY -> INCOMPLETE
      * actual_items == 0 -> EMPTY
    """
    payload = json.loads(raw_text)
    target = payload.get("result", payload) if isinstance(payload, dict) else payload
    actual_items = _count_valid_load_points(target)

    if actual_items >= EXPECTED_ITEMS_PER_DAY:
        status = "VALID"
    elif actual_items > 0:
        status = "INCOMPLETE"
    else:
        status = "EMPTY"

    return {
        "source_name": SOURCE_NAME,
        "data_date": data_date,
        "bronze_key": bronze_key if actual_items > 0 else None,
        "batch_id": batch_id,
        "status": status,
        "expected_items": EXPECTED_ITEMS_PER_DAY,
        "actual_items": actual_items,
        "attempt_count": 1,
        "note": f"{actual_items}/{EXPECTED_ITEMS_PER_DAY} valid load data points in JSON result",
        "checked_at": checked_at,
    }


def crawl_nsmo_dates(
    session: requests.Session,
    dates_to_crawl: list[date],
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> tuple[list[dict], list[dict], list[date]]:
    records: list[dict] = []
    log_rows: list[dict] = []
    failed_dates: list[date] = []
    checked_at = ingestion_timestamp.isoformat()

    for index, current_date in enumerate(dates_to_crawl, start=1):
        # API của NSMO yêu cầu format dd/MM/yyyy
        day_str_api = current_date.strftime("%d/%m/%Y")
        # Metadata Lakehouse yêu cầu format YYYY-MM-DD
        source_data_date = current_date.isoformat()

        print(f"Crawling NSMO snapshot for data_date={source_data_date} (API param: {day_str_api})")

        try:
            response = session.get(
                f"{BASE_URL}{API_PATH}",
                params={"day": day_str_api},
                verify=False,
                timeout=30,
            )
            response.raise_for_status()

            # Xử lý trường hợp session cookie hết hạn giữa chừng (trả về HTML/Redirect thay vì JSON)
            content_type = response.headers.get("content-type", "")

            if "application/json" not in content_type.lower():
                print("  -> Response is not JSON. Re-warming session and retrying...")

                session.get(
                    f"{BASE_URL}/HeThongDien",
                    verify=False,
                    timeout=30,
                )

                response = session.get(
                    f"{BASE_URL}{API_PATH}",
                    params={"day": day_str_api},
                    verify=False,
                    timeout=30,
                )
                response.raise_for_status()

                content_type = response.headers.get("content-type", "")

                if "application/json" not in content_type.lower():
                    raise RuntimeError(
                        f"Expected JSON but received {content_type}. "
                        f"URL: {response.url}"
                    )

            bronze_key = f"{batch_id}_{index}"
            log_entry = validate_nsmo_raw(
                raw_text=response.text,
                data_date=source_data_date,
                bronze_key=bronze_key,
                batch_id=batch_id,
                checked_at=checked_at,
            )
            log_rows.append(log_entry)

            print(
                f"  -> HTTP 200, length: {len(response.content)} bytes, "
                f"validation: {log_entry['status']} ({log_entry['actual_items']}/{EXPECTED_ITEMS_PER_DAY})"
            )

            if log_entry["actual_items"] > 0:
                records.append({
                    "bronze_key": bronze_key,
                    "source_name": SOURCE_NAME,
                    "source_url": response.url,
                    "source_data_date": source_data_date,
                    "batch_id": batch_id,
                    "ingestion_timestamp": checked_at,
                    "ingest_date": ingest_date,
                    "raw": response.text,
                })

        except (requests.RequestException, RuntimeError, ValueError) as e:
            print(
                f"  -> [ERROR] Failed to fetch data for "
                f"data_date={source_data_date}: {e}"
            )
            failed_dates.append(current_date)
            log_rows.append({
                "source_name": SOURCE_NAME,
                "data_date": source_data_date,
                "bronze_key": None,
                "batch_id": batch_id,
                "status": "FAILED",
                "expected_items": EXPECTED_ITEMS_PER_DAY,
                "actual_items": 0,
                "attempt_count": 1,
                "note": f"Error: {str(e)[:200]}",
                "checked_at": checked_at,
            })

        time.sleep(0.1)

    return records, log_rows, failed_dates


def write_bronze(spark, records: list[dict], ingest_date: str) -> None:
    write_raw_bronze(
        spark,
        records,
        TABLE_NAME,
        ingest_date,
        namespace=NAMESPACE,
    )


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    ingestion_timestamp = datetime.now(timezone.utc)
    ingest_date = ingestion_timestamp.date().isoformat()
    batch_id = ingestion_timestamp.strftime("%Y%m%d%H%M%S")

    print(f"batch_id: {batch_id}")
    print(f"ingest_date: {ingest_date}")

    start_date = START_DATE_DEFAULT
    end_date = datetime.now(VN_TZ).date()

    print(f"Target dataset date range: {start_date.isoformat()} to {end_date.isoformat()}")

    if start_date > end_date:
        print("No new dates to crawl.")
        return

    spark = None
    try:
        spark = create_spark_session("ingest_nsmo")

        dates_to_crawl = get_dates_to_crawl(
            spark=spark,
            source_name=SOURCE_NAME,
            start_date=start_date,
            end_date=end_date,
            recent_days=RECENT_RECHECK_DAYS,
        )

        if not dates_to_crawl:
            print("All dates in range are already VALID or EXHAUSTED. Nothing to crawl.")
            return

        print(f"Dates needing crawl/re-check: {len(dates_to_crawl)} day(s)")

        session = create_nsmo_session()
        records, log_rows, failed_dates = crawl_nsmo_dates(
            session=session,
            dates_to_crawl=dates_to_crawl,
            ingestion_timestamp=ingestion_timestamp,
            ingest_date=ingest_date,
            batch_id=batch_id,
        )

        if failed_dates:
            print("\n=== SUMMARY OF FAILED DATES ===")
            for fd in failed_dates:
                print(f"- {fd.isoformat()}")
            print("===============================\n")

        if records:
            write_bronze(spark, records, ingest_date)

        if log_rows:
            merge_ingestion_log(spark=spark, log_rows=log_rows)

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
