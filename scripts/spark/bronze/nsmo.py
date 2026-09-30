import sys
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import urllib3

from bronze.bronze_utils import write_raw_bronze
from utils.spark import create_spark_session

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://www.nsmo.vn"
API_PATH = "/api/services/app/Pages/GetChartPhuTaiVM"
SOURCE_NAME = "nsmo"
NAMESPACE = "nessie.bronze"
TABLE_NAME = "nessie.bronze.nsmo"

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
START_DATE_DEFAULT = date(2023, 1, 1)

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


def crawl_nsmo_dates(
    session: requests.Session,
    start_date: date,
    end_date: date,
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> tuple[list[dict], list[date]]:
    records = []
    failed_dates = []
    current_date = start_date
    index = 1

    while current_date <= end_date:
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

                # Kiểm tra lại sau khi retry
                content_type = response.headers.get("content-type", "")

                if "application/json" not in content_type.lower():
                    raise RuntimeError(
                        f"Expected JSON but received {content_type}. "
                        f"URL: {response.url}"
                    )

            records.append({
                "bronze_key": f"{batch_id}_{index}",
                "source_name": SOURCE_NAME,
                "source_url": response.url,
                "source_data_date": source_data_date,
                "batch_id": batch_id,
                "ingestion_timestamp": ingestion_timestamp.isoformat(),
                "ingest_date": ingest_date,
                "raw": response.text,
            })
            index += 1
            
            print(f"  -> HTTP 200, length: {len(response.content)} bytes")

        except (requests.RequestException, RuntimeError) as e:
            print(
                f"  -> [ERROR] Failed to fetch data for "
                f"data_date={source_data_date}: {e}"
            )
            failed_dates.append(current_date)
        
        current_date += timedelta(days=1)
        time.sleep(0.1)

    return records, failed_dates


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

    # Mặc định crawl toàn bộ từ 2023-01-01 đến ngày hiện tại (không dùng incremental watermark)
    start_date = START_DATE_DEFAULT
    end_date = datetime.now(VN_TZ).date()

    print(f"Dataset date range: {start_date.isoformat()} to {end_date.isoformat()}")

    if start_date > end_date:
        print("No new dates to crawl.")
        return

    session = create_nsmo_session()
    records, failed_dates = crawl_nsmo_dates(
        session=session,
        start_date=start_date,
        end_date=end_date,
        ingestion_timestamp=ingestion_timestamp,
        ingest_date=ingest_date,
        batch_id=batch_id,
    )

    if failed_dates:
        print("\n=== SUMMARY OF FAILED DATES ===")
        for fd in failed_dates:
            print(f"- {fd.isoformat()}")
        print("===============================\n")

    spark = None
    try:
        spark = create_spark_session("ingest_nsmo")
        write_bronze(spark, records, ingest_date)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
