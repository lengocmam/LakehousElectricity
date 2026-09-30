import re
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from lxml import html

from bronze.bronze_utils import (
    get_dates_to_crawl,
    merge_ingestion_log,
    write_raw_bronze,
)
from bronze.http_client import create_legacy_tls_session
from utils.spark import create_spark_session


# URL trang nhúng bảng hồ chứa thủy điện của EVN.
# Tham số td (thời điểm) được truyền vào dưới dạng dd/MM/yyyy HH:mm.
BASE_URL = "https://hochuathuydien.evn.com.vn/PageHoChuaThuyDienEmbedEVN.aspx"

# Tên nguồn dữ liệu — khớp với convention source_name
SOURCE_NAME = "evn_hydro"

# Dataset bắt đầu từ 01/01/2023
START_DATE_DEFAULT = date(2023, 1, 1)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# Thời điểm cuối ngày dùng để request snapshot của ngày đó.
END_OF_DAY_HOUR = 23
END_OF_DAY_MINUTE = 0

# Ngưỡng số hồ chứa có số liệu mực nước thực tế tối thiểu để công nhận VALID
EXPECTED_RESERVOIRS = 30
RECENT_RECHECK_DAYS = 3

NUMERIC_RE = re.compile(r"\d+(?:[.,]\d+)?")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
}

# Nessie catalog — namespace và tên bảng Bronze Hydro
NAMESPACE = "nessie.bronze"
TABLE_NAME = "nessie.bronze.evn_hydro"


def create_hydro_session() -> requests.Session:
    return create_legacy_tls_session(BASE_URL)


def build_end_of_day_td_param(data_date: date) -> str:
    """
    Tạo chuỗi tham số td theo format website EVN Hydro yêu cầu:
    dd/MM/yyyy HH:mm — đại diện cho thời điểm cuối ngày của data_date.
    """
    return (
        f"{data_date.day:02d}/{data_date.month:02d}/{data_date.year} "
        f"{END_OF_DAY_HOUR:02d}:{END_OF_DAY_MINUTE:02d}"
    )


def validate_hydro_raw(
    raw_content: bytes,
    data_date: str,
    bronze_key: str,
    batch_id: str,
    checked_at: str,
) -> dict:
    """
    Kiểm tra trực tiếp trên raw HTML bảng hồ chứa thủy điện EVN:
    - Dùng lxml.html.fromstring đếm số dòng `<tr>` hồ chứa có số liệu thực tế
      (ít nhất 2 ô số liệu hợp lệ và không bị toàn bộ "KXD").
    - Đánh giá trạng thái:
      * actual_items >= EXPECTED_RESERVOIRS (30) -> VALID
      * 0 < actual_items < EXPECTED_RESERVOIRS -> INCOMPLETE
      * actual_items == 0 -> EMPTY
    """
    tree = html.fromstring(raw_content)
    rows = tree.xpath("//table//tr[td]")

    valid_reservoirs = 0
    kxd_rows = 0

    for tr in rows:
        cells = [
            " ".join(td.itertext()).strip()
            for td in tr.xpath("./td")
        ]
        if len(cells) < 5:
            continue

        # Bỏ qua các dòng tiêu đề / tên lưu vực gom nhóm
        first_cell = cells[0]
        if not first_cell or first_cell.lower().startswith(("tên hồ", "stt", "lưu vực")):
            continue

        metric_cells = cells[1:]
        numeric_count = sum(
            1
            for c in metric_cells
            if c and "KXD" not in c.upper() and NUMERIC_RE.search(c)
        )
        if numeric_count >= 2:
            valid_reservoirs += 1
        elif any("KXD" in c.upper() for c in metric_cells):
            kxd_rows += 1

    if valid_reservoirs >= EXPECTED_RESERVOIRS:
        status = "VALID"
    elif valid_reservoirs > 0:
        status = "INCOMPLETE"
    else:
        status = "EMPTY"

    return {
        "source_name": SOURCE_NAME,
        "data_date": data_date,
        "bronze_key": bronze_key if valid_reservoirs > 0 else None,
        "batch_id": batch_id,
        "status": status,
        "expected_items": EXPECTED_RESERVOIRS,
        "actual_items": valid_reservoirs,
        "attempt_count": 1,
        "note": (
            f"{valid_reservoirs}/{EXPECTED_RESERVOIRS} valid reservoirs parsed "
            f"(KXD rows={kxd_rows}, total tr={len(rows)})"
        ),
        "checked_at": checked_at,
    }


def crawl_hydro_dates(
    session: requests.Session,
    dates_to_crawl: list[date],
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> tuple[list[dict], list[dict], list[date]]:
    """
    Thực hiện HTTP GET để lấy raw HTML snapshot hồ chứa thủy điện
    cho danh sách `dates_to_crawl` và kiểm chứng số lượng hồ chứa thực tế.
    """
    records: list[dict] = []
    log_rows: list[dict] = []
    failed_dates: list[date] = []
    checked_at = ingestion_timestamp.isoformat()

    for index, current_date in enumerate(dates_to_crawl, start=1):
        td_param = build_end_of_day_td_param(current_date)
        source_url = f"{BASE_URL}?td={td_param}"
        data_date_str = current_date.isoformat()

        print(f"Crawling Hydro snapshot for data_date={data_date_str} (URL: {source_url})")

        try:
            response = session.get(
                BASE_URL,
                params={"td": td_param},
                headers=HEADERS,
                timeout=60,
            )
            response.raise_for_status()

            bronze_key = f"{batch_id}_{index}"
            log_entry = validate_hydro_raw(
                raw_content=response.content,
                data_date=data_date_str,
                bronze_key=bronze_key,
                batch_id=batch_id,
                checked_at=checked_at,
            )
            log_rows.append(log_entry)

            print(
                f"  HTTP status: {response.status_code}, Length: {len(response.content)} bytes, "
                f"validation: {log_entry['status']} ({log_entry['actual_items']}/{EXPECTED_RESERVOIRS} reservoirs)"
            )

            if log_entry["actual_items"] > 0:
                records.append({
                    "bronze_key": bronze_key,
                    "source_name": SOURCE_NAME,
                    "source_url": response.url,
                    "source_data_date": data_date_str,
                    "batch_id": batch_id,
                    "ingestion_timestamp": checked_at,
                    "ingest_date": ingest_date,
                    "raw": response.text,
                })

        except Exception as e:
            print(f"Error crawling data_date={data_date_str}: {e}")
            failed_dates.append(current_date)
            log_rows.append({
                "source_name": SOURCE_NAME,
                "data_date": data_date_str,
                "bronze_key": None,
                "batch_id": batch_id,
                "status": "FAILED",
                "expected_items": EXPECTED_RESERVOIRS,
                "actual_items": 0,
                "attempt_count": 1,
                "note": f"Error: {str(e)[:200]}",
                "checked_at": checked_at,
            })

        time.sleep(0.1)

    return records, log_rows, failed_dates


def write_bronze(
    spark,
    records: list[dict],
    ingest_date: str,
) -> None:
    write_raw_bronze(
        spark,
        records,
        TABLE_NAME,
        ingest_date,
        namespace=NAMESPACE,
    )


def main() -> None:
    ingestion_timestamp = datetime.now(timezone.utc)
    ingest_date = ingestion_timestamp.date().isoformat()
    batch_id = ingestion_timestamp.strftime("%Y%m%d%H%M%S")

    print(f"batch_id: {batch_id}")
    print(f"ingest_date: {ingest_date}")

    spark = None
    try:
        spark = create_spark_session("ingest_evn_hydro")

        start_date = START_DATE_DEFAULT
        end_date = datetime.now(VN_TZ).date() - timedelta(days=1)

        print(f"Target dataset date range: {start_date.isoformat()} to {end_date.isoformat()}")

        if start_date > end_date:
            print("No new dates to crawl. Dataset is up to date.")
            return

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

        session = create_hydro_session()

        records, log_rows, failed_dates = crawl_hydro_dates(
            session=session,
            dates_to_crawl=dates_to_crawl,
            ingestion_timestamp=ingestion_timestamp,
            ingest_date=ingest_date,
            batch_id=batch_id,
        )

        if records:
            write_bronze(
                spark=spark,
                records=records,
                ingest_date=ingest_date,
            )

        if log_rows:
            merge_ingestion_log(
                spark=spark,
                log_rows=log_rows,
            )

        if failed_dates:
            print(f"Warning: {len(failed_dates)} date(s) failed and were logged as FAILED.")

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
