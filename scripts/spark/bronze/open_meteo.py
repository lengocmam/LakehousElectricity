import json
import random
import sys
import time
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import requests

from bronze.bronze_utils import (
    get_dates_to_crawl,
    merge_ingestion_log,
    write_raw_bronze,
)
from utils.spark import create_spark_session


BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

SOURCE_NAME = "open-meteo"
NAMESPACE = "nessie.bronze"
TABLE_NAME = "nessie.bronze.open_meteo"
OPEN_METEO_DATE_COLUMNS = (
    "source_data_start_date",
    "source_data_end_date",
)

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

START_DATE_DEFAULT = date(2023, 5, 28)
REQUEST_WINDOW_DAYS = 14
RECENT_RECHECK_DAYS = 7
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2
DEFAULT_429_WAIT_SECONDS = 60

LOCATIONS = [
    {"location_name": "Ha Noi", "latitude": 21.0278, "longitude": 105.8342},
    {"location_name": "Cao Bang", "latitude": 22.6666, "longitude": 106.2639},
    {"location_name": "Tuyen Quang", "latitude": 21.8233, "longitude": 105.2140},
    {"location_name": "Dien Bien", "latitude": 21.3860, "longitude": 103.0230},
    {"location_name": "Lai Chau", "latitude": 22.3964, "longitude": 103.4582},
    {"location_name": "Son La", "latitude": 21.3256, "longitude": 103.9188},
    {"location_name": "Lao Cai", "latitude": 21.7168, "longitude": 104.8986},
    {"location_name": "Thai Nguyen", "latitude": 21.5944, "longitude": 105.8482},
    {"location_name": "Lang Son", "latitude": 21.8537, "longitude": 106.7610},
    {"location_name": "Quang Ninh", "latitude": 21.0064, "longitude": 107.2925},
    {"location_name": "Bac Ninh", "latitude": 21.2731, "longitude": 106.1946},
    {"location_name": "Phu Tho", "latitude": 21.3227, "longitude": 105.4020},
    {"location_name": "Hung Yen", "latitude": 20.6464, "longitude": 106.0511},
    {"location_name": "Hai Phong", "latitude": 20.8449, "longitude": 106.6881},
    {"location_name": "Ninh Binh", "latitude": 20.2506, "longitude": 105.9745},
    {"location_name": "Thanh Hoa", "latitude": 19.8067, "longitude": 105.7852},
    {"location_name": "Nghe An", "latitude": 18.6796, "longitude": 105.6813},
    {"location_name": "Ha Tinh", "latitude": 18.3559, "longitude": 105.8877},

    {"location_name": "Quang Tri", "latitude": 17.4677, "longitude": 106.6220},
    {"location_name": "Hue", "latitude": 16.4637, "longitude": 107.5909},
    {"location_name": "Da Nang", "latitude": 16.0544, "longitude": 108.2022},
    {"location_name": "Quang Ngai", "latitude": 15.1214, "longitude": 108.8044},
    {"location_name": "Gia Lai", "latitude": 13.7820, "longitude": 109.2196},
    {"location_name": "Khanh Hoa", "latitude": 12.2388, "longitude": 109.1967},
    {"location_name": "Lam Dong", "latitude": 11.9404, "longitude": 108.4583},
    {"location_name": "Dak Lak", "latitude": 12.6667, "longitude": 108.0500},

    {"location_name": "Dong Nai", "latitude": 10.9453, "longitude": 106.8243},
    {"location_name": "Ho Chi Minh City", "latitude": 10.8231, "longitude": 106.6297},
    {"location_name": "Tay Ninh", "latitude": 10.6956, "longitude": 106.2431},
    {"location_name": "Can Tho", "latitude": 10.0452, "longitude": 105.7469},
    {"location_name": "Vinh Long", "latitude": 10.2537, "longitude": 105.9722},
    {"location_name": "Dong Thap", "latitude": 10.4493, "longitude": 106.3420},
    {"location_name": "An Giang", "latitude": 10.0125, "longitude": 105.0809},
    {"location_name": "Ca Mau", "latitude": 9.1527, "longitude": 105.1961},
]

EXPECTED_ITEMS_PER_DAY = len(LOCATIONS) * 24  # 34 locations * 24 hours = 816

LATITUDES = ",".join(
    str(location["latitude"])
    for location in LOCATIONS
)

LONGITUDES = ",".join(
    str(location["longitude"])
    for location in LOCATIONS
)

HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "cloud_cover",
    "shortwave_radiation",
    "wind_speed_10m",
    "weather_code",
]

DAILY_VARIABLES = [
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "precipitation_hours",
    "sunshine_duration",
    "cloud_cover_mean",
    "shortwave_radiation_sum",
    "wind_speed_10m_max",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def create_open_meteo_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def build_request_windows(
    dates_to_crawl: list[date],
    max_window_days: int = REQUEST_WINDOW_DAYS,
) -> list[tuple[date, date]]:
    """
    Gom danh sách ngày cần cào thành các cửa sổ tối đa `max_window_days` ngày
    để tối ưu số lượng HTTP request gọi tới Open-Meteo Archive API.
    """
    if not dates_to_crawl:
        return []

    sorted_dates = sorted(set(dates_to_crawl))
    windows: list[tuple[date, date]] = []
    win_start = sorted_dates[0]
    win_end = sorted_dates[0]

    for current in sorted_dates[1:]:
        if (current - win_start).days < max_window_days:
            win_end = current
        else:
            windows.append((win_start, win_end))
            win_start = current
            win_end = current

    windows.append((win_start, win_end))
    return windows


def validate_open_meteo_raw(
    raw_text: str,
    window_start_date: date,
    window_end_date: date,
    bronze_key: str,
    batch_id: str,
    checked_at: str,
) -> list[dict]:
    """
    Kiểm tra trực tiếp trên JSON raw của Open-Meteo:
    - Duyệt qua từng ngày d trong [window_start_date, window_end_date].
    - Kỳ vọng chuẩn: 34 tỉnh * 24 giờ = 816 giá trị `temperature_2m != None`.
    - Đánh giá:
      * actual_items == 816 -> VALID
      * 0 < actual_items < 816 -> INCOMPLETE
      * actual_items == 0 -> EMPTY
    """
    payload = json.loads(raw_text)
    location_items = payload if isinstance(payload, list) else [payload]

    actual_counts: dict[str, int] = {}
    span_days = (window_end_date - window_start_date).days + 1
    for offset in range(span_days):
        d_str = (window_start_date + timedelta(days=offset)).isoformat()
        actual_counts[d_str] = 0

    for loc_obj in location_items:
        hourly = loc_obj.get("hourly") or {}
        times = hourly.get("time") or []
        temps = hourly.get("temperature_2m") or []
        for ts_str, temp_val in zip(times, temps):
            if not ts_str:
                continue
            d_str = str(ts_str)[:10]
            if d_str in actual_counts and temp_val is not None:
                actual_counts[d_str] += 1

    log_rows: list[dict] = []
    for offset in range(span_days):
        d_str = (window_start_date + timedelta(days=offset)).isoformat()
        actual = actual_counts.get(d_str, 0)
        if actual >= EXPECTED_ITEMS_PER_DAY:
            status = "VALID"
        elif actual > 0:
            status = "INCOMPLETE"
        else:
            status = "EMPTY"

        log_rows.append(
            {
                "source_name": SOURCE_NAME,
                "data_date": d_str,
                "bronze_key": bronze_key if actual > 0 else None,
                "batch_id": batch_id,
                "status": status,
                "expected_items": EXPECTED_ITEMS_PER_DAY,
                "actual_items": actual,
                "attempt_count": 1,
                "note": (
                    f"{actual}/{EXPECTED_ITEMS_PER_DAY} valid temperature_2m "
                    f"across {len(location_items)} locations"
                ),
                "checked_at": checked_at,
            }
        )

    return log_rows


def get_retry_after_seconds(response: requests.Response) -> float | None:
    retry_after = response.headers.get("Retry-After")

    if not retry_after:
        return None

    try:
        return max(float(retry_after), 0)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(retry_after)

            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)

            return max(
                (retry_at - datetime.now(timezone.utc)).total_seconds(),
                0,
            )
        except (TypeError, ValueError):
            return None


def fetch_open_meteo(
    window_start_date: date,
    window_end_date: date,
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
    task_index: int,
    session: requests.Session,
) -> tuple[dict | None, list[dict], tuple[date, str] | None]:

    window_start_date_str = window_start_date.isoformat()
    window_end_date_str = window_end_date.isoformat()
    checked_at = ingestion_timestamp.isoformat()

    params = {
        "latitude": LATITUDES,
        "longitude": LONGITUDES,
        "start_date": window_start_date_str,
        "end_date": window_end_date_str,
        "hourly": ",".join(HOURLY_VARIABLES),
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "Asia/Ho_Chi_Minh",
    }

    print(
        f"Crawling Open-Meteo: "
        f"window={window_start_date_str} to {window_end_date_str}, "
        f"locations={len(LOCATIONS)}"
    )

    for attempt in range(MAX_RETRIES):

        try:
            response = session.get(
                BASE_URL,
                params=params,
                timeout=120,
            )

            if response.status_code == 429:
                error_message = f"HTTP 429: {response.text}"

                if attempt == MAX_RETRIES - 1:
                    return None, [], (window_start_date, error_message)

                retry_after_seconds = get_retry_after_seconds(response)
                wait_seconds = (
                    retry_after_seconds
                    if retry_after_seconds is not None
                    else DEFAULT_429_WAIT_SECONDS * (2 ** attempt)
                )
                wait_seconds += random.uniform(0, 1)

                print(
                    f"-> [RETRY] Rate limited: "
                    f"window={window_start_date_str} to {window_end_date_str}, "
                    f"retry={attempt + 1}/{MAX_RETRIES}, "
                    f"sleep={wait_seconds:.1f}s, "
                    f"error={error_message}"
                )

                time.sleep(wait_seconds)
                continue

            response.raise_for_status()

            content_type = response.headers.get(
                "Content-Type",
                "",
            )

            if "application/json" not in content_type:
                raise RuntimeError(
                    f"Unexpected Content-Type: {content_type}"
                )

            bronze_key = f"{batch_id}_{task_index:06d}"

            window_log_rows = validate_open_meteo_raw(
                raw_text=response.text,
                window_start_date=window_start_date,
                window_end_date=window_end_date,
                bronze_key=bronze_key,
                batch_id=batch_id,
                checked_at=checked_at,
            )

            valid_days = sum(1 for r in window_log_rows if r["status"] == "VALID")
            incomplete_days = sum(1 for r in window_log_rows if r["status"] == "INCOMPLETE")
            empty_days = sum(1 for r in window_log_rows if r["status"] == "EMPTY")
            print(
                f"  -> Validation summary for {window_start_date_str}..{window_end_date_str}: "
                f"VALID={valid_days}, INCOMPLETE={incomplete_days}, EMPTY={empty_days}"
            )

            record = {
                "bronze_key": bronze_key,
                "source_name": SOURCE_NAME,
                "source_url": response.url,
                "source_data_start_date": window_start_date_str,
                "source_data_end_date": window_end_date_str,
                "batch_id": batch_id,
                "ingestion_timestamp": checked_at,
                "ingest_date": ingest_date,
                "raw": response.text,
            }

            return record, window_log_rows, None

        except (
            requests.RequestException,
            RuntimeError,
            ValueError,
        ) as exc:

            if attempt == MAX_RETRIES - 1:
                return None, [], (
                    window_start_date,
                    str(exc),
                )

            wait_seconds = INITIAL_BACKOFF_SECONDS * (2 ** attempt)

            print(
                f"-> [RETRY] Request failed: "
                f"window={window_start_date_str} to {window_end_date_str}, "
                f"retry={attempt + 1}/{MAX_RETRIES}, "
                f"sleep={wait_seconds}s, "
                f"error={exc}"
            )

            time.sleep(wait_seconds)

    return None, [], (
        window_start_date,
        "Max retries exceeded",
    )


def crawl_open_meteo_windows(
    windows: list[tuple[date, date]],
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> tuple[list[dict], list[dict], list[tuple[date, str]]]:

    records: list[dict] = []
    log_rows: list[dict] = []
    failed_dates: list[tuple[date, str]] = []

    total_requests = len(windows)
    print(f"Total API request windows: {total_requests}")
    print(f"Locations per request: {len(LOCATIONS)}")

    session = create_open_meteo_session()
    completed_requests = 0

    try:
        for window_start_date, window_end_date in windows:
            print(
                f"\n===== WINDOW: "
                f"{window_start_date.isoformat()} "
                f"to {window_end_date.isoformat()} ====="
            )

            task_index = completed_requests + 1

            record, window_logs, error = fetch_open_meteo(
                window_start_date=window_start_date,
                window_end_date=window_end_date,
                ingestion_timestamp=ingestion_timestamp,
                ingest_date=ingest_date,
                batch_id=batch_id,
                task_index=task_index,
                session=session,
            )

            completed_requests += 1

            if record is not None:
                records.append(record)
            if window_logs:
                log_rows.extend(window_logs)

            if error is not None:
                failed_dates.append(error)
                span_days = (window_end_date - window_start_date).days + 1
                for offset in range(span_days):
                    d_str = (window_start_date + timedelta(days=offset)).isoformat()
                    log_rows.append(
                        {
                            "source_name": SOURCE_NAME,
                            "data_date": d_str,
                            "bronze_key": None,
                            "batch_id": batch_id,
                            "status": "FAILED",
                            "expected_items": EXPECTED_ITEMS_PER_DAY,
                            "actual_items": 0,
                            "attempt_count": 1,
                            "note": f"Request failed: {error[1][:200]}",
                            "checked_at": ingestion_timestamp.isoformat(),
                        }
                    )
                break

            print(f"Progress: {completed_requests}/{total_requests}")

    finally:
        session.close()

    records.sort(key=lambda x: x["bronze_key"])
    return records, log_rows, failed_dates


def ensure_open_meteo_date_columns(spark) -> None:
    if not spark.catalog.tableExists(TABLE_NAME):
        return

    existing_columns = {
        field.name
        for field in spark.table(TABLE_NAME).schema.fields
    }
    missing_columns = [
        column
        for column in OPEN_METEO_DATE_COLUMNS
        if column not in existing_columns
    ]

    if missing_columns:
        columns_sql = ", ".join(
            f"{column} STRING"
            for column in missing_columns
        )
        spark.sql(
            f"ALTER TABLE {TABLE_NAME} "
            f"ADD COLUMNS ({columns_sql})"
        )


def write_bronze(
    spark,
    records: list[dict],
    ingest_date: str,
) -> None:
    ensure_open_meteo_date_columns(spark)

    if write_raw_bronze(
        spark,
        records,
        TABLE_NAME,
        ingest_date,
        namespace=NAMESPACE,
    ):
        print("\n=== VERIFY TABLE ===")
        spark.sql("SHOW TABLES IN nessie.bronze").show(truncate=False)
        print("====================\n")


def main() -> None:

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(
            encoding="utf-8",
            errors="replace",
        )

    ingestion_timestamp = datetime.now(timezone.utc)

    ingest_date = (
        ingestion_timestamp
        .astimezone(VN_TZ)
        .date()
        .isoformat()
    )

    batch_id = ingestion_timestamp.strftime(
        "%Y%m%d%H%M%S"
    )

    print(f"batch_id: {batch_id}")
    print(f"ingest_date: {ingest_date}")
    print(f"Locations per API request: {len(LOCATIONS)}")

    start_date = START_DATE_DEFAULT
    end_date = datetime.now(VN_TZ).date() - timedelta(days=1)

    print(
        f"Target dataset date range: "
        f"{start_date.isoformat()} to {end_date.isoformat()}"
    )

    if start_date > end_date:
        print("No dates to crawl.")
        return

    spark = None

    try:
        spark = create_spark_session("ingest_open_meteo")

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

        windows = build_request_windows(
            dates_to_crawl,
            max_window_days=REQUEST_WINDOW_DAYS,
        )

        print(
            f"Dates needing crawl/re-check: {len(dates_to_crawl)} day(s) "
            f"grouped into {len(windows)} request window(s)."
        )

        records, log_rows, failed_dates = crawl_open_meteo_windows(
            windows=windows,
            ingestion_timestamp=ingestion_timestamp,
            ingest_date=ingest_date,
            batch_id=batch_id,
        )

        if records:
            write_bronze(
                spark,
                records,
                ingest_date,
            )

        if log_rows:
            merge_ingestion_log(
                spark=spark,
                log_rows=log_rows,
            )

        if failed_dates:
            print("\n=== SUMMARY OF FAILED REQUESTS ===")
            for failed_date, error_message in failed_dates:
                print(f"- {failed_date.isoformat()}: {error_message}")
            print("===================================\n")
            raise RuntimeError(
                f"Open-Meteo ingestion failed for {len(failed_dates)} request window(s)."
            )

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
