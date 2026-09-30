import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from bronze.bronze_utils import write_raw_bronze
from bronze.http_client import create_legacy_tls_session
from utils.spark import create_spark_session


# URL trang nhúng bảng hồ chứa thủy điện của EVN.
# Tham số td (thời điểm) được truyền vào dưới dạng dd/MM/yyyy HH:mm.
BASE_URL = "https://hochuathuydien.evn.com.vn/PageHoChuaThuyDienEmbedEVN.aspx"

# Tên nguồn dữ liệu — khớp với convention source_name của evn.py
SOURCE_NAME = "evn_hydro"

# Dataset bắt đầu từ 01/01/2023
START_DATE_DEFAULT = date(2023, 1, 1)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# Thời điểm cuối ngày dùng để request snapshot của ngày đó.
# Sử dụng 23:00 thay vì 23:59 vì đây là múi giờ chốt thường gặp của hệ thống EVN.
END_OF_DAY_HOUR = 23
END_OF_DAY_MINUTE = 0

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
    Ví dụ: date(2026, 9, 22) → "22/09/2026 23:00"
    """
    return (
        f"{data_date.day:02d}/{data_date.month:02d}/{data_date.year} "
        f"{END_OF_DAY_HOUR:02d}:{END_OF_DAY_MINUTE:02d}"
    )


def crawl_hydro_dates(
    session: requests.Session,
    start_date: date,
    end_date: date,
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> list[dict]:
    """
    Thực hiện HTTP GET để lấy raw HTML snapshot hồ chứa thủy điện
    cho từng ngày từ start_date đến end_date.

    Trả về list chứa các raw records. Nếu một ngày lỗi, throw exception
    để dừng pipeline, không tạo record giả.
    """
    records = []
    current_date = start_date
    index = 1

    while current_date <= end_date:
        td_param = build_end_of_day_td_param(current_date)
        source_url = f"{BASE_URL}?td={td_param}"

        print(f"Crawling Hydro snapshot for data_date={current_date.isoformat()} (URL: {source_url})")

        try:
            response = session.get(
                BASE_URL,
                params={"td": td_param},
                headers=HEADERS,
                timeout=60,
            )
            response.raise_for_status()
            
            print(f"  HTTP status: {response.status_code}, Length: {len(response.content)} bytes")

            record = {
                "bronze_key": f"{batch_id}_{index}",
                "source_name": SOURCE_NAME,
                "source_url": response.url,
                "source_data_date": current_date.isoformat(),
                "batch_id": batch_id,
                "ingestion_timestamp": ingestion_timestamp.isoformat(),
                "ingest_date": ingest_date,
                "raw": response.text,
            }
            records.append(record)
            
        except Exception as e:
            print(f"Error crawling data_date={current_date.isoformat()}: {e}")
            raise

        current_date += timedelta(days=1)
        index += 1
        time.sleep(0.1)

    return records


def write_bronze(
    spark,
    records: list[dict],
    ingest_date: str,
) -> None:
    write_raw_bronze(spark, records, TABLE_NAME, ingest_date)


def main() -> None:
    # Thời điểm thực thi ingestion (UTC) — dùng để tạo batch_id và ingest_date
    ingestion_timestamp = datetime.now(timezone.utc)
    ingest_date = ingestion_timestamp.date().isoformat()
    batch_id = ingestion_timestamp.strftime("%Y%m%d%H%M%S")

    print(f"batch_id: {batch_id}")
    print(f"ingest_date: {ingest_date}")

    spark = None
    try:
        spark = create_spark_session("ingest_evn_hydro")
        
        # Đảm bảo namespace Bronze tồn tại
        print(f"Creating namespace if needed: {NAMESPACE}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
        
        start_date = START_DATE_DEFAULT
        
        # Kiểm tra bảng đã tồn tại để thiết lập ngày bắt đầu (tái sử dụng Metadata/Watermark từ bảng gốc)
        if spark.catalog.tableExists(TABLE_NAME):
            try:
                # Lấy ngày lớn nhất đã crawl thành công từ table thay vì framework watermark ngoài
                max_date_row = spark.sql(f"SELECT MAX(source_data_date) FROM {TABLE_NAME}").collect()
                max_date_str = max_date_row[0][0] if max_date_row else None
                if max_date_str:
                    max_date = date.fromisoformat(max_date_str)
                    # Bắt đầu từ ngày sau ngày cuối cùng đã có
                    start_date = max_date + timedelta(days=1)
            except Exception as e:
                print(f"Warning: Could not fetch max date from table, defaulting to {START_DATE_DEFAULT.isoformat()}: {e}")

        # END_DATE là ngày hôm trước của ngày hiện tại ở Việt Nam (do chốt data lúc 23:00)
        end_date = datetime.now(VN_TZ).date() - timedelta(days=1)

        print(f"Dataset date range: {start_date.isoformat()} to {end_date.isoformat()}")

        if start_date > end_date:
            print("No new dates to crawl. Dataset is up to date.")
            return

        session = create_hydro_session()

        records = crawl_hydro_dates(
            session=session,
            start_date=start_date,
            end_date=end_date,
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

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
