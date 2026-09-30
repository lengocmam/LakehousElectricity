import re
import time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urljoin
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


BASE_URL = "https://www.evn.com.vn"
SOURCE_NAME = "evn"

FIRST_URL = (
    f"{BASE_URL}/vi-VN/news-l/"
    "Thong-tin-tom-tat-van-hanh-HTD-Quoc-gia-60-2015"
)

TARGET_PREFIX = (
    "/d/vi-VN/news/"
    "Thong-tin-chung-ve-van-hanh-he-thong-dien-Quoc-gia-ngay-"
)

# Nessie catalog
NAMESPACE = "nessie.bronze"
TABLE_NAME = "nessie.bronze.evn"

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
START_DATE_DEFAULT = date(2023, 1, 1)
RECENT_RECHECK_DAYS = 7
EXHAUSTED_AFTER_DAYS = 10
MIN_ARTICLE_TEXT_LENGTH = 200

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
}

DATE_RE = re.compile(
    r"ngay-(\d+)"
)

CONTENT_DATE_RE = re.compile(
    r"ngày\s+(\d{1,2})[/-](\d{1,2})[/-](\d{4})"
)


def create_evn_session() -> requests.Session:
    return create_legacy_tls_session(BASE_URL)


def parse_date(day: str, month: str, year: str) -> date | None:
    try:
        return date(
            int(year),
            int(month),
            int(day)
        )
    except ValueError:
        return None


def extract_source_data_date_from_url(url: str) -> date | None:

    match = DATE_RE.search(url)

    if not match:
        return None

    value = match.group(1)

    if len(value) <= 4:
        return None

    year = int(value[-4:])
    day_month = value[:-4]

    # Format: DDMMYYYY
    if len(day_month) == 4:
        day = int(day_month[:2])
        month = int(day_month[2:])

        return parse_date(
            str(day),
            str(month),
            str(year)
        )

    # Format: DMMYYYY or DDMYYYY
    if len(day_month) == 3:

        # Try DD + M
        result = parse_date(
            day_month[:2],
            day_month[2:],
            str(year)
        )

        if result:
            return result

        # Try D + MM
        return parse_date(
            day_month[:1],
            day_month[1:],
            str(year)
        )

    # Format: DMYYYY
    if len(day_month) == 2:
        return parse_date(
            day_month[:1],
            day_month[1:],
            str(year)
        )

    return None


def extract_source_data_date_from_content(tree) -> date | None:
    title_text = (
        tree.xpath("string(//h1)")
        or tree.xpath("string(//title)")
    )

    match = CONTENT_DATE_RE.search(title_text)

    if not match:
        return None

    day, month, year = match.groups()

    return parse_date(day, month, year)


def extract_source_data_date(url: str, tree) -> date | None:
    return (
        extract_source_data_date_from_url(url)
        or extract_source_data_date_from_content(tree)
    )


def crawl_listing_pages(
    session: requests.Session,
    target_dates: set[date],
) -> set[str]:
    """
    Quét danh sách bài báo vận hành EVN và dừng sớm (early stopping)
    ngay khi toàn bộ bài viết trên trang đã cũ hơn ngày nhỏ nhất cần cào
    (`min_target_date`).
    """
    if not target_dates:
        return set()

    min_target_date = min(target_dates)
    selected_links: set[str] = set()
    seen_all_links: set[str] = set()
    page = 1

    while True:
        print(f"Scanning page {page} (min_target_date={min_target_date.isoformat()})")

        response = session.get(
            FIRST_URL,
            params={"page": page},
            headers=HEADERS,
            timeout=20,
        )
        response.raise_for_status()

        tree = html.fromstring(response.content)
        page_links: set[str] = set()
        page_dates: list[date] = []

        for a_tag in tree.xpath(
            '//div[@id="ContentPlaceHolder1_ctl00_row2_container_col1"]'
            '//a[@href]'
        ):
            url = a_tag.get("href")
            if url and url.startswith(TARGET_PREFIX):
                full_url = urljoin(BASE_URL, url)
                page_links.add(full_url)
                url_date = extract_source_data_date_from_url(full_url)
                if url_date is not None:
                    page_dates.append(url_date)
                    if url_date in target_dates:
                        selected_links.add(full_url)
                else:
                    # Nếu không tách được ngày từ URL thì vẫn tải bài báo để tách từ <h1>
                    selected_links.add(full_url)

        print(f"Found {len(page_links)} EVN article links on page {page}")

        if not page_links:
            if page == 1:
                raise RuntimeError(
                    "No article links found on the first page. "
                    "The EVN HTML structure or XPath may have changed."
                )
            break

        new_links = page_links - seen_all_links
        if not new_links:
            print("No new links on page. Stopping pagination.")
            break

        seen_all_links.update(new_links)

        # Early stopping: Nếu tất cả các bài trên trang đều có ngày < min_target_date
        if page_dates and max(page_dates) < min_target_date:
            print(
                f"Early stopping at page {page}: max article date on page "
                f"({max(page_dates).isoformat()}) < min_target_date ({min_target_date.isoformat()})."
            )
            break

        page += 1
        time.sleep(0.5)

    return selected_links


def crawl_articles(
    session: requests.Session,
    links: set[str],
    target_dates: set[date],
    end_date: date,
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> tuple[list[dict], list[dict]]:
    """
    Tải chi tiết từng bài báo, kiểm chứng độ dài nội dung và tổng hợp kết quả
    vào `log_rows` theo từng `data_date`.
    """
    records: list[dict] = []
    date_to_valid_articles: dict[str, list[str]] = {}
    checked_at = ingestion_timestamp.isoformat()

    for index, link in enumerate(sorted(links), start=1):
        try:
            print(f"Crawling {index}/{len(links)}: {link}")

            response = session.get(
                link,
                headers=HEADERS,
                timeout=20,
            )
            response.raise_for_status()

            detail_tree = html.fromstring(response.content)
            source_data_date = extract_source_data_date(link, detail_tree)

            if source_data_date is None:
                print(f"Warning: could not extract source_data_date: {link}")
                continue

            if source_data_date not in target_dates:
                continue

            body_text = " ".join(detail_tree.xpath("//body//text()")).strip()
            is_valid_content = len(body_text) >= MIN_ARTICLE_TEXT_LENGTH

            bronze_key = f"{batch_id}_{index}"
            data_date_str = source_data_date.isoformat()

            records.append(
                {
                    "bronze_key": bronze_key,
                    "source_name": SOURCE_NAME,
                    "source_url": link,
                    "source_data_date": data_date_str,
                    "batch_id": batch_id,
                    "ingestion_timestamp": checked_at,
                    "ingest_date": ingest_date,
                    "raw": response.text,
                }
            )

            if is_valid_content:
                date_to_valid_articles.setdefault(data_date_str, []).append(bronze_key)

            time.sleep(0.1)

        except requests.RequestException as error:
            print(f"Failed: {link} -> {error}")

    # Xây dựng log_rows cho toàn bộ target_dates
    log_rows: list[dict] = []
    for d in sorted(target_dates):
        d_str = d.isoformat()
        valid_keys = date_to_valid_articles.get(d_str, [])
        actual_items = len(valid_keys)

        if actual_items >= 1:
            log_rows.append({
                "source_name": SOURCE_NAME,
                "data_date": d_str,
                "bronze_key": valid_keys[-1],
                "batch_id": batch_id,
                "status": "VALID",
                "expected_items": 1,
                "actual_items": actual_items,
                "attempt_count": 1,
                "note": f"Found {actual_items} valid EVN operational article(s)",
                "checked_at": checked_at,
            })
        else:
            age_days = (end_date - d).days
            # Những ngày quá khứ (> 10 ngày) không có bài báo do nghỉ lễ/cuối tuần
            # được chuyển sang EXHAUSTED để lần sau không phải lật lại trang cũ
            status = "EXHAUSTED" if age_days > EXHAUSTED_AFTER_DAYS else "EMPTY"
            note = (
                "No article published (holiday/weekend - marked EXHAUSTED)"
                if status == "EXHAUSTED"
                else "No article published yet for recent date"
            )
            log_rows.append({
                "source_name": SOURCE_NAME,
                "data_date": d_str,
                "bronze_key": None,
                "batch_id": batch_id,
                "status": status,
                "expected_items": 1,
                "actual_items": 0,
                "attempt_count": 1,
                "note": note,
                "checked_at": checked_at,
            })

    return records, log_rows


def write_bronze(
    spark,
    records: list[dict],
    ingest_date: str,
):
    write_raw_bronze(
        spark,
        records,
        TABLE_NAME,
        ingest_date,
        namespace=NAMESPACE,
        fail_on_empty=False,
    )


def main():
    ingestion_timestamp = datetime.now(timezone.utc)
    ingest_date = ingestion_timestamp.date().isoformat()
    batch_id = ingestion_timestamp.strftime("%Y%m%d%H%M%S")

    start_date = START_DATE_DEFAULT
    end_date = datetime.now(VN_TZ).date() - timedelta(days=1)

    spark = None
    try:
        spark = create_spark_session("ingest_evn")

        dates_to_crawl = get_dates_to_crawl(
            spark=spark,
            source_name=SOURCE_NAME,
            start_date=start_date,
            end_date=end_date,
            recent_days=RECENT_RECHECK_DAYS,
        )

        if not dates_to_crawl:
            print("All EVN dates are already VALID or EXHAUSTED. Nothing to crawl.")
            return

        target_dates = set(dates_to_crawl)
        print(f"Target EVN dates needing crawl/re-check: {len(target_dates)} day(s)")

        session = create_evn_session()
        links = crawl_listing_pages(session, target_dates=target_dates)

        print(f"Total matching article links to fetch: {len(links)}")
        if links:
            print("Sample links:", sorted(links)[:5])

        records, log_rows = crawl_articles(
            session=session,
            links=links,
            target_dates=target_dates,
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

        if log_rows:
            merge_ingestion_log(
                spark=spark,
                log_rows=log_rows,
            )

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
