import re
import time
from datetime import date, datetime, timezone
from urllib.parse import urljoin

import requests
from lxml import html

from bronze.bronze_utils import write_raw_bronze
from bronze.http_client import create_legacy_tls_session
from utils.spark import create_spark_session


BASE_URL = "https://www.evn.com.vn"

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

    # Format: DMMYYYY
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


def crawl_listing_pages(session: requests.Session) -> set[str]:
    links = set()
    page = 1

    while True:
        print(f"Scanning page {page}")

        response = session.get(
            FIRST_URL,
            params={"page": page},
            headers=HEADERS,
            timeout=20,
        )

        response.raise_for_status()

        print("STATUS =", response.status_code)
        print("REQUEST URL =", FIRST_URL)
        print("FINAL URL =", response.url)
        print("HISTORY =", [
            (r.status_code, r.url)
            for r in response.history
        ])
        print("CONTENT TYPE =", response.headers.get("Content-Type"))
        print("CONTENT LENGTH =", len(response.content))
        print("FIRST 500 CHARACTERS:")
        print(response.text[:500])

        tree = html.fromstring(response.content)

        page_links = set()

        for a_tag in tree.xpath(
            '//div[@id="ContentPlaceHolder1_ctl00_row2_container_col1"]'
            '//a[@href]'
        ):
            url = a_tag.get("href")

            print("URL =", repr(url))
            print(
                "MATCH =",
                url.startswith(TARGET_PREFIX)
                if url
                else False
            )

            if url and url.startswith(TARGET_PREFIX):
                page_links.add(
                    urljoin(BASE_URL, url)
                )

        print(f"Found {len(page_links)} links")

        if not page_links:

            if page == 1:
                raise RuntimeError(
                    "No article links found on the first page. "
                    "The EVN HTML structure or XPath may have changed."
                )

            break

        new_links = page_links - links

        if not new_links:
            print("No new links. Stop pagination.")
            break

        links.update(new_links)

        page += 1

        time.sleep(0.5)

    return links


def crawl_articles(
    session: requests.Session,
    links: set[str],
    ingestion_timestamp: datetime,
    ingest_date: str,
    batch_id: str,
) -> list[dict]:

    records = []

    for index, link in enumerate(
        sorted(links),
        start=1
    ):

        try:
            print(
                f"Crawling {index}/{len(links)}: {link}"
            )

            response = session.get(
                link,
                headers=HEADERS,
                timeout=20
            )

            response.raise_for_status()

            detail_tree = html.fromstring(
                response.content
            )

            source_data_date = extract_source_data_date(
                link,
                detail_tree
            )

            if source_data_date is None:
                print(
                    f"Warning: could not extract "
                    f"source_data_date: {link}"
                )

            records.append(
                {
                    "bronze_key": f"{batch_id}_{index}",
                    "source_name": "evn",
                    "source_url": link,
                    "source_data_date": (
                        source_data_date.isoformat()
                        if source_data_date
                        else None
                    ),
                    "batch_id": batch_id,
                    "ingestion_timestamp": (
                        ingestion_timestamp.isoformat()
                    ),
                    "ingest_date": ingest_date,
                    "raw": response.text,
                }
            )

            time.sleep(0.1)

        except requests.RequestException as error:
            print(f"Failed: {link}")
            print(error)

    return records


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
        fail_on_empty=True,
    )


def main():

    ingestion_timestamp = datetime.now(timezone.utc)

    ingest_date = (
        ingestion_timestamp
        .date()
        .isoformat()
    )

    batch_id = ingestion_timestamp.strftime(
        "%Y%m%d%H%M%S"
    )

    session = create_evn_session()

    links = crawl_listing_pages(session)

    print(
        f"Total unique links: {len(links)}"
    )

    print(
        "Sample links:",
        sorted(links)[:5]
    )

    records = crawl_articles(
        session=session,
        links=links,
        ingestion_timestamp=ingestion_timestamp,
        ingest_date=ingest_date,
        batch_id=batch_id,
    )

    spark = None

    try:

        spark = create_spark_session(
            "ingest_evn"
        )

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
