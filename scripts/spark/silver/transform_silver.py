import json
import re
from datetime import datetime, timezone
from lxml import html
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from bronze.open_meteo import LOCATIONS
from silver.silver_utils import (
    PROVINCE_TO_REGION,
    SILVER_NAMESPACE,
    ensure_entity_dimension,
    get_valid_bronze_keys_to_process,
    resolve_reservoir_entity,
)
from utils.spark import create_spark_session


WEATHER_DAILY_TABLE = f"{SILVER_NAMESPACE}.weather_daily"
NSMO_LOAD_DAILY_TABLE = f"{SILVER_NAMESPACE}.grid_load_daily"
HYDRO_DAILY_TABLE = f"{SILVER_NAMESPACE}.hydro_reservoir_daily"
EVN_OPS_DAILY_TABLE = f"{SILVER_NAMESPACE}.evn_daily_operations"

WEATHER_DAILY_SCHEMA = StructType([
    StructField("data_date", StringType(), False),
    StructField("location_name", StringType(), False),
    StructField("region_code", StringType(), False),
    StructField("temperature_2m_mean", DoubleType(), True),
    StructField("temperature_2m_max", DoubleType(), True),
    StructField("temperature_2m_min", DoubleType(), True),
    StructField("precipitation_sum", DoubleType(), True),
    StructField("shortwave_radiation_sum", DoubleType(), True),
    StructField("wind_speed_10m_max", DoubleType(), True),
    StructField("bronze_key", StringType(), False),
    StructField("processed_at", StringType(), False),
])

NSMO_LOAD_DAILY_SCHEMA = StructType([
    StructField("data_date", StringType(), False),
    StructField("peak_load_mw", DoubleType(), True),
    StructField("min_load_mw", DoubleType(), True),
    StructField("avg_load_mw", DoubleType(), True),
    StructField("valid_cycle_points", IntegerType(), False),
    StructField("bronze_key", StringType(), False),
    StructField("processed_at", StringType(), False),
])

HYDRO_DAILY_SCHEMA = StructType([
    StructField("data_date", StringType(), False),
    StructField("jc_entity_id", StringType(), False),
    StructField("canonical_name", StringType(), False),
    StructField("raw_reservoir_name", StringType(), False),
    StructField("province_name", StringType(), False),
    StructField("region_code", StringType(), False),
    StructField("upstream_level_m", DoubleType(), True),
    StructField("normal_level_m", DoubleType(), True),
    StructField("dead_level_m", DoubleType(), True),
    StructField("headroom_to_dead_m", DoubleType(), True),
    StructField("inflow_m3s", DoubleType(), True),
    StructField("total_discharge_m3s", DoubleType(), True),
    StructField("open_spillway_gates", IntegerType(), True),
    StructField("bronze_key", StringType(), False),
    StructField("processed_at", StringType(), False),
])

EVN_OPS_DAILY_SCHEMA = StructType([
    StructField("data_date", StringType(), False),
    StructField("peak_capacity_mw", DoubleType(), True),
    StructField("daily_energy_million_kwh", DoubleType(), True),
    StructField("hydro_energy_million_kwh", DoubleType(), True),
    StructField("coal_energy_million_kwh", DoubleType(), True),
    StructField("evidence_status", StringType(), False),
    StructField("source_url", StringType(), False),
    StructField("bronze_key", StringType(), False),
    StructField("processed_at", StringType(), False),
])


def _parse_vn_float(val_str: str | None) -> float | None:
    if not val_str:
        return None
    cleaned = val_str.strip()
    if not cleaned or "KXD" in cleaned.upper():
        return None
    match = re.search(r"-?\d+(?:[.,]\d+)?", cleaned)
    if not match:
        return None
    token = match.group(0)
    # Chuẩn hóa dấu phẩy thập phân tiếng Việt
    if "," in token and "." not in token:
        token = token.replace(",", ".")
    elif "," in token and "." in token:
        token = token.replace(".", "").replace(",", ".")
    try:
        return float(token)
    except ValueError:
        return None


def _upsert_silver_table(
    spark: SparkSession,
    rows: list[dict],
    schema: StructType,
    table_name: str,
    merge_keys: list[str],
) -> None:
    if not rows:
        print(f"[{table_name}] No new Silver rows to upsert.")
        return

    df = spark.createDataFrame(rows, schema=schema)
    if spark.catalog.tableExists(table_name):
        view_name = f"tmp_{table_name.split('.')[-1]}"
        df.createOrReplaceTempView(view_name)
        on_clause = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
        spark.sql(
            f"""
            MERGE INTO {table_name} AS t
            USING {view_name} AS s
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    else:
        df.writeTo(table_name).using("iceberg").create()

    print(f"[{table_name}] Upserted {len(rows)} row(s).")


def transform_open_meteo_silver(spark: SparkSession, processed_at: str) -> None:
    valid_map = get_valid_bronze_keys_to_process(
        spark,
        source_name="open-meteo",
        silver_table=WEATHER_DAILY_TABLE,
    )
    if not valid_map or not spark.catalog.tableExists("nessie.bronze.open_meteo"):
        print("[Silver Open-Meteo] No new VALID dates to transform.")
        return

    target_keys = sorted(set(valid_map.values()))
    keys_sql = ", ".join(f"'{k}'" for k in target_keys)
    raw_rows = (
        spark.table("nessie.bronze.open_meteo")
        .filter(f"bronze_key IN ({keys_sql})")
        .select("bronze_key", "raw")
        .collect()
    )

    silver_rows: list[dict] = []
    for row in raw_rows:
        b_key = row["bronze_key"]
        payload = json.loads(row["raw"])
        loc_items = payload if isinstance(payload, list) else [payload]

        for idx, loc_obj in enumerate(loc_items):
            loc_name = (
                LOCATIONS[idx]["location_name"]
                if idx < len(LOCATIONS)
                else f"Location_{idx}"
            )
            region_code = PROVINCE_TO_REGION.get(loc_name, "UNMAPPED")
            daily = loc_obj.get("daily") or {}
            dates = daily.get("time") or []
            t_mean = daily.get("temperature_2m_mean") or []
            t_max = daily.get("temperature_2m_max") or []
            t_min = daily.get("temperature_2m_min") or []
            precip = daily.get("precipitation_sum") or []
            rad = daily.get("shortwave_radiation_sum") or []
            wind = daily.get("wind_speed_10m_max") or []

            for i, d_str in enumerate(dates):
                if valid_map.get(d_str) != b_key:
                    continue
                silver_rows.append({
                    "data_date": str(d_str),
                    "location_name": loc_name,
                    "region_code": region_code,
                    "temperature_2m_mean": float(t_mean[i]) if i < len(t_mean) and t_mean[i] is not None else None,
                    "temperature_2m_max": float(t_max[i]) if i < len(t_max) and t_max[i] is not None else None,
                    "temperature_2m_min": float(t_min[i]) if i < len(t_min) and t_min[i] is not None else None,
                    "precipitation_sum": float(precip[i]) if i < len(precip) and precip[i] is not None else None,
                    "shortwave_radiation_sum": float(rad[i]) if i < len(rad) and rad[i] is not None else None,
                    "wind_speed_10m_max": float(wind[i]) if i < len(wind) and wind[i] is not None else None,
                    "bronze_key": b_key,
                    "processed_at": processed_at,
                })

    _upsert_silver_table(
        spark,
        silver_rows,
        WEATHER_DAILY_SCHEMA,
        WEATHER_DAILY_TABLE,
        merge_keys=["data_date", "location_name"],
    )


def _extract_nsmo_load_values(node) -> list[float]:
    values: list[float] = []
    if isinstance(node, list):
        for item in node:
            if isinstance(item, (int, float)) and not isinstance(item, bool) and item > 0:
                values.append(float(item))
            elif isinstance(item, dict):
                for k, v in item.items():
                    if (
                        isinstance(v, (int, float))
                        and not isinstance(v, bool)
                        and k.lower() not in {"id", "hour", "index", "stt", "order"}
                        and v > 0
                    ):
                        values.append(float(v))
                        break
            else:
                values.extend(_extract_nsmo_load_values(item))
    elif isinstance(node, dict):
        for key in ("data", "items", "series", "chartData", "values", "phuTai", "listData"):
            if key in node:
                sub = _extract_nsmo_load_values(node[key])
                if sub:
                    return sub
        for v in node.values():
            sub = _extract_nsmo_load_values(v)
            if sub:
                return sub
    return values


def transform_nsmo_silver(spark: SparkSession, processed_at: str) -> None:
    valid_map = get_valid_bronze_keys_to_process(
        spark,
        source_name="nsmo",
        silver_table=NSMO_LOAD_DAILY_TABLE,
    )
    if not valid_map or not spark.catalog.tableExists("nessie.bronze.nsmo"):
        print("[Silver NSMO] No new VALID dates to transform.")
        return

    keys_sql = ", ".join(f"'{k}'" for k in sorted(set(valid_map.values())))
    raw_rows = (
        spark.table("nessie.bronze.nsmo")
        .filter(f"bronze_key IN ({keys_sql})")
        .select("bronze_key", "source_data_date", "raw")
        .collect()
    )

    silver_rows: list[dict] = []
    for row in raw_rows:
        d_str = row["source_data_date"]
        b_key = row["bronze_key"]
        if not d_str or valid_map.get(d_str) != b_key:
            continue

        payload = json.loads(row["raw"])
        target = payload.get("result", payload) if isinstance(payload, dict) else payload
        vals = _extract_nsmo_load_values(target)
        if not vals:
            continue

        silver_rows.append({
            "data_date": str(d_str),
            "peak_load_mw": max(vals),
            "min_load_mw": min(vals),
            "avg_load_mw": sum(vals) / len(vals),
            "valid_cycle_points": len(vals),
            "bronze_key": b_key,
            "processed_at": processed_at,
        })

    _upsert_silver_table(
        spark,
        silver_rows,
        NSMO_LOAD_DAILY_SCHEMA,
        NSMO_LOAD_DAILY_TABLE,
        merge_keys=["data_date"],
    )


def transform_hydro_silver(spark: SparkSession, processed_at: str) -> None:
    valid_map = get_valid_bronze_keys_to_process(
        spark,
        source_name="evn_hydro",
        silver_table=HYDRO_DAILY_TABLE,
    )
    if not valid_map or not spark.catalog.tableExists("nessie.bronze.evn_hydro"):
        print("[Silver EVN Hydro] No new VALID dates to transform.")
        return

    keys_sql = ", ".join(f"'{k}'" for k in sorted(set(valid_map.values())))
    raw_rows = (
        spark.table("nessie.bronze.evn_hydro")
        .filter(f"bronze_key IN ({keys_sql})")
        .select("bronze_key", "source_data_date", "raw")
        .collect()
    )

    silver_rows: list[dict] = []
    for row in raw_rows:
        d_str = row["source_data_date"]
        b_key = row["bronze_key"]
        if not d_str or valid_map.get(d_str) != b_key:
            continue

        tree = html.fromstring(row["raw"].encode("utf-8", errors="ignore"))
        for tr in tree.xpath("//table//tr[td]"):
            cells = [" ".join(td.itertext()).strip() for td in tr.xpath("./td")]
            if len(cells) < 7:
                continue

            raw_name = cells[0]
            if not raw_name or raw_name.lower().startswith(("tên hồ", "stt", "lưu vực")):
                continue

            upstream_m = _parse_vn_float(cells[2]) if len(cells) > 2 else None
            normal_m = _parse_vn_float(cells[3]) if len(cells) > 3 else None
            dead_m = _parse_vn_float(cells[4]) if len(cells) > 4 else None
            inflow = _parse_vn_float(cells[5]) if len(cells) > 5 else None
            total_discharge = _parse_vn_float(cells[6]) if len(cells) > 6 else None
            deep_gates = int(_parse_vn_float(cells[9]) or 0) if len(cells) > 9 else 0
            surface_gates = int(_parse_vn_float(cells[10]) or 0) if len(cells) > 10 else 0

            if upstream_m is None and inflow is None:
                continue

            entity = resolve_reservoir_entity(raw_name)
            headroom = (
                round(upstream_m - dead_m, 3)
                if upstream_m is not None and dead_m is not None
                else None
            )

            silver_rows.append({
                "data_date": str(d_str),
                "jc_entity_id": entity["jc_entity_id"],
                "canonical_name": entity["canonical_name"],
                "raw_reservoir_name": raw_name,
                "province_name": entity["province_name"],
                "region_code": entity["region_code"],
                "upstream_level_m": upstream_m,
                "normal_level_m": normal_m,
                "dead_level_m": dead_m,
                "headroom_to_dead_m": headroom,
                "inflow_m3s": inflow,
                "total_discharge_m3s": total_discharge,
                "open_spillway_gates": deep_gates + surface_gates,
                "bronze_key": b_key,
                "processed_at": processed_at,
            })

    _upsert_silver_table(
        spark,
        silver_rows,
        HYDRO_DAILY_SCHEMA,
        HYDRO_DAILY_TABLE,
        merge_keys=["data_date", "jc_entity_id"],
    )


def transform_evn_silver(spark: SparkSession, processed_at: str) -> None:
    """
    Bóc tách số liệu vận hành từ bài viết EVN kèm cơ chế kiểm chứng bằng chứng
    (Evidence Verification — Chuẩn SalesNow):
    - Nếu trích xuất được đầy đủ công suất đỉnh hoặc sản lượng ngày có bằng chứng số rõ ràng
      trong văn bản: `evidence_status = 'VERIFIED'`.
    - Nếu văn bản thay đổi mẫu câu không thể trích xuất chắc chắn:
      đánh dấu `evidence_status = 'UNVERIFIABLE'` ("không thể xác định") thay vì đoán mò.
    """
    valid_map = get_valid_bronze_keys_to_process(
        spark,
        source_name="evn",
        silver_table=EVN_OPS_DAILY_TABLE,
    )
    if not valid_map or not spark.catalog.tableExists("nessie.bronze.evn"):
        print("[Silver EVN News] No new VALID dates to transform.")
        return

    keys_sql = ", ".join(f"'{k}'" for k in sorted(set(valid_map.values())))
    raw_rows = (
        spark.table("nessie.bronze.evn")
        .filter(f"bronze_key IN ({keys_sql})")
        .select("bronze_key", "source_data_date", "source_url", "raw")
        .collect()
    )

    peak_re = re.compile(r"công\s+suất\s+lớn\s+nhất[^0-9]{1,40}(\d+(?:[.,]\d+)?)", re.IGNORECASE)
    energy_re = re.compile(r"sản\s+lượng[^0-9]{1,40}(\d+(?:[.,]\d+)?)\s*triệu\s*kWh", re.IGNORECASE)
    hydro_re = re.compile(r"thủy\s+điện[^0-9]{1,30}(\d+(?:[.,]\d+)?)", re.IGNORECASE)
    coal_re = re.compile(r"nhiệt\s+điện\s+than[^0-9]{1,30}(\d+(?:[.,]\d+)?)", re.IGNORECASE)

    silver_rows: list[dict] = []
    for row in raw_rows:
        d_str = row["source_data_date"]
        b_key = row["bronze_key"]
        if not d_str or valid_map.get(d_str) != b_key:
            continue

        tree = html.fromstring(row["raw"].encode("utf-8", errors="ignore"))
        text = " ".join(tree.xpath("//body//text()"))

        peak_m = peak_re.search(text)
        energy_m = energy_re.search(text)
        hydro_m = hydro_re.search(text)
        coal_m = coal_re.search(text)

        peak_mw = _parse_vn_float(peak_m.group(1)) if peak_m else None
        daily_kwh = _parse_vn_float(energy_m.group(1)) if energy_m else None
        hydro_kwh = _parse_vn_float(hydro_m.group(1)) if hydro_m else None
        coal_kwh = _parse_vn_float(coal_m.group(1)) if coal_m else None

        evidence_status = (
            "VERIFIED"
            if (peak_mw is not None or daily_kwh is not None)
            else "UNVERIFIABLE"
        )

        silver_rows.append({
            "data_date": str(d_str),
            "peak_capacity_mw": peak_mw,
            "daily_energy_million_kwh": daily_kwh,
            "hydro_energy_million_kwh": hydro_kwh,
            "coal_energy_million_kwh": coal_kwh,
            "evidence_status": evidence_status,
            "source_url": row["source_url"],
            "bronze_key": b_key,
            "processed_at": processed_at,
        })

    _upsert_silver_table(
        spark,
        silver_rows,
        EVN_OPS_DAILY_SCHEMA,
        EVN_OPS_DAILY_TABLE,
        merge_keys=["data_date"],
    )


def main() -> None:
    processed_at = datetime.now(timezone.utc).isoformat()
    spark = None
    try:
        spark = create_spark_session("transform_silver")
        ensure_entity_dimension(spark)
        transform_open_meteo_silver(spark, processed_at)
        transform_nsmo_silver(spark, processed_at)
        transform_hydro_silver(spark, processed_at)
        transform_evn_silver(spark, processed_at)
        print("All Silver transformations completed successfully.")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
