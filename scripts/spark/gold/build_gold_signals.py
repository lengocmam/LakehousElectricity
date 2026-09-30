from datetime import datetime, timezone
from pyspark.sql import SparkSession

from bronze.bronze_utils import INGESTION_LOG_TABLE, ensure_ingestion_log_table
from silver.transform_silver import (
    EVN_OPS_DAILY_TABLE,
    HYDRO_DAILY_TABLE,
    NSMO_LOAD_DAILY_TABLE,
    WEATHER_DAILY_TABLE,
)
from utils.spark import create_spark_session


GOLD_NAMESPACE = "nessie.gold"
SLA_SUMMARY_TABLE = f"{GOLD_NAMESPACE}.sla_data_quality_summary"
DAILY_GRID_HEALTH_TABLE = f"{GOLD_NAMESPACE}.fact_daily_grid_health"
ACTIONABLE_SIGNALS_TABLE = f"{GOLD_NAMESPACE}.fact_actionable_signals"


def build_sla_data_quality_summary(spark: SparkSession) -> None:
    """
    Xây dựng bảng giám sát SLA Chất lượng Dữ liệu (Độ đầy đủ, Độ mới, Độ chính xác)
    từ `nessie.bronze.ingestion_log` cho từng nguồn.
    """
    ensure_ingestion_log_table(spark)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")

    query = f"""
    CREATE OR REPLACE TABLE {SLA_SUMMARY_TABLE}
    USING iceberg
    AS
    SELECT
        source_name,
        COUNT(*) AS total_tracked_days,
        SUM(CASE WHEN status = 'VALID' THEN 1 ELSE 0 END) AS valid_days,
        SUM(CASE WHEN status = 'INCOMPLETE' THEN 1 ELSE 0 END) AS incomplete_days,
        SUM(CASE WHEN status = 'EMPTY' THEN 1 ELSE 0 END) AS empty_days,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_days,
        SUM(CASE WHEN status = 'EXHAUSTED' THEN 1 ELSE 0 END) AS exhausted_days,
        ROUND(
            100.0 * SUM(CASE WHEN status = 'VALID' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*) - SUM(CASE WHEN status = 'EXHAUSTED' THEN 1 ELSE 0 END), 0),
            2
        ) AS completeness_sla_pct,
        MAX(CASE WHEN status = 'VALID' THEN data_date ELSE NULL END) AS latest_valid_date,
        datediff(
            current_date(),
            to_date(MAX(CASE WHEN status = 'VALID' THEN data_date ELSE NULL END))
        ) AS freshness_lag_days,
        ROUND(AVG(CAST(attempt_count AS DOUBLE)), 2) AS avg_attempts_per_day,
        CASE
            WHEN SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) > 0 THEN 'DEGRADED'
            WHEN datediff(
                current_date(),
                to_date(MAX(CASE WHEN status = 'VALID' THEN data_date ELSE NULL END))
            ) > 5 THEN 'BREACHED'
            ELSE 'HEALTHY'
        END AS sla_status,
        CAST(current_timestamp() AS STRING) AS evaluated_at
    FROM {INGESTION_LOG_TABLE}
    GROUP BY source_name
    """
    spark.sql(query)
    print(f"[{SLA_SUMMARY_TABLE}] Updated SLA summary table.")


def build_daily_grid_health(spark: SparkSession) -> None:
    """
    Hợp nhất dữ liệu Thời tiết (Open-Meteo), Thủy điện (EVN Hydro), Phụ tải (NSMO)
    và Báo cáo vận hành (EVN News) theo từng ngày `data_date`.
    """
    required_tables = [
        WEATHER_DAILY_TABLE,
        NSMO_LOAD_DAILY_TABLE,
        HYDRO_DAILY_TABLE,
        EVN_OPS_DAILY_TABLE,
    ]
    if not all(spark.catalog.tableExists(t) for t in required_tables):
        print("[Gold Grid Health] Skipping until all 4 Silver tables exist.")
        return

    query = f"""
    CREATE OR REPLACE TABLE {DAILY_GRID_HEALTH_TABLE}
    USING iceberg
    AS
    WITH w AS (
        SELECT
            data_date,
            ROUND(AVG(temperature_2m_mean), 2) AS national_temp_mean_c,
            ROUND(MAX(temperature_2m_max), 2) AS national_temp_max_c,
            ROUND(SUM(precipitation_sum), 2) AS total_precipitation_mm
        FROM {WEATHER_DAILY_TABLE}
        GROUP BY data_date
    ),
    h AS (
        SELECT
            data_date,
            COUNT(DISTINCT jc_entity_id) AS reported_reservoirs,
            ROUND(AVG(headroom_to_dead_m), 2) AS avg_headroom_to_dead_m,
            ROUND(MIN(headroom_to_dead_m), 2) AS min_headroom_to_dead_m,
            ROUND(SUM(inflow_m3s), 2) AS total_inflow_m3s,
            SUM(COALESCE(open_spillway_gates, 0)) AS total_open_spillway_gates
        FROM {HYDRO_DAILY_TABLE}
        GROUP BY data_date
    )
    SELECT
        COALESCE(w.data_date, h.data_date, n.data_date, e.data_date) AS data_date,
        w.national_temp_mean_c,
        w.national_temp_max_c,
        w.total_precipitation_mm,
        h.reported_reservoirs,
        h.avg_headroom_to_dead_m,
        h.min_headroom_to_dead_m,
        h.total_inflow_m3s,
        h.total_open_spillway_gates,
        n.peak_load_mw AS nsmo_peak_load_mw,
        n.avg_load_mw AS nsmo_avg_load_mw,
        e.peak_capacity_mw AS evn_reported_peak_mw,
        e.daily_energy_million_kwh,
        e.evidence_status AS evn_evidence_status
    FROM w
    FULL OUTER JOIN h ON w.data_date = h.data_date
    FULL OUTER JOIN {NSMO_LOAD_DAILY_TABLE} n
        ON COALESCE(w.data_date, h.data_date) = n.data_date
    FULL OUTER JOIN {EVN_OPS_DAILY_TABLE} e
        ON COALESCE(w.data_date, h.data_date, n.data_date) = e.data_date
    """
    spark.sql(query)
    print(f"[{DAILY_GRID_HEALTH_TABLE}] Updated unified daily grid health table.")


def build_actionable_signals(spark: SparkSession) -> None:
    """
    Tạo bảng Tín hiệu Hành động (Actionable Signals — Chuẩn SalesNow) kèm bằng chứng
    (`evidence_bronze_key`, `confidence_status`, `evidence_summary`):
    1. LOW_RESERVOIR_HEADROOM: Mực nước hồ chứa cách mực nước chết <= 2.0m.
    2. SPILLWAY_FLOOD_DISCHARGE: Hồ chứa đang mở cửa xả lũ (open_spillway_gates > 0).
    3. CROSS_SOURCE_LOAD_DISCREPANCY: Độ lệch công suất đỉnh giữa NSMO API và bài báo EVN > 10%.
    """
    if not spark.catalog.tableExists(HYDRO_DAILY_TABLE):
        print("[Gold Signals] Skipping actionable signals until hydro Silver table exists.")
        return

    cross_source_union = ""
    if (
        spark.catalog.tableExists(NSMO_LOAD_DAILY_TABLE)
        and spark.catalog.tableExists(EVN_OPS_DAILY_TABLE)
    ):
        cross_source_union = f"""
        UNION ALL
        SELECT
            CONCAT('SIG-XCHK-', n.data_date) AS signal_id,
            n.data_date,
            'VN-GRID-NATIONAL' AS jc_entity_id,
            'National Grid' AS entity_name,
            'NATIONAL' AS region_code,
            'CROSS_SOURCE_LOAD_DISCREPANCY' AS signal_type,
            'MEDIUM' AS severity,
            'VERIFIED' AS confidence_status,
            CONCAT(
                'NSMO peak_load_mw=', ROUND(n.peak_load_mw, 1),
                ' vs EVN peak_capacity_mw=', ROUND(e.peak_capacity_mw, 1),
                ' (diff > 10%)'
            ) AS evidence_summary,
            CONCAT(n.bronze_key, '|', e.bronze_key) AS evidence_bronze_key,
            CAST(current_timestamp() AS STRING) AS generated_at
        FROM {NSMO_LOAD_DAILY_TABLE} n
        INNER JOIN {EVN_OPS_DAILY_TABLE} e ON n.data_date = e.data_date
        WHERE n.peak_load_mw IS NOT NULL
          AND e.peak_capacity_mw IS NOT NULL
          AND ABS(n.peak_load_mw - e.peak_capacity_mw) / NULLIF(e.peak_capacity_mw, 0) > 0.10
        """

    query = f"""
    CREATE OR REPLACE TABLE {ACTIONABLE_SIGNALS_TABLE}
    USING iceberg
    AS
    SELECT
        CONCAT('SIG-LOW-', data_date, '-', jc_entity_id) AS signal_id,
        data_date,
        jc_entity_id,
        canonical_name AS entity_name,
        region_code,
        'LOW_RESERVOIR_HEADROOM' AS signal_type,
        CASE WHEN headroom_to_dead_m <= 0.5 THEN 'CRITICAL' ELSE 'HIGH' END AS severity,
        'VERIFIED' AS confidence_status,
        CONCAT(
            'Reservoir ', canonical_name, ' upstream=', upstream_level_m,
            'm is within ', headroom_to_dead_m, 'm of dead level (', dead_level_m, 'm)'
        ) AS evidence_summary,
        bronze_key AS evidence_bronze_key,
        CAST(current_timestamp() AS STRING) AS generated_at
    FROM {HYDRO_DAILY_TABLE}
    WHERE headroom_to_dead_m IS NOT NULL AND headroom_to_dead_m <= 2.0

    UNION ALL

    SELECT
        CONCAT('SIG-FLOOD-', data_date, '-', jc_entity_id) AS signal_id,
        data_date,
        jc_entity_id,
        canonical_name AS entity_name,
        region_code,
        'SPILLWAY_FLOOD_DISCHARGE' AS signal_type,
        CASE WHEN open_spillway_gates >= 3 THEN 'CRITICAL' ELSE 'HIGH' END AS severity,
        'VERIFIED' AS confidence_status,
        CONCAT(
            'Reservoir ', canonical_name, ' opened ', open_spillway_gates,
            ' spillway gate(s), total_discharge=', COALESCE(total_discharge_m3s, 0), ' m3/s'
        ) AS evidence_summary,
        bronze_key AS evidence_bronze_key,
        CAST(current_timestamp() AS STRING) AS generated_at
    FROM {HYDRO_DAILY_TABLE}
    WHERE open_spillway_gates IS NOT NULL AND open_spillway_gates > 0
    {cross_source_union}
    """
    spark.sql(query)
    print(f"[{ACTIONABLE_SIGNALS_TABLE}] Updated actionable signals table.")


def main() -> None:
    spark = None
    try:
        spark = create_spark_session("build_gold_signals")
        build_sla_data_quality_summary(spark)
        build_daily_grid_health(spark)
        build_actionable_signals(spark)
        print("All Gold marts and signals built successfully.")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
