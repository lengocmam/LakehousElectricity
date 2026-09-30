{{ config(materialized='table') }}

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
    ROUND(AVG(unit_cost_ms_per_item), 3) AS avg_unit_cost_ms_per_item,
    ROUND(SUM(COALESCE(payload_bytes, 0)) / 1048576.0, 2) AS total_payload_mb
FROM {{ source('bronze', 'ingestion_log') }}
GROUP BY source_name
