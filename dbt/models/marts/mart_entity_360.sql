{{ config(materialized='table') }}

WITH latest_hydro AS (
    SELECT
        jc_entity_id,
        data_date,
        upstream_level_m,
        normal_level_m,
        dead_level_m,
        headroom_to_dead_m,
        inflow_m3s,
        total_discharge_m3s,
        open_spillway_gates,
        bronze_key,
        ROW_NUMBER() OVER (PARTITION BY jc_entity_id ORDER BY data_date DESC) AS rn
    FROM {{ source('silver', 'hydro_reservoir_daily') }}
),
current_hydro AS (
    SELECT *
    FROM latest_hydro
    WHERE rn = 1
)
SELECT
    e.jc_entity_id,
    e.entity_type,
    e.canonical_name,
    e.province_name,
    e.region_code,
    e.designed_capacity_mw,
    h.data_date AS latest_observation_date,
    h.upstream_level_m,
    h.normal_level_m,
    h.dead_level_m,
    h.headroom_to_dead_m,
    h.inflow_m3s,
    h.total_discharge_m3s,
    h.open_spillway_gates,
    w.temperature_2m_max AS province_temp_max_c,
    w.precipitation_sum AS province_precipitation_mm,
    CASE
        WHEN h.open_spillway_gates > 0 THEN 'FLOOD_DISCHARGE_ACTIVE'
        WHEN h.headroom_to_dead_m IS NOT NULL AND h.headroom_to_dead_m <= 2.0 THEN 'LOW_WATER_ALERT'
        ELSE 'NORMAL_OPERATION'
    END AS operational_signal,
    h.bronze_key AS evidence_bronze_key
FROM {{ source('silver', 'dim_grid_entities') }} e
LEFT JOIN current_hydro h
    ON e.jc_entity_id = h.jc_entity_id
LEFT JOIN {{ source('silver', 'weather_daily') }} w
    ON e.province_name = w.location_name
   AND h.data_date = w.data_date
