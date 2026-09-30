"""
FastAPI Data Serving API & Model Context Protocol (MCP) Server for ElectricityLakehouse.
Cho phép cả ứng dụng nghiệp vụ lẫn AI Agents (Claude, Cursor) truy vấn trực tiếp:
1. SLA Chất lượng dữ liệu (nessie.gold.sla_data_quality_summary & nessie.bronze.ingestion_log)
2. Tín hiệu hành động kèm bằng chứng (nessie.gold.fact_actionable_signals)
3. Hồ sơ thực thể hợp nhất theo mã định danh chuẩn jc_entity_id (Entity Resolution)
"""

import os
from typing import Any
import requests
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel


TRINO_BASE_URL = os.getenv("TRINO_BASE_URL", "http://localhost:8082")
TRINO_USER = os.getenv("TRINO_USER", "trino")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")

app = FastAPI(
    title="Electricity Lakehouse Data API & MCP Server",
    description=(
        "Serving Layer & Model Context Protocol (MCP) cho phép AI Agents (Claude/Cursor) "
        "truy vấn dữ liệu vận hành hệ thống điện, hồ sơ thực thể (jc_entity_id) và bằng chứng kiểm chứng."
    ),
    version="1.0.0",
)


def execute_trino_sql(sql: str) -> list[dict[str, Any]]:
    """
    Thực thi truy vấn SQL trực tiếp qua Trino HTTP REST API (/v1/statement)
    và trả về danh sách bản ghi dưới dạng dictionary.
    """
    headers = {
        "X-Trino-User": TRINO_USER,
        "X-Trino-Catalog": TRINO_CATALOG,
    }
    resp = requests.post(
        f"{TRINO_BASE_URL}/v1/statement",
        data=sql.encode("utf-8"),
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()

    columns: list[str] = []
    rows: list[list[Any]] = []

    while True:
        if "error" in payload:
            raise RuntimeError(payload["error"].get("message", "Trino query failed"))
        if "columns" in payload and not columns:
            columns = [col["name"] for col in payload["columns"]]
        if "data" in payload and payload["data"]:
            rows.extend(payload["data"])
        next_uri = payload.get("nextUri")
        if not next_uri:
            break
        resp = requests.get(next_uri, headers=headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

    return [dict(zip(columns, row)) for row in rows]


@app.get("/api/v1/sla")
def get_data_quality_sla() -> dict[str, Any]:
    """
    Trả về bảng SLA Chất lượng dữ liệu của cả 4 nguồn (Độ đầy đủ, Độ mới, Tỷ lệ EXHAUSTED).
    """
    sql = """
    SELECT
        source_name,
        total_tracked_days,
        valid_days,
        incomplete_days,
        empty_days,
        failed_days,
        exhausted_days,
        completeness_sla_pct,
        latest_valid_date,
        freshness_lag_days,
        sla_status,
        evaluated_at
    FROM iceberg.gold.sla_data_quality_summary
    ORDER BY source_name
    """
    try:
        data = execute_trino_sql(sql)
        return {"count": len(data), "sla_metrics": data}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/v1/signals")
def get_actionable_signals(
    severity: str | None = Query(default=None, description="CRITICAL | HIGH | MEDIUM"),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    """
    Trả về danh sách Tín hiệu Hành động (Actionable Signals) kèm bằng chứng
    (`evidence_bronze_key`, `confidence_status`, `evidence_summary`).
    """
    where_clause = f"WHERE severity = '{severity.upper()}'" if severity else ""
    sql = f"""
    SELECT
        signal_id,
        data_date,
        jc_entity_id,
        entity_name,
        region_code,
        signal_type,
        severity,
        confidence_status,
        evidence_summary,
        evidence_bronze_key,
        generated_at
    FROM iceberg.gold.fact_actionable_signals
    {where_clause}
    ORDER BY data_date DESC, severity ASC
    LIMIT {int(limit)}
    """
    try:
        signals = execute_trino_sql(sql)
        return {"count": len(signals), "signals": signals}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/v1/entities/{jc_entity_id}")
def get_entity_profile(jc_entity_id: str) -> dict[str, Any]:
    """
    Trả về Hồ sơ Thực thể hợp nhất (Entity Resolution Profile) theo mã `jc_entity_id`
    (ví dụ: `VN-RES-SL01` cho Thủy điện Sơn La), kết hợp số liệu mực nước hồ chứa mới nhất
    và thời tiết tỉnh tương ứng.
    """
    safe_id = jc_entity_id.replace("'", "")
    sql = f"""
    SELECT
        e.jc_entity_id,
        e.entity_type,
        e.canonical_name,
        e.province_name,
        e.region_code,
        e.designed_capacity_mw,
        h.data_date,
        h.upstream_level_m,
        h.normal_level_m,
        h.dead_level_m,
        h.headroom_to_dead_m,
        h.inflow_m3s,
        h.total_discharge_m3s,
        h.open_spillway_gates,
        w.temperature_2m_max,
        w.precipitation_sum,
        h.bronze_key AS hydro_evidence_key
    FROM iceberg.silver.dim_grid_entities e
    LEFT JOIN iceberg.silver.hydro_reservoir_daily h
        ON e.jc_entity_id = h.jc_entity_id
    LEFT JOIN iceberg.silver.weather_daily w
        ON e.province_name = w.location_name AND h.data_date = w.data_date
    WHERE e.jc_entity_id = '{safe_id}'
    ORDER BY h.data_date DESC
    LIMIT 14
    """
    try:
        rows = execute_trino_sql(sql)
        if not rows:
            raise HTTPException(status_code=404, detail=f"Entity {jc_entity_id} not found")
        return {
            "jc_entity_id": jc_entity_id,
            "canonical_name": rows[0]["canonical_name"],
            "province_name": rows[0]["province_name"],
            "region_code": rows[0]["region_code"],
            "recent_observations": rows,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: int | str | None = None
    method: str
    params: dict[str, Any] | None = None


@app.post("/mcp")
def handle_mcp_request(req: MCPRequest) -> dict[str, Any]:
    """
    Endpoint chuẩn Model Context Protocol (MCP) cho phép AI Agents (Claude, Cursor)
    khám phá công cụ (`tools/list`) và gọi truy vấn dữ liệu kèm bằng chứng (`tools/call`).
    """
    if req.method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": req.id,
            "result": {
                "tools": [
                    {
                        "name": "get_data_quality_sla",
                        "description": "Truy vấn SLA chất lượng dữ liệu (VALID, INCOMPLETE, EMPTY, EXHAUSTED) của 4 nguồn Lakehouse.",
                        "inputSchema": {"type": "object", "properties": {}},
                    },
                    {
                        "name": "get_actionable_grid_signals",
                        "description": "Lấy danh sách tín hiệu cảnh báo vận hành điện & hồ chứa kèm khóa bằng chứng (evidence_bronze_key).",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "severity": {"type": "string", "enum": ["CRITICAL", "HIGH", "MEDIUM"]},
                                "limit": {"type": "integer", "default": 20},
                            },
                        },
                    },
                    {
                        "name": "get_entity_profile",
                        "description": "Tra cứu hồ sơ thực thể hợp nhất (Entity Resolution) theo mã jc_entity_id (VD: VN-RES-SL01).",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "jc_entity_id": {"type": "string"},
                            },
                            "required": ["jc_entity_id"],
                        },
                    },
                ]
            },
        }

    if req.method == "tools/call":
        params = req.params or {}
        tool_name = params.get("name")
        args = params.get("arguments") or {}

        if tool_name == "get_data_quality_sla":
            data = get_data_quality_sla()
        elif tool_name == "get_actionable_grid_signals":
            data = get_actionable_signals(
                severity=args.get("severity"),
                limit=int(args.get("limit", 20)),
            )
        elif tool_name == "get_entity_profile":
            data = get_entity_profile(jc_entity_id=str(args["jc_entity_id"]))
        else:
            return {
                "jsonrpc": "2.0",
                "id": req.id,
                "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"},
            }

        return {
            "jsonrpc": "2.0",
            "id": req.id,
            "result": {"content": [{"type": "text", "text": str(data)}]},
        }

    return {
        "jsonrpc": "2.0",
        "id": req.id,
        "error": {"code": -32601, "message": f"Method not supported: {req.method}"},
    }
