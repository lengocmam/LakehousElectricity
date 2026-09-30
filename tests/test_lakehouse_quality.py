import json
import sys
import unittest
from datetime import date
from pathlib import Path

# Thêm scripts/spark vào PYTHONPATH để chạy unit test độc lập không cần cụm Spark
SPARK_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts" / "spark"
if str(SPARK_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_SCRIPTS_DIR))

from bronze.open_meteo import LOCATIONS, validate_open_meteo_raw
from bronze.nsmo import validate_nsmo_raw
from bronze.hydro import validate_hydro_raw
from silver.silver_utils import resolve_reservoir_entity
from silver.llm_evidence_judge import verify_extraction_with_evidence


class TestLakehouseDataQualityAndEntityResolution(unittest.TestCase):

    def test_open_meteo_validation_distinguishes_valid_and_empty_days(self):
        day1 = date(2026, 9, 20)
        day2 = date(2026, 9, 21)
        times = (
            [f"2026-09-20T{h:02d}:00" for h in range(24)]
            + [f"2026-09-21T{h:02d}:00" for h in range(24)]
        )
        # Ngày 1 đủ 24h (28.5C), Ngày 2 bị null toàn bộ (mô phỏng trễ 3-5 ngày của Archive API)
        temps = [28.5] * 24 + [None] * 24
        payload = [
            {"hourly": {"time": times, "temperature_2m": temps}}
            for _ in range(len(LOCATIONS))
        ]
        logs = validate_open_meteo_raw(
            raw_text=json.dumps(payload),
            window_start_date=day1,
            window_end_date=day2,
            bronze_key="batch_001",
            batch_id="20260930000000",
            checked_at="2026-09-30T00:00:00Z",
        )
        self.assertEqual(len(logs), 2)
        self.assertEqual(logs[0]["data_date"], "2026-09-20")
        self.assertEqual(logs[0]["status"], "VALID")
        self.assertEqual(logs[0]["actual_items"], 816)

        self.assertEqual(logs[1]["data_date"], "2026-09-21")
        self.assertEqual(logs[1]["status"], "EMPTY")
        self.assertEqual(logs[1]["actual_items"], 0)
        self.assertIsNone(logs[1]["bronze_key"])

    def test_nsmo_validation_catches_empty_load_array(self):
        empty_json = json.dumps({"result": {"data": []}})
        res_empty = validate_nsmo_raw(
            raw_text=empty_json,
            data_date="2023-01-05",
            bronze_key="b_nsmo_1",
            batch_id="20260930",
            checked_at="2026-09-30T00:00:00Z",
        )
        self.assertEqual(res_empty["status"], "EMPTY")
        self.assertEqual(res_empty["actual_items"], 0)

        valid_json = json.dumps({
            "result": {
                "data": [{"hour": h, "load_mw": 35000.0 + h * 100} for h in range(1, 25)]
            }
        })
        res_valid = validate_nsmo_raw(
            raw_text=valid_json,
            data_date="2026-09-25",
            bronze_key="b_nsmo_2",
            batch_id="20260930",
            checked_at="2026-09-30T00:00:00Z",
        )
        self.assertEqual(res_valid["status"], "VALID")
        self.assertEqual(res_valid["actual_items"], 24)

    def test_hydro_validation_detects_kxd_vs_valid_reservoirs(self):
        kxd_rows = "".join(
            f"<tr><td>Hồ {i}</td><td>07:00</td><td>KXD</td><td>KXD</td><td>KXD</td><td>KXD</td></tr>"
            for i in range(35)
        )
        res_empty = validate_hydro_raw(
            raw_content=f"<table>{kxd_rows}</table>".encode("utf-8"),
            data_date="2026-09-20",
            bronze_key="b_hydro_1",
            batch_id="20260930",
            checked_at="2026-09-30T00:00:00Z",
        )
        self.assertEqual(res_empty["status"], "EMPTY")

        valid_rows = "".join(
            f"<tr><td>Hồ {i}</td><td>23:00</td><td>215,0</td><td>215,0</td><td>175,0</td><td>850,5</td></tr>"
            for i in range(32)
        )
        res_valid = validate_hydro_raw(
            raw_content=f"<table>{valid_rows}</table>".encode("utf-8"),
            data_date="2026-09-21",
            bronze_key="b_hydro_2",
            batch_id="20260930",
            checked_at="2026-09-30T00:00:00Z",
        )
        self.assertEqual(res_valid["status"], "VALID")
        self.assertEqual(res_valid["actual_items"], 32)

    def test_entity_resolution_maps_reservoirs_to_jc_entity_id(self):
        son_la = resolve_reservoir_entity("Hồ chứa Thủy điện Sơn La")
        self.assertEqual(son_la["jc_entity_id"], "VN-RES-SL01")
        self.assertEqual(son_la["province_name"], "Son La")
        self.assertEqual(son_la["region_code"], "NORTH")

        ialy = resolve_reservoir_entity("NMTĐ Yaly")
        self.assertEqual(ialy["jc_entity_id"], "VN-RES-IL01")
        self.assertEqual(ialy["province_name"], "Gia Lai")
        self.assertEqual(ialy["region_code"], "CENTRAL")

    def test_evidence_judge_rejects_unverifiable_or_out_of_bound_metrics(self):
        article = (
            "Ngày 25/09/2026, công suất lớn nhất của hệ thống điện quốc gia đạt 45.200 MW, "
            "sản lượng tiêu thụ ngày đạt 915,4 triệu kWh."
        )
        status_ok, _ = verify_extraction_with_evidence(
            raw_text=article,
            peak_mw=45200.0,
            daily_kwh=915.4,
            evidence_quote="công suất lớn nhất của hệ thống điện quốc gia đạt 45.200 MW",
        )
        self.assertEqual(status_ok, "VERIFIED")

        # Trích dẫn không có thật trong bài viết -> bắt buộc đánh dấu UNVERIFIABLE
        status_fake, _ = verify_extraction_with_evidence(
            raw_text=article,
            peak_mw=45200.0,
            daily_kwh=915.4,
            evidence_quote="đoạn trích dẫn do AI tự suy diễn không có trong bài báo",
        )
        self.assertEqual(status_fake, "UNVERIFIABLE")


if __name__ == "__main__":
    unittest.main()
