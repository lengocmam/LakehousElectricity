import re
import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from bronze.bronze_utils import INGESTION_LOG_TABLE, ensure_ingestion_log_table


SILVER_NAMESPACE = "nessie.silver"
DIM_ENTITY_TABLE = f"{SILVER_NAMESPACE}.dim_grid_entities"

# Bảng đối chiếu Entity Resolution (tương tự mã JC của SalesNow):
# Liên kết Tên hồ chứa thủy điện (evn_hydro / evn) <-> Tỉnh thành (open_meteo) <-> Miền điện lực (nsmo / evn)
CANONICAL_ENTITIES = [
    # Miền Bắc (NORTH)
    {"jc_entity_id": "VN-RES-SL01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Sơn La", "aliases": "son la|thuy dien son la|ho son la", "province_name": "Son La", "region_code": "NORTH", "designed_capacity_mw": 2400.0},
    {"jc_entity_id": "VN-RES-HB01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Hòa Bình", "aliases": "hoa binh|thuy dien hoa binh|ho hoa binh", "province_name": "Phu Tho", "region_code": "NORTH", "designed_capacity_mw": 1920.0},
    {"jc_entity_id": "VN-RES-LC01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Lai Châu", "aliases": "lai chau|thuy dien lai chau|ho lai chau", "province_name": "Lai Chau", "region_code": "NORTH", "designed_capacity_mw": 1200.0},
    {"jc_entity_id": "VN-RES-TQ01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Tuyên Quang", "aliases": "tuyen quang|thuy dien tuyen quang|ho tuyen quang", "province_name": "Tuyen Quang", "region_code": "NORTH", "designed_capacity_mw": 342.0},
    {"jc_entity_id": "VN-RES-TB01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Thác Bà", "aliases": "thac ba|thuy dien thac ba|ho thac ba", "province_name": "Lao Cai", "region_code": "NORTH", "designed_capacity_mw": 120.0},
    {"jc_entity_id": "VN-RES-BC01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Bản Chát", "aliases": "ban chat|thuy dien ban chat", "province_name": "Lai Chau", "region_code": "NORTH", "designed_capacity_mw": 220.0},
    {"jc_entity_id": "VN-RES-HQ01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Huội Quảng", "aliases": "huoi quang|thuy dien huoi quang", "province_name": "Lai Chau", "region_code": "NORTH", "designed_capacity_mw": 520.0},
    {"jc_entity_id": "VN-RES-TS01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Trung Sơn", "aliases": "trung son|thuy dien trung son", "province_name": "Thanh Hoa", "region_code": "NORTH", "designed_capacity_mw": 260.0},
    {"jc_entity_id": "VN-RES-BV01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Bản Vẽ", "aliases": "ban ve|thuy dien ban ve", "province_name": "Nghe An", "region_code": "NORTH", "designed_capacity_mw": 320.0},

    # Miền Trung & Tây Nguyên (CENTRAL)
    {"jc_entity_id": "VN-RES-IL01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Ialy", "aliases": "ialy|yaly|thuy dien ialy", "province_name": "Gia Lai", "region_code": "CENTRAL", "designed_capacity_mw": 720.0},
    {"jc_entity_id": "VN-RES-SS04", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Sê San 4", "aliases": "se san 4|sesan 4|thuy dien se san 4", "province_name": "Gia Lai", "region_code": "CENTRAL", "designed_capacity_mw": 360.0},
    {"jc_entity_id": "VN-RES-AV01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "A Vương", "aliases": "a vuong|thuy dien a vuong", "province_name": "Da Nang", "region_code": "CENTRAL", "designed_capacity_mw": 210.0},
    {"jc_entity_id": "VN-RES-SB04", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Sông Bung 4", "aliases": "song bung 4|thuy dien song bung 4", "province_name": "Da Nang", "region_code": "CENTRAL", "designed_capacity_mw": 156.0},
    {"jc_entity_id": "VN-RES-ST02", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Sông Tranh 2", "aliases": "song tranh 2|thuy dien song tranh 2", "province_name": "Da Nang", "region_code": "CENTRAL", "designed_capacity_mw": 190.0},
    {"jc_entity_id": "VN-RES-DD01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Đắk Đrinh", "aliases": "dak drinh|dakdrinh|thuy dien dak drinh", "province_name": "Quang Ngai", "region_code": "CENTRAL", "designed_capacity_mw": 125.0},
    {"jc_entity_id": "VN-RES-BH01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Bình Điền", "aliases": "binh dien|thuy dien binh dien", "province_name": "Hue", "region_code": "CENTRAL", "designed_capacity_mw": 44.0},
    {"jc_entity_id": "VN-RES-HP01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Hương Điền", "aliases": "huong dien|thuy dien huong dien", "province_name": "Hue", "region_code": "CENTRAL", "designed_capacity_mw": 81.0},
    {"jc_entity_id": "VN-RES-SB01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Sông Ba Hạ", "aliases": "song ba ha|thuy dien song ba ha", "province_name": "Dak Lak", "region_code": "CENTRAL", "designed_capacity_mw": 220.0},

    # Miền Nam (SOUTH)
    {"jc_entity_id": "VN-RES-TA01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Trị An", "aliases": "tri an|thuy dien tri an|ho tri an", "province_name": "Dong Nai", "region_code": "SOUTH", "designed_capacity_mw": 400.0},
    {"jc_entity_id": "VN-RES-DN01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Đa Nhim", "aliases": "da nhim|thuy dien da nhim", "province_name": "Lam Dong", "region_code": "SOUTH", "designed_capacity_mw": 160.0},
    {"jc_entity_id": "VN-RES-HT01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Hàm Thuận", "aliases": "ham thuan|thuy dien ham thuan", "province_name": "Lam Dong", "region_code": "SOUTH", "designed_capacity_mw": 300.0},
    {"jc_entity_id": "VN-RES-DT01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Đại Ninh", "aliases": "dai ninh|thuy dien dai ninh", "province_name": "Lam Dong", "region_code": "SOUTH", "designed_capacity_mw": 300.0},
    {"jc_entity_id": "VN-RES-DN03", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Đồng Nai 3", "aliases": "dong nai 3|thuy dien dong nai 3", "province_name": "Lam Dong", "region_code": "SOUTH", "designed_capacity_mw": 180.0},
    {"jc_entity_id": "VN-RES-DN04", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Đồng Nai 4", "aliases": "dong nai 4|thuy dien dong nai 4", "province_name": "Lam Dong", "region_code": "SOUTH", "designed_capacity_mw": 340.0},
    {"jc_entity_id": "VN-RES-TM01", "entity_type": "HYDRO_RESERVOIR", "canonical_name": "Thác Mơ", "aliases": "thac mo|thuy dien thac mo", "province_name": "Dong Nai", "region_code": "SOUTH", "designed_capacity_mw": 150.0},
]

PROVINCE_TO_REGION = {
    "Ha Noi": "NORTH", "Cao Bang": "NORTH", "Tuyen Quang": "NORTH", "Dien Bien": "NORTH",
    "Lai Chau": "NORTH", "Son La": "NORTH", "Lao Cai": "NORTH", "Thai Nguyen": "NORTH",
    "Lang Son": "NORTH", "Quang Ninh": "NORTH", "Bac Ninh": "NORTH", "Phu Tho": "NORTH",
    "Hung Yen": "NORTH", "Hai Phong": "NORTH", "Ninh Binh": "NORTH", "Thanh Hoa": "NORTH",
    "Nghe An": "NORTH", "Ha Tinh": "NORTH",
    "Quang Tri": "CENTRAL", "Hue": "CENTRAL", "Da Nang": "CENTRAL", "Quang Ngai": "CENTRAL",
    "Gia Lai": "CENTRAL", "Khanh Hoa": "CENTRAL", "Lam Dong": "CENTRAL", "Dak Lak": "CENTRAL",
    "Dong Nai": "SOUTH", "Ho Chi Minh City": "SOUTH", "Tay Ninh": "SOUTH", "Can Tho": "SOUTH",
    "Vinh Long": "SOUTH", "Dong Thap": "SOUTH", "An Giang": "SOUTH", "Ca Mau": "SOUTH",
}

DIM_ENTITY_SCHEMA = StructType([
    StructField("jc_entity_id", StringType(), False),
    StructField("entity_type", StringType(), False),
    StructField("canonical_name", StringType(), False),
    StructField("aliases", StringType(), False),
    StructField("province_name", StringType(), False),
    StructField("region_code", StringType(), False),
    StructField("designed_capacity_mw", DoubleType(), True),
])


def normalize_vietnamese_text(text: str) -> str:
    """
    Chuẩn hóa chuỗi tiếng Việt (bỏ dấu, chuyển chữ thường, xóa tiền tố 'hồ', 'thủy điện')
    phục vụ Entity Resolution giữa các nguồn có quy ước đặt tên khác nhau.
    """
    if not text:
        return ""
    s = text.strip().lower()
    s = s.replace("đ", "d")
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"^(ho\s+chua|ho|nha\s+may\s+thuy\s+dien|thuy\s+dien|nmtd)\s+", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def resolve_reservoir_entity(raw_name: str) -> dict:
    """
    Đối sánh tên hồ chứa thô từ bảng HTML EVN với mã định danh chuẩn `jc_entity_id`.
    Nếu chưa có trong danh mục chuẩn, sinh mã định danh tạm thời với hậu tố UNMAPPED
    để không làm rơi bản ghi nhưng vẫn gắn cờ kiểm chứng.
    """
    norm = normalize_vietnamese_text(raw_name)
    for entity in CANONICAL_ENTITIES:
        alias_list = [a.strip() for a in entity["aliases"].split("|") if a.strip()]
        if norm in alias_list or any(alias in norm for alias in alias_list):
            return entity

    slug = re.sub(r"\s+", "_", norm).upper()[:12] or "UNKNOWN"
    return {
        "jc_entity_id": f"VN-RES-UNMAPPED-{slug}",
        "entity_type": "HYDRO_RESERVOIR",
        "canonical_name": raw_name.strip(),
        "aliases": norm,
        "province_name": "UNMAPPED",
        "region_code": "UNMAPPED",
        "designed_capacity_mw": None,
    }


def ensure_entity_dimension(spark: SparkSession) -> None:
    """
    Khởi tạo và đồng bộ bảng Master Entity Resolution `nessie.silver.dim_grid_entities`.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")
    df = spark.createDataFrame(CANONICAL_ENTITIES, schema=DIM_ENTITY_SCHEMA)
    if spark.catalog.tableExists(DIM_ENTITY_TABLE):
        df.createOrReplaceTempView("tmp_dim_grid_entities")
        spark.sql(
            f"""
            MERGE INTO {DIM_ENTITY_TABLE} AS t
            USING tmp_dim_grid_entities AS s
            ON t.jc_entity_id = s.jc_entity_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    else:
        df.writeTo(DIM_ENTITY_TABLE).using("iceberg").create()


def get_valid_bronze_keys_to_process(
    spark: SparkSession,
    source_name: str,
    silver_table: str | None = None,
    silver_date_col: str = "data_date",
) -> dict[str, str]:
    """
    Quality Gatekeeper Pattern (Chuẩn SalesNow):
    - Chỉ đọc các bản ghi có `status = 'VALID'` và `bronze_key IS NOT NULL`
      từ bảng `nessie.bronze.ingestion_log`.
    - Nếu bảng Silver đã tồn tại, loại bỏ những `(data_date, bronze_key)` đã được
      chuyển đổi thành công ở lần chạy trước để đảm bảo tính Incremental & Idempotent.
    Trả về dict: `{data_date: bronze_key}`.
    """
    ensure_ingestion_log_table(spark)

    valid_rows = (
        spark.table(INGESTION_LOG_TABLE)
        .filter(
            f"source_name = '{source_name}' "
            f"AND status = 'VALID' "
            f"AND bronze_key IS NOT NULL"
        )
        .select("data_date", "bronze_key")
        .collect()
    )

    valid_map = {
        str(r["data_date"]): str(r["bronze_key"])
        for r in valid_rows
        if r["data_date"] and r["bronze_key"]
    }

    if not valid_map or not silver_table or not spark.catalog.tableExists(silver_table):
        return valid_map

    processed_rows = (
        spark.table(silver_table)
        .select(silver_date_col, "bronze_key")
        .distinct()
        .collect()
    )
    processed_pairs = {
        (str(r[silver_date_col]), str(r["bronze_key"]))
        for r in processed_rows
        if r[silver_date_col] and r["bronze_key"]
    }

    return {
        d_str: b_key
        for d_str, b_key in valid_map.items()
        if (d_str, b_key) not in processed_pairs
    }
