from pyspark.sql.functions import udf, col, sha2
from pyspark.sql.types import StructType, StructField, StringType
from IP2Location import IP2Location

# Đường dẫn file BIN
DB_PATH = "/data/IP-COUNTRY-REGION-CITY.BIN"
ip2location = IP2Location(DB_PATH)

# UDF để lấy country_short và country_long
def get_country(ip):
    try:
        rec = ip2location.get_all(ip)
        return (rec.country_short, rec.country_long)
    except Exception:
        return (None, None)

# Define UDF với kiểu trả về là StructType
country_schema = StructType([
    StructField("country_short", StringType(), True),
    StructField("country_long", StringType(), True)
])

get_country_udf = udf(get_country, country_schema)

def process_dim_country(df, write_fn):
    dim_df = df.select("ip").dropna().dropDuplicates(["ip"]) \
        .withColumn("country_info", get_country_udf(col("ip"))) \
        .select(
            col("country_info.country_short"),
            col("country_info.country_long"),
            col("ip")
        ) \
        .dropna(subset=["country_short", "country_long"]) \
        .dropDuplicates(["country_short", "country_long"]) \
        .withColumn("country_id", sha2(col("ip"), 256).substr(0, 16).cast("long")) \
        .dropna()

    final_df = dim_df.select("country_id", "country_short", "country_long")

    write_fn(final_df, "dim_country")
