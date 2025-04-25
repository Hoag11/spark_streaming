from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from IP2Location import IP2Location
from pyspark.sql.functions import sha2


DB = "/data/IP-COUNTRY-REGION-CITY.BIN"
ip2location = IP2Location(DB)

def get_region(ip):
    try:
        rec = ip2location.get_all(ip)
        return rec.region
    except:
        return None


get_region_udf = udf(get_region, StringType())

def process_dim_region(df, write_fn):
    dim_df = df.select(
        col("ip")). \
        dropDuplicates(["ip"]) \
        .dropna(subset=["ip"]) \
        .withColumn("region", get_region_udf(col("ip"))) \

    dim_df = dim_df.withColumn("region_id", sha2(col("ip"), 256).substr(0, 8).cast("long")) \
    .dropna()

    df = dim_df.select("region_id", "region")

    write_fn(df, "dim_region")
