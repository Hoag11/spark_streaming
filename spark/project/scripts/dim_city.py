from pyspark.sql.functions import col, udf
from pyspark.sql.functions import sha2, conv, substring
from pyspark.sql.types import StringType
from IP2Location import IP2Location

DB = "/data/IP-COUNTRY-REGION-CITY.BIN"
ip2location = IP2Location(DB)

def get_city(ip):
    rec = ip2location.get_all(ip)
    return rec.city

get_city_udf = udf(get_city, StringType())

def process_dim_city(df, write_fn):
    dim_df = df.select(
        col("ip")
    ).dropDuplicates(["ip"]) \
        .withColumn("city", get_city_udf(col("ip"))) \
        .dropDuplicates(["city"]) \
        .dropna(subset=["city"])

    dim_df = dim_df.withColumn("city_id", sha2(col("ip"), 256).substr(0, 8).cast("long")) \
    .dropna()
    df = dim_df.select("city_id", "city")

    write_fn(df, "dim_city")