from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType
from IP2Location import IP2Location

DB = "/data/IP-COUNTRY-REGION-CITY.BIN"
ip2location = IP2Location(DB)

def get_country(ip):
    try:
        rec = ip2location.get_all(ip)
        return rec.country_long
    except:
        return None

get_country = udf(get_country, StringType())
def process_dim_store(df, write_fn):
    store_df = df.select("store_id").dropna().dropDuplicates(["store_id"]) \
        .withColumn("country", get_country(col("ip")))
    write_fn(store_df, "dim_store")