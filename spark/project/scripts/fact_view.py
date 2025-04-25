from pyspark.sql.functions import col, sha2, substring, unix_timestamp, to_timestamp, lit

def process_fact_product_views(df, spark, write_fn, pg_conf):
    df = df.withColumn("timestamp", to_timestamp(col("local_time"), "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("time_id", unix_timestamp("timestamp").cast("int")) \
           .withColumn("product_id", col("product_id").cast("string")) \
           .withColumn("store_id", col("store_id").cast("string")) \
           .withColumn("user_agent_id", substring(sha2(col("user_agent").cast("binary"), 256), 0, 8).cast("int")) \
           .withColumn("referrer_id", substring(sha2(col("referrer_url").cast("binary"), 256), 0, 8).cast("int")) \
           .withColumn("view_count", lit(1)) \
           .withColumn("country_id", sha2(col("ip"), 256).substr(0, 8).cast("long")) \
           .withColumn("region_id", sha2(col("ip"), 256).substr(0, 8).cast("long")) \
           .withColumn("city_id", sha2(col("ip"), 256).substr(0, 8).cast("long"))
    fact_df = df.filter("id IS NOT NULL AND time_id IS NOT NULL AND product_id IS NOT NULL AND store_id IS NOT NULL") \
                .dropDuplicates(["id"])

    fact_final = fact_df.select(
        col("id").alias("log_id"),
        "time_id",
        "product_id",
        "store_id",
        "device_id",
        "email",
        "ip",
        "referrer_id",
        "user_agent_id",
        "view_count",
        "country_id",
        "region_id",
        "city_id"
    )

    write_fn(fact_final, "fact_view")