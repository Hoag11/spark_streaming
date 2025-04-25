from pyspark.sql.functions import sha2, col

def process_dim_referrer(df, write_fn):
    ref_df = df.select("referrer_url").dropna().dropDuplicates(["referrer_url"]) \
        .withColumn("referrer_id", sha2(col("referrer_url"), 256).substr(0, 8).cast("long")) \
        .dropna()
    write_fn(ref_df.select("referrer_id", "referrer_url"), "dim_referrer")