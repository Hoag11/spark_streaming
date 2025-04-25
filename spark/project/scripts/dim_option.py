from pyspark.sql.functions import col, explode

def process_dim_option(df, write_fn):
    option_df = df \
        .select("option") \
        .withColumn("option", explode("option")) \
        .select(
            col("option.option_id").alias("option_id"),
            col("option.option_label").alias("option_label")
        ) \
        .dropDuplicates(["option_id"]).dropna()

    write_fn(option_df, "dim_option")
