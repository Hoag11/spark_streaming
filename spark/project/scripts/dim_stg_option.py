from pyspark.sql.functions import col, explode

def process_dim_stg_option(df, write_fn):
    exploded_df = df.select(
        col("id"),
        explode("option").alias("option")
    ).select(
        col("id"),
        col("option.option_id").alias("option_id"),
        col("option.option_label").alias("option_label")
    ).dropna(subset=["option_id", "option_label"])

    write_fn(exploded_df, "dim_stg_option")
