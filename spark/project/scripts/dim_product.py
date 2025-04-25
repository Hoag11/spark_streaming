from pyspark.sql.functions import col

def process_dim_product(df, write_fn):
    product_df = df.select(col("product_id")).dropna().dropDuplicates()
    write_fn(product_df, "dim_product")