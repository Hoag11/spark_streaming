from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType, MapType
from util.input_schema import kafka_schema

from scripts.dim_time import process_dim_time
from scripts.dim_user_agent import process_dim_user_agent
from scripts.dim_referrer import process_dim_referrer
from scripts.dim_option import process_dim_option
from scripts.dim_product import process_dim_product
from scripts.dim_store import process_dim_store
from scripts.dim_stg_option import process_dim_stg_option
from scripts.dim_country import process_dim_country
from scripts.dim_city import process_dim_city
from scripts.dim_region import process_dim_region
from scripts.fact_view import process_fact_product_views

from util.config import Config
from util.logger import Log4j

if __name__ == '__main__':
    conf = Config()
    spark_conf = conf.spark_conf
    kaka_conf = conf.kafka_conf
    pg_conf = conf.postgres_conf

    spark = SparkSession.builder \
        .appName("ETL_Streaming") \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"spark_conf: {spark_conf.getAll()}")
    log.info(f"kafka_conf: {kaka_conf.items()}")
    log.info(f"postgres_conf: {pg_conf.items()}")

    df = spark.readStream \
        .format("kafka") \
        .options(**kaka_conf) \
        .load()

    json_df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), kafka_schema).alias("data")) \
        .select("data.*")


    def write_df(df, table_name):
        df.write \
            .format("jdbc") \
            .option("url", pg_conf["jdbc.url"]) \
            .option("dbtable", table_name) \
            .option("user", pg_conf["jdbc.user"]) \
            .option("password", pg_conf["jdbc.password"]) \
            .option("driver", pg_conf["jdbc.driver"]) \
            .mode("overwrite") \
            .save()


    def batching(df, epoch_id):
        print(f"Epoch {epoch_id} - Bắt đầu xử lý...")

        # Xử lý các bảng dim
        process_dim_country(df, write_df)
        process_dim_time(df, write_df)
        process_dim_user_agent(df, write_df)
        process_dim_referrer(df, write_df)
        process_dim_option(df, write_df)
        process_dim_product(df, write_df)
        process_dim_store(df, write_df)
        process_dim_stg_option(df, write_df)
        process_dim_city(df, write_df)
        process_dim_region(df, write_df)

        # Xử lý bảng fact
        process_fact_product_views(df, spark, write_df, pg_conf)

        print(f"Epoch {epoch_id} - Xử lý hoàn tất.")


    # Start stream
    json_df.writeStream \
        .foreachBatch(batching) \
        .outputMode("append") \
        .start() \
        .awaitTermination()
