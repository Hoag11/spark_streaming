from pyspark.sql.functions import to_timestamp, unix_timestamp, col, hour, dayofmonth, month, year, date_format

def process_dim_time(df, write_fn):
    time_df = df.withColumn("timestamp", to_timestamp("local_time", "yyyy-MM-dd HH:mm:ss")) \
                .withColumn("time_id", unix_timestamp("timestamp").cast("int")) \
                .withColumn("date", date_format("timestamp", "yyyy-MM-dd")) \
                .withColumn("hour", hour("timestamp")) \
                .withColumn("day", dayofmonth("timestamp")) \
                .withColumn("month", month("timestamp")) \
                .withColumn("year", year("timestamp")) \
                .withColumn("weekday", date_format("timestamp", "E"))
    write_fn(time_df.select("time_id", "timestamp", "date", "day", "month", "year", "hour", "weekday"), "dim_time")