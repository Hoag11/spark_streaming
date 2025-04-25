from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from user_agents import parse
from pyspark.sql.functions import sha2


def extract_browser(user_agent_str):
    try:
        ua = parse(user_agent_str)
        return ua.browser.family
    except:
        return None

def extract_os(user_agent_str):
    try:
        ua = parse(user_agent_str)
        return ua.os.family
    except:
        return None

extract_browser_udf = udf(extract_browser, StringType())
extract_os_udf = udf(extract_os, StringType())

def process_dim_user_agent(df, write_fn):
    dim_df = df.select("user_agent") \
        .dropna(subset=["user_agent"]) \
        .dropDuplicates(["user_agent"]) \
        .withColumn("user_agent_id", sha2(col("user_agent"), 256).substr(0, 8).cast("long"))

    dim_df = dim_df \
        .withColumn("browser", extract_browser_udf(col("user_agent"))) \
        .withColumn("os", extract_os_udf(col("user_agent"))) \
        .dropna(subset=["browser", "os"])

    result_df = dim_df.select("user_agent_id", "user_agent", "browser", "os")

    write_fn(result_df, "dim_user_agent")
