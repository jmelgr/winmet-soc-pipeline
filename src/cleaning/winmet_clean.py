import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, MapType
from pyspark.sql.functions import col

dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(dotenv_path)

BRONZE_WINMET_PATH = os.getenv("BRONZE_WINMET_PATH")
SILVER_WINMET_PATH = os.getenv("SILVER_WINMET_PATH")

spark = SparkSession.builder \
    .appName("WinMET_Silver_Clean") \
    .config("spark.driver.memory", "10g") \
    .config("spark.executor.memory", "10g") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

telemetry_schema = StructType([
    StructField("target", StructType([
        StructField("file", StructType([
            StructField("sha256", StringType(), True),
            StructField("name", StringType(), True),
            StructField("size", LongType(), True)
        ]), True)
    ]), True),
    StructField("behavior", StructType([
        StructField("processes", ArrayType(
            StructType([
                StructField("process_name", StringType(), True),
                StructField("pid", LongType(), True),
                StructField("command_line", StringType(), True)
            ])
        ), True),
        StructField("summary", MapType(StringType(), ArrayType(StringType())), True)
    ]), True)
])

df_winmet = spark.read \
    .schema(telemetry_schema) \
    .option("multiline", "true") \
    .json(BRONZE_WINMET_PATH)

df_prepared = df_winmet \
    .withColumn("sha256", col("target.file.sha256")) \
    .withColumn("file_size", col("target.file.size"))

df_winmet_clean = df_prepared \
    .dropna(subset=["sha256"]) \
    .dropDuplicates(["sha256"]) \
    .filter((col("file_size") > 0) & (col("file_size") < 500 * 1024 * 1024))

path = f"{SILVER_WINMET_PATH}/winmet_clean"
df_winmet_clean.write.mode("overwrite").parquet(path)

spark.stop()
