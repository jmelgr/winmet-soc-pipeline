import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim

dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(dotenv_path)

SILVER_CONSENSUS_PATH = os.getenv("SILVER_CONSENSUS_PATH")

spark = SparkSession.builder \
    .appName("Consensus_Silver_Transformation") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

input_path = f"{SILVER_CONSENSUS_PATH}/consensus_clean"
df_clean = spark.read.parquet(input_path)

df_transformed = df_clean \
    .withColumn("avclass_lower", lower(trim(col("avclass"))))

df_transformed.printSchema()

df_transformed.show(5, truncate=False)

output_path = f"{SILVER_CONSENSUS_PATH}/consensus_transformed"
df_transformed.write.mode("overwrite").parquet(output_path)

spark.stop()
