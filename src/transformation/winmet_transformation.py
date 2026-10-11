import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim

dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(dotenv_path)

SILVER_WINMET_PATH = os.getenv("SILVER_WINMET_PATH")

spark = SparkSession.builder \
    .appName("WinMET_Silver_Transformation_Control") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

input_path = f"{SILVER_WINMET_PATH}/winmet_clean"
df_clean = spark.read.parquet(input_path)

df_transformed = df_clean \
    .withColumn("file_size_mb", (col("file_size") / (1024 * 1024)).cast("double")) \
    .withColumn("file_name_lower", lower(trim(col("target.file.name"))))

df_transformed.printSchema()

df_transformed.select("sha256", "file_size", "file_size_mb", "target.file.name", "file_name_lower").show(5, truncate=False)

output_path = f"{SILVER_WINMET_PATH}/winmet_transformed"
df_transformed.write.mode("overwrite").parquet(output_path)

spark.stop()
