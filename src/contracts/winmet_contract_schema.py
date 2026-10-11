import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, MapType

dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(dotenv_path)

BRONZE_WINMET_PATH = os.getenv("BRONZE_WINMET_PATH")

spark = SparkSession.builder \
    .appName("WinMET_Bronze_Load") \
    .config("spark.driver.memory", "10g") \
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

df_winmet.printSchema()

print("\n--- Muestra de Datos Estructurados ---")
df_winmet.select("target.file.sha256", "target.file.name").show(5, truncate=False)

spark.stop()
