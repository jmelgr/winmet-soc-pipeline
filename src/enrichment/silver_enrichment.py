import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, size, lit, broadcast

dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(dotenv_path)

SILVER_WINMET_PATH = os.getenv("SILVER_WINMET_PATH")
SILVER_CONSENSUS_PATH = os.getenv("SILVER_CONSENSUS_PATH")

spark = SparkSession.builder \
    .appName("Silver_Enrichment_Integration") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

winmet_path = f"{SILVER_WINMET_PATH}/winmet_transformed"
consensus_path = f"{SILVER_CONSENSUS_PATH}/consensus_transformed"

df_winmet = spark.read.parquet(winmet_path)
df_consensus = spark.read.parquet(consensus_path)

df_joined = broadcast(df_winmet).join(df_consensus, on="sha256", how="left")

df_enriched = df_joined \
    .withColumn(
        "has_malware_label", 
        when(col("avclass").isNotNull(), True).otherwise(False)
    ) \
    .withColumn(
        "malware_family", 
        when(col("avclass").isNotNull(), col("avclass")).otherwise("Unlabeled")
    ) \
    .withColumn(
        "process_count", 
        when(col("behavior.processes").isNotNull(), size(col("behavior.processes"))).otherwise(0)
    )

df_enriched.printSchema()

df_enriched.select("sha256", "file_name_lower", "file_size_mb", "malware_family", "has_malware_label", "process_count").show(5, truncate=False)

output_path = f"{SILVER_WINMET_PATH}/enriched"
df_enriched.write.mode("overwrite").parquet(output_path)

spark.stop()
