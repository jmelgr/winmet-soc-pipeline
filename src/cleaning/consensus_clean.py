import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import json

dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(dotenv_path)

RAW_CONSENSUS_PATH = os.getenv("RAW_CONSENSUS_PATH")
SILVER_CONSENSUS_PATH = os.getenv("SILVER_CONSENSUS_PATH")

spark = SparkSession.builder \
    .appName("Consensus_Silver_Clean") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
hdfs_path_labels = RAW_CONSENSUS_PATH

text_data = spark.sparkContext.textFile(hdfs_path_labels).collect()
raw_data = json.loads("".join(text_data))

flat_records = []
for sha256, details in raw_data.items():
    if isinstance(details, dict):
        flat_records.append({
            "sha256": sha256,
            "avclass": details.get("avclass"),
            "cape": details.get("cape")
        })

df_consensus = spark.createDataFrame(flat_records)

df_consensus_clean = df_consensus \
    .dropna(subset=["sha256"]) \
    .dropDuplicates(["sha256"]) \
    .fillna({"avclass": "unknown", "cape": "(n/a)"})

path = f"{SILVER_CONSENSUS_PATH}/consensus_clean"
df_consensus_clean.write.mode("overwrite").parquet(path)

spark.stop()
