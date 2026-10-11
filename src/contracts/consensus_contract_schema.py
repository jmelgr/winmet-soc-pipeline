import os
from pyspark.sql import SparkSession
import json

spark = SparkSession.builder \
    .appName("Consensus_Load_Optimized") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../data")
)

path_labels = os.path.join(BASE_DIR, "raw", "reports_consensus_label.json")

with open(path_labels, "r") as f:
    raw_data = json.load(f)

flat_records = []
for sha256, details in raw_data.items():
    if isinstance(details, dict):
        flat_records.append({
            "sha256": sha256,
            "avclass": details.get("avclass"),
            "cape": details.get("cape")
        })

df_consensus = spark.createDataFrame(flat_records)

df_consensus.printSchema()

df_consensus.show(5, truncate=False)

spark.stop()
