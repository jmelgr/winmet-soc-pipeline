import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(dotenv_path)

SILVER_WINMET_PATH = os.getenv("SILVER_WINMET_PATH")

spark = SparkSession.builder \
    .appName("Top_Processes") \
    .getOrCreate()

path = f"{SILVER_WINMET_PATH}/enriched"
df_silver = spark.read.parquet(path)

df_processes = df_silver.select(
    F.explode(F.col("behavior.processes")).alias("process_info")
).select(
    F.col("process_info.process_name").alias("process_name")
)

df_top_processes = df_processes.groupBy("process_name") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .limit(15)

df_top_processes.show(15, truncate=False)

pdf = df_top_processes.toPandas()

plt.figure(figsize=(11, 6))
bars = plt.barh(pdf["process_name"], pdf["count"], color="#2b5c8f", edgecolor="black", alpha=0.85)
plt.xlabel("Frecuencia Total de Invocación", fontsize=11)
plt.ylabel("Nombre del Proceso / Binario", fontsize=11)
plt.title("Top 15 Procesos Más Frecuentes Invocados (Indicadores de Living off the Land)", fontsize=12, fontweight="bold")
plt.gca().invert_yaxis()
plt.grid(axis="x", linestyle="--", alpha=0.7)

for bar in bars:
    width = bar.get_width()
    plt.text(width + (max(pdf["count"]) * 0.01), bar.get_y() + bar.get_height()/2, 
             f'{int(width):,}', va='center', ha='left', fontsize=9)

plt.tight_layout()

output_dir = os.path.join(os.path.dirname(__file__), '../../outputs')
os.makedirs(output_dir, exist_ok=True)

plt.savefig(os.path.join(output_dir, "analisis_3_top_procesos.png"), dpi=300)
plt.close()
