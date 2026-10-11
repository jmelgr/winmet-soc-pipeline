import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(dotenv_path)

SILVER_WINMET_PATH = os.getenv("SILVER_WINMET_PATH")

spark = SparkSession.builder \
    .appName("Process_Complexity") \
    .getOrCreate()

path = f"{SILVER_WINMET_PATH}/enriched"
df_silver = spark.read.parquet(path)

df_categorized = df_silver.withColumn(
    "rango_procesos",
    F.when(F.col("process_count") == 1, "1. Monoproceso (Sigiloso / Simple)")
     .when((F.col("process_count") >= 2) & (F.col("process_count") <= 5), "2. Complejidad Baja (2-5 procesos)")
     .when((F.col("process_count") >= 6) & (F.col("process_count") <= 15), "3. Complejidad Media (6-15 procesos)")
     .otherwise("4. Complejidad Alta (>15 procesos)")
)

df_analysis2 = df_categorized.groupBy("rango_procesos") \
    .count() \
    .orderBy("rango_procesos")

df_analysis2.show(truncate=False)

pdf = df_analysis2.toPandas()

plt.figure(figsize=(9, 5))
bars = plt.barh(pdf["rango_procesos"], pdf["count"], color="#2b5c8f", edgecolor="black", alpha=0.85)
plt.xlabel("Cantidad de Muestras", fontsize=11)
plt.ylabel("Rango de Complejidad Operativa", fontsize=11)
plt.title("Distribución de Muestras según Complejidad de Ejecución", fontsize=12, fontweight="bold")
plt.gca().invert_yaxis()
plt.grid(axis="x", linestyle="--", alpha=0.7)

for bar in bars:
    width = bar.get_width()
    plt.text(width + (max(pdf["count"]) * 0.01), bar.get_y() + bar.get_height()/2, 
             f'{int(width):,}', va='center', ha='left', fontsize=10)

plt.tight_layout()

output_dir = os.path.join(os.path.dirname(__file__), '../../outputs')
os.makedirs(output_dir, exist_ok=True)

plt.savefig(os.path.join(output_dir, "analisis_2_complejidad_procesos.png"), dpi=300)
plt.close()
