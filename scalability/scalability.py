import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession

dotenv_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path)

SILVER_WINMET_PATH = os.getenv("SILVER_WINMET_PATH")

spark = SparkSession.builder \
    .appName("Scalability_Test") \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

path = f"{SILVER_WINMET_PATH}/enriched"
df = spark.read.parquet(path)

# --- 1. ESCALA 1x (Real) ---
start = time.time()
count_1x = df.count()
t_1x = time.time() - start
print(f"[1x] {count_1x} registros - Tiempo: {t_1x:.3f} s")

# --- 2. ESCALA 10x ---
df_10x = df
for _ in range(9):
    df_10x = df_10x.union(df)

start = time.time()
count_10x = df_10x.count()
t_10x = time.time() - start
print(f"[10x] {count_10x} registros - Tiempo: {t_10x:.3f} s")

# --- 3. ESCALA 100x ---
df_100x = df_10x
for _ in range(9):
    df_100x = df_100x.union(df_10x)

start = time.time()
count_100x = df_100x.count()
t_100x = time.time() - start
print(f"[100x] {count_100x} registros - Tiempo: {t_100x:.3f} s")

spark.stop()
