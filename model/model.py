import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import matplotlib.pyplot as plt

dotenv_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path)

SILVER_WINMET_PATH = os.getenv("SILVER_WINMET_PATH")

spark = SparkSession.builder \
    .appName("Base_Model_Evaluation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

path = f"{SILVER_WINMET_PATH}/enriched"
df = spark.read.parquet(path)

df_ml = df.select("file_size_mb", "process_count", "has_malware_label") \
          .na.drop() \
          .withColumn("label", df["has_malware_label"].cast("double"))

assembler = VectorAssembler(inputCols=["file_size_mb", "process_count"], outputCol="features")
df_vectorized = assembler.transform(df_ml)

train_data, test_data = df_vectorized.randomSplit([0.8, 0.2], seed=42)

lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
model = lr.fit(train_data)

predictions = model.transform(test_data)
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)

print(f"=== LÍNEA BASE TÉCNICA - MODELO BASE ===")
print(f"AUC-ROC del Modelo Base: {auc}")

summary = model.evaluate(test_data)
roc = summary.roc.toPandas()

plt.figure(figsize=(7, 6))
plt.plot(roc['FPR'], roc['TPR'], color='#2b5c8f', lw=2, label=f'Modelo Base (AUC = {auc:.2f})')
plt.plot([0, 1], [0, 1], color='gray', linestyle='--', lw=1.5, label='Clasificador Aleatorio (AUC = 0.50)')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('Tasa de Falsos Positivos (1 - Especificidad)', fontsize=11)
plt.ylabel('Tasa de Verdaderos Positivos (Sensibilidad)', fontsize=11)
plt.title('Curva ROC - Modelo Base de Clasificación (Línea Base Técnica)', fontsize=12, fontweight='bold')
plt.legend(loc="lower right", fontsize=10)
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()

output_dir = os.path.join(os.path.dirname(__file__), '../outputs')
os.makedirs(output_dir, exist_ok=True)
plt.savefig(os.path.join(output_dir, "analisis_modelo_base_roc.png"), dpi=300)
plt.close()

spark.stop()
