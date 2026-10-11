import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(dotenv_path)

BRONZE_WINMET_PATH = os.getenv("BRONZE_WINMET_PATH")

spark = SparkSession.builder \
    .appName("WinMET_Bronze_Load") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
df_winmet = spark.read.option("multiline", "true").json(BRONZE_WINMET_PATH)

df_winmet.printSchema()

spark.stop() 
