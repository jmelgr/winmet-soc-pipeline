import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(dotenv_path)

RAW_CONSENSUS_PATH = os.getenv("RAW_CONSENSUS_PATH")

spark = SparkSession.builder \
    .appName("Consensus_Load") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df_labels = spark.read.option("multiline", "true").json(RAW_CONSENSUS_PATH)

df_labels.printSchema()

spark.stop() 
