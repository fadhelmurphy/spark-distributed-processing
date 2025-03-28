from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import spark_partition_id
import shutil
import glob

executor_instances = int(os.getenv("SPARK_EXECUTOR_INSTANCES", "2"))
executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
executor_cores = os.getenv("SPARK_EXECUTOR_CORES", "2")

spark = SparkSession.builder \
    .appName("DistributedCSVProcessing") \
    .master("spark://spark-master:7077") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "10") \
    .config("spark.executor.memory", executor_memory) \
    .config("spark.executor.cores", executor_cores) \
    .config("spark.executor.instances", executor_instances) \
    .getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", executor_instances)

data_path = "/opt/bitnami/spark/data/input.csv"
output_folder = "/opt/bitnami/spark/data/output"

df = spark.read.option("header", "true").csv(data_path)
df = df.repartition(executor_instances) 
df = df.withColumn("worker", spark_partition_id())

df.write \
    .mode("overwrite") \
    .option("header", True) \
    .partitionBy("worker") \
    .csv(output_folder)

for folder in glob.glob(f"{output_folder}/worker=*"):
    worker_id = folder.split("=")[-1]
    
    for file in glob.glob(f"{folder}/part-*.csv"):
        shutil.move(file, f"{output_folder}/worker-{worker_id}.csv")

    shutil.rmtree(folder)

print("Hasilnya jadi worker-0.csv, worker-1.csv, dst.")

spark.stop()
