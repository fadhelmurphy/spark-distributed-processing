from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col, lit, monotonically_increasing_id
import shutil

# Ambil konfigurasi dari environment
executor_instances = int(os.getenv("SPARK_EXECUTOR_INSTANCES", "2"))
executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
executor_cores = os.getenv("SPARK_EXECUTOR_CORES", "2")

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("DistributedCSVProcessing") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", executor_memory) \
    .config("spark.executor.cores", executor_cores) \
    .config("spark.executor.instances", executor_instances) \
    .getOrCreate()

# Path input & output CSV
data_path = "/opt/bitnami/spark/data/input.csv"
output_folder = "/opt/bitnami/spark/data/output"

# Baca data CSV
df = spark.read.option("header", "true").csv(data_path)

# Pastikan data tersebar ke semua worker
df = df.repartition(executor_instances)

# Simpan ID worker dengan id unik per baris
df = df.withColumn("executor_id", (monotonically_increasing_id() % executor_instances).cast("string"))

# Simpan file per worker
df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .partitionBy("executor_id") \
    .csv(output_folder)

# Loop ke setiap folder executor_id
for folder in os.listdir(output_folder):
    folder_path = os.path.join(output_folder, folder)

    if os.path.isdir(folder_path) and folder.startswith("executor_id="):
        worker_id = folder.split("=")[-1]

        # Cari file CSV di dalam folder executor_id
        for file in os.listdir(folder_path):
            if file.startswith("part-") and file.endswith(".csv"):
                src_path = os.path.join(folder_path, file)
                dst_path = os.path.join(output_folder, f"worker-{worker_id}.csv")

                # Pindahkan file dengan nama baru
                shutil.move(src_path, dst_path)

        # Hapus folder kosong setelah memindahkan file
        shutil.rmtree(folder_path)

print("✅ File berhasil diubah menjadi worker-N.csv")

spark.stop()
