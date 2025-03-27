# Distributed CSV Processing with Apache Spark

## Deskripsi
Proyek ini bertujuan untuk melakukan pemrosesan file CSV secara terdistribusi menggunakan Apache Spark yang berjalan di dalam cluster Docker. Setiap worker akan menghasilkan output CSV masing-masing berdasarkan hasil pembagian data secara otomatis.

## Struktur Folder
```
├── data
│   ├── input.csv
│   └── output
├── docker-compose.yml
└── scripts
    ├── generate_csv.py
    ├── monitor_autoscale.sh
    └── process_csv.py 
```

## Cara Menjalankan

### 0 Generate Data

Menghasilkan file input.csv dengan jumlah 3 juta row
```
python scripts/generate_csv.py
```

### 1 Jalankan Cluster Docker
Pastikan Docker Compose sudah terinstal, lalu jalankan perintah berikut untuk memulai cluster Spark:
```sh
docker-compose up -d
```

### 2️ Cek Status Container

Pastikan container berjalan dengan baik:

docker ps

### 3️ Jalankan Pemrosesan CSV dengan Spark

Setelah cluster aktif, jalankan Spark job dengan perintah berikut:

docker exec -it spark-master spark-submit /opt/bitnami/spark/scripts/process_csv.py

### 4️ Cek Hasil Output

Setelah proses selesai, hasil output akan tersedia di folder data/output/ dalam format:

```
worker-1.csv
worker-2.csv
worker-3.csv
worker-4.csv
```

## ⚙️ Konfigurasi

Konfigurasi executor dapat diatur melalui variabel environment di dalam docker-compose.yml. Contoh:

```
environment:
  - SPARK_EXECUTOR_INSTANCES=4  # Jumlah executor
  - SPARK_EXECUTOR_MEMORY=4G    # Alokasi memori per executor
  - SPARK_EXECUTOR_CORES=2      # Jumlah core per executor
```

## 🛠 Troubleshooting

Jika output CSV tidak sesuai jumlah executor, pastikan SPARK_EXECUTOR_INSTANCES sesuai dengan jumlah partisi dalam process_csv.py.

Gunakan perintah docker logs spark-master untuk melihat log jika ada kesalahan.
