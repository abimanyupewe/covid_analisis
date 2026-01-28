# Laporan Teknis Proyek Big Data: Analisis COVID-19

## 1. Persiapan Lingkungan (Setup Awal)
Sebelum menjalankan aplikasi data processing, lingkungan pengembangan disiapkan dengan spesifikasi berikut:

*   **Sistem Operasi**: Linux
*   **Bahasa Pemrograman**: Python 3.12
*   **Framework Big Data**: Apache Spark 3.5 (PySpark)
*   **Storage**: Hadoop HDFS (Hadoop Distributed File System)
*   **Interface**: Streamlit & Plotly

### Langkah Instalasi Library
Perintah terminal untuk menginstal *dependencies* yang diperlukan:

```bash
# Membuat virtual environment (opsional)
python3 -m venv env_covid
source env_covid/bin/activate

# Install library Python
pip install pyspark streamlit plotly pandas
```

## 2. Ingesti Data ke HDFS
Data mentah (`owid_covid_data.csv`) diunggah ke sistem penyimpanan terdistribusi (HDFS) agar dapat diakses oleh Spark worker.

**Perintah Terminal:**
```bash
# 1. Membuat direktori di HDFS
hdfs dfs -mkdir -p /uas_bigdata/dataset/

# 2. Mengunggah file dataset dari lokal ke HDFS
hdfs dfs -put owid_covid_data.csv /uas_bigdata/dataset/

# 3. Verifikasi file berhasil masuk
hdfs dfs -ls /uas_bigdata/dataset/
```
*Lokasi file di HDFS:* `hdfs://localhost:9600/uas_bigdata/dataset/owid_covid_data.csv`

## 3. Pembersihan Data (Data Cleaning)
Proses pembersihan dilakukan secara *in-memory* menggunakan PySpark untuk menjamin kualitas data sebelum analisis.

**Kode Implementasi:**
```python
# MANIPULASI 1: FILTERING & TYPE CASTING
# Menggunakan filter continent isNotNull adalah cara yang lebih robust untuk mengambil data negara saja.
df_clean = df.filter(col("continent").isNotNull()) \
             .withColumn("Tanggal", to_date(col("date"), "yyyy-MM-dd")) \
             .filter(col("new_cases").isNotNull())
```

## 4. Tiga (3) Tahapan Manipulasi Data Kompleks
Sesuai persyaratan proyek Big Data, berikut adalah 3 manipulasi utama yang diterapkan:

### Manipulasi 1: Advanced Filtering & Type Casting
Mengubah tipe data string menjadi objek tanggal dan memfilter entitas non-negara secara efisien menggunakan pengecekan kolom `continent`.

Filtering Agregat: Menggunakan `col("continent").isNotNull()` untuk secara otomatis menghapus baris data agregat (seperti "World", "Asia", "High income") yang nilai benuanya kosong. Ini lebih *robust* daripada _hardcoded exclusion list_.

Type Casting: Mengubah kolom 
date (string) menjadi format tanggal (DateType) agar bisa diurutkan secara waktu.

Null Removal: Membuang baris data di mana new_cases bernilai kosong/NULL untuk menjaga akurasi perhitungan rata-rata.

### Manipulasi 2: Window Function (Analisis Tren)
Menggunakan `Window Partition` untuk menghitung **Moving Average (Rata-rata Bergerak) 7-Hari** per negara. Ini teknik krusial dalam data *time-series* untuk melihat tren tanpa gangguan fluktuasi harian.

```python
window_trend = Window.partitionBy("location").orderBy("Tanggal").rowsBetween(-6, 0)
df_trend = df_filtered.withColumn("MovingAvg", avg("new_cases").over(window_trend))
```

### Manipulasi 3: Aggregation & Global Ranking
Melakukan agregasi total (Sum) untuk seluruh dataset, kemudian memberikan peringkat (Rank) untuk menemukan negara dengan dampak terparah.

```python
df_rank = df_spark.groupBy("location").agg(spark_sum("new_deaths").alias("TotalDeaths"))
window_rank = Window.orderBy(desc("TotalDeaths"))
df_top10 = df_rank.withColumn("Rank", rank().over(window_rank)).filter(col("Rank") <= 10)
```

## 5. Hasil Analisis
Berdasarkan visualisasi dashboard, berikut adalah temuan utama:

### A. Distribusi & Hotspot Global
Visualisasi **Peta Choropleth** menunjukkan bahwa hotspot COVID-19 berpusat di negara-negara dengan populasi padat seperti **Amerika Serikat, India, dan Brasil**. Warna merah pekat pada peta mengindikasikan akumulasi kasus absolut yang jauh melampaui negara-negara lain.

### B. Pola Gelombang (Waves)
Analisis **Tren Waktu** memperlihatkan pola gelombang yang berulang.
*   Puncak gelombang di setiap negara tidak terjadi bersamaan.
*   Analisis *Moving Average* mampu memperhalus data, memperjelas kapan sebuah gelombang dimulai dan berakhir dibandingkan grafik harian yang penuh *noise*.

### C. Case Fatality Rate (CFR)
Analisis perbandingan **Bar Chart Top 10** menunjukkan insight anomali:
*   Negara dengan kasus tertinggi (AS) memiliki tingkat fatalitas (CFR) sekitar **1-2%**.
*   Sebaliknya, beberapa negara berkembang di Amerika Selatan (seperti Peru/Meksiko) bisa memiliki CFR yang jauh lebih tinggi (>5%), mengindikasikan ketimpangan sistem kesehatan atau kapasitas testing (hanya kasus parah yang terdeteksi).


