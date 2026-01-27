# COVID-19 Big Data Analytics Dashboard

![Python](https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge&logo=python)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange?style=for-the-badge&logo=apachespark)
![Streamlit](https://img.shields.io/badge/Streamlit-1.28-red?style=for-the-badge&logo=streamlit)
![Plotly](https://img.shields.io/badge/Plotly-5.18-3f4f75?style=for-the-badge&logo=plotly)

Aplikasi dashboard interaktif untuk analisis data COVID-19 skala besar menggunakan **Apache Spark** sebagai engine pemrosesan data dan **Streamlit** untuk antarmuka pengguna yang modern dan responsif. Project ini dikembangkan untuk memenuhi tugas akhir semester (UAS) mata kuliah Big Data.

## Fitur Utama

-   **Pemrosesan Big Data**: Menggunakan PySpark untuk membaca dan memproses dataset besar dari HDFS secara efisien.
-   **Dashboard Interaktif**: Filter dinamis berdasarkan negara, rentang tanggal, dan jenis analisis.
-   **Visualisasi Mendalam**:
    -   **Global Ranking**: Top 10 negara dengan tingkat kematian tertinggi.
    -   **Tren Waktu**: Grafik garis interaktif (Smoothed & Raw) untuk melacak penyebaran kasus.
    -   **Analisis Fatalitas**: Scatter plot logaritmik untuk korelasi kasus vs kematian (CFR).
-   **Data Eksplorasi**: Tabel data interaktif yang dapat diunduh dalam format CSV.

## Tech Stack

-   **Backend / Data Processing**: Python, Apache Spark (PySpark)
-   **Storage**: Hadoop Distributed File System (HDFS)
-   **Frontend / UI**: Streamlit
-   **Visualization**: Plotly Express, Plotly Graph Objects

## Prasyarat

Sebelum menjalankan aplikasi, pastikan environment Anda memiliki:

1.  **Python 3.8+**
2.  **Java 8/11** (untuk Spark)
3.  **Hadoop HDFS** yang berjalan (local/cluster)

## Instalasi

1.  **Clone Repository** (atau download folder project):
    ```bash
    git clone <repository-url>
    cd covid_analisis
    ```

2.  **Buat Virtual Environment (Opsional tapi direkomendasikan)**:
    ```bash
    python3 -m venv env_covid
    source env_covid/bin/activate
    ```

3.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
    *Jika `requirements.txt` belum ada:*
    ```bash
    pip install pyspark streamlit plotly pandas
    ```

## Cara Menjalankan

Pastikan HDFS sudah berjalan dan file dataset tersedia di path HDFS yang sesuai (default: `hdfs://localhost:9600/uas_bigdata/dataset/owid_covid_data.csv`).

Jalankan perintah berikut di terminal:

```bash
streamlit run covid_analisis.py
```

Dashboard akan otomatis terbuka di browser Anda (biasanya di `http://localhost:8501`).

## Struktur Project

```
covid_analisis/
├── covid_analisis.py    # Main application code
├── requirements.txt     # Python dependencies
└── README.md            # Dokumentasi project
```

## Catatan Pengembang

Kode ini menggunakan optimasi **Apache Arrow** untuk mempercepat konversi data dari Spark DataFrame ke Pandas DataFrame untuk keperluan visualisasi.

```python
.config("spark.sql.execution.arrow.pyspark.enabled", "true")
```

---
**Dibuat untuk UAS Big Data Semester 5**
