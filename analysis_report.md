# 📊 Laporan Analisis Data COVID-19

## 1. Pendahuluan
Proyek ini bertujuan untuk menganalisis penyebaran, tren, dan tingkat fatalitas pandemi COVID-19 secara global menggunakan teknologi Big Data (Apache Spark). Dengan memproses jutaan baris data, dashboard ini menyajikan visualisasi interaktif untuk pengambilan wawasan (insight) yang mendalam.

## 2. Hasil Analisis Visualisasi

### A. Distribusi Geografis (Peta Global)
*   **Temuan**: Visualisasi peta choropleth menunjukkan bahwa **Amerika Serikat, India, dan Brasil** merupakan negara dengan akumulasi kasus tertinggi (warna merah pekat).
*   **Insight**: Penyebaran virus tidak merata dan sangat berkorelasi dengan mobilitas internasional serta kepadatan penduduk di negara-negara besar tersebut. Sebaliknya, wilayah Afrika dan sebagian Oseania menunjukkan akumulasi kasus yang relatif lebih rendah berdasarkan data yang dilaporkan.

### B. Analisis Tren Waktu (Moving Average 7-Hari)
*   **Temuan**: Grafik garis rata-rata bergerak (moving average) memperjelas adanya **gelombang lonjakan kasus (waves)** yang berulang.
*   **Insight**: Lonjakan kasus sering kali tidak terjadi secara serentak di seluruh dunia. Misalnya, saat gelombang Delta memuncak di India (Asia), negara-negara Eropa mungkin sedang mengalami penurunan kasus. Hal ini mengindikasikan bahwa varian baru membutuhkan waktu untuk menyebar antar-benua, memberikan "jeda waktu" bagi negara lain untuk bersiap.

### C. Ranking Global & Fatalitas
*   **Temuan**: Berdasarkan Bar Chart "Top 10 Negara dengan Kematian Tertinggi", **Amerika Serikat** memimpin angka kematian absolut, diikuti oleh Brasil dan India.
*   **Insight**: Namun, tingginya angka kematian absolut tidak selalu mencerminkan penanganan medis yang buruk, melainkan bisa disebabkan oleh besarnya populasi yang terinfeksi. Oleh karena itu, diperlukan analisis lanjutan menggunakan *Case Fatality Rate* (CFR).

### D. Case Fatality Rate (CFR) pada Negara Terinsdampak
*   **Temuan**: Grafik Bar Chart *Case Fatality Rate* menunjukkan anomali menarik. Meskipun AS memiliki jumlah kasus tertinggi, **Meksiko atau Peru** (tergantung data terkini) seringkali memiliki persentase CFR yang jauh lebih tinggi (>5-9% pada periode awal).
*   **Insight**: Tingginya CFR di negara berkembang dibandingkan negara maju (seperti AS/Eropa dengan CFR ~1-2%) mengindikasikan adanya kesenjangan dalam **fasiitas kesehatan, kapasitas testing, dan akses vaksin**. Negara dengan testing rendah cenderung hanya mendeteksi kasus parah, sehingga pembagi (denominator) kecil dan CFR terlihat sangat tinggi.

### E. Variabilitas Distribusi Harian (Box Plot)
*   **Temuan**: Box plot menunjukkan bahwa **Indonesia** memiliki rentang sebaran kasus harian yang lebar dengan banyak *outliers* di bagian atas.
*   **Insight**: *Outliers* ekstrem ini mencerminkan kejadian "super-spreader events" atau puncak gelombang varian tertentu (seperti varian Delta pada Juli 2021). Stabilitas kurva landai (tanpa outliers tinggi) lebih jarang ditemukan di negara dengan kepadatan penduduk tinggi.

## 3. Kesimpulan Teknis
Penggunaan **Apache Spark** memungkinkan pemrosesan *dataset* berukuran besar secara *in-memory* dengan cepat. Teknik *Window Function* sangat krusial dalam menghaluskan data yang fluktuatif (noise) melalui *Moving Average*, sehingga pola tren jangka panjang dapat diidentifikasi dengan lebih akurat dibandingkan hanya melihat data harian mentah.

---
*Laporan ini disusun berdasarkan output visualisasi dashboard `covid_analisis.py`.*
