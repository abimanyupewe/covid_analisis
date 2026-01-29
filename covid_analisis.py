import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, sum as spark_sum, rank, date_format, desc
from pyspark.sql.window import Window
import pandas as pd
import sys

# ==========================================
# KONFIGURASI HALAMAN (Modern Look)
# ==========================================
st.set_page_config(
    page_title="COVID-19 Big Data Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS untuk estetika modern (Light Theme)
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    
    html, body, [class*="css"]  {
        font-family: 'Inter', sans-serif;
        color: #1e293b; 
    }
    
    .stApp {
        background-color: #f8fafc;
    }
    
    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #e2e8f0;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #0f172a !important;
        font-weight: 700;
        letter-spacing: -0.5px;
    }
    
    h1 { margin-bottom: 0.5rem; }
    
    /* Metric Cards */
    .metric-container {
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        padding: 1.5rem;
        background-color: #ffffff;
        border-radius: 12px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
        margin-bottom: 1rem;
        transition: transform 0.2s;
    }
    
    .metric-container:hover {
        transform: translateY(-2px);
        border-color: #cbd5e1;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05);
    }
    
    .metric-label {
        font-size: 0.875rem;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #0f172a;
    }
    
    /* Plotly Chart Container */
    .stPlotlyChart {
        background-color: #ffffff;
        border-radius: 12px;
        padding: 15px;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
        border: 1px solid #e2e8f0;
    }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 1. INISIALISASI SPARK (CACHED)
# ==========================================
@st.cache_resource
def get_spark_session():
    """Membuat session Spark sekali saja."""
    return SparkSession.builder \
        .appName("Streamlit_COVID_Dashboard") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

spark = get_spark_session()

# ==========================================
# 2. LOAD & PREPROCESS DATA
# ==========================================
# Note: Spark DataFrames are lazy and cannot be cached by st.cache_data directly.
# We rely on Spark's internal caching if needed (e.g., .cache()).
def load_and_clean_data():
    """Load data dari HDFS dan lakukan manipulasi dasar."""
    file_path = "hdfs://localhost:9600/uas_bigdata/dataset/owid_covid_data.csv"
    
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    except Exception as e:
        st.error(f"Gagal koneksi ke HDFS: {e}")
        return None

    # MANIPULASI 1: FILTERING & TYPE CASTING
    # Menggunakan filter continent isNotNull adalah cara yang lebih robust untuk mengambil data negara saja.
    # Data agregat seperti 'World', 'Asia', 'High income' biasanya memiliki continent = NULL.
    df_clean = df.filter(col("continent").isNotNull()) \
                 .withColumn("Tanggal", to_date(col("date"), "yyyy-MM-dd")) \
                 .filter(col("new_cases").isNotNull())
    
    # Pre-compute Global Stats untuk performa visualisasi cepat
    # Konversi ke Pandas dilakukan SESEDIKIT mungkin, hanya setelah aggregasi atau filter
    
    # Hitung jumlah data
    raw_count = df.count()
    clean_count = df_clean.count()
    
    return df_clean, raw_count, clean_count

df_spark, raw_count, clean_count = load_and_clean_data()

if df_spark is None:
    st.stop()

# ==========================================
# 3. SIDEBAR CONTROLS
# ==========================================
with st.sidebar:
    st.markdown("## Kontrol Analisis")
    
    # Ambil daftar negara (convert to list python - ringan)
    # Ambil daftar negara
    country_list_raw = [row.location for row in df_spark.select("location").distinct().sort("location").collect()]
    country_options = ["All"] + country_list_raw
    
    selected_input = st.multiselect(
        "Pilih Negara untuk Perbandingan:",
        options=country_options,
        default=["Indonesia", "Malaysia", "Singapore"]
    )

    if "All" in selected_input:
        selected_countries = country_list_raw
        selected_display_text = "Semua Negara"
    else:
        selected_countries = selected_input
        selected_display_text = ", ".join(selected_countries)
    
    # Mode Analisis dihapus untuk penyederhanaan "Essential Dashboard"
    st.markdown("---")
    st.markdown("### Statistik Data")
    st.write(f"**Total Data Mentah:** {raw_count:,}")
    st.write(f"**Data Cleaning:** {clean_count:,}")
    st.progress(clean_count / raw_count if raw_count > 0 else 0)
    st.caption(f"Retention Rate: {((clean_count/raw_count)*100):.1f}%")
    
    st.markdown("---")
    st.info("Data Engine: **Apache Spark 3.5**\nData Source: **HDFS**")

# ==========================================
# 4. DASHBOARD LOGIC (SIMPLIFIED ESSENTIALS)
# ==========================================

st.title("Dashboard Analisis COVID-19")
st.markdown("<h3 style='color: #64748b !important; font-weight: 400;'>Essential Metrics & Trends</h3>", unsafe_allow_html=True)
st.markdown("---")

# Custom Plotly Template for Consistency (Light Mode)
def update_layout_style(fig, title):
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=18, family="Inter, sans-serif", color="#0f172a") # Dark text
        ),
        font=dict(family="Inter, sans-serif", size=12, color="#64748b"), # Slate-500 text
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=20, r=20, t=60, b=20),
        legend=dict(
            bgcolor='rgba(255, 255, 255, 0.8)',
            bordercolor='#e2e8f0',
            borderwidth=1,
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    return fig

# --- 1. GLOBAL KEY METRICS ---
col1, col2 = st.columns(2)

total_cases_world = df_spark.agg(spark_sum("new_cases")).collect()[0][0]
total_deaths_world = df_spark.agg(spark_sum("new_deaths")).collect()[0][0]

with col1:
    st.markdown(f"""
    <div class="metric-container">
        <div class="metric-label">Total Kasus Global</div>
        <div class="metric-value">{total_cases_world:,.0f}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-container">
        <div class="metric-label">Total Kematian Global</div>
        <div class="metric-value">{total_deaths_world:,.0f}</div>
    </div>
    """, unsafe_allow_html=True)

# --- 1.5 PETA SEBARAN GLOBAL (Restored) ---
st.subheader("Distribusi Geografis")

# Agregasi data untuk peta (Total Kasus per Negara)
# Kita ambil max total_cases karena itu adalah cumulative count
df_map = df_spark.groupBy("location").agg(
    spark_sum("new_cases").alias("TotalCases"),
    spark_sum("new_deaths").alias("TotalDeaths")
)
pdf_map = df_map.toPandas()

# Visualisasi Map
fig_map = px.choropleth(
    pdf_map,
    locations="location",
    locationmode="country names",
    color="TotalCases",
    hover_name="location",
    hover_data=["TotalDeaths"],
    color_continuous_scale="Reds",
    template="plotly_white" # Light template
)
fig_map.update_geos(
    bgcolor='rgba(0,0,0,0)', 
    showcountries=True, countrycolor="#cbd5e1",
    showcoastlines=False,
    showland=True, landcolor="#f1f5f9"
)
fig_map = update_layout_style(fig_map, "Peta Sebaran Global: Total Kasus Terkonfirmasi")
st.plotly_chart(fig_map, use_container_width=True)

# --- 2. TOP 10 RANKING (Bar Chart) ---
st.subheader("Top 10 Negara dengan Kematian Tertinggi")
# MANIPULASI: AGGREGATION & RANKING (Simple Top 10)
df_rank = df_spark.groupBy("location").agg(spark_sum("new_deaths").alias("TotalDeaths"))
# Note: Spark optimization - filtering top 10 before collecting is efficient
df_top10 = df_rank.orderBy(desc("TotalDeaths")).limit(10)
pdf_top10 = df_top10.toPandas()

fig_bar = px.bar(
    pdf_top10, 
    x='TotalDeaths', 
    y='location', 
    orientation='h',
    color='TotalDeaths',
    color_continuous_scale='Reds',
    text_auto='.2s'
)
fig_bar = update_layout_style(fig_bar, "Top 10 Negara dengan Angka Kematian Tertinggi (Total)")
fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
st.plotly_chart(fig_bar, use_container_width=True)

# --- 2.5 ANALISIS FATALITAS (CFR) ---
st.subheader("Analisis Fatalitas (Case Fatality Rate)")
st.caption("Membandingkan tingkat kematian pada 10 negara dengan jumlah kasus tertinggi.")

# Agregasi Global untuk mencari Top 10 by Cases
df_top_cfr = df_spark.groupBy("location").agg(
    spark_sum("new_cases").alias("TotalCases"),
    spark_sum("new_deaths").alias("TotalDeaths")
).orderBy(desc("TotalCases")).limit(10)

pdf_top_cfr = df_top_cfr.toPandas()
pdf_top_cfr['CFR'] = (pdf_top_cfr['TotalDeaths'] / pdf_top_cfr['TotalCases']) * 100

# Format label untuk text
pdf_top_cfr['CFR_Label'] = pdf_top_cfr['CFR'].map('{:,.2f}%'.format)

fig_bar_cfr = px.bar(
    pdf_top_cfr, 
    x="location", 
    y="CFR",
    text="CFR_Label",
    title="Case Fatality Rate (%) in Top 10 Countries by Total Cases",
    color_discrete_sequence=["#9b59b6"] # Warna Ungu
)

fig_bar_cfr.update_layout(
    font=dict(family="Inter, sans-serif", size=12, color="#64748b"),
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    xaxis_title="Negara",
    yaxis_title="Fatality Rate (%)",
    margin=dict(l=20, r=20, t=60, b=40),
    title_font=dict(size=18, color="#0f172a") # Sesuaikan ukuran font title agar konsisten
)
# Menampilkan teks di atas bar
fig_bar_cfr.update_traces(textposition='outside')

st.plotly_chart(fig_bar_cfr, use_container_width=True)

# --- 3. TREND LINE (Time Series) ---
st.subheader(f"Tren Kasus Harian: {selected_display_text}")

# Filter Data di Spark
df_filtered = df_spark.filter(col("location").isin(selected_countries))

# MANIPULASI: WINDOW FUNCTION (Moving Average)
window_trend = Window.partitionBy("location").orderBy("Tanggal").rowsBetween(-6, 0)
df_trend = df_filtered.withColumn("MovingAvg", avg("new_cases").over(window_trend))

pdf_trend = df_trend.select("Tanggal", "location", "MovingAvg", "new_cases").toPandas()

fig_line = px.line(
    pdf_trend, x="Tanggal", y="MovingAvg", color="location",
    color_discrete_sequence=px.colors.qualitative.Bold,
    labels={"MovingAvg": "Kasus Baru (7-Day Avg)"}
)
fig_line = update_layout_style(fig_line, "Rata-rata Bergerak 7-Hari")
st.plotly_chart(fig_line, use_container_width=True)

# --- 4. CHECK DATA SECTION (Raw Data) ---
with st.expander("🔍 Lihat Data Mentah & Pengecekan Kualitas Data"):
    st.write("Menampilkan sampel data yang digunakan untuk verifikasi cleaning.")
    
    # User Control for Row Limit
    limit_rows = st.slider("Jumlah Baris Data yang Ditampilkan:", min_value=100, max_value=5000, value=1000, step=100)
    
    # Check null continents
    null_continent_count = df_filtered.filter(col("continent").isNull()).count()
    if null_continent_count == 0:
        st.success("✅ Validasi Data Bersih: Tidak ada continent NULL (Data Agregat/World dihapus).")
    else:
        st.warning(f"⚠️ Ditemukan {null_continent_count} baris dengan Continent NULL.")

    # Show data with dynamic limit
    st.dataframe(pdf_trend.head(limit_rows), use_container_width=True)
    
    # Download button (based on displayed data)
    csv = pdf_trend.head(limit_rows).to_csv(index=False).encode('utf-8')
    st.download_button(
        "Download Data ini sebagai CSV",
        csv,
        "covid_data_sample.csv",
        "text/csv",
        key='download-csv'
    )