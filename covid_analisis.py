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

    # Filter Region/Non-Negara
    list_filter = ["World", "Asia", "Europe", "North America", "South America", "Africa", "European Union", "High income"]
    
    # MANIPULASI 1: FILTERING & TYPE CASTING
    df_clean = df.filter(~col("location").isin(list_filter)) \
                 .withColumn("Tanggal", to_date(col("date"), "yyyy-MM-dd")) \
                 .filter(col("new_cases").isNotNull())
    
    # Pre-compute Global Stats untuk performa visualisasi cepat
    # Konversi ke Pandas dilakukan SESEDIKIT mungkin, hanya setelah aggregasi atau filter
    return df_clean

df_spark = load_and_clean_data()

if df_spark is None:
    st.stop()

# ==========================================
# 3. SIDEBAR CONTROLS
# ==========================================
with st.sidebar:
    st.markdown("## 🎛️ Kontrol Analisis")
    
    # Ambil daftar negara (convert to list python - ringan)
    country_list = [row.location for row in df_spark.select("location").distinct().sort("location").collect()]
    
    selected_countries = st.multiselect(
        "Pilih Negara untuk Perbandingan:",
        options=country_list,
        default=["Indonesia", "Malaysia", "Singapore"]
    )
    
    analysis_mode = st.radio(
        "Mode Analisis:",
        ["Overview Global", "Tren Waktu", "Analisis Fatalitas", "Analisis Distribusi & Ranking", "Data Mentah"]
    )
    
    st.markdown("---")
    st.info("Data Engine: **Apache Spark 3.5**\nData Source: **HDFS**")

# ==========================================
# 4. DASHBOARD LOGIC
# ==========================================

st.title("Dashboard Analisis COVID-19 Terintegrasi")
st.markdown("<h3 style='color: #64748b !important; font-weight: 400;'>Platform Big Data Analytics - UAS Semester 5</h3>", unsafe_allow_html=True)
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
            borderwidth=1
        )
    )
    return fig

if analysis_mode == "Overview Global":
    # --- PETA SEBARAN GLOBAL (Baru) ---
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

    # --- BAGIAN TOP RANKING ---
    st.markdown("---")
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Ranking Global")
        
        # MANIPULASI 3: AGGREGATION & RANKING
        df_rank = df_spark.groupBy("location").agg(spark_sum("new_deaths").alias("TotalDeaths"))
        window_rank = Window.orderBy(desc("TotalDeaths"))
        df_top10 = df_rank.withColumn("Rank", rank().over(window_rank)).filter(col("Rank") <= 10)
        
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
        fig_bar = update_layout_style(fig_bar, "Top 10 Negara dengan Angka Kematian Tertinggi")
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

    with col2:
        st.subheader("Key Performance Indicators")
        total_cases_world = df_spark.agg(spark_sum("new_cases")).collect()[0][0]
        total_deaths_world = df_spark.agg(spark_sum("new_deaths")).collect()[0][0]
        
        st.markdown(f"""
        <div class="metric-container">
            <div class="metric-label">Total Kasus Terkonfirmasi</div>
            <div class="metric-value">{total_cases_world:,.0f}</div>
        </div>
        <div class="metric-container">
            <div class="metric-label">Total Kematian Dilaporkan</div>
            <div class="metric-value">{total_deaths_world:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)

elif analysis_mode == "Tren Waktu":
    # --- BAGIAN TREND ANALYSIS (LINE CHART) ---
    st.subheader(f"Tren Penyebaran Virus: {', '.join(selected_countries)}")
    
    # Filter Data di Spark
    df_filtered = df_spark.filter(col("location").isin(selected_countries))
    
    # MANIPULASI 2: WINDOW FUNCTION (Moving Average)
    window_trend = Window.partitionBy("location").orderBy("Tanggal").rowsBetween(-6, 0)
    df_trend = df_filtered.withColumn("MovingAvg", avg("new_cases").over(window_trend))
    
    pdf_trend = df_trend.select("Tanggal", "location", "MovingAvg", "new_cases").toPandas()
    
    tab1, tab2 = st.tabs(["Smoothed (7-Day Avg)", "Harian (Raw)"])
    
    with tab1:
        fig_line = px.line(
            pdf_trend, x="Tanggal", y="MovingAvg", color="location",
            color_discrete_sequence=px.colors.qualitative.Bold,
            labels={"MovingAvg": "Kasus Baru (7-Day Avg)"}
        )
        fig_line = update_layout_style(fig_line, "Rata-rata Bergerak 7-Hari")
        st.plotly_chart(fig_line, use_container_width=True)
        
    with tab2:
        fig_raw = px.line(
            pdf_trend, x="Tanggal", y="new_cases", color="location",
            color_discrete_sequence=px.colors.qualitative.Pastel,
            labels={"new_cases": "Kasus Baru Harian"}
        )
        fig_raw = update_layout_style(fig_raw, "Kasus Harian Aktual")
        st.plotly_chart(fig_raw, use_container_width=True)

elif analysis_mode == "Analisis Fatalitas":
    # --- BAGIAN ANALISIS LANJUTAN ---
    st.subheader("Analisis Rasio Kematian (CFR)")
    
    # Agregasi Total per Negara pilihan
    df_agg = df_spark.filter(col("location").isin(selected_countries)) \
        .groupBy("location") \
        .agg(
            spark_sum("new_cases").alias("TotalCases"),
            spark_sum("new_deaths").alias("TotalDeaths")
        )
    
    pdf_agg = df_agg.toPandas()
    pdf_agg['CFR (%)'] = (pdf_agg['TotalDeaths'] / pdf_agg['TotalCases']) * 100
    
    col_a, col_b = st.columns(2)
    
    with col_a:
        fig_scatter = px.scatter(
            pdf_agg, x="TotalCases", y="TotalDeaths",
            size="CFR (%)", color="location",
            hover_name="location", log_x=True, log_y=True,
            color_discrete_sequence=px.colors.qualitative.Prism,
            size_max=30, # Diperkecil agar tidak menutupi
            labels={
                "TotalCases": "Total Kasus (Log)", 
                "TotalDeaths": "Total Kematian (Log)",
                "CFR (%)": "Case Fatality Rate (%)"
            }
        )
        fig_scatter = update_layout_style(fig_scatter, "Hubungan Kasus vs Kematian")
        # Pindahkan legend ke bawah agar chart lebih lebar
        fig_scatter.update_layout(
            legend=dict(
                orientation="h", 
                yanchor="top", 
                y=-0.2, 
                xanchor="center", 
                x=0.5
            ),
            margin=dict(l=20, r=20, t=60, b=80) # Tambah margin bawah untuk legend
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
        
    with col_b:
        fig_pie = px.pie(
            pdf_agg, values='TotalDeaths', names='location',
            hole=0.5,
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        fig_pie = update_layout_style(fig_pie, "Proporsi Kematian Relatif")
        # Pindahkan legend ke bawah
        fig_pie.update_layout(
            legend=dict(
                orientation="h", 
                yanchor="top", 
                y=-0.2, 
                xanchor="center", 
                x=0.5
            ),
            margin=dict(l=20, r=20, t=60, b=80)
        )
        st.plotly_chart(fig_pie, use_container_width=True)

elif analysis_mode == "Analisis Distribusi & Ranking":
    st.subheader("Analisis Distribusi Regional & Ranking Global")
    
    df_dist = df_spark.filter(col("location").isin(selected_countries)) \
                      .select("location", "Tanggal", "new_cases")
    pdf_dist = df_dist.toPandas()
    
    # Box Plot
    fig_box = px.box(
        pdf_dist, x="location", y="new_cases",
        color="location",
        points="outliers",
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig_box = update_layout_style(fig_box, "Distribusi Kasus Harian (Box Plot)")
    st.plotly_chart(fig_box, use_container_width=True)
    
    # Bar Chart: Case Fatality Rate Top 10 Countries by Total Cases
    # 1. Agregasi Global untuk mencari Top 10 by Cases
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
        color_discrete_sequence=["#9b59b6"] # Warna Ungu seperti referensi
    )
    
    fig_bar_cfr.update_layout(
        font=dict(family="Inter, sans-serif", size=12, color="#64748b"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis_title="Country",
        yaxis_title="Fatality Rate (%)",
        margin=dict(l=20, r=20, t=80, b=100), # Margin diperbesar agar tidak terpotong
        title_font=dict(size=16, color="#0f172a")
    )
    # Menampilkan teks di atas bar
    fig_bar_cfr.update_traces(textposition='outside')
    
    st.plotly_chart(fig_bar_cfr, use_container_width=True)

elif analysis_mode == "Data Mentah":
    st.subheader("Eksplorasi Data Mentah")
    
    limit_rows = st.slider("Jumlah Baris Data:", 100, 5000, 1000)
    
    df_show = df_spark.filter(col("location").isin(selected_countries)) \
                      .select("location", "Tanggal", "new_cases", "new_deaths", "total_cases") \
                      .orderBy(desc("Tanggal")) \
                      .limit(limit_rows)
                      
    st.dataframe(df_show.toPandas(), use_container_width=True)
    
    csv = df_show.toPandas().to_csv(index=False).encode('utf-8')
    st.download_button(
        "Download Data ini sebagai CSV",
        csv,
        "covid_data_sample.csv",
        "text/csv",
        key='download-csv'
    )