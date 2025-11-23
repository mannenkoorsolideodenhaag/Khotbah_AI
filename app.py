import pandas as pd
import streamlit as st
from databricks import sql
from openai import OpenAI

# =========================
# Konfigurasi dari Secrets
# =========================
# Di Streamlit Cloud, set di: Settings -> Secrets
# Contoh isi secrets:
# DATABRICKS_SERVER_HOSTNAME = "xxxxx.azuredatabricks.net"
# DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/xxxxxx"
# DATABRICKS_TOKEN = "dapiXXXXXXXX"
# OPENAI_API_KEY = "sk-XXXXXXXX"

DATABRICKS_SERVER_HOSTNAME = st.secrets["dbc-b0932e94-bef9.cloud.databricks.com"]
DATABRICKS_HTTP_PATH = st.secrets["/sql/1.0/warehouses/6c3031561d363a1a"]
DATABRICKS_TOKEN = st.secrets["dapi123e7cc240dcd8f8f347a42543d77170"]

# OpenAI client otomatis pakai OPENAI_API_KEY dari secrets
client = OpenAI(api_key=st.secrets["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"])

# Batas maksimum karakter CSV yang dikirim ke model
MAX_CSV_CHARS = 30000


# =========================
# Fungsi ambil data khotbah
# =========================
def load_khotbah(limit_rows: int = 100) -> pd.DataFrame:
    """
    Ambil data dari tabel khotbah.`01_curated`.pdf_khotbah_ai_analysis
    dan kembalikan sebagai pandas DataFrame.
    """
    query = f"""
        SELECT *
        FROM khotbah.`01_curated`.pdf_khotbah_ai_analysis
        LIMIT {limit_rows}
    """

    with sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as connection:
        df = pd.read_sql(query, connection)

    return df


# =========================
# Fungsi panggil ChatGPT
# =========================
def ask_chatgpt(full_prompt: str) -> str:
    """
    Kirim prompt ke ChatGPT (gpt-4.1-mini) dan balikan teks jawabannya.
    """
    resp = client.responses.create(
        model="gpt-5.1",  # bisa diganti "gpt-4.1" kalau mau
        input=[
            {
                "role": "system",
                "content": (
                    "Kamu adalah asisten AI yang menganalisis data khotbah gereja "
                    "dan menjawab dalam bahasa Indonesia yang jelas."
                ),
            },
            {"role": "user", "content": full_prompt},
        ],
    )
    return resp.output[0].content[0].text


# =========================
# STREAMLIT UI
# =========================
st.set_page_config(page_title="Khotbah AI", layout="wide")

st.title("Khotbah AI Prompt")
st.write(
    "Aplikasi sederhana untuk mengirim prompt ke ChatGPT dengan sumber data dari "
    "`khotbah.`01_curated`.pdf_khotbah_ai_analysis` di Databricks."
)

# Pilih jumlah baris
limit_rows = st.slider(
    "Jumlah baris yang diambil dari tabel Databricks",
    min_value=10,
    max_value=500,
    value=100,
    step=10,
)

# Prompt dari kamu
default_instruction = (
    "Tolong analisis dataset khotbah ini:\n"
    "- Ringkas tema utama dari khotbah.\n"
    "- Berikan 5–10 insight penting.\n"
    "- Jelaskan dengan bahasa yang mudah dimengerti oleh jemaat."
)

user_instruction = st.text_area(
    "Instruksi / Prompt ke ChatGPT",
    value=default_instruction,
    height=220,
)

st.markdown("---")

if st.button("Kirim ke ChatGPT"):
    try:
        # 1. Ambil data dari Databricks
        with st.spinner("Mengambil data dari Databricks..."):
            df = load_khotbah(limit_rows=limit_rows)

        st.success(f"Berhasil ambil {len(df)} baris dan {len(df.columns)} kolom.")
        st.subheader("Preview Data (5 baris pertama)")
        st.dataframe(df.head())

        # 2. Convert ke CSV dan batasi panjang
        csv_text = df.to_csv(index=False)
        if len(csv_text) > MAX_CSV_CHARS:
            csv_text_short = csv_text[:MAX_CSV_CHARS]
            st.warning(
                f"CSV panjangnya {len(csv_text)} karakter. "
                f"Hanya {MAX_CSV_CHARS} karakter pertama yang dikirim ke model."
            )
        else:
            csv_text_short = csv_text

        # 3. Buat prompt final
        full_prompt = f"""
        Berikut adalah data khotbah dari tabel khotbah.`01_curated`.pdf_khotbah_ai_analysis dalam format CSV (dipotong bila terlalu panjang).
        
        INSTRUKSI SAYA:
        {user_instruction}
        
        DATA CSV:
        ```csv
        {csv_text_short}
        """.strip()
        
        # 4. Panggil ChatGPT
        with st.spinner("Meminta jawaban dari ChatGPT..."):
            answer = ask_chatgpt(full_prompt)
        # 5. Tampilkan jawaban
        st.markdown("### Jawaban ChatGPT")
        st.markdown(answer)

except Exception as e:
    st.error(f"Terjadi error: {e}")
