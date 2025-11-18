import streamlit as st
import pandas as pd
import unicodedata
import pyarrow.parquet as pq

# -----------------------------------------------------
# PAGE SETTINGS
# -----------------------------------------------------
st.set_page_config(page_title="Tiruppur District Voter Search", layout="wide")

# -----------------------------------------------------
# CUSTOM CSS
# -----------------------------------------------------
st.markdown("""
<style>
body {
    background-color: #C2D9EA !important;
    font-family: 'Segoe UI', sans-serif;
}
h2 {
    color: #6a64ef;
    text-align: center;
    text-shadow: 1px 1px 2px #aaa;
}
.stButton > button {
    background-color: #c19962;
    color: white;
    font-weight: bold;
    border-radius: 10px;
    padding: 10px 20px;
}
.stButton > button:hover {
    background-color: #45a049;
}
.block-container { 
    padding-top: 1rem; 
    padding-left: 0.6rem; 
    padding-right: 0.6rem; 
}
.dataframe th {
    background-color: #1f77b4 !important;
    color: white !important;
    text-align: center !important;
}
.dataframe td {
    text-align: center !important;
}
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------
# HEADER
# -----------------------------------------------------
st.markdown("""
<div style='height:45px;'></div>
<h2 style='color:#b56edc;'>திருப்பூர் மாவட்ட வாக்காளர் விவரம் - 2002</h2>
""", unsafe_allow_html=True)

# -----------------------------------------------------
# FILE MAPPING
# -----------------------------------------------------
FILE_MAP = {
    "102 - அவினாசி (தனி) (Avanashi (SC))": "AC_102_Avanashi.parquet",
    "111 - உடுமலைப்பேட்டை (Udumalpet)": "AC_111_Udumalpet.parquet",
    "112 - தாராபுரம் (தனி) (Dharapuram (SC))": "AC_112_Dharapuram.parquet",
    "113 - வெள்ளகோவில் (Vellakoil)": "AC_113_Vellakovil.parquet",
    "114 - பொங்கலூர் (Pongalur)": "AC_114_Pongalur.parquet",
    "115 - பல்லடம் (Palladam)": "AC_115_Palladam.parquet",
    "116 - திருப்பூர் (Tiruppur)": "AC_116_Tiruppur.parquet",
    "117 - காங்கேயம் (Kangayam)": "AC_117_Kangayam.parquet",
}

# -----------------------------------------------------
# DATA LOADING WITH PRE-NORMALIZED COLUMNS
# -----------------------------------------------------
@st.cache_resource
def load_all_parquet():
    data = {}
    for ac_name, pq_file in FILE_MAP.items():
        try:
            df = pd.read_parquet(pq_file).copy()

            # Normalize Tamil columns
            for col in ["FM_NAME_V2", "RLN_FM_NM_V2"]:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
                    df[col] = df[col].apply(lambda x: unicodedata.normalize("NFC", x))

            # Pre-normalized lowercase columns for fast search
            df["FM_NAME_NORM"] = df["FM_NAME_V2"].apply(lambda x: unicodedata.normalize("NFC", x).lower())
            df["RLN_NAME_NORM"] = df["RLN_FM_NM_V2"].apply(lambda x: unicodedata.normalize("NFC", x).lower())

            data[ac_name] = df

        except Exception as e:
            st.error(f"❌ Failed loading {pq_file}: {e}")
            data[ac_name] = None

    return data

with st.spinner("📦 Loading constituency data..."):
    DATA = load_all_parquet()

# -----------------------------------------------------
# SORT CONSTITUENCIES BY NUMBER
# -----------------------------------------------------
sorted_keys = sorted(FILE_MAP.keys(), key=lambda x: int(x.split("-")[0].strip()))

ac = st.selectbox(
    "தொகுதியைத் தேர்ந்தெடுக்கவும்:",
    ["-- Choose --"] + sorted_keys
)

if ac == "-- Choose --":
    st.stop()

df = DATA.get(ac)
if df is None:
    st.error("❌ இந்த தொகுதி கோப்பை ஏற்ற முடியவில்லை.")
    st.stop()

st.success(f"📌 {ac} — {len(df)} வரிசைகள் கிடைத்தன.")

# -----------------------------------------------------
# SEARCH INPUTS
# -----------------------------------------------------
st.markdown("### 📝 விவரங்களை உள்ளிடவும் (Enter Details)")

name_input = st.text_input("வாக்காளர் பெயர் (Tamil Only)", placeholder="உதா: பிரகாஷ்")
rname_input = st.text_input("தந்தை / கணவர் பெயர் (Tamil Only)", placeholder="உதா: வேலுசாமி")

# -----------------------------------------------------
# INPUT CLEANING
# -----------------------------------------------------
def clean(text):
    """Normalize unicode, lowercase, and remove extra spaces."""
    text = " ".join(text.split()).strip()
    return unicodedata.normalize("NFC", text).lower()

# -----------------------------------------------------
# SEARCH BUTTON
# -----------------------------------------------------
if st.button("🔍 தேடு (Search)"):
    name_input = clean(name_input)
    rname_input = clean(rname_input)

    if not name_input and not rname_input:
        st.warning("⚠️ குறைந்தது ஒரு பெயரை உள்ளிடுங்கள்.")
        st.stop()

    results = df.copy()

    # --- Search Logic ---
    if name_input:
        results = results[results["FM_NAME_NORM"].str.contains(name_input, na=False)]

    if rname_input:
        results = results[results["RLN_NAME_NORM"].str.contains(rname_input, na=False)]

    # --- Results Display ---
    if results.empty:
        st.error("❌ பொருந்தும் பதிவுகள் இல்லை.")
    else:
        st.success(f"✔ {len(results)} பதிவுகள் கிடைத்தன.")
        st.dataframe(results, use_container_width=True)

        # Download button
        csv_data = results.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            "⬇️ பதிவுகளை CSV ஆக பதிவிறக்கவும்", 
            csv_data, 
            f"{ac}_voter_results.csv", 
            "text/csv"
        )
