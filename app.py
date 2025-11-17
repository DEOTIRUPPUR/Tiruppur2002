import streamlit as st
import pandas as pd

# ----------------------------------------
# PAGE SETTINGS + MOBILE CSS
# ----------------------------------------
st.set_page_config(page_title="Coimbatore District Voter Search", layout="wide")

st.markdown("""
<style>
.block-container { padding-top: 1rem; padding-left: 0.6rem; padding-right: 0.6rem; }
input[type="text"] { font-size: 1.15rem; padding: 10px; }
.stButton > button { width: 100%; padding: 12px; font-size: 1.12rem; border-radius: 8px; }
.stDataFrame { overflow-x: auto !important; }
.dataframe td, .dataframe th {
    white-space: normal !important;
    word-break: break-word !important;
    font-size: 1.05rem;
    line-height: 1.35rem;
}
@media (max-width: 600px) {
  .stDataFrame > div { min-width: 1100px !important; }
}
</style>
""", unsafe_allow_html=True)

# ----------------------------------------
# HEADER (WITH EXTRA SPACE)
# ----------------------------------------
st.markdown(
    """
    <div style='height:25px;'></div>
    <h2 style='width:100%; text-align:center; font-size:1.6rem;
               white-space:normal; line-height:2.2rem; margin-top:10px;'>
        திருப்பூர் மாவட்ட வாக்காளர் விவரம் - 2002
    </h2>
    """,
    unsafe_allow_html=True
)

# ----------------------------------------
# FILE MAP WITH TAMIL SPELLINGS
# ----------------------------------------
FILE_MAP = {
    "102 - அவினாசி (தனி) (Avanashi (SC))": "AC_101_Mettupalayam.parquet",
    "111 - உடுமலைப்பேட்டை (Udumalpet)": "AC_103_Thondamuthur.parquet",
    "112 - தாராபுரம் (தனி) (Dharapuram (SC))": "AC_104_Singanallur.parquet",
    "113 - வெள்ளகோவில் (Vellakoil)": "AC_105_Coimbatore(West).parquet",
    "114 - பொங்கலூர் (Pongalur)": "AC_106_Coimbatore(East).parquet",
    "115 - பல்லடம் (Palladam)": "data/AC_115_Palladam.parquet",
    "116 - திருப்பூர் (Tiruppur)": "AC_108_Kinathukadavu.parquet",
    "117 - காங்கேயம் (Kangayam)": "AC_109_Pollachi.parquet",
}

# ----------------------------------------
# PRELOAD PARQUET FILES
# ----------------------------------------
@st.cache_resource
def load_all_parquet():
    data = {}
    for ac_name, pq_file in FILE_MAP.items():
        try:
            df = pd.read_parquet(pq_file)

            if "FM_NAME_V2" in df.columns:
                df["FM_NAME_V2"] = df["FM_NAME_V2"].astype(str).str.strip()

            if "RLN_FM_NM_V2" in df.columns:
                df["RLN_FM_NM_V2"] = df["RLN_FM_NM_V2"].astype(str).str.strip()

            data[ac_name] = df
        except:
            data[ac_name] = None
    return data

DATA = load_all_parquet()

# ----------------------------------------
# SORT CONSTITUENCIES
# ----------------------------------------
sorted_keys = sorted(FILE_MAP.keys(), key=lambda x: int(x.split()[0]))

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

# ----------------------------------------
# INPUT FIELDS
# ----------------------------------------
st.markdown("### 📝 விவரங்களை உள்ளிடவும் (Enter Details)")

name_input = st.text_input(
    "வாக்காளர் பெயர் (Voter's Name) – தமிழ் மட்டும் (Tamil Only)",
    placeholder="உதா: முருகன்"
)

rname_input = st.text_input(
    "தந்தை / கணவர் பெயர் (Father's / Husband's Name) – தமிழ் மட்டும் (Tamil Only)",
    placeholder="உதா: மதியழகன்"
)

# ----------------------------------------
# CLEAN INPUT
# ----------------------------------------
def clean(x):
    return " ".join(x.split()).strip()

# ----------------------------------------
# SEARCH
# ----------------------------------------
if st.button("🔍 தேடு (Search)"):

    name_input = clean(name_input)
    rname_input = clean(rname_input)

    if not name_input and not rname_input:
        st.warning("⚠️ குறைந்தது ஒரு பெயரை உள்ளிடுங்கள்.")
        st.stop()

    results = df.copy()

    def match(series, value):
        return series.astype(str).str.contains(value, case=False, na=False, regex=False)

    if name_input:
        results = results[match(results["FM_NAME_V2"], name_input)]

    if rname_input:
        results = results[match(results["RLN_FM_NM_V2"], rname_input)]

    if results.empty:
        st.error("❌ பொருந்தும் பதிவுகள் இல்லை.")
    else:
        st.success(f"✔ {len(results)} பதிவுகள் கிடைத்தன.")
        st.dataframe(results, use_container_width=True)
