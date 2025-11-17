import streamlit as st
import pandas as pd
import unicodedata
import html
import pyarrow.parquet as pq
import pyarrow as pa

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
<h2>திருப்பூர் மாவட்ட வாக்காளர் விவரம் - 2002</h2>
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
# SAFE PARQUET LOADER (FIXES DUPLICATE COLUMNS)
# -----------------------------------------------------
def safe_read_parquet(path):

    table = pq.read_table(path)

    # Remove internal/metadata columns
    bad_columns = {
        "__fragment_index", "__batch_index", "__last_in_fragment",
        "__filename"
    }

    columns = [c for c in table.column_names if c not in bad_columns]
    table = table.select(columns)

    # Fix duplicate column names
    new_cols = []
    seen = {}

    for c in table.column_names:
        if c not in seen:
            seen[c] = 1
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")

    table = table.rename_columns(new_cols)

    return table.to_pandas()


# -----------------------------------------------------
# LOAD ALL PARQUET FILES SAFELY
# -----------------------------------------------------
@st.cache_resource
def load_all_parquet():
    data = {}
    for ac_name, pq_file in FILE_MAP.items():
        try:
            df = safe_read_parquet(pq_file)

            # Normalize Tamil fields
            for col in ["FM_NAME_V2", "RLN_FM_NM_V2"]:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
                    df[col] = df[col].apply(lambda x: unicodedata.normalize("NFC", x))

            data[ac_name] = df

        except Exception as e:
            st.error(f"❌ Failed loading {pq_file}: {e}")
            data[ac_name] = None

    return data


with st.spinner("📦 Loading constituency data..."):
    DATA = load_all_parquet()

# -----------------------------------------------------
# SELECT CONSTITUENCY
# -----------------------------------------------------
sorted_keys = sorted(FILE_MAP.keys(), key=lambda x: int(x.split()[0]))

ac = st.selectbox("தொகுதியைத் தேர்ந்தெடுக்கவும்:", ["-- Choose --"] + sorted_keys)

if ac == "-- Choose --":
    st.stop()

df = DATA.get(ac)

if df is None:
    st.error("❌ இந்த தொகுதி கோப்பை ஏற்ற முடியவில்லை.")
    st.stop()

st.success(f"📌 {ac} — {len(df)} வரிசைகள் கிடைத்தன.")

# -----------------------------------------------------
# INPUT SECTION
# -----------------------------------------------------
st.markdown("### 📝 விவரங்களை உள்ளிடவும் (Enter Details)")

name_input = st.text_input("வாக்காளர் பெயர் (Tamil Only)", placeholder="உதா: பிரகாஷ்")
rname_input = st.text_input("தந்தை / கணவர் பெயர் (Tamil Only)", placeholder="உதா: வேலுசாமி")

# -----------------------------------------------------
# CLEANING
# -----------------------------------------------------
def clean(x: str):
    x = " ".join(x.split()).strip()
    return unicodedata.normalize("NFC", x)

# -----------------------------------------------------
# HIGHLIGHT FUNCTION
# -----------------------------------------------------
def safe_highlight(text, term):
    if not term:
        return text
    escaped = html.escape(text)
    term_esc = html.escape(term)
    return escaped.replace(term_esc, f"<mark><b>{term_esc}</b></mark>")

# -----------------------------------------------------
# SEARCH LOGIC
# -----------------------------------------------------
if st.button("🔍 தேடு (Search)"):

    name_input = clean(name_input)
    rname_input = clean(rname_input)

    if not name_input and not rname_input:
        st.warning("⚠️ குறைந்தது ஒரு பெயரை உள்ளிடுங்கள்.")
        st.stop()

    results = df.copy()

    def match(series, term):
        series_norm = series.astype(str).apply(lambda x: unicodedata.normalize("NFC", x))
        return series_norm.str.contains(term, case=False, na=False, regex=False)

    if name_input:
        results = results[match(results["FM_NAME_V2"], name_input)]

    if rname_input:
        results = results[match(results["RLN_FM_NM_V2"], rname_input)]

    if results.empty:
        st.error("❌ பொருந்தும் பதிவுகள் இல்லை.")
        st.stop()

    st.success(f"✔ {len(results)} பதிவுகள் கிடைத்தன.")

    # Highlight
    styled_df = results.copy()

    if name_input:
        styled_df["FM_NAME_V2"] = styled_df["FM_NAME_V2"].astype(str).apply(
            lambda x: safe_highlight(x, name_input))

    if rname_input:
        styled_df["RLN_FM_NM_V2"] = styled_df["RLN_FM_NM_V2"].astype(str).apply(
            lambda x: safe_highlight(x, rname_input))

    st.markdown("### 📋 முடிவுகள் (Results)")
    st.write(styled_df.to_html(escape=False, index=False), unsafe_allow_html=True)

    csv_data = results.to_csv(index=False).encode("utf-8-sig")

    st.download_button(
        "⬇️ பதிவுகளை CSV ஆக பதிவிறக்கவும்",
        csv_data,
        file_name=f"{ac}_voter_results.csv",
        mime="text/csv"
    )
