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
