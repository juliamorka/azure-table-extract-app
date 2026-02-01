import streamlit as st
import requests
import time
import os
import pandas as pd

FUNCTION_URL = os.environ.get("FUNCTION_URL")

st.set_page_config(
    page_title="TableExtract",
    page_icon="📄",
    layout="centered"
)

st.title("TableExtract")

st.markdown(
    """
    Fast and convenient tables extraction for all your PDF files.
    """
)

uploaded_file = st.file_uploader(
    "Upload a file",
    type=["pdf"]
)

if uploaded_file is not None:
    if st.button("Extract"):
        with st.spinner("Extracting..."):
            response = requests.post(
                FUNCTION_URL,
                files={"file": uploaded_file}
            )
            try:
                result = response.json()
        
                for table in result["tables"]:
                    df = pd.DataFrame(table["data"])
                    st.dataframe(df)
            except Exception as e:
                st.write(response)
                st.write("Please try again or reach out to the app administrators for help!")
