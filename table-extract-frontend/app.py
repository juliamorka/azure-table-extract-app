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
    Asynchronuous tables extraction for your PDF and Docx files.
    """
)

uploaded_file = st.file_uploader(
    "Upload a file",
    type=["pdf", "docx"]
)

if uploaded_file is not None:
    if st.button("Extract"):
        with st.spinner("Queuing..."):
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
                st.write(type(response))
                raise e
