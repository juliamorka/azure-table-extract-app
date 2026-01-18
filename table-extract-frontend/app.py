import streamlit as st
import requests
import time

API_BASE_URL = st.secrets.get("API_BASE_URL")
UPLOAD_ENDPOINT = f"{API_BASE_URL}/jobs"
STATUS_ENDPOINT = f"{API_BASE_URL}/jobs/{{job_id}}"
DOWNLOAD_ENDPOINT = f"{API_BASE_URL}/jobs/{{job_id}}/result"
POLL_INTERVAL_SEC = 3

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
            files = {
                "file": (uploaded_file.name, uploaded_file, uploaded_file.type)
            }

            try:
                response = requests.post(
                    UPLOAD_ENDPOINT,
                    files=files,
                    timeout=30
                )
                response.raise_for_status()
                job_id = response.json()["job_id"]

                st.session_state["job_id"] = job_id
                st.success(f"Job created with ID: {job_id}")

            except requests.RequestException as e:
                st.error("An error has occured while triggering the extraction! Please try again.")
                st.exception(e)

if "job_id" in st.session_state:
    job_id = st.session_state["job_id"]

    st.divider()
    st.subheader("Current job status")

    status_placeholder = st.empty()

    while True:
        try:
            response = requests.get(
                STATUS_ENDPOINT.format(job_id=job_id),
                timeout=10
            )
            response.raise_for_status()

            data = response.json()
            status = data["status"]

            status_placeholder.info(f"**{status}**")

            if status == "DONE":
                st.success("Extraction finished!")
                break

            if status == "ERROR":
                st.error("An error has occured during the extraction.")
                break

            time.sleep(POLL_INTERVAL_SEC)

        except requests.RequestException as e:
            st.error("An error has occured when fetching the job status.")
            st.exception(e)
            break

    if status == "DONE":
        st.divider()
        st.subheader("Download the result")

        format_choice = st.selectbox(
            "Format",
            ["csv", "json"]
        )

        download_url = (
            DOWNLOAD_ENDPOINT.format(job_id=job_id)
            + f"?format={format_choice}"
        )

        st.markdown(
            f"[Download in ({format_choice.upper()})]({download_url})"
        )
