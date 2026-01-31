import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from uuid import uuid4
import os
import json
from docx2pdf import convert
import tempfile

STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]
DOCUMENT_INTELLIGENCE_ENDPOINT = os.environ["DOCUMENT_INTELLIGENCE_ENDPOINT"]
DOCUMENT_INTELLIGENCE_KEY = os.environ["DOCUMENT_INTELLIGENCE_KEY"]
BLOB_CONN_STR = os.environ["BLOB_CONN_STR"]

def convert_docx_to_pdf(file_bytes):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as tmp_docx:
        tmp_docx.write(file_bytes)
        tmp_docx.flush()
        pdf_path = tmp_docx.name.replace(".docx", ".pdf")
        convert(tmp_docx.name, pdf_path)
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
    return pdf_bytes

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing document for table extraction")

    try:
        file = req.files.get("file")
        if not file:
            return func.HttpResponse("Missing file in request", status_code=400)

        file_bytes = file.read()
        filename = file.filename or f"{uuid4()}.pdf"
        file_ext = filename.lower().split(".")[-1]

        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        blob_name = f"uploads/{uuid4()}-{filename}"
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(file_bytes, overwrite=True)

        if file_ext == "docx":
            logging.info("Converting DOCX to PDF")
            file_bytes = convert_docx_to_pdf(file_bytes)
            # zmieniamy nazwę pliku, żeby miała .pdf
            filename = filename.rsplit(".", 1)[0] + ".pdf"

        elif file_ext != "pdf":
            return func.HttpResponse(
                "Unsupported file type. Only PDF and DOCX are supported.",
                status_code=400
            )

        document_client = DocumentAnalysisClient(
            endpoint=DOCUMENT_INTELLIGENCE_ENDPOINT,
            credential=AzureKeyCredential(DOCUMENT_INTELLIGENCE_KEY),
        )

        poller = document_client.begin_analyze_document(
            model_id="prebuilt-layout",
            document=file_bytes
        )

        result = poller.result()

        tables_output = []

        for table_index, table in enumerate(result.tables):
            rows = table.row_count
            cols = table.column_count
            table_data = [[""] * cols for _ in range(rows)]

            for cell in table.cells:
                table_data[cell.row_index][cell.column_index] = cell.content

            tables_output.append({
                "table_index": table_index,
                "row_count": rows,
                "column_count": cols,
                "data": table_data
            })

        return func.HttpResponse(
            body=json.dumps({
                "file_name": filename,
                "table_count": len(tables_output),
                "tables": tables_output
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception("Table extraction failed")
        return func.HttpResponse(
            f"Internal server error: {str(e)}",
            status_code=500
        )
