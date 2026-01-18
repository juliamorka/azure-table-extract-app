import uuid
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import RedirectResponse
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.data.tables import TableServiceClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from datetime import datetime, timedelta
import os
import pandas as pd
import io
import json
import asyncio

AZURE_STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
AZURE_FORM_RECOGNIZER_ENDPOINT = os.environ["AZURE_FORM_RECOGNIZER_ENDPOINT"]
AZURE_FORM_RECOGNIZER_KEY = os.environ["AZURE_FORM_RECOGNIZER_KEY"]

BLOB_CONTAINER_INPUT = "input"
BLOB_CONTAINER_OUTPUT = "output"
TABLE_NAME = "jobs"

blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
table_service = TableServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
table_client = table_service.get_table_client(TABLE_NAME)

document_client = DocumentAnalysisClient(
    endpoint=AZURE_FORM_RECOGNIZER_ENDPOINT,
    credential=AzureKeyCredential(AZURE_FORM_RECOGNIZER_KEY)
)

app = FastAPI(title="TableXtract API")

def create_job(job_id: str, filename: str):
    table_client.create_entity({
        "PartitionKey": "jobs",
        "RowKey": job_id,
        "status": "PENDING",
        "filename": filename
    })

def update_status(job_id: str, status: str):
    entity = table_client.get_entity("jobs", job_id)
    entity["status"] = status
    table_client.update_entity(entity)

def get_job(job_id: str):
    try:
        return table_client.get_entity("jobs", job_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Job not found")

async def process_document(job_id: str, blob_name: str):
    update_status(job_id, "PROCESSING")

    # Generate SAS URL for Form Recognizer access
    blob_client = blob_service.get_blob_client(BLOB_CONTAINER_INPUT, blob_name)
    sas_url = blob_client.url + "?" + generate_blob_sas(
        account_name=blob_service.account_name,
        container_name=BLOB_CONTAINER_INPUT,
        blob_name=blob_name,
        account_key=blob_service.credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )

    # Analyze document using prebuilt-document model
    poller = document_client.begin_analyze_document_from_url("prebuilt-document", sas_url)
    result = poller.result()

    # Extract tables
    tables = []
    for table in result.tables:
        rows = []
        for r in range(len(table.rows)):
            row = []
            for c in range(len(table.columns)):
                cell = table.get_cell(r, c)
                row.append(cell.content if cell else "")
            rows.append(row)
        tables.append(pd.DataFrame(rows))

    # Save results in CSV and JSON
    for fmt in ["csv", "json"]:
        out_blob_client = blob_service.get_blob_client(
            container=BLOB_CONTAINER_OUTPUT,
            blob=f"{job_id}/tables.{fmt}"
        )
        if fmt == "csv":
            with io.StringIO() as f:
                for i, df in enumerate(tables):
                    df.to_csv(f, index=False)
                out_blob_client.upload_blob(f.getvalue(), overwrite=True)
        else:
            data = [df.to_dict(orient="records") for df in tables]
            out_blob_client.upload_blob(json.dumps(data), overwrite=True)

    update_status(job_id, "DONE")

@app.post("/jobs")
async def create_job_endpoint(file: UploadFile = File(...)):
    if not file.filename.lower().endswith((".pdf", ".docx")):
        raise HTTPException(status_code=400, detail="Unsupported file type")

    job_id = str(uuid.uuid4())
    blob_name = f"{job_id}/{file.filename}"

    # Upload to input container
    blob_client = blob_service.get_blob_client(BLOB_CONTAINER_INPUT, blob_name)
    blob_client.upload_blob(await file.read(), overwrite=True)

    # Create job in Table Storage
    create_job(job_id, file.filename)

    # Start document processing asynchronously
    asyncio.create_task(process_document(job_id, blob_name))

    return {"job_id": job_id}

@app.get("/jobs/{job_id}")
def get_status(job_id: str):
    job = get_job(job_id)
    return {"status": job["status"]}

@app.get("/jobs/{job_id}/result")
def download_result(job_id: str, format: str = "csv"):
    if format not in ("csv", "json"):
        raise HTTPException(status_code=400, detail="Invalid format")

    job = get_job(job_id)
    if job["status"] != "DONE":
        raise HTTPException(status_code=409, detail="Result not ready")

    blob_name = f"{job_id}/tables.{format}"
    blob_client = blob_service.get_blob_client(BLOB_CONTAINER_OUTPUT, blob_name)

    if not blob_client.exists():
        raise HTTPException(status_code=404, detail="Result not found")

    return RedirectResponse(blob_client.url)
