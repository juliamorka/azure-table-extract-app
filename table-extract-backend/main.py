import uuid
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from azure.storage.blob import BlobServiceClient
from azure.data.tables import TableServiceClient
import os

AZURE_STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
BLOB_CONTAINER_INPUT = "input"
BLOB_CONTAINER_OUTPUT = "output"
TABLE_NAME = "jobs"

blob_service = BlobServiceClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING
)
table_service = TableServiceClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING
)
table_client = table_service.get_table_client(TABLE_NAME)

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

@app.post("/jobs")
async def create_job_endpoint(file: UploadFile = File(...)):
    if not file.filename.lower().endswith((".pdf", ".docx")):
        raise HTTPException(status_code=400, detail="Unsupported file type")
    job_id = str(uuid.uuid4())
    blob_client = blob_service.get_blob_client(
        container=BLOB_CONTAINER_INPUT,
        blob=f"{job_id}/{file.filename}"
    )
    blob_client.upload_blob(
        await file.read(),
        overwrite=True
    )
    create_job(job_id, file.filename)
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
    blob_client = blob_service.get_blob_client(
        container=BLOB_CONTAINER_OUTPUT,
        blob=blob_name
    )
    if not blob_client.exists():
        raise HTTPException(status_code=404, detail="Result not found")
    return RedirectResponse(blob_client.url)
