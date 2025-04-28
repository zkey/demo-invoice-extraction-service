import os
import io
import json
import logging
import uuid
import base64 # Import base64 for encoding file content
from typing import Optional, Dict
from dotenv import load_dotenv
from fastapi import FastAPI, File, UploadFile, HTTPException, status, Path
from pydantic import BaseModel
import redis

# --- Configuration & Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = 6379
REDIS_DB = 0

# --- Redis Connection ---
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    redis_client.ping()
    logging.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.exceptions.ConnectionError as e:
    logging.error(f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {e}")
    redis_client = None

# --- Pydantic Models ---
try:
    from schema import GeneralizedInvoiceData
except ImportError:
    logging.error("Could not import GeneralizedInvoiceData from schema.py.")
    class GeneralizedInvoiceData(BaseModel): pass # Dummy

class TaskCreationResponse(BaseModel):
    task_id: str
    status_url: str

class TaskStatus(BaseModel):
    status: str
    result: Optional[Dict] = None
    error: Optional[str] = None

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Invoice Extractor API",
    description="API to submit invoices for extraction and poll for results.",
)

# --- API Endpoints ---

@app.post(
    "/extract_invoice/",
    response_model=TaskCreationResponse,
    status_code=status.HTTP_202_ACCEPTED
)
async def submit_invoice_for_extraction(
    file: UploadFile = File(...)
):
    """
    Accepts an invoice (PDF/TXT), stores its raw content and type
    for background processing, and returns a task ID for polling.
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Service Unavailable: Cannot connect to Redis.")

    logging.info(f"Received file: {file.filename}, Content-Type: {file.content_type}")

    # 1. Validate Content Type FIRST
    if file.content_type not in ['application/pdf', 'text/plain']:
        logging.warning(f"Unsupported file type received: {file.content_type}")
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Unsupported file type: {file.content_type}. Please upload PDF or TXT.",
        )

    try:
        # 2. Read raw file content
        file_content_bytes = await file.read()
        if not file_content_bytes:
             raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Received empty file.",
            )

        # 3. Encode content for storage in Redis (JSON compatible)
        file_content_b64 = base64.b64encode(file_content_bytes).decode('utf-8')

        # 4. Generate Task ID and store task data in Redis
        task_id = str(uuid.uuid4())
        task_key = f"task:{task_id}"
        task_data = {
            "status": "PENDING",
            "original_filename": file.filename,
            "content_type": file.content_type, # Store original content type
            "file_content_b64": file_content_b64, # Store base64 encoded content
            "result": None,
            "error": None
        }

        # Store as JSON string in Redis
        redis_client.set(task_key, json.dumps(task_data))

        logging.info(f"Task {task_id} created and stored in Redis for file: {file.filename}")

        status_url = app.url_path_for("get_task_status", task_id=task_id)
        return {"task_id": task_id, "status_url": status_url}

    except HTTPException as http_exc:
        raise http_exc # Re-raise FastAPI exceptions
    except Exception as e:
        # Catch errors during file reading or Redis interaction
        logging.error(f"Unexpected error processing file {file.filename}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error during task submission.")

@app.get(
    "/tasks/{task_id}",
    response_model=TaskStatus,
    name="get_task_status"
)
async def get_task_status(task_id: str = Path(...)):
    """
    Poll this endpoint to check the status and retrieve the result of an extraction task.
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Service Unavailable: Cannot connect to Redis.")

    task_key = f"task:{task_id}"
    task_data_json = redis_client.get(task_key)

    if not task_data_json:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

    try:
        task_data = json.loads(task_data_json)
        # Remove sensitive file content before returning status to client
        task_data.pop("file_content_b64", None)
        task_data.pop("content_type", None) # Optionally remove content_type too

        return TaskStatus(**task_data)

    except json.JSONDecodeError:
         logging.error(f"Failed to decode JSON data for task {task_id} from Redis.")
         raise HTTPException(status_code=500, detail="Internal server error: Invalid task data.")
    except Exception as e:
        logging.error(f"Error retrieving task {task_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error retrieving task status.")
