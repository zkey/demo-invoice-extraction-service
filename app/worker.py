import os
import io        # Import io
import json
import logging
import time
import base64    # Import base64
from typing import Dict, Optional
from dotenv import load_dotenv
from openai import OpenAI
import redis
import PyPDF2    # Import PyPDF2

# --- Configuration & Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = 6379
REDIS_DB = 0
WORKER_POLL_INTERVAL = 1

# --- Schema Import ---
try:
    from schema import GeneralizedInvoiceData, get_invoice_schema_json_string
except ImportError:
    logging.error("Could not import from schema.py.")
    class GeneralizedInvoiceData: pass
    def get_invoice_schema_json_string(): return "{}"

# --- OpenAI Client Initialization ---
try:
    openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    if not os.environ.get("OPENAI_API_KEY"):
        logging.warning("OPENAI_API_KEY environment variable not set or empty.")
        openai_client = None
    LLM_MODEL = "gpt-4o"
except Exception as e:
    logging.error(f"Error initializing OpenAI client: {e}")
    openai_client = None

# --- Redis Connection ---
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    redis_client.ping()
    logging.info(f"Worker successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.exceptions.ConnectionError as e:
    logging.error(f"Worker failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {e}")
    redis_client = None

# --- Helper Functions (File Reading - Moved here) ---
def read_pdf(file_stream: io.BytesIO) -> str:
    """Reads text content from a PDF file stream."""
    try:
        reader = PyPDF2.PdfReader(file_stream)
        text = ""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        return text
    except Exception as e:
        # Log specific error during PDF reading
        logging.error(f"PyPDF2 error reading PDF stream: {e}", exc_info=True)
        # Re-raise a more specific error for the worker to catch
        raise ValueError(f"Failed to read PDF content: {e}")

def read_txt(file_stream: io.BytesIO) -> str:
    """Reads text content from a TXT file stream."""
    try:
        # Attempt common encodings if utf-8 fails
        try:
            return file_stream.read().decode("utf-8")
        except UnicodeDecodeError:
            logging.warning("UTF-8 decoding failed, trying latin-1.")
            file_stream.seek(0) # Reset stream position
            return file_stream.read().decode("latin-1")
    except Exception as e:
        logging.error(f"Error reading TXT stream: {e}", exc_info=True)
        raise ValueError(f"Failed to read TXT content: {e}")

# --- LLM Extraction Logic (Remains the same) ---
def extract_invoice_data_with_llm(document_text: str) -> Dict:
    """Uses an LLM to extract structured data from document text."""
    # ... (Keep the existing LLM call logic exactly as it was) ...
    if not openai_client:
         logging.error("LLM client is not configured (API key missing?). Cannot process request.")
         # Raise an error here so the task is marked FAILED
         raise ConnectionError("LLM client not available")

    json_schema_string = get_invoice_schema_json_string()
   
    # --- LLM Prompt Engineering ---
    # Ensure the schema string is correctly inserted
    prompt = f"""
    You are an expert AI assistant specializing in extracting structured data from invoice documents, regardless of language (e.g., English, German).

    Your task is to analyze the provided invoice text and extract relevant information according to the JSON schema defined below.

    **CRITICAL INSTRUCTIONS:**

    1.  **Output Format:** You MUST return the extracted information ONLY as a single, valid JSON object. Do not include *any* introductory text, explanations, summaries, code block markers (like ```json), or apologies before or after the JSON object itself. Your entire response must be just the JSON.
    2.  **Schema Adherence:** The JSON object MUST strictly adhere to the structure, field names, and data types defined in the provided schema. Pay close attention to nested objects (`vendor`, `customer`, `line_items`). Use the field descriptions in the schema as your guide.
    3.  **Mapping & Language:** Use the field descriptions within the schema to correctly map information from the invoice text, even if the terminology or language differs.
    4.  **Missing Data:** If a specific piece of information for a field defined in the schema (including fields within nested objects) is not found or cannot be reliably determined from the invoice text, you MUST use the JSON value `null` for that field. Do not omit the field key itself; represent its absence explicitly with `null`.
    5.  **Inference (Use Carefully):** Infer `currency`, `due_date`, `payment_status` only if clearly indicated or logically derivable. Default to `null` if ambiguous.
    6.  **Combined Fields:** Populate the `payment_terms_or_notes` field by concatenating relevant text like payment instructions, deadlines, bank details, and other miscellaneous notes. If no such text exists, use `null`.
    7.  **Line Items:** Extract all distinct line items into the `line_items` list. Ensure `line_total` reflects the total for that specific line before tax.
    8.  **Handling Unrecognized Data:** If you find relevant information in the invoice text that does not clearly map to any of the specific fields defined in the schema (at the top level, within vendor/customer details, or within line items), place that information as key-value pairs inside the corresponding `other_data` dictionary field. Use descriptive keys based on the source text (e.g., `"Project Code": "XYZ"`). If no such extra data is found for a section, leave its `other_data` field as `null`.

    **Target JSON Schema:**
    ```json
    {json_schema_string}
    ```

    Invoice Text to Analyze:
    {document_text[:8000]}

    JSON Output:
    """
    try:
        logging.info(f"Sending request to LLM model: {LLM_MODEL}")
        response = openai_client.chat.completions.create(
            model=LLM_MODEL,
            response_format={ "type": "json_object" },
            messages=[
                {"role": "system", "content": "You are an expert invoice data extraction assistant. You only output valid JSON matching the provided schema."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
        )
        json_string = response.choices[0].message.content
        logging.info("Received response from LLM.")
        extracted_data = json.loads(json_string)
        return extracted_data

    except json.JSONDecodeError as e:
        # Log the raw content that failed parsing
        raw_content = "N/A"
        try:
            raw_content = response.choices[0].message.content
        except Exception: pass
        logging.error(f"LLM returned invalid JSON: {e}. Raw content: '{raw_content}'")
        raise ValueError(f"LLM returned invalid JSON") # Raise to signal failure
    except Exception as e:
        logging.error(f"Error during LLM API call: {e}", exc_info=True)
        raise # Re-raise other exceptions (like API errors) to signal failure


# --- Worker Processing Logic ---
def process_task(task_id: str, task_data: Dict):
    """Processes a single task retrieved from Redis."""
    task_key = f"task:{task_id}"
    original_filename = task_data.get("original_filename", "N/A")
    logging.info(f"Processing task {task_id} for file: {original_filename}")

    file_text = None # Initialize file_text

    try:
        # 1. Update status to PROCESSING
        task_data["status"] = "PROCESSING"
        # Remove file content before updating status to avoid storing it longer than needed
        # We keep it in the local task_data dict for this function's scope
        redis_task_update = task_data.copy()
        redis_task_update.pop("file_content_b64", None)
        redis_client.set(task_key, json.dumps(redis_task_update))

        # 2. Retrieve content type and decode file content
        content_type = task_data.get("content_type")
        file_content_b64 = task_data.get("file_content_b64")

        if not content_type or not file_content_b64:
            raise ValueError("Missing 'content_type' or 'file_content_b64' in task data.")

        logging.debug(f"Task {task_id}: Decoding base64 content...")
        file_content_bytes = base64.b64decode(file_content_b64)
        file_stream = io.BytesIO(file_content_bytes)
        logging.debug(f"Task {task_id}: Decoded {len(file_content_bytes)} bytes.")

        # 3. Extract text based on content type
        logging.info(f"Task {task_id}: Extracting text for type {content_type}...")
        if content_type == 'application/pdf':
            file_text = read_pdf(file_stream)
        elif content_type == 'text/plain':
            file_text = read_txt(file_stream)
        else:
            # Should not happen if API validates, but handle defensively
            raise ValueError(f"Unsupported content_type '{content_type}' found in task data.")

        file_stream.close() # Close the stream

        if not file_text or not file_text.strip():
            raise ValueError("No text could be extracted from the document.")

        logging.info(f"Task {task_id}: Text extracted successfully (length: {len(file_text)}).")

        # 4. Extract data using LLM
        extracted_data_dict = extract_invoice_data_with_llm(file_text)

        logging.info(f"Task {task_id}: LLM extraction successful.")

        # 5. Optional: Validate with Pydantic
        validated_result = None
        try:
            # Use the raw dict from LLM for validation
            validated_data = GeneralizedInvoiceData(**extracted_data_dict)
            validated_result = validated_data.model_dump() # Get dict from Pydantic model
            logging.info(f"Task {task_id}: Pydantic validation successful.")
        except Exception as validation_error:
            # Log validation failure but proceed with raw data if needed, or fail task
            logging.error(f"Pydantic validation failed for task {task_id}: {validation_error}")
            logging.error(f"Raw LLM output: {extracted_data_dict}")
            # Option 1: Fail the task due to validation error
            raise ValueError(f"Schema validation failed: {validation_error}")

        # 6. Update Redis with final status and result
        task_data["status"] = "COMPLETED"
        task_data["result"] = validated_result # Store the validated (or raw) dict
        task_data["error"] = None
        # Remove file content before final update
        task_data.pop("file_content_b64", None)
        redis_client.set(task_key, json.dumps(task_data))
        logging.info(f"Task {task_id} completed successfully.")

    except Exception as e:
        logging.error(f"Error processing task {task_id}: {e}", exc_info=True)
        # Update status to FAILED in Redis
        try:
            # Ensure task_data exists even if error happened early
            if 'task_data' not in locals(): task_data = {}
            task_data["status"] = "FAILED"
            task_data["error"] = f"{type(e).__name__}: {str(e)}" # Store error type and message
            task_data["result"] = None
            # Remove potentially large file content before storing error state
            task_data.pop("file_content_b64", None)
            redis_client.set(task_key, json.dumps(task_data))
            logging.info(f"Task {task_id} marked as FAILED.")
        except Exception as redis_err:
             logging.error(f"CRITICAL: Failed to update Redis status to FAILED for task {task_id}: {redis_err}")


# --- Main Worker Loop (Using simple polling for now) ---
def main_loop():
    """Continuously checks Redis for pending tasks and processes them."""
    if not redis_client:
        logging.error("Worker cannot start: No connection to Redis.")
        return

    logging.info("Worker started. Waiting for tasks...")
    while True:
        task_to_process = None
        task_id = None
        try:
            # Option A: Simple Polling (less efficient)
            task_keys = redis_client.keys("task:*")
            for task_key in task_keys:
                task_data_json = redis_client.get(task_key)
                if not task_data_json: continue
                try:
                    task_data = json.loads(task_data_json)
                    if task_data.get("status") == "PENDING":
                        task_id = task_key.split(":", 1)[1]
                        task_to_process = task_data
                        break # Process one task per loop iteration
                except (json.JSONDecodeError, IndexError):
                    logging.error(f"Invalid data or key format for {task_key}")
                    # Consider moving/deleting the bad key
                except Exception as e:
                    logging.error(f"Error checking task {task_key}: {e}")

            # Process the found task (if any)
            if task_to_process and task_id:
                process_task(task_id, task_to_process)
            else:
                # No pending task found, wait before checking again
                time.sleep(WORKER_POLL_INTERVAL)

        except redis.exceptions.ConnectionError:
             logging.error("Redis connection lost. Attempting to reconnect...")
             time.sleep(5)
             try:
                 redis_client.ping()
                 logging.info("Reconnected to Redis.")
             except redis.exceptions.ConnectionError:
                 logging.error("Reconnect failed.")
        except Exception as e:
            logging.error(f"An unexpected error occurred in the main worker loop: {e}", exc_info=True)
            time.sleep(5) # Wait before retrying loop

if __name__ == "__main__":
    main_loop()