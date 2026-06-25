# =============================================================================
# IMPORTS
# =============================================================================
import azure.functions as func            # Azure Functions SDK
import azure.durable_functions as df      # Durable Functions extension
from azure.data.tables import TableServiceClient  # Azure Table Storage SDK
from pypdf import PdfReader               # pypdf library for reading PDF files
import logging                            # Python built-in logging
import json                               # Python built-in JSON handling
import io                                 # Python built-in for byte stream handling
import os                                 # Python built-in for environment variables
import uuid                               # Python built-in for generating unique IDs
import re                                 # Python built-in regular expressions
from datetime import datetime             # Python built-in for timestamps


# =============================================================================
# CREATE THE DURABLE FUNCTION APP
# =============================================================================
# Same as Lab 2: we use df.DFApp because this project uses Durable Functions.
# The app includes a Blob trigger, an orchestrator, activity functions,
# and an HTTP endpoint for retrieving stored results.
myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# =============================================================================
# TABLE STORAGE HELPER
# =============================================================================
# This helper function creates a connection to Azure Table Storage.
# The table is used to store the final PDF analysis reports.
#
# For local development, PDFStorageConnection points to Azurite:
#   UseDevelopmentStorage=true
#
# For Azure deployment, PDFStorageConnection should contain the real
# Azure Storage Account connection string.
TABLE_NAME = "PDFAnalysisResults"


def get_table_client():
    """Get a TableClient for storing and retrieving PDF analysis results."""
    connection_string = os.environ["PDFStorageConnection"]
    table_service = TableServiceClient.from_connection_string(connection_string)

    # create_table_if_not_exists ensures the table exists before we use it.
    # This avoids errors when the project is run for the first time.
    table_service.create_table_if_not_exists(TABLE_NAME)

    return table_service.get_table_client(TABLE_NAME)


# =============================================================================
# PDF HELPER FUNCTION
# =============================================================================
# This helper converts the PDF bytes back into a readable PDF object.
#
# Why do we need this?
# The Blob trigger reads the uploaded PDF as raw bytes. Since Durable Functions
# inputs must be JSON serializable, the blob bytes are converted into a list
# of integers before being passed into the orchestrator.
#
# In each activity function, we convert that list back into bytes and use
# pypdf's PdfReader to read the PDF content.
def read_pdf(inputData: dict):
    """Convert serialized PDF bytes back into a PdfReader object."""
    pdf_bytes = bytes(inputData["blob_bytes"])
    return PdfReader(io.BytesIO(pdf_bytes))


# =============================================================================
# 1. CLIENT FUNCTION (Blob Trigger - The Entry Point)
# =============================================================================
# This function triggers automatically when a PDF file is uploaded to the
# "pdfs" Blob Storage container.
#
# How it works:
#   1. User uploads a PDF to the "pdfs" container.
#   2. Azure Blob Storage detects the new file.
#   3. This function reads the PDF file and starts the Durable orchestrator.
#
# The path "pdfs/{name}" means:
#   - Watch the "pdfs" container.
#   - {name} captures the uploaded file name.
@myApp.blob_trigger(
    arg_name="myblob",
    path="pdfs/{name}",
    connection="PDFStorageConnection"
)
@myApp.durable_client_input(client_name="client")
async def blob_trigger(myblob: func.InputStream, client):
    # Get the full blob name, such as "pdfs/sample.pdf".
    blob_name = myblob.name

    # Read the uploaded PDF file as raw bytes.
    blob_bytes = myblob.read()

    # Calculate the file size in KB for the final report.
    blob_size_kb = round(len(blob_bytes) / 1024, 2)

    logging.info(f"New PDF detected: {blob_name} ({blob_size_kb} KB)")

    # Prepare input data for the orchestrator.
    #
    # Important:
    # Durable Functions inputs must be JSON serializable.
    # Raw bytes cannot be stored directly in JSON, so we convert the bytes
    # into a list of integers.
    input_data = {
        "blob_name": blob_name,
        "blob_bytes": list(blob_bytes),
        "blob_size_kb": blob_size_kb
    }

    # Start the PDF analyzer orchestrator.
    # The orchestrator will run the four analysis activities in parallel.
    instance_id = await client.start_new(
        "pdf_analyzer_orchestrator",
        client_input=input_data
    )

    logging.info(f"Started orchestration {instance_id} for {blob_name}")


# =============================================================================
# 2. ORCHESTRATOR FUNCTION (The Workflow Manager)
# =============================================================================
# This orchestrator implements the required hybrid pattern:
#
#   1. Fan-Out/Fan-In:
#      Run four independent PDF analysis activities in parallel.
#
#   2. Chaining:
#      After all four activities finish, generate a combined report and store it.
#
# This is very similar to Lab 2, except the image analysis activities are
# replaced with PDF analysis activities.
@myApp.orchestration_trigger(context_name="context")
def pdf_analyzer_orchestrator(context):
    # Get the input data passed from the Blob trigger.
    input_data = context.get_input()

    logging.info(f"PDF orchestrator started for: {input_data['blob_name']}")

    # =========================================================================
    # STEP 1: FAN-OUT - Run all 4 PDF analyses in parallel
    # =========================================================================
    # Each call_activity creates a task.
    # We do NOT yield each activity one by one.
    # Instead, we collect all tasks into a list and wait for all of them together.
    analysis_tasks = [
        context.call_activity("extract_text", input_data),
        context.call_activity("extract_metadata", input_data),
        context.call_activity("analyze_statistics", input_data),
        context.call_activity("detect_sensitive_data", input_data),
    ]

    # =========================================================================
    # STEP 2: FAN-IN - Wait for all parallel activities to complete
    # =========================================================================
    # context.task_all waits until all four activities are finished.
    # The results are returned in the same order as the task list above.
    results = yield context.task_all(analysis_tasks)

    # results[0] = extract_text result
    # results[1] = extract_metadata result
    # results[2] = analyze_statistics result
    # results[3] = detect_sensitive_data result

    # =========================================================================
    # STEP 3: CHAIN - Generate a unified report
    # =========================================================================
    # After the parallel work is complete, the workflow continues sequentially.
    report_input = {
        "blob_name": input_data["blob_name"],
        "blob_size_kb": input_data["blob_size_kb"],
        "text": results[0],
        "metadata": results[1],
        "statistics": results[2],
        "sensitiveData": results[3],
    }

    report = yield context.call_activity("generate_report", report_input)

    # =========================================================================
    # STEP 4: CHAIN - Store the report in Table Storage
    # =========================================================================
    record = yield context.call_activity("store_results", report)

    return record


# =============================================================================
# 3. ACTIVITY: Extract Text
# =============================================================================
# This activity extracts text content from every page of the uploaded PDF.
#
# It returns:
#   - text for each page
#   - combined full text
#   - character count
#   - whether the PDF contains readable text
#
# Note:
# pypdf works best with text-based PDFs. If the PDF is only scanned images,
# this function may return empty text because OCR is not included in this project.
@myApp.activity_trigger(input_name="inputData")
def extract_text(inputData: dict):
    logging.info("Extracting text from PDF...")

    try:
        reader = read_pdf(inputData)
        pages_text = []

        # Loop through each page and extract text.
        for index, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""

            pages_text.append({
                "pageNumber": index,
                "text": text.strip()
            })

        # Combine all page text into one large string.
        full_text = "\n".join(page["text"] for page in pages_text)

        return {
            "pageTexts": pages_text,
            "fullText": full_text,
            "characterCount": len(full_text),
            "hasText": len(full_text.strip()) > 0
        }

    except Exception as e:
        logging.error(f"Text extraction failed: {str(e)}")

        return {
            "pageTexts": [],
            "fullText": "",
            "characterCount": 0,
            "hasText": False,
            "error": str(e)
        }


# =============================================================================
# 4. ACTIVITY: Extract Metadata
# =============================================================================
# This activity extracts PDF metadata from the uploaded document.
#
# Metadata may include:
#   - title
#   - author
#   - subject
#   - creator
#   - producer
#   - creation date
#   - modification date
#
# Not every PDF contains metadata, so empty values are expected for some files.
@myApp.activity_trigger(input_name="inputData")
def extract_metadata(inputData: dict):
    logging.info("Extracting PDF metadata...")

    try:
        reader = read_pdf(inputData)
        metadata = reader.metadata or {}

        return {
            "title": str(metadata.get("/Title", "")),
            "author": str(metadata.get("/Author", "")),
            "subject": str(metadata.get("/Subject", "")),
            "creator": str(metadata.get("/Creator", "")),
            "producer": str(metadata.get("/Producer", "")),
            "creationDate": str(metadata.get("/CreationDate", "")),
            "modificationDate": str(metadata.get("/ModDate", "")),
            "pageCount": len(reader.pages),
            "isEncrypted": reader.is_encrypted,
            "sizeKB": inputData["blob_size_kb"]
        }

    except Exception as e:
        logging.error(f"Metadata extraction failed: {str(e)}")

        return {
            "title": "",
            "author": "",
            "subject": "",
            "creator": "",
            "producer": "",
            "creationDate": "",
            "modificationDate": "",
            "pageCount": 0,
            "isEncrypted": False,
            "sizeKB": inputData.get("blob_size_kb", 0),
            "error": str(e)
        }


# =============================================================================
# 5. ACTIVITY: Analyze Statistics
# =============================================================================
# This activity calculates basic document statistics.
#
# Required statistics:
#   - page count
#   - word count
#   - average words per page
#   - estimated reading time
#
# Reading time is estimated using 200 words per minute, which is a common
# simple estimate for average reading speed.
@myApp.activity_trigger(input_name="inputData")
def analyze_statistics(inputData: dict):
    logging.info("Analyzing PDF statistics...")

    try:
        reader = read_pdf(inputData)

        page_count = len(reader.pages)
        full_text = ""

        # Extract text from all pages so we can calculate word count.
        for page in reader.pages:
            full_text += (page.extract_text() or "") + "\n"

        # Use a regular expression to identify words.
        words = re.findall(r"\b\w+\b", full_text)
        word_count = len(words)

        avg_words_per_page = round(word_count / page_count, 2) if page_count > 0 else 0
        estimated_reading_time = round(word_count / 200, 2)

        return {
            "pageCount": page_count,
            "wordCount": word_count,
            "characterCount": len(full_text),
            "averageWordsPerPage": avg_words_per_page,
            "estimatedReadingTimeMinutes": estimated_reading_time
        }

    except Exception as e:
        logging.error(f"Statistics analysis failed: {str(e)}")

        return {
            "pageCount": 0,
            "wordCount": 0,
            "characterCount": 0,
            "averageWordsPerPage": 0,
            "estimatedReadingTimeMinutes": 0,
            "error": str(e)
        }


# =============================================================================
# 6. ACTIVITY: Detect Sensitive Data
# =============================================================================
# This activity scans the extracted PDF text for common sensitive data patterns.
#
# Required patterns:
#   - email addresses
#   - phone numbers
#   - URLs
#   - dates
#
# This activity uses regular expressions. It does not prove that every match
# is truly sensitive, but it identifies common patterns that may require review.
@myApp.activity_trigger(input_name="inputData")
def detect_sensitive_data(inputData: dict):
    logging.info("Detecting sensitive data...")

    try:
        reader = read_pdf(inputData)
        full_text = ""

        # Extract text from all pages before scanning for patterns.
        for page in reader.pages:
            full_text += (page.extract_text() or "") + "\n"

        # Regular expression patterns for sensitive data detection.
        email_pattern = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
        phone_pattern = r"(\+?\d{1,3}[\s.-]?)?(\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}"
        url_pattern = r"https?://[^\s]+|www\.[^\s]+"
        date_pattern = r"\b(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})\b"

        # Find all matches in the PDF text.
        emails = re.findall(email_pattern, full_text)
        raw_urls = re.findall(url_pattern, full_text)
        dates = re.findall(date_pattern, full_text)
        urls = [url.rstrip(".,;:)") for url in raw_urls]

        # For phone numbers, use finditer so we can return the full matched string.
        phone_matches = re.finditer(phone_pattern, full_text)
        phones = [match.group().strip() for match in phone_matches]

        # Remove duplicate values by converting lists to sets, then back to lists.
        unique_emails = list(set(emails))
        unique_phones = list(set(phones))
        unique_urls = list(set(urls))
        unique_dates = list(set(dates))

        return {
            "emails": unique_emails,
            "phoneNumbers": unique_phones,
            "urls": unique_urls,
            "dates": unique_dates,
            "counts": {
                "emails": len(unique_emails),
                "phoneNumbers": len(unique_phones),
                "urls": len(unique_urls),
                "dates": len(unique_dates)
            },
            "hasSensitiveData": any([
                len(unique_emails) > 0,
                len(unique_phones) > 0,
                len(unique_urls) > 0,
                len(unique_dates) > 0
            ])
        }

    except Exception as e:
        logging.error(f"Sensitive data detection failed: {str(e)}")

        return {
            "emails": [],
            "phoneNumbers": [],
            "urls": [],
            "dates": [],
            "counts": {
                "emails": 0,
                "phoneNumbers": 0,
                "urls": 0,
                "dates": 0
            },
            "hasSensitiveData": False,
            "error": str(e)
        }


# =============================================================================
# 7. ACTIVITY: Generate Report
# =============================================================================
# This activity combines the four parallel analysis results into one report.
#
# This is the "reduce" step after Fan-In:
#   - extract_text result
#   - extract_metadata result
#   - analyze_statistics result
#   - detect_sensitive_data result
#
# The final report includes:
#   - unique report ID
#   - file information
#   - analysis results
#   - summary values for quick viewing
@myApp.activity_trigger(input_name="reportData")
def generate_report(reportData: dict):
    logging.info("Generating PDF analysis report...")

    blob_name = reportData["blob_name"]

    # Extract only the file name from the full blob path.
    # Example: "pdfs/sample.pdf" becomes "sample.pdf".
    filename = blob_name.split("/")[-1] if "/" in blob_name else blob_name

    report = {
        "id": str(uuid.uuid4()),
        "fileName": filename,
        "blobPath": blob_name,
        "analyzedAt": datetime.utcnow().isoformat(),
        "analyses": {
            "text": reportData["text"],
            "metadata": reportData["metadata"],
            "statistics": reportData["statistics"],
            "sensitiveData": reportData["sensitiveData"]
        },
        "summary": {
            "pageCount": reportData["statistics"].get("pageCount", 0),
            "wordCount": reportData["statistics"].get("wordCount", 0),
            "averageWordsPerPage": reportData["statistics"].get("averageWordsPerPage", 0),
            "estimatedReadingTimeMinutes": reportData["statistics"].get("estimatedReadingTimeMinutes", 0),
            "hasText": reportData["text"].get("hasText", False),
            "hasSensitiveData": reportData["sensitiveData"].get("hasSensitiveData", False),
            "emailCount": reportData["sensitiveData"].get("counts", {}).get("emails", 0),
            "phoneCount": reportData["sensitiveData"].get("counts", {}).get("phoneNumbers", 0),
            "urlCount": reportData["sensitiveData"].get("counts", {}).get("urls", 0),
            "dateCount": reportData["sensitiveData"].get("counts", {}).get("dates", 0),
            "title": reportData["metadata"].get("title", ""),
            "author": reportData["metadata"].get("author", ""),
            "sizeKB": reportData.get("blob_size_kb", 0)
        }
    }

    logging.info(f"PDF report generated: {report['id']}")

    return report


# =============================================================================
# 8. ACTIVITY: Store Results in Table Storage
# =============================================================================
# This activity saves the generated report to Azure Table Storage.
#
# Table Storage entities are flat key-value pairs.
# Because our report contains nested dictionaries, we convert the complex
# analysis sections into JSON strings before storing them.
#
# Table Storage requires:
#   - PartitionKey: groups related records
#   - RowKey: unique ID for each record
@myApp.activity_trigger(input_name="report")
def store_results(report: dict):
    logging.info(f"Storing PDF analysis results for {report['fileName']}...")

    try:
        table_client = get_table_client()

        entity = {
            "PartitionKey": "PDFAnalysis",
            "RowKey": report["id"],
            "FileName": report["fileName"],
            "BlobPath": report["blobPath"],
            "AnalyzedAt": report["analyzedAt"],

            # Store nested data as JSON strings.
            "Summary": json.dumps(report["summary"]),
            "TextAnalysis": json.dumps(report["analyses"]["text"]),
            "MetadataAnalysis": json.dumps(report["analyses"]["metadata"]),
            "StatisticsAnalysis": json.dumps(report["analyses"]["statistics"]),
            "SensitiveDataAnalysis": json.dumps(report["analyses"]["sensitiveData"])
        }

        # upsert_entity inserts the entity if it does not exist,
        # or updates it if it already exists.
        table_client.upsert_entity(entity)

        logging.info(f"PDF results stored with ID: {report['id']}")

        return {
            "id": report["id"],
            "fileName": report["fileName"],
            "status": "stored",
            "analyzedAt": report["analyzedAt"],
            "summary": report["summary"]
        }

    except Exception as e:
        logging.error(f"Failed to store PDF results: {str(e)}")

        return {
            "id": report.get("id", "unknown"),
            "status": "error",
            "error": str(e)
        }


# =============================================================================
# 9. HTTP FUNCTION: Get Analysis Results
# =============================================================================
# This HTTP function retrieves stored PDF analysis results from Table Storage.
#
# This function is separate from the Durable Functions workflow.
# It does not start an orchestration. It only reads previously stored reports.
#
# Usage:
#   GET /api/results
#       Returns the latest 10 PDF analysis reports.
#
#   GET /api/results?limit=5
#       Returns the latest 5 PDF analysis reports.
#
#   GET /api/results/{id}
#       Returns one specific PDF analysis report by ID.
@myApp.route(route="results/{id?}", methods=["GET"])
def get_results(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Get PDF results endpoint called")

    try:
        table_client = get_table_client()
        result_id = req.route_params.get("id")

        if result_id:
            # =========================================================================
            # CASE 1: Retrieve a specific result by ID
            # =========================================================================
            try:
                entity = table_client.get_entity(
                    partition_key="PDFAnalysis",
                    row_key=result_id
                )

                # Convert JSON strings back into dictionaries.
                result = {
                    "id": entity["RowKey"],
                    "fileName": entity["FileName"],
                    "blobPath": entity["BlobPath"],
                    "analyzedAt": entity["AnalyzedAt"],
                    "summary": json.loads(entity["Summary"]),
                    "analyses": {
                        "text": json.loads(entity["TextAnalysis"]),
                        "metadata": json.loads(entity["MetadataAnalysis"]),
                        "statistics": json.loads(entity["StatisticsAnalysis"]),
                        "sensitiveData": json.loads(entity["SensitiveDataAnalysis"])
                    }
                }

                return func.HttpResponse(
                    json.dumps(result, indent=2),
                    mimetype="application/json",
                    status_code=200
                )

            except Exception:
                return func.HttpResponse(
                    json.dumps({"error": f"Result not found: {result_id}"}),
                    mimetype="application/json",
                    status_code=404
                )

        else:
            # =========================================================================
            # CASE 2: Retrieve multiple recent results
            # =========================================================================
            limit = int(req.params.get("limit", "10"))

            entities = table_client.query_entities(
                query_filter="PartitionKey eq 'PDFAnalysis'"
            )

            results = []

            for entity in entities:
                results.append({
                    "id": entity["RowKey"],
                    "fileName": entity["FileName"],
                    "analyzedAt": entity["AnalyzedAt"],
                    "summary": json.loads(entity["Summary"])
                })

            # Sort by analyzedAt descending so the newest results appear first.
            results.sort(key=lambda x: x["analyzedAt"], reverse=True)

            # Apply the limit parameter.
            results = results[:limit]

            return func.HttpResponse(
                json.dumps({"count": len(results), "results": results}, indent=2),
                mimetype="application/json",
                status_code=200
            )

    except Exception as e:
        logging.error(f"Failed to retrieve PDF results: {str(e)}")

        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )