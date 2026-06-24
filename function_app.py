import azure.functions as func            
import azure.durable_functions as df      
import pymupdf as pdf
from azure.data.tables import TableServiceClient, TableClient  
import logging                            
import json                               
import io                                 
import os                                 
import uuid                               
from datetime import datetime             

# *********************** Create Base App ************************** #
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


TABLE_NAME = "pdfAnalysis"

def get_table_client():
    connection_string = os.environ["PDF_UPLOAD_CONNECTION"]
    table_service = TableServiceClient.from_connection_string(connection_string)

    # create_table_if_not_exists ensures the table exists before we use it
    table_service.create_table_if_not_exists(TABLE_NAME)
    return table_service.get_table_client(TABLE_NAME)



@app.blob_trigger(arg_name="myfile", path="pdfs/{filename}", connection="PDF_UPLOAD_CONNECTION")
@app.durable_client_input(client_name="client")
async def pdf_upload_trigger(myfile: func.InputStream, client):

    blob_name = myfile.name
    blob_bytes = myfile.read()
    blob_size_kb = round(len(blob_bytes) / 1024, 2)

    logging.info(f"New image detected: {blob_name} ({blob_size_kb} KB)")

    input_data = {
        "blob_name": blob_name,
        "blob_bytes": list(blob_bytes),
        "blob_size_kb": blob_size_kb
    }

    # Orchestrator Start
    instance_id = await client.start_new(
        "pdf_analyzer_orchestrator",
        client_input=input_data
    )

    logging.info(f"Passing to orchestrator {instance_id} for {blob_name}")


@app.orchestration_trigger(context_name="context")
def pdf_analyzer_orchestrator(context):
    # Get the input data passed from the blob trigger
    input_data = context.get_input()

    logging.info(f"Orchestrator started for: {input_data['blob_name']}")

    # *************** RUN ACTIVITY FUNCTIONS IN PARALLEL ***************** #
    analysis_tasks = [
        # context.call_activity("extract_text", input_data), uncomment when function implemented
        context.call_activity("extract_metadata", input_data),
        # context.call_activity("analyze_statistics", input_data), uncomment when function implemented
        # context.call_activity("detect_sensitive_data", input_data), uncomment when function implemented
    ]

    # *************** WAIT FOR ALL TASKS TO COMPLETE THEN START NEXT ACTIVITY ***************** #
    results = yield context.task_all(analysis_tasks)

    report_input = {
        "stats": "", # place analyze_statistics here
        "metadata": results[1], # 2nd task in above analysis_tasks
        "text": "", # place extract_text here
        "sensitive_data": "", # place sensitive_data here
    }

    # # ************** GENERATE REPORT ************** #
    report = yield context.call_activity("generate_report", report_input)

    # # ************** ARCHIVE RESULTS ************** #
    record = yield context.call_activity("store_results", report)

    return record


@app.activity_trigger(input_name="inputData")
def extract_metadata(inputData: dict):
    logging.info("Analyzing metadata...")

    try:
        # Open pdf
        opened_file = pdf.open(stream=bytes(inputData["blob_bytes"]), filetype="pdf")
        metadata = opened_file.metadata
        
        # normalize pdf date
        orig_date = metadata["creationDate"]

        # strip "D:" from pdf date format
        modified_date = orig_date[2:]
        year = modified_date[:4]
        month = modified_date[4:6]
        day = modified_date[6:8]


        return {
            "format": metadata["format"],
            "title": metadata["title"],
            "author": metadata["author"],
            "created_on": f"{year}-{month}-{day}"
        }

    except Exception as e:
        logging.error(f"Metadata analysis failed: {str(e)}")
        return {
            "format": "",
            "title": "",
            "author": "",
            "created_on": ""
        }

@app.activity_trigger(input_name="inputData")
def extract_text(inputData: dict):
    print()
    # TODO: write text extraction


@app.activity_trigger(input_name="inputData")
def analyze_statistics(inputData: dict):
    print()
    # TODO: write text extraction

@app.activity_trigger(input_name="inputData")
def detect_sensitive_data(inputData: dict):
    print()
    # TODO: write text extraction


@app.activity_trigger(input_name="reportData")
def generate_report(reportData: dict):
    logging.info("Starting Report Generation, Please Wait...")

    blob_name = reportData["blob_name"]
    filename = blob_name.split("/")[-1] if "/" in blob_name else blob_name

    report = {
        "id": str(uuid.uuid4()),
        "fileName": filename,
        "blobPath": blob_name,
        "analyzed_on": datetime.now(datetime.timezone.utc).isoformat(),
        "analysis": {
            "statistics": reportData["stats"],
            "metadata": reportData["metadata"],
            "text": reportData["text"],
            "sensitive_data": reportData["sensitive_data"],
        },
        "summary": {
            "format": reportData["metadata"].get("format", "Unknown"),
            "author": reportData["metadata"].get("author", "Unknown"),
            "hasText": reportData["text"].get("hasText", False),
        }
    }

    logging.info(f"Analysis Report Id: {report['id']}")
    return report


@app.activity_trigger(input_name="report")
def store_results(report: dict):
    logging.info(f"Archiving {report['fileName']}...")

    try:
        table_client = get_table_client()

        entity = {
            "PartitionKey": "PdfAnalysis",
            "RowKey": report["id"],
            "FileName": report["fileName"],
            "BlobPath": report["blobPath"],
            "AnalyzedAt": report["analyzed_on"],
            # Store complex data as JSON strings
            "Summary": json.dumps(report["summary"]),
            "Statistics": json.dumps(report["analyses"]["statistics"]),
            "Metadata": json.dumps(report["analyses"]["meta_data"]),
            "Text": json.dumps(report["analyses"]["text"]),
            "SensitiveData": json.dumps(report["analyses"]["sensitive_data"]),
        }

        table_client.upsert_entity(entity)

        logging.info(f"Results Archived-  Report Id:{report['id']}")

        return {
            "id": report["id"],
            "fileName": report["fileName"],
            "status": "stored",
            "analyzed_on": report["analyzed_on"],
            "summary": report["summary"]
        }

    except Exception as e:
        logging.error(f"Failed to store results: {str(e)}")
        return {
            "id": report.get("id", "unknown"),
            "status": "error",
            "error": str(e)
        }


# Usage:
#   GET /api/results          - Get all results (last 10)
#   GET /api/results?limit=5  - Get last 5 results
#   GET /api/results/{id}     - Get a specific result by ID
@app.route(route="results/{id?}")
def get_results(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Get results endpoint called")

    try:
        table_client = get_table_client()
        result_id = req.route_params.get("id")

        if result_id:
            # Get a specific result by ID
            try:
                entity = table_client.get_entity(
                    partition_key="PdfAnalysis",
                    row_key=result_id
                )

                # Parse JSON strings back into objects
                result = {
                    "id": entity["RowKey"],
                    "fileName": entity["FileName"],
                    "blobPath": entity["BlobPath"],
                    "analyzedAt": entity["AnalyzedAt"],
                    "summary": json.loads(entity["Summary"]),
                    "analyses": {
                        "statistics": json.loads(entity["Statistics"]),
                        "meta_data": json.loads(entity["Metadata"]),
                        "text": json.loads(entity["Text"]),
                        "sensitive_data": json.loads(entity["SensitiveData"]),
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
            # Get all results (with optional limit)
            limit = int(req.params.get("limit", "10"))

            entities = table_client.query_entities(
                query_filter="PartitionKey eq 'PdfAnalysis'"
            )

            results = []
            for entity in entities:
                results.append({
                    "id": entity["RowKey"],
                    "fileName": entity["FileName"],
                    "analyzedAt": entity["AnalyzedAt"],
                    "summary": json.loads(entity["Summary"]),
                })

            # Sort by analyzedAt descending (most recent first)
            results.sort(key=lambda x: x["analyzedAt"], reverse=True)
            results = results[:limit]

            return func.HttpResponse(
                json.dumps({"count": len(results), "results": results}, indent=2),
                mimetype="application/json",
                status_code=200
            )

    except Exception as e:
        logging.error(f"Failed to retrieve results: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )