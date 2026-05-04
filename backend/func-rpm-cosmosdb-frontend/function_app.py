import azure.functions as func
import json
import logging
import os
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from dotenv import load_dotenv 
load_dotenv()
app = func.FunctionApp()

# --------------------------------------------------
# 🔹 COSMOS DB CLIENT (READ ONLY)
# --------------------------------------------------
cosmos_client = CosmosClient(
    os.environ["COSMOS_ENDPOINT"],
    credential=os.environ["COSMOS_KEY"]
)

database = cosmos_client.get_database_client(os.environ["COSMOS_DB"])
container = database.get_container_client(os.environ["COSMOS_CONTAINER"])

logging.warning("✅ FRONTEND READ API LOADED")

# --------------------------------------------------
# 🔹 GET ONE PATIENT (DETAIL VIEW)
# --------------------------------------------------
@app.route(route="patients/{patient_id}", methods=["GET"])
def get_patient(req: func.HttpRequest) -> func.HttpResponse:
    patient_id = req.route_params.get("patient_id")

    try:
        patient_doc = container.read_item(
            item=patient_id,
            partition_key=patient_id
        )

        return func.HttpResponse(
            json.dumps(patient_doc),
            status_code=200,
            mimetype="application/json"
        )

    except CosmosResourceNotFoundError:
        return func.HttpResponse(
            json.dumps({"error": "Patient not found"}),
            status_code=404,
            mimetype="application/json"
        )

# --------------------------------------------------
# 🔹 TRIAGE QUEUE (HACKATHON SHOWCASE)
# --------------------------------------------------
SEVERITY_RANK = {
    "Critical": 1,
    "High": 2,
    "Low": 3,
    "Informational": 4
}

@app.route(route="triage", methods=["GET"])
def triage_queue(req: func.HttpRequest) -> func.HttpResponse:
    try:
        patients = list(container.read_all_items())
        queue = []

        for p in patients:
            if not p.get("records"):
                continue

            latest = p["records"][-1]
            triage = latest["agents"]["triage"]

            queue.append({
                "patientId": p["patientId"],
                "severity": triage["severity"],
                "recommendedAction": triage.get("recommendedAction"),
                "timestamp": latest["timestamp"],
                "vitals": latest["vitals"]
            })

        queue.sort(
            key=lambda x: SEVERITY_RANK.get(x["severity"], 99)
        )

        return func.HttpResponse(
            json.dumps(queue),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception(e)
        return func.HttpResponse(
            json.dumps({"error": "Failed to load triage queue"}),
            status_code=500,
            mimetype="application/json"
        )