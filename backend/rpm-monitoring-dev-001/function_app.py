import azure.functions as func
import logging
import json
import os
from datetime import datetime

# Azure SDKs
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from azure.ai.projects import AIProjectClient
from azure.core.credentials import AzureKeyCredential

# --------------------------------------------------
# 🔹 FUNCTION APP
# --------------------------------------------------
app = func.FunctionApp()

# --------------------------------------------------
# 🔹 CONFIGURATION 
# --------------------------------------------------
PROJECT_ENDPOINT = os.environ.get(
    "PROJECT_ENDPOINT",
    "https://healthcare-remotemonitoring-demo.services.ai.azure.com/api/projects/proj-default"
)

FOUNDRY_API_KEY = os.environ.get("FOUNDRY_API_KEY")

if not FOUNDRY_API_KEY:
    raise RuntimeError("FOUNDRY_API_KEY is missing")

# --------------------------------------------------
# 🔹 COSMOS DB CLIENT
# --------------------------------------------------
cosmos_client = CosmosClient(
    os.environ["COSMOS_ENDPOINT"],
    credential=os.environ["COSMOS_KEY"]
)

database = cosmos_client.get_database_client(os.environ["COSMOS_DB"])
container = database.get_container_client(os.environ["COSMOS_CONTAINER"])

# --------------------------------------------------
# 🔹 FOUNDRY CLIENT (INITIALIZED ONCE)
# --------------------------------------------------
logging.warning("🔥 RPM FUNCTION APP LOADED")

project_client = AIProjectClient(
    endpoint=PROJECT_ENDPOINT,
    credential=AzureKeyCredential(FOUNDRY_API_KEY)
)

openai_client = project_client.get_openai_client(
    api_key=FOUNDRY_API_KEY
)

# --------------------------------------------------
# 🔹 AGENT CALLER
# --------------------------------------------------
def call_agent(agent_name: str, input_data: dict) -> dict:
    logging.info(f"🔹 Calling {agent_name}")

    conversation = openai_client.conversations.create()

    try:
        response = openai_client.responses.create(
            conversation=conversation.id,
            input=json.dumps(input_data, indent=2),
            extra_body={
                "agent_reference": {
                    "name": agent_name,
                    "type": "agent_reference"
                }
            }
        )

        raw = response.output_text.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)

    finally:
        openai_client.conversations.delete(conversation.id)

# --------------------------------------------------
# 🔹 EVENT HUB TRIGGER
# --------------------------------------------------
@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="patient-data",
    connection="IOTHUB_EVENT_CONN",
    consumer_group="$Default"
)
def eventhub_trigger(event: func.EventHubEvent):

    try:
        # ==================================================
        # ✅ TASK 1: INGEST DATA
        # ==================================================
        logging.info("🔹 TASK 1: Ingesting patient vitals")

        data = json.loads(event.get_body().decode("utf-8"))
        patient_id = data.get("patientId")

        if not patient_id:
            logging.error("❌ Missing patientId")
            return

        vitals = {
            "heartRate": data.get("heartRate"),
            "spo2": data.get("spo2"),
            "temperature": data.get("temperature")
        }

        timestamp = datetime.utcnow().isoformat()

        logging.info(
            f"PATIENT_RECEIVED | Patient={patient_id} | "
            f"HR={vitals['heartRate']} | "
            f"SpO2={vitals['spo2']} | "
            f"Temp={vitals['temperature']}"
        )

        # ==================================================
        # ✅ TASK 2: LOAD / INIT PATIENT
        # ==================================================
        logging.info("🔹 TASK 2: Loading patient record")

        try:
            patient_doc = container.read_item(
                item=patient_id,
                partition_key=patient_id
            )
            logging.info("✅ Existing patient found")

        except CosmosResourceNotFoundError:
            logging.info("✅ New patient document created")
            patient_doc = {
                "id": patient_id,
                "patientId": patient_id,
                "records": [],
                "meta": {}
            }

        history = [r["vitals"] for r in patient_doc["records"][-5:]]

        # ==================================================
        # ✅ TASK 3: RPM AI WORKFLOW
        # ==================================================
        logging.info("🔹 TASK 3: Running RPM AI workflow")

        trends = call_agent("Trends-Agent", {
            "patientId": patient_id,
            "latestReading": vitals,
            "recentReadings": history
        })

        correlation = call_agent("Correlation-Agent", {
            "latestReading": vitals,
            "trends": trends
        })

        risk = call_agent("Risk-Agent", {
            "latestReading": vitals,
            "trends": trends,
            "correlation": correlation
        })

        triage = call_agent("Triage-Agent", {
            "latestReading": vitals,
            "risk": risk
        })

        analysis = {
            "trends": trends,
            "correlation": correlation,
            "risk": risk,
            "triage": triage
        }

        logging.info("✅ RPM AI workflow finished")

        # ==================================================
        # ✅ TASK 4: STORE DATA (ALWAYS RUNS)
        # ==================================================
        logging.info("🔹 TASK 4: Persisting patient record")

        record = {
            "timestamp": timestamp,
            "vitals": vitals,
            "agents": analysis
        }

        patient_doc["records"].append(record)

        # ✅ Prevent unlimited growth (recommended)
        MAX_RECORDS = 20
        if len(patient_doc["records"]) > MAX_RECORDS:
            patient_doc["records"] = patient_doc["records"][-MAX_RECORDS:]

        patient_doc["meta"] = {
            "latestSeverity": triage.get("severity"),
            "lastUpdatedAt": timestamp
        }

        container.upsert_item(patient_doc)

        logging.info(
            f"✅ Patient {patient_id} saved | Severity={triage.get('severity')}"
        )

        # ==================================================
        # ✅ TASK 5: ALERT (DEMO)
        # ==================================================
        if triage.get("severity") in ["High", "Critical"]:
            logging.warning(
                f"🚨 ALERT | Patient={patient_id} | "
                f"Severity={triage.get('severity')} | "
                f"Action={triage.get('recommendedAction')}"
            )

        logging.info(f"✅ EVENT COMPLETED FOR PATIENT {patient_id}")

    except Exception as e:
        logging.error("❌ FUNCTION CRASHED")
        logging.exception(e)
        raise
