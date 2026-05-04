from azure.iot.device import IoTHubDeviceClient
import json, time, random
import os 
from dotenv import load_dotenv 
load_dotenv()

conn_str = os.getenv("IOT_HUB_CONN_STR")

client = IoTHubDeviceClient.create_from_connection_string(conn_str)

while True:
    data = {
        "patientId": f"P{random.randint(1,10)}",
        "heartRate": random.randint(65,130),
        "spo2": random.randint(85,100),
        "temperature": round(random.uniform(35,39),2)
    }

    print("Sending:", data)
    client.send_message(json.dumps(data))

    time.sleep(30)