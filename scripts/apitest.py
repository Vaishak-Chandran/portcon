import requests
import csv
import json
import os 
from dotenv import load_dotenv

load_dotenv()

url = "https://sg-mdh-api.mpa.gov.sg/v1/vessel/departure/date/2025-07-01"
headers = {
    "apikey": os.environ.get("SING_KEY") 
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    items = data if isinstance(data, list) else data.get("data", data.get("results", []))
    csv_file = "vessel_departures.csv"
    csv_file = os.path.join("data", "vessel_departures.csv")
    csv_headers = ["vesselName", "callSign", "imoNumber", "flag", "departedTime"]
    with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=csv_headers)
        writer.writeheader()
        for item in items:
            vessel_particulars = item.get("vesselParticulars", {})
            writer.writerow({
                "vesselName": vessel_particulars.get("vesselName", ""),
                "callSign": vessel_particulars.get("callSign", ""),
                "imoNumber": vessel_particulars.get("imoNumber", ""),
                "flag": vessel_particulars.get("flag", ""),
                "departedTime": item.get("departedTime", "")
            })
    
    print(f"Data written to {csv_file}")
else:
    print(f"Failed to fetch data: {response.status_code} - {response.text}")