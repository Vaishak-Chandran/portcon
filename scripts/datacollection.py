# scripts/collect_aisstream.py

import os
import json
import asyncio
import pandas as pd
from datetime import datetime, timezone
import websockets
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
WS_URL  = "wss://stream.aisstream.io/v0/stream"

# Define bounding boxes for your ports
BOUNDING_BOXES = {
    "singapore": [[ [1.14, 103.60], [1.29, 104.03] ]],
    "rotterdam": [[ [51.85, 4.23], [51.97, 4.55] ]],
    "los_angeles": [[ [33.71, -118.30], [33.80, -118.15] ]]
}

async def stream_port(port_name: str, bbox: list):
    subscribe_msg = {
        "APIKey": API_KEY,
        "BoundingBoxes": bbox,
        "FilterMessageTypes": ["PositionReport"]
    }

    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps(subscribe_msg))
        print(f"[{port_name}] Subscription sent, waiting for data…")

        # Collect for e.g. 1000 messages or 5 minutes
        rows = []
        start = datetime.now(timezone.utc)
        async for message in ws:
            msg = json.loads(message)
            if msg.get("MessageType") != "PositionReport":
                continue

            data = msg["MetaData"]
            data.update(msg["Message"]["PositionReport"])
            data["port"] = port_name
            data["time_utc"] = datetime.now(timezone.utc).isoformat()

            rows.append(data)

            # simple stop condition
            if len(rows) >= 500 or (datetime.now(timezone.utc) - start).seconds > 300:
                break

        # Save to CSV
        df = pd.DataFrame(rows)
        filename = f"data/ais_{port_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.csv"
        df.to_csv(filename, index=False)
        print(f"[{port_name}] Saved {len(rows)} rows to {filename}")

async def main():
    # Run all three ports in parallel
    await asyncio.gather(*[
        stream_port(name, bbox)
        for name, bbox in BOUNDING_BOXES.items()
    ])

if __name__ == "__main__":
    asyncio.run(main())
