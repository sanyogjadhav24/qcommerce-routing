import requests
from datetime import datetime, timedelta

for i in range(5):
    payload = {
        "pickup_lat": 19.076,
        "pickup_lon": 72.8777,
        "drop_lat": 19.1,
        "drop_lon": 72.9,
        "earliest": datetime.utcnow().isoformat(),
        "latest": (datetime.utcnow() + timedelta(minutes=30)).isoformat(),
        "volume": 1.0
    }
    r = requests.post("http://localhost:8000/orders", json=payload)
    print(r.json())
