from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
import uuid
from kafka import KafkaProducer
import json
import os
import time
import redis
from fastapi.responses import HTMLResponse

app = FastAPI()

_producer = None


def get_kafka_broker() -> str:
    return os.getenv("KAFKA_BROKER", "kafka:9092")


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is not None:
        return _producer
    broker = get_kafka_broker()
    last_err = None
    for _ in range(30):  # ~30 seconds total
        try:
            _producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=5000,
                retries=5,
                retry_backoff_ms=500,
            )
            return _producer
        except Exception as e:
            last_err = e
            time.sleep(1)
    raise RuntimeError(f"Failed to initialize KafkaProducer for {broker}: {last_err}")


redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, db=0)


class Order(BaseModel):
    pickup_lat: float
    pickup_lon: float
    drop_lat: float
    drop_lon: float
    earliest: datetime
    latest: datetime
    volume: float

@app.post("/orders")
async def create_order(order: Order):
    order_id = str(uuid.uuid4())
    payload = {
        "order_id": order_id,
        "pickup": [order.pickup_lat, order.pickup_lon],
        "drop": [order.drop_lat, order.drop_lon],
        "earliest": order.earliest.isoformat(),
        "latest": order.latest.isoformat(),
        "volume": order.volume
    }
    get_producer().send("orders", payload)
    return {"status": "queued", "order_id": order_id}

@app.get("/")
def root():
    return {"message": "Q-Commerce Routing API running"}


@app.get("/routes/latest")
def get_latest_route():
    latest_id = redis_client.get("routes:latest")
    if not latest_id:
        return {"status": "empty"}
    data = redis_client.get(f"routes:{latest_id.decode('utf-8')}")
    if not data:
        return {"status": "not_found"}
    return json.loads(data)


def _encode_polyline(coords_latlng, precision=5):
    factor = 10 ** precision
    output = []
    prev_lat = 0
    prev_lng = 0
    for lat, lng in coords_latlng:
        lat_i = int(round(lat * factor))
        lng_i = int(round(lng * factor))
        d_lat = lat_i - prev_lat
        d_lng = lng_i - prev_lng
        prev_lat = lat_i
        prev_lng = lng_i
        for v in (d_lat, d_lng):
            v = ~(v << 1) if v < 0 else (v << 1)
            while v >= 0x20:
                output.append(chr((0x20 | (v & 0x1f)) + 63))
                v >>= 5
            output.append(chr(v + 63))
    return "".join(output)


@app.post("/routes/demo")
def create_demo_route():
    # Simple hardcoded demo path around Bangalore
    points = [
        (12.9716, 77.5946),
        (12.9650, 77.6000),
        (12.9500, 77.6100),
        (12.9400, 77.6000),
        (12.9350, 77.5900),
    ]
    geometry = _encode_polyline(points)
    payload = {
        "batch_id": "demo",
        "created_at": int(time.time()),
        "num_orders": 5,
        "route": {
            "distance_m": 8500.0,
            "duration_s": 1200.0,
            "geometry": geometry,
            "fallback": True,
        },
        "orders": [],
        "coords": points,
    }
    redis_client.set("routes:demo", json.dumps(payload))
    redis_client.set("routes:latest", "demo")
    return {"status": "ok", "batch_id": "demo"}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    # Minimal page to visualize latest route using Leaflet
    return """
<!DOCTYPE html>
<html>
<head>
  <meta charset=\"utf-8\"/>
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>
  <title>Q-Commerce Routing Dashboard</title>
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\"/>
  <style>
    html, body, #map { height: 100%; margin: 0; }
    .panel { position: absolute; top: 10px; left: 10px; z-index: 999; background: white; padding: 8px; border-radius: 6px; box-shadow: 0 2px 6px rgba(0,0,0,0.2); }
  </style>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>
  <script>
    async function fetchLatest() {
      const res = await fetch('/routes/latest');
      return await res.json();
    }

    async function init() {
      const map = L.map('map').setView([12.9716, 77.5946], 12);
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors'
      }).addTo(map);

      async function refresh() {
        const data = await fetchLatest();
        if (data && data.route && data.route.geometry) {
          if (window.routeLayer) { map.removeLayer(window.routeLayer); }
          const coords = L.Polyline.decode(data.route.geometry);
          window.routeLayer = L.polyline(coords, { color: '#ff3333', weight: 4 });
          window.routeLayer.addTo(map);
          // Add start/end markers for clarity
          if (coords.length > 0) {
            if (window.startMarker) { map.removeLayer(window.startMarker); }
            if (window.endMarker) { map.removeLayer(window.endMarker); }
            window.startMarker = L.marker(coords[0]).addTo(map);
            window.endMarker = L.marker(coords[coords.length - 1]).addTo(map);
          }
          map.fitBounds(window.routeLayer.getBounds(), { padding: [20, 20] });
          document.getElementById('info').innerText = `Orders: ${data.num_orders} | Dist: ${(data.route.distance_m/1000).toFixed(2)} km | Dur: ${(data.route.duration_s/60).toFixed(1)} min`;
        }
      }

      // Polyline codec
      // Minimal polyline decoder (Leaflet.encoded-like)
      L.Polyline.fromEncoded = function(encoded, options) {
        var polyline = L.polyline([], options);
        polyline.setLatLngs(L.Polyline.decode(encoded));
        return polyline;
      };
      L.Polyline.decode = function(str, precision) {
        var index = 0, lat = 0, lng = 0, coordinates = [], shift = 0, result = 0, byte = null, latitude_change, longitude_change,
          factor = Math.pow(10, precision || 5);
        while (index < str.length) {
          byte = null; shift = 0; result = 0;
          do { byte = str.charCodeAt(index++) - 63; result |= (byte & 0x1f) << shift; shift += 5; } while (byte >= 0x20);
          latitude_change = ((result & 1) ? ~(result >> 1) : (result >> 1));
          shift = 0; result = 0;
          do { byte = str.charCodeAt(index++) - 63; result |= (byte & 0x1f) << shift; shift += 5; } while (byte >= 0x20);
          longitude_change = ((result & 1) ? ~(result >> 1) : (result >> 1));
          lat += latitude_change; lng += longitude_change;
          coordinates.push([lat / factor, lng / factor]);
        }
        return coordinates;
      };

      await refresh();
      setInterval(refresh, 5000);
    }
    window.onload = init;
  </script>
</head>
<body>
  <div class=\"panel\"><span id=\"info\">Waiting for route...</span></div>
  <div id=\"map\"></div>
</body>
</html>
    """
