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

class OrderBatch(BaseModel):
    orders: list[Order]

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


@app.post("/orders/batch")
async def create_orders_batch(batch: OrderBatch):
    # Send all orders with deferral hint, then a flush control to build one combined route
    for o in batch.orders:
        payload = {
            "order_id": str(uuid.uuid4()),
            "pickup": [o.pickup_lat, o.pickup_lon],
            "drop": [o.drop_lat, o.drop_lon],
            "earliest": o.earliest.isoformat(),
            "latest": o.latest.isoformat(),
            "volume": o.volume,
            "_defer": True,
        }
        get_producer().send("orders", payload)
    # control message to trigger immediate flush
    get_producer().send("orders", {"_control": "flush"})
    return {"status": "queued", "count": len(batch.orders)}


@app.delete("/orders/{order_id}")
def cancel_order(order_id: str):
    # Mark order as canceled so optimizer can skip it
    redis_client.set(f"orders:cancelled:{order_id}", "1", ex=24*3600)
    return {"status": "cancelled", "order_id": order_id}

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
            "stops": points,
            "naive_geometry": geometry
        },
        "orders": [],
        "coords": points,
    }
    redis_client.set("routes:demo", json.dumps(payload))
    redis_client.set("routes:latest", "demo")
    return {"status": "ok", "batch_id": "demo"}


@app.post("/routes/demoA")
def create_demo_a():
    # Pre-baked demo A (central/east pickups; south/west drops)
    pairs = [
        ((12.9733, 77.6110), (12.9250, 77.5938)),
        ((12.9786, 77.6400), (12.9420, 77.5710)),
        ((12.9350, 77.6240), (13.0040, 77.5690)),
    ]
    naive_seq = []
    pickups = []
    drops = []
    for p, d in pairs:
        naive_seq.extend([p, d])
        pickups.append(p)
        drops.append(d)
    # Simple "optimized" heuristic for demo: visit all pickups first (nearest-neighbour), then drops
    def nn(order):
        if not order:
            return []
        seq = [order[0]]
        remaining = order[1:]
        while remaining:
            last = seq[-1]
            remaining.sort(key=lambda x: (x[0]-last[0])**2 + (x[1]-last[1])**2)
            seq.append(remaining.pop(0))
        return seq
    opt_pickups = nn(pickups)
    # map pickup order to corresponding drops
    pickup_to_drop = {p: d for p, d in pairs}
    opt_drops = [pickup_to_drop[p] for p in opt_pickups]
    optimized_seq = opt_pickups + opt_drops
    geometry = _encode_polyline(optimized_seq)
    naive_geometry = _encode_polyline(naive_seq)
    payload = {
        "batch_id": "demoA",
        "created_at": int(time.time()),
        "num_orders": 3,
        "route": {
            "distance_m": None,
            "duration_s": None,
            "geometry": geometry,
            "fallback": True,
            "stops": optimized_seq,
            "naive_geometry": naive_geometry,
        },
        "orders": [],
        "coords": optimized_seq,
    }
    redis_client.set("routes:demoA", json.dumps(payload))
    redis_client.set("routes:latest", "demoA")
    return {"status": "ok", "batch_id": "demoA"}


@app.post("/routes/demoB")
def create_demo_b():
    # Pre-baked demo B (north/east pickups; south/west drops)
    pairs = [
        ((12.9835, 77.6033), (12.9089, 77.5851)),
        ((12.9537, 77.6682), (12.9980, 77.5530)),
        ((12.9605, 77.6388), (12.9181, 77.5732)),
    ]
    naive_seq = []
    pickups = []
    drops = []
    for p, d in pairs:
        naive_seq.extend([p, d])
        pickups.append(p)
        drops.append(d)
    def nn(order):
        if not order:
            return []
        seq = [order[0]]
        remaining = order[1:]
        while remaining:
            last = seq[-1]
            remaining.sort(key=lambda x: (x[0]-last[0])**2 + (x[1]-last[1])**2)
            seq.append(remaining.pop(0))
        return seq
    opt_pickups = nn(pickups)
    pickup_to_drop = {p: d for p, d in pairs}
    opt_drops = [pickup_to_drop[p] for p in opt_pickups]
    optimized_seq = opt_pickups + opt_drops
    geometry = _encode_polyline(optimized_seq)
    naive_geometry = _encode_polyline(naive_seq)
    payload = {
        "batch_id": "demoB",
        "created_at": int(time.time()),
        "num_orders": 3,
        "route": {
            "distance_m": None,
            "duration_s": None,
            "geometry": geometry,
            "fallback": True,
            "stops": optimized_seq,
            "naive_geometry": naive_geometry,
        },
        "orders": [],
        "coords": optimized_seq,
    }
    redis_client.set("routes:demoB", json.dumps(payload))
    redis_client.set("routes:latest", "demoB")
    return {"status": "ok", "batch_id": "demoB"}


@app.post("/routes/clear")
def clear_latest():
    redis_client.delete("routes:latest")
    return {"status": "cleared"}


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
    .toolbar { position: absolute; top: 10px; right: 10px; z-index: 999; background: white; padding: 8px; border-radius: 6px; box-shadow: 0 2px 6px rgba(0,0,0,0.2); display: flex; gap: 6px; }
    .toolbar button { padding: 6px 10px; border: 1px solid #ddd; background: #f8f9fa; border-radius: 4px; cursor: pointer; }
    .legend { position: absolute; bottom: 10px; left: 10px; z-index: 999; background: white; padding: 8px; border-radius: 6px; box-shadow: 0 2px 6px rgba(0,0,0,0.2); font-size: 12px; }
    .legend .item { display: flex; align-items: center; gap: 6px; margin-bottom: 4px; }
    .chip { width: 18px; height: 4px; }
    .chip.red { background: #ff3333; }
    .chip.blue { background: #60a5fa; border-top: 2px dashed #60a5fa; height: 0; }
    .chip.green { background: #1a7f37; height: 8px; border-radius: 10px; }
    .chip.end { background: #b91c1c; height: 8px; border-radius: 10px; }
  </style>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>
  <script>
    async function fetchLatest() {
      const res = await fetch('/routes/latest');
      return await res.json();
    }

    function haversineKm(a, b) {
      const toRad = d => d * Math.PI / 180;
      const R = 6371; // km
      const dLat = toRad(b[0] - a[0]);
      const dLon = toRad(b[1] - a[1]);
      const lat1 = toRad(a[0]);
      const lat2 = toRad(b[0]);
      const s = Math.sin(dLat/2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon/2) ** 2;
      return 2 * R * Math.asin(Math.sqrt(s));
    }

    function computeDistanceKm(coords) {
      let km = 0;
      for (let i = 1; i < coords.length; i++) {
        km += haversineKm(coords[i - 1], coords[i]);
      }
      return km;
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
          if (window.naiveLayer) { map.removeLayer(window.naiveLayer); }
          const coords = L.Polyline.decode(data.route.geometry);
          window.routeLayer = L.polyline(coords, { color: '#ff3333', weight: 4 });
          window.routeLayer.addTo(map);
          // Add source/destination markers
          if (coords.length > 0) {
            if (window.startMarker) { map.removeLayer(window.startMarker); }
            if (window.endMarker) { map.removeLayer(window.endMarker); }
            window.startMarker = L.circleMarker(coords[0], { radius: 7, color: '#1a7f37', fillColor: '#1a7f37', fillOpacity: 0.9 }).addTo(map).bindTooltip('Source');
            window.endMarker = L.circleMarker(coords[coords.length - 1], { radius: 7, color: '#b91c1c', fillColor: '#b91c1c', fillOpacity: 0.9 }).addTo(map).bindTooltip('Destination');
          }
          // Render stops: prefer route.stops; fallback to orders' pickup/drop or demo coords
          if (window.stopMarkers) { window.stopMarkers.forEach(m => map.removeLayer(m)); }
          window.stopMarkers = [];
          let stops = [];
          if (data.route.stops && Array.isArray(data.route.stops) && data.route.stops.length > 0) {
            stops = data.route.stops;
          } else if (Array.isArray(data.orders) && data.orders.length > 0) {
            data.orders.forEach(o => {
              if (Array.isArray(o.pickup) && o.pickup.length === 2) stops.push(o.pickup);
              if (Array.isArray(o.drop) && o.drop.length === 2) stops.push(o.drop);
            });
          } else if (Array.isArray(data.coords) && data.coords.length > 0) {
            stops = data.coords;
          }
          stops.forEach((pt, idx) => {
            try {
              const m = L.marker(pt, { title: `Stop ${idx + 1}` }).addTo(map);
              window.stopMarkers.push(m);
            } catch (e) { /* ignore bad point */ }
          });
          // Draw a naive path from encoded geometry if present
          try {
            if (data.route.naive_geometry) {
              const naive = L.Polyline.decode(data.route.naive_geometry);
              // Softer but more visible than grey, still secondary to main red route
              window.naiveLayer = L.polyline(naive, { color: '#60a5fa', weight: 3, opacity: 0.6, dashArray: '6,6' }).addTo(map);
            }
          } catch (e) {}

          map.fitBounds(window.routeLayer.getBounds(), { padding: [20, 20] });
          const hasDist = Number.isFinite(data.route.distance_m);
          const hasDur = Number.isFinite(data.route.duration_s);
          let distKm = hasDist ? (data.route.distance_m / 1000) : computeDistanceKm(coords);
          // fallback speed 25 km/h if duration missing
          let durMin = hasDur ? (data.route.duration_s / 60) : (distKm / 25) * 60;
          document.getElementById('info').innerText = `Orders: ${data.num_orders} | Dist: ${distKm.toFixed(2)} km | Dur: ${durMin.toFixed(1)} min`;
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
  <div class=\"toolbar\">
    <button onclick=\"fetch('/routes/demo',{method:'POST'}).then(()=>location.reload())\">Demo</button>
    <button onclick=\"fetch('/routes/demoA',{method:'POST'}).then(()=>location.reload())\">Demo A</button>
    <button onclick=\"fetch('/routes/demoB',{method:'POST'}).then(()=>location.reload())\">Demo B</button>
  </div>
  <div class=\"legend\">
    <div class=\"item\"><span class=\"chip red\"></span> Optimized route</div>
    <div class=\"item\"><span class=\"chip blue\"></span> Naive route</div>
    <div class=\"item\"><span class=\"chip green\"></span> Source</div>
    <div class=\"item\"><span class=\"chip end\"></span> Destination</div>
  </div>
  <div id=\"map\"></div>
</body>
</html>
    """
