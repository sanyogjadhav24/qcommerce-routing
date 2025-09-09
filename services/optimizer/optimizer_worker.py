from kafka import KafkaConsumer
import json
from solver import ORToolsVRPSolver
import redis
import os
import time
import requests
from haversine import haversine
import uuid

r = redis.Redis(host="redis", port=6379, db=0)

def get_kafka_broker() -> str:
    return os.getenv("KAFKA_BROKER", "kafka:9092")


def create_consumer() -> KafkaConsumer:
    broker = get_kafka_broker()
    last_err = None
    for _ in range(30):  # retry ~30s
        try:
            return KafkaConsumer(
                "orders",
                bootstrap_servers=[broker],
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                group_id="optimizer",
                enable_auto_commit=True,
                auto_offset_reset="latest",
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
                retry_backoff_ms=500,
            )
        except Exception as e:
            last_err = e
            time.sleep(1)
    raise RuntimeError(f"Failed to connect KafkaConsumer to {broker}: {last_err}")

consumer = create_consumer()

print("Optimizer worker started...", flush=True)

# Simple batching by count or time window
batch: list[dict] = []
last_flush_ts = time.time()


def flush_batch_if_needed(force: bool = False) -> None:
    global batch, last_flush_ts
    if not batch:
        return
    if not force and len(batch) < 5 and (time.time() - last_flush_ts) < 10:
        return

    batch_id = str(uuid.uuid4())
    try:
        print(f"Flushing batch {batch_id} with {len(batch)} orders...", flush=True)
        route_result = build_route(batch)
        payload = {
            "batch_id": batch_id,
            "created_at": int(time.time()),
            "num_orders": len(batch),
            "route": route_result,
            "orders": batch,
        }
        r.set(f"routes:{batch_id}", json.dumps(payload))
        r.set("routes:latest", batch_id)
        print(f"Batch {batch_id} stored with {len(batch)} orders", flush=True)
        batch = []
    except Exception as e:
        print(f"Failed to build/store route: {e}", flush=True)
    finally:
        last_flush_ts = time.time()


def get_osrm_base() -> str:
    return os.getenv("OSRM_BASE_URL", "http://osrm:5000")


def encode_polyline(coords_latlng: list[tuple[float, float]], precision: int = 5) -> str:
    # coords as (lat, lng)
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


def build_route(orders: list[dict]) -> dict:
    # Naive: build OSRM trip over sequence of pickup then drop points in order received
    # In production, use proper VRPTW with capacity and time windows.
    coords = []
    for o in orders:
        p_lat, p_lon = o["pickup"]
        d_lat, d_lon = o["drop"]
        coords.append(f"{p_lon},{p_lat}")
        coords.append(f"{d_lon},{d_lat}")
    coord_str = ";".join(coords)
    base = get_osrm_base().rstrip('/')
    url = f"{base}/trip/v1/driving/{coord_str}?overview=full&roundtrip=false&source=first&destination=last"
    last_err = None
    data = None
    for _ in range(45):  # retry ~45s
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            last_err = e
            time.sleep(1)

    if data and data.get("trips"):
        trip = data["trips"][0]
        return {
            "distance_m": trip.get("distance"),
            "duration_s": trip.get("duration"),
            "geometry": trip.get("geometry"),
        }

    # Fallback: straight-line polyline over the sequence
    latlng_seq: list[tuple[float, float]] = []
    total_km = 0.0
    last_point = None
    for o in orders:
        p_lat, p_lon = o["pickup"]
        d_lat, d_lon = o["drop"]
        for lat, lon in [(p_lat, p_lon), (d_lat, d_lon)]:
            latlng_seq.append((lat, lon))
            if last_point is not None:
                total_km += haversine(last_point, (lat, lon))
            last_point = (lat, lon)
    # Assume 25 km/h
    duration_s = (total_km / 25.0) * 3600.0
    geometry = encode_polyline(latlng_seq)
    return {
        "distance_m": total_km * 1000.0,
        "duration_s": duration_s,
        "geometry": geometry,
        "fallback": True,
        "error": str(last_err) if last_err else "no_osrm_data",
    }


for msg in consumer:
    order = msg.value
    print(f"New order received: {order['order_id']}", flush=True)
    batch.append(order)
    flush_batch_if_needed(force=True)
