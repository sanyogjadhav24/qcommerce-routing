from kafka import KafkaConsumer
import json
from solver import ORToolsVRPSolver
import redis
import os
import time
import requests
from haversine import haversine
import uuid
from datetime import datetime
from services.ml.predictor import TravelTimePredictor

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
    # Solve VRPTW with capacity using OR-Tools and ML travel time predictor
    model_path = os.getenv("TRAVEL_TIME_MODEL_PATH", "/app/services/ml/travel_time_lgbm.pkl")
    predictor = TravelTimePredictor(model_path)
    solver = ORToolsVRPSolver(vehicle_count=1, capacity=10)

    def predict_seconds(origin, dest, _now):
        # origin/dest are (lat, lon)
        return predictor.predict_seconds(origin, dest, datetime.utcnow())

    result = solver.solve(orders, predict_seconds)
    if result.get("status") != "ok" or not result.get("nodes"):
        # fallback to naive OSRM trip logic
        coords = []
        seq_latlon: list[tuple[float, float]] = []
        for o in orders:
            p_lat, p_lon = o["pickup"]
            d_lat, d_lon = o["drop"]
            coords.append(f"{p_lon},{p_lat}")
            coords.append(f"{d_lon},{d_lat}")
            seq_latlon.extend([(p_lat, p_lon), (d_lat, d_lon)])
        coord_str = ";".join(coords)
        base = get_osrm_base().rstrip('/')
        url = f"{base}/trip/v1/driving/{coord_str}?overview=full&roundtrip=false&source=first&destination=last"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data and data.get("trips"):
                trip = data["trips"][0]
                return {
                    "distance_m": trip.get("distance"),
                    "duration_s": trip.get("duration"),
                    "geometry": trip.get("geometry"),
                    "stops": seq_latlon,
                }
        except Exception:
            pass
        # Final fallback straight line
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
        duration_s = (total_km / 25.0) * 3600.0
        geometry = encode_polyline(latlng_seq)
        return {
            "distance_m": total_km * 1000.0,
            "duration_s": duration_s,
            "geometry": geometry,
            "fallback": True,
            "stops": latlng_seq,
        }

    # Build ordered coordinate list from nodes result and query OSRM for polyline
    # Nodes are indices into [depot, p1, d1, p2, d2, ...]
    seq_latlon: list[tuple[float, float]] = []
    depot = tuple(orders[0]["pickup"])  # (lat, lon)
    nodes = result["nodes"]
    mapping: list[tuple[float, float]] = [depot]
    for o in orders:
        mapping.append(tuple(o["pickup"]))
        mapping.append(tuple(o["drop"]))
    for node in nodes:
        seq_latlon.append(mapping[node])

    # Try OSRM route for geometry over this sequence (as a polyline)
    base = get_osrm_base().rstrip('/')
    coords_param = ";".join([f"{lon},{lat}" for lat, lon in seq_latlon])
    url = f"{base}/route/v1/driving/{coords_param}?overview=full&geometries=polyline"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data and data.get("routes"):
            route = data["routes"][0]
            return {
                "distance_m": route.get("distance"),
                "duration_s": route.get("duration"),
                "geometry": route.get("geometry"),
                "stops": seq_latlon,
            }
    except Exception:
        pass

    # Fallback: encode straight line polyline of sequence
    geometry = encode_polyline(seq_latlon)
    # Approximate duration using ML between successive nodes
    total_seconds = 0.0
    for i in range(len(seq_latlon) - 1):
        total_seconds += predict_seconds(seq_latlon[i], seq_latlon[i + 1], datetime.utcnow())
    return {
        "distance_m": None,
        "duration_s": total_seconds,
        "geometry": geometry,
        "fallback": True,
        "stops": seq_latlon,
    }


for msg in consumer:
    order = msg.value
    print(f"New order received: {order['order_id']}", flush=True)
    # Skip canceled orders
    if r.get(f"orders:cancelled:{order['order_id']}"):
        print(f"Order {order['order_id']} is cancelled. Skipping.", flush=True)
        continue
    batch.append(order)
    flush_batch_if_needed(force=True)
