# services/ml/generate_trips.py
import pandas as pd
import random
from datetime import datetime, timedelta
from haversine import haversine

def generate_synthetic_data(num_samples=1000, city_center=(19.076, 72.8777), radius_km=10):
    """
    Generate synthetic trip data:
    origin_lat, origin_lon, dest_lat, dest_lon, depart_ts, duration_seconds
    """
    rows = []
    for _ in range(num_samples):
        # random origin within radius
        origin_lat = city_center[0] + random.uniform(-0.05, 0.05)
        origin_lon = city_center[1] + random.uniform(-0.05, 0.05)

        # random destination within radius
        dest_lat = city_center[0] + random.uniform(-0.05, 0.05)
        dest_lon = city_center[1] + random.uniform(-0.05, 0.05)

        # random departure time in past week
        days_ago = random.randint(0, 6)
        hour = random.randint(6, 22)
        minute = random.randint(0, 59)
        depart_ts = datetime.utcnow() - timedelta(days=days_ago)
        depart_ts = depart_ts.replace(hour=hour, minute=minute, second=0, microsecond=0)

        # compute base duration (distance / speed) + noise
        dist_km = haversine((origin_lat, origin_lon), (dest_lat, dest_lon))
        avg_speed_kmph = random.choice([20, 25, 30])  # simulate traffic conditions
        duration = (dist_km / avg_speed_kmph) * 3600  # in seconds
        duration += random.uniform(-60, 120)  # add noise (-1 min to +2 min)

        rows.append([origin_lat, origin_lon, dest_lat, dest_lon, depart_ts, int(duration)])

    df = pd.DataFrame(rows, columns=[
        "origin_lat", "origin_lon", "dest_lat", "dest_lon", "depart_ts", "duration_seconds"
    ])
    return df

if __name__ == "__main__":
    df = generate_synthetic_data(num_samples=5000)
    df.to_csv("synthetic_trips.csv", index=False)
    print("Generated dataset: synthetic_trips.csv")
