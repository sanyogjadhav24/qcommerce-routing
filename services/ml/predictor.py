
# services/ml/predictor.py
import joblib
import numpy as np
from haversine import haversine

class TravelTimePredictor:
    def __init__(self, model_path: str, fallback_speed_kmph: float = 25.0):
        """
        model_path: path to trained LightGBM model
        fallback_speed_kmph: default speed for fallback calculation
        """
        self.model = joblib.load(model_path)
        self.fallback_speed_kmph = fallback_speed_kmph

    def featurize(self, origin, dest, depart_ts, historical_mean=None):
        """
        origin, dest: (lat, lon)
        depart_ts: datetime object
        historical_mean: optional historical mean duration in seconds
        """
        dist_km = haversine(origin, dest)
        hour = depart_ts.hour + depart_ts.minute / 60.0
        dow = depart_ts.weekday()
        features = {
            "dist_km": dist_km,
            "hour": hour,
            "dow": dow,
            "historical_mean": historical_mean if historical_mean is not None else -1.0
        }
        return np.array([
            features["dist_km"],
            features["hour"],
            features["dow"],
            features["historical_mean"]
        ]).reshape(1, -1)

    def predict_seconds(self, origin, dest, depart_ts, historical_mean=None):
        """
        Predict travel time in seconds for given OD pair and departure time.
        """
        X = self.featurize(origin, dest, depart_ts, historical_mean)
        try:
            pred_seconds = float(self.model.predict(X)[0])
            return max(10.0, pred_seconds)  # at least 10 sec
        except Exception:
            # fallback calculation using naive speed assumption
            dist_km = haversine(origin, dest)
            return (dist_km / self.fallback_speed_kmph) * 3600.0
