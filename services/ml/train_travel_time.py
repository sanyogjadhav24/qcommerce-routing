# services/ml/train_travel_time.py
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import lightgbm as lgb
import joblib
from haversine import haversine

def engineer_features(df):
    """
    df must contain: origin_lat, origin_lon, dest_lat, dest_lon, depart_ts, duration_seconds
    """
    df['depart_ts'] = pd.to_datetime(df['depart_ts'])
    df['hour'] = df['depart_ts'].dt.hour + df['depart_ts'].dt.minute / 60.0
    df['dow'] = df['depart_ts'].dt.weekday
    df['dist_km'] = df.apply(lambda r: haversine(
        (r.origin_lat, r.origin_lon),
        (r.dest_lat, r.dest_lon)), axis=1)
    # placeholder for historical mean; in real pipeline, compute from past data
    df['hist_mean'] = df['duration_seconds']
    return df

def train(csv_path, out_model_path='travel_time_lgbm.pkl'):
    df = pd.read_csv(csv_path)
    df = engineer_features(df)

    features = ['dist_km', 'hour', 'dow', 'hist_mean']
    X = df[features]
    y = df['duration_seconds']

    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

    train_data = lgb.Dataset(X_train, label=y_train)
    val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)

    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'learning_rate': 0.05,
        'num_leaves': 31,
        'seed': 42
    }

    gbm = lgb.train(
    params,
    train_data,
    num_boost_round=2000,
    valid_sets=[val_data],
    callbacks=[lgb.early_stopping(stopping_rounds=50)]
)


    joblib.dump(gbm, out_model_path)
    print(f"Model saved to {out_model_path}")

if __name__ == '__main__':
    import sys
    csv_path = sys.argv[1]
    out_model_path = sys.argv[2] if len(sys.argv) > 2 else 'travel_time_lgbm.pkl'
    train(csv_path, out_model_path)
