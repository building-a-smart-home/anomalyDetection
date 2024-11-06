import pandas as pd
from bq_client import bq_client
from google.cloud.bigquery import LoadJobConfig
from sklearn.ensemble import IsolationForest

# Fetch data for training
def load_data_from_bigquery():
    query = """
    SELECT timestamp, chest_acc_x, chest_acc_y, chest_acc_z, ecg_1, ecg_2,
           ankle_acc_x, ankle_acc_y, ankle_acc_z, arm_acc_x, arm_acc_y, arm_acc_z
    FROM `heya-440312.mHealth2024.activity_facts`
    """
    data = bq_client.query(query).to_dataframe()
    return data


# Data loading and preprocessing
data = load_data_from_bigquery()
features = data.drop(columns=['timestamp'])  # Exclude timestamp for model training


# Initialize and train the Isolation Forest model
def train_anomaly_detection_model(features):
    isolation_forest = IsolationForest(n_estimators=100, contamination=0.1, random_state=42)
    isolation_forest.fit(features)
    return isolation_forest


isolation_forest = train_anomaly_detection_model(features)

# Predict anomalies (-1 indicates anomaly, 1 indicates normal)
data['anomaly_score'] = isolation_forest.decision_function(features)
predictions = isolation_forest.predict(features)

# Convert predictions to a pandas Series and map -1 to 1 (anomaly) and 1 to 0 (normal)
data['is_anomaly'] = pd.Series(predictions).map(lambda x: 1 if x == -1 else 0)

# Optionally, view the anomaly scores and labels
print(data[['timestamp', 'anomaly_score', 'is_anomaly']].head())

# Add rolling window calculations for each sensor reading column
def add_rolling_features(data, features, window_size=50):
    for col in features.columns:
        data[f'{col}_mean'] = features[col].rolling(window=window_size).mean()
        data[f'{col}_std'] = features[col].rolling(window=window_size).std()
    data.dropna(inplace=True)  # Drop rows with NaN values from rolling calculations


add_rolling_features(data, features)


# Save results to BigQuery
def save_to_bigquery(data):
    data['timestamp'] = pd.to_datetime(data['timestamp'])  # Ensure timestamp is in datetime format
    table_id = 'heya-440312.mHealth2024.activity_anomalies'

    # Define job config with overwrite setting
    job_config = LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    # Load data to BigQuery and wait for completion
    job = bq_client.load_table_from_dataframe(data[['timestamp', 'is_anomaly', 'anomaly_score']], table_id,
                                              job_config=job_config)
    job.result()  # Waits for the job to complete
    print("Data uploaded to BigQuery successfully.")


save_to_bigquery(data)

