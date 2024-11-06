import pandas as pd
import random
from bq_client import bq_client

# --------------------------- subject_dim ---------------------------
subject_data = [
    {'subject_id': i, 'age': random.randint(20, 70), 'gender': random.choice(['M', 'F']),
     'height': random.randint(150, 190), 'weight': random.randint(50, 100), 'medical_history': random.choice(['Healthy', 'Asthma', 'Hypertension', 'Diabetes'])}
    for i in range(1, 11)  # Generate data for 10 subjects
]

subject_df = pd.DataFrame(subject_data)

# --------------------------- activity_dim ---------------------------
activity_data = [
    {'activity_type_id': i, 'activity_label': i, 'activity_name': name, 'duration_minutes': duration}
    for i, (name, duration) in enumerate([
        ('Standing still', 1), ('Sitting and relaxing', 1), ('Lying down', 1),
        ('Walking', 1), ('Climbing stairs', 1), ('Waist bends forward', 20),
        ('Frontal elevation of arms', 20), ('Knees bending', 20), ('Cycling', 1),
        ('Jogging', 1), ('Running', 1), ('Jump front & back', 20)], 1)
]

activity_df = pd.DataFrame(activity_data)
print(activity_df.columns)
print(activity_df.values)

# --------------------------- sensor_dim ---------------------------
sensor_data = [
    {'sensor_id': i, 'sensor_location': loc, 'sensor_type': sensor, 'units': unit}
    for i, (loc, sensor, unit) in enumerate([
        ('chest', 'accelerometer', 'm/s²'), ('chest', 'ECG', 'mV'),
        ('left ankle', 'accelerometer', 'm/s²'), ('left ankle', 'gyroscope', 'deg/s'),
        ('left ankle', 'magnetometer', 'local'), ('right wrist', 'accelerometer', 'm/s²')], 1)
]

sensor_df = pd.DataFrame(sensor_data)

# --------------------------- anomaly_dim ---------------------------
anomaly_data = [
    {'anomaly_id': i, 'anomaly_name': name, 'anomaly_description': desc, 'severity': severity}
    for i, (name, desc, severity) in enumerate([
        ('Abnormal heart rate', 'Heart rate above or below normal thresholds', 'High'),
        ('Abnormal ECG', 'Irregular ECG patterns indicating arrhythmia', 'High'),
        ('Low activity', 'Insufficient movement detected over a period', 'Medium'),
        ('High heart rate', 'Heart rate exceeding safe exercise levels', 'Medium'),
        ('Low heart rate', 'Heart rate below safe threshold', 'High')], 1)
]

anomaly_df = pd.DataFrame(anomaly_data)

# --------------------------- Upload Data to BigQuery ---------------------------

# Define BigQuery table IDs
subject_table_id = 'heya-440312.mHealth2024.subject_dim'
activity_table_id = 'heya-440312.mHealth2024.activity_dim'
sensor_table_id = 'heya-440312.mHealth2024.sensor_dim'
anomaly_table_id = 'heya-440312.mHealth2024.anomaly_dim'

# Upload data to BigQuery
def upload_to_bigquery(df, table_id):
    job = bq_client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for the job to complete
    print(f"Data uploaded to {table_id}")

# Upload the DataFrames
upload_to_bigquery(subject_df, subject_table_id)
upload_to_bigquery(activity_df, activity_table_id)
upload_to_bigquery(sensor_df, sensor_table_id)
upload_to_bigquery(anomaly_df, anomaly_table_id)
