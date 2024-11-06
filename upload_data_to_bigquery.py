import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
from feature_engineering.bq_client import bq_client
import numpy as np

# Path where your processed data is stored
processed_dir = './data/processed/'

# BigQuery dataset and table details
dataset_id = 'mHealth2024'
table_id = 'activity_facts'

activity_label_to_id = {
    'Standing still': 1,
    'Sitting and relaxing': 2,
    'Lying down': 3,
    'Walking': 4,
    'Climbing stairs': 5,
    'Waist bends forward': 6,
    'Frontal elevation of arms': 7,
    'Knees bending': 8,
    'Cycling': 9,
    'Jogging': 10,
    'Running': 11,
    'Jump front & back': 12
}

activity_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


# Function to generate mock timestamps for each record
def generate_mock_timestamps(df, start_time="2023-01-01", frequency="min"):
    # Convert start_time to a datetime object
    base_time = datetime.strptime(start_time, "%Y-%m-%d")

    # Create a list of timestamps
    timestamps = [base_time + timedelta(minutes=i) for i in range(len(df))]

    # Add the list of timestamps as a new column in the dataframe
    df['timestamp'] = timestamps

    return df


# Function to process CSV file, add subject_id, mock timestamp, and upload to BigQuery
def process_and_upload_csv(subject_file_path):
    # Extract subject_id from filename, e.g., 'mHealth_subject1_processed.csv' -> subject_id=1
    subject_id = int(subject_file_path.split('subject')[1].split('_')[0])

    # Read CSV into a pandas dataframe
    df = pd.read_csv(subject_file_path)

    # Add the subject_id as a new column
    df['subject_id'] = subject_id

    # Map activity_label to activity_id
    df['activity_id'] = np.random.choice(activity_ids, size=len(df))

    # Generate mock timestamps
    df = generate_mock_timestamps(df)

    # Upload the data to BigQuery
    # List of columns that match your BigQuery schema
    required_columns = [
        'timestamp', 'activity_id', 'chest_acc_x', 'chest_acc_y', 'chest_acc_z',
        'ecg_1', 'ecg_2', 'ankle_acc_x', 'ankle_acc_y', 'ankle_acc_z',
        'arm_acc_x', 'arm_acc_y', 'arm_acc_z', 'subject_id'
    ]

    # Filter the DataFrame to keep only the required columns
    df = df[required_columns]

    #  upload the DataFrame to BigQuery
    upload_to_bigquery(df)


# Function to upload dataframe to BigQuery
def upload_to_bigquery(df):
    # Set the BigQuery table reference
    table_ref = bq_client.dataset(dataset_id).table(table_id)

    # Load the dataframe into BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append data to the table
        skip_leading_rows=1,  # Skip header row if present
        autodetect=True,  # Automatically detect schema from the dataframe
    )

    # Start the load job from the dataframe
    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    # Wait for the job to finish
    job.result()
    print(f'Loaded {job.output_rows} rows into {dataset_id}:{table_id}.')


# Loop through the processed directory and process all subject CSV files
for filename in os.listdir(processed_dir):
    if filename.endswith("_processed.csv"):
        subject_file_path = os.path.join(processed_dir, filename)
        process_and_upload_csv(subject_file_path)
