from bq_client import bq_client

def avg_heartRate():
    # SQL query to extract average heart rate and other sensor features
    query = f"""
    SELECT
        activity_id,
        AVG(ecg_1) AS avg_ecg_1,
        AVG(ecg_2) AS avg_ecg_2,
        STDDEV(chest_acc_x) AS chest_acc_x_deviation,
        STDDEV(chest_acc_y) AS chest_acc_y_deviation,
        STDDEV(chest_acc_z) AS chest_acc_z_deviation,
        TIMESTAMP_DIFF(MAX(timestamp), MIN(timestamp), SECOND) AS activity_duration_seconds
    FROM
        `{project_id}.{fact_id}`
    WHERE
        activity_id != 0
    GROUP BY
        activity_id
    """

    # Run the query and store the results in a pandas DataFrame
    query_job = bq_client.query(query)
    results = query_job.result()

    # Convert the results to a DataFrame
    df = results.to_dataframe()

    # You can add more custom features here if needed, for example:
    df['activity_intensity'] = df['chest_acc_x_deviation'] + df['chest_acc_y_deviation']

    # Return the final DataFrame with engineered features
    return df

# 2. Activity Pattern Analysis
def activity_pattern_analysis():
    query = """
    SELECT s.subject_id, a.activity_name, COUNT(f.activity_id) AS activity_count
    FROM `heya-440312.mHealth2024.activity_facts` f
    JOIN `heya-440312.mHealth2024.subject_dim` s ON f.subject_id = s.subject_id
    JOIN `heya-440312.mHealth2024.activity_dim` a ON f.activity_id = a.activity_type_id
    GROUP BY s.subject_id, a.activity_name;
    """
    results = bq_client.query(query).to_dataframe()
    print("Activity Pattern Analysis:\n", results)
    return results

# 3. Sensor Performance Analysis
def sensor_performance_analysis(activity_name='Running'):
    query = f"""
    SELECT f.timestamp, f.chest_acc_x, f.chest_acc_y, f.chest_acc_z
    FROM `heya-440312.mHealth2024.activity_facts` f
    JOIN `heya-440312.mHealth2024.activity_dim` a ON f.activity_id = a.activity_type_id
    WHERE a.activity_name = '{activity_name}';
    """
    results = bq_client.query(query).to_dataframe()
    print(f"Sensor Performance Analysis for {activity_name}:\n", results)
    return results

# Define BigQuery table IDs
project_id = 'heya-440312.mHealth2024'
fact_id = 'activity_facts'
subject_table_id = 'subject_dim'
activity_table_id = 'activity_dim'
sensor_table_id = 'sensor_dim'
anomaly_table_id = 'anomaly_dim'