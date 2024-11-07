import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, col, when, count
from google.oauth2 import service_account
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.clustering import KMeans  # Alternative to Isolation Forest in Spark

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("AnomalyDetection") \
    .config("spark.jars", "./spark-3.5-bigquery-0.41.0.jar") \
    .getOrCreate()

# Load data from BigQuery
def load_data_from_bigquery():
    return spark.read \
        .format("bigquery") \
        .option("table", "heya-440312.mHealth2024.activity_facts") \
        .load()

data = load_data_from_bigquery()

# Select features and drop timestamp for model input
features = data.select(
    "chest_acc_x", "chest_acc_y", "chest_acc_z", "ecg_1", "ecg_2",
    "ankle_acc_x", "ankle_acc_y", "ankle_acc_z", "arm_acc_x", "arm_acc_y", "arm_acc_z"
)

# Assemble features into a single vector column (required for Spark ML models)
assembler = VectorAssembler(inputCols=features.columns, outputCol="features")
data_with_features = assembler.transform(data)

# Standardize features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(data_with_features)
scaled_data = scaler_model.transform(data_with_features)

# Initialize KMeans as a simple anomaly detection replacement
kmeans = KMeans(k=2, featuresCol="scaled_features", predictionCol="prediction")
kmeans_model = kmeans.fit(scaled_data)
predictions = kmeans_model.transform(scaled_data)

# Map the predictions to indicate anomalies (e.g., label one cluster as anomalous based on counts)
# Assuming smaller cluster is the anomalous one
cluster_sizes = predictions.groupBy("prediction").count().orderBy("count")
anomalous_cluster = cluster_sizes.first()["prediction"]
data = predictions.withColumn("is_anomaly", when(col("prediction") == anomalous_cluster, 1).otherwise(0))

# Optionally, add rolling mean and std deviation calculations for each sensor reading
window_size = 50  # Adjust based on your sampling rate
for col_name in features.columns:
    data = data.withColumn(f"{col_name}_mean", mean(col(col_name)).over(Window.rowsBetween(-window_size, 0)))
    data = data.withColumn(f"{col_name}_std", stddev(col(col_name)).over(Window.rowsBetween(-window_size, 0)))

# Save results back to BigQuery
def save_to_bigquery(df, table_id):
    df.write \
        .format("bigquery") \
        .option("table", table_id) \
        .mode("overwrite") \
        .save()

# Specify table to save processed results with anomaly labels and scores
save_to_bigquery(data.select("timestamp", "is_anomaly"), "heya-440312.mHealth2024.activity_anomalies")

spark.stop()
