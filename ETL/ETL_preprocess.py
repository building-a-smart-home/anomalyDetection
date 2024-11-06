import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, max
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MHealth Data Preprocessing for Multiple Files") \
    .getOrCreate()

# Define the directory where raw data is stored
data_dir = '../data/raw/MHEALTHDATASET/'

# Define the schema for the CSV data
schema = StructType([
    StructField('chest_acc_x', FloatType(), True),
    StructField('chest_acc_y', FloatType(), True),
    StructField('chest_acc_z', FloatType(), True),
    StructField('ecg_1', FloatType(), True),
    StructField('ecg_2', FloatType(), True),
    StructField('ankle_acc_x', FloatType(), True),
    StructField('ankle_acc_y', FloatType(), True),
    StructField('ankle_acc_z', FloatType(), True),
    StructField('ankle_gyro_x', FloatType(), True),
    StructField('ankle_gyro_y', FloatType(), True),
    StructField('ankle_gyro_z', FloatType(), True),
    StructField('ankle_mag_x', FloatType(), True),
    StructField('ankle_mag_y', FloatType(), True),
    StructField('ankle_mag_z', FloatType(), True),
    StructField('arm_acc_x', FloatType(), True),
    StructField('arm_acc_y', FloatType(), True),
    StructField('arm_acc_z', FloatType(), True),
    StructField('arm_gyro_x', FloatType(), True),
    StructField('arm_gyro_y', FloatType(), True),
    StructField('arm_gyro_z', FloatType(), True),
    StructField('arm_mag_x', FloatType(), True),
    StructField('arm_mag_y', FloatType(), True),
    StructField('arm_mag_z', FloatType(), True),
    StructField('activity_label', IntegerType(), True)
])

# Define the directory where processed data will be saved
processed_dir = '../data/processed/'

# Ensure the processed directory exists
if not os.path.exists(processed_dir):
    os.makedirs(processed_dir)


# Function to process a single file
def process_file(file_path, output_path):
    # Load data from the CSV file
    raw_data = spark.read.csv(file_path, schema=schema, sep=' ', header=False)

    # Drop rows with activity_label == 0 (null activity)
    data_clean = raw_data.filter(raw_data['activity_label'] != 0)

    # Handle missing values by dropping rows with any nulls
    data_clean = data_clean.dropna()

    # Normalize selected columns (e.g., acceleration, gyroscope)
    sensor_columns = [
        'chest_acc_x', 'chest_acc_y', 'chest_acc_z', 'ecg_1', 'ecg_2',
        'ankle_acc_x', 'ankle_acc_y', 'ankle_acc_z', 'ankle_gyro_x', 'ankle_gyro_y', 'ankle_gyro_z',
        'ankle_mag_x', 'ankle_mag_y', 'ankle_mag_z', 'arm_acc_x', 'arm_acc_y', 'arm_acc_z',
        'arm_gyro_x', 'arm_gyro_y', 'arm_gyro_z', 'arm_mag_x', 'arm_mag_y', 'arm_mag_z'
    ]

    # Apply Standardization (Z-score) using Spark's built-in functions
    for col_name in sensor_columns:
        mean_val = data_clean.agg(mean(col(col_name))).collect()[0][0]
        stddev_val = data_clean.agg(stddev(col(col_name))).collect()[0][0]
        data_clean = data_clean.withColumn(
            col_name,
            (col(col_name) - mean_val) / stddev_val
        )

    # Calculate rolling stats (e.g., chest_acc_x_mean, chest_acc_y_std, ankle_gyro_z_max)
    windowSpec = Window.orderBy("activity_label").rowsBetween(-50, 50)  # 50 row window (approx. 1 sec at 50Hz)

    data_clean = data_clean.withColumn(
        'chest_acc_x_mean',
        mean('chest_acc_x').over(windowSpec)
    ).withColumn(
        'chest_acc_y_std',
        stddev('chest_acc_y').over(windowSpec)
    ).withColumn(
        'ankle_gyro_z_max',
        max('ankle_gyro_z').over(windowSpec)
    )

    # Drop rows with NaN values introduced by rolling window
    data_clean = data_clean.dropna()

    # Save the cleaned and processed data back to a CSV file
    data_clean.write.csv(output_path, header=True, mode='overwrite')


# List all log files in the data directory
log_files = [f for f in os.listdir(data_dir) if f.endswith('.log')]

# Process each file and save it to the processed directory
for log_file in log_files:
    file_path = os.path.join(data_dir, log_file)
    output_file_name = f"processed_{log_file.replace('.log', '.csv')}"
    output_path = os.path.join(processed_dir, output_file_name)

    # Process the file and save the result
    process_file(file_path, output_path)

# Stop the Spark session
spark.stop()
