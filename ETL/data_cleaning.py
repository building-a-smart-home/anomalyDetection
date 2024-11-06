import pandas as pd
import os

# Define the directory where raw data is stored
data_dir = '../data/raw/MHEALTHDATASET/'

# Load a sample file for exploration
file_path = os.path.join(data_dir, 'mHealth_subject10.log')
data = pd.read_csv(file_path, delim_whitespace=True, header=None)

# Assign column names based on the provided data structure
column_names = [
    'chest_acc_x', 'chest_acc_y', 'chest_acc_z', 'ecg_1', 'ecg_2',
    'ankle_acc_x', 'ankle_acc_y', 'ankle_acc_z', 'ankle_gyro_x', 'ankle_gyro_y', 'ankle_gyro_z',
    'ankle_mag_x', 'ankle_mag_y', 'ankle_mag_z', 'arm_acc_x', 'arm_acc_y', 'arm_acc_z',
    'arm_gyro_x', 'arm_gyro_y', 'arm_gyro_z', 'arm_mag_x', 'arm_mag_y', 'arm_mag_z', 'activity_label'
]
data.columns = column_names

# Display basic information
print(data.head())
print(data.info())
print(data['activity_label'].value_counts())  # Check activity distribution

# Drop rows with activity_label == 0 (null activity)
data = data[data['activity_label'] != 0]

# Check for missing values
if data.isnull().sum().any():
    data = data.dropna()

# Normalize selected columns (e.g., acceleration, gyroscope)
from sklearn.preprocessing import StandardScaler

sensor_columns = column_names[:-1]  # Exclude the label
scaler = StandardScaler()
data[sensor_columns] = scaler.fit_transform(data[sensor_columns])

print("Data after preprocessing:")
print(data.head())

# Calculate rolling statistics
window_size = 50  # 1 second window for 50Hz sampling rate
data['chest_acc_x_mean'] = data['chest_acc_x'].rolling(window=window_size).mean()
data['chest_acc_y_std'] = data['chest_acc_y'].rolling(window=window_size).std()
data['ankle_gyro_z_max'] = data['ankle_gyro_z'].rolling(window=window_size).max()
# Drop rows with NaN values introduced by rolling window
data.dropna(inplace=True)

processed_dir = '../data/processed/'
processed_file_path = os.path.join(processed_dir, 'mHealth_subject10_processed.csv')
data.to_csv(processed_file_path, index=False)