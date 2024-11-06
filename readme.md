
## HeyA! - Intelligent ambient assisted living (AAL) environment with ML ##

**Objective:** 

Design a data pipeline and machine learning model that collects, processes, and analyzes sensor data to detect anomalies in the daily routines of elderly individuals.

*Key Outcomes:*
- Real-time anomaly detection.
- Alert system for caregivers.
- Dashboard for monitoring activity patterns and deviations.

**MHEALTH (Mobile Health) Dataset:**

Contains sensor data for body movements and physiological signals such as ECG, accelerometer, and gyroscope signals collected from wearable sensors on different body parts.
Available on the UCI Machine Learning Repository.

**Data Storage**

Google Big Query
![bigData.png](bigData.png)
**Star Schema Design for mHealth Data**
1. Fact Table: activity_fact
The fact table holds the core, granular data with sensor readings and activity labels. Each row represents a sensor reading at a given timestamp for a specific activity performed by a subject.
2. Dimension Tables
The dimension tables store descriptive information about activities, subjects, sensor types, and anomalies. These tables help us analyze data by grouping or filtering based on these attributes.

![star_schema.png](artifacts/star_schema.png)

**Data Transformation**
- Use Apache Spark or AWS Glue to process and clean data.
- Create ETL jobs to handle transformations like timestamp formatting, null value handling, and data aggregation.

**Machine Learning Layer**
- Feature Engineering:
1. Extract average heart rate and other sensor features
![feature1.png](feature1.png)
2. Activity Pattern Analysis
![feature2.png](feature2.png)
3. Sensor Performance Analysis
![feature3.png](feature3.png)
- Model Development:


Anomaly Detection Algorithms: 
Use algorithm - Isolation Forest
![IsolationForest.png](IsolationForest.png)

**Store Detected Anomalies Back to BigQuery**
![anomalies0.png](anomalies0.png)
![anomalies1.png](anomalies1.png)


**Alerting and Notification System**
- Event Trigger System:

Set up alerts using services like AWS Lambda, Apache Airflow, or custom scripts to monitor real-time predictions.
Configure the system to trigger alerts when an anomaly (e.g., an unexpected drop in activity) is detected.
- Notification Channels:

Integrate with SMS, email, or mobile push notifications to alert caregivers and family members of anomalies.
Consider a configurable threshold setting for different users to customize alerts based on sensitivity.

**Data Visualization and Monitoring Dashboard**
- Dashboard Development:

Build a monitoring dashboard using tools like Tableau, Power BI, or Grafana.
Include visualizations such as time-series plots, daily activity patterns, anomaly occurrence graphs, and overall health scores.
- Real-Time Insights:

Display real-time metrics, alerts, and system health on the dashboard.
Implement custom views for different stakeholders (e.g., elderly individual’s family, caregivers, health providers).

Model Training and Evaluation
After preparing the dataset:

Split Data: Create training and test sets.
Select Model: Use machine learning models like Random Forest, SVM, or deep learning models like LSTMs (for time series).
Evaluate: Use metrics such as accuracy, F1-score, and confusion matrix to evaluate activity recognition performance.

Anomaly Detection
For anomaly detection (e.g., identifying unusual heart rates), you could implement threshold-based or machine learning-based approaches.

Threshold-based: Define acceptable ranges and flag outliers.
ML-based: Use unsupervised learning techniques like Isolation Forests or One-Class SVM.
Example for threshold-based anomaly detection on ecg_1:

