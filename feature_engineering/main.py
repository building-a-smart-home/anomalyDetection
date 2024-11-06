from feature_engineering import avg_heartRate, activity_pattern_analysis, sensor_performance_analysis
def main():

    # Specify the dataset and table names
    project_id = 'heya-440312'
    dataset_id = 'mHealth2024'
    table_id = 'activity_facts'

    # Fetch feature engineering results from BigQuery
    features_df = avg_heartRate()
    print("Extract average heart rate and other sensor features :: ")
    # Print the resulting DataFrame with features
    print(features_df.head())
    activity_pattern_analysis()
    sensor_performance_analysis(activity_name='Running')

if __name__ == '__main__':
    main()
