import pandas as pd
from evidently.report import Report
from evidently.metrics import ColumnQuantileMetric, ColumnMissingValuesMetric
from sqlalchemy import create_engine

# URL to download the March 2024 Green Taxi data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-03.parquet"

# Load the data into a DataFrame
green_taxi_data = pd.read_parquet(url)

# Ensure the 'lpep_pickup_datetime' column is in datetime format
green_taxi_data['lpep_pickup_datetime'] = pd.to_datetime(green_taxi_data['lpep_pickup_datetime'])

# Extract the date part
green_taxi_data['date'] = green_taxi_data['lpep_pickup_datetime'].dt.date

# Initialize an empty list to store daily metrics
metrics_list = []

# Calculate daily metrics
for date, group in green_taxi_data.groupby('date'):
    report = Report(
        metrics=[
            ColumnQuantileMetric(column_name="fare_amount", quantile=0.5),
            ColumnMissingValuesMetric(column_name="fare_amount")
        ]
    )
    report.run(current_data=group, reference_data=group)
    report_results = report.as_dict()
    
    quantile_value = report_results['metrics'][0]['result']['current']['value']
    missing_values = report_results['metrics'][1]['result']['current']['number_of_missing_values']
    
    metrics_list.append({
        "date": date,
        "quantile_0_5": quantile_value,
        "missing_values": missing_values
    })

# Create a DataFrame from the list of metrics
metrics_df = pd.DataFrame(metrics_list)

# Print the DataFrame with metrics
print(metrics_df)

# PostgreSQL connection details
db_url = "postgresql://username:password@localhost:5432/taxi_metrics"
engine = create_engine(db_url)

# Store metrics in the PostgreSQL database
metrics_df.to_sql('daily_metrics', engine, if_exists='replace', index=False)

print("Metrics stored in PostgreSQL successfully.")