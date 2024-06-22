import pandas as pd
from evidently import ColumnMapping
from evidently.report import Report
from evidently.dashboard import Dashboard
from evidently.dashboard.tabs import DataDriftTab, RegressionPerformanceTab
from evidently.metrics import ColumnQuantileMetric, ColumnMissingValuesMetric

# URL to download the March 2024 Green Taxi data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-03.parquet"

# Load the data into a DataFrame
green_taxi_data = pd.read_parquet(url)

# Ensure the 'lpep_pickup_datetime' column is in datetime format
green_taxi_data['lpep_pickup_datetime'] = pd.to_datetime(green_taxi_data['lpep_pickup_datetime'])

# Extract the date part
green_taxi_data['date'] = green_taxi_data['lpep_pickup_datetime'].dt.date

# Define the report with the desired metrics
report = Report(
    metrics=[
        ColumnQuantileMetric(column_name="fare_amount", quantile=0.5),
        ColumnMissingValuesMetric(column_name="fare_amount"),
    ]
)

# Run the report
report.run(current_data=green_taxi_data, reference_data=green_taxi_data)

# Print the report results
report_results = report.as_dict()
quantile_value = report_results['metrics'][0]['result']['current']['value']
missing_values = report_results['metrics'][1]['result']['current']['number_of_missing_values']

print(f"The 0.5 quantile value of the fare_amount column is: {quantile_value}")
print(f"The number of missing values in the fare_amount column is: {missing_values}")

# Define the dashboard with the desired tabs
dashboard = Dashboard(tabs=[DataDriftTab(), RegressionPerformanceTab()])

# Create a dummy target column for demonstration purposes
green_taxi_data['target'] = green_taxi_data['fare_amount']

# Define column mapping for the dashboard
column_mapping = ColumnMapping()
column_mapping.target = "target"

# Run the dashboard
dashboard.calculate(reference_data=green_taxi_data, current_data=green_taxi_data, column_mapping=column_mapping)

# Save the dashboard as HTML
dashboard.save("05-monitoring/dashboards/green_taxi_dashboard.html")

# Save the dashboard config
dashboard_config = dashboard.json()
with open("05-monitoring/config/dashboard_config.json", "w") as f:
    f.write(dashboard_config)

print("Dashboard created and saved successfully.")