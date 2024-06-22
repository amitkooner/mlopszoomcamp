import pandas as pd
from evidently.metrics import ColumnQuantileMetric, ColumnMissingValuesMetric
from evidently.report import Report

# URL to download the March 2024 Green Taxi data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-03.parquet"

# Load the data into a DataFrame
green_taxi_data = pd.read_parquet(url)

# Define the report with the desired metrics
report = Report(
    metrics=[
        ColumnQuantileMetric(column_name="fare_amount", quantile=0.5),
        ColumnMissingValuesMetric(column_name="fare_amount"),
        # Add other metrics here if needed
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