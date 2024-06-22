import pandas as pd
from evidently.metrics import ColumnQuantileMetric
from evidently.report import Report

# URL to download the March 2024 Green Taxi data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-03.parquet"

# Load the data into a DataFrame
green_taxi_data = pd.read_parquet(url)

# Ensure the 'lpep_pickup_datetime' column is in datetime format
green_taxi_data['lpep_pickup_datetime'] = pd.to_datetime(green_taxi_data['lpep_pickup_datetime'])

# Extract the date part
green_taxi_data['date'] = green_taxi_data['lpep_pickup_datetime'].dt.date

# Initialize a list to store daily quantiles
daily_quantiles = []

# Calculate the 0.5 quantile for each day
for date, group in green_taxi_data.groupby('date'):
    report = Report(
        metrics=[
            ColumnQuantileMetric(column_name="fare_amount", quantile=0.5),
        ]
    )
    report.run(current_data=group, reference_data=group)
    report_results = report.as_dict()
    quantile_value = report_results['metrics'][0]['result']['current']['value']
    daily_quantiles.append(quantile_value)

# Determine the maximum quantile value
max_quantile_value = max(daily_quantiles)

print(f"The maximum value of the 0.5 quantile for the 'fare_amount' column during March 2024 is: {max_quantile_value}")