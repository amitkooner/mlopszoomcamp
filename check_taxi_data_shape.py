import pandas as pd

# URL to download the March 2024 Green Taxi data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-03.parquet"

# Load the data into a DataFrame
green_taxi_data = pd.read_parquet(url)

# Get the shape of the DataFrame
data_shape = green_taxi_data.shape

# Print the shape of the DataFrame
print(f"The shape of the data is: {data_shape}")
print(f"The number of rows in the data is: {data_shape[0]}")