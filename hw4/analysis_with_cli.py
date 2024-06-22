import argparse
import pickle
import pandas as pd
import numpy as np

# Function to read and preprocess data
def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

# Load the model
with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)

# Define categorical columns
categorical = ['PULocationID', 'DOLocationID']

# Parse command line arguments
parser = argparse.ArgumentParser(description='Predict taxi trip durations.')
parser.add_argument('--year', type=int, required=True, help='Year of the data')
parser.add_argument('--month', type=int, required=True, help='Month of the data')

args = parser.parse_args()
year = args.year
month = args.month

# Generate the filename based on the provided year and month
filename = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'

# Load the data
df = read_data(filename)

# Create an artificial ride_id column
df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

# Prepare the data for prediction
dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)

# Predict durations
y_pred = model.predict(X_val)

# Create a dataframe with results
df_result = pd.DataFrame({
    'ride_id': df['ride_id'],
    'predicted_duration': y_pred
})

# Save the results to a parquet file
output_file = f'predicted_durations_{year:04d}_{month:02d}.parquet'
df_result.to_parquet(
    output_file,
    engine='pyarrow',
    compression=None,
    index=False
)

# Calculate and print the mean predicted duration
mean_predicted_duration = np.mean(y_pred)
print(f"The mean predicted duration for {year:04d}-{month:02d} is: {mean_predicted_duration:.2f}")

# Check the size of the output file
import os
file_size = os.path.getsize(output_file) / (1024 * 1024)  # Size in MB
print(f"The size of the output file is: {file_size:.2f} MB")