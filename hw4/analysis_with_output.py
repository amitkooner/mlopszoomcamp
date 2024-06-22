import pickle
import pandas as pd
import numpy as np

# Load the model
with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)

# Define categorical columns
categorical = ['PULocationID', 'DOLocationID']

# Function to read and preprocess data
def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

# Load the data
df = read_data('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet')

# Create an artificial ride_id column
year = 2023
month = 3
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
output_file = 'predicted_durations.parquet'
df_result.to_parquet(
    output_file,
    engine='pyarrow',
    compression=None,
    index=False
)

# Check the size of the output file
import os
file_size = os.path.getsize(output_file) / (1024 * 1024)  # Size in MB
print(f"The size of the output file is: {file_size:.2f} MB")