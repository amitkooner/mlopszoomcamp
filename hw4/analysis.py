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

# Prepare the data for prediction
dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)

# Predict durations
y_pred = model.predict(X_val)

# Calculate the standard deviation
std_dev = np.std(y_pred)
print(f"The standard deviation of the predicted duration is: {std_dev}")