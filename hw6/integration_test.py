import os
import pandas as pd
from datetime import datetime

# Helper function to create datetime objects
def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

# Create the DataFrame
data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
]

columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df_input = pd.DataFrame(data, columns=columns)

# Set up the S3 endpoint URL and bucket information
S3_ENDPOINT_URL = "http://localhost:4566"
input_file = "s3://nyc-duration/in/2023-01.parquet"
output_file = "s3://nyc-duration/out/2023-01.parquet"

# Storage options for pyarrow
options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}

# Save the DataFrame to S3
df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)

# Set environment variables
os.environ['INPUT_FILE_PATTERN'] = 's3://nyc-duration/in/{year:04d}-{month:02d}.parquet'
os.environ['OUTPUT_FILE_PATTERN'] = 's3://nyc-duration/out/{year:04d}-{month:02d}.parquet'
os.environ['S3_ENDPOINT_URL'] = S3_ENDPOINT_URL

# Run the batch script
os.system('python batch.py 2023 1')

# Read the output file from S3
df_result = pd.read_parquet(output_file, storage_options=options)

# Calculate the sum of predicted durations
predicted_duration_sum = df_result['predicted_duration'].sum()
print(f'Sum of predicted durations: {predicted_duration_sum}')
