import pandas as pd

# Load one of the CSV files to inspect
df = pd.read_csv('/Users/AKooner/Desktop/coding/mlops_zoomcamp/Final Project/2014 Q1 (Jan-Mar)-Central.csv')

# Display the first few rows and the columns
print(df.head())
print(df.columns)