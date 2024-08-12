from prefect import task, Flow
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import mlflow
import mlflow.sklearn

# Define tasks for each step of the pipeline

@task
def load_data():
    # Define the full paths to your CSV files
    csv_files = [
        '/Users/AKooner/Desktop/coding/mlops_zoomcamp/Final Project/2014 Q1 (Jan-Mar)-Central.csv',
        '/Users/AKooner/Desktop/coding/mlops_zoomcamp/Final Project/2014 Q2 spring (Apr-Jun)-Central.csv',
        '/Users/AKooner/Desktop/coding/mlops_zoomcamp/Final Project/2014 Q3 (Jul-Sep)-Central.csv',
        '/Users/AKooner/Desktop/coding/mlops_zoomcamp/Final Project/2014 Q4 autumn (Oct-Dec)-Central.csv'
    ]
    
    # Read and concatenate all CSV files into a single DataFrame
    data_frames = []
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        data_frames.append(df)

    data = pd.concat(data_frames, ignore_index=True)
    return data

@task
def preprocess_data(data):
    # Combine Date and Time columns into a single datetime column
    data['Datetime'] = pd.to_datetime(data['Date'] + ' ' + data['Time'], format='%d/%m/%Y %H:%M:%S')
    
    # Feature engineering: extract temporal features
    data['Start Hour'] = data['Datetime'].dt.hour
    data['Start Day of Week'] = data['Datetime'].dt.dayofweek
    data['Start Month'] = data['Datetime'].dt.month
    
    # Define features and target
    features = ['Start Hour', 'Start Day of Week', 'Start Month']
    X = data[features]
    y = data['Count']  # Target variable
    
    return X, y

@task
def train_model(X, y):
    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Log model parameters
    n_estimators = 100
    max_depth = 10
    min_samples_split = 2
    min_samples_leaf = 1
    
    with mlflow.start_run():
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("max_depth", max_depth)
        mlflow.log_param("min_samples_split", min_samples_split)
        mlflow.log_param("min_samples_leaf", min_samples_leaf)
        
        # Train the model
        model = RandomForestRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            min_samples_leaf=min_samples_leaf,
            random_state=42
        )
        model.fit(X_train, y_train)
        
        # Make predictions and calculate metrics
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        
        # Log metrics
        mlflow.log_metric("mse", mse)
        
        # Log the model
        mlflow.sklearn.log_model(model, "random_forest_model")
        
        print(f"Model logged with MSE: {mse}")
    
    return mse

@task
def report_metrics(mse):
    print(f"Final MSE of the model: {mse}")

# Instantiate the flow without the `with` statement
flow = Flow("Bike_Rental_Prediction_Pipeline")

# Add tasks to the flow
data = load_data()
X, y = preprocess_data(data)
mse = train_model(X, y)
report_metrics(mse)

# Manually add tasks to the flow
flow.add_task(data)
flow.add_task(X)
flow.add_task(y)
flow.add_task(mse)

# Define dependencies (which tasks should run before others)
flow.set_dependencies(task=data, downstream_tasks=[X, y])
flow.set_dependencies(task=X, downstream_tasks=[mse])
flow.set_dependencies(task=mse, downstream_tasks=[report_metrics])

# Run the flow
if __name__ == "__main__":
    flow.run()