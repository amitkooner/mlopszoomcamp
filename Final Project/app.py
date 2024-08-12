from flask import Flask, request, jsonify
import mlflow.sklearn
import pandas as pd

app = Flask(__name__)

# Load the model as a global variable
model = None

@app.before_first_request
def load_model():
    global model
    model = mlflow.sklearn.load_model("random_forest_model")  # Adjust the path as needed

@app.route('/predict', methods=['POST'])
def predict():
    # Get the data from the request
    data = request.get_json(force=True)
    df = pd.DataFrame(data)
    
    # Make predictions
    predictions = model.predict(df)
    
    # Return predictions as JSON
    return jsonify(predictions.tolist())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)