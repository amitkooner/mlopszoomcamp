# ML Ops Final Project

## Overview

This project demonstrates the complete end-to-end process of building, deploying, and monitoring a machine learning model. The project uses data from Santander bike rentals in Central London during 2014. The key steps include data processing, model training, deployment, and monitoring, all orchestrated using modern ML Ops tools.

## Data

The dataset used in this project consists of bike rental data from Santander in Central London during 2014. The data is split into four quarters:

- **Q1 (Jan-Mar)**
- **Q2 (Apr-Jun)**
- **Q3 (Jul-Sep)**
- **Q4 (Oct-Dec)**

Each dataset includes the following features:

- `Year`: The year and quarter of the data.
- `UnqID`: Unique identifier for each record.
- `Date`: The date of the rental.
- `Weather`: Weather conditions during the rental.
- `Time`: The time of day the rental started.
- `Day`: The day of the week.
- `Round`: The round or sequence of the rental.
- `Dir`: The direction of the bike trip.
- `Path`: The path taken during the trip.
- `Mode`: The mode of transport.
- `Count`: The number of rentals.

## Project Structure

The project contains the following key files and scripts:

### **1. `Dockerfile`**
The Dockerfile defines the environment for the project, including all dependencies required to run the machine learning model. It is used to build a Docker image, which can be deployed to a cloud service.

### **2. `requirements.txt`**
This file lists all the Python packages and dependencies required for the project, including:

- `pandas`
- `scikit-learn`
- `mlflow`
- `prefect`
- `evidently`
- `flask`
- `gunicorn`

### **3. `app.py`**
This script serves the trained machine learning model via a Flask web service. It exposes an API endpoint (`/predict`) where users can send data and receive predictions.

### **4. `pipeline.py`**
The pipeline script orchestrates the entire machine learning workflow using Prefect. It includes tasks for loading data, preprocessing, model training, and evaluating the model. The pipeline ensures that each step is executed in the correct order.

### **5. `monitor_model.py`**
This script sets up monitoring for the deployed model using Evidently. It tracks model performance over time, detects data drift, and generates a monitoring dashboard.

### **6. `.github/workflows/main.yml`**
This file (to be created in Step 8) sets up a Continuous Integration/Continuous Deployment (CI/CD) pipeline using GitHub Actions. It automates the testing and deployment of the project every time code is pushed to the repository.

## How to Run the Project

### **1. Clone the Repository**
```bash
git clone https://github.com/YOUR_USERNAME/ml_ops_final_project.git
cd ml_ops_final_project