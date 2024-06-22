FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

WORKDIR /app

# Copy the script into the container
COPY analysis_with_cli.py .

# Install the required libraries
RUN pip install pandas pyarrow scikit-learn==1.5.0

# Define the entry point for the container
ENTRYPOINT ["python", "analysis_with_cli.py"]