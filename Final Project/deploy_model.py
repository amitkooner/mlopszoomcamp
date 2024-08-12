from google.cloud import aiplatform

# Initialize the AI Platform model
aiplatform.init(project="sincere-nirvana-340100", location="us-central1")

# Create and deploy the model
model = aiplatform.Model.upload(
    display_name="ml_ops_zoomcamp_final_project_model",
    artifact_uri="gs://ml_ops_zoomcamp_final_project_data_bucket/model/",
    serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-3:latest",
)

# Deploy the model to an endpoint
endpoint = model.deploy(machine_type="n1-standard-4")

print(f"Model deployed to endpoint: {endpoint.resource_name}")