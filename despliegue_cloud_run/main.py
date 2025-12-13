import os
from google.cloud import aiplatform

def run_pipeline():
    
    project = os.environ.get("PROJECT_ID", "mlops13")
    location = os.environ.get("LOCATION", "us-central1")
    
    
    pipeline_path = os.environ.get("PIPELINE_PATH", "gs://mlops13/projectB/pipeline-prediction/pipeline_bigquery.json")
    service_account = os.environ.get("SERVICE_ACCOUNT", "vertex-process@mlops13.iam.gserviceaccount.com")
    validate_table = os.environ.get("VALIDA_TABLE", "mlops13.github_mlops.census_by_age")
    input_age = os.environ.get("INPUT_AGE", 60)


    aiplatform.init(project=project, location=location)

    job = aiplatform.PipelineJob(
        display_name="bigquery test pipeline",
        template_path=pipeline_path,
        enable_caching=False,
        project=project,
        location=location,
        parameter_values={
            "project": project,
            "validate_table": validate_table,
            "input_age": input_age
        }
    )

    print('submit pipeline job ...')
    job.submit(service_account=service_account)

if __name__ == "__main__":
    run_pipeline()
