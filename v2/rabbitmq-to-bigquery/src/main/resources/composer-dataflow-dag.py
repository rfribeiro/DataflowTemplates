"""Example Airflow DAG that creates a Cloud Dataflow workflow which takes a
text file and adds the rows to a BigQuery table.

This DAG relies on four Airflow variables
https://airflow.apache.org/concepts.html#variables
* project_id - Google Cloud Project ID to use for the Cloud Dataflow cluster.
* gce_zone - Google Compute Engine zone where Cloud Dataflow cluster should be
  created.
* gce_region - Google Compute Engine region where Cloud Dataflow cluster should be
  created.
Learn more about the difference between the two here:
https://cloud.google.com/compute/docs/regions-zones
* bucket_path - Google Cloud Storage bucket where you've stored the User Defined
Function (.js), the input file (.txt), and the JSON schema (.json).
"""

from datetime import datetime, timedelta

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.utils.dates import days_ago

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "dataflow_default_options": {
        "project": project_id,
        # Set to your region
        "region": gce_region,
        # Set to your zone
        "zone": gce_zone,
        # This is a subfolder for storing temporary files, like the staged pipeline job.
        "temp_location": bucket_path + "/tmp/",
    },
}
job = f'rabbitmqtobigquery{datetime.now().strftime("%Y%m%d%H%M%S%f")}'
parameters = {
			'outputTableSpec': 'rabbitmq-integration-301417:rabbitmq.payload_history',
			'javascriptTextTransformGcsPath': 'gs://rabbitmq-storage/udf-template/transform.js',
			'javascriptTextTransformFunctionName': 'transform'
}
template = "gs://rabbitmq-storage/templates/rabbitmq-to-bigquery-image-spec.json"

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    # The id you will see in the DAG airflow page
    "composer_dataflow_dag",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval=timedelta(days=1),  # Override to match your needs
) as dag:
    start_template_job = DataflowStartFlexTemplateOperator(
        # The task id of your job
        task_id="dataflow_operator_rabbitmq_to_bq",
        project_id="rabbitmq-integration-301417",
        location="us-central1",
        wait_until_finished=False,
        body={
				'launchParameter': {
					'jobName': job,
					'containerSpecGcsPath': template,
					'parameters': parameters
				}
			}
    )
