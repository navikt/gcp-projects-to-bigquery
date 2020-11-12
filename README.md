# GCP Projects to BigQuery
Cloud Function that reads Resource Manager API to extract projects and team labels and writes these to BigQuery.
Cloud Scheduler to trigger the Function every day. 

## Requirements
* service account `projects-to-bigquery` with org level permissions to read resource manager API