import json
import os

import pandas as pd
from google.cloud import bigquery, resourcemanager, secretmanager


def main(request):
    set_secret_as_env(secret_name='projects/871274913172/secrets/nav-organization-id/versions/latest')

    projects = list_projects()
    print(f'Found {len(projects)} projects in total')

    table_id = "nais-analyse-prod-2dcc.navbilling.gcp_projects"
    update_projects_in_bq(projects, table_id)

    return json.dumps({'success':True}), 200, {'ContentType':'application/json'}



def update_projects_in_bq(projects, table_id):
    # Construct BQ client
    client = bigquery.Client(project='nais-analyse-prod-2dcc')

    # Delete and recreate table (trunc workaround)
    schema = [bigquery.SchemaField("project", "STRING", mode="NULLABLE"),
              bigquery.SchemaField("project_id", "STRING", mode="NULLABLE"),
              bigquery.SchemaField("team", "STRING", mode="NULLABLE"),
              bigquery.SchemaField("tenant", "STRING", mode="NULLABLE"),
              bigquery.SchemaField("environment", "STRING", mode="NULLABLE")]
    table = bigquery.Table(table_id, schema=schema)
    table = truncate_target_table(client, table_id, table)

    # Insert rows
    try:
        client.insert_rows_from_dataframe(table, projects)
    except Exception as e:
        print(e)

    return True


def list_projects():    
    # Initialize resource manager client
    client = resourcemanager.ProjectsClient()

    folders = get_all_folder_ids_from_organization(os.environ['ORG_ID'])
    
    # Collect project data in a list of dictionaries
    project_data = []
    
    for folder_name, folder_ids in folders.items():
        for folder_id in folder_ids:
            for project in client.list_projects(parent=f"folders/{folder_id}"):
                name = project.name
                project_id = project.project_id
                team = get_label('team', project.labels)
                tenant = get_label('tenant', project.labels)
                environment = get_label('environment', project.labels)

                project_data.append({
                    'project': name,
                    'project_id': project_id,
                    'team': team,
                    'tenant': tenant,
                    'environment': environment
                })
            print(f'Found a total of {len(project_data)} projects after including folder {folder_name}, {folder_id}')

    # Create DataFrame from the list of dictionaries
    projects = pd.DataFrame(project_data, columns=['project', 'project_id', 'team', 'tenant', 'environment'])

    return projects


def truncate_target_table(client, table_id, table):
    from google.api_core.exceptions import AlreadyExists, NotFound

    # Delete table if exists
    try:
        client.delete_table(table_id)
        print(f'{table_id} deleted')
    except NotFound:
        print(f'Table {table_id} not found, not deleted')

    table = client.create_table(table)  # Make an API request.
    print(f'Created table {table.project}.{table.dataset_id}.{table.table_id}')

    return table


def get_all_folder_ids_from_organization(organization_id):
    folder_client = resourcemanager.FoldersClient()

    def get_subfolders(parent):
        subfolders = {}
        folders = folder_client.list_folders(parent=parent)
        for folder in folders:
            folder_name = folder.display_name
            folder_id = folder.name.split('/')[-1]
            if folder_name not in subfolders:
                subfolders[folder_name] = []
            subfolders[folder_name].append(folder_id)
            # Recursively get subfolders
            nested_subfolders = get_subfolders(f'folders/{folder_id}')
            for nested_name, nested_ids in nested_subfolders.items():
                if nested_name not in subfolders:
                    subfolders[nested_name] = []
                subfolders[nested_name].extend(nested_ids)
        return subfolders

    # Get all folders under the organization
    all_folders = get_subfolders(f'organizations/{organization_id}')
    return all_folders


def get_label(label, labels):
    if label in labels:
        return labels[label]
    else:
        return None


def set_secret_as_env(secret_name, split_on='='):
    secrets = secretmanager.SecretManagerServiceClient()
    secret = secrets.access_secret_version(name=secret_name)
    secrets = secret.payload.data.decode('UTF-8')
    for secret in secrets.splitlines():
        key, value = secret.split(split_on)
        os.environ[key] = value