def main(request):
    import json
    projects = list_projects()
    print(f'Found {len(projects)} projects')

    table_id = "nais-analyse-prod-2dcc.navbilling.gcp_projects"
    update_projects_in_bq(projects, table_id)

    return json.dumps({'success':True}), 200, {'ContentType':'application/json'}


def update_projects_in_bq(projects, table_id):
    # Construct BQ client
    from google.cloud import bigquery
    client = bigquery.Client()

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
    from google.cloud import resource_manager
    client = resource_manager.Client()
    import pandas as pd

    projects = pd.DataFrame(columns=['project', 'project_id', 'team', 'tenant', 'environment'])

    # PROD
    for project in client.list_projects():
        name = project.name
        project_id = project.project_id
        team = get_label('team', project.labels)
        tenant = get_label('tenant', project.labels)
        environment = get_label('environment', project.labels)

        projects = projects.append({
            'project': name,
            'project_id': project_id,
            'team': team,
            'tenant': tenant,
            'environment': environment
        }, ignore_index=True)

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


def get_label(label, labels):
    if label in labels:
        return labels[label]
    else:
        return None
