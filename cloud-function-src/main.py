def main(request):
    import json
    projects =  list_projects()

    table_id = "nais-analyse-prod-2dcc.navbilling.gcp_projects"
    #table_id = "nais-billing.navbilling.gcp_projects"
    update_projects_in_bq(projects, table_id)

    return json.dumps({'success':True}), 200, {'ContentType':'application/json'}


def update_projects_in_bq(projects, table_id):
    # Construct BQ client
    from google.cloud import bigquery
    client = bigquery.Client()

    # Delete and recreate table (trunc workaround)
    schema = [bigquery.SchemaField("project", "STRING", mode="REQUIRED"),
              bigquery.SchemaField("team", "STRING", mode="NULLABLE")]
    table = bigquery.Table(table_id, schema=schema)
    truncate_target_table(client, table_id, table)

    # Insert rows
    client.insert_rows_from_dataframe(table, projects)

    return True


def list_projects():
    # Initialize resource manager client
    from google.cloud import resource_manager
    client = resource_manager.Client()
    import pandas as pd

    projects = pd.DataFrame(columns=['project', 'team'])

    # PROD
    for project in client.list_projects():
        name = project.name
        if 'team' in project.labels:
            team = project.labels['team']
        else:
            team = None

        projects = projects.append({'project': name, 'team': team}, ignore_index=True)

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

    return True