from google.cloud import storage, bigquery
import builtins as exceptions
import urllib
import looker_sdk 
import json
import time
import requests

sdk = looker_sdk.init40()
models = looker_sdk.models40

bq_client = bigquery.Client()
storage_client = storage.Client()
dashboard_dataset = "{}.bogus".format(bq_client.project)

def swap_dataset(dataset="hubspot_marketing", connection = "brick-layer"):
    """Swaps the 'dataset' parameter for a given connection on app.dev"""
    sdk.update_connection(connection, models.WriteDBConnection(database=dataset)) #Update connection reference 
    
def download_dashboard(
    dataset,
    dashboard,
    style = "tiled",
    width = 612*2,
    height = 792*2,
    filters = None,
):
    """
    Creates render jobs for given dashbaord id. 
    Uploads jpg to GCS and inserts rows in BQ table w/dashboard metadata
    """
    #init render task
    task = sdk.create_dashboard_render_task(
        dashboard,
        "jpg",
        models.CreateDashboardRenderTask(
            dashboard_style=style,
            dashboard_filters=None,
        ),
        width,
        height,
    )

    #check if task could be created correctly 
    if not (task and task.id):
        raise exceptions.RenderTaskError(
            'Could not create a render task for "{}"'.format(dashboard)
    )

    #get jpg name and init GCS client
    jpg_name = "{}-{}".format(dataset, dashboard)
    bucket = storage_client.bucket("brick-layer-testing")

    elapsed = 0.0 # poll the render task until it completes
    delay = 0.5  # wait .5 seconds
    while True:
        poll = sdk.render_task(task.id)
        if poll.status == "failure":
            print(poll)
            raise exceptions.RenderTaskError('Render failed for "{}"'.format(jpg_name))
        elif poll.status == "success":
            break

        time.sleep(delay)
        elapsed += delay

    result = sdk.render_task_results(task.id) #get render task results and set filename
    
    filename = "bogus_test/{}.jpg".format(jpg_name)
    #result_str = base64.b64encode(result).decode('utf8')

    blob = bucket.blob(filename) #init GCS blob to write to
    blob.upload_from_string(result, "image/jpg") 
    
    table = bq_client.get_table("{}.bogus_dashboards".format(dashboard_dataset))  #get table reference 
    rows_to_insert = [(dataset, dashboard, "{}.jpg".format(jpg_name), elapsed)]

    errors = bq_client.insert_rows(table, rows_to_insert)  #insert rows to BQ table
    if errors == []:
        print("New rows have been added.")
    
def make_table(table_name):
    """
    Creates bq table to hold dashboard metadata and jpg names
    """
    table_id = "{}.{}".format(dashboard_dataset, table_name)
    bq_client.delete_table(table_id, not_found_ok=True)

    schema = [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dashboard_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("image", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("run_time", "FLOAT", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = bq_client.create_table(table) 


def get_dashboards(dataset):
    """
    Runs an inline query against system activity 
    on app.dev to pull dashboard ids that contain the model name
    """
    print(dataset)
    query = models.WriteQuery(
        model = 'system__activity',
        view  = 'dashboard',
        fields = ['dashboard.id'],
        filters = {
            'space.name'        : "%{}%".format(dataset)
        },
        limit=10
    )
    return json.loads(sdk.run_inline_query('json', query))
    

def main(request):
    req = request.get_json() 
    model = req['data']['value']
    form_params = req['form_params']

    if form_params['reset_db'] == 'replace':
        make_table("bogus_dashboards")

    dataset = form_params['dataset']

    if form_params['make_new_data'] == 'yes':
    #send request to makedata to generate a given variant 
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        r = requests.post("https://us-central1-spencer-white-tckt87992.cloudfunctions.net/makedata", json={"data": req["data"], "form_params": form_params}, headers=headers)
        
    #swap the connection to given variant
    swap_dataset(dataset)

    #get dashboard ids and generate jpgs
    dashboard_ids = get_dashboards(model)
    print(dashboard_ids)
    for dashboard in dashboard_ids:
        if dashboard['dashboard.id'] == 711:
            continue
        download_dashboard(dataset, str(dashboard['dashboard.id']))

    #return connection to default
    swap_dataset()

    return json.dumps({
        'looker' : {
            'success' : True
        }
    })

