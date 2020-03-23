from google.cloud import storage
import sys
import builtins as exceptions
import looker_sdk 
import os
import time
from typing import Dict, Optional

sdk = looker_sdk.init40()
models = looker_sdk.models40

def swap_dataset(dataset: str="hubspot_marketing", connection: str = "brick-layer"):
    sdk.update_connection(connection, models.WriteDBConnection(database=dataset)) #Update connection reference 

def download_dashboard(dataset: str, id: int):
    child = os.fork() #Fork a process to run in the background
    if child == 0:
        run_render_task(dataset, id) #run render task as child 
    else:
        parent, status = os.waitpid(child, os.WNOHANG) #let parent exit 
    
def run_render_task(
    dataset: str,
    dashboard: str,
    style: str = "tiled",
    width: int = 612*2,
    height: int = 792*2,
    filters: Optional[Dict[str, str]] = None,
):

    #init render task
    task = sdk.create_dashboard_render_task(
        dashboard,
        "pdf",
        models.CreateDashboardRenderTask(
            dashboard_style=style,
            dashboard_filters=urllib.parse.urlencode(filters) if filters else None,
        ),
        width,
        height,
    )

    #check if task could be created correctly 
    if not (task and task.id):
        raise exceptions.RenderTaskError(
            f'Could not create a render task for "{pdf_name}"'
    )

    #get pdf name and init GCS client
    pdf_name = "{}-{}".format(dataset, id)
    storage_client = storage.Client()
    bucket = storage_client.bucket("brick-layer-testing")

    elapsed = 0.0 # poll the render task until it completes
    delay = 0.5  # wait .5 seconds
    while True:
        poll = sdk.render_task(task.id)
        if poll.status == "failure":
            print(poll)
            raise exceptions.RenderTaskError(f'Render failed for "{pdf_name}"')
        elif poll.status == "success":
            break

        time.sleep(delay)
        elapsed += delay

    result = sdk.render_task_results(task.id) #get render task results and set filename
    filename = f"bogus_test/{pdf_name}.pdf"

    
    blob = bucket.blob(filename) #init GCS blob to write to
    blob.upload_from_string(result, "application/pdf") 
    
    os._exit(0) #exit child process 