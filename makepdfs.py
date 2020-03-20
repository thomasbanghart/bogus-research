import sys
import builtins as exceptions
import looker_sdk 
import time
from typing import Dict, Optional

sdk = looker_sdk.init40()
models = looker_sdk.models40

def swap_dataset(connection: str = "brick-layer", dataset: str="hubspot_marketing"):
    sdk.update_connection(connection, models.WriteDBConnection(database=dataset)) #Update connection reference 
    return
    
def download_dashboard(
    # dashboard: models.Dashboard,
    dashboard: id,
    style: str = "tiled",
    width: int = 612*2,
    height: int = 792*2,
    filters: Optional[Dict[str, str]] = None,
):
    """Download specified dashboard as PDF"""
    # assert dashboard.id
    # id = int(dashboard.id)
    task = sdk.create_dashboard_render_task(
        id,
        "pdf",
        models.CreateDashboardRenderTask(
            dashboard_style=style,
            dashboard_filters=urllib.parse.urlencode(filters) if filters else None,
        ),
        width,
        height,
    )

    if not (task and task.id):
        raise exceptions.RenderTaskError(
            f'Could not create a render task for "{dashboard.title}"'
        )

    # poll the render task until it completes
    elapsed = 0.0
    delay = 0.5  # wait .5 seconds
    while True:
        poll = sdk.render_task(task.id)
        if poll.status == "failure":
            print(poll)
            raise exceptions.RenderTaskError(f'Render failed for "{dashboard.title}"')
        elif poll.status == "success":
            break

        time.sleep(delay)
        elapsed += delay
    print(f"Render task completed in {elapsed} seconds")

    result = sdk.render_task_results(task.id)
    filename = f"{dashboard}.pdf"
    with open(filename, "wb") as f:
        f.write(result)
    print(f'Dashboard pdf saved to "{filename}"')

    return

