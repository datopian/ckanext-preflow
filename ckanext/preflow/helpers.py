import json
from ckan.plugins import toolkit as tk
import ckan.model as model

def get_preflow_badge(resource_id: str):
    """
    Helper function to get the status of a preflow for a given resource ID.
    Returns a dictionary with the status information.
    """
    context = {
        "model": model,
        "ignore_auth": True,
    }
    
    try:
        preflow_status = tk.get_action("preflow_status")(context, {"id": resource_id})
    except (tk.ObjectNotFound, tk.ValidationError):
        return ""

    if not preflow_status:
        return ""

    status = preflow_status.get("state", "").capitalize()



    badge_class = {
        "Completed": "bg-success text-white",
        "Failed": "bg-danger text-white",
        "Pending": "bg-warning text-dark",
    }.get(status, "bg-secondary text-white")

    # Create animated badge with initial dot and hover expansion
    return (
        f'<span class="badge-container d-inline-flex align-items-center">'
        f'<span class="badge-label rounded-pill overflow-hidden border" style="font-size: 12px;">'
        f'<span class="bg-dark text-white px-2 py-1">Pipeline</span>'
        f'<span class="{badge_class} px-2 py-1">{status}</span>'
        '</span>'
        '</span>'
    )