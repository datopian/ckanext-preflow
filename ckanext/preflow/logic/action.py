# encoding: utf-8
from ckan.types import Context

import logging
import json
import datetime
import requests

import ckan.plugins as p
import ckan.plugins.toolkit as tk

log = logging.getLogger(__name__)


def preflow_submit(context: Context, data_dict: dict[str, str]) -> dict[str, str]:
    """
    Submits a Prefect flow run for data ingestion and processing.

    :param context: The context dictionary, usually containing user and authorization information.
    :type context: dict
    :param data_dict: Resource dictionary with parameters for the flow run.
    :returns: A dictionary with Prefect flow run metadata and status.
    :rtype: dict
    """
    log.debug("Submitting Prefect flow with parameters: %s", data_dict)
    tk.check_access("preflow_submit", context, data_dict)

    if data_dict.get("url_type") == "datastore" or "_datastore_only_resource" in (
        data_dict.get("url") or ""
    ):
        log.debug("Dump files are managed with the Datastore API")
        p.toolkit.get_action("preflow_status_update")(
            context,
            {
                "resource_id": data_dict.get("id", ""),
                "state": "complete",
                "message": "Dump files are managed with the Datastore API",
                "clear": True,
            },
        )

    data_dict["schema"] = data_dict.get("schemas", {})

    flow_payload = {
        "parameters": {
            "resource_dict": data_dict,
            "ckan_config": {
                "ckan_url": tk.config.get("ckan.site_url"),
                "api_key": tk.config.get("ckanext.preflow.api_key", ""),
                "datastore_db_url": tk.config.get("ckan.datastore.write_url", ""),
            },
        },
        "tags": [],
        "state": {
            "type": "SCHEDULED",
            "message": "",
        },
    }

    deployment_id = tk.config.get("ckanext.preflow.prefect_deployment_id")
    prefect_api_url = tk.config.get(
        "ckanext.preflow.prefect_api_url", "http://127.0.0.1:4200/api"
    )
    prefect_api_key = tk.config.get("ckanext.preflow.prefect_api_key")

    headers = {"Content-Type": "application/json"}
    if prefect_api_key:
        headers["Authorization"] = f"Bearer {prefect_api_key}"

    try:
        flow_run = requests.post(
            f"{prefect_api_url}/deployments/{deployment_id}/create_flow_run",
            headers=headers,
            json=flow_payload,
        )
        flow_run.raise_for_status()
        flow_run_data = flow_run.json()
        log.info("Flow run created successfully: %s", flow_run_data)

        tk.get_action("preflow_status_update")(
            context,
            {
                "resource_id": data_dict.get("id", ""),
                "state": "Pending",
                "flow_run_id": flow_run_data.get("id"),
                "message": f"Data processing is scheduled with Prefect flow run ID: {flow_run_data.get('id')}",
                "clear": True,
            },
        )
        return flow_run_data
    except requests.RequestException as e:
        log.error("Failed to create Prefect flow run: %s", e)
        tk.get_action("preflow_status_update")(
            context,
            {
                "resource_id": data_dict.get("id", ""),
                "state": "Failed",
                "type": "error",
                "message": f"Failed to create Prefect flow run: {str(e)}",
                "clear": True,
            },
        )


@tk.side_effect_free
def preflow_status(context: Context, data_dict: dict[str, str]) -> dict[str, str]:
    """
    Retrieve the status of a Preflow run associated with a CKAN resource.

    :param context: The context dictionary, usually containing user and authorization information.
    :type context: dict
    :param data_dict: Dictionary with parameters for the flow status check.
        Must include:
            - resource_id (str): ID of the CKAN resource to check the flow status for preflow RUN.
            - flow_run_id (str, optional): Specific Prefect flow run ID to retrieve status for.
    :type data_dict: dict

    :returns: A dictionary containing the Prefect flow run status and related metadata.
    :rtype: dict

    :raises NotFound: If the resource or flow run is not found.
    :raises NotAuthorized: If the user is not authorized to view the resource status.

    Example::
        {'id': 'xxxx', 'flow_run_id': 'xyz-789', 'state': 'Success', ...}
    """
    # Use 'id' as 'resource_id' if present
    if "id" in data_dict:
        data_dict["resource_id"] = data_dict["id"]

    res_id = tk.get_or_bust(data_dict, "resource_id")

    task_status = p.toolkit.get_action("task_status_show")(
        context, {"entity_id": res_id, "task_type": "preflow", "key": "pipeline"}
    )

    prefect_api_url = tk.config.get(
        "ckanext.preflow.prefect_api_url", "http://127.0.0.1:4200/api"
    )
    prefect_api_key = tk.config.get("ckanext.preflow.prefect_api_key")

    try:
        flow_run_id = json.loads(task_status.get("value", "{}")).get("flow_run_id", "")
    except Exception:
        log.warning(f"Task status for resource {res_id} not found or invalid.")
        flow_run_id = ""

    flow_run_id = data_dict.get("flow_run_id") or flow_run_id

    headers = {"Authorization": f"Bearer {prefect_api_key}"} if prefect_api_key else {}
    try:
        response = requests.get(
            f"{prefect_api_url}/flow_runs/{flow_run_id}", headers=headers
        )
        response.raise_for_status()
        flow_run_status = response.json()

        state = flow_run_status.get("state", {}).get("type") or task_status.get("state")

        task_status["state"] = state.lower()

        task_status["flow_run_id"] = flow_run_id
        task_status["flow_run_details"] = flow_run_status
    except requests.RequestException as e:
        log.error(f"Failed to fetch Prefect flow run status: {e}")

    return task_status


def preflow_status_update(
    context: Context, data_dict: dict[str, str]
) -> dict[str, str]:
    """
    Update the preflow status for a resource, appending log entries.
    """
    now = str(datetime.datetime.utcnow())
    message = data_dict.get("message", "")
    resource_id = data_dict.get("resource_id")
    flow_run_id = data_dict.get("flow_run_id", "")
    state = data_dict.get("state", "")
    clear_log = data_dict.get("clear", False)
    key = data_dict.get("key", "pipeline")
    _type = data_dict.get("type", "info")
    validation_report = data_dict.get("validation_report")

    logs, previous_report = [], None
    if not clear_log:
        try:
            value = p.toolkit.get_action("task_status_show")(
                context,
                {
                    "entity_id": resource_id,
                    "task_type": "preflow",
                    "key": key,
                },
            ).get("value")
            if value:
                parsed = json.loads(value)
                logs = parsed.get("logs", [])
                previous_report = parsed.get("validation_report")
        except Exception:
            pass

    validation_report = validation_report or previous_report
    logs.append({"datetime": now, "message": message})

    value = {
        "flow_run_id": flow_run_id,
        "logs": logs,
        **(
            {"validation_report": validation_report}
            if validation_report and _type != "error"
            else {}
        ),
    }

    error = None
    if _type == "error":
        error = {"message": message}
        if validation_report:
            error["validation_report"] = validation_report

    task_dict = {
        "entity_id": resource_id,
        "entity_type": "resource",
        "task_type": "preflow",
        "state": "failed" if _type == "error" else state,
        "last_updated": data_dict.get("last_updated", now),
        "key": key,
        "value": json.dumps(value),
        "error": "" if clear_log else (json.dumps(error) if error else None),
    }

    return tk.get_action("task_status_update")(context, task_dict)


def preflow_hook(context: Context, data_dict: dict[str, str]) -> dict[str, str]:
    pass
