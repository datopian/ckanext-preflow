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
    :param data_dict: Dictionary of parameters for the flow run.
        Must include:
            - source_url (str): URL of the source data to ingest.
            - ckan_url (str): Base URL of the CKAN instance.
            - organization_id (str): ID of the CKAN organization.
            - resource (dict): CKAN resource metadata, including:
                - id (str): Resource ID.
                - name (str): Resource name.
                - schema (dict or None): Schema definition.
                - format (str): Resource format (e.g., 'csv', 'json').
                 - flow_parameters (dict): Additional Prefect flow parameters.
            - run_asynchronous (bool): Submit flow run asynchronously.
            - prefect_labels (list): Prefect agent labels.
            - prefect_api_key (str): Prefect Cloud API key.
            - prefect_flow_id (str): Prefect flow ID to trigger.
        Optional:
            - bigquery_config (dict): BigQuery configuration.

    :returns: A dictionary with Prefect flow run metadata and status.
    :rtype: dict
    """
    log.debug("Submitting Prefect flow with parameters: %s", data_dict)
    tk.check_access("preflow_submit", context, data_dict)

    if (data_dict.get("url_type") == "datastore") or (
        "_datastore_only_resource" in data_dict.get("url")
    ):
        log.debug("Dump files are managed with the Datastore API")
        p.toolkit.get_action("aircan_status_update")(
            context,
            {
                "resource_id": data_dict.get("id", ""),
                "state": "complete",
                "message": "Dump files are managed with the Datastore API",
                "clear": True,
            },
        )

    flow_payload = {
        "parameters": {},
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
                "message": f"Data ingestion is add to the queue for processing with flow run ID: '{flow_run_data.get('id')}'",
                "clear": True,
            },
        )
        return flow_run_data
    except requests.RequestException as e:
        log.error("Failed to create Prefect flow run: %s", e)
        raise tk.ValidationError(f"Failed to create Prefect flow run: {e}")


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
        context, {"entity_id": res_id, "task_type": "preflow", "key": "ingestion"}
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


def preflow_hook(context: Context, data_dict: dict[str, str]) -> dict[str, str]:
    pass


def preflow_status_update(
    context: Context, data_dict: dict[str, str]
) -> dict[str, str]:
    """
    Update the preflow status for a resource, appending log entries.
    """
    now = str(datetime.datetime.utcnow())
    new_log_entry = {"datetime": now, "message": data_dict.get("message", "")}
    resource_id = data_dict.get("resource_id")
    flow_run_id = data_dict.get("flow_run_id", "")
    state = data_dict.get("state", "")
    clear_log = data_dict.get("clear", False)
    key = data_dict.get("key", "ingestion")  # Default key for ingestion tasks

    # Get previous logs unless clearing
    logs_entry = []
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
            flow_run_id = json.loads(value).get("flow_run_id", flow_run_id)
            logs_entry = json.loads(value).get("logs_entry", []) if value else []
        except Exception:
            pass
    logs_entry.append(new_log_entry)

    value = {
        "flow_run_id": flow_run_id,
        "logs_entry": logs_entry,
    }

    task_dict = {
        "entity_id": resource_id,
        "entity_type": "resource",
        "task_type": "preflow",
        "state": state,
        "last_updated": data_dict.get("last_updated", now),
        "key": key,
        "value": json.dumps(value),
        "error": json.dumps(data_dict.get("error")),
    }

    return tk.get_action("task_status_update")(context, task_dict)
