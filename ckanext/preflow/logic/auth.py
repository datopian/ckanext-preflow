from ckan.types import Any, AuthResult, Context

import ckan.plugins as p
import ckan.plugins.toolkit as tk

import ckanext.datastore.logic.auth as auth


def preflow_submit(context: Context, data_dict: dict[str, Any]) -> AuthResult:
    """
    Check auth for the Prefect flow submission action.
    """
    return auth.datastore_auth(context, data_dict)


@tk.side_effect_free
def preflow_status(context: Context, data_dict: dict[str, Any]) -> AuthResult:
    """
    Check auth for the Prefect flow status action.
    """
    return auth.datastore_auth(context, data_dict, "resource_show")


@tk.side_effect_free
def preflow_status_update(context: Context, data_dict: dict[str, Any]) -> AuthResult:
    """
    Check auth for the Prefect flow status update action.
    """
    return auth.datastore_auth(context, data_dict)
