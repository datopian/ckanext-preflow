from ckan.types import Any, Action, AuthFunction, Schema

import logging


import ckan.plugins as p
import ckan.plugins.toolkit as tk
import ckan.model as model

from ckanext.preflow.logic import action, auth
from ckanext.preflow.views import preflow


DEFAULT_FORMATS = [
    "csv",
    "xls",
    "xlsx",
    "tsv",
    "ssv",
    "tab",
    "ods",
    "geojson",
    "shp",
    "qgis",
    "zip",
]

log = logging.getLogger(__name__)


class PreflowPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IResourceUrlChange)
    p.implements(p.IResourceController, inherit=True)
    p.implements(p.IBlueprint)

    # IConfigurer
    def update_config(self, config_):
        tk.add_template_directory(config_, "templates")
        tk.add_public_directory(config_, "public")
        tk.add_resource("assets", "preflow")

    def update_config_schema(self, schema: Schema):
        not_empty = tk.get_validator("not_empty")
        unicode_safe = tk.get_validator("unicode_safe")
        url_validator = tk.get_validator("url_validator")
        uuid_validator = tk.get_validator("uuid_validator")

        schema.update(
            {
                "ckanext.preflow.prefect_api_url": [
                    not_empty,
                    unicode_safe,
                    url_validator,
                ],
                "ckanext.preflow.prefect_api_key": [not_empty, unicode_safe],
                "ckanext.preflow.prefect_deployment_id": [
                    not_empty,
                    unicode_safe,
                    uuid_validator,
                ],
                "ckanext.preflow.supported_formats": [not_empty, unicode_safe],
            }
        )
        return schema

    # IResourceUrlChange
    def notify(self, resource: model.Resource):
        context = {
            "model": model,
            "ignore_auth": True,
        }
        resource_dict = tk.get_action("resource_show")(
            context,
            {
                "id": resource.id,
            },
        )
        self._submit_to_preflow(resource_dict)

    def after_resource_create(self, context, resource_dict: dict[str, Any]):
        self._submit_to_preflow(resource_dict)

    # IAuthFunctions
    def get_auth_functions(self) -> dict[str, AuthFunction]:
        return {
            "preflow_submit": auth.preflow_submit,
            "preflow_status": auth.preflow_status,
        }

    # IActions
    def get_actions(self) -> dict[str, Action]:
        return {
            "preflow_submit": action.preflow_submit,
            "preflow_status": action.preflow_status,
            "preflow_hook": action.preflow_hook,
            "preflow_status_update": action.preflow_status_update,
        }

    def get_blueprint(self):
        return preflow

    def _submit_to_preflow(self, resource_dict: dict[str, Any]) -> None:
        context = {"model": model, "ignore_auth": True, "defer_commit": True}
        resource_format = resource_dict.get("format")

        supported_formats = tk.config.get(
            "ckanext.preflow.supported_formats", DEFAULT_FORMATS
        )
        supported_formats = [
            fmt.strip().lower() for fmt in supported_formats.split(",") if fmt.strip()
        ]

        submit = resource_format and resource_format.lower() in supported_formats

        if not submit:
            return
        else:
            log.info(
                "Submitting resource %s to Prefect for processing",
                resource_dict.get("id"),
            )
            tk.get_action("preflow_submit")(
                context,
                resource_dict,
            )
