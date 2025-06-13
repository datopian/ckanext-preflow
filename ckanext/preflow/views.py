import json
from flask import Blueprint
import ckan.plugins.toolkit as tk
from flask.views import MethodView
import ckan.model as model
import ckan.logic as logic
from ckan.common import request

preflow = Blueprint("preflow", __name__)


class ResourcePipelineController(MethodView):
    def _prepare(self, id: str, resource_id: str):

        context = {
            "model": model,
            "session": model.Session,
            "user": tk.c.user,
            "auth_user_obj": tk.c.userobj,
        }
        return context

    def post(self, id: str, resource_id: str):
        context = self._prepare(id, resource_id)

        try:
            resource_dict = tk.get_action("resource_show")(
                context,
                {
                    "id": resource_id,
                },
            )

            tk.get_action("preflow_submit")(
                context,
                resource_dict,
            )

        except logic.ValidationError:
            pass
            tk.h.flash_error(
                tk._("There was an error submitting the resource for processing.")
            )

        return tk.h.redirect_to(
            controller="preflow",
            action="resource_pipeline",
            id=id,
            resource_id=resource_id,
        )

    def get(self, id: str, resource_id: str):
        context = self._prepare(id, resource_id)
        try:
            pkg_dict = tk.get_action("package_show")(context, {"id": id})
            resource = tk.get_action("resource_show")(context, {"id": resource_id})
        except (logic.NotFound, logic.NotAuthorized):
            tk.abort(404, tk._("Resource not found"))

        try:
            preflow_status = tk.get_action("preflow_status")(
                context, {"resource_id": resource_id}
            )
        except logic.NotFound:
            preflow_status = {}
        except logic.NotAuthorized:
            tk.abort(403, tk._("Not authorized to see this page"))

        if preflow_status:
            value = preflow_status.get("value", "")
            logs = None
            try:
                value_json = json.loads(value) if value else {}
                logs = value_json.get("logs") or value_json.get("pipeline") or None
            except Exception:
                logs = None
            preflow_status["logs"] = logs

            error_val = preflow_status.get("error")
            try:
                preflow_status["error"] = json.loads(error_val) if error_val else {}
            except Exception:
                preflow_status["error"] = {"message": str(error_val)}

        return tk.render(
            "resource_pipeline.html",
            extra_vars={
                "status": preflow_status,
                "pkg_dict": pkg_dict,
                "resource": resource,
            },
        )


class ValidationReportController(MethodView):
    def _prepare(self, id: str, resource_id: str):

        context = {
            "model": model,
            "session": model.Session,
            "user": tk.c.user,
            "auth_user_obj": tk.c.userobj,
        }
        return context

    def get(self, id: str, resource_id: str):
        context = self._prepare(id, resource_id)
        try:
            pkg_dict = tk.get_action("package_show")(context, {"id": id})
            resource = tk.get_action("resource_show")(context, {"id": resource_id})
        except (logic.NotFound, logic.NotAuthorized):
            tk.abort(404, tk._("Resource not found"))

        try:
            preflow_status = tk.get_action("preflow_status")(
                context, {"resource_id": resource_id}
            )
        except logic.NotFound:
            preflow_status = {}
        except logic.NotAuthorized:
            tk.abort(403, tk._("Not authorized to see this page"))

        try:
            error_dict = json.loads(preflow_status.get("error", "{}") or "{}")
            value_dict = json.loads(preflow_status.get("value", "{}") or "{}")
        except Exception:
            error_dict = {}
            value_dict = {}

        validation_report = {
            **error_dict.get("validation_report", {}),
            **value_dict.get("validation_report", {}),
        }


        return tk.render(
            "validation_report.html",
            extra_vars= {
                "validation_report": validation_report,
                "resource_id": resource_id,
                "pkg_dict": pkg_dict,
                "resource": resource,
            },
        )


preflow.add_url_rule(
    "/dataset/<id>/resource_pipeline/<resource_id>",
    view_func=ResourcePipelineController.as_view(str("resource_pipeline")),
)


preflow.add_url_rule(
    "/dataset/<id>/<resource_id>/validation_report",
    view_func=ValidationReportController.as_view(str("validation_report")),
)
