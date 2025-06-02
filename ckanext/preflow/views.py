import json
from flask import Blueprint
import ckan.plugins.toolkit as tk
from flask.views import MethodView
import ckan.model as model
import ckan.logic as logic
from ckan.common import request

preflow = Blueprint("preflow", __name__)

class ResourceDataController(MethodView):
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

        return tk.h.redirect_to(
            controller="preflow", action="resource_data", id=id, resource_id=resource_id
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
            print(f"Preflow status fetched: {preflow_status.get('error')}")
        except logic.NotFound:
            preflow_status = {}
        except logic.NotAuthorized:
            tk.abort(403, tk._("Not authorized to see this page"))

        if preflow_status:
            preflow_status.update({"logs": json.loads(preflow_status.get("value", ""))})
        

        return tk.render(
            "resource_data.html",
            extra_vars={
                "status": preflow_status,
                "pkg_dict": pkg_dict,
                "resource": resource,
            },
        )

preflow.add_url_rule(
    "/dataset/<id>/resource_data/<resource_id>",
    view_func=ResourceDataController.as_view(str("resource_data")),
)