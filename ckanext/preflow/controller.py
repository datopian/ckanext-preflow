from flask import Blueprint, request, jsonify
from .prefect_flow import prefect_datapusher_flow

preflow_blueprint = Blueprint('preflow', __name__)

@preflow_blueprint.route('/api/3/action/prefect_trigger', methods=['POST'])
def prefect_trigger():
    data = request.get_json() or {}
    source_url = data.get('source_url')
    ckan_url = data.get('ckan_url')
    api_key = data.get('api_key')
    resource_id = data.get('resource_id')
    if not all([source_url, ckan_url, api_key, resource_id]):
        return jsonify({'success': False, 'error': 'Missing required parameters'}), 400
    # Run Prefect flow synchronously (for demo; production should use async or orchestration)
    result = prefect_datapusher_flow(source_url, ckan_url, api_key, resource_id)
    return jsonify({'success': True, 'result': result})
