# ckanext-preflow


A CKAN extension that integrates [Prefect](https://www.prefect.io/) data workflows with CKAN resources. This extension allows you to trigger, monitor, and display the status and logs of Prefect data ingestion flows directly from the CKAN interface.

## Features

- **Trigger Prefect Flows**: Automatically or manually submit CKAN resources for processing via Prefect.
- **Status & Logs**: View the current status and logs of Prefect flow runs associated with CKAN resources.


## Requirements

- CKAN 2.9 or later (not tested on earlier versions)
- Python 3.8+
- A running [Prefect](https://www.prefect.io/) server or Prefect Cloud

---

## Installation

1. **Activate your CKAN virtual environment**  
   ```bash
   . /usr/lib/ckan/default/bin/activate
   ```
2. **Clone and install the extension**
    ```bash
    git clone https://github.com//ckanext-preflow.git
    cd ckanext-preflow
    pip install -e .
    pip install -r requirements.txt
    ```
3. **Add preflow to your CKAN config**
    Open your CKAN configuration file (usually `ckan.ini`) and add the following line:
    ```ini
    ckan.plugins = preflow
    ```
4. **Configure Prefect settings**
    Add the following settings to your CKAN configuration file:
    ```ini
    ckanext.preflow.prefect_api_url = http://127.0.0.1:4200/api
    ckanext.preflow.prefect_api_key = <your-prefect-api-key>
    ckanext.preflow.prefect_deployment_id = <your-prefect-deployment-id>
    ckanext.preflow.supported_formats = csv,xls,xlsx,tsv,ssv,tab,ods,geojson,shp,qgis,zip
    ```
