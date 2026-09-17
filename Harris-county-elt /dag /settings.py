import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from airflow.models import Variable

# Fetch variables using the updated Harris County CAD key
_variables = Variable.get("harris_county_cad_elt", deserialize_json=True)
_feature_name = _variables["feature_name"]


@dataclass
class Settings:
    dag_folder: str = os.getenv("DAGS_FOLDER", "")
    dag_default_args: dict = None
    project_id: str = os.getenv("GCP_PROJECT", "")
    queries_path: str = ""
    dataset: str = ""
    hcad_raw_table: str = ""
    hcad_table: str = ""
    variables: dict = None

    def __post_init__(self):
        self.dag_default_args = {
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2026, 1, 1),
        }
        self.queries_path = os.path.join(
            self.dag_folder, _feature_name, 'dag', 'queries'
        )
        self.dataset = _variables["dataset"]
        self.hcad_raw_table = _variables["hcad_raw_table"]
        self.hcad_table = _variables["hcad_table"]
        self.variables = _variables
