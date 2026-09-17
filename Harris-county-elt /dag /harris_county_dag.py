import airflow
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from jinja2 import Template
from harris_county_cad_elt.dag.settings import Settings

settings = Settings()


def get_jinja_template(file_path: str) -> Template:
    with open(f'{settings.queries_path}/{file_path}') as fp:
        return Template(fp.read())


with airflow.DAG(
    "harris_county_cad_elt",
    default_args=settings.dag_default_args,
    schedule_interval=None,
) as dag:

    # 1. Load raw HCAD files (CSV/text extracts) from GCS to BQ Raw Table
    load_hcad_raw_to_bq = GCSToBigQueryOperator(
        task_id='load_hcad_raw_to_bq',
        bucket=settings.variables['hcad_input_bucket'],
        source_objects=[settings.variables['hcad_source_object']],
        destination_project_dataset_table=(
            f'{settings.project_id}.{settings.dataset}.{settings.hcad_raw_table}'
        ),
        source_format='CSV',  # Updated for standard HCAD tabular file downloads
        skip_leading_rows=1,  # Set to 1 if CSV includes headers, or 0 if headerless
        field_delimiter='\t',  # HCAD files often use tab-delimiters (adjust as needed)
        compression='NONE',
        create_disposition=settings.variables['hcad_raw_create_disposition'],
        write_disposition=settings.variables['hcad_raw_write_disposition'],
        autodetect=True,
    )

    # 2. Render Jinja SQL template for data transformation/staging
    compute_and_insert_hcad_domain_query = get_jinja_template(
        'compute_and_insert_hcad_data.sql'
    ).render(
        project_id=settings.project_id,
        dataset=settings.dataset,
        hcad_table=settings.hcad_table,
        hcad_raw_table=settings.hcad_raw_table,
    )

    # 3. Transform and insert data into target domain/analytics table
    compute_and_insert_hcad_domain = BigQueryInsertJobOperator(
        task_id='compute_hcad_domain',
        configuration={
            "query": {
                "query": compute_and_insert_hcad_domain_query,
                "useLegacySql": False,
            }
        },
        location='US',  # Updated region to US (standard for Harris County / US datasets)
    )

    # Task dependency
    load_hcad_raw_to_bq >> compute_and_insert_hcad_domain
