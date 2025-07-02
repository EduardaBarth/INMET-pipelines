import datetime

from airflow.decorators import dag, task_group, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator

from inmet_pipeline_utils import NebulosidadePipeline

QUERY_SCHEMA = """CREATE SCHEMA IF NOT EXISTS apresentacao_airflow"""

QUERY_TABLE = """
    CREATE TABLE IF NOT EXISTS apresentacao_airflow.raw_nebulosidade (
        codigo              INTEGER,
        nome_da_estacao     TEXT,
        uf                  TEXT,
        janeiro             DOUBLE PRECISION,
        fevereiro           DOUBLE PRECISION,
        marco               DOUBLE PRECISION,
        abril               DOUBLE PRECISION,
        maio                DOUBLE PRECISION,
        junho               DOUBLE PRECISION,
        julho               DOUBLE PRECISION,
        agosto              DOUBLE PRECISION,
        setembro            TEXT,
        outubro             DOUBLE PRECISION,
        novembro            DOUBLE PRECISION,
        dezembro            DOUBLE PRECISION,
        ano                 TEXT
    );
"""

URL = "https://portal.inmet.gov.br/uploads/normais/Normal-Climatologica-NEB.xlsx"


@dag(
    dag_id="dag_nebulosidade",
    start_date=datetime.datetime(2024, 1, 1),
    schedule="30 5 1 6 ?",
    catchup=False,
    description="DAG para extração e carregamento dos dados de nebulosidade do inmet"
)
def incorporate_inmet_pipeline():
    nebulosidade_pipeline = NebulosidadePipeline()

    @task_group(group_id="init", tooltip="Initializing the Workflow")
    def init_workflow() -> None:
        EmptyOperator(task_id="start")

    @task_group(group_id="create_table", tooltip="Create table")
    def create_table() -> None:
        create_schema = PythonOperator(
            task_id='create_schema',
            python_callable=nebulosidade_pipeline.execute_quey,
            op_args=[QUERY_SCHEMA],
        )

        create_table = PythonOperator(
            task_id='create_table',
            python_callable=nebulosidade_pipeline.execute_quey,
            op_args=[QUERY_TABLE],
        )

        table_exists = ShortCircuitOperator(
            task_id="verify_table_exists",
            python_callable=nebulosidade_pipeline.verify_table_exists,
            op_args=["nebulosidade"],
        )

        create_schema >> create_table >> table_exists

    @task_group()
    def basic_etl() -> None:
        @task(
            retries=0,
            retry_delay=datetime.timedelta(seconds=30),
            doc_md="Baixa um arquivo do INMET com dados sobre a nebulosidade de algumas cidade de acordo com o mês e o ano e carrega no neon"
        )
        def nebulosidade_etl() -> None:
            nebulosidade_pipeline.nebulosidade_etl(URL)

        nebulosidade_etl()


    @task_group(group_id="end", tooltip="Finishing the Workflow")
    def end_workflow() -> None:
        EmptyOperator(task_id="finish")

    init_workflow() >> create_table() >> basic_etl() >> end_workflow()

incorporate_inmet_pipeline_dag = incorporate_inmet_pipeline()