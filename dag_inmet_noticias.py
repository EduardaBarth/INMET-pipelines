from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from airflow.operators.python import get_current_context
from airflow.utils.context import Context

from inmet_pipeline_utils import NebulosidadePipeline

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

BASE_URL = "https://portal.inmet.gov.br"
URL = BASE_URL + "/noticias"

EMAIL = "eduarda.b2004@aluno.ifsc.edu.br"


@dag(
    dag_id="dag_alertas_inmet",
    schedule="@daily",
    start_date=datetime(2025, 6, 28),
    catchup=False,
    default_args=DEFAULT_ARGS,
    description="Envia email quando há novos alertas do INMET"
)
def send_emails_pipeline():
    nebulosidade_pipeline = NebulosidadePipeline()

    @task(
        task_id="get_old_articles",
        execution_timeout=timedelta(minutes=1),
        doc_md="Carrega um DataFrame com as notícias antigas"
    )
    def get_old_articles() -> pd.DataFrame:
        return pd.read_json("fake_datalake/inmet_noticias.json")

    @task(
        task_id="check_new",
        execution_timeout=timedelta(minutes=2),
        doc_md="Verifica se tem novas notícias"
    )
    def check_new(df_old_articles: pd.DataFrame) -> pd.DataFrame:
        return nebulosidade_pipeline.check_if_has_news_articles(df_old_articles, URL, BASE_URL)

    @task(
        task_id="send_alert",
        doc_md="Envia um email quando há novas publicações do inmet no portal"
    )
    def send_alert(df_new_articles: pd.DataFrame):
        context: Context = get_current_context()

        if df_new_articles.empty:
            return

        msgs = "\n\n".join([f"{it['title']}: {it['url']}" for it in df_new_articles.to_dict(orient="records")])
        email = EmailOperator(
            task_id="send_email",
            to=EMAIL,
            subject=f"Novos alertas INMET ({len(df_new_articles)})",
            html_content=msgs
        )

        email.execute(context=context)

    @task(
        task_id="update_saved_articles",
        doc_md="Atualiza o arquivo com os antigos artigos"
    )
    def update_saved_articles(df_new_articles: pd.DataFrame, df_old_articles: pd.DataFrame) -> None:
        merged_df = df_new_articles.merge(df_old_articles, how='outer')
        merged_df.to_json("fake_datalake/inmet_noticias.json")

    df_old_articles = get_old_articles()
    df_new_articles = check_new(df_old_articles)
    send_alert(df_new_articles)
    update_saved_articles(df_new_articles, df_old_articles)

send_emails_pipeline_dag = send_emails_pipeline()
