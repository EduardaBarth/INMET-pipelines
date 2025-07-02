from urllib.request import urlopen

import pandas as pd
import psycopg2
import requests
import unicodedata
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, inspect, text


class NebulosidadePipeline(object):
    def __init__(self):
        self.engine = create_engine("postgresql://testeAirflow_owner:npg_x0K4smeSbCDy@ep-sweet-smoke-a568j9dp-pooler.us-east-2.aws.neon.tech/testeAirflow?sslmode=require&channel_binding=require")

    def execute_quey(self, query: str) -> None:
        with self.engine.begin() as connection:
            connection.execute(text(query))

    def verify_table_exists(self, table_name: str) -> bool:
        inspetor = inspect(self.engine)
        return inspetor.has_table(table_name, schema="apresentacao_airflow")

    def nebulosidade_etl(self, url: str) -> None:
        local_file = self.download_file(url)
        df = pd.read_excel(local_file, header=2)

        self.load_into_neon(df)

    def load_into_neon(self, df):
        conn = psycopg2.connect(
            dbname="testeAirflow",
            user="testeAirflow_owner",
            password="npg_x0K4smeSbCDy",
            host="ep-sweet-smoke-a568j9dp-pooler.us-east-2.aws.neon.tech",
            port="5432",
            sslmode="require",
            channel_binding="require"
        )

        cursor = conn.cursor()

        self.pattern_collumns(df)

        cols = ",".join(df.columns)
        values_placeholders = ",".join(["%s"] * len(df.columns))
        insert_query = f"INSERT INTO apresentacao_airflow.raw_nebulosidade ({cols}) VALUES ({values_placeholders})"

        data = df.values.tolist()
        cursor.executemany(insert_query, data)

        conn.commit()
        cursor.close()
        conn.close()

    def pattern_collumns(self, df):
        remover_acentos = lambda s: ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
        df.columns = [
            remover_acentos(col.strip().lower().replace(" ", "_"))
            for col in df.columns
        ]

    @staticmethod
    def download_file(url: str) -> str:
        response = urlopen(url)
        filename_directory = "fake_datalake/raw_nebulosidade.xlsx"

        with open(filename_directory, "wb") as f:
            f.write(response.read())
        return filename_directory

    def get_articles(self, url: str, base_url: str) -> pd.DataFrame:
        articles = self.get_html_page(url)
        news = []

        for article in articles:
            article_dict = {}

            a_tag = article.find("a")
            h2_tag = article.find("h2")
            p_tag = article.find("p")

            if a_tag and h2_tag and p_tag:
                article_dict["url"] = base_url + a_tag["href"]
                article_dict["title"] = h2_tag.get_text(strip=True)
                article_dict["data"] = p_tag.get_text(strip=True)
                news.append(article_dict)

        return pd.DataFrame(news)

    @staticmethod
    def get_html_page(url: str) -> list:
        response = requests.get(url)
        html_page = BeautifulSoup(response.text, "html.parser")
        articles = html_page.find_all("article")
        return articles

    def check_if_has_news_articles(self, df_old_articles: pd.DataFrame, url: str, base_url: str) -> pd.DataFrame:
        df_articles_from_updated_page = self.get_articles(url, base_url)

        merged_df = df_articles_from_updated_page.merge(df_old_articles, how='outer', indicator=True)
        df_new_articles = merged_df.query('_merge == "left_only"')

        return df_new_articles.drop("_merge", axis=1)
