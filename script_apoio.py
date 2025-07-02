"""Esse script é somente para gerar o primeiro arquivo que será usado como base para descobrir se tem uma nova notícia"""

import pandas as pd
import requests
from bs4 import BeautifulSoup

if __name__ == '__main__':
    noticias = []
    base_url = "https://portal.inmet.gov.br"

    response = requests.get(f"{base_url}/noticias")
    html_page = BeautifulSoup(response.text, "html.parser")
    artigos = html_page.find_all("article")

    for artigo in artigos:
        artigo_dict = {}

        a_tag = artigo.find("a")
        h2_tag = artigo.find("h2")
        p_tag = artigo.find("p")

        if a_tag and h2_tag and p_tag:
            artigo_dict["url"] = base_url + a_tag["href"]
            artigo_dict["title"] = h2_tag.get_text(strip=True)
            artigo_dict["data"] = p_tag.get_text(strip=True)
            noticias.append(artigo_dict)

    # Excluí a última notícia publicada para forçar o envio do email.
    #del noticias[0]
    df_noticias = pd.DataFrame(noticias)
    df_noticias.to_json("fake_datalake/inmet_noticias.json")
