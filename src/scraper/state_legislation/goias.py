import requests
import fitz

from io import BytesIO
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper

TYPES = {
    "Constituição Estadual": 12,
    "Emenda Constitucional": 13,
    "Lei Complementar": 1,
    "Lei Ordinária": 2,
    "Lei Delegada": 4,
    "Decreto Lei": 8,
    "Decreto Numerado": 3,
    "Decreto Orçamentário": 5,
    "Portaria Orçaentária": 6,
    "Resolução": 7,
}

# situations are gotten from doc data while scraping
VALID_SITUATIONS = []
INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS

lock = Lock()


class LegislaGoias(BaseScaper):
    """Webscraper for Espirito Santo state legislation website (https://legisla.casacivil.go.gov.br)

    Example search request: https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes?ano=1798&ordenarPor=data&page=1&qtd_por_pagina=10&tipo_legislacao=7
    """

    def __init__(
        self,
        base_url: str = "https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.params = {
            "ano": 1800,
            "ordenarPor": "data",
            "qtd_por_pagina": 100,
            "tipo_legislacao": "",
            "page": 1,
        }
        self.docs_save_dir = self.docs_save_dir / "GOIAS"
        self._initialize_saver()

    def _format_search_url(self, norm_type_id: str, year: int, page: int = 1) -> str:
        self.params["ano"] = year
        self.params["tipo_legislacao"] = norm_type_id
        self.params["page"] = page

        return f"{self.base_url}?{requests.compat.urlencode(self.params)}"

    def _get_doc_info(self, doc: dict) -> dict:
        """Get document info from given doc data"""
        doc_info = {
            "id": doc["id"],
            "norm_number": doc["numero"],
            "situation": doc["estado_legislacao"]["nome"],
            "date": doc["data_legislacao"],
            "title": f'{doc["tipo_legislacao"]} {doc["numero"]} de {doc["ano"]}',
            "summary": doc["ementa"],
        }

        # html link will be in the format https://legisla.casacivil.go.gov.br/pesquisa_legislacao/{doc_id}/lei-{doc_number}

        html_link = f'https://legisla.casacivil.go.gov.br/pesquisa_legislacao/{doc["id"]}/lei-{doc["numero"]}'
        # using lock to avoid issues with using selenium in multiple threads and mixing up the results
        with lock:
            soup = self._selenium_get_soup(html_link)

        #  class="folha"
        norm_text_tag = soup.find("div", class_="folha")
        if norm_text_tag:
            html_string = norm_text_tag.prettify()
            doc_info["html_string"] = html_string
            doc_info["document_url"] = html_link
        else:
            # check for <a href="https://legisla.casacivil.go.gov.br/api/v1/arquivos/8095" target="_blank"><img alt="" border="0" src="/assets/ver_lei.jpg"></a> and download pdf
            pdf_link = soup.find("a", href=True)
            if pdf_link:
                pdf_link = pdf_link["href"]

                # since there is no html, the pdf must be an image
                pdf_content = self._make_request(pdf_link).content
                text_markdown = self._get_pdf_image_markdown(pdf_content)

                doc_info["text_markdown"] = text_markdown
                doc_info["document_url"] = pdf_link

                return doc_info

        # pdf link will be in the format https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes/{doc_id}/pdf
        pdf_link = f'{self.base_url}/{doc_info["id"]}/pdf'
        text_markdown = self._get_markdown(pdf_link)

        doc_info["text_markdown"] = text_markdown
        if not doc_info.get("document_url"):
            doc_info["document_url"] = pdf_link
        else:
            doc_info["pdf_link"] = pdf_link

        return doc_info

    def _get_doc_data(self, url: str) -> list:
        """Get document data from given url"""
        response = self._make_request(url).json()

        total_results = response["total_resultados"]
        if total_results == 0:
            return []

        data = response["resultados"]
        docs = []

        # concurrent processing

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._get_doc_info, doc) for doc in data]

            for future in tqdm(
                as_completed(futures),
                desc="GOIAS | Get document info",
                total=len(futures),
                disable=not self.verbose,
            ):
                doc_info = future.result()
                docs.append(doc_info)

        return docs

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for norm_type, norm_type_id in tqdm(
            self.types.items(),
            desc=f"GOIAS | Year: {year} | Types",
            total=len(self.types),
            disable=not self.verbose,
        ):

            url = self._format_search_url(norm_type_id, year, 0)
            response = self._make_request(url)

            data = response.json()
            total_results = data["total_resultados"]

            if total_results == 0:
                continue

            pages = total_results // 100 + 1

            # get all norms
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(
                        self._get_doc_data,
                        self._format_search_url(norm_type_id, year, page),
                    )
                    for page in range(1, pages + 1)
                ]

                for future in tqdm(
                    as_completed(futures),
                    desc="GOIAS | Get document data",
                    total=len(futures),
                    disable=not self.verbose,
                ):

                    try:
                        norms = future.result()
                        if not norms:
                            continue

                        for norm in norms:
                            # save to one drive
                            queue_item = {
                                "year": year,
                                "type": norm_type,
                                **norm,
                            }

                            self.queue.put(queue_item)
                            results.append(queue_item)

                    except Exception as e:
                        print(f"Error getting document data | Error: {e}")

            self.results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                )
