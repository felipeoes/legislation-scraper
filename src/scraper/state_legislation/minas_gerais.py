import requests
import re
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper

TYPES = {
    "Constituição Estadual": 2,
    "Decisão": 16,
    "Decreto": 4,
    "Decreto-Lei": 5,
    "Deliberação": 6,
    "Emenda Constitucional": 7,
    "Lei": 9,
    "Lei Complementar": 10,
    "Lei Constitucional": 11,
    "Lei Delegada": 12,
    "Ordem de Serviço": 13,
    "Portaria": 14,
    "Resolução": 15,
}

VALID_SITUATIONS = {
    "Não consta revogação expressa": 1
}  # Conama does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = {
    "Revogada": 2,
    "Inconstitucional": 3,
}  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS | INVALID_SITUATIONS


class MGAlmgScraper(BaseScaper):
    """Webscraper for Minas Gerais state legislation website (https://www.almg.gov.br)

    Example search request: https://www.almg.gov.br/atividade-parlamentar/leis/legislacao-mineira/?pagina=2&aba=pesquisa&q=&ano=1989&dataFim=&num=&grupo=4&ordem=0&pesquisou=true&dataInicio=&sit=1
    """

    def __init__(
        self,
        base_url: str = "https://www.almg.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "MINAS_GERAIS"
        self.params = {
            "pagina": "",
            "aba": "pesquisa",
            "q": "",
            "ano": "",
            "dataFim": "",
            "num": "",
            "grupo": "",
            "ordem": "0",
            "pesquisou": "true",
            "dataInicio": "",
            "sit": "",
        }
        self.reached_end_page = False
        self._initialize_saver()

    def _format_search_url(
        self, norm_type_id: str, situation_id: int, year: int, page: int
    ) -> str:
        """Format url for search request"""
        self.params["grupo"] = norm_type_id
        self.params["sit"] = situation_id
        self.params["ano"] = year
        self.params["pagina"] = page
        return f"{self.base_url}/atividade-parlamentar/leis/legislacao-mineira?{requests.compat.urlencode(self.params)}"

    def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'html_link'
        """
        soup = self._get_soup(url)

        docs = []

        items = soup.find_all("article")
        # check if the page is empty
        if len(items) == 0:
            self.reached_end_page = True
            return []

        for item in items:
            title = item.find("a").text.strip()
            html_link = item.find("a")["href"]
            summary = item.find("div").text.strip()
            docs.append({"title": title, "summary": summary, "html_link": html_link})

        return docs

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")
        url = requests.compat.urljoin(self.base_url, html_link)

        soup_data = self._get_soup(url)

        origin = soup_data.find("span", text="Origem").next_sibling.text.strip()
        publication = soup_data.find("span", text="Fonte")
        if publication:
            publication = publication.find_next("div").text.strip()

        tags = soup_data.find("span", text="Resumo")
        if tags:
            tags = tags.next_sibling.text.strip()

        subject = soup_data.find("span", text="Assunto Geral")
        if subject:
            subject = subject.next_sibling.text.strip()

        # get link for real html (first look for Text atualizado, if not found, look for Texto original)
        html_link = soup_data.find(lambda tag: tag.text.strip() == "Texto atualizado")
        if html_link:
            html_link = html_link.find("a")["href"]
        else:

            html_link = soup_data.find(lambda tag: tag.text.strip() == "Texto original")
            if html_link:
                html_link = html_link.find("a")["href"]

        html_link = requests.compat.urljoin(self.base_url, html_link)
        if (
            html_link == self.base_url
        ):  # norm is invalid because it does not have a link to the document text
            return None

        soup = self._get_soup(html_link)
        norm_text_tag = soup.find("span", class_="textNorma").prettify()

        # remove Data da última atualização: 14/09/2007 from text
        norm_text_tag = re.sub(
            r"Data da última atualização: \d{2}/\d{2}/\d{4}", "", norm_text_tag
        )

        if not norm_text_tag:  # some documents are not available, so we skip them
            return None

        html_string = f"<html><body>{norm_text_tag}</body></html>"

        buffer = BytesIO()
        buffer.write(html_string.encode())
        buffer.seek(0)

        text_markdown = self._get_markdown(stream=buffer)

        return {
            **doc_info,
            "origin": origin,
            "publication": publication,
            "tags": tags,
            "subject": subject,
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": html_link,
        }

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for situation, situation_id in tqdm(
            self.situations.items(),
            desc="MINAS GERAIS | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc=f"MINAS GERAIS | Year: {year} | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):

                # total pages info is not available, so we need to check if the page is empty. In order to make parallel calls, we will assume an initial number of pages and increase if needed. We will know that all the pages were scraped when we request a page and it shows a error message

                total_pages = 1  # just to start and avoid making a lot of requests for empty pages
                self.reached_end_page = False

                # Get documents html links
                documents = []
                while not self.reached_end_page:
                    start_page = 1

                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = [
                            executor.submit(
                                self._get_docs_links,
                                self._format_search_url(
                                    norm_type_id, situation_id, year, page
                                ),
                            )
                            for page in range(start_page, total_pages + 1)
                        ]

                        for future in tqdm(
                            as_completed(futures),
                            total=total_pages - start_page + 1,
                            desc="MINAS GERAIS | Get document link",
                            disable=not self.verbose,
                        ):
                            docs = future.result()
                            if docs:
                                documents.extend(docs)

                    start_page += total_pages
                    total_pages += self.max_workers

                # Get document data
                results = []
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, doc_info)
                        for doc_info in documents
                    ]
                    for future in tqdm(
                        as_completed(futures),
                        total=len(documents),
                        desc="MINAS GERAIS | Get document data",
                        disable=not self.verbose,
                    ):
                        result = future.result()
                        if result is None:
                            continue

                        # save to one drive
                        queue_item = {
                            "year": year,
                            "situation": situation,
                            "type": norm_type,
                            **result,
                        }

                        self.queue.put(queue_item)
                        results.append(queue_item)

                self.results.extend(results)
                self.count += len(results)

                if self.verbose:
                    print(
                        f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                    )
