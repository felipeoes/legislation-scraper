import requests
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests.compat
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper


TYPES = {
    "Decreto Legislativo": 14,
    "Lei Complementar": 12,
    "Lei Ordinária": 13,
    "Resolução": 15,
    "Emenda Constitucional": 11,
}

VALID_SITUATIONS = [
    "Não consta"
]  # Alap does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class AmapaAlapScraper(BaseScaper):
    """Webscraper for Amapa state legislation website (https://al.ap.leg.br)

    Example search request: https://al.ap.leg.br/pagina.php?pg=buscar_legislacao&aba=legislacao&submenu=listar_legislacao&especie_documento=13&ano=2020&pesquisa=&n_doeB=&n_leiB=&data_inicial=&data_final=&orgaoB=&autor=&legislaturaB=&pagina=2
    """

    def __init__(
        self,
        base_url: str = "https://al.ap.leg.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "AMAPA"
        self.params = {
            "pg": "buscar_legislacao",
            "aba": "legislacao",
            "submenu": "listar_legislacao",
            "especie_documento": "",
            "ano": "",
            "pesquisa": "",
            "n_doeB": "",
            "n_leiB": "",
            "data_inicial": "",
            "data_final": "",
            "orgaoB": "",
            "autor": "",
            "legislaturaB": "",
        }
        self.reached_end_page = False
        self._initialize_saver()

    def _format_search_url(self, norm_type_id: str, year: int, page: int) -> str:
        """Format url for search request"""
        self.params["especie_documento"] = norm_type_id
        self.params["ano"] = year
        self.params["pagina"] = page

        return f"{self.base_url}/pagina.php?{'&'.join([f'{key}={value}' for key, value in self.params.items()])}"

    def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'doe_number', 'date',  'proposition_number', 'html_link'
        """
        soup = self._get_soup(url)

        docs = []
        items = soup.find("tbody").find_all("tr")

        # check if the page is empty (tbody is empty)
        if len(items) == 0:
            self.reached_end_page = True
            return []

        for item in items:
            tds = item.find_all("td")
            if len(tds) != 6:
                continue

            title = tds[0].text.strip()
            summary = tds[1].text.strip()
            doe_number = tds[2].text.strip()
            date = tds[3].text.strip()
            proposition_number = tds[4].text.strip()

            try:
                html_link = tds[5].find("a")["href"]
            except Exception as e:
                print(
                    f"Error getting html link: {e}"
                )  # some documents are not available, so we skip them
                continue

            docs.append(
                {
                    "title": title,
                    "summary": summary,
                    "doe_number": doe_number,
                    "date": date,
                    "proposition_number": proposition_number,
                    "html_link": html_link,
                }
            )

        return docs

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")
        url = requests.compat.urljoin(self.base_url, html_link)

        response = self._make_request(url)
        soup = BeautifulSoup(response.content, "html.parser")

        # remove header containing print link
        header = soup.find("a", class_="texto_noticia3")
        if header:
            header.decompose()

        # this website won't return the <html> tag, so we need to add it
        html_string = f"<html>{soup.prettify()}</html>"
        soup = BeautifulSoup(html_string, "html.parser")

        buffer = BytesIO()
        buffer.write(soup.html.encode())
        buffer.seek(0)

        text_markdown = self._get_markdown(stream=buffer)

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url

        return doc_info

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for situation in tqdm(
            self.situations,
            desc="AMAPA | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc=f"AMAPA | Year: {year} | Types",
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
                                self._format_search_url(norm_type_id, year, page),
                            )
                            for page in range(start_page, total_pages + 1)
                        ]

                        for future in tqdm(
                            as_completed(futures),
                            total=total_pages - start_page + 1,
                            desc="AMAPA | Get document link",
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
                            desc="AMAPA | Get document data",
                            disable=not self.verbose,
                        ):
                            result = future.result()
                            if result is None:
                                continue

                            # save to one drive
                            queue_item = {
                                "year": year,
                                # hardcode since we only get valid documents in search request
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
