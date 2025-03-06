import requests
import time

from os import environ
from datetime import datetime
from bs4 import BeautifulSoup
from markitdown import MarkItDown
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from multiprocessing import Queue
from src.database.saver import OneDriveSaver
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# We don't have situations for São Paulo, since the websitew only publishes valid documents (no invalid, no expired, no archived, no revoked, etc.)


VALID_SITUATIONS = {
    "Sem revogação expressa": 1,
}  # only norms with these situations (are actually valid norms)

INVALID_SITUATIONS = {
    "Declarada inconstitucional": 2,
    "Eficácia suspensa": 3,
    "Eficácia exaurida": 4,
    "Revogada": 5,
    "Anulada": 6,
}  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = {**VALID_SITUATIONS, **INVALID_SITUATIONS}

TYPES = {  # dict with norm type and its id
    "Decreto": 3,
    "Decreto Legislativo": 28,
    "Decreto-Lei": 25,
    "Decreto-Lei Complementar": 1,
    "Emenda Constitucional": 55,
    "Lei": 9,
    "Lei Complementar": 2,
    "Resolução": 14,
    "Resolução da Alesp": 19,
    "Decisão da Mesa": 12,
    "Ato da Mesa": 21,
    "Ato do Presidente": 22,
    "Decisão do Presidente": 23,
    "Constituição Estadual": 59,
}


YEAR_START = 1808  # CHECK IF NECESSARY LATER
ONEDRIVE_STATE_LEGISLATION_SAVE_DIR = (
    rf"{environ.get('ONEDRIVE_STATE_LEGISLATION_SAVE_DIR')}"
)


class SaoPauloAlespScraper:
    """Webscraper for Alesp (Assembleia Legislativa do Estado de São Paulo) website (https://www.al.sp.gov.br/)

    Example search request url: # https://www.al.sp.gov.br/norma/resultados?page=0&size=500&tipoPesquisa=E&buscaLivreEscape=&buscaLivreDecode=&_idsTipoNorma=1&idsTipoNorma=3&nuNorma=&ano=&complemento=&dtNormaInicio=&dtNormaFim=&idTipoSituacao=1&_idsTema=1&palavraChaveEscape=&palavraChaveDecode=&_idsAutorPropositura=1&_temQuestionamentos=on&_pesquisaAvancada=on
    """

    def __init__(
        self,
        base_url: str = "https://www.al.sp.gov.br/norma/resultados",
        types: dict = TYPES,
        situations: dict = SITUATIONS,
        year_start: int = YEAR_START,
        year_end: int = datetime.now().year,
        docs_save_dir: str = Path(ONEDRIVE_STATE_LEGISLATION_SAVE_DIR) / "SAO_PAULO",
        max_workers: int = 16,  # low max_workers bacause alesp website often returns server error
        verbose: bool = True,
    ):
        self.base_url = base_url
        self.types = types
        self.situations = situations
        self.year_start = year_start
        self.year_end = year_end
        self.verbose = verbose
        self.docs_save_dir = docs_save_dir
        self.max_workers = max_workers
        self.years = [year for year in range(self.year_start, self.year_end + 1)]
        self.params = {
            "size": 500,
            "tipoPesquisa": "E",
            "buscaLivreEscape": "",
            "buscaLivreDecode": "",
            "_idsTipoNorma": 1,
            "nuNorma": "",
            "ano": "",
            "complemento": "",
            "dtNormaInicio": "",
            "dtNormaFim": "",
            "idTipoSituacao": 1,  # only valid documents
            "_idsTema": 1,
            "palavraChaveEscape": "",
            "palavraChaveDecode": "",
            "_idsAutorPropositura": 1,
            "_temQuestionamentos": "on",
            "_pesquisaAvancada": "on",
        }
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
        }
        self.queue = Queue()
        self.error_queue = Queue()
        self.saver = OneDriveSaver(self.queue, self.error_queue, self.docs_save_dir)
        self.results = []
        self.count = 0  # keep track of number of results
        self.md = MarkItDown()
        self.soup = None

    def _format_search_url(
        self, year: str, norm_type_id: int, norm_situation_id
    ) -> str:
        """Format url for search request"""
        self.params["ano"] = year
        self.params["idsTipoNorma"] = norm_type_id
        self.params["idTipoSituacao"] = norm_situation_id
        return (
            self.base_url
            + "?"
            + "&".join([f"{key}={value}" for key, value in self.params.items()])
        )

    def _make_request(self, url: str) -> requests.Response:
        """Make request to given url"""
        retries = 5
        for _ in range(retries):
            try:
                response = requests.get(url, headers=self.headers)

                # check  "O servidor encontrou um erro interno, ou está sobrecarregado" error
                if (
                    "O servidor encontrou um erro interno, ou está sobrecarregado"
                    in response.text
                ):
                    print("Server error, retrying...")
                    time.sleep(5)
                    continue

                return response
            except Exception as e:
                print(f"Error getting response from url: {url}")
                print(e)
                time.sleep(5)

        return None

    def _get_soup(self, url: str) -> BeautifulSoup:
        """Get BeautifulSoup object from given url"""
        response = self._make_request(url)

        if response is None:
            return None

        return BeautifulSoup(response.text, "html.parser")

    def _get_markdown(self, url: str) -> str:
        """Get markdown response from given url"""
        response = self._make_request(url)
        return self.md.convert(response).text_content

    def _get_docs_html_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'html_link'"""
        soup = self._get_soup(url)

        # Get all documents html links from page
        trs = soup.find_all("tr")
        docs_html_links = []
        for tr in trs:
            tds = tr.find_all("td")
            if len(tds) == 2:
                if "Mostrando".lower() in tds[0].text.strip().lower():
                    continue
                title = tds[0].find("span").text
                summary = tds[1].find("span").text
                # first <a> tag which contains the html link for the html document
                url = tds[0].find("a", href=True)["href"]
                norm_link = tds[0].find("a", class_="link_norma", href=True)
                norm_link = requests.compat.urljoin(
                    self.base_url.replace("/norma/resultados", ""), norm_link["href"]
                )
                html_link = requests.compat.urljoin(
                    self.base_url.replace("/norma/resultados", ""), url
                )
                docs_html_links.append(
                    {
                        "title": title,
                        "summary": summary,
                        "html_link": html_link,
                        "norm_link": norm_link,
                    }
                )

        return docs_html_links

    def _get_norm_data(self, norm_link: str) -> dict:
        """Get norm data from given norm link"""
        soup = self._get_soup(norm_link)

        # get "promulgacao", "projeto", "temas", "palavras-chave" if they exist
        promulgacao = soup.find("label", text="Promulgação")
        if promulgacao:
            promulgacao = promulgacao.find_next("label").text
            if not promulgacao:
                promulgacao = ""

        projeto = soup.find("label", text="Projeto")
        if projeto:
            projeto = projeto.find_next("label").text
            if not projeto:
                projeto = ""

        temas = soup.find("label", text="Temas")
        if temas:
            temas = [
                button.text for button in temas.find_next("div").find_all("button")
            ]

        palavras_chave = soup.find("label", text="Palavras-chave")
        if palavras_chave:
            palavras_chave = [
                a.text for a in palavras_chave.find_next("div").find_all("a")
            ]

        return {
            "promulgation": promulgacao,
            "project": projeto,
            "themes": temas,
            "keywords": palavras_chave,
        }

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given html link"""
        doc_html_link = doc_info["html_link"]

        # get norm data
        norm_link = doc_info["norm_link"]
        norm_data = self._get_norm_data(norm_link)

        # check if pdf
        if doc_html_link.endswith(".pdf"):
            # pdf_content = requests.get(doc_html_link).content

            # # read pdf content
            # doc = fitz.open(stream=pdf_content, filetype="pdf")
            # pdf_text = ""
            # for page in doc:
            #     pdf_text += page.get_text()
            text_markdown = self._get_markdown(doc_html_link)

            return {
                "title": doc_info["title"],
                "summary": doc_info["summary"],
                "html_string": "",
                "text_markdown": text_markdown,
                "document_url": doc_html_link,
                **norm_data,
            }

        soup = self._get_soup(doc_html_link)

        # remove a tags with 'Assembleia Legislativa do Estado de São Paulo' and 'Ficha informativa'
        for a in soup.find_all("a"):
            if a.decomposed:
                continue

            a_text = a.text.lower()
            a_href = a.get("href", "").lower()
            if (
                "Assembleia Legislativa do Estado de São Paulo".lower() in a_text
                or "Ficha informativa".lower() in a_text
                or "http://www.al.sp.gov.br".lower() in a_href
                or "https://www.al.sp.gov.br".lower() in a_href
            ):
                a.decompose()

        # get data
        if soup.body:
            html_string = soup.body.prettify(formatter="html")
        else:
            html_string = soup.prettify(formatter="html")

        # get text markdown
        text_markdown = self._get_markdown(doc_html_link)

        return {
            "title": doc_info["title"],
            "summary": doc_info["summary"],
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": doc_html_link,
            **norm_data,
        }

    def _scrape_year(self, year: str):
        """Scrape norms for a specific year"""

        for situation, situation_id in tqdm(
            self.situations.items(),
            desc="ALESP | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):

            # get data from all types
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc="ALESP | Types",
                total=len(self.types),
                disable=True,
            ):
                url = self._format_search_url(year, norm_type_id, situation_id)
                soup = self._get_soup(url)

                # check if <div class="card cinza text-center">Nenhuma norma encontrada como os parâmetros informados</div> exists
                if (
                    "Nenhuma norma encontrada como os parâmetros informados".lower()
                    in soup.text.lower()
                ):
                    continue

                # get number of pages
                total = soup.find("span", text="página")
                if total is None:
                    total = soup.find("span", text="páginas")
                total = total.previous_sibling.previous_sibling.text
                total = int(total.strip().split()[-1])

                if total == 0:
                    if self.verbose:
                        print(
                            f"No results for {norm_type} in {year} with situation {situation}"
                        )

                    continue

                pages = total // self.params["size"] + 1

                # Get documents html links from all pages using ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    documents_html_links = []
                    futures = [
                        executor.submit(
                            self._get_docs_html_links,
                            url + f"&page={page}",
                        )
                        for page in range(pages)
                    ]
                    for future in tqdm(
                        as_completed(futures),
                        desc="ALESP | Get document link",
                        total=pages,
                    ):
                        documents_html_links.extend(future.result())

                # Get data from all  documents text links using ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    results = []

                    futures = [
                        executor.submit(self._get_doc_data, doc_html_link)
                        for doc_html_link in documents_html_links
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc="ALESP | Get document data",
                        total=len(documents_html_links),
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

    def scrape(self) -> list:
        """Scrape data from all years"""
        # start saver thread
        self.saver.start()

        # check if can resume from last scrapped year
        resume_from = YEAR_START  # 1808
        forced_resume = self.year_start > YEAR_START
        if self.saver.last_year is not None and not forced_resume:
            resume_from = int(self.saver.last_year)

        # # scrape data from all years
        for year in tqdm(self.years, desc="ALESP | Years", total=len(self.years)):
            if year < resume_from:
                continue

            self._scrape_year(year)

        # stop saver thread
        self.saver.stop()

        # wait for saver thread to finish
        self.saver.join()

        return self.results
