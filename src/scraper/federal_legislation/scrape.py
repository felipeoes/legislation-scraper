import requests
import time
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from multiprocessing import Queue
from src.database.saver import OneDriveSaver, ONEDRIVE_SAVE_DIR

SITUATIONS = ['Não%20consta%20revogação%20expressa', 'Não%20Informado',
              "Convertida%20em%20Lei", "Reeditada", "Reeditada%20com%20alteração"]
# OBS: empty string means all (Toda legislação). OPTIONS: 'Legislação+Interna' 'OR Legislação+Federal'
COVERAGE = ['']
TYPES = ['Decreto', 'Decreto+Legislativo', "Decreto%20Sem%20Número", 'Decreto-Lei', 'Emenda+Constitucional', 'Lei+Complementar', 'Lei+Ordin%C3%A1ria', 'Medida+Provis%C3%B3ria', 'Resolu%C3%A7%C3%A3o+da+C%C3%A2mara+dos+Deputados', 'Constitui%C3%A7%C3%A3o',
         'Lei', 'Lei+Sem+Número', 'Lei+Constitucional', 'Portaria', 'Portaria+Sem+Número', 'Regulamento', 'Resolu%C3%A7%C3%A3o+da+Assembl%C3%A9ia+Nacional+Constituinte', 'Resolu%C3%A7%C3%A3o+do+Congresso+Nacional', 'Resolu%C3%A7%C3%A3o+do+Senado+Federal']
ORDERING = "data%3AASC"
YEAR_START = 1808  # CHECK IF NECESSARY LATER


class CamaraDepScraper:
    """ Webscraper for Camara dos Deputados website (https://www.camara.leg.br/legislacao/)

    Example search request url: https://www.camara.leg.br/legislacao/busca?geral=&ano=&situacao=&abrangencia=&tipo=Decreto%2CDecreto+Legislativo%2CDecreto-Lei%2CEmenda+Constitucional%2CLei+Complementar%2CLei+Ordin%C3%A1ria%2CMedida+Provis%C3%B3ria%2CResolu%C3%A7%C3%A3o+da+C%C3%A2mara+dos+Deputados%2CConstitui%C3%A7%C3%A3o%2CLei%2CLei+Constitucional%2CPortaria%2CRegulamento%2CResolu%C3%A7%C3%A3o+da+Assembl%C3%A9ia+Nacional+Constituinte%2CResolu%C3%A7%C3%A3o+do+Congresso+Nacional%2CResolu%C3%A7%C3%A3o+do+Senado+Federal&origem=&numero=&ordenacao=data%3AASC
    """

    def __init__(self, base_url: str = "https://www.camara.leg.br/legislacao/",
                 situations: list = SITUATIONS, coverage: list = COVERAGE,
                 types: list = TYPES, ordering: str = ORDERING,
                 year_start: int = YEAR_START, year_end: int = datetime.now().year,
                 docs_save_dir: str = ONEDRIVE_SAVE_DIR,
                 verbose: bool = False):
        self.base_url = base_url
        self.situations = situations
        self.coverage = coverage
        self.types = types
        self.ordering = ordering
        self.year_start = year_start
        self.year_end = year_end
        self.verbose = verbose
        self.docs_save_dir = docs_save_dir
        self.years = [str(year) for year in range(
            self.year_start, self.year_end + 1)]
        self.params = {
            "abrangencia": "",
            "geral": "",
            "ano": "",
            "situacao": "",
            "origem": "",
            "numero": "",
            "ordenacao": ""
        }
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
        }
        self.queue = Queue()
        self.error_queue = Queue()
        self.saver = OneDriveSaver(
            self.queue, self.error_queue, self.docs_save_dir)
        self.results = []
        self.count = 0  # keep track of number of results
        self.soup = None

    def _format_search_url(self, year: str, situation: str, type: str) -> str:
        """ Format search url with given year """
        self.params["ano"] = year
        self.params["abrangencia"] = self.coverage[0]
        self.params["ordenacao"] = self.ordering
        self.params["situacao"] = situation
        self.params["tipo"] = type

        url = self.base_url + "busca?" + \
            "&".join([f"{key}={value}" for key, value in self.params.items()])

        return url

    def _get_soup(self, url: str) -> BeautifulSoup:
        """ Get BeautifulSoup object from given url """
        retries = 3
        for _ in range(retries):
            try:
                response = requests.get(url, headers=self.headers)

                # check  "O servidor encontrou um erro interno, ou está sobrecarregado" error
                if "O servidor encontrou um erro interno, ou está sobrecarregado" in response.text:
                    print("Server error, retrying...")
                    time.sleep(5)
                    continue

                break
            except Exception as e:
                print(f"Error getting response from url: {url}")
                print(e)
                time.sleep(5)

        return BeautifulSoup(response.text, "html.parser")
        # response = requests.get(url, headers=self.headers)
        # return BeautifulSoup(response.text, "html.parser")

    def _get_documents_html_links(self, url: str) -> "list[dict]":
        """ Get html links from given url. Returns a list of dictionaries in the format {
            "title": str,
            "summary": str,
            "html_link": str
        } """
        soup = self._get_soup(url)

        # Get all documents html links from page
        documents = soup.find_all("li", class_="busca-resultados__item")
        documents_html_links_info = []
        for document in documents:
            a_tag = document.find(
                "h3", class_="busca-resultados__cabecalho").find("a")
            document_html_link = a_tag["href"]
            title = a_tag.text.strip()
            summary = document.find(
                "p", class_="busca-resultados__descricao js-fade-read-more").text.strip()
            documents_html_links_info.append(
                {"title": title, "summary": summary, "html_link": document_html_link})

        return documents_html_links_info

    def _get_document_text_link(self, document_html_link: str, title: str, summary: str) -> dict:
        """ Get proper document text link from given document html link """

        soup = self._get_soup(document_html_link)
        document_text_links = soup.find("div", class_="sessao")
        if not document_text_links:
            # probably link doesn't exist (error in website)
            print(f"Could not find text link for document: {title}")
            error_data = {"title": title, "year": self.params["ano"], "situation": self.params["situacao"],
                          "type": self.params["tipo"], "summary": summary, "html_link": document_html_link}
            self.error_queue.put(error_data)
            return None

        document_text_links = document_text_links.find_all("a")
        document_text_link = None
        for link in document_text_links:
            if 'texto - publicação original' in link.text.strip().lower():
                url = link["href"]

                # get full url
                document_text_link = requests.compat.urljoin(
                    document_html_link, url)
                break

        if document_text_link is None:
            print(f"Could not find text link for document: {title}")
            return None

        return {"title": title, "summary": summary, "html_link": document_text_link}

    def _get_document_data(self, document_text_link: str, title: str, summary: str) -> dict:
        """ Get data from given document text link . Data will be in the format {
            "title": str,
            "summary": str,
            "html_string": str,
        }"""
        soup = self._get_soup(document_text_link)

        try:
            # get html string
            html_string = soup.find(
                "div", class_="textoNorma").prettify(formatter='html')
            return {"title": title, "summary": summary, "html_string": html_string, "document_url": document_text_link}
        except Exception as e:
            print(f"Error getting html string for document: {title}")
            print(e)
            error_data = {"title": title, "year": self.params["ano"], "situation": self.params["situacao"],
                          "type": self.params["tipo"], "summary": summary, "html_link": document_text_link}
            self.error_queue.put(error_data)
            return None

    def _scrape_year(self, year: str) -> list:
        """ Scrape data from given year """
        for situation in tqdm(self.situations, desc="CamaraDEP | Situations", total=len(self.situations)):
            results = []

            for type in self.types:
                url = self._format_search_url(year, situation, type)
                # Each page has 20 results, find the total and calculate the number of pages
                per_page = 20
                self.soup = self._get_soup(url)

                total = self.soup.find(
                    "div", class_="busca-info__resultado busca-info__resultado--informado").text
                total = int(total.strip().split()[-1])

                if total == 0:
                    if self.verbose:
                        print(
                            f"No results for Year: {year} | Situation: {situation} | Type: {type}")
                    continue
                pages = total // per_page + 1

                # Get documents html links from all pages using ThreadPoolExecutor
                with ThreadPoolExecutor() as executor:
                    documents_html_links_info = []
                    futures = [executor.submit(
                        self._get_documents_html_links, url + f"&pagina={page}") for page in range(1, pages + 1)]
                    for future in tqdm(as_completed(futures), desc="CamaraDEP |Pages",
                                       #    disable=not self.verbose,
                                       total=len(futures)):
                        documents_html_links_info.extend(future.result())

                # Get proper document text link from each document html link
                with ThreadPoolExecutor() as executor:
                    futures = []
                    documents_text_links = []
                    futures.extend([executor.submit(self._get_document_text_link, document_html_link.get("html_link"), document_html_link.get(
                        "title"), document_html_link.get("summary"))
                        for document_html_link in documents_html_links_info if document_html_link is not None])

                    for future in tqdm(as_completed(futures), desc="CamaraDEP | Text link", total=len(futures)):
                        documents_text_links.append(future.result())

                # Get data from all  documents text links using ThreadPoolExecutor
                with ThreadPoolExecutor() as executor:
                    results = []
                    futures = [executor.submit(self._get_document_data, document_text_link.get("html_link"), document_text_link.get(
                        "title"), document_text_link.get("summary"))
                        for document_text_link in documents_text_links if document_text_link is not None]

                    for future in tqdm(as_completed(futures), desc="CamaraDEP |Documents text", total=len(futures)):
                        result = future.result()

                        if result is None:
                            continue

                        # save to onedrive
                        queue_item = {
                            "year": year,
                            "situation": situation,
                            "type": type,
                            **result
                        }
                        self.queue.put(queue_item)
                        results.append(queue_item)

                self.results.extend(results)
                self.count += len(results)

            print(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {type} | Results: {len(results)} | Total: {self.count}")

    def scrape(self) -> list:
        """ Scrape data from all years """
        # start saver thread
        self.saver.start()
        
        # check if can resume from last scrapped year
        resume_from = YEAR_START  # 1808
        if self.saver.last_year is not None:
            resume_from = int(self.saver.last_year)

        # scrape data from all years
        for year in tqdm(self.years, desc="CamaraDEP | Years", total=len(self.years)):
            if int(year) < resume_from:
                continue
            
            self._scrape_year(year)

        # stop saver thread
        self.saver.stop()

        # wait for saver thread to finish
        self.saver.join()

        return self.results
