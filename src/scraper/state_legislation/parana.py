import re
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
import time
from src.scraper.base.scraper import (
    BaseScaper,
    YEAR_START,
    DEFAULT_VALID_SITUATION,
    DEFAULT_INVALID_SITUATION,
)

TYPES = {
    "Lei": 1,
    "Lei Complementar": 3,
    "Consituição Estadual": 10,
    "Decreto": 11,
    "Emenda Constitucional": 9,
    "Resolução": 13,
    "Portaria": 14,
}

VALID_SITUATIONS = (
    []
)  # Casa Civil for Parana does not have a situation field, invalid norms will be inferred from an indication in the document text (Revogado pelo | Revogada pela | Revogado por | Revogada por)

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS

lock = Lock()


class ParanaCVScraper(BaseScaper):
    """Webscraper for Parana do Sul state legislation website (https://www.legislacao.pr.gov.br)

    Example search request: https://www.legislacao.pr.gov.br/legislacao/pesquisarAto.do?action=listar&opt=tm&indice=1&site=1

    payload = {
        pesquisou: true
        opcaoAno: 2
        opcaoNro: 1
        optPesquisa: tm
        tiposAtoStr: 1
        site: 1
        codigoTipoAto:
        tipoOrdenacao:
        ordAsc: false
        optTexto: 2
        texto:
        anoInicialAto:
        anoFinalAto:
        nroInicialAto:
        nroFinalAto:
        tipoAto:
        nroAto:
        anoAto:
        tema: 0
        anoInicialAtoTema: 2020
        anoFinalAtoTema: 2020
        nroInicialAtoTema:
        nroFinalAtoTema:
        tiposAtoTema: 1
    }
    """

    def __init__(
        self,
        base_url: str = "https://www.legislacao.pr.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "PARANA"
        self.params = {
            "pesquisou": True,
            "opcaoAno": 2,
            "opcaoNro": 1,
            "optPesquisa": "tm",
            "tiposAtoStr": "",
            "site": 1,
            "codigoTipoAto": None,
            "tipoOrdenacao": None,
            "ordAsc": False,
            "optTexto": 2,
            "texto": None,
            "anoInicialAto": None,
            "anoFinalAto": None,
            "nroInicialAto": None,
            "nroFinalAto": None,
            "tipoAto": None,
            "nroAto": None,
            "anoAto": None,
            "tema": 0,
            "anoInicialAtoTema": "",
            "anoFinalAtoTema": "",
            "nroInicialAtoTema": None,
            "nroFinalAtoTema": None,
        }
        self._regex_list_items = re.compile(r"list_cor_(sim|nao)")
        self._regex_invalid_situations = re.compile(
            r"(Revogado pelo|Revogada pela|Revogado por|Revogada por)"
        )
        self._regex_total_pages = re.compile(r"Página \d+ de (\d+)")
        self._regex_total_records = re.compile(r"Total de (\d+) registros")
        self._initialize_saver()
        self._change_vpn_connection()

    def _format_search_url(
        self, norm_type_id: str, year_index: int, page: int = 1
    ) -> str:
        """Format url for search request"""
        self.params["tiposAtoStr"] = norm_type_id
        self.params["tiposAtoTema"] = norm_type_id
        self.params["anoInicialAtoTema"] = year_index
        self.params["anoFinalAtoTema"] = year_index

        return f"{self.base_url}/legislacao/pesquisarAto.do?action=listar&opt=tm&indice{page}&site=1"

    def _selenium_click_page(self, page: int):
        """Emulate click on page number with selenium, using javascript.

        The page url will be built using the total number of records and the page number in the following format:
        javascript:pesquisarPaginado('pesquisarAto.do?action=listar&opt=tm&indice={page}&totalRegistros={total_records}#resultado');
        """

        self._handle_blocked_access()

        # find the total number of records (totalRegistros=474#resultado)
        total_records = self._regex_total_records.search(self.driver.page_source)
        if total_records:
            total_records = int(total_records.group(1))
        else:
            print("Total records not found")
            return

        # create javascript to click on page
        js = f"javascript:pesquisarPaginado('pesquisarAto.do?action=listar&opt=tm&indice={page}&totalRegistros={total_records}#resultado');"

        self.driver.execute_script(js)
        time.sleep(5)

    def _is_access_blocked(self) -> bool:
        """Check if access is blocked by the website"""
        if "Acesso temporariamente bloqueado" in self.driver.page_source:
            print("Access temporarily blocked")
            return True

        return False

    def _handle_blocked_access(self):
        """Check if access is blocked and change vpn"""

        access_blocked = self._is_access_blocked()
        while access_blocked:
            print("Access blocked, changing VPN")
            self._change_vpn_connection()
            time.sleep(5)

            self.driver.refresh()
            access_blocked = self._is_access_blocked()

        # if "Acesso temporariamente bloqueado" in self.driver.page_source:
        #     print("Access temporarily blocked")

        #     # wait 5 seconds to see if page will finish loading
        #     time.sleep(5)

        # if "Acesso temporariamente bloqueado" in self.driver.page_source:
        #     self._change_vpn_connection()
        #     time.sleep(5)
        #     return True

        # return False

    def _selenium_fill_form(self, year: int, norm_type_id: int):
        """Fill the search form with the given year and norm type"""

        if "Your connection was interrupted" in self.driver.page_source:
            print("Connection interrupted")
            self.driver.refresh()
            time.sleep(3)

        # fill the year
        year_input = self.driver.find_element(By.ID, "anoInicialAtoTema")
        year_input.clear()
        year_input.send_keys(year)

        # check the checkbox for the norm type
        norm_type_checkbox = self.driver.find_elements(By.ID, "tiposAtoTema")
        norm_type_checkbox = [
            checkbox
            for checkbox in norm_type_checkbox
            if checkbox.get_attribute("value") == str(norm_type_id)
        ]
        if norm_type_checkbox:
            norm_type_checkbox[0].click()
            time.sleep(5)
        else:
            print(f"Norm type checkbox not found for {norm_type_id}")
            return

        # click on the search button
        search_button = self.driver.find_element(By.ID, "btPesquisar3")
        search_button.click()
        time.sleep(5)

        # wait until the page is loaded
        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        # <td class="msg_sucesso">Nenhum registro encontrado.</td>
        while not soup.find("table", id="list_tabela") and not soup.find(
            "td", class_="msg_sucesso", text="Nenhum registro encontrado."
        ):
            blocked_access = self._is_access_blocked()
            if blocked_access:
                return
            time.sleep(5)

    def _search_norms(self, url: str, year: int, norm_type_id: int) -> str:
        """Search for norms in the given year and norm type"""
        retries = 6
        page_loaded = False
        while not page_loaded and retries > 0:
            try:
                self.driver.get(url)
                page_loaded = True
            except Exception as e:
                print(f"Error getting url: {e}")
                time.sleep(5)
                retries -= 1

        if not page_loaded:
            # try changing vpn
            retries = 6
            self._change_vpn_connection()
            while not page_loaded and retries > 0:
                try:
                    self.driver.get(url)
                    page_loaded = True
                except Exception as e:
                    print(f"Error getting url: {e}")
                    time.sleep(5)
                    retries -= 1

        self._handle_blocked_access()
        # while blocked_access:
        #     try:  # may have a timeout problem with the driver.get because of the vpn change
        #         self.driver.get(url)
        #     except Exception as e:
        #         print(f"Error getting url: {e}")
        #         time.sleep(5)
        #         continue

        #     blocked_access = self._handle_blocked_access()

        self._selenium_fill_form(year, norm_type_id)

        time.sleep(
            5
        )  # need to have big wait times to avoid being blocked by the website
        return self.driver.page_source

    def _get_docs_links(
        self, url: str, year: int, norm_type_id: int, page: int
    ) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'id', 'title', 'summary', 'date', 'html_link'
        """
        with lock:
            if page > 1:
                while not f"indice={page}" in self.driver.current_url:
                    self._search_norms(url, year, norm_type_id)
                    self._selenium_click_page(page)

                    self._handle_blocked_access()
                    time.sleep(5)
            else:
                while not "#resultado" in self.driver.current_url:
                    self._search_norms(url, year, norm_type_id)

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

        docs = []

        table = soup.find("table", id="list_tabela")
        items = table.find_all("tr", class_=self._regex_list_items)

        for item in items:
            tds = item.find_all("td")

            id = tds[0].find("a", href=True)
            id = id["href"].split("'")[1]
            if not id:
                print("ID not found")

            title = tds[1].text.strip()
            summary = tds[2].text.strip()
            date = tds[3].text.strip()

            # html_link must be built from the id, in the following format:
            # https://www.legislacao.pr.gov.br/legislacao/pesquisarAto.do?action=exibir&codAto=234748

            html_link = f"/legislacao/pesquisarAto.do?action=exibir&codAto={id}"
            html_link = requests.compat.urljoin(self.base_url, html_link)

            docs.append(
                {
                    "id": id,
                    "title": title,
                    "summary": summary,
                    "date": date,
                    "html_link": html_link,
                }
            )

        return docs

    def _infer_invalid_situation(self, soup: BeautifulSoup) -> str:
        """Infer invalid situation from document text"""
        text = soup.get_text()
        match = self._regex_invalid_situations.search(text)
        if match:
            return DEFAULT_INVALID_SITUATION

        return None

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given doc info"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")

        with lock:
            soup = self._selenium_get_soup(html_link)
            self._handle_blocked_access()

            # check if access is blocked and change vpn
            # blocked_access = self._handle_blocked_access()
            # while blocked_access:
            #     try:
            #         self.driver.get(html_link)
            #     except Exception as e:
            #         print(f"Error getting url: {e}")
            #         time.sleep(5)
            #         continue

            #     blocked_access = self._handle_blocked_access()

            time.sleep(
                5
            )  # need to have big wait times to avoid being blocked by the website
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

        # norm text will be the form name="pesquisarAtoForm"
        norm_text_tag = soup.find("form", attrs={"name": "pesquisarAtoForm"})

        # remove table id="list_tabela" and "\n ANEXOS:" from the text
        table = norm_text_tag.find("table", id="list_tabela")
        if table:
            table.decompose()

        html_string = norm_text_tag.prettify().replace("\n ANEXOS:", "").strip()

        # inferr situation from text
        situation = self._infer_invalid_situation(soup)
        if not situation:
            situation = DEFAULT_VALID_SITUATION

        # since we're getting the form tag, need to add the html and body tags to make it a valid html for markitdown
        html_string = f"<html><body>{html_string}</body></html>"

        # get text markdown
        buffer = BytesIO()
        buffer.write(html_string.encode())
        buffer.seek(0)

        text_markdown = self._get_markdown(stream=buffer).strip()

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = html_link
        doc_info["situation"] = situation

        return doc_info

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for norm_type, norm_type_id in tqdm(
            self.types.items(),
            desc=f"PARANA | Year: {year} | Types",
            total=len(self.types),
            disable=not self.verbose,
        ):
            url = self._format_search_url(norm_type_id, year)
            time.sleep(
                5
            )  # need to have big wait times to avoid being blocked by the website
            page_html = self._search_norms(url, year, norm_type_id)
            soup = BeautifulSoup(page_html, "html.parser")

            # get total pages
            total_pages = self._regex_total_pages.search(soup.get_text())
            if total_pages:
                total_pages = int(total_pages.group(1))
            else:
                continue

            # Get documents html links
            documents = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(
                        self._get_docs_links,
                        url,
                        year,
                        norm_type_id,
                        page,
                    )
                    for page in range(1, total_pages + 1)
                ]

                for future in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc="PARANA | Get document link",
                    disable=not self.verbose,
                ):
                    docs = future.result()
                    if docs:
                        documents.extend(docs)

            # get all norms
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self._get_doc_data, doc_info)
                    for doc_info in documents
                ]

                for future in tqdm(
                    as_completed(futures),
                    desc="PARANA | Get document data",
                    total=len(futures),
                    disable=not self.verbose,
                ):
                    norm = future.result()
                    if not norm:
                        continue

                    # save to one drive
                    queue_item = {"year": year, "type": norm_type, **norm}

                    self.queue.put(queue_item)
                    results.append(queue_item)

            self.results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                )
