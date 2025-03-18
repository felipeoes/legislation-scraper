import time
import re
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper

lock = Lock()

TYPES = {
    "Lei": {
        "id": 1,
        "subtypes": {
            "Lei Ordinária": 2,
            "Lei Complementar": 3,
        },
    },
    "Emenda Constitucional": 5,
    "Decreto Legislativo": 6,
    "Resolução Legislativa": 7,
    "Resolução Administrativa": 8,
}

VALID_SITUATIONS = [
    "Não consta"
]  # Alema does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class MaranhaoAlemaScraper(BaseScaper):
    """Webscraper for Maranhao state legislation website (https://legislacao.al.ma.leg.br)

    Example search request: https://legislacao.al.ma.leg.br/ged/busca.html?dswid=1381

    payload: {
        javax.faces.partial.ajax: true
        javax.faces.source: table_resultados
        javax.faces.partial.execute: table_resultados
        javax.faces.partial.render: table_resultados
        javax.faces.behavior.event: page
        javax.faces.partial.event: page
        table_resultados_pagination: true
        table_resultados_first: 0
        table_resultados_rows: 10
        table_resultados_skipChildren: true
        table_resultados_encodeFeature: true
        j_idt44: j_idt44
        in_tipo_doc_focus:
        in_tipo_doc_input: 1
        j_idt53: 2
        in_nro_doc:
        in_ano_doc: 2020
        ementa:
        in_nro_proj_lei:
        in_ano_proj_lei:
        in_ini_public_input:
        in_fim_public_input:
        table_resultados_rowExpansionState:
        javax.faces.ViewState: -1509641436052460021:2441054440402057157
        javax.faces.ClientWindow: 1381
    }

    """

    def __init__(
        self,
        base_url: str = "https://legislacao.al.ma.leg.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "MARANHAO"
        self.params = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "table_resultados",
            "javax.faces.partial.execute": "table_resultados",
            "javax.faces.partial.render": "table_resultados",
            "javax.faces.behavior.event": "page",
            "javax.faces.partial.event": "page",
            "table_resultados_pagination": "true",
            "table_resultados_first": 0,
            "table_resultados_rows": 10,  # fixed number of results per page = 10
            "table_resultados_skipChildren": "true",
            "table_resultados_encodeFeature": "true",
            "j_idt44": "j_idt44",
            "in_tipo_doc_focus": "",
            "in_tipo_doc_input": 1,
            "j_idt53": "",  # subtype for Lei type (id = 1), for other types, this field is empty
            "in_nro_doc": "",
            "in_ano_doc": "",
            "ementa": "",
            "in_nro_proj_lei": "",
            "in_ano_proj_lei": "",
            "in_ini_public_input": "",
            "in_fim_public_input": "",
            "table_resultados_rowExpansionState": "",
            "javax.faces.ViewState": "-1509641436052460021:2441054440402057157",
            "javax.faces.ClientWindow": 1381,
        }
        self._initialize_saver()

    def _format_search_url(
        self, norm_type_id: str, year: int, page: int, subtype_id=""
    ) -> str:
        """Format url for search request"""
        self.params["in_tipo_doc_input"] = norm_type_id
        self.params["j_idt53"] = subtype_id
        self.params["in_ano_doc"] = year
        self.params["table_resultados_first"] = (
            page * self.params["table_resultados_rows"]
        )
        return f"{self.base_url}/ged/busca.html?dswid=1381"

    def _selenium_click_page(self, page: int):
        """Click on page number with selenium"""
        # <span class="ui-paginator-pages"><a class="ui-paginator-page ui-state-default ui-corner-all ui-state-active" aria-label="Page 1" tabindex="0" href="#">1</a><a class="ui-paginator-page ui-state-default ui-corner-all" aria-label="Page 2" tabindex="0" href="#">2</a><a class="ui-paginator-page ui-state-default ui-corner-all" aria-label="Page 3" tabindex="0" href="#">3</a><a class="ui-paginator-page ui-state-default ui-corner-all" aria-label="Page 4" tabindex="0" href="#">4</a><a class="ui-paginator-page ui-state-default ui-corner-all" aria-label="Page 5" tabindex="0" href="#">5</a><a class="ui-paginator-page ui-state-default ui-corner-all" aria-label="Page 6" tabindex="0" href="#">6</a><a class="ui-paginator-page ui-state-default ui-corner-all" aria-label="Page 7" tabindex="0" href="#">7</a><a class="ui-paginator-page ui-state-default ui-corner-all" aria-label="Page 8" tabindex="0" href="#">8</a><a class="ui-paginator-page ui-state-default ui-corner-all" aria-label="Page 9" tabindex="0" href="#">9</a><a class="ui-paginator-page ui-state-default ui-corner-all" aria-label="Page 10" tabindex="0" href="#">10</a></span>

        # check if page number is available to click
        current_visible_pages = self.driver.find_elements(
            By.CLASS_NAME, "ui-paginator-page"
        )
        current_visible_pages = [int(page.text) for page in current_visible_pages]
        
        # if no pages are visible it may have only one page. Just return and do nothing
        if len(current_visible_pages) == 0:
            return
            
        

        # click next page until the desired page is visible
        while page not in current_visible_pages:
            next_page = self.driver.find_element(By.CLASS_NAME, "ui-paginator-next")
            next_page.click()
            current_visible_pages = self.driver.find_elements(
                By.CLASS_NAME, "ui-paginator-page"
            )
            current_visible_pages = [int(page.text) for page in current_visible_pages]

        # click on the desired page
        page_element = self.driver.find_element(By.XPATH, f"//a[text()='{page}']")
        page_element.click()

        time.sleep(1)

    def _get_docs_links(self, page: int, norm_type: str) -> list:
        """Get documents links from given page.
        Returns a list of dicts with keys 'title', 'publication', 'project', 'summary', 'pdf_link'
        """

        # navigate to the page using selenium. Using lock to avoid error with multiple threads
        with lock:
            self._selenium_click_page(page)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

        docs = []

        items = soup.find_all("tr", class_="ui-widget-content")
        for item in items:
            title = item.find("label", class_="ui-outputlabel ui-widget").text
            publication = item.find_all("label", class_="ui-outputlabel ui-widget")[
                3
            ].text
            project = item.find_all("label", class_="ui-outputlabel ui-widget")[2].text
            summary = item.find("label", class_="ui-outputlabel ui-widget ementa").text
            pdf_link = item.find("a")["href"]
            docs.append(
                {
                    "title": f"{norm_type} - {title}",
                    "publication": publication,
                    "project": project,
                    "summary": summary,
                    "pdf_link": pdf_link,
                }
            )

        return docs

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""
        # remove pdf_link from doc_info
        pdf_link = doc_info.pop("pdf_link")

        text_markdown = self._get_markdown(pdf_link)
        if not text_markdown:
            print(f"Failed to get markdown for {pdf_link}")
            return None

        # check for error with url (The requested URL was not found on this server)
        if "the requested url was not found on this server" in text_markdown.lower():
            print(f"Invalid document: {pdf_link}")
            return None
            
        

        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = pdf_link
        return doc_info

    def _selenium_search_norms(
        self,
        norm_type: str,
        norm_type_id: str,
        year: int,
        page: int,
        subtype: str = None,
        subtype_id: str = None,
    ) -> BeautifulSoup:
        """Use selenium to search for norms for a specific year and type"""
        url = self._format_search_url(norm_type_id, year, page, subtype_id)
        self.driver.get(url)

        # change option via actionschains
        actions = ActionChains(self.driver)
        in_tipo_doc = self.driver.find_element(By.ID, "in_tipo_doc")
        actions.move_to_element(in_tipo_doc).click().perform()

        # go down to the desired option
        for type, _ in self.types.items():
            actions.send_keys(Keys.ARROW_DOWN)
            if type == norm_type:
                break

        actions.send_keys(Keys.ENTER)
        actions.perform()
        
        time.sleep(1)

        if subtype_id:
            # let only the subtype checkbox checked
            checkbox_trs = self.driver.find_element(By.ID, "j_idt53").find_elements(
                By.TAG_NAME, "tr"
            )

            for checkbox_tr in checkbox_trs:
                checkbox = checkbox_tr.find_element(By.TAG_NAME, "input")
                label = checkbox_tr.find_element(By.TAG_NAME, "label")
                if (
                    label.text == subtype
                    and not checkbox.get_attribute("checked") == "true"
                ):
                    label.click()

                elif (
                    label.text != subtype
                    and checkbox.get_attribute("checked") == "true"
                ):
                    label.click()

        in_ano_doc = self.driver.find_element(By.ID, "in_ano_doc")
        in_ano_doc.send_keys(year)

        # submit form
        submit_button = self.driver.find_element(By.ID, "j_idt71")
        submit_button.click()

        time.sleep(1)

        return BeautifulSoup(self.driver.page_source, "html.parser")

    def _scrape_norms(
        self,
        norm_type: str,
        norm_type_id: str,
        year: int,
        situation: str,
        subtype: str = None,
        subtype_id: str = None,
    ):
        """Scrape norms for a specific year, type and situation"""
        # url = self._format_search_url(norm_type_id, year, 0, subtype_id)

        soup = self._selenium_search_norms(
            norm_type, norm_type_id, year, 0, subtype, subtype_id
        )

        # # first make get request to get the view state
        # soup = self._selenium_get_soup(url)
        # viewstate = soup.find("input", {"name": "javax.faces.ViewState"})
        # viewstate_value = viewstate["value"]
        # self.params["javax.faces.ViewState"] = viewstate_value

        # response = self._selenium_post_request(url, self.params)

        # # response = self._make_request(url, method="POST", payload=self.params)
        # soup = BeautifulSoup(response.content, "html.parser")

        # <div class="ui-datatable-header ui-widget-header ui-corner-top">
        # 			            		Consulta de Documentos Eletrônicos - 200 registro(s) encontrado(s)
        # 			        		</div>

        # get total pages
        total_docs = soup.find(
            "div", class_="ui-datatable-header ui-widget-header ui-corner-top"
        )
        if not total_docs:  # no documents found for the given year, type and situation
            return

        total_docs_regex = re.search(
            r"(\d+) registro\(s\) encontrado\(s\)", total_docs.text
        )

        total_docs = int(total_docs_regex.group(1))

        # total_docs = int(total_docs.text.split(" ")[-3])
        total_pages = total_docs // self.params["table_resultados_rows"]
        if total_docs % self.params["table_resultados_rows"]:
            total_pages += 1

        # Get documents html links
        documents = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self._get_docs_links, page, norm_type if not subtype else subtype
                )
                for page in range(1, total_pages + 1)
            ]

            for future in tqdm(
                as_completed(futures),
                total=total_pages,
                desc="MARANHAO | Get document link",
                disable=not self.verbose,
            ):
                docs = future.result()
                if docs:
                    documents.extend(docs)

        # Get document data
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._get_doc_data, doc) for doc in documents]

            for future in tqdm(
                as_completed(futures),
                total=len(documents),
                desc="MARANHAO | Get document data",
                disable=not self.verbose,
            ):
                result = future.result()
                if result is None:
                    continue

                # save to one drive
                queue_item = {
                    "year": year,
                    # hardcode since it seems we only get valid documents in search request
                    "situation": situation,
                    "type": norm_type if not subtype else subtype,
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

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for situation in tqdm(
            self.situations,
            desc="MARANHAO | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc=f"MARANHAO | Year: {year} | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):
                if isinstance(norm_type_id, dict):
                    subtypes = norm_type_id["subtypes"]
                    norm_type_id = norm_type_id["id"]
                    for subtype, subtype_id in subtypes.items():
                        self._scrape_norms(
                            norm_type,
                            norm_type_id,
                            year,
                            situation,
                            subtype=subtype,
                            subtype_id=subtype_id,
                        )
                else:
                    self._scrape_norms(norm_type, norm_type_id, year, situation)
