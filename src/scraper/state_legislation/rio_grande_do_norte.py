import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper, YEAR_START

TYPES = {
    "Lei Ordinária": "lei ord",
    "Lei Complementar": "lei comp",
    "Emenda Constitucional": "emenda",
    "Constituição Estadual": "constituição",
}

VALID_SITUATIONS = [
    "Não consta"
]  # ALRN does not have a situation field, so we can not distinguish between valid and invalid norms

INVALID_SITUATIONS = []
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class RNAlrnScraper(BaseScaper):
    """Webscraper for Rio Grande do Norte state legislation website (https://www.al.rn.leg.br/legislacao/pesquisa)

    Example search request: https://www.al.rn.leg.br/legislacao/pesquisa?tipo=nome&nome=lei%20ord&page=4

    payload = {
        "tipo": "nome",
        "nome": "lei ord",
        "page": 4,
    }
    """

    def __init__(
        self,
        base_url: str = "https://www.al.rn.leg.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "RIO_GRANDE_DO_NORTE"
        self.params = {
            "tipo": "nome",
            "nome": "",
            "page": 1,
        }
        self._initialize_saver()

    def _format_search_url(self, norm_type_id: int, page: int) -> str:
        self.params["nome"] = norm_type_id
        self.params["page"] = page

        return f"{self.base_url}/legislacao/pesquisa?{requests.compat.urlencode(self.params)}"

    def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'html_link'
        """
        response = self._make_request(url)
        soup = BeautifulSoup(response.content, "html.parser")

        docs = []

        table = soup.find("table", class_="table table-sm table-striped")
        items = table.find_all("tr")

        if not items:
            print(f"Empty table for url: {url}")

        for item in items:
            tds = item.find_all("td")
            if len(tds) == 0:  # skip invalid rows, valid documents have at least 1 td
                continue

            th = item.find("th")

            title = th.text.strip()
            year = int(tds[0].text.strip())
            pdf_link = tds[1].find("a")
            pdf_link = pdf_link["href"]

            docs.append(
                {
                    "year": year,
                    "title": title,
                    "summary": "",  # do not have a field for summary
                    "pdf_link": pdf_link,
                }
            )

        return docs

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""
        # remove pdf_link from doc_info
        pdf_link = doc_info.pop("pdf_link")
        response = self._make_request(pdf_link)

        text_markdown = self._get_markdown(response=response)

        if not text_markdown or not text_markdown.strip():
            # probably image pdf
            text_markdown = self._get_pdf_image_markdown(response.content)

        if (
            not text_markdown or not text_markdown.strip()
        ):  # indeed an invalid or unavailable pdf
            return None

        doc_info["text_markdown"] = text_markdown.strip()
        doc_info["document_url"] = pdf_link

        return doc_info

    def _scrape_norms(self, norm_type: str, norm_type_id: str, situation: str):
        url = self._format_search_url(norm_type_id, 1)
        soup = self._get_soup(url)

        total_pages = soup.find("ul", class_="pagination")
        if not total_pages:  # must have only one page
            total_pages = 1
        else:
            total_pages = total_pages.find_all("li")[-2]
            total_pages = int(total_pages.find("a").text.strip())

        # Get documents html links
        documents = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self._get_docs_links,
                    self._format_search_url(norm_type_id, page),
                )
                for page in range(1, total_pages + 1)
            ]

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="RIO GRANDE DO NORTE | Get document link",
                disable=not self.verbose,
            ):
                docs = future.result()
                if docs:
                    documents.extend(docs)

        # Get document data
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._get_doc_data, doc_info) for doc_info in documents
            ]
            for future in tqdm(
                as_completed(futures),
                total=len(documents),
                desc="RIO GRANDE DO NORTE | Get document data",
                disable=not self.verbose,
            ):

                try:
                    result = future.result()

                    if result:
                        # save to one drive
                        queue_item = {
                            # hardcode since we only get valid documents in search request
                            "situation": situation,
                            "type": norm_type,
                            **result,
                        }

                        self.queue.put(queue_item)
                        results.append(queue_item)
                    else:
                        print("Invalid document returned from get_doc_data")
                except Exception as e:
                    print(f"Error getting document data: {e}")

        self.results.extend(results)
        self.count += len(results)

        if self.verbose:
            print(
                f"Finished scraping for Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
            )

    def scrape(self) -> list:
        """Scrape data from all years"""

        # start saver thread
        self.saver.start()

        # check if can resume from last scrapped year
        resume_from = self.year_start  # 1808
        forced_resume = self.year_start != YEAR_START
        if self.saver.last_year is not None and not forced_resume:
            print(f"Resuming from {self.saver.last_year}")
            resume_from = int(self.saver.last_year)
        else:
            print(f"Starting from {resume_from}")

        # scrape data
        for situation in tqdm(
            self.situations,
            desc="RIO GRANDE DO NORTE | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc="RIO GRANDE DO NORTE | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):
                self._scrape_norms(norm_type, norm_type_id, situation)

        # stop saver thread
        self.saver.stop()

        # wait for saver thread to finish
        self.saver.join()

        return self.results
