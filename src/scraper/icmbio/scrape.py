import requests

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper


TYPES = {
    "Instrução Normativa": "instrucao-normativa",
    "Portaria": "portaria",
    "Resolução": "resolucao",
    "Acórdão": "acordaos",
    "Moção": "mocao",
    "Termo de Referência": "termo-de-referencia",
}

# only getting the types above, because the other types are actually encountered in federal or state legislation and we already have scrapers for those

VALID_SITUATIONS = [
    "Não consta"
]  # Conama does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class ICMBioScraper(BaseScaper):
    """Webscraper for ICMBio (Instituto Chico Mendes de Conservação da Biodiversidade) website (https://www.icmbio.gov.br/cepsul/legislacao)

    Example search request: https://www.icmbio.gov.br/cepsul/legislacao/instrucao-normativa.html
    """

    def __init__(
        self,   
        base_url: str = "https://www.icmbio.gov.br/cepsul/legislacao",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "ICMBIO"
        self.cached_years_links = {}
        self._initialize_saver()

    def _format_search_url(self, norm_type_id: str) -> str:
        """Format url for search request"""
        return f"{self.base_url}/{norm_type_id}.html"

    def _get_years_links(self, soup: BeautifulSoup, norm_type_id: str) -> list:
        """Get years links from soup object"""
        # get total pages
        pagination = soup.find("div", class_="pagination")

        # if no pagination, only have one page
        if pagination is None:
            total_pages = 1
        else:
            total_pages = int(pagination.find("p").text.split()[-1])

        # get all years links, each page will have at most 10 years
        # https://www.icmbio.gov.br/cepsul/legislacao/instrucao-normativa.html?start=10
        years_links = {}  # will be a dict with keys 'year' and 'html_link'
        for page in range(total_pages):
            url = f"{self.base_url}/{norm_type_id}.html?start={page*10}"

            soup = self._selenium_get_soup(url)
            trs = soup.find_all("tr")
            for tr in trs:
                a = tr.find("a")
                year = int(a.text.strip())
                html_link = a["href"]
                years_links[year] = html_link

        return years_links

    def _get_docs_links(self, soup) -> list:
        """Get documents html links from soup object.
        Returns a list of dicts with keys 'title', 'year', 'summary' and 'html_link'
        """

        trs = soup.find_all("tr")
        docs = []
        for tr in trs:
            tds = tr.find_all("td")
            if len(tds) != 5:
                continue
            
            # if all tds are empty, continue
            if all([td.text.strip() == "" for td in tds]):
                continue
            
            try: # some documents don't have link to pdf, in this case we will skip them
                title = tds[0].find("a").text.strip()
                date = tds[1].text.strip()
                uf = tds[2].text.strip()
                summary = tds[3].text.strip()
                situation = tds[4].text.strip()
                html_link = tds[0].find("a")["href"]
                docs.append(
                    {
                        "title": title,
                        "date": date,
                        "uf": uf,
                        "summary": summary,
                        "status": situation,
                        "html_link": html_link,
                    }
                )
            except Exception as e:
                print(f"Error getting document data: {e}")
                continue

        return docs

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from norm dict. Download url for pdf will follow the pattern: https://www.icmbio.gov.br/portal/legislacao/{html_link}"""

        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")

        url = requests.compat.urljoin(self.base_url, html_link)
        text_markdown = self._get_markdown(url)

        if text_markdown is None:
            return None

        return {**doc_info, "text_markdown": text_markdown, "document_url": url}

    def _scrape_year(self, year: str):
        """Scrape norms for a specific year"""
        for situation in tqdm(
            self.situations,
            desc="ICMBIO | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc=f"ICMBIO | Year: {year} | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):

                if not self.cached_years_links.get(norm_type_id):
                    url = self._format_search_url(norm_type_id)
                    soup = self._selenium_get_soup(url)

                    years_links = self._get_years_links(soup, norm_type_id)
                    self.cached_years_links[norm_type_id] = years_links
                else:
                    years_links = self.cached_years_links[norm_type_id]

                # if current year is not in years_links, continue
                year_link = years_links.get(year)
                if year_link is None:
                    continue

                url = requests.compat.urljoin(self.base_url, year_link)
                soup = self._selenium_get_soup(url)

                # just one request since all docs for a year are in the same page
                norms = self._get_docs_links(soup)

                # get all norms data
                results = []

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, norm) for norm in norms
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc="ICMBIO | Get document data",
                        total=len(norms),
                        disable=not self.verbose,
                    ):

                        result = future.result()
                        if result is None:
                            continue

                        # save to one drive
                        queue_item = {
                            "year": year,
                            "type": norm_type,
                            "situation": situation,
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
