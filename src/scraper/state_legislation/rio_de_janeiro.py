import requests
import re

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dotenv import load_dotenv
from src.scraper.base.scraper import BaseScaper

load_dotenv()

# http://alerjln1.alerj.rj.gov.br/contlei.nsf/DecretoAnoInt?OpenForm&Start=1&Count=300
# obs: LeiComp = Lei Complementar; LeiOrd = Lei Ordinária;
TYPES = ["Decreto", "Emenda", "LeiComp", "LeiOrd", "Resolucao"]

VALID_SITUATIONS = ["Sem revogação expressa"]

INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class RJAlerjScraper(BaseScaper):
    """Webscraper for Alesp (Assembleia Legislativa do Rio de Janeiro) website (https://www.alerj.rj.gov.br/)

    Example search request: http://alerjln1.alerj.rj.gov.br/contlei.nsf/DecretoAnoInt?OpenForm&Start=1&Count=300

    Observation: Only valid norms are published on the Alerj website (the invalid ones are archived and available only on another search engine that is not working currently), so we don't need to check for validity

    """

    def __init__(
        self,
        base_url: str = "http://alerjln1.alerj.rj.gov.br/contlei.nsf",
        **kwargs,
        # year_start: int = YEAR_START,
        # year_end: int = datetime.now().year,
        # docs_save_dir: str = Path(ONEDRIVE_STATE_LEGISLATION_SAVE_DIR)
        # / "RIO_DE_JANEIRO",
        # verbose: bool = False,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.params = {
            "OpenForm": "",
            "Start": 1,
            "Count": 500,
        }
        self.docs_save_dir = self.docs_save_dir / "RIO_DE_JANEIRO"
        self._initialize_saver()

    def _format_search_url(self, norm_type: str) -> str:
        """Format url for search request"""
        return f"{self.base_url}/{norm_type}AnoInt?OpenForm&Start={self.params['Start']}&Count={self.params['Count']}"

    def _get_docs_html_links(self, norm_type: str, soup: BeautifulSoup) -> list:
        """Get documents html links from soup object.
        Returns a list of dicts with keys 'title', 'date', 'author', 'summary' and 'html_link'
        """

        # get all tr's with 6 td's
        trs = soup.find_all("tr", valign="top")

        # get all html links
        html_links = []
        for tr in trs:
            tds = tr.find_all("td")
            if len(tds) == 6:
                title = f"{norm_type} {tds[1].text.strip()}"
                date = tds[2].text.strip()
                author = tds[3].text.strip()
                summary = tds[4].text.strip()
                url = tds[1].find("a")["href"]
                html_link = requests.compat.urljoin(self.base_url, url)
                html_links.append(
                    {
                        "title": title,
                        "date": date,
                        "author": author,
                        "summary": summary,
                        "html_link": html_link,
                    }
                )

        return html_links

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given html link"""
        doc_html_link = doc_info["html_link"]
        soup = self._get_soup(doc_html_link)

        # check if <font > some text [ Revogado ] some text</font> exists and skip if it does
        if soup.find("font", text=re.compile(r"\s*\[ Revogado \]\s*")):
            return None

        # get all html content in body until reach <a name="_Section2"></a>
        body = soup.body
        if body is None:
            return None

        # Decompose all descendants after <a name="_Section2"></a>
        descendants = [desc for desc in body.descendants]
        start = False
        for desc in descendants:
            if not desc:
                continue

            if desc.name == "a" and desc.get("name") == "_Section2":
                start = True

            if start and not desc.decomposed and hasattr(desc, "decompose"):
                desc.decompose()

        # Remove all <s> tags, which are not valid articles or paragraphs in the norm
        for s in body.find_all("s"):
            if not s:
                continue

            if not s.decomposed and hasattr(s, "decompose"):
                s.decompose()

        html_string = body.prettify(formatter="html")

        # get text markdown
        text_markdown = self._get_markdown(doc_html_link)

        return {
            **doc_info,
            "situation": "situation",
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": doc_html_link.strip().replace(
                "?OpenDocument", ""
            ),  # need to remove just for alerj
        }

    def _scrape_year(self, year: str):
        """Scrape data from given year"""
        # get data from all types
        for norm_type in tqdm(
            self.types, desc=f"RJ - ALERJ | {year} | Types", total=len(self.types)
        ):
            url = self._format_search_url(norm_type)
            soup = self._get_soup(url)

            # check if there are any results for the year
            #  <tr valign="top"><td><a name="1"></a><a href="/contlei.nsf/LeiOrdAnoInt?OpenForm&amp;Start=1&amp;Count=500&amp;Expand=1" target="_self"><img src="/icons/expand.gif" border="0" height="16" width="16" alt="Show details for 2024"></a></td><td><b><font size="1" face="Verdana">2024</font></b></td></tr>
            if soup.find("tr", valign="top") is None:
                continue

            # find img item with 'Show details for {year}' that is inside a item
            img_item = soup.find("img", alt=f"Show details for {year}")
            if img_item is None:
                continue

            year_item = img_item.find_parent("a")
            if year_item is None:
                continue

            year_url = year_item["href"]
            year_url = requests.compat.urljoin(url, year_url)
            soup = self._get_soup(year_url)

            # get all tr's with 6 td's
            documents_html_links = self._get_docs_html_links(norm_type, soup)

            # Get data from all  documents text links using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = []
                futures = [
                    executor.submit(self._get_doc_data, doc)
                    for doc in documents_html_links
                ]

                for future in tqdm(
                    as_completed(futures),
                    desc=f"RJ - ALERJ | Get document data",
                    total=len(documents_html_links),
                ):
                    result = future.result()

                    if result is None:
                        continue

                    # save to one drive
                    queue_item = {
                        "year": year,
                        # website only shows documents without any revocation
                        "situation": "Sem revogação expressa",
                        "type": norm_type,
                        **result,
                    }

                    self.queue.put(queue_item)
                    self.results.append(queue_item)

                self.results.extend(results)
                self.count += len(results)

                if self.verbose:
                    print(f"Scraped {len(results)} data for {norm_type}  in {year}")
