import requests
import re
import base64
import urllib.parse

from io import BytesIO
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dotenv import load_dotenv
from src.scraper.base.scraper import BaseScaper

load_dotenv()

# obs: LeiComp = Lei Complementar; LeiOrd = Lei Ordinária;
TYPES = ["Constituição Estadual", "Decreto", "Emenda", "LeiComp", "LeiOrd", "Resolucao"]

# situations will be inferred from the text of the norm
VALID_SITUATIONS = []
INVALID_SITUATIONS = []
# norms with these situations are invalid norms (no longer have legal effect)

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
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.params = {
            "OpenForm": "",
            "Start": 1,
            "Count": 500,
        }
        self.docs_save_dir = self.docs_save_dir / "RIO_DE_JANEIRO"
        self.fetched_constitution = False
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

        # check if <font > some text [ Revogado ] some text</font> exists
        if soup.find("font", text=re.compile(r"\s*\[ Revogado \]\s*")):
            situation = "Revogada"
        else:
            situation = "Sem revogação expressa"

        html_string = body.prettify()

        # get text markdown
        text_markdown = self._get_markdown(doc_html_link)

        return {
            **doc_info,
            "situation": situation,
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": doc_html_link.strip().replace(
                "?OpenDocument", ""
            ),  # need to remove just for alerj
        }

    def scrape_constitution(self):
        """Scrape constitution data"""
        # url already base64 encoded that will return all constitution sections. Don't need to click "next" button to gather all sections
        url = "http://www3.alerj.rj.gov.br/lotus_notes/default.asp?id=73&url=L2NvbnN0ZXN0Lm5zZi9JbmRpY2VJbnQ/T3BlbkZvcm0mU3RhcnQ9MSZDb3VudD0zMDA="
        soup = self._get_soup(url)

        # get all a tags with data-role (and any text), they will contain the links
        a_links = soup.find_all("a", attrs={"data-role": True})

        # remove the ones that have "Indice" or "OpenNavigator" in data-role; also remove the link who has the text "Emendas Constitucionais", since we're already scraping it later
        a_links = [
            a
            for a in a_links
            if "Indice" not in a["data-role"]
            and "OpenNavigator" not in a["data-role"]
            and "EMENDAS CONSTITUCIONAIS" not in a.text.strip().upper()
        ]

        html_string = ""

        for index, a_link in tqdm(
            enumerate(a_links), desc="RJ - ALERJ | Constitution", total=len(a_links)
        ):
            original_path = a_link["data-role"]
            path_bytes = original_path.encode("utf-8")
            encoded_path_bytes = base64.b64encode(path_bytes)
            encoded_path_str = encoded_path_bytes.decode("ascii")

            query_params = {"id": 73, "url": encoded_path_str}
            section_url = f"http://www3.alerj.rj.gov.br/lotus_notes/default.asp?{urllib.parse.urlencode(query_params)}"
            section_soup = self._get_soup(section_url)

            # Clean the fetched content
            for div_to_remove in section_soup.find_all(
                "div", class_="alert alert-warning"
            ):
                div_to_remove.decompose()
            for div_to_remove in section_soup.find_all("div", id="barraBotoes"):
                div_to_remove.decompose()

            # remove tags containing "Texto do Título", "Texto do Capítulo", "Ttexto da Seção"
            for tag_to_remove in section_soup.find_all(
                text=re.compile(r"Texto do Título|Texto do Capítulo|Texto da Seção")
            ):
                parent = tag_to_remove.parent
                if parent and not parent.decomposed:
                    parent.decompose()

            # remove images
            for img_to_remove in section_soup.find_all("img"):
                img_to_remove.decompose()

            content_div = section_soup.find("div", id="divConteudo")

            # Get the inner HTML content, prettify adds formatting
            html_content = content_div.prettify()
            html_content = html_content.replace("\n", "")

            if index != len(a_links) - 1:
                html_content = html_content + "<hr/>"  # separator for chapters

            html_string += html_content

        # Convert HTML to Markdown
        # add <html><body> tags to the html string to avoid error with markdown conversion
        html_string = "<html><body>" + html_string + "</body></html>"

        buffer = BytesIO()
        buffer.write(html_string.encode())
        buffer.seek(0)
        text_markdown = self._get_markdown(stream=buffer).strip()

        # save to one drive
        queue_item = {
            "year": 1989,
            "type": "Constituição Estadual",
            "title": "Constituição Estadual do Rio de Janeiro",
            "date": "05/10/1989",
            "author": "",
            "summary": "",
            "html_link": url,
            "html_string": html_string,
            "text_markdown": text_markdown,
            "situation": "Sem revogação expressa",
            "document_url": url,
        }

        self.queue.put(queue_item)
        self.results.append(queue_item)
        self.count += 1

        self.fetched_constitution = True

    def _scrape_year(self, year: str):
        """Scrape data from given year"""
        # get data from all types
        for norm_type in tqdm(
            self.types, desc=f"RJ - ALERJ | {year} | Types", total=len(self.types)
        ):

            if not self.fetched_constitution and norm_type == "Constituição Estadual":
                self.scrape_constitution()
                continue

            url = self._format_search_url(norm_type)
            soup = self._get_soup(url)

            # check if there are any results for the year
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

            results = []
            # Get data from all  documents text links using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
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
                        "type": norm_type,
                        **result,
                    }

                    self.queue.put(queue_item)
                    self.results.append(queue_item)

                self.results.extend(results)
                self.count += len(results)

                if self.verbose:
                    print(f"Scraped {len(results)} data for {norm_type}  in {year}")
