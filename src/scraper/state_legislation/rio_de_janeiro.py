import requests
import time
import re

from os import environ
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from multiprocessing import Queue
from src.database.saver import OneDriveSaver
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# http://alerjln1.alerj.rj.gov.br/contlei.nsf/DecretoAnoInt?OpenForm&Start=1&Count=300
# obs: LeiComp = Lei Complementar; LeiOrd = Lei Ordinária;
TYPES = ['Decreto', 'Emenda', 'LeiComp', 'LeiOrd', 'Resolucao']
YEAR_START = 1808  # CHECK IF NECESSARY LATER

ONEDRIVE_STATE_LEGISLATION_SAVE_DIR = rf"{environ.get('ONEDRIVE_STATE_LEGISLATION_SAVE_DIR')}"


class RJAlerjScraper:
    """ Webscraper for Alesp (Assembleia Legislativa do Rio de Janeiro) website (https://www.alerj.rj.gov.br/) 

    Example search request: http://alerjln1.alerj.rj.gov.br/contlei.nsf/DecretoAnoInt?OpenForm&Start=1&Count=300

    """

    def __init__(self, base_url: str = "http://alerjln1.alerj.rj.gov.br/contlei.nsf", types: list = TYPES, year_start: int = YEAR_START, year_end: int = datetime.now().year, docs_save_dir: str = Path(ONEDRIVE_STATE_LEGISLATION_SAVE_DIR) / "RIO_DE_JANEIRO",  verbose: bool = False):
        self.base_url = base_url
        self.types = types
        self.year_start = year_start
        self.year_end = year_end
        self.docs_save_dir = docs_save_dir
        self.verbose = verbose
        self.years = list(range(self.year_start, self.year_end + 1))
        self.params = {
            'OpenForm': '',
            'Start': 1,
            'Count': 300
        }
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
                            AppleWebKit/537.36 (KHTML, like Gecko) \
                            Chrome/80.0.3987.149 Safari/537.36'
        }
        self.queue = Queue()
        self.error_queue = Queue()
        self.saver = OneDriveSaver(
            self.queue, self.error_queue, self.docs_save_dir)
        self.results = []
        self.count = 0  # keep track of number of results
        self.soup = None

    def _format_search_url(self, norm_type: str) -> str:
        """ Format url for search request """
        return f"{self.base_url}/{norm_type}AnoInt?OpenForm&Start={self.params['Start']}&Count={self.params['Count']}"

    def _get_soup(self, url: str) -> BeautifulSoup:
        """ Get soup object from url """
        retries = 3
        for _ in range(retries):
            try:
                response = requests.get(url, headers=self.headers)
                soup = BeautifulSoup(response.content, 'html.parser')
                return soup
            except Exception as e:
                print(f"Error {e} while getting soup for {url}. Retrying...")
                time.sleep(5)
                continue

    def _get_docs_html_links(self, norm_type: str, soup: BeautifulSoup) -> list:
        """ Get documents html links from soup object.
            Returns a list of dicts with keys 'title', 'date', 'author', 'summary' and 'html_link' """

        # <tr valign="top"><td></td><td><font size="1" face="Verdana"><a href="/contlei.nsf/b24a2da5a077847c032564f4005d4bf2/e1afb12df8833fc603258aa000691f66?OpenDocument">10277</a></font></td><td><font size="1" face="Verdana">10/01/2024</font></td><td><font size="1" face="Verdana">Poder Executivo</font></td><td><font size="1" face="Verdana">ESTIMA A RECEITA E FIXA A DESPESA DO ESTADO DO RIO DE JANEIRO PARA O EXERCÍCIO FINANCEIRO DE 2024</font></td><td><img src="/icons/ecblank.gif" border="0" height="16" width="1" alt=""></td></tr>

        # get all tr's with 6 td's
        trs = soup.find_all('tr', valign='top')

        # get all html links
        html_links = []
        for tr in trs:
            tds = tr.find_all('td')
            if len(tds) == 6:
                title = f"{norm_type} {tds[1].text.strip()}"
                date = tds[2].text.strip()
                author = tds[3].text.strip()
                summary = tds[4].text.strip()
                url = tds[1].find('a')['href']
                html_link = requests.compat.urljoin(self.base_url, url)
                html_links.append({
                    'title': title,
                    'date': date,
                    'author': author,
                    'summary': summary,
                    'html_link': html_link
                })

        return html_links

    def _get_doc_data(self, doc_info: dict) -> dict:
        """ Get document data from given html link """
        doc_html_link = doc_info['html_link']
        soup = self._get_soup(doc_html_link)
        
        # check if <font > some text [ Revogado ] some text</font> exists and skip if it does
        if soup.find('font', text=re.compile(r'\s*\[ Revogado \]\s*')):
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
            
            if desc.name == 'a' and desc.get('name') == '_Section2':
                start = True

            if start and not desc.decomposed and hasattr(desc, 'decompose'):
                desc.decompose()
                
                
        # Remove all <s> tags, which are not valid articles or paragraphs in the norm
        for s in body.find_all('s'):
            if not s:
                continue
            
            if not s.decomposed and hasattr(s, 'decompose'):
                s.decompose()
                
                

        html_string = body.prettify(formatter='html')

        return {
            **doc_info,
            'html_string': html_string,
            'document_url': doc_html_link.strip().replace('?OpenDocument', '') # need to remove just for alerj
        }

    def _scrape_year(self, year: str):
        """ Scrape data from given year """
        # get data from all types
        for norm_type in tqdm(self.types, desc=f"RJ - ALERJ | {year} | Types", total=len(self.types)):
            url = self._format_search_url(norm_type)
            soup = self._get_soup(url)

            # check if there are any results for the year
            #  <tr valign="top"><td><a name="1"></a><a href="/contlei.nsf/LeiOrdAnoInt?OpenForm&amp;Start=1&amp;Count=500&amp;Expand=1" target="_self"><img src="/icons/expand.gif" border="0" height="16" width="16" alt="Show details for 2024"></a></td><td><b><font size="1" face="Verdana">2024</font></b></td></tr>
            if soup.find('tr', valign='top') is None:
                continue

            # find img item with 'Show details for {year}' that is inside a item
            img_item = soup.find('img', alt=f'Show details for {year}')
            if img_item is None:
                continue

            year_item = img_item.find_parent('a')
            if year_item is None:
                continue

            year_url = year_item['href']
            year_url = requests.compat.urljoin(url, year_url)
            soup = self._get_soup(year_url)

            # get all tr's with 6 td's
            documents_html_links = self._get_docs_html_links(norm_type, soup)

            # Get data from all  documents text links using ThreadPoolExecutor
            with ThreadPoolExecutor() as executor:
                results = []
                futures = [executor.submit(self._get_doc_data, doc)
                           for doc in documents_html_links]

                for future in tqdm(as_completed(futures), desc=f"RJ - ALERJ | Get document data", total=len(documents_html_links)):
                    result = future.result()

                    if result is None:
                        continue

                    # save to one drive
                    queue_item = {
                        "year": year,
                        # website only shows documents without any revocation
                        "situation": "Sem revogação expressa",
                        "type": norm_type,
                        **result
                    }

                    self.queue.put(queue_item)
                    self.results.append(queue_item)

                self.results.extend(results)
                self.count += len(results)

                if self.verbose:
                    print(
                        f"Scraped {len(results)} data for {norm_type}  in {year}")

    def scrape(self) -> list:
        """ Scrape data from all years """
        # start saver thread
        self.saver.start()

        # check if can resume from last scrapped year
        resume_from = YEAR_START  # 1808
        if self.saver.last_year is not None:
            resume_from = int(self.saver.last_year)

        # scrape data from all years
        for year in tqdm(self.years, desc="RJ - ALERJ | Years", total=len(self.years)):
            if year < resume_from:
                continue

            self._scrape_year(year)

        # stop saver thread
        self.saver.stop()

        # wait for saver thread to finish
        self.saver.join()

        return self.results
