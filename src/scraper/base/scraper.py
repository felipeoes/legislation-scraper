import requests
import time

from openai import OpenAI
from io import BytesIO
from os import environ
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from markitdown import MarkItDown, UnsupportedFormatException, FileConversionException
from tqdm import tqdm
from multiprocessing import Queue
from pathlib import Path
from dotenv import load_dotenv
from src.database.saver import OneDriveSaver

load_dotenv()


YEAR_START = 1808  # CHECK IF NECESSARY LATER

ONEDRIVE_STATE_LEGISLATION_SAVE_DIR = (
    rf"{environ.get('ONEDRIVE_STATE_LEGISLATION_SAVE_DIR')}"
)


class BaseScaper:
    """Base class for state legislation scrapers"""

    def __init__(
        self,
        base_url: str,
        types: list,
        situations: str,
        year_start: int = YEAR_START,
        year_end: int = datetime.now().year,
        docs_save_dir: Path = Path(ONEDRIVE_STATE_LEGISLATION_SAVE_DIR),
        llm_client: OpenAI = None,
        llm_model: str = None,
        llm_prompt: str = "Extraia todo  o conteúdo da imagem. Retorne somente o conteúdo extraído",
        use_selenium: bool = False,
        max_workers: int = 16,
        verbose: bool = False,
    ):
        self.base_url = base_url
        self.types = types
        self.situations = situations
        self.year_start = year_start
        self.year_end = year_end
        self.docs_save_dir = Path(docs_save_dir)
        self.llm_client = llm_client
        self.llm_model = llm_model
        self.llm_prompt = llm_prompt
        self.use_selenium = use_selenium
        self.verbose = verbose
        self.max_workers = max_workers
        self.years = list(range(self.year_start, self.year_end + 1))
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
                            AppleWebKit/537.36 (KHTML, like Gecko) \
                            Chrome/80.0.3987.149 Safari/537.36"
        }
        self.queue = Queue()
        self.error_queue = Queue()
        self.results = []
        self.count = 0  # keep track of number of results
        self.md = MarkItDown(llm_client=llm_client, llm_model=llm_model)
        self.soup = None
        self.driver: Chrome = None
        self.saver: OneDriveSaver = None
        self.initialize_selenium()

    def initialize_selenium(self):
        """Initialize selenium driver"""
        if self.use_selenium:
            options = Options()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--enable-javascript")
            self.driver = Chrome(options=options)

    def _initialize_saver(self):
        """Initialize saver class. The child class should call this method in its __init__ method, after setting the docs_save_dir attribute."""
        self.saver = OneDriveSaver(self.queue, self.error_queue, self.docs_save_dir)

    def _make_request(
        self, url: str, method: str = "GET", json: dict = None
    ) -> requests.Response:
        """Make request to given url"""
        retries = 5
        for _ in range(retries):
            try:

                if method == "POST":
                    response = requests.post(url, headers=self.headers, json=json, verify=False)
                else:
                    response = requests.get(url, headers=self.headers, verify=False)

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

    def _selenium_get_soup(self, url: str) -> BeautifulSoup:
        """Get BeautifulSoup object from given url using selenium"""
        self.driver.get(url)
        return BeautifulSoup(self.driver.page_source, "html.parser")

    def _get_markdown(
        self,
        url: str = None,
        response: requests.Response = None,
        stream: BytesIO = None,
    ) -> str:
        """Get markdown response from given url"""
        try:
            if stream is not None:
                md_content = self.md.convert_stream(
                    stream, llm_prompt=self.llm_prompt
                ).text_content
                return md_content

            if response is None:
                response = self._make_request(url)
            md_content = self.md.convert(
                response,
                llm_prompt=(
                    self.llm_prompt
                    if not isinstance(response, requests.Response)
                    else None
                ),
            ).text_content

        except FileConversionException as e:
            print(f"Error converting to markdown: {e}")
            md_content = None

        except UnsupportedFormatException as e:
            print(f"Error converting to markdown: {e}")
            md_content = None

        except Exception as e:
            print(f"Error getting markdown from url: {url} | Error: {e}")
            md_content = None

        return md_content

    def _format_search_url(self, *args, **kwargs):
        pass

    def _get_docs_links(self, *args, **kwargs):
        pass

    def _get_doc_data(self, *args, **kwargs):
        pass

    def _scrape_year(self, year: int):
        pass

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

        # # scrape data from all years
        for year in tqdm(
            self.years, desc=f"{self.__class__.__name__} | Years", total=len(self.years)
        ):
            if year < resume_from:
                continue

            self._scrape_year(year)

        # stop saver thread
        self.saver.stop()

        # wait for saver thread to finish
        self.saver.join()

        return self.results
