import os
import requests
import time
import fitz
import psutil

from typing import Union
from PIL import Image
from openai import OpenAI
from io import BytesIO
from os import environ
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options


from markitdown import MarkItDown, UnsupportedFormatException, FileConversionException
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue
from pathlib import Path
from dotenv import load_dotenv
from src.database.saver import OneDriveSaver
from src.utils.openvpn import OpenVPNManager

load_dotenv()

YEAR_START = 1808  # CHECK IF NECESSARY LATER
DEFAULT_VALID_SITUATION = "Não consta revogação expressa"
DEFAULT_INVALID_SITUATION = "Revogada"

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
        use_requests_session: bool = False,
        use_openvpn: bool = False,
        config_files: list = None,
        openvpn_credentials_map: dict = None,
        proxies: dict = None,
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
        self.use_requests_session = use_requests_session
        self.use_openvpn = use_openvpn
        self.config_files = config_files
        self.openvpn_credentials_map = openvpn_credentials_map
        self.verbose = verbose
        self.proxies = proxies
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
        self.session: requests.Session = None
        self.driver: Chrome = None
        self.saver: OneDriveSaver = None
        self.openvpn_manager: OpenVPNManager = None
        self.initialize_selenium()
        self.initialize_requests_session()
        self.initialize_openvpn_manager()

    def initialize_requests_session(self):
        """Initialize requests session"""
        if self.use_requests_session:
            self.session = requests.Session()
            self.session.headers.update(self.headers)

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

    def initialize_openvpn_manager(self):
        """Initialize openvpn manager"""
        if self.use_openvpn:
            self.openvpn_manager = OpenVPNManager(
                config_files=self.config_files,
                credentials_map=self.openvpn_credentials_map,
            )

    def _get_request(self, url: str, **kwargs) -> requests.Response:
        """Get request from given url"""
        if self.use_requests_session:
            response = self.session.get(url, proxies=self.proxies, **kwargs)
        else:
            response = requests.get(url, proxies=self.proxies, **kwargs)

        return response

    def _post_request(self, url: str, json: dict, **kwargs) -> requests.Response:
        """Post request to given url"""
        if self.use_requests_session:
            response = self.session.post(url, json=json, proxies=self.proxies, **kwargs)
        else:
            response = requests.post(url, json=json, proxies=self.proxies, **kwargs)

        return response

    def _make_request(
        self,
        url: str,
        method: str = "GET",
        json: dict = None,
        payload: list | dict = None,
    ) -> requests.Response:
        """Make request to given url"""
        retries = 5
        for _ in range(retries):
            try:

                if method == "POST":
                    response = self._post_request(
                        url,
                        json=json,
                        data=payload,  # payload will be used for form data in POST requests, useful when have files or duplicate keys
                        headers=self.headers,
                        verify=False,
                    )
                else:
                    response = self._get_request(
                        url,
                        headers=self.headers,
                        verify=False,
                    )

                # check  "O servidor encontrou um erro interno, ou está sobrecarregado" error
                if (
                    "O servidor encontrou um erro interno, ou está sobrecarregado"
                    in response.text
                ):
                    print("Server error, retrying...")
                    time.sleep(5)
                    continue

                # check for 429 or 503 status code (right now useful for mato grosso scraper)
                if response.status_code in [429, 503]:
                    # print(f"Status code {response.status_code}, retrying...")
                    time.sleep(5)
                    continue

                return response
            except Exception as e:
                print(f"Error getting response from url: {url}")
                print(e)
                time.sleep(5)

        return None

    def _change_vpn_connection(self):
        """Change VPN connection. Currently the supported VPN is ProtonVPN and the way to reconnect is by killing the process and starting it again."""
        if not self.use_openvpn:
            print("OpenVPN is not enabled, skipping VPN connection change")
            return

        if self.openvpn_manager is None:
            print("OpenVPN manager is not initialized, skipping VPN connection change")
            return

        self.openvpn_manager.change_vpn_connection()

    def _get_soup(self, url: Union[str, requests.Response]) -> BeautifulSoup:
        """Get BeautifulSoup object from given url"""

        if isinstance(url, requests.Response):
            return BeautifulSoup(url.content, "html.parser")

        res = self._make_request(url)

        if res is None:
            return None

        return BeautifulSoup(res.content, "html.parser")

    def _selenium_get_soup(self, url: str) -> BeautifulSoup:
        """Get BeautifulSoup object from given url using selenium"""
        retries = 3
        while retries > 0:
            try:
                self.driver.get(url)
                if self.use_openvpn:
                    self._handle_blocked_access()

                time.sleep(1)
                break
            except Exception as e:
                print(f"Error: {e}")
                retries -= 1

        return BeautifulSoup(self.driver.page_source, "html.parser")

    def _pdf_to_images(self, doc: fitz.Document) -> list:
        """
        Converts a PDF document to a list of images, one image per page.

        Args:
            doc (fitz.Document): PyMuPDF Document object.

        Returns:
            list: List of PIL Image objects, one for each page in the PDF.
        """
        image_list = []

        for page_num in range(doc.page_count):
            page = doc.load_page(page_num)
            pix = page.get_pixmap(
                matrix=fitz.Identity,
                dpi=None,
                colorspace=fitz.csRGB,
                clip=None,
                annots=True,
            )

            # Convert PyMuPDF Pixmap to PIL Image
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            image_list.append(img)

        return image_list

    def _get_pdf_image_markdown(self, pdf_content: bytes) -> str:
        """Get markdown response from given pdf content"""
        pdf_file_stream = BytesIO(pdf_content)
        text_markdown_raw = self._get_markdown(stream=pdf_file_stream)
        if text_markdown_raw and len(text_markdown_raw) > 200:
            print("Text extracted from pdf")
            return text_markdown_raw

        # get images from pdf
        pdf = fitz.open("pdf", pdf_content)
        images = self._pdf_to_images(pdf)

        # paralllel processing
        text_markdown_img = ""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for img in images:
                buffer = BytesIO()
                img.save(buffer, format="PNG")
                img = BytesIO(buffer.getvalue())
                future = executor.submit(self._get_markdown, stream=img)
                futures.append(future)

            for future in tqdm(
                futures,  # not using as_completed because we want the results in order
                desc="Converting images to markdown",
                total=len(futures),
                disable=not self.verbose,
            ):
                md_content = future.result()
                text_markdown_img += md_content + "\n\n"

        if not text_markdown_img:
            print("No images found in pdf")

        if not text_markdown_raw:
            print("No text found in pdf")

        text_markdown = text_markdown_raw + text_markdown_img
        return text_markdown

    def _get_markdown(
        self,
        url: str = None,
        response: requests.Response = None,
        stream: BytesIO = None,
        retries: int = 2,
    ) -> str:
        """Get markdown response from given url"""
        while retries > 0:
            try:
                if stream is not None:
                    md_content = self.md.convert_stream(
                        stream, llm_prompt=self.llm_prompt
                    ).text_content

                    if (
                        not md_content
                    ):  # for images, sometimes the mllm struggles to process, try again
                        continue

                    return md_content

                if response is None:
                    response = self._make_request(url)
                md_content = self.md.convert(response).text_content

            except FileConversionException as e:
                print(f"Error converting to markdown: {e}")
                md_content = ""

            except UnsupportedFormatException as e:
                print(f"Error converting to markdown: {e}")
                md_content = ""

            except Exception as e:
                print(f"Error getting markdown from url: {url} | Error: {e}")
                md_content = ""

            if md_content:
                break

            retries -= 1

        return md_content

    def _handle_blocked_access(self, *args, **kwargs):
        pass

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
