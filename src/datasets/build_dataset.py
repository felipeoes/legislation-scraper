from dotenv import load_dotenv
from markdownify import markdownify as md
from urllib.parse import unquote
from pathlib import Path
import re
import os
import pandas as pd
import warnings

from threading import Thread, Lock
from multiprocessing import Queue, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from datasets import Dataset
from tqdm import tqdm

# remove pd warning
warnings.simplefilter(action="ignore", category=FutureWarning)

load_dotenv()

HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN")
ONEDRIVE_SAVE_DIR = os.getenv("ONEDRIVE_SAVE_DIR")
DIR_PATH = Path(ONEDRIVE_SAVE_DIR)


class BackgroundSaver(Thread):
    """Thread to save data in background"""

    def __init__(self, queue: Queue, output_path: Path):
        Thread.__init__(self)
        self.queue = queue
        self.output_path = output_path
        self.stop = False

    def run(self):
        while not self.stop:
            # get data from queue
            data: pd.DataFrame = self.queue.get()

            # save data
            data = data.drop_duplicates(subset=["document_url"])
            data.to_csv(self.output_path, index=False)


class DatasetBuilder:
    """Dataset builder from scrapped data in json format. The folder strucuture will be as follows:
    - base_dir_path
        - year
            - norm_type
                - norm_situation
                    - json files

    Example:
        - data
            - 2020
                - lei
                    - aprovada
                        - 1.json
                        - 2.json
                    - rejeitada
                        - 1.json
                        - 2.json
                - decreto
                    - aprovada
                        - 1.json
                        - 2.json
                    - rejeitada
                        - 1.json
                        - 2.json
            - 2021
                - lei
                    - aprovada
                        - 1.json
                        - 2.json
                    - rejeitada
                        - 1.json
                        - 2.json
                - decreto
                    - aprovada
                        - 1.json
                        - 2.json
                    - rejeitada
                        - 1.json
                        - 2.json

    Dataset must have the columns:
        - text: the text of the norm (from raw data must come in 'html_string' or 'pdf_content' field)
        - type: the type of the norm
        - situation: the situation of the norm
        - year: the year of the norm
        - id: the id of the norm (file name)
        - all fields in the json file (may override the previous fields)
    """

    def __init__(self, base_dir_path: Path = DIR_PATH, output_path: Path = None):
        self.base_dir_path = base_dir_path
        self.output_path = output_path or DIR_PATH / "dataset.csv"
        self.queue = Queue()
        self.lock = Lock()
        self.data: list = []
        self.background_saver = None
        self._initialize_background_saver()
        self._load_data()

    def _initialize_background_saver(self):
        """Initialize background saver"""

        # initialize background saver
        self.background_saver = BackgroundSaver(self.queue, self.output_path)

        # start background saver
        self.background_saver.start()

    def _resume_from_checkpoint(self):
        """Resume from existing dataset, if exists"""
        if self.output_path.exists():
            data = pd.read_csv(self.output_path, encoding="utf-8")
            resume_from = data.shape[0]
            self.data = data.to_dict("records")
            return resume_from

        return 0

    def _read_json(self, path: str, index: int, total: int):
        try:
            if path:
                data = pd.read_json(path, typ="series", encoding="utf-8").to_dict()

                # check if 'html_string' or 'pdf_content' is empty
                if "html_string" in data and not data["html_string"]:
                    return
                
                if "pdf_content" in data and not data["pdf_content"]:
                    return
                
                with self.lock:
                    self.data.append(data)

                    if index % 500 == 0 or index == total - 1:
                        self.queue.put(pd.DataFrame(self.data))

        except Exception as e:
            print(f"Error reading {path}: {e}")

    def _load_data(self):
        """Load data from json files"""
        # use tqdm to show progress bar
        paths = list(tqdm(self.base_dir_path.glob("**/*.json")))

        # sort paths by file name (can resume from last file)
        paths.sort(key=lambda x: x.name)

        resume_from = self._resume_from_checkpoint()

        if resume_from:
            paths = paths[resume_from:]

        # load data from json files
        # save_every = 500
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []

            for index, path in enumerate(
                tqdm(paths, desc="Loading data", total=len(paths))
            ):
                if path.is_file():
                    futures.append(
                        executor.submit(self._read_json, path, index, len(paths))
                    )

            for future in tqdm(as_completed(futures), total=len(futures)):
                pass

    def build_dataset(
        self, dataset_name: str, output_path: Path = DIR_PATH / "dataset.csv"
    ):
        """Build dataset from json files"""

        # drop duplicates based on 'document_url' column ( May have duplicates because of the types "{norm_type} Sem Número")
        df_pd = pd.DataFrame(self.data).drop_duplicates(subset=["document_url"])

        print(f"Dataset shape: {df_pd.shape}")
        print(f"Dataset columns: {df_pd.columns}")

        # join 'html_string' and 'pdf_content' columns if both exists
        if "html_string" in df_pd.columns and "pdf_content" in df_pd.columns:
            df_pd["text"] = df_pd["html_string"] + df_pd["pdf_content"]
            df_pd.drop(columns=["html_string", "pdf_content"], inplace=True)

        elif "html_string" in df_pd.columns:
            df_pd.rename(columns={"html_string": "text"}, inplace=True)

        elif "pdf_content" in df_pd.columns:
            df_pd.rename(columns={"pdf_content": "text"}, inplace=True)

        # convert html or pdf to markdown. Remove img and a tags. Regex replaces four or more '\n' with three '\n'
        regex = re.compile(r"\n{4,}")
        df_pd["text"] = df_pd["text"].apply(
            lambda x: regex.sub(
                "\n\n\n", md(x, heading_style="ATX", strip=["img", "a"])
            ).strip()
        )

        # sanitize columns
        df_pd["type"] = df_pd["type"].apply(lambda x: unquote(x))
        df_pd["situation"] = df_pd["situation"].apply(lambda x: unquote(x))

        # save without index
        df_pd.to_csv(output_path, index=False)

        # print first ten rows
        print(df_pd.head(10))

        # save to huggingface datasets
        dataset = Dataset.from_pandas(df_pd)
        dataset.push_to_hub(dataset_name, token=HUGGINGFACE_TOKEN)


# test
try:
    output_dir = Path(__file__).resolve().parents[2] / "csv-datasets"
    output_path = output_dir / "dataset.csv"
    dir_path = DIR_PATH / "LEGISLACAO_FEDERAL"
    builder = DatasetBuilder(dir_path, output_path)
    dataset_name = "felipeoes/br_federal_legislation"
    # builder.build_dataset(dataset_name, output_path=dir_path / "dataset.csv")
    # 'csv-datasets' folder is two levels above the current folder
   
    builder.build_dataset(dataset_name, output_path=output_path) 
except KeyboardInterrupt:
    print("Interrupted")
