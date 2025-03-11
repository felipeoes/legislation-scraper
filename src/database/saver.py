import json
import time
import re
from unidecode import unidecode
from os import environ
from pathlib import Path
from threading import Thread, Lock
from multiprocessing import Queue
from urllib.parse import unquote
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

ONEDRIVE_SAVE_DIR = rf"{environ.get('ONEDRIVE_SAVE_DIR', 'outputs/legislation')}"
ERROR_LOG_DIR = rf"{environ.get('ERROR_LOG_DIR', 'logs/legislation')}"

print(f"Default saving to ONEDRIVE_SAVE_DIR: {ONEDRIVE_SAVE_DIR}")
print(f"Default saving to ERROR_LOG_DIR: {ERROR_LOG_DIR}")


class OneDriveSaver(Thread):
    """Background thread to save data to txt files in OneDrive folder"""

    def __init__(
        self,
        queue: Queue,
        error_queue: Queue,
        save_dir: str = ONEDRIVE_SAVE_DIR,
        error_log_dir: str = ERROR_LOG_DIR,
        max_path_length: int = 245,  # sinology max path length
    ):
        super().__init__(daemon=True)
        self.queue = queue
        self.error_queue = error_queue
        self.save_dir = save_dir
        self.error_log_dir = error_log_dir
        self.max_path_length = max_path_length
        self.format_regex_1 = re.compile(r"[\s]+")
        self.format_regex_2 = re.compile(r"[^\w\s-]")
        self.lock = Lock()
        self.running = True
        self.last_year = None
        self._set_last_year()
        print(f"Saving to {save_dir}")
        print(f"Saving errors to {error_log_dir}")

    def _set_last_year(self):
        """Set the last year that was saved (always the year before the current year in save_dir, to account for some possible delay in saving)"""
        save_dir = Path(self.save_dir)
        if not save_dir.exists():
            self.last_year = None
            return

        years = [int(year.name) for year in save_dir.iterdir() if year.is_dir()]
        self.last_year = max(years) - 1 if years else None

    def run(self):
        while self.running:
            if self.queue.empty() and self.error_queue.empty():
                time.sleep(3)
                continue

            if not self.queue.empty():
                data = self.queue.get()
                self.save(data)

            if not self.error_queue.empty():
                data = self.error_queue.get()
                self.save_error(data)

        # get all remaining data in queue
        print(f"Saving remaining {self.queue.qsize()} data in queue")
        progress = tqdm(total=self.queue.qsize())
        while not self.queue.empty():
            data = self.queue.get()
            self.save(data)
            progress.update(1)

        print(
            f"{self.__class__.__name__} stopped since queue is empty and running is {self.running}"
        )

    def truncate_file_path(self, file_path: Path, max_length: int) -> Path:
        """Truncate file path to max_length"""
        file_length = len(str(file_path))
        if file_length <= max_length:
            return file_path

        len_to_remove = file_length - max_length

        # truncate file path. Don't remove file extension (remove length from stem only)
        file_path = Path(
            file_path.parent / (file_path.stem[:-len_to_remove] + file_path.suffix)
        )

        return file_path

    def save(self, data: dict):
        """Save data to json file. Data will be a dict with keys 'title', 'year', 'situation', 'type', 'summary', 'html_string' and 'document_url'. Folder structure will be 'ONEDRIVE_SAVE_DIR/{year}/{type}/{situation}/{title}_{document_url}.json'"""
        with self.lock:
            try:
                save_dir = Path(self.save_dir)
                year_dir = save_dir / str(data["year"])
                type_dir = year_dir / data["type"]
                situation_dir = type_dir / data["situation"]

                # decode path
                situation_dir = Path(unquote(str(situation_dir)))
                situation_dir.mkdir(parents=True, exist_ok=True)

                # use regex to remove invalid characters
                title = unidecode(data["title"]).replace(" ", "_")
                title = self.format_regex_1.sub("_", title)
                title = self.format_regex_2.sub("", title)

                document_url = unidecode(data["document_url"]).replace(" ", "_")
                document_url = self.format_regex_1.sub("_", Path(document_url).stem)
                document_url = self.format_regex_2.sub("", document_url)

                file_path = situation_dir / f"{title}_{document_url}.json"
                file_path = self.truncate_file_path(file_path, self.max_path_length)

                # save json
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=True, indent=4)

            except Exception as e:
                print(f"Error saving {data['title']} to {file_path}: {e}")
                self.save_error(data)

    def save_error(self, data: dict):
        """Save error data to txt file. Data will be a dict with keys {"title": title, "year": self.params["ano"], "situation": self.params["situacao"], "type": self.params["tipo"], "summary": summary, "html_link": document_html_link}. Folder structure will be 'ERROR_LOG_DIR/{year}/{type}/{situation}/{title}_{document_url}.json"""
        with self.lock:
            try:
                save_dir = Path(self.error_log_dir)
                year_dir = save_dir / str(data["year"])
                type_dir = year_dir / data["type"]
                situation_dir = type_dir / data["situation"]

                # decode path
                situation_dir = Path(unquote(str(situation_dir)))
                situation_dir.mkdir(parents=True, exist_ok=True)

                # use regex to remove invalid characters
                title = unidecode(data["title"]).replace(" ", "_")
                title = self.format_regex_1.sub("_", title)
                title = self.format_regex_2.sub("", title)

                html_link = unidecode(data["html_link"]).replace(" ", "_")
                html_link = self.format_regex_1.sub("_", Path(html_link).stem)
                html_link = self.format_regex_2.sub("", html_link)

                file_path = situation_dir / f"{title}_{html_link}.json"
                file_path = self.truncate_file_path(file_path, self.max_path_length)

                # save json
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

            except Exception as e:
                print(f"Error saving error {data['title']} to {file_path}: {e}")

    def stop(self):
        print(f"Sending stop signal to {self.__class__.__name__}")
        self.running = False
