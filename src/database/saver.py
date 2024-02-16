import json
from os import environ
from pathlib import Path
from threading import Thread, Lock
from multiprocessing import Queue
from urllib.parse import unquote
from dotenv import load_dotenv

load_dotenv()

ONEDRIVE_SAVE_DIR = rf"{environ.get('ONEDRIVE_SAVE_DIR')}"
ERROR_LOG_DIR = rf"{environ.get('ERROR_LOG_DIR', 'logs/federal_legislation')}"
print(f"Default saving to {ONEDRIVE_SAVE_DIR}")


class OneDriveSaver(Thread):
    """ Background thread to save data to txt files in OneDrive folder """

    def __init__(self, queue: Queue, error_queue: Queue, save_dir: str = ONEDRIVE_SAVE_DIR, error_log_dir: str = ERROR_LOG_DIR):
        super().__init__()
        self.queue = queue
        self.error_queue = error_queue
        self.save_dir = save_dir
        self.error_log_dir = error_log_dir
        self.lock = Lock()
        self.running = True
        self.last_year = None
        self.set_last_year()

    def set_last_year(self):
        """ Set the last year that was saved """
        save_dir = Path(self.save_dir)
        if not save_dir.exists():
            self.last_year = None
            return

        years = [int(year.name)
                 for year in save_dir.iterdir() if year.is_dir()]
        self.last_year = max(years) if years else None

    def run(self):
        while self.running:
            if not self.queue.empty():
                data = self.queue.get()
                self.save(data)

            if not self.error_queue.empty():
                data = self.error_queue.get()
                self.save_error(data)

    def save(self, data: dict):
        """ Save data to json file. Data will be a dict with keys 'title', 'year', 'situation', 'type', 'summary', 'html_string' and 'document_url'. Folder structure will be 'ONEDRIVE_SAVE_DIR/{year}/{type}/{situation}/{title}_{document_url}.json' """
        with self.lock:
            save_dir = Path(self.save_dir)
            year_dir = save_dir / str(data['year'])
            type_dir = year_dir / data['type']
            situation_dir = type_dir / data['situation']

            # decode path
            situation_dir = Path(unquote(str(situation_dir)))
            situation_dir.mkdir(parents=True, exist_ok=True)

            title_file = situation_dir / \
                f"{data['title'].replace(' ', '_').replace('/', '_')}_{Path(data['document_url']).stem}.json"

            # save json
            with open(title_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

    def save_error(self, data: dict):
        """ Save error data to txt file. Data will be a dict with keys {"title": title, "year": self.params["ano"], "situation": self.params["situacao"], "type": self.params["tipo"], "summary": summary, "html_link": document_html_link}. Folder structure will be 'ERROR_LOG_DIR/{year}/{type}/{situation}/{title}_{document_url}.json """
        with self.lock:
            save_dir = Path(self.error_log_dir)
            year_dir = save_dir / str(data['year'])
            type_dir = year_dir / data['type']
            situation_dir = type_dir / data['situation']

            # decode path
            situation_dir = Path(unquote(str(situation_dir)))
            situation_dir.mkdir(parents=True, exist_ok=True)

            title_file = situation_dir / \
                f"{data['title'].replace(' ', '_').replace('/', '_')}_{Path(data['html_link']).stem}.json"

            # save json
            with open(title_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

    def stop(self):
        self.running = False
