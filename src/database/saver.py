import json
from os import environ
from pathlib import Path
from threading import Thread, Lock
from multiprocessing import Queue
from urllib.parse import unquote
from dotenv import load_dotenv

load_dotenv()

ONEDRIVE_SAVE_DIR = rf"{environ.get('ONEDRIVE_SAVE_DIR')}"

print(f"Default saving to {ONEDRIVE_SAVE_DIR}")


class OneDriveSaver(Thread):
    """ Background thread to save data to txt files in OneDrive folder """

    def __init__(self, queue: Queue, save_dir: str = ONEDRIVE_SAVE_DIR):
        super().__init__()
        self.queue = queue
        self.save_dir = save_dir
        self.lock = Lock()
        self.running = True

    def run(self):
        while self.running:
            if not self.queue.empty():
                data = self.queue.get()
                self.save(data)

    def save(self, data: dict):
        """ Save data to json file. Data will be a dict with keys 'title', 'year', 'situation', 'type', 'summary', 'html_string' and 'document_url'. Folder structure will be 'ONEDRIVE_SAVE_DIR/{year}/{type}/{situation}/{title}_{document_url}.json' """
        with self.lock:
            save_dir = Path(self.save_dir)
            year_dir = save_dir / data['year']
            type_dir = year_dir / data['type']
            situation_dir = type_dir / data['situation']

            # decode path
            situation_dir = Path(unquote(str(situation_dir)))
            situation_dir.mkdir(parents=True, exist_ok=True)

            title_file = situation_dir / \
                f"{data['title'].replace(' ', '_')}_{Path(data['document_url']).stem}.json"

            # save json
            with open(title_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

    def stop(self):
        self.running = False
