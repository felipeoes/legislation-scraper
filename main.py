import os
from typing import List, Dict
from src.scraper.base.scraper import BaseScaper
from src.scraper.federal_legislation.scrape import CamaraDepScraper
from src.scraper.conama.scrape import ConamaScraper
from src.scraper.state_legislation import SaoPauloAlespScraper, RJAlerjScraper
from dotenv import load_dotenv

load_dotenv()

ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR = os.environ.get(
    "ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR"
)

if __name__ == "__main__":

    try:

        scrapers: List[Dict[str, BaseScaper]] = [
            # {
            #     "scraper": CamaraDepScraper(verbose=False, year_start=1808, year_end=2024),
            #     "name": "Camara dos Deputados"
            # },
            # {
            #     "scraper": SaoPauloAlespScraper(),
            #     "name": "Alesp"
            # },
            # {
            #     "scraper": RJAlerjScraper(year_start=1968),
            #     "name": "Alerj"
            # },
            {
                "scraper": ConamaScraper(
                    year_start=1984,
                    docs_save_dir=ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR,
                ),
                "name": "CONAMA",
            }
        ]

        for scraper in scrapers:
            data = scraper["scraper"].scrape()
            print(f"Scraped {len(data)} data for {scraper['name']}")

    except KeyboardInterrupt:
        for scraper in scrapers:
            scraper["scraper"].saver.running = False
            scraper["scraper"].saver.join()

        print("KeyboardInterrupt: Exiting...")

    print("Exiting...")
    exit(0)
