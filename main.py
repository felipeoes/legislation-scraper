import os
from openai import OpenAI
from typing import List, Dict
from src.scraper.base.scraper import BaseScaper
from src.scraper.federal_legislation.scrape import CamaraDepScraper
from src.scraper.conama.scrape import ConamaScraper
from src.scraper.state_legislation import (
    AcreLegisScraper,
    AlagoasSefazScraper,
    SaoPauloAlespScraper,
    RJAlerjScraper,
)
from dotenv import load_dotenv

load_dotenv()

ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR = os.environ.get(
    "ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR"
)

if __name__ == "__main__":

    try:

        client = OpenAI(
            api_key=os.environ.get("LLM_API_KEY"),
            base_url=os.environ.get("PROVIDER_BASE_URL"),
        )
        model = os.environ.get("LLM_MODEL")

        scrapers: List[Dict[str, BaseScaper]] = [
            # {
            #     "scraper": CamaraDepScraper(verbose=False, year_start=1808, year_end=2024),
            #     "name": "Camara dos Deputados"
            # },
            # {
            #     "scraper": ConamaScraper(
            #         year_start=1984,
            #         docs_save_dir=ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR,
            #     ),
            #     "name": "CONAMA",
            # },
            # {
            #     "scraper": AcreLegisScraper(verbose=True, max_workers=32),
            #     "name": "ACLegis",
            # },
            {
                "scraper": AlagoasSefazScraper(
                    year_start=1899,
                    llm_client=client,  # using LLM API for OCR (some documents are actually images embedded in pdf)
                    llm_model=model,
                    verbose=True,
                    max_workers=48,
                ),
                "name": "ALSefaz",
            }
            # {
            #     "scraper": SaoPauloAlespScraper(),
            #     "name": "SPAlesp"
            # },
            # {
            #     "scraper": RJAlerjScraper(year_start=1968),
            #     "name": "RJAlerj"
            # },
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
