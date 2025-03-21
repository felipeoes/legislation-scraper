import os
from openai import OpenAI
from typing import List, Dict
from src.scraper.base.scraper import BaseScaper
from src.scraper.federal_legislation.scrape import CamaraDepScraper
from src.scraper.conama.scrape import ConamaScraper
from src.scraper.icmbio.scrape import ICMBioScraper
from src.scraper.state_legislation import (
    AcreLegisScraper,
    AlagoasSefazScraper,
    LegislaAMScraper,
    AmapaAlapScraper,
    BahiaLegislaScraper,
    CearaAleceScraper,
    DFSinjScraper,
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
            {
                "scraper": CamaraDepScraper,
                "params": {
                    "verbose": False,
                    "year_start": 1808,
                    "year_end": 2024,
                },
                "name": "Camara dos Deputados",
                "run": False,
            },
            {
                "scraper": ConamaScraper,
                "params": {
                    "year_start": 1984,
                    "docs_save_dir": ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR,
                    "verbose": True,
                },
                "name": "CONAMA",
                "run": False,
            },
            {
                "scraper": ICMBioScraper,
                "params": {
                    "year_start": 1800,
                    "use_selenium": True,
                    "docs_save_dir": ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR,
                    "verbose": True,
                },
                "name": "ICMBio",
                "run": False,
            },
            {
                # "scraper": AcreLegisScraper(
                #     year_start=1800, verbose=True, max_workers=32
                # ),
                "scraper": AcreLegisScraper,
                "params": {
                    "year_start": 1800,
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "ACLegis",
                "run": False,
            },
            {
                "scraper": AlagoasSefazScraper,
                "params": {
                    "year_start": 2010,
                    "llm_client": client,
                    "llm_model": model,
                    "verbose": True,
                    "max_workers": 48,
                },
                "name": "ALSefaz",
                "run": False,
            },
            {
                "scraper": LegislaAMScraper,
                "params": {
                    "year_start": 1953,
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "LegislaAM",
                "run": False,
            },
            {
                "scraper": AmapaAlapScraper,
                "params": {
                    "year_start": 1991,  # 1991 is the earliest year available
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "APAlap",
                "run": False,
            },
            {
                "scraper": BahiaLegislaScraper,
                "params": {
                    "year_start": 1891,  # 1891 is the earliest year available
                    "verbose": True,
                    "max_workers": 48,
                },
                "name": "BALegisla",
                "run": False,
            },
            {
                "scraper": CearaAleceScraper,
                "params": {
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "CEAlece",
                "run": False,
            },
            {
                "scraper": DFSinjScraper,
                "params": {
                    "verbose": True,
                },
                "name": "DFSinj",
                "run": True,
            },
            {
                "scraper": SaoPauloAlespScraper,
                "params": {},
                "name": "SPAlesp",
                "run": False,
            },
            {
                "scraper": RJAlerjScraper,
                "params": {
                    "year_start": 1968,
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "RJAlerj",
                "run": False,
            },
        ]

        for scraper in scrapers:
            if scraper["run"]:
                data = scraper["scraper"](**scraper["params"]).scrape()
                print(f"Scraped {len(data)} data for {scraper['name']}")

    except KeyboardInterrupt:
        for scraper in scrapers:
            if scraper["run"]:
                scraper["scraper"].saver.running = False
                scraper["scraper"].saver.join()

        print("KeyboardInterrupt: Exiting...")

    print("Exiting...")
    exit(0)
