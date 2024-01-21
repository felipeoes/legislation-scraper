from src.scraper.scrape import CamaraDepScraper

if __name__ == "__main__":
    scraper = CamaraDepScraper()
    data = scraper.scrape()

    print(f"Scraped {len(data)} data")
