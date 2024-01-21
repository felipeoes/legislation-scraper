from src.scraper.federal_legislation.scrape import CamaraDepScraper
from src.scraper.state_legislation.sao_paulo import AlespScraper

if __name__ == "__main__":
    
    camara_scraper = CamaraDepScraper(year_start=1933)
    data = camara_scraper.scrape()
    
    alesp_scraper = AlespScraper(year_start=1808)
     
    print(f"Scraped {len(data)} data")
