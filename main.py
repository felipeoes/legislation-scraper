from src.scraper.federal_legislation.scrape import CamaraDepScraper
from src.scraper.state_legislation.sao_paulo import SaoPauloAlespScraper

if __name__ == "__main__":
    
    # camara_scraper = CamaraDepScraper(year_start=1933)
    # data = camara_scraper.scrape()
    # print(f"Scraped {len(data)} data for Camara dos Deputados")
    
    alesp_scraper = SaoPauloAlespScraper(year_start=1865) # only have data starting from 1865
    data = alesp_scraper.scrape()
    print(f"Scraped {len(data)} data for Alesp")
    
     
    
