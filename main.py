from src.scraper.federal_legislation.scrape import CamaraDepScraper
from src.scraper.state_legislation.sao_paulo import SaoPauloAlespScraper
from src.scraper.state_legislation.rio_de_janeiro import RJAlerjScraper

if __name__ == "__main__":

    try:
        camara_scraper = CamaraDepScraper(verbose=False, year_start=1991)
        data = camara_scraper.scrape()
        print(f"Scraped {len(data)} data for Camara dos Deputados")

    # alesp_scraper = SaoPauloAlespScraper(year_start=1865) # only have data starting from 1865
    # data = alesp_scraper.scrape()
    # print(f"Scraped {len(data)} data for Alesp")

    # alerj_scraper = RJAlerjScraper(year_start=1968)
    # data = alerj_scraper.scrape()
    # print(f"Scraped {len(data)} data for Alerj")
    except KeyboardInterrupt:
        # wait saver to finish saving
        camara_scraper.saver.running = False
        camara_scraper.saver.join()
        print("KeyboardInterrupt: Exiting...")

    exit(0)
