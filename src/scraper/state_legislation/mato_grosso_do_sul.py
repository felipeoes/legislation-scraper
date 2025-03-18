import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper

TYPES = {
    "Constituição Estadual": "/Web%5CConstituição%20Estadual",
    "Decreto": "/Decreto",
    "Decreto E": "/DecretoE",
    "Decreto-Lei": "/Decreto-Lei",
    "Deliberação Conselho de Governança": "/Web%5CDeliberacaoConselhoGov",
    "Emenda Constitucional": "/Emenda",
    "Lei Complementar": "/Lei%20Complementar",
    "Lei Estadual": "/Lei%20Estadual",
    "Mensagem Vetada": "/Mensagem%20Veto",
    "Resolução": "/Resolucoes",
    "Resolução Conjunta": "/Web%5CResolução%20Conjunta",
}

VALID_SITUATIONS = [
    "Não consta"
]  # Alems does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class MSAlemsScraper(BaseScaper):
    """Webscraper for Mato Grosso do Sul state legislation website (https://www.al.ms.gov.br/)

    Example search request: http://aacpdappls.net.ms.gov.br/appls/legislacao/secoge/govato.nsf/Emenda?OpenView&Start=1&Count=30&Expand=1#1
    
    OBS: Start=1&Count=30&Expand=1#1, for Expand 1 is the index related to the year
    """

    def __init__(
        self,
        base_url: str = "http://aacpdappls.net.ms.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "MATO_GROSSO_DO_SUL"
        self.params = {
            "OpenView": "",
            "Start": 1,
            "Count": 10000,  # there is no limit for count, so setting to a large number to get all norms in one request
            "Expand": "",
        }
        self._initialize_saver()

    def _format_search_url(self, norm_type_id: str, year_index: int) -> str:
        """Format url for search request"""
        self.params["Expand"] = year_index
        return f"{self.base_url}/appls/legislacao/secoge/govato.nsf/{norm_type_id}?{requests.compat.urlencode(self.params)}"
    
    