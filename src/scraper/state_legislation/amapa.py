import requests
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests.compat
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper


TYPES = {
    "Decreto Legislativo": 14,
    "Lei Complementar": 12,
    "Lei Ordinária": 13,
    "Resolução": 15,
    "Emenda Constitucional": 11,
    "Constituição Estadual": "12/1989/10/746",  # texto completo, modificar a lógica no scraper
}