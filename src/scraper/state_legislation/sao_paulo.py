import requests
import time
from os import environ
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from multiprocessing import Queue
from src.database.saver import OneDriveSaver
from dotenv import load_dotenv

load_dotenv()

# We don't have situations for São Paulo, since the websitew only publishes valid documents (no invalid, no expired, no archived, no revoked, etc.)

TYPES = {  # dict with norm type and its id
    'Decreto': 3,
    'Decreto Legislativo': 28,
    'Decreto-Lei': 25,
    'Decreto-Lei Complementar': 1,
    'Emenda Constitucional': 55,
    'Lei': 9,
    'Lei Complementar': 2,
    'Resolução': 14,
    'Resolução da Alesp': 19,
}


YEAR_START = 1808  # CHECK IF NECESSARY LATER
ONEDRIVE_ALESP_SAVE_DIR = rf"{environ.get('ONEDRIVE_ALESP_SAVE_DIR')}"


class AlespScraper:
    """ Webscraper for Alesp (Assembleia Legislativa do Estado de São Paulo) website (https://www.al.sp.gov.br/) 

    Example search request url: # https://www.al.sp.gov.br/norma/resultados?page=0&size=500&tipoPesquisa=E&buscaLivreEscape=&buscaLivreDecode=&_idsTipoNorma=1&idsTipoNorma=3&nuNorma=&ano=&complemento=&dtNormaInicio=&dtNormaFim=&idTipoSituacao=1&_idsTema=1&palavraChaveEscape=&palavraChaveDecode=&_idsAutorPropositura=1&_temQuestionamentos=on&_pesquisaAvancada=on
    """

    def __init__(self,  base_url: str = "https://www.al.sp.gov.br/norma/resultados",
                 types: dict = TYPES,
                 year_start: int = YEAR_START, year_end: int = datetime.now().year,
                 docs_save_dir: str = ONEDRIVE_ALESP_SAVE_DIR,
                 verbose: bool = False):
        self.base_url = base_url
        self.types = types
        self.year_start = year_start
        self.year_end = year_end
        self.verbose = verbose
        self.docs_save_dir = docs_save_dir
        self.years = [str(year) for year in range(
            self.year_start, self.year_end + 1)]
        self.params = {
            "size": 500,
            "tipoPesquisa": "E",
            "buscaLivreEscape": "",
            "buscaLivreDecode": "",
            "_idsTipoNorma": 1,
            "nuNorma": "",
            "ano": "",
            "complemento": "",
            "dtNormaInicio": "",
            "dtNormaFim": "",
            "idTipoSituacao": 1,  # only valid documents
            "_idsTema": 1,
            "palavraChaveEscape": "",
            "palavraChaveDecode": "",
            "_idsAutorPropositura": 1,
            "_temQuestionamentos": "on",
            "_pesquisaAvancada": "on",
        }
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
        }
        self.queue = Queue()
        self.error_queue = Queue()
        self.saver = OneDriveSaver(
            self.queue, self.error_queue, self.docs_save_dir)
        self.results = []
        self.count = 0  # keep track of number of results
        self.soup = None

    def _format_search_url(self, year: str, norm_type_id: int):
        """ Format url for search request """
        self.params['ano'] = year
        self.params['_idsTipoNorma'] = norm_type_id
        return self.base_url + "?" + "&".join([f"{key}={value}" for key, value in self.params.items()])

    def _get_soup(self, url: str) -> BeautifulSoup:
        """ Get BeautifulSoup object from given url """
        response = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup

    def _get_docs_html_links(self, url: str) -> list:
        """ Get documents html links from given page.
            Returns a list of dicts with keys 'title', 'summary', 'html_link' """
        # <tr class="odd">
        # 							<td width="40%">
        # 								<a target="_blank" href="/repositorio/legislacao/resolucao.alesp/2023/resolucao.alesp-941-04.12.2023.html"><img src="https://www.al.sp.gov.br/_img/fichaNP/ico_texto.png"></a>
        # 								<a target="_blank" href="/norma/publicacao/208880"><img src="https://www.al.sp.gov.br/_img/fichaNP/ico_DO.png"></a>
        # 								<a class="link_norma" href="/norma/208880"><span>Resolução da Alesp nº 941, de 04/12/2023</span></a>
        # 							</td>
        # 							<td><span>Altera a <a target="_blank" class="anotacao-link" href="/norma/5191">Resolução nº 805, de 3 de maio de 2000</a>, que dispõe sobre a afiliação da ALESP à União Nacional dos Legislativos Estaduais - UNALE.</span></td>
        # 						</tr>
        soup = self._get_soup(url)

        # Get all documents html links from page
        trs = soup.find_all('tr')
        docs_html_links = []
        for tr in trs:
            tds = tr.find_all('td')
            if len(tds) == 2:
                title = tds[0].find('span').text
                summary = tds[1].find('span').text
                # first <a> tag which contains the html link for the html document
                html_link = tds[0].find('a', href=True)['href']
                docs_html_links.append(
                    {'title': title, 'summary': summary, 'html_link': html_link})

        return docs_html_links

    def _get_doc_data(self, doc_info: dict) -> dict:
        """ Get document data from given html link """
#       <body>
# <a href="https://www.al.sp.gov.br">Assembleia Legislativa do Estado de São Paulo</a>
# <a href="/norma/208880">Ficha informativa</a><br>
# <h1>RESOLUÇÃO DA ALESP N° 941, DE 04 DE DEZEMBRO DE 2023</h1>
# <h2>(Projeto de Resolução n° 49, de 2023)</h2><h3>Altera a Resolução n° 805, de 3 de maio de 2000, que dispõe sobre a afiliação da ALESP à União Nacional dos Legislativos Estaduais - UNALE.</h3>
# <p>O PRESIDENTE DA ASSEMBLEIA LEGISLATIVA DO ESTADO DE SÃO PAULO, no uso da atribuição que lhe confere a alínea "h" do inciso II do artigo 18 do Regimento Interno, promulga a seguinte resolução:</p><p><strong>Artigo 1° - </strong>O § 2° do artigo 1° da <a data-idanotacao="5191" class="anotacao-link" target="blank" href="https://www.al.sp.gov.br/norma/5191">Resolução n° 805, de 3 de maio de 2000</a>, passa a vigorar com nova redação, e fica acrescido a esse artigo o § 3°, na seguinte conformidade:</p><p>"Artigo 1°- (...)</p><p>§ 2° - Fica a Mesa da Assembleia Legislativa autorizada a firmar termo de cooperação com a UNALE que discipline de maneira pormenorizada ações de fortalecimento do Poder Legislativo Estadual junto aos demais Poderes constituídos. (NR)</p><p>§ 3° - Enquanto perdurar a vigência do termo de cooperação a que se refere o § 2° deste artigo, fica a ALESP autorizada a transferir à UNALE o valor referente ao pagamento das mensalidades da associação, não se aplicando a sistemática da Lei Federal n° 13.019, de 31 de julho de 2014, em razão do disposto no artigo 3°, inciso IX, do referido diploma."</p><p><strong>Artigo 2° - </strong>As despesas decorrentes da execução desta resolução correrão à conta das dotações orçamentárias próprias, suplementadas, se necessário.</p><p><strong>Artigo 3° - </strong>Esta resolução entra em vigor na data de sua publicação.</p><p>Assembleia Legislativa do Estado de São Paulo, em 4/12/2023.</p><p>ANDRÉ DO PRADO - Presidente</p>

# </body>
        doc_html_link = doc_info['html_link']
        soup = self._get_soup(doc_html_link)

        # remove a tags with 'Assembleia Legislativa do Estado de São Paulo' and 'Ficha informativa'
        for a in soup.find_all('a'):
            a_text = a.text.lower()
            if a_text == 'Assembleia Legislativa do Estado de São Paulo'.lower() or a_text == 'Ficha informativa'.lower():
                a.decompose()
        
        # get data
        html_string = soup.body.prettify(formatter='html')

        return {
            "title": doc_info['title'],
            "summary": doc_info['summary'],
            "html_string": html_string,
            "document_url": doc_html_link
        }

    def _scrape_year(self, year: str):
        """ Scrape data from given year """
        # get data from all types
        for norm_type, norm_type_id in tqdm(self.types.items(), desc="ALESP | Types", total=len(self.types)):
            url = self._format_search_url(year, norm_type_id)
            soup = self._get_soup(url)

            # check if <div class="card cinza text-center">Nenhuma norma encontrada como os parâmetros informados</div> exists
            if 'Nenhuma norma encontrada como os parâmetros informados'.lower() in soup.text.lower():
                continue

            # get number of pages
            # <h5 class="mt-3">
                # <b>
                # 	<span>Resultado: 923 normas em 2</span>
                # 	<span>páginas</span>

                # </b>
                # </h5>
            total = soup.find(
                'span', text='página').previous_sibling.previous_sibling.text
            total = int(total.strip().split()[-1])

            if total == 0:
                if self.verbose:
                    print(f"No results for {norm_type} in {year}")

                continue

            pages = total // self.params['size'] + 1

            # Get documents html links from all pages using ThreadPoolExecutor
            with ThreadPoolExecutor() as executor:
                documents_html_links = []
                futures = [executor.submit(self._get_docs_html_links, url + f"&page={page}",
                                           ) for page in range(pages)]
                for future in tqdm(as_completed(futures), desc="ALESP | Get document link", total=pages):
                    documents_html_links.extend(future.result())

            # Get data from all  documents text links using ThreadPoolExecutor
            with ThreadPoolExecutor() as executor:
                results = []
                futures = [executor.submit(self._get_doc_data, doc_html_link)
                           for doc_html_link in documents_html_links]

                for future in tqdm(as_completed(futures), desc="ALESP | Get document data", total=len(documents_html_links)):
                    result = future.result()

                    if result is None:
                        continue

                    # save to one drive
                    queue_item = {
                        "year": year,
                        # hardcode since we only get valid documents in search request
                        "situation": "Sem revogação expressa",
                        "type": norm_type,
                        **result
                    }
                    self.queue.put(queue_item)
                    results.append(queue_item)

            self.results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Scraped {len(results)} results for {norm_type} in {year}")

    def scrape(self) -> list:
        """ Scrape data from all years """
        # start saver thread
        self.saver.start()

        # scrape data from all years
        for year in tqdm(self.years, desc="ALESP | Years", total=len(self.years)):
            self._scrape_year(year)

        # stop saver thread
        self.saver.stop()

        # wait for saver thread to finish
        self.saver.join()

        return self.results
