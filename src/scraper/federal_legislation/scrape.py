import requests
import time
from datetime import datetime
from bs4 import BeautifulSoup
from markitdown import MarkItDown
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from multiprocessing import Queue
from src.database.saver import OneDriveSaver, ONEDRIVE_SAVE_DIR

VALID_SITUATIONS = [
    "Não%20consta%20revogação%20expressa",
    "Não%20Informado",  # since there is no explicit information about it's not valid, we consider it valid
    "Convertida%20em%20Lei",
    "Reeditada",
    "Reeditada%20com%20alteração",
]  # only norms with these situations (are actually valid norms)

INVALID_SITUATIONS = [
    "Arquivada",
    "Rejeitada",
    "Revogada",
    "Sem%20Eficácia",
]  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS

# OBS: empty string means all (Toda legislação). OPTIONS: 'Legislação+Interna' 'OR Legislação+Federal'
COVERAGE = [""]

TYPES = [
    "Alvará",
    "Ato",
    "Carta%20Régia",
    "Carta+Imperial",
    "Constitui%C3%A7%C3%A3o",
    "Decisão",
    "Decreto",
    "Emenda+Constitucional",
    "Instrução",
    "Lei",
    "Manifesto",
    "Mensagem",
    "Pacto",
    "Proclamação",
    "Protocolo",
    "Medida+Provis%C3%B3ria",
    "Ordem+de+Serviço",
    "Portaria",
    "Regulamento",
    "Resolu%C3%A7%C3%A3o+da+Assembl%C3%A9ia+Nacional+Constituinte",
    "Resolu%C3%A7%C3%A3o+da+C%C3%A2mara+dos+Deputados",
    "Resolução+da+Mesa",
    "Resolu%C3%A7%C3%A3o+do+Congresso+Nacional",
    "Resolu%C3%A7%C3%A3o+do+Senado+Federal",
]
ORDERING = "data%3AASC"
YEAR_START = 1808  # CHECK IF NECESSARY LATER


class CamaraDepScraper:
    """Webscraper for Camara dos Deputados website (https://www.camara.leg.br/legislacao/)

    Example search request url: https://www.camara.leg.br/legislacao/busca?geral=&ano=&situacao=&abrangencia=&tipo=Decreto%2CDecreto+Legislativo%2CDecreto-Lei%2CEmenda+Constitucional%2CLei+Complementar%2CLei+Ordin%C3%A1ria%2CMedida+Provis%C3%B3ria%2CResolu%C3%A7%C3%A3o+da+C%C3%A2mara+dos+Deputados%2CConstitui%C3%A7%C3%A3o%2CLei%2CLei+Constitucional%2CPortaria%2CRegulamento%2CResolu%C3%A7%C3%A3o+da+Assembl%C3%A9ia+Nacional+Constituinte%2CResolu%C3%A7%C3%A3o+do+Congresso+Nacional%2CResolu%C3%A7%C3%A3o+do+Senado+Federal&origem=&numero=&ordenacao=data%3AASC
    """

    def __init__(
        self,
        base_url: str = "https://www.camara.leg.br/legislacao/",
        situations: list = SITUATIONS,
        coverage: list = COVERAGE,
        types: list = TYPES,
        ordering: str = ORDERING,
        year_start: int = YEAR_START,
        year_end: int = datetime.now().year,
        docs_save_dir: str = ONEDRIVE_SAVE_DIR,
        verbose: bool = False,
    ):
        self.base_url = base_url
        self.situations = situations
        self.coverage = coverage
        self.types = types
        self.ordering = ordering
        self.year_start = year_start
        self.year_end = year_end
        self.verbose = verbose
        self.docs_save_dir = docs_save_dir
        self.years = [str(year) for year in range(self.year_start, self.year_end + 1)]
        self.params = {
            "abrangencia": "",
            "geral": "",
            "ano": "",
            "situacao": "",
            "origem": "",
            "numero": "",
            "ordenacao": "",
        }
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
        }
        self.queue = Queue()
        self.error_queue = Queue()
        self.saver = OneDriveSaver(self.queue, self.error_queue, self.docs_save_dir)
        self.md = MarkItDown()
        self.remove_markdown_header = """* [Ir ao conteúdo](#main-content)
* [Ir à navegação principal](#main-nav)

[Página inicial](https://www.camara.leg.br)

* [Acessibilidade](https://www2.camara.leg.br/acessibilidade/recursos-de-acessibilidade)
* [Fale Conosco](https://www.camara.leg.br/fale-conosco)

* [Congresso](http://www.congressonacional.leg.br)
* [Senado](http://www.senado.leg.br)

PT

* [English
  EN](https://www2.camara.leg.br/english)
* [Español
  ES](https://www2.camara.leg.br/espanol)

[Página inicial](https://www.camara.leg.br)

* Assuntos
  + [Agropecuária](https://www.camara.leg.br/temas/agropecuaria)
  + [Cidades e transportes](https://www.camara.leg.br/temas/cidades-e-transportes)
  + [Ciência, tecnologia e comunicações](https://www.camara.leg.br/temas/ciencia-tecnologia-e-comunicacoes)
  + [Consumidor](https://www.camara.leg.br/temas/consumidor)
  + [Direitos humanos](https://www.camara.leg.br/temas/direitos-humanos)
  + [Economia](https://www.camara.leg.br/temas/economia)
  + [Educação, cultura e esportes](https://www.camara.leg.br/temas/educacao-cultura-e-esportes)
  + [Meio ambiente e energia](https://www.camara.leg.br/temas/meio-ambiente-e-energia)
  + [Política e administração pública](https://www.camara.leg.br/temas/politica-e-administracao-publica)
  + [Relações exteriores](https://www.camara.leg.br/temas/relacoes-exteriores)
  + [Saúde](https://www.camara.leg.br/temas/saude)
  + [Segurança](https://www.camara.leg.br/temas/seguranca)
  + [Trabalho, previdência e assistência](https://www.camara.leg.br/temas/trabalho-previdencia-e-assistencia)
* Institucional
  + [Agenda](https://www.camara.leg.br/agenda)
  + [Serviços](https://www2.camara.leg.br/transparencia/servicos-ao-cidadao)
  + [Presidência](https://www2.camara.leg.br/a-camara/estruturaadm/mesa/presidencia)
  + [Biblioteca e publicações](https://www.camara.leg.br/biblioteca-e-publicacoes/)
  + [Escola da Câmara](https://www.camara.leg.br/escola-da-camara/)
  + [Papel e estrutura](https://www.camara.leg.br/papel-e-estrutura/)
  + [História e arquivo](https://www.camara.leg.br/historia-e-arquivo/)
  + [Visite](https://www2.camara.leg.br/a-camara/visiteacamara)
* Deputados
  + [Quem são](https://www.camara.leg.br/deputados/quem-sao)
  + [Lideranças e bancadas](https://www.camara.leg.br/deputados/liderancas-e-bancadas-partidarias)
  + [Frentes e grupos parlamentares](https://www2.camara.leg.br/deputados/frentes-e-grupos-parlamentares)
* Atividade Legislativa
  + [Propostas legislativas](https://www.camara.leg.br/busca-portal/proposicoes/pesquisa-simplificada)
  + [Plenário](https://www.camara.leg.br/plenario)
  + [Comissões](https://www.camara.leg.br/comissoes)
  + [Discursos e debates](https://www2.camara.leg.br/atividade-legislativa/discursos-e-notas-taquigraficas)
  + [Estudos legislativos](https://www2.camara.leg.br/atividade-legislativa/estudos-e-notas-tecnicas)
  + [Orçamento da União](https://www2.camara.leg.br/atividade-legislativa/orcamento-da-uniao)
  + [Legislação](https://www.camara.leg.br/legislacao)
  + [Entenda o processo legislativo](https://www.camara.leg.br/entenda-o-processo-legislativo/)
  + [Participe](https://www2.camara.leg.br/atividade-legislativa/participe)
* Comunicação
  + [Agência Câmara de Notícias](https://www.camara.leg.br/noticias)
  + [TV Câmara](https://www.camara.leg.br/tv)
  + [Rádio Câmara](https://www.camara.leg.br/radio)
  + [Banco de Imagens](https://www.camara.leg.br/banco-imagens)
  + [Assessoria de Imprensa](https://www.camara.leg.br/assessoria-de-imprensa)
  + [Comprove uma notícia](https://www.camara.leg.br/comprove)
* Transparência e prestação de contas
  + [Transparência](https://www.camara.leg.br/transparencia/)
  + [Prestação de contas](https://www2.camara.leg.br/transparencia/prestacao-de-contas)
  + [Dados abertos](https://dadosabertos.camara.leg.br/)

* [Acessibilidade](https://www2.camara.leg.br/acessibilidade/recursos-de-acessibilidade)
* [Fale Conosco](https://www.camara.leg.br/fale-conosco)

PT

* [English
  EN](https://www2.camara.leg.br/english)
* [Español
  ES](https://www2.camara.leg.br/espanol)

Pesquise no Portal da Câmara

1. [Início](//www.camara.leg.br)
2. [Atividade Legislativa](http://www2.camara.gov.br/atividade-legislativa)
3. [Legislação](http://www2.camara.gov.br/atividade-legislativa/legislacao)
4. Esta página"""
        self.remove_markdown_footer = """**57ª Legislatura - 2ª Sessão Legislativa Ordinária**

Câmara dos Deputados - Palácio do Congresso Nacional - Praça dos Três Poderes
Brasília - DF - Brasil - CEP 70160-900
CNPJ: 00.530.352/0001-59

* Disque-Câmara: 0800-0-619-619, de 8h às 20h
* Atendimento presencial: de 9h às 19h

* [Whatsapp](https://whatsapp.com/channel/0029Va2fexI3gvWgfMs6Fv31)
* [Telegram](https://t.me/CamaradosDeputados)
* [Facebook](https://www.facebook.com/camaradeputados)
* [X](https://twitter.com/camaradeputados)
* [Tiktok](https://tiktok.com/%40camaradosdeputados)
* [Instagram](https://www.instagram.com/camaradeputados)

* [Sobre o Portal](https://www2.camara.leg.br/sobre-o-portal)
* [Termos de Uso](https://www2.camara.leg.br/termo-de-uso-e-politica-de-privacidade)
* [Aplicativos](https://www2.camara.leg.br/aplicativos/)
* [Extranet](https://camaranet.camara.leg.br)

##### Carregando

Por favor, aguarde."""
        self.results = []
        self.count = 0  # keep track of number of results
        self.soup = None

    def _format_search_url(self, year: str, situation: str, type: str) -> str:
        """Format search url with given year"""
        self.params["ano"] = year
        self.params["abrangencia"] = self.coverage[0]
        self.params["ordenacao"] = self.ordering
        self.params["situacao"] = situation
        self.params["tipo"] = type

        url = (
            self.base_url
            + "busca?"
            + "&".join([f"{key}={value}" for key, value in self.params.items()])
        )

        return url

    def _make_request(self, url: str) -> requests.Response:
        """Make request to given url"""
        retries = 3
        for _ in range(retries):
            try:
                response = requests.get(url, headers=self.headers)

                # check  "O servidor encontrou um erro interno, ou está sobrecarregado" error
                if (
                    "O servidor encontrou um erro interno, ou está sobrecarregado"
                    in response.text
                ):
                    print("Server error, retrying...")
                    time.sleep(5)
                    continue

                return response
            except Exception as e:
                print(f"Error getting response from url: {url}")
                print(e)
                time.sleep(5)

        return None

    def _get_soup(self, url: str) -> BeautifulSoup:
        """Get BeautifulSoup object from given url"""
        response = self._make_request(url)

        if response is None:
            return None

        return BeautifulSoup(response.text, "html.parser")
        # response = requests.get(url, headers=self.headers)
        # return BeautifulSoup(response.text, "html.parser")

    def _get_markdown(self, url: str) -> str:
        """Get markdown response from given url"""
        response = self._make_request(url)
        return self.md.convert(response).text_content

    def _get_documents_html_links(self, url: str) -> "list[dict]":
        """Get html links from given url. Returns a list of dictionaries in the format {
            "title": str,
            "summary": str,
            "html_link": str
        }"""
        soup = self._get_soup(url)

        # Get all documents html links from page
        documents = soup.find_all("li", class_="busca-resultados__item")
        documents_html_links_info = []
        for document in documents:
            a_tag = document.find("h3", class_="busca-resultados__cabecalho").find("a")
            document_html_link = a_tag["href"]
            title = a_tag.text.strip()
            summary = document.find(
                "p", class_="busca-resultados__descricao js-fade-read-more"
            ).text.strip()
            documents_html_links_info.append(
                {"title": title, "summary": summary, "html_link": document_html_link}
            )

        return documents_html_links_info

    def _get_document_text_link(
        self, document_html_link: str, title: str, summary: str
    ) -> dict:
        """Get proper document text link from given document html link"""

        soup = self._get_soup(document_html_link)
        document_text_links = soup.find("div", class_="sessao")
        if not document_text_links:
            # probably link doesn't exist (error in website)
            print(f"Could not find text link for document: {title}")
            error_data = {
                "title": title,
                "year": self.params["ano"],
                "situation": self.params["situacao"],
                "type": self.params["tipo"],
                "summary": summary,
                "html_link": document_html_link,
            }
            self.error_queue.put(error_data)
            return None

        document_text_links = document_text_links.find_all("a")
        document_text_link = None
        for link in document_text_links:
            if "texto - publicação original" in link.text.strip().lower():
                url = link["href"]

                # get full url
                document_text_link = requests.compat.urljoin(document_html_link, url)
                break

        if document_text_link is None:
            print(f"Could not find text link for document: {title}")
            return None

        return {"title": title, "summary": summary, "html_link": document_text_link}

    def _get_document_data(
        self, document_text_link: str, title: str, summary: str
    ) -> dict:
        """Get data from given document text link . Data will be in the format {
            "title": str,
            "summary": str,
            "html_string": str,
            "text_markdown": str,
            "document_url": str
        }"""
        soup = self._get_soup(document_text_link)

        try:
            # get html string
            html_string = soup.find("div", class_="textoNorma").prettify(
                formatter="html"
            )

            # get text markdown
            text_markdown = self._get_markdown(document_text_link)
            text_markdown = text_markdown.replace(
                self.remove_markdown_header, ""
            ).replace(self.remove_markdown_footer, "")

            return {
                "title": title,
                "summary": summary,
                "html_string": html_string,
                "text_markdown": text_markdown,
                "document_url": document_text_link,
            }
        except Exception as e:
            print(f"Error getting html string for document: {title}")
            print(e)
            error_data = {
                "title": title,
                "year": self.params["ano"],
                "situation": self.params["situacao"],
                "type": self.params["tipo"],
                "summary": summary,
                "html_link": document_text_link,
            }
            self.error_queue.put(error_data)
            return None

    def _scrape_year(self, year: str) -> list:
        """Scrape data from given year"""
        for situation in tqdm(
            self.situations,
            desc="CamaraDEP | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            results = []

            for type in self.types:
                url = self._format_search_url(year, situation, type)
                # Each page has 20 results, find the total and calculate the number of pages
                per_page = 20
                self.soup = self._get_soup(url)

                total = self.soup.find(
                    "div",
                    class_="busca-info__resultado busca-info__resultado--informado",
                ).text
                total = int(total.strip().split()[-1])

                if total == 0:
                    if self.verbose:
                        print(
                            f"No results for Year: {year} | Situation: {situation} | Type: {type}"
                        )
                    continue
                pages = total // per_page + 1

                # Get documents html links from all pages using ThreadPoolExecutor
                with ThreadPoolExecutor() as executor:
                    documents_html_links_info = []
                    futures = [
                        executor.submit(
                            self._get_documents_html_links, url + f"&pagina={page}"
                        )
                        for page in range(1, pages + 1)
                    ]
                    for future in tqdm(
                        as_completed(futures),
                        desc="CamaraDEP |Pages",
                        disable=not self.verbose,
                        total=len(futures),
                    ):
                        documents_html_links_info.extend(future.result())

                # Get proper document text link from each document html link
                with ThreadPoolExecutor() as executor:
                    futures = []
                    documents_text_links = []
                    futures.extend(
                        [
                            executor.submit(
                                self._get_document_text_link,
                                document_html_link.get("html_link"),
                                document_html_link.get("title"),
                                document_html_link.get("summary"),
                            )
                            for document_html_link in documents_html_links_info
                            if document_html_link is not None
                        ]
                    )

                    for future in tqdm(
                        as_completed(futures),
                        desc="CamaraDEP | Text link",
                        total=len(futures),
                        disable=not self.verbose,
                    ):
                        documents_text_links.append(future.result())

                # Get data from all  documents text links using ThreadPoolExecutor
                with ThreadPoolExecutor() as executor:
                    results = []
                    futures = [
                        executor.submit(
                            self._get_document_data,
                            document_text_link.get("html_link"),
                            document_text_link.get("title"),
                            document_text_link.get("summary"),
                        )
                        for document_text_link in documents_text_links
                        if document_text_link is not None
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc="CamaraDEP |Documents text",
                        total=len(futures),
                        disable=not self.verbose,
                    ):
                        result = future.result()

                        if result is None:
                            continue

                        # save to onedrive
                        queue_item = {
                            "year": year,
                            "situation": situation,
                            "type": type,
                            **result,
                        }
                        self.queue.put(queue_item)
                        results.append(queue_item)

                self.results.extend(results)
                self.count += len(results)

            print(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {type} | Results: {len(results)} | Total: {self.count}"
            )

    def scrape(self) -> list:
        """Scrape data from all years"""
        # start saver thread
        self.saver.start()

        # check if can resume from last scrapped year
        resume_from = self.year_start  # 1808 by default
        forced_resume = self.year_start > YEAR_START
        if self.saver.last_year is not None and not forced_resume:
            resume_from = int(self.saver.last_year)

        # scrape data from all years
        for year in tqdm(self.years, desc="CamaraDEP | Years", total=len(self.years)):
            if int(year) < resume_from:
                continue

            self._scrape_year(year)

        # stop saver thread
        self.saver.stop()

        # wait for saver thread to finish
        self.saver.join()

        return self.results
