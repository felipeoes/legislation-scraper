from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper


# gotten from https://sapl3.al.pb.leg.br/api/norma/tiponormajuridica/

TYPES = {
    "Ação Direta de Inconstitucionalidade Estadual": 15,
    "Constituição Estadual": 5,
    "Decreto Executivo": 18,
    "Decreto Legislativo": 6,  # the record with id == 3 is invalid, thus using 6
    "Decreto-Lei": 17,
    "Emenda Constitucional": 9,
    "Lei Complementar": 1,
    "Lei Ordinária": 2,
    "Lei Ordinária Promulgada": 8,
    "Regimento Interno": 14,
    "Resolução": 4,
}

# Alpb does not have a situation field, it will be inferred from the norm data
VALID_SITUATIONS = []

INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer have legal effect)

SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS

SUBJECTS = {
    50: "Administração Pública do Estado da Paraíba",
    19: "Agropecuária",
    66: "Câmaras Municipais do Estado da Paraíba",
    157: "Certificado de Congratulações",
    113: "Certificado de Excelência Ecológica",
    101: "Certificado de Qualidade em Serviço Público Municipal",
    153: "Certificado de Reconhecimento de Atividade de Relevante Interesse Ambiental",
    117: "Certificado de Responsabilidade Social",
    55: "Cidadania",
    76: "Cidadão Paraibano",
}


class ParaibaAlpbScraper(BaseScaper):
    """Webscraper for Paraíba state legislation website (https://sapl3.al.pb.leg.br/)

    Example search request: https://sapl3.al.pb.leg.br/api/norma/normajuridica/?tipo=2&page=3&ano=2025

    params = {
        tipo: 2
        page: 3
        ano: 2025
    }
    """

    def __init__(
        self,
        base_url: str = "https://sapl3.al.pb.leg.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.subjects = SUBJECTS
        self.docs_save_dir = self.docs_save_dir / "PARAIBA"
        self.params = {
            "tipo": "",
            "ano": "",
            "page": 1,
        }
        self._initialize_saver()

    def _format_search_url(
        self,
        norm_type_id: str,
        year: int,
        page: int = 1,
    ) -> str:
        """Format url for search request"""
        return f"{self.base_url}/api/norma/normajuridica/?tipo={norm_type_id}&page={page}&ano={year}"

    def _get_docs_links(self, url: str) -> list:
        """Get document links from search request. Returns a list of dicts with keys 'id', 'title', 'situation', 'summary', 'subject', 'date', 'origin', 'publication', 'pdf_link'"""

        response = self._make_request(url)
        docs = []

        items = response.json()["results"]
        #     "results": [
        # {
        #   "id": 17498,
        #   "__str__": "Lei Ordinária nº 13.578, de 06 de março de 2025",
        #   "metadata": {

        #   },
        #   "texto_integral": "https://sapl3.al.pb.leg.br/media/sapl/public/normajuridica/2025/17498/lei_13.578.pdf",
        #   "numero": "13578",
        #   "ano": 2025,
        #   "esfera_federacao": "E",
        #   "data": "2025-03-06",
        #   "data_publicacao": "2025-03-07",
        #   "veiculo_publicacao": "DOE",
        #   "pagina_inicio_publicacao": 2,
        #   "pagina_fim_publicacao": 2,
        #   "ementa": "Institui o Programa Estadual de Prevenção ao Alcoolismo entre Mulheres e dá outras providências.",
        #   "indexacao": "",
        #   "observacao": "",
        #   "complemento": false,
        #   "data_vigencia": null,
        #   "timestamp": "2025-03-10T14:10:13.495389-03:00",
        #   "data_ultima_atualizacao": "2025-03-10T14:10:13.496944-03:00",
        #   "ip": "10.83.19.254",
        #   "ultima_edicao": "2025-03-10T14:10:13.485620-03:00",
        #   "tipo": 2,
        #   "materia": 115040,
        #   "orgao": null,
        #   "user": 191,
        #   "assuntos": [34, 25, 14],
        #   "autores": []
        # },

        situation = "Não consta revogação expressa"  # default valid situation
        for item in items:

            # infer situation from data_vigencia
            if item["data_vigencia"] is not None:
                situation = "Revogada"  # just to know the norm is invalid

            doc = {
                "id": item["id"],
                "norm_number": item["numero"],
                "title": item["__str__"],
                "situation": situation,
                "summary": item["ementa"],
                "subject": [self.subjects[subject] for subject in item["assuntos"]],
                "date": item["data"],
                "origin": item["esfera_federacao"],
                "publication": item["veiculo_publicacao"],
                "pdf_link": item["texto_integral"],
            }
            docs.append(doc)

        return docs

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data"""
        # remove pdf_link from doc_info
        pdf_link = doc_info.pop("pdf_link")

        text_markdown = self._get_markdown(pdf_link)
        if not text_markdown:
            return None

        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = pdf_link

        return doc_info

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for norm_type, norm_type_id in tqdm(
            self.types.items(),
            desc=f"PARAIBA | Year: {year} | Types",
            total=len(self.types),
            disable=not self.verbose,
        ):

            url = self._format_search_url(norm_type_id, year)

            # get total number of pages
            response = self._make_request(url)
            if response.status_code == 400: # no norms for this year
                continue
            total_pages = response.json()["total_pages"]

            documents = []

            # get all norms
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(
                        self._get_docs_links,
                        self._format_search_url(norm_type_id, year, page=page),
                    )
                    for page in range(1, total_pages + 1)
                ]

                for future in tqdm(
                    as_completed(futures),
                    desc="PARAIBA | Get document links",
                    total=len(futures),
                    disable=not self.verbose,
                ):
                    result = future.result()
                    documents.extend(result)

            results = []

            # get all norm data
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(self._get_doc_data, doc_info)
                    for doc_info in documents
                ]

                for future in tqdm(
                    as_completed(futures),
                    desc="PARAIBA | Get document data",
                    total=len(futures),
                    disable=not self.verbose,
                ):
                    result = future.result()

                    # save to one drive
                    queue_item = {
                        "year": year,
                        "type": norm_type,
                        **result,
                    }

                    self.queue.put(queue_item)
                    results.append(queue_item)

            self.results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Finished scraping for Year: {year}  | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                )
