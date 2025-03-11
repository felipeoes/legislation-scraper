import requests

from io import BytesIO
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper

TYPES = [
    "Ato da Mesa Diretora",
    "Ato Declaratório",
    "Ato Declaratório Interpretativo",
    "Ato do Presidente",
    "Ato Regimental",
    "Decisão",
    "Decreto",
    "Decreto Executivo",
    "Decreto Legislativo",
    "Deliberação",
    "Despacho",
    "Determinação",
    "Emenda Constitucional",
    "Emenda Regimental",
    "Estatuto",
    "Instrução",
    "Instrução de Serviço",
    "Instrução Normativa",
    "Lei",
    "Lei Complementar",
    "Norma Técnica",
    "Portaria",
    "Ordem de Serviço",
    "Ordem de Serviço Conjunta",
    "Parecer Normativo",
    "Parecer Referencial",
    "Plano",
    "Portaria",
    "Portaria Conjunta",
    "Portaria Normativa",
    "Recomendação",
    "Regimento",
    "Regimento Interno",
    "Regulamento",
    "Resolução",
    "Resolução Administrativa",
    "Resolução Normativa",
    "Resolução Ordinária",
    "Súmula",
    "Súmula Administrativa",
]

VALID_SITUATIONS = {
    "Ajuizado": "ajuizado",
    "Alterado": "alterado",
    "Julgado Procedente": "julgadoprocedente",
    "Não conhecida": "naoconhecida",
    "Sem revogação expressa": "semrevogacaoexpressa",
}

INVALID_SITUATIONS = {
    "Anulado": "anulado",
    "Cancelado": "cancelado",
    "Cessar os efeitos": "cessarosefeitos",
    "Extinta": "extinta",
    "Inconstitucional": "inconstitucional",
    "Prejudicada": "prejudicada",
    "Revogado": "revogado",
    "Suspenso": "suspenso",
    "Sustado(a)": "sustado",
    "Tornado sem efeito": "tornadosemefeito",
}  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = {**VALID_SITUATIONS, **INVALID_SITUATIONS}


class DFSinjScraper(BaseScaper):
    """Webscraper for Distrito Federal state legislation website (https://www.sinj.df.gov.br/sinj/)

    Example search request: https://www.sinj.df.gov.br/sinj/ashx/Datatable/ResultadoDePesquisaNormaDatatable.ashx

    payload: {
        "bbusca": "sinj_norma",
        "iColumns": 9,
        "sColumns": ",,,,,,,,",
        "iDisplayStart": 0,
        "iDisplayLength": 10,
        "mDataProp_0": "_score",
        "sSearch_0": "",
        "bRegex_0": "false",
        "bSearchable_0": "true",
        "bSortable_0": "false",
        "mDataProp_1": "_score",
        "sSearch_1": "",
        "bRegex_1": "false",
        "bSearchable_1": "true",
        "bSortable_1": "true",
        "mDataProp_2": "nm_tipo_norma",
        "sSearch_2": "",
        "bRegex_2": "false",
        "bSearchable_2": "true",
        "bSortable_2": "true",
        "mDataProp_3": "dt_assinatura",
        "sSearch_3": "",
        "bRegex_3": "false",
        "bSearchable_3": "true",
        "bSortable_3": "true",
        "mDataProp_4": "origens",
        "sSearch_4": "",
        "bRegex_4": "false",
        "bSearchable_4": "true",
        "bSortable_4": "false",
        "mDataProp_5": "ds_ementa",
        "sSearch_5": "",
        "bRegex_5": "false",
        "bSearchable_5": "true",
        "bSortable_5": "false",
        "mDataProp_6": "nm_situacao",
        "sSearch_6": "",
        "bRegex_6": "false",
        "bSearchable_6": "true",
        "bSortable_6": "true",
        "mDataProp_7": 7,
        "sSearch_7": "",
        "bRegex_7": "false",
        "bSearchable_7": "true",
        "bSortable_7": "false",
        "mDataProp_8": 8,
        "sSearch_8": "",
        "bRegex_8": "false",
        "bSearchable_8": "true",
        "bSortable_8": "false",
        "sSearch": "",
        "bRegex": "false",
        "iSortCol_0": 1,
        "sSortDir_0": "desc",
        "iSortingCols": 1,
        "tipo_pesquisa": "norma",
        "all": "",
        "ch_tipo_norma": 46000000,
        "nm_tipo_norma": "Lei",
        "nr_norma": "",
        "ano_assinatura": 1800,
        "ch_orgao": "",
        "ch_hierarquia": "",
        "sg_hierarquia_nm_vigencia": "",
        "origem_por": "toda_a_hierarquia_em_qualquer_epoca1",
        "argumento": "autocomplete#ch_situacao#Situação#igual#igual a#sustado#Sustado(a)#E"
    }
    """

    def __init__(
        self,
        base_url: str = "https://www.sinj.df.gov.br/sinj",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "DISTRITO_FEDERAL"
        self.params = {
            "bbusca": "sinj_norma",
            "iColumns": 9,
            "sColumns": ",,,,,,,,",
            "iDisplayStart": 0,
            "iDisplayLength": 10,
            "mDataProp_0": "_score",
            "sSearch_0": "",
            "bRegex_0": "false",
            "bSearchable_0": "true",
            "bSortable_0": "false",
            "mDataProp_1": "_score",
            "sSearch_1": "",
            "bRegex_1": "false",
            "bSearchable_1": "true",
            "bSortable_1": "true",
            "mDataProp_2": "nm_tipo_norma",
            "sSearch_2": "",
            "bRegex_2": "false",
            "bSearchable_2": "true",
            "bSortable_2": "true",
            "mDataProp_3": "dt_assinatura",
            "sSearch_3": "",
            "bRegex_3": "false",
            "bSearchable_3": "true",
            "bSortable_3": "true",
            "mDataProp_4": "origens",
            "sSearch_4": "",
            "bRegex_4": "false",
            "bSearchable_4": "true",
            "bSortable_4": "false",
            "mDataProp_5": "ds_ementa",
            "sSearch_5": "",
            "bRegex_5": "false",
            "bSearchable_5": "true",
            "bSortable_5": "false",
            "mDataProp_6": "nm_situacao",
            "sSearch_6": "",
            "bRegex_6": "false",
            "bSearchable_6": "true",
            "bSortable_6": "true",
            "mDataProp_7": 7,
            "sSearch_7": "",
            "bRegex_7": "false",
            "bSearchable_7": "true",
            "bSortable_7": "false",
            "mDataProp_8": 8,
            "sSearch_8": "",
            "bRegex_8": "false",
            "bSearchable_8": "true",
            "bSortable_8": "false",
            "sSearch": "",
            "bRegex": "false",
            "iSortCol_0": 1,
            "sSortDir_0": "desc",
            "iSortingCols": 1,
            "tipo_pesquisa": "norma",
            "all": "",
            "ch_tipo_norma": 46000000,
            "nm_tipo_norma": "Lei",
            "nr_norma": "",
            "ano_assinatura": 1800,
            "ch_orgao": "",
            "ch_hierarquia": "",
            "sg_hierarquia_nm_vigencia": "",
            "origem_por": "toda_a_hierarquia_em_qualquer_epoca1",
            "argumento": "autocomplete#ch_situacao#Situação#igual#igual a#sustado#Sustado(a)#E",
        }
        self._initialize_saver()

    def _format_search_url(
        self, situation: str, norm_type_id: str, year: int, page: int = 1
    ) -> str:
        """Format url for search request"""
        self.params["nm_tipo_norma"] = norm_type_id
        self.params["ano_assinatura"] = year
        self.params["argumento"] = (
            f"autocomplete#ch_situacao#Situação#igual#igual a#{situation}#{situation}#E"
        )
        self.params["iDisplayLength"] = 100
        self.params["iDisplayStart"] = (page - 1) * self.params["iDisplayLength"]

        return f"{self.base_url}/ashx/Datatable/ResultadoDePesquisaNormaDatatable.ashx"

    def _get_docs_links(self, url: str) -> list:
        """Get document links from search request. Returns a list of dicts with keys 'title', 'summary', 'date', 'html_link'"""
        response = self._make_request(url, method="POST", json=self.params)
        if response is None:
            return []

        # https://www.sinj.df.gov.br/sinj/Norma/fdf64867a5154c31b1ebd7c141f716ab/Resolu_o_Ordin_ria_59_26_08_2020.html

        def transform_norm_type(norm_type: str) -> str:
            # change all special characters to _
            new_chars = []
            for char in norm_type:
                if char.isalnum():
                    new_chars.append(char)
                else:
                    new_chars.append("_")

            return "".join(new_chars)

        data = response.json()

        docs = []
        for item in data["aaData"]:
            item_info = item["_source"]
            title = item_info["nm_norma"]
            norm_number = item_info["nr_norma"]
            ch_norma = item_info["ch_norma"]
            norm_type = item_info["nm_tipo_norma"]
            dt_assinatura = item_info["dt_assinatura"]

            transformed_tipo_norma = transform_norm_type(norm_type)

            html_link = f"{self.base_url}{ch_norma}/{transformed_tipo_norma}_{norm_number}_{dt_assinatura.replace('/', '_')}.html"
            docs.append(
                {
                    "title": title,
                    "summary": item_info["ds_ementa"],
                    "date": dt_assinatura,
                    "html_link": html_link,
                }
            )

        return docs

    def _get_doc_data(self, doc_info: dict) -> list:
        """Get document data from html link"""

        # remove html link from doc_info
        html_link = doc_info.pop("html_link")
        response = self._make_request(html_link)

        soup = BeautifulSoup(response.content, "html.parser")

        # get id="div_texto"
        norm_text_tag = soup.find("div", id="div_texto")
        html_string = f"<html>{norm_text_tag.prettify()}</html>"

        buffer = BytesIO()
        buffer.write(html_string.encode())
        buffer.seek(0)

        # get markdown text
        text_markdown = self._get_markdown(stream=buffer)

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = html_link

        return doc_info

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for situation, situation_id in tqdm(
            self.situations.items(),
            desc="DISTRITO FEDERAL | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type in tqdm(
                self.types,
                desc=f"DISTRITO FEDERAL | Year: {year} | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):
                url = self._format_search_url(situation_id, norm_type, year)
                response = self._make_request(url, method="POST", json=self.params)

                data = response.json()

                # if iTotalRecords is None, then there are no norms for this year
                if not data["iTotalRecords"]:
                    continue

                total_norms = data["iTotalDisplayRecords"]

                pages = total_norms // self.params["iDisplayLength"]
                if total_norms % self.params["iDisplayLength"]:
                    pages += 1

                norms = []

                # get all norms
                with ThreadPoolExecutor() as executor:
                    futures = [
                        executor.submit(
                            self._get_docs_links,
                            self._format_search_url(
                                situation_id, norm_type, year, page
                            ),
                        )
                        for page in range(1, pages + 1)
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc="DISTRITO FEDERAL | Get document links",
                        total=len(futures),
                        disable=not self.verbose,
                    ):
                        result = future.result()
                        norms.extend(result)

                results = []

                # get all norm data
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, norm) for norm in norms
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc="DISTRITO FEDERAL | Get document data",
                        total=len(norms),
                        disable=not self.verbose,
                    ):
                        result = future.result()

                        # save to one drive
                        queue_item = {
                            "year": year,
                            "type": norm_type,
                            "situation": situation,
                            **result,
                        }

                        self.queue.put(queue_item)
                        results.append(queue_item)

                self.results.extend(results)
                self.count += len(results)

                if self.verbose:
                    print(
                        f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                    )
