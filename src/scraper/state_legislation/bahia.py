import requests
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests.compat
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper

TYPES = {
    "Lei Complementar": 11,
    "Constituição Estadual 1967": 33,
    "Constituição Estadual 1947": 32,
    "Constituição Estadual 1935": 31,
    "Constituição Estadual 1891": 30,
    "Decreto": 2,
    "Decreto Financeiro": 1,
    "Decreto Simples": 3,
    "Emenda Constitucional": 4,
    "Lei Complementar": 5,
    "Lei Delegada": 6,
    "Lei Ordinária": 7,
    "Portaria Casa Civil": 19,
    "Portaria Conjunta Casa Civil": 20,
    "Instrução Normativa Casa Civil": 92,
}

VALID_SITUATIONS = [
    "Não consta"
]  # Conama does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class BahiaLegislaScraper(BaseScaper):
    """Webscraper for Bahia state legislation website (https://www.legislabahia.ba.gov.br/)

    Example search request: https://www.legislabahia.ba.gov.br/documentos?categoria%5B%5D=7&num=&ementa=&exp=&data%5Bmin%5D=2025-01-01&data%5Bmax%5D=2025-12-31&page=0
    """

    def __init__(
        self,
        base_url: str = "https://www.legislabahia.ba.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "BAHIA"
        self.params = {
            "categoria[]": "",
            "num": "",
            "ementa": "",
            "exp": "",
            "data[min]": "",
            "data[max]": "",
            "page": 0,
        }
        self._initialize_saver()

    def _format_search_url(self, norm_type_id: str, year: int, page: int) -> str:
        """Format url for search request"""
        self.params["categoria[]"] = norm_type_id
        self.params["data[min]"] = f"{year}-01-01"
        self.params["data[max]"] = f"{year}-12-31"
        self.params["page"] = page
        return f"{self.base_url}/documentos?{requests.compat.urlencode(self.params)}"

    def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'html_link'
        """
        soup = self._get_soup(url)

        #     <tbody>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23500-de-17-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23500-de-17-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.500 DE 17 DE FEVEREIRO DE 2025</a></b>
        #                           Dispõe sobre a organização e funcionamento das Câmaras de Prevenção e Resolução Administrativa de Conflitos da Administração Pública Estadual - CPRAC, no âmbito da Procuradoria Geral do Estado - PGE, na forma que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23459-de-13-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23459-de-13-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.459 DE 13 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de desapropriação, a área de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23458-de-13-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23458-de-13-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.458 DE 13 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de desapropriação, a área de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23457-de-13-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23457-de-13-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.457 DE 13 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de desapropriação, as áreas de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23456-de-13-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23456-de-13-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.456 DE 13 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de constituição de servidão administrativa, a área de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23455-de-13-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23455-de-13-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.455 DE 13 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de desapropriação, as áreas de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23454-de-13-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23454-de-13-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.454 DE 13 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de desapropriação, as áreas de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23453-de-13-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23453-de-13-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.453 DE 13 DE FEVEREIRO DE 2025</a></b>
        #                           Homologa o Decreto Municipal de "Situação de Emergência” que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23452-de-13-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23452-de-13-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.452 DE 13 DE FEVEREIRO DE 2025</a></b>
        #                           Homologa o Decreto Municipal de "Situação de Emergência” que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23451-de-13-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23451-de-13-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.451 DE 13 DE FEVEREIRO DE 2025</a></b>
        #                           Homologa o Decreto Municipal de "Situação de Emergência” que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23450-de-13-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23450-de-13-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.450 DE 13 DE FEVEREIRO DE 2025</a></b>
        #                           Homologa o Decreto Municipal de "Situação de Emergência” que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23449-de-13-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23449-de-13-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.449 DE 13 DE FEVEREIRO DE 2025</a></b>
        #                           Altera o quadro de cargos em comissão da Polícia Civil do Estado da Bahia - PCBA, na forma que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23448-de-12-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23448-de-12-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.448 DE 12 DE FEVEREIRO DE 2025</a></b>
        #                           Prorroga o prazo da Declaração do Estado de Emergência Zoossanitária em todo território baiano, para fins de prevenção da Influenza Aviária H5N1 de Alta Patogenicidade - IAAP, conforme disposto no Decreto nº 22.174, de 21 de…
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23447-de-11-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23447-de-11-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.447 DE 11 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de desapropriação, a área de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23446-de-11-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23446-de-11-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.446 DE 11 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de constituição de servidão administrativa, a área de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23445-de-11-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23445-de-11-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.445 DE 11 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de desapropriação, a área de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23444-de-11-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23444-de-11-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.444 DE 11 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de desapropriação, a área de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23443-de-11-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23443-de-11-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.443 DE 11 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de desapropriação, a área de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23442-de-11-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23442-de-11-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.442 DE 11 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de desapropriação, a área de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #       <tr>
        #                         <td headers="view-title-table-column"><span>
        #                           <a href="/documentos/decreto-no-23441-de-11-de-fevereiro-de-2025" target="_blank"><b class="clearfix"></b></a><b class="clearfix"><a href="/documentos/decreto-no-23441-de-11-de-fevereiro-de-2025" hreflang="pt-br" target="_blank">DECRETO Nº 23.441 DE 11 DE FEVEREIRO DE 2025</a></b>
        #                           Declara de utilidade pública, para fins de desapropriação, a área de terra que indica.
        #                         </span>          </td>
        #                                                                                     <td headers="view-field-categoria-doc-table-column" class="views-field views-field-field-categoria-doc views-align-left">Decretos Numerados          </td>
        #           </tr>
        #   </tbody>

        docs = []

        # <tr class="odd">
        #                     <td colspan="2" class="views-empty"><center>Nenhum resultado encontrado</center>          </td>
        #       </tr>

        # check if the page is empty ("Nenhum resultado encontrado")
        if soup.find("td", class_="views-empty"):
            return []

        items = soup.find("tbody").find_all("tr")

        for item in items:
            tds = item.find_all("td")
            if len(tds) != 2:
                continue

            title = tds[0].text.strip()
            html_link = tds[0].find("a")["href"]

            docs.append(
                {
                    "title": title,
                    "html_link": html_link,
                }
            )

        return docs

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")
        url = requests.compat.urljoin(self.base_url, html_link)

        response = self._make_request(url)
        soup = BeautifulSoup(response.content, "html.parser")

        # get norm_number, date, publication_date and summary
        norm_number = soup.find("div", class_="field--name-field-numero-doc").find(
            "div", class_="field--item"
        )
        if norm_number:
            norm_number = norm_number.text.strip()

        date = soup.find("div", class_="field--name-field-data-doc").find(
            "div", class_="field--item"
        )
        if date:
            date = date.text.strip()

        publication_date = soup.find(
            "div", class_="field--name-field-data-de-publicacao-no-doe"
        ).find("div", class_="field--item")
        if publication_date:
            publication_date = publication_date.text.strip()

        summary = soup.find("div", class_="field--name-field-ementa").find(
            "div", class_="field--item"
        )
        if summary:
            summary = summary.text.strip()

        # get html string and text markdown
        # class="visivel-separador field field--name-body field--type-text-with-summary field--label-hidden field--item"
        norm_text_tag = soup.find("div", class_="field--name-body")
        html_string = f"<html>{norm_text_tag.prettify()}</html>"

        buffer = BytesIO()
        buffer.write(html_string.encode())
        buffer.seek(0)

        text_markdown = self._get_markdown(stream=buffer)

        doc_info["norm_number"] = norm_number
        doc_info["date"] = date
        doc_info["publication_date"] = publication_date
        doc_info["summary"] = summary
        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url

        return doc_info

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for situation in tqdm(
            self.situations,
            desc="BAHIA | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc=f"BAHIA | Year: {year} | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):
                url = self._format_search_url(norm_type_id, year, 0)
                soup = self._get_soup(url)

                # get total pages
                pagination = soup.find("ul", class_="pagination js-pager__items")
                if pagination:
                    pages = pagination.find_all("li")
                    last_page = pages[-1].find("a")["href"]
                    total_pages = int(last_page.split("page=")[-1])
                else:
                    total_pages = 1

                # Get documents html links
                documents = []
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(
                            self._get_docs_links,
                            self._format_search_url(norm_type_id, year, page),
                        )
                        for page in range(total_pages)
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        total=total_pages,
                        desc="BAHIA | Get document link",
                        disable=not self.verbose,
                    ):
                        docs = future.result()
                        if docs:
                            documents.extend(docs)

                # Get document data
                results = []
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, doc) for doc in documents
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        total=len(documents),
                        desc="BAHIA | Get document data",
                        disable=not self.verbose,
                    ):
                        result = future.result()
                        if result is None:
                            continue

                        # save to one drive
                        queue_item = {
                            "year": year,
                            # hardcode since we only get valid documents in search request
                            "situation": situation,
                            "type": norm_type,
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
