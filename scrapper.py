import requests
from bs4 import BeautifulSoup as soup
from pydantic import BaseModel as _BaseModel, Field, ConfigDict
from typing import Iterable, Any
import pandas as pd
import re
from tqdm.auto import trange


class BaseModel(_BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class CardData(BaseModel):
    ies: str = Field(alias="IES")
    nome: str = Field(alias="NOME/CURSO")
    modalidade: str = Field(alias="MODALIDADE")
    verbete: str = Field(alias="VERBETE")
    titulacao: str = Field(alias="TITULAÇÃO")
    campus: str = Field(alias="CAMPUS")
    categoria: str = Field(alias="CATEGORIA")
    duracao: str = Field(alias="DURAÇÃO")
    endereco: str | None = Field(None, alias="ENDEREÇO")
    site: str = Field(alias="SITE")
    telefone: str = Field(alias="TELEFONE")
    avaliacao: int = Field(alias="AVALIAÇÃO")
    cidade: str | None = Field(None, alias="CIDADE")
    estado: str | None = Field(None, alias="ESTADO")
    ano_avaliação: int = Field(alias="ANO DE AVALIAÇÃO")


def get_page_from_url(url: str, **params) -> soup:
    result = requests.get(url, params)
    return soup(result.content, "html.parser")


def get_page_by_year(year: int) -> soup:
    return get_page_from_url(
        "https://publicacoes.estadao.com.br/guia-da-faculdade",
        post_type=f"faculdades_{year}",
        ano=year,
    )


def process_card(card: soup, year: int) -> CardData:
    header = card.find("div", class_="box-basico")
    body = card.find("div", class_="box-completo")
    data: dict[str, Any] = {"ano_avaliação": year}

    # body
    field_order = [
        "ies",
        "nome",
        "modalidade",
        "verbete",
        "titulacao",
        "campus",
        "categoria",
        "duracao",
        "site",
        "telefone",
    ]

    if "ead" not in body.text.lower():
        field_order.insert(8, "endereco")

    space_regex = re.compile(" +")
    comma_regex = re.compile(", *,")
    for field, p_elem in zip(field_order, body.find_all("p")):  # type: ignore
        p_elem.find("span").decompose()
        content = p_elem.text.strip().replace("\n", " ")
        content = space_regex.sub(" ", content)
        content = comma_regex.sub(",", content)
        data[field] = content

    # header
    data["avaliacao"] = len(header.find_all("img", class_="estrela"))  # type: ignore
    if data["modalidade"].lower() != "ead":
        cidade, uf = header.find_all("p")[1].text.rsplit("|", maxsplit=1)[-1].rsplit("-", maxsplit=1)  # type: ignore
        data["cidade"] = cidade.strip()
        data["estado"] = uf.strip()

    return CardData.model_validate(data)


def get_page_count(page: soup) -> int:
    return int(page.find_all("a", class_="page-numbers")[-2].text.replace(".", ""))


def process_page(page: soup, year: int) -> Iterable[CardData]:
    cards_list = page.find_all("div", class_="box-listagem")
    for card in cards_list:
        yield process_card(card, year)


def get_next_page_link(page: soup) -> str | None:
    link = page.find("a", class_="next")
    if link:
        return link.get("href")
    return None


def scrapper(start_year: int, end_year: int, *, output_file: str):
    df = pd.DataFrame()
    for year in trange(start_year, end_year + 1, desc="year loop"):
        page = get_page_by_year(year)

        page_count = get_page_count(page)
        for _ in trange.get(page_count, desc="page loop"):
            next_page_link = get_next_page_link(page)

            cards_iter = process_page(page, year)
            page_records = []
            for card in cards_iter:
                page_records.append(card.model_dump(by_alias=True))

            df = pd.concat([df, pd.DataFrame(page_records)], ignore_index=True)

            if not next_page_link:
                break
            page = get_page_from_url(next_page_link)

    df.to_csv(output_file)


if __name__ == "__main__":
    scrapper(2020, 2023, output_file="xorume.csv")
