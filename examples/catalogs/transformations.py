import json
import pandas as pd


"""
===== NAMING CONVENTION =====

df__***             - Pandas dataframe;
df_pl__***          - Polars dataframe;
filepath__***       - string with filepath;
meta__***           - meta-information about output data;

***__input__***     - input object;
***__prev__***      - previous chain object;
***__output__***    - output object.
***__data__***      - data object.
"""


def parse_cars(df__input__cars_scanned: pd.DataFrame) -> pd.DataFrame:
    list_output = []

    for _, row in df__input__cars_scanned.iterrows():
        with open(row["filepath"], "r") as file:
            json_record = json.load(file)

        list_output.append(
            {
                "file_name": row["file_name"],
                "model_id": json_record["model_id"],
                "manufacture_country": json_record["manufacture_country"],
                "color": json_record["color"],
                "price": json_record["price"],
                "year": json_record["year"],
                "new": json_record["new"],
            },
        )
    
    df__output__cars_parsed = pd.DataFrame(list_output)
    
    df__output__cars_parsed["new"] = df__output__cars_parsed["new"].astype(bool)

    return df__output__cars_parsed


def agg__price_by_manufacture_country(
        df__input__cars_parsed: pd.DataFrame,
) -> pd.DataFrame:
    
    # Selecting required data
    df__input__cars_parsed = df__input__cars_parsed[
        ["manufacture_country", "price"]
    ]

    # Processing
    df__output__price_by_manufacture_country = (
        df__input__cars_parsed
        .groupby("manufacture_country", as_index=False)
        .sum()
    )

    return df__output__price_by_manufacture_country


def agg__price_by_color(
        df__input__cars_parsed: pd.DataFrame,
) -> pd.DataFrame:
    
    # Selecting required data
    df__input__cars_parsed = df__input__cars_parsed[
        ["color", "price"]
    ]

    # Processing
    df__output__price_by_color = (
        df__input__cars_parsed
        .groupby("color", as_index=False)
        .sum()
    )

    return df__output__price_by_color


def agg__price_by_manufacture_country_and_color(
        df__input__cars_parsed: pd.DataFrame,
) -> pd.DataFrame:
    
    # Selecting required data
    df__input__cars_parsed = df__input__cars_parsed[
        ["manufacture_country", "color", "price"]
    ]

    # Processing
    df__output__price_by_manufacture_country_and_color = (
        df__input__cars_parsed
        .groupby(["manufacture_country", "color"], as_index=False)
        .sum()
    )

    return df__output__price_by_manufacture_country_and_color


def generate__cars_docs(
        df__input__cars_parsed: pd.DataFrame,
        docs: list,
) -> pd.DataFrame:
    
    # Selecting required data
    df__input__cars_parsed = df__input__cars_parsed[["file_name"]]

    # Processing
    df__output__cars_docs = df__input__cars_parsed.join(
        pd.DataFrame(
            {
                "doc_name": docs,
                "created_at": pd.Timestamp.now(),
            }
        ),
        on=None,
        how="cross",
    )

    return df__output__cars_docs


def parse_countries(df__input__countries_scanned: pd.DataFrame) -> pd.DataFrame:
    list_output = []

    for _, row in df__input__countries_scanned.iterrows():
        with open(row["filepath"], "r") as file:
            json_record = json.load(file)

        list_output.append(
            {
                "file_name": row["file_name"],
                "manufacture_country": json_record["country"],
                "vat": json_record["vat"],
            },
        )
    
    df__output__countries_parsed = pd.DataFrame(list_output)
    
    return df__output__countries_parsed


def generate__cars_price_with_vat(
        df__input__cars_parsed: pd.DataFrame,
        df__input__countries_parsed: pd.DataFrame,
) -> pd.DataFrame:
    
    # Selecting required data
    df__input__cars_parsed = df__input__cars_parsed[
        ["file_name", "manufacture_country", "price"]
    ]

    df__input__countries_parsed = df__input__countries_parsed[
        ["manufacture_country", "vat"]
    ]

    # Processing
    df__output__cars_price_with_vat = df__input__cars_parsed.merge(
        df__input__countries_parsed,
        on=["manufacture_country"],
        how="left",
    )

    df__output__cars_price_with_vat["price_with_vat"] = (
        df__output__cars_price_with_vat["price"] * (1 + df__output__cars_price_with_vat["vat"])
    )

    df__output__cars_price_with_vat.drop(columns=["vat"], inplace=True)

    return df__output__cars_price_with_vat
