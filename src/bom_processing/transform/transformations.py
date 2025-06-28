import logging
from pathlib import Path

import pandas as pd

from bom_processing.extract.read_boms_from_excel import extract_bom_data
from config.logging_config import configure_logging


logger = logging.getLogger(__name__)


def _drop_unneeded_columns(bom_df: pd.DataFrame) -> pd.DataFrame:
    columns_to_keep = [
        "part_tag",
        "quantity",
        "material_type",
        "material_subtype",
        "designation",
        "height",
        "width",
        "length",
        "usage_quantity",
        "finish_quantity",
        "element",
        "additional_info",
    ]

    bom_df = bom_df[columns_to_keep]

    return bom_df


def _aggregate_bom_data(
    bom_df: pd.DataFrame,
    columns_to_group_by: list,
) -> pd.DataFrame:
    sum_columns = [
        "quantity",
        "usage_quantity",
        "finish_quantity",
    ]
    non_sum_columns = [col for col in bom_df.columns if col not in sum_columns]

    aggregated_df = bom_df.groupby(columns_to_group_by, as_index=False)[
        sum_columns
    ].sum()

    retained_columns = bom_df.groupby(columns_to_group_by, as_index=False)[
        non_sum_columns
    ].first()

    final_aggregated_df = pd.merge(
        aggregated_df, retained_columns, on=columns_to_group_by, how="left"
    )

    return final_aggregated_df


def _transform_primary_a_bom(primary_a_df: pd.DataFrame) -> pd.DataFrame:
    primary_a_df = _drop_unneeded_columns(primary_a_df)
    primary_a_df = primary_a_df.round(
        {
            "width": 0,
            "height": 0,
            "length": 0,
        }
    )
    primary_a_df[["width", "height", "length"]] = primary_a_df[
        ["width", "height", "length"]
    ].astype(int)

    primary_a_df["usage_quantity"] = round(
        (primary_a_df["quantity"] * primary_a_df["length"]) / 1000, 1
    )

    # ensure height is smaller than width
    primary_a_df[["height", "width"]] = primary_a_df[["height", "width"]].apply(
        lambda row: sorted(row), axis=1, result_type="expand"
    )

    # standardize material types and subtypes
    # for Douglas Fir Glulam
    primary_a_df.loc[
        primary_a_df["material_type"].str.contains(
            "type-A-X", case=False, na=False
        )
        | primary_a_df["material_subtype"].str.contains(
            "type-A-X", case=False, na=False
        ),
        ["material_type", "material_subtype"],
    ] = ["TYPE-A", "TYPE-A-X"]

    # for standard SPF glulam (excluding Douglas Fir Glulam)
    primary_a_df.loc[
        primary_a_df["material_type"].str.contains("type-a", case=False, na=False)
        & ~primary_a_df["material_subtype"].str.contains(
            "type-A-X", case=False, na=False
        ),
        ["material_type", "material_subtype"],
    ] = ["TYPE-A", "TYPE-A-S"]

    # for RBK
    primary_a_df.loc[
        primary_a_df["material_type"].str.contains("type-b", case=False, na=False),
        ["material_type", "material_subtype"],
    ] = ["TYPE-B", "TYPE-B-S"]

    # for dimensional lumber (e.g. 2x4, 2x6, etc.) to be material_type "Lumber"
    primary_a_df.loc[
        primary_a_df["material_type"].str.contains(
            r"^\d+x\d+$", case=False, na=False
        ),
        "material_type",
    ] = "TYPE-C"

    columns_to_group_by = [
        "part_tag",
        "material_type",
        "material_subtype",
        "height",
        "width",
        "length",
    ]
    primary_a_df = _aggregate_bom_data(primary_a_df, columns_to_group_by)

    return primary_a_df


def _transform_primary_b_bom(primary_b_df: pd.DataFrame) -> pd.DataFrame:
    primary_b_df = _drop_unneeded_columns(primary_b_df)

    primary_b_df = primary_b_df.round(
        {
            "height": 0,
            "width": 0,
            "length": 0,
        }
    )
    primary_b_df[["width", "height", "length"]] = primary_b_df[
        ["width", "height", "length"]
    ].astype(int)

    # total finish only measures one side of piece, so multiply by 2
    primary_b_df["finish_quantity"] = primary_b_df["finish_quantity"] * 2

    # standardizing material_types and subtypes
    # for composite material A
    primary_b_df.loc[
        primary_b_df["material_type"].str.contains(
            "composite-a", case=False, na=False
        ),
        ["material_type", "material_subtype"],
    ] = ["COMPOSITE-A", "COMPOSITE-A-S"]

    # for composite material B
    primary_b_df.loc[
        primary_b_df["material_type"].str.contains("composite-b", case=False, na=False),
        ["material_type", "material_subtype"],
    ] = ["COMPOSITE-B", "COMPOSITE-B-S"]

    # for plain material
    primary_b_df.loc[
        primary_b_df["material_type"].str.contains("plain", case=False, na=False),
        "material_type",
    ] = "PLAIN"

    # Find inserts and standardize designation column for them
    primary_b_df.loc[
        primary_b_df["designation"].str.contains("insert", case=False, na=False),
        "designation",
    ] = "INSERT"

    columns_to_group_by = [
        "part_tag",
        "material_type",
        "material_subtype",
        "height",
        "width",
        "length",
    ]

    primary_b_df = _aggregate_bom_data(primary_b_df, columns_to_group_by)

    return primary_b_df


def transform_bom(bom_df: pd.DataFrame, bom_category: str) -> pd.DataFrame:
    logger.debug(f"Starting transformation for {bom_category} BOM.")
    if bom_category == "primary_a":
        transformed_df = _transform_primary_a_bom(bom_df)
    elif bom_category == "primary_b":
        transformed_df = _transform_primary_b_bom(bom_df)
    else:
        transformed_df = pd.DataFrame()

    logger.debug(f"Transformed {bom_category} BOM.")

    return transformed_df


if __name__ == "__main__":
    configure_logging()
    primary_a_bom = extract_bom_data(
        Path.cwd() / "tests/data/test_timber_list.xls"
    )

    if primary_a_bom is not None:
        primary_a_bom_transformed = transform_bom(
            primary_a_bom["df"], primary_a_bom["category"]
        )
        print(primary_a_bom_transformed)
