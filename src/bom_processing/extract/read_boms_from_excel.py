import logging
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

from bom_processing.constants import (
    COLUMN_RENAME_MAPS,
    REQUIRED_COLUMNS_AFTER_RENAME,
    NUMERIC_AS_STRING_COLUMNS,
    EXPECTED_DTYPES_CLEANING,
    NULLABLE_COLUMNS,
)
from bom_processing.validation.column_validation import (
    validate_required_columns,
    validate_non_null_columns,
    ValidationError,
)


logger = logging.getLogger(__name__)


class BOMTypeError(Exception):
    pass


def _identify_bom_category(bom_df: pd.DataFrame) -> Optional[str]:
    """
    Identify the type of BOM based on the presence of specific columns
    Args:
        bom_df: Dataframe representing the BOM
    Return:
        str | None: The BOM type (e.g. primary_a, primary_b, secondary, unknown)
    """
    bom_type = None
    if "H [mm]" in bom_df.columns:
        bom_type = "primary_a"
    elif "T [mm]" in bom_df.columns:
        bom_type = "primary_b"
    elif "Dia [mm]" in bom_df.columns:
        bom_type = "secondary"

    logger.debug(f"BOM type: {bom_type}")
    return bom_type


def is_summary_row(row: pd.Series) -> bool:
    blank_columns_in_summary_rows = [
        "part_tag",
        "height",
        "width",
        "length",
    ]
    return all(pd.isna(row[col]) for col in blank_columns_in_summary_rows)


def assign_dtypes(
    df: pd.DataFrame,
    expected_dtypes: dict,
    numeric_as_string: list,
    reverse_renaming: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    for col, dtype in expected_dtypes.items():
        if col not in df.columns:
            raise KeyError(f"Expected column '{col}' not found in BOM")

        try:
            if dtype == "string":
                if col in numeric_as_string and col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="raise").astype(
                        "Int64"
                    )
                else:
                    df[col] = df[col].astype("string")
            else:
                if dtype == "int":
                    df[col] = (
                        pd.to_numeric(df[col], errors="raise")
                        .apply(np.ceil)
                        .astype("int")
                    )
                elif dtype == "Float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(
                        "Float64"
                    )
                else:
                    df[col] = df[col].astype(dtype)
        except Exception as e:
            if reverse_renaming:
                col = reverse_renaming.get(col, col)
            raise ValueError(
                f"Failed to convert column '{col}' to {dtype}: {e}"
            )
    return df


def clean_primary_a_bom(primary_a_df: pd.DataFrame) -> pd.DataFrame:
    column_renaming = COLUMN_RENAME_MAPS["primary_a"]
    reverse_renaming = {v: k for k, v in column_renaming.items()}
    primary_a_df = primary_a_df.rename(columns=column_renaming)

    required_columns = REQUIRED_COLUMNS_AFTER_RENAME["primary"]
    validate_required_columns(
        primary_a_df,
        required_columns,
        stage="cleaning",
        reverse_renaming=reverse_renaming,
    )

    primary_a_df = primary_a_df[~primary_a_df.apply(is_summary_row, axis=1)]

    nullable_columns = NULLABLE_COLUMNS["primary"]
    validate_non_null_columns(
        primary_a_df,
        nullable_columns,
        stage="cleaning",
        reverse_renaming=reverse_renaming,
    )

    expected_dtypes = EXPECTED_DTYPES_CLEANING["primary"]
    numeric_strings = NUMERIC_AS_STRING_COLUMNS["primary"]
    primary_a_df = assign_dtypes(
        primary_a_df, expected_dtypes, numeric_strings, reverse_renaming
    )

    return primary_a_df


def clean_primary_b_bom(primary_b_df: pd.DataFrame) -> pd.DataFrame:
    column_renaming = COLUMN_RENAME_MAPS["primary_b"]
    reverse_renaming = {v: k for k, v in column_renaming.items()}
    primary_b_df = primary_b_df.rename(columns=column_renaming)

    required_columns = REQUIRED_COLUMNS_AFTER_RENAME["primary"]
    validate_required_columns(
        primary_b_df,
        required_columns,
        "cleaning",
        reverse_renaming=reverse_renaming,
    )

    primary_b_df = primary_b_df[~primary_b_df.apply(is_summary_row, axis=1)]

    nullable_columns = NULLABLE_COLUMNS["primary"]
    validate_non_null_columns(
        primary_b_df,
        nullable_columns,
        stage="cleaning",
        reverse_renaming=reverse_renaming,
    )

    expected_dtypes = EXPECTED_DTYPES_CLEANING["primary"]
    numeric_strings = NUMERIC_AS_STRING_COLUMNS["primary"]
    primary_b_df = assign_dtypes(
        primary_b_df, expected_dtypes, numeric_strings, reverse_renaming
    )

    return primary_b_df


def clean_secondary_bom(metal_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Secondary BOM processing not yet implemented")
    return metal_df


def extract_bom_data(bom_path: Path) -> Optional[dict[str, Any]]:
    """
    Extract and perform basic cleaning for a single BOM

    Args:
        bom_path (Path): A single BOM file path.

    Return:
        dict[str, Any] | None: Dictionary containing cleaned BOM and category, or None if extraction fails
    """
    logger.debug(f"Extracting BOM {bom_path.name}")

    try:
        bom_df = pd.read_excel(bom_path)

        bom_category = _identify_bom_category(bom_df)

        if bom_category is None:
            raise BOMTypeError(f"Unknown BOM category for: {bom_path.name}")

        if bom_category == "primary_a":
            cleaned_bom = clean_primary_a_bom(bom_df)
        elif bom_category == "primary_b":
            cleaned_bom = clean_primary_b_bom(bom_df)
        elif bom_category == "secondary":
            cleaned_bom = clean_secondary_bom(bom_df)
        else:
            logger.warning(f"Unknown BOM category for {bom_path.name}")
            raise BOMTypeError(f"Unhandled BOM category for: {bom_path.name}")

        bom_dict = {"df": cleaned_bom, "category": bom_category}

        return bom_dict

    except ValidationError as ve:
        logger.debug(f"{bom_path.name}: Validation error: {ve}")
        raise ve
    except KeyError as ke:
        logger.debug(f"{bom_path.name}: Missing column: {ke}")
        raise ke
    except ValueError as ve:
        logger.debug(f"{bom_path.name}: Dtype conversion error: {ve}")
        raise ve
    except Exception as e:
        logger.warning(f"{bom_path.name}: Unexpected error: {e}")
        raise e

