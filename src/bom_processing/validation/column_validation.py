from typing import Optional
import pandas as pd


class ValidationError(Exception):
    pass


class MissingColumnError(ValidationError):
    pass


class MissingValueError(ValidationError):
    pass


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: set,
    stage: str,
    reverse_renaming: Optional[dict[str, str]] = None,
):
    """
    Ensure all required columns are present

    Raises:
        MissingColumnError: if any required columns are missing
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        if reverse_renaming:
            missing = [reverse_renaming.get(col, col) for col in missing]
        raise MissingColumnError(
            f"Missing columns at {stage} stage: {missing}"
        )


def validate_non_null_columns(
    df: pd.DataFrame,
    nullable_columns: dict[str, bool],
    stage: str,
    reverse_renaming: Optional[dict[str, str]] = None,
):
    """
    Ensure non-null columns do not contain any null values

    Raises:
        MissingValueError: if any required values are missing
    """
    missing = [
        col
        for col, nullable in nullable_columns.items()
        if not nullable and (col not in df.columns or df[col].isnull().any())
    ]

    if missing:
        if reverse_renaming:
            missing = [reverse_renaming.get(col, col) for col in missing]
        raise MissingValueError(
            f"Missing values at {stage} stage. The following columns are missing values: {missing}"
        )
