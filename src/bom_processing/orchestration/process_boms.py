from datetime import datetime, timezone
import logging

import pandas as pd

from bom_processing.constants import REQUIRED_SQL_COLUMNS
from bom_processing.extract.get_bom_paths import (
    scrape_bom_paths_from_design_directory,
)
from bom_processing.extract.read_boms_from_excel import extract_bom_data
from bom_processing.transform.transformations import transform_bom
from bom_processing.validation.column_validation import (
    validate_required_columns,
    ValidationError,
)
from config.logging_config import configure_logging


logger = logging.getLogger(__name__)


def _get_snapshot_time():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def add_metadata(
    df: pd.DataFrame,
    pon: str,
    category: str,
    load_method: str,
    filename: str,
    uploader: str,
) -> pd.DataFrame:
    logger.debug(f"Adding metadata to {filename} for PON {pon}")
    df["pon"] = str(pon)
    df["material_category"] = category
    df["load_method"] = load_method
    df["bom_filename"] = filename
    df["uploaded_by"] = uploader

    return df


def process_boms(
    bom_records: list[dict],
    load_method: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Takes in list of BOM records with metadata
    Reads, validates, cleans, transforms, and re-validates
    Adds metadata
    """
    primary_boms = []
    secondary_boms = []

    success_count = 0
    failure_count = 0

    total_boms = len(bom_records)

    logger.info(f"Running ETL process for {total_boms} BOMs")

    for record in bom_records:
        bom_path = record["path"]
        pon = record["pon"]
        uploaded_by = record["username"]
        logger.info(f"Processing BOM: {bom_path.name}")

        try:
            bom_dict = extract_bom_data(bom_path)
        except ValidationError as ve:
            bom_dict = None
            logger.warning(f"Skipping BOM due to validation error: {ve}")
            failure_count += 1
            continue
        except ValueError as ve:
            bom_dict = None
            logger.warning(f"Skipping BOM due to value error: {ve}")
            failure_count += 1
            continue
        except Exception as e:
            bom_dict = None
            logger.warning(f"Skipping BOM due to validation error: {e}")
            failure_count += 1
            continue

        if bom_dict is None:
            logger.warning(
                f"Skipping BOM due to extraction failure: {bom_path.name}"
            )
            failure_count += 1
            continue

        bom_data = bom_dict["df"]
        category = bom_dict["category"]

        transformed_df = transform_bom(bom_data, category)
        transformed_df = add_metadata(
            transformed_df,
            pon,
            category,
            load_method,
            bom_path.name,
            uploaded_by,
        )

        if category in ("timber", "sheet"):
            primary_boms.append(transformed_df)
        elif category == "metal":
            secondary_boms.append(transformed_df)

        success_count += 1

    primary_columns = REQUIRED_SQL_COLUMNS["primary"]
    primary_boms_df = (
        pd.concat(primary_boms).reset_index(drop=True)
        if primary_boms
        else pd.DataFrame(columns=primary_columns)
    )

    secondary_boms_df = (
        pd.concat(secondary_boms).reset_index(drop=True)
        if secondary_boms
        else pd.DataFrame()
    )

    snapshot_time = _get_snapshot_time()
    if not primary_boms_df.empty:
        primary_boms_df["snapshot_time_utc"] = snapshot_time
        validate_required_columns(
            primary_boms_df, set(primary_columns), "post transform"
        )
        # reorder columns to match SQL table
        primary_boms_df = primary_boms_df[primary_columns]

    if not secondary_boms_df.empty:
        secondary_boms_df["snapshot_time_utc"] = snapshot_time

    logger.info(f"BOM processing snapshot time: {snapshot_time}")
    logger.info(
        f"Finished processing {total_boms} BOMs: {success_count} succeeded, {failure_count} failed."
    )

    return primary_boms_df, secondary_boms_df


if __name__ == "__main__":
    configure_logging()
    bom_paths = scrape_bom_paths_from_design_directory()
    processed = process_boms(bom_paths, "test")
    print(processed)
