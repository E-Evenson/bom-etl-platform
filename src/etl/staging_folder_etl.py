import logging

from bom_processing.extract.get_bom_paths import (
    scrape_bom_paths_from_staging_folder,
)
from bom_processing.orchestration.process_boms import process_boms
from bom_processing.load.load_to_sql import (
    delete_and_insert_to_sql,
    insert_uploads_into_history_table,
    refresh_final_current_bom_table,
)


logger = logging.getLogger(__name__)


def main():
    """
    Ingest and process validated files from Staging Folder
    Appends to historical BOM final table
    Overwrites current BOM final table
    """
    configure_logging()

    logger.info("Initializing ETL process for staging folder")

    bom_paths = scrape_bom_paths_from_staging_folder()

    primary_boms_df, secondary_boms_df = process_boms(
        bom_paths,
        "upload",
    )

    delete_and_insert_to_sql(
        "example_bom_staging", primary_boms_df
    )

    insert_uploads_into_history_table()

    for path in bom_paths:
        path = path["path"]
        try:
            path.unlink()
            logger.info(f"Deleting file from staging: {path.name}")
        except Exception as e:
            logger.warning(f"Could not delete {path.name}: {e}")

    refresh_final_current_bom_table()

    return


if __name__ == "__main__":
    from config.logging_config import configure_logging

    main()
