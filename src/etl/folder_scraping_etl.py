import logging

from bom_processing.extract.get_bom_paths import (
    scrape_bom_paths_from_design_directory,
)
from bom_processing.orchestration.process_boms import process_boms
from bom_processing.load.load_to_sql import (
    delete_and_insert_to_sql,
    refresh_final_bom_table,
)


logger = logging.getLogger(__name__)


def main():
    """
    Ingest and process BOM files scraped from Design Active Projects folder
    Overwrite BOM final table
    """
    configure_logging()

    logger.info("Initializing ETL process to scrape design folder")

    bom_paths = scrape_bom_paths_from_design_directory()

    primary_boms_df, secondary_boms_df = process_boms(
        bom_paths,
        "full",
    )

    delete_and_insert_to_sql(
        "example_bom_staging", primary_boms_df
    )
    refresh_final_bom_table()

    return


if __name__ == "__main__":
    from config.logging_config import configure_logging

    main()
