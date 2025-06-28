from importlib import resources
import logging
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

from config.config import DB_HOST, DB_NAME, DB_SCHEMA, DB_USER, DB_PASS

logger = logging.getLogger(__name__)


def _get_db_connection() -> sa.engine.Connection:
    """
    Creates and returns a connection to the database using environment variables
    """
    driver = "ODBC Driver 18 for SQL Server"

    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Missing database connection environment variables.")
        raise ValueError("Missing database connection environment variables.")

    logger.info(
        f"Attempting to connect to {DB_NAME} on {DB_HOST}"
    )
    connection_string = f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?TrustServerCertificate=yes&Driver={driver}"

    try:
        engine = sa.create_engine(connection_string)
        conn = engine.connect()
        logger.info(f"Successfully connected to {DB_NAME} on {DB_HOST}.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to the database {str(e)}")
        raise


def clear_table(table_name: str, conn: sa.engine.Connection) -> None:
    """
    Deletes all rows in existing table_name in SQL Server

    Args:
        table_name (str): table to load the BOM df into
        conn : SQL Alchemy connection to Database
    """
    logger.info(f"Deleting data from {DB_SCHEMA}.{table_name}")
    if not DB_SCHEMA:
        raise ValueError(
            "Missing DB_SCHEMA environment variable. Please set the schema for the database."
        )

    try:
        conn.execute(sa.text(f"DELETE FROM {DB_SCHEMA}.{table_name}"))
        conn.commit()
        logger.info(f"Successfully cleared {DB_SCHEMA}.{table_name}")
    except sa.exc.SQLAlchemyError as e:
        logger.error(f"Delete failed for {DB_SCHEMA}.{table_name}: {e}")
        raise


def load_df_to_sql(
    table_name: str, bom_df: pd.DataFrame, conn: sa.engine.Connection
) -> None:
    """
    Appends bom_df into table_name. Assumes columns have been validated

    Args:
        table_name (str): table to load the BOM df into
        bom_df (pd.DataFrame, optional): df to load into table_name
        conn : SQL Alchemy connection to Database
    """

    logger.info(
        f"Saving BOMs to {DB_SCHEMA}.{table_name} in {DB_NAME} on {DB_HOST}"
    )

    if not DB_SCHEMA:
        raise ValueError(
            "Missing DB_SCHEMA environment variable. Please set the schema for the database."
        )

    try:
        bom_df.to_sql(
            table_name,
            con=conn,
            schema=DB_SCHEMA,
            if_exists="append",
            index=False,
        )
        logger.info(f"Data loaded in to {DB_SCHEMA}.{table_name}")

    except sa.exc.SQLAlchemyError as e:
        logger.error(
            f"Error loading data into {DB_SCHEMA}.{table_name}: {str(e)}"
        )
        raise


def delete_and_insert_to_sql(table_name: str, bom_df: pd.DataFrame) -> None:
    """
    Assumes df columns have been validated

    Args:
        table_name (str): table to load the BOM df into
        bom_df (pd.DataFrame, optional): df to load into table_name
    """
    with _get_db_connection() as conn:
        clear_table(table_name, conn)
        load_df_to_sql(table_name, bom_df, conn)


def refresh_final_bom_table():
    """
    Refreshes the forecast_timber_bom_final table from the view by deleting
    existing rows and inserting fresh data.

    Uses an SQL script to delete and insert the most recent BOM data.
    For a full table refresh only
    """
    logger.info(
        f"Refreshing final table in {DB_HOST}: {DB_SCHEMA}.example_bom_final"
    )

    try:
        with (
            resources.files("bom_processing.sql")
            .joinpath("refresh_example_final_table.sql")
            .open("r") as file
        ):
            sql_refresh_query = file.read()
        with _get_db_connection() as conn:
            conn.execute(sa.text(sql_refresh_query))
            conn.commit()

        logger.info(
            f"Final table refreshed in {DB_SCHEMA}.example_bom_final"
        )

    except Exception as e:
        logger.error(f"Error refreshing final table: {e}")
        raise

    return


def insert_uploads_into_history_table():
    """
    Inserts data from the view into the historic final BOM table

    Uses an SQL script to insert the most recent processed uploaded BOMs.
    For user uploaded BOMs only
    """
    logger.info(
        f"Inserting staging data into {DB_HOST}: {DB_SCHEMA}.example_bom_final_history"
    )

    try:
        with (
            resources.files("bom_processing.sql")
            .joinpath("insert_staging_into_history_table.sql")
            .open("r") as file
        ):
            sql_refresh_query = file.read()
        with _get_db_connection() as conn:
            conn.execute(sa.text(sql_refresh_query))
            conn.commit()

        logger.info(
            f"Final table refreshed in {DB_SCHEMA}.example_bom_final_history"
        )

    except Exception as e:
        logger.error(f"Error refreshing final history table: {e}")
        raise

    return


def refresh_final_current_bom_table():
    """
    Refreshes the forecast_timber_bom_final table from the view by deleting
    existing rows and inserting fresh data.

    Uses an SQL script to delete and insert the most recent BOM data.
    For a full table refresh only
    """
    logger.info(
        f"Refreshing final table in {DB_HOST}: {DB_SCHEMA}.example_bom_final_current"
    )

    try:
        with (
            resources.files("bom_processing.sql")
            .joinpath("refresh_example_final_current_table.sql")
            .open("r") as file
        ):
            sql_refresh_query = file.read()
        with _get_db_connection() as conn:
            conn.execute(sa.text(sql_refresh_query))
            conn.commit()

        logger.info(
            f"Final table refreshed in {DB_SCHEMA}.example_bom_final_current"
        )

    except Exception as e:
        logger.error(f"Error refreshing final table: {e}")
        raise

    return

