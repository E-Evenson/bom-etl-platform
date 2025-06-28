## Pipeline Stages

This document outlines each stage of the BOM ETL Pipeline, from extraction to Power BI reporting. Each stage includes responsible modules, key functions, dependencies, and expected inputs/outputs.

### 1. BOM Path Extraction

- Gets paths and metadata of all BOMs to be uploaded to SQL

- **Full Scrape**: Gets BOM paths and metadata from design directory
    - **Module**: `src/bom_processing/extract/get_bom_paths.py`
    - **Function**: `scrape_bom_paths_from_design_directory`
    - **Input**: Projects directory
    - **Output**: List of dicts: each with keys pon, username, and path
    - **Assumptions**: Relies on:
        - Folder structure and naming conventions
        - File naming conventions
        - Username defaults to "system"
    - **Logging**: Warnings for:
        - Project folders with no outputs folders
        - Outputs folders with no BOMs

- **Staging Folder uploads**
    - **Module**: `src/bom_processing/extract/get_bom_paths.py`
    - **Function**: `scrape_bom_paths_from_staging_folder`
    - **Input**: staging folder location (populated by GUI uploads)
    - **Output**: List of dicts: each with keys pon, username, and path
    - **Assumptions**:
        - metadata (pon and username) inferred from filename
        - filenames properly formatted (uploaded via GUI)
        - All files expected to be BOMs
    - **Logging**: Warnings for:
        - Invalid filenames/file types
        - Skipped files (due to invalid naming/type)

---

### 2. BOM Processing

- Processes BOMs through validation, cleaning, transformation, and schema enforcement
- **Script**: `src/bom_processing/orchestration/process_boms.py`
- **Function**: `process_boms`
- **Input**:
    - List of dicts: each with keys pon, username, and path
    - Loading method (full scrape, upload, etc.)
- **Output**: Cleaned DataFrame
- **Dependencies**:
    - **read_boms_from_excel.py**: Read BOMs from Excel, validates required columns and non-nullable fields, prelim cleaning
    - **transformations.py**: Take in DataFrame, clean, and transform the data
    - **column_validation.py**: Validates required columns and non-nullable fields after transformation
    - **constants.py**: Defines required columns, dtypes, renaming maps, nullables, etc.
- **Logging**: Warnings for:
    - Failed validations
    - Skipped BOMs

---

### 3. SQL Staging Load

- Loads BOM data into the staging table in SQL Server
- **Module**: `src/bom_processing/load/load_to_sql.py`
- **Function**: `delete_and_insert_to_sql`
- **Input**:
    - Target table name
    - Pandas DataFrame to load
- **Target Table**: `example_bom_staging`
- **Dependencies**: 
    - `SQLAlchemy`
    - `pyodbc`
    - Database credentials from .env.* file
- **Logging**: 
    - Failed connection
    - Failed SQL queries

---

### 4. SQL Transformation

- **Source Tables**: `example_bom_staging`, `item_id_reference`
- **Target View**: `vw_example_bom_with_item_ids`
- **Logic**: 
    - Joins BOM rows with item_id_reference using:
        - material type
        - material subtype
        - Closest match on height and width that is larger than BOM requirements
    - Each row receives one item_id
- **Notes**: Used as source for both example_bom_final and example_bom_final_history

---

### 5. SQL Final Load

- **Full Scrape**: 
    - **Module** `src/bom_processing/load/load_to_sql.py`
    - **Function**: `refresh_final_bom_table`
    - **Source**: `vw_example_bom_with_item_ids`
    - **Target Table**: `example_bom_final`
    - **Dependencies**: 
        - `SQLAlchemy`
        - `pyodbc`
        - Database access credentials from .env.* files
        - SQL query: `refresh_example_bom_final_table.sql`
    - **Logic**: 
        - Clears example_bom_final before inserting new data
        - Inserts all data from vw_example_bom_with_item_ids

- **Staging Folder uploads**
    - **Module** `src/bom_processing/load/load_to_sql.py`
    - **Function**: `insert_uploads_into_history_table`
    - **Source**: `vw_example_bom_with_item_ids`
    - **Target Table**: `example_bom_final_history`
    - **Dependencies**: 
        - `SQLAlchemy`
        - `pyodbc`
        - Database access credentials from .env.* files
        - SQL query: `insert_staging_into_history_table.sql`
    - **Logic**: 
        - Appends all data from view into example_bom_final_history
        - No deletions, existing data retained for historical tracking

---

### 6. Power BI

- **Source**: `example_bom_final`
- **Purpose**: Basic dashboard to visualize production needs
- **Implementation**: Connected to SQL table and Semantic model for production needs forecasting
- **Refresh Method**: Manual (can be automated with gateway support)
- **Note**: Integration point only. Not representative of broader data modeling or reporting work

---