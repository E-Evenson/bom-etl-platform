# Self Serve BOM ETL Platform

## About this portfolio version

**This project was independently designed and developed including architecture, orchestration, data modeling, and GUI implementation. It was developed as part of my role at a manufacturing company. This project has been adapted for public portfolio use. This public facing version of the BOM ETL Platform is shared to demonstrate technical capability in building end-to-end data pipelines. Sensitive names, filepaths, and identifiers have been removed or replaced with placeholders. Test data has also been removed for privacy.**

> Status: Functional Internal Tool - Adapted for portfolio demonstration
> Requires: Python 3.13+

---

## Overview

**Purpose**:  
Supports material and manufacturing demand by transforming department design BOMs
into a queryable format. Automates the ETL of BOMs into SQL Server. There are two main pipeline processes.

This system is modular and can be adapted for other BOM formats or ingestion workflows with minimal changes.

**Pipeline Processes**

1. User Uploads
    - Designers upload BOM files through a GUI
    - GUI validates files, renames them with metadata, and copies them to a staging folder
    - A daily python ETL script loads these files into the staging table and appends them to the bom_final_history table
    - The script also updates the bom_final_current table with only the most up to date BOMs for each PON (project opportunity number)

2. Design Folder Full Scrape
    - A full scrape of the design folder runs independently to rebuild a baseline
    - Loads files into bom_staging table then overwrites bom_final
    - This is primarily used for rebuilding the system from scratch

**Pipeline Flow**:

- Core ETL Flow:
`Read BOMs -> Clean, Validate, and Transform BOMs -> SQL Staging -> Item ID mapping (via view)`

- Full Scrape:
`Scrape BOM paths and metadata from design directory -> Core ETL Flow -> Load to bom_final`

- Upload Via GUI:
`User Selects BOMs -> GUI validates files -> Rename files with metadata and upload files to staging directory -> Scheduled script scrapes staging directory for BOM paths and metadata -> Core ETL Flow -> Append to bom_final_history -> View selects most recent BOM per PON -> Load to bom_final_current`

![BOM ETL Pipeline Flow Diagram](docs/BOM%20ETL%20Pipeline%20Flow%20Diagram.png)

**System Interactions**:
- Salesforce (via Power BI)
- SharePoint (BOM folders)
- SQL Server (DEV/PROD)
- Power BI (dashboards/visuals)

**Potential Improvements**:
- Allow user to delete staged but not yet processed files from upload Staging Folder
- Create audit tables for uploaded BOMs
- Create audit tables for processed BOMs
- Create link to Salesforce data for auditing Parent/Child PON changes
- Create robust testing framework (yes please)
- Set up email alerts for upload and processing success/failure

---

## Core Capabilities Demonstrated

- End-to-end data pipeline design and implementation
- Multi-source BOM ingestion and normalization
- GUI design for user-uploaded files with validation
- SQL Server staging and historical table management
- View-driven modeling for item ID mapping
- Modular and testable Python code structure
- Windows-based orchestration with PowerShell + Task Scheduler
- Secure config via environment variables and `.env` files

---

## Tech Stack

- Windows Task Scheduler: Pipeline Orchestration
- Python: pandas, pyodbc, SQLAlchemy
- SQL Server: Views, ETL Staging + Final Tables
- Poetry: Dependency management
- GUI Tkinter (Tkinter was chosen for simplicity and platform compatibility during development. A refactor to a different GUI could be explored in future)
- Power BI: Dashboards

---

## Setup

This project uses [Poetry](https://python-poetry.org/) for dependency and environment management.
It is set up to use pyproject.toml for dependencies and .env.* files for configuration.

### Install Dependencies

1. **Install Python**:
    Install Python version 3.13+, correct version for your OS: https://www.python.org/
    During installation do NOT check the box that says "Add Python to PATH"
    After installing open command prompt and run "py --version" to confirm installation

2. **Install Poetry** (if not already installed):
    Follow the official instructions: https://python-poetry.org/docs/#installation

3. **Clone Project Repository**:
    Repository to be cloned to working environment: bom_etl_platform
    Run: git clone {Appropriate GitHub repository URL}

4. **Install Project Dependencies**:
    In command prompt, navigate into project directory and run: poetry install

5. **Install pyodbc drivers**:
    For Windows: 
        https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17#download-for-windows

### Configure Environment
    Use .env.template as reference, keep .env files in project directory

    .env.local for local use
    .env.dev for dev environment
    .env.prod for production

### Schedule task
    To schedule in Task Scheduler:

    - create new task
    - create new action. Settings:
        - Program: powershell.exe
        - Add Arguments: 
            - Full Scrape: -Command "$env:ENV = 'REPLACE WITH DESIRED ENV (local, dev, or prod)'; poetry run python -m src.etl.folder_scraping_etl"
            - Staging Folder processing: -Command "$env:ENV = 'REPLACE WITH DESIRED ENV (local, dev, or prod)'; poetry run python -m src.etl.staging_folder_etl"
        - Start in: Absolute/path/to/project

---

## Pipeline Orchestration

### Core ETL Flow

- **Description**: Extracts, Transforms, and Loads BOM data into SQL staging area. Data is then enriched with item IDs via a SQL view
- **ETL Steps**:
    - Read, validate, and clean data from BOMs
    - Transform data to standardized formats
    - Load data into bom_staging table
    - Item ID mapping using vw_bom_with_item_ids

### Upload Via GUI

- **Description**: Designers use the GUI to upload revised BOMs to central staging folder
- **Script**: **`src/upload_gui/gui.py`**
- **Flow**:
    - GUI validates BOM structure and required columns
    - User enters metadata
    - Valid files and their metadata are saved to the BOM staging folder
        - If PON has files existing in staging folder already, they will be deleted before saving new ones
        - This is to allow for corrections of minor mistakes before staging folder is processed by the ETL script
    - Invalid files or metadata are rejected with feedback to user

### Processing of BOM Staging Folder

- **Description**: Processes BOMs in staging folder. Appends results to historical BOM table
- **Orchestrated by**: **`src/etl/staging_folder_etl.py`**
- **ETL Steps**:
    - Extract BOM paths and metadata
    - Run core ETL flow
    - Insert enriched data from vw_bom_with_item_ids to bom_final_history table (append only)
    - Create vw_bom_final_current view from bom_final_history table to filter to the most recent BOM uploaded for each PON
    - Overwrite data in bom_final_current table with data from vw_bom_final_current

### Full Scrape of Design Folder (currently deployed)

- **Description**: Scans the entire active projects folder for BOMs, rebuilding bom_final table from scratch
- **Orchestrated by**: **`src/etl/folder_scraping_etl.py`**
- **ETL Steps**:
    - Extract BOM paths from folder structure
    - Run core ETL flow
    - Overwrite bom_final with data from vw_bom_with_item_ids
- **Notes**:
    - relies on folder structure and naming conventions:
        - Design folder contains project folders (`PON - NAME`)
        - Project folders contain Outputs folders (`PON - NAME - Outputs`)
        - Outputs folders contain BOMs (`PON_otherstuff.xls`)

---

## Configuration

- **Environment Switching**: Controlled by `.env.dev`, `.env.prod`, etc.
- **Config File**: `src/config/config.py`
- **Logging Config**: `src/config/logging_config.py`
- **Secrets**: Stored in `.env.*` files (excluded from Git)

---

## Known Issues

- Errors in design folder structure or file name could lead to missing BOMs
    - Warning logs have been implemented to note folders with no outputs
- Mismatches in item ID
    - Check dimension rounding/mapping logic, check for duplicated part_tags within PONs
- Database access issues
    - Check connection credentials in env files are accurate, and that they have correct privileges

---

## How to Extend / Move to PROD

- Create `.env.prod` with correct DESIGN_PROJECT_DIRECTORY and prod database credentials
- Ensure machine has access to department sharepoint through file explorer (if running full scrape)
- Ensure machine has access to staging folder (if running GUI and staging folder ETL)
- Setup Power BI gateway with access to PROD SQL
- Notify IT to help with gateway + data refresh permissions

---

## Potential Improvements

- **GUI Improvements**
    - Deploy GUI to designers for testing and feedback
    - Determine appropriate location for staging uploaded files
    - Add error logging for GUI users' visibility
    - Allow designers to delete uploaded files from Staging Folder before folder processing

- **Robustness and Monitoring**
    - Build robust unit testing framework
    - Add audit tables for uploaded/processed BOMs
    - Create link to Salesforce data for auditing PON relationships
    - Set up email alerts for GUI Upload and ETL success/failure

- **Pipeline Flexibility**
    - Expand ingestion logic to support additional BOM Formats

## Additional Documentation

- [Pipeline Stages](docs/Pipeline%20Stages.md)
- [Scheduling and Automation](docs/Scheduling%20and%20Automation.md)
- [SQL Model](docs/SQL%20Model.md)
- [Pipeline Flow Diagram](docs/BOM%20ETL%20Pipeline%20Flow%20Diagram.png)