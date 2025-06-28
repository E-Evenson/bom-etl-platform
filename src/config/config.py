import os
from pathlib import Path
import sys

from dotenv import load_dotenv

env = os.getenv("ENV", "local").lower()
if env == "dev":
    load_dotenv(".env.dev")
elif env == "prod":
    load_dotenv(".env.prod")
else:
    load_dotenv(".env.local")

# Logging variables
LOG_DIR = Path(os.getenv("LOG_DIR", "logs/"))
LOG_FORMAT = os.getenv(
    "LOG_FORMAT", "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
DEBUG = os.getenv("DEBUG", "False").lower() == "true"
LOG_TO_FILE = os.getenv("LOG_TO_FILE", "True").lower() == "true"

# Set logging level based on DEBUG flag
LOG_LEVEL = "DEBUG" if DEBUG else os.getenv("LOG_LEVEL", "INFO").upper()

# DB Variables
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_SCHEMA = os.getenv("DB_SCHEMA")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# File handling
staging_dir_env = os.getenv("STAGING_DIR")
if not staging_dir_env:
    # If the BOM upload GUI is being run from an exe file
    # use this fallback path for convenience during development
    if getattr(sys, "frozen", False):
        staging_dir_env = (
            r"X:\path\to\staging_folder"
        )
    else:
        raise RuntimeError("STAGING_DIR environment variable not set!")

STAGING_DIR = Path(staging_dir_env)
if not STAGING_DIR.is_absolute():
    raise RuntimeError("STAGING_DIR path must be absolute")

# Other crap
design_dir = os.getenv("DESIGN_PROJECT_DIRECTORY")
if design_dir is not None:
    DESIGN_PROJECT_DIRECTORY = Path(design_dir)
