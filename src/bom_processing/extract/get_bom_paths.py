import logging
import os
from pathlib import Path
import re

from config.config import DESIGN_PROJECT_DIRECTORY, STAGING_DIR


logger = logging.getLogger(__name__)


def _get_parent_folders_from(design_directory: Path) -> list[Path]:
    """
    Find project parent folders in design Active Projects
    """
    logger.debug("Getting parent folders...")
    parent_folder_name_format: str = r"^\d+ *- *.+"

    parent_folders: list[Path] = [
        folder
        for folder in design_directory.iterdir()
        if re.match(parent_folder_name_format, folder.name)
    ]

    if not parent_folders:
        logger.warning(f"No project folders in {design_directory}")

    return parent_folders


def _get_output_folders_from(parent_folder: Path) -> list[Path]:
    """
    Find output folders in a single parent folder
    """
    logger.debug(f"Getting output folders from {parent_folder.name}")
    output_folder_name_format: str = r"^\d+ *- *.+ *- *outputs"

    output_folders = [
        parent_folder / folder
        for folder in parent_folder.iterdir()
        if re.match(output_folder_name_format, folder.name.lower())
    ]

    if not output_folders:
        logger.warning(f"No output folders in {parent_folder.name}")

    return output_folders


def _get_output_folders(parent_folders: list[Path]) -> list[Path]:
    """
    Find outputs folders from multiple parent folders
    """
    logger.debug(
        f"Getting output folders for {len(parent_folders)} parent folders"
    )
    output_folders = []

    for parent_folder in parent_folders:
        output_folders.extend(_get_output_folders_from(parent_folder))

    return output_folders


def _get_bom_paths_from(output_folder: Path) -> dict[str, list[Path]]:
    """
    Find bom files in a single outputs folder
    """
    logger.debug(f"Searching for BOMs in {output_folder.name}")
    material_list_name_format = r"^\d+.*.xls(x)?$"
    pon = output_folder.name.split("-")[0].strip()
    bom_files = []

    for file in os.listdir(output_folder):
        if re.match(material_list_name_format, file):
            bom_files.append(output_folder / file)

    if not bom_files:
        logger.warning(f"No BOMs found in {output_folder.name}")

    return {pon: bom_files} if bom_files else {}


def _aggregate_bom_paths(output_folders: list[Path]) -> dict[str, list[Path]]:
    """
    Find bom paths in multiple output folders
    """
    logger.debug(
        f"Getting BOM paths from {len(output_folders)} output folders"
    )
    bom_dict = {}

    for output_folder in output_folders:
        bom_files = _get_bom_paths_from(output_folder)
        if bom_files:
            for pon, files in bom_files.items():
                if pon in bom_dict:
                    logging.warning(
                        f"Multiple output folders for PON {pon} found"
                    )
                    bom_dict[pon].extend(files)
                else:
                    bom_dict[pon] = files

    return bom_dict


def scrape_bom_paths_from_design_directory(
    root_folder: Path = DESIGN_PROJECT_DIRECTORY,
) -> list[dict]:
    logger.info(f"Scraping BOM paths from {root_folder}")

    parent_folders = _get_parent_folders_from(root_folder)
    outputs_folders = _get_output_folders(parent_folders)
    bom_paths = _aggregate_bom_paths(outputs_folders)

    bom_records = []
    for pon, files in bom_paths.items():
        for path in files:
            bom_records.append(
                {
                    "pon": pon,
                    "username": "system",
                    "path": path,
                }
            )

    return bom_records


def scrape_bom_paths_from_staging_folder(
    staging_folder: Path = STAGING_DIR,
) -> list[dict]:
    logger.info(f"Collecting BOM paths from {staging_folder}")

    valid_filename_pattern = re.compile(
        r"^(\d{5,7})_.*_staging_(\w+)\.xls[x]?$"
    )

    bom_records = []
    invalid_filenames = []

    for path in staging_folder.iterdir():
        match = valid_filename_pattern.match(path.name)
        if not match:
            logger.error(f"Unexpected file type in staging: {path.name}")
            invalid_filenames.append(path.name)
            continue

        pon = match.group(1)
        username = match.group(2)
        bom_records.append(
            {
                "pon": pon,
                "username": username,
                "path": path,
            }
        )

    if invalid_filenames:
        raise ValueError(f"Unexpected file names: {sorted(invalid_filenames)}")

    return bom_records

