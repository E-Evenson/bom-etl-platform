import tempfile
import os
from pathlib import Path

from bom_processing.extract.get_bom_paths import _get_bom_paths_from


def create_mock_folder_with_files(folder_path: Path, filenames: list):
    os.makedirs(folder_path, exist_ok=True)
    for filename in filenames:
        file_path = folder_path / filename
        file_path.touch()


def test_get_bom_paths_from_returns_correct():
    with tempfile.TemporaryDirectory() as temp_dir:
        output_folder = Path(temp_dir) / "123456 - Test Project - Outputs"
        filenames = [
            "123456_material_list.xls",
            "123456_extra_list.xls",
            "123456_alternate_format.xlsx",
            "not_a_bom.xls",
            "123456_not_excel.txt",
            "123456_backup.xls.bak",
        ]

        create_mock_folder_with_files(output_folder, filenames)

        result = _get_bom_paths_from(output_folder)

        assert "123456" in result

        returned_file_paths = result["123456"]

        returned_file_names = [file.name for file in returned_file_paths]

        assert "123456_material_list.xls" in returned_file_names
        assert "123456_extra_list.xls" in returned_file_names
        assert "123456_alternate_format.xlsx" in returned_file_names
        assert "not_a_bom.xls" not in returned_file_names
        assert "123456_not_excel.txt" not in returned_file_names
        assert "123456_backup.xls.bak" not in returned_file_names
