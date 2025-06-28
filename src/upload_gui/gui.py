import getpass
import logging
import os
from pathlib import Path
import shutil
from tkinter import filedialog, Tk, Label, Entry, StringVar, Button, messagebox
import uuid

from bom_processing.extract.read_boms_from_excel import extract_bom_data
from config.config import STAGING_DIR
from config.logging_config import configure_logging


logger = logging.getLogger(__name__)


if not STAGING_DIR:
    raise RuntimeError("STAGING_DIR environment variable is not set!")


class BOMUploaderGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("BOM Uploader")

        self.selected_filepaths = []
        self.validated_boms = {}

        Button(
            root,
            text="Select BOMs",
            command=self.select_boms,
        ).grid(row=0, column=0, padx=10, pady=10)

        Label(root, text="PON:").grid(row=1, column=0, padx=0, pady=10)
        self.pon = StringVar()
        Entry(root, textvariable=self.pon).grid(
            row=1, column=1, padx=0, pady=10
        )

        self.upload_button = Button(
            root,
            text="Upload Selected BOMs",
            command=self.upload_boms,
            state="disabled",
        )
        self.upload_button.grid(row=3, column=1, padx=10, pady=10)

    def select_boms(self):
        logger.info("Selecting BOMs for upload")

        filepaths = filedialog.askopenfilenames(
            title="Select BOMs to upload",
            filetypes=[("Excel files", "*.xlsx *.xls")],
        )

        self.selected_filepaths.clear()
        self.validated_boms.clear()
        self.selected_filepaths.extend(
            Path(filepath) for filepath in filepaths
        )

        if not self.selected_filepaths:
            logger.info("No BOMs selected")
            messagebox.showwarning("Invalid Selection", "No BOMs selected")
            self.upload_button.config(state="disabled")
            return

        failed = {}
        for path in self.selected_filepaths:
            try:
                logger.info(f"Validating BOM: {path.name}")
                bom_dict = extract_bom_data(path)
                if bom_dict:
                    self.validated_boms[path] = bom_dict["category"]
                    logger.info("Validation Successful")
            except Exception as e:
                logger.error(f"Validation failed for {path.name}: {e}")
                failed[path.name] = str(e)

        failure_message_lines = ["The following BOMs failed validation:\n"]
        if failed:
            for path, error in failed.items():
                failure_message_lines.append(f"- {path}: {error}")

            message = "\n".join(failure_message_lines)
            messagebox.showwarning("Validation error", message)
            self.upload_button.config(state="disabled")

        else:
            self.upload_button.config(state="normal")

    def upload_boms(self) -> None:
        temp_dir = STAGING_DIR / f"tmp_upload_{uuid.uuid4().hex}"
        pon = self.pon.get().strip()
        username = getpass.getuser()
        if not pon.isdigit() or len(pon) < 5 or len(pon) > 7:
            logger.warning(f"Invalid PON: {pon}")
            messagebox.showwarning(
                "Invalid PON",
                "PON must be a number with 5-7 digits",
            )
            raise Exception

        existing_files_for_pon = list(Path(STAGING_DIR).glob(f"{pon}_*.xls*"))
        if existing_files_for_pon:
            logger.info("Existing files found")
            ok_to_delete = messagebox.askokcancel(
                "Existing Files",
                f"There are existing files for PON {pon} waiting to be processed. Click OK to delete them.",
            )
            if ok_to_delete:
                for file in existing_files_for_pon:
                    logger.info(f"Deleting existing file: {file}")
                    file.unlink()
            else:
                logger.info("BOM upload aborted due to user input")
                raise Exception

        try:
            logger.info(
                f"Copying {len(self.validated_boms)} files to temporary directory"
            )
            temp_dir.mkdir(parents=True, exist_ok=False)
            for filepath, category in self.validated_boms.items():
                bom_extension = filepath.suffix

                category_count = 1
                new_filename = (
                    f"{pon}_{category}_staging_{username}{bom_extension}"
                )

                while os.path.exists(temp_dir / new_filename):
                    category_count += 1
                    new_filename = f"{pon}_{category}_{category_count}_staging_{username}{bom_extension}"

                logger.info(
                    f"Copying {filepath.name} to temporary directory as {new_filename}"
                )
                shutil.copy(filepath, temp_dir / new_filename)

            for filepath in temp_dir.iterdir():
                logger.info(
                    f"Copying {filepath.name} files into staging folder"
                )
                final_destination = STAGING_DIR / filepath.name
                if final_destination.exists():
                    final_destination.unlink()
                shutil.move(filepath, final_destination)

            try:
                temp_dir.rmdir()
            except OSError:
                shutil.rmtree(temp_dir, ignore_errors=True)

            logger.info("BOM Upload Successful")
            messagebox.showinfo(
                "Upload Complete", "All BOMs uploaded successfully"
            )

            self.selected_filepaths.clear()
            self.validated_boms.clear()
            self.pon.set("")
            self.upload_button.config(state="disabled")

        except Exception as e:
            logger.error(f"Upload aborted: {e}")
            shutil.rmtree(temp_dir, ignore_errors=True)
            messagebox.showerror("Upload Failed", f"Upload aborted: {e}")
            # self.upload_button.config(state="disabled")


def main():
    configure_logging()
    root = Tk()
    BOMUploaderGUI(root)
    root.mainloop()

    return


if __name__ == "__main__":
    main()
