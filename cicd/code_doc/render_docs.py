"""Module for customizing mkdocs documentation."""

# Import necessary libraries
import os
import shutil
from pathlib import Path

# Define the root directory and the necessary directories
root_path = Path(__file__).parents[2]
code_doc_path = root_path / "cicd" / "code_doc"
mkdocs_base_path = code_doc_path / "mkdocs"
mkdocs_build_path = mkdocs_base_path / "docs"
documentation_path = root_path / "artefacts" / "docs"

# Files and directories to be copied to build the mkdocs documentation
documentation_to_copy = {
    "directories_to_copy": [
        {
            "source": root_path / "lakehouse_engine_usage",
            "target": mkdocs_build_path / "lakehouse_engine_usage",
        },
        {
            "source": root_path / "lakehouse_engine",
            "target": mkdocs_base_path / "lakehouse_engine" / "packages",
        },
        {
            "source": "./assets",
            "target": mkdocs_build_path / "assets",
        },
    ],
    "files_to_copy": [
        {
            "source": "README.md",
            "target": mkdocs_build_path / "index.md",
        },
        {
            "source": "pyproject.toml",
            "target": mkdocs_build_path / "pyproject.toml",
        },
    ],
}


def _copy_documentation(directories: list = "", files: list = ""):
    """Copy files to other directory based on given parameters.

    Args:
        directories (list): list of directories to copy.
        files (list): list of files to copy.
    """
    if directories:
        for directory in directories:
            shutil.copytree(
                directory.get("source"), directory.get("target"), dirs_exist_ok=True
            )
    if files:
        for file in files:
            shutil.copyfile(file.get("source"), file.get("target"))


_copy_documentation(
    directories=documentation_to_copy.get("directories_to_copy"),
    files=documentation_to_copy.get("files_to_copy"),
)

# Use mkdocs build command to build the documentation into the "site" folder
os.system(f"cd {code_doc_path} && mkdocs build --site-dir {documentation_path}/site")

# Remove the temporary docs directory mkdocs_base_path
shutil.rmtree(mkdocs_base_path)
