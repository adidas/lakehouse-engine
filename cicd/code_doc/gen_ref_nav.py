"""Module to generate code reference docs."""

# Import necessary libraries
from pathlib import Path
import mkdocs_gen_files

# Create a new navigation structure
nav = mkdocs_gen_files.Nav()

# Define the root directory and the source directory
root = Path(__file__).parent
src = root / "mkdocs/lakehouse_engine"

print(f"Looking for files in {src}")

# Loop over all Python files in the source directory
for path in sorted(src.rglob("*.py")):
    # Get the module path and the documentation path for each file
    module_path = path.relative_to(src).with_suffix("")
    doc_path = path.relative_to(src / "").with_suffix(".md")
    full_doc_path = Path("reference", doc_path)

    # Split the module path into parts
    parts = tuple(module_path.parts)

    # Skip files that start with an underscore or have no parts
    if not parts:
        continue

    # If the file is an __init__.py file, remove the last part and rename the doc file to index.md
    if parts[-1] == "__init__" and str(parts[:-1]) != "()":
        parts = parts[:-1]
        doc_path = doc_path.with_name("index.md")
        full_doc_path = full_doc_path.with_name("index.md")
    elif parts[-1].startswith("_"):
        continue

    # Skip the loop iteration if there is no doc path
    if not doc_path:
        continue

    # If the doc path has at least one part, add it to the navigation
    if len(doc_path.parts) >= 1:
        nav_parts = [f"{part}" for part in parts]
        nav[tuple(nav_parts)] = doc_path.as_posix()

        # Open the full doc path and write the module identifier to it
        with mkdocs_gen_files.open(full_doc_path, "w") as fd:
            ident = ".".join(parts)
            fd.write(f"::: {ident}")

        # Set the edit path for the file
        mkdocs_gen_files.set_edit_path(
            full_doc_path, ".." / path.relative_to(root))

# Open the index.md file and write the built navigation to it
with mkdocs_gen_files.open("reference/index.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())