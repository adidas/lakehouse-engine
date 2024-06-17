"""Module for customizing pdoc documentation."""

import json
import os
import shutil
import warnings
from pathlib import Path

import pygments.formatters.html
from markupsafe import Markup
from pdoc import pdoc, render

STACK_LEVEL = 2

logo_path = (
    "https://github.com/adidas/lakehouse-engine/blob/master/assets/img/"
    "lakehouse_engine_logo_no_bg_160.png?raw=true"
)


def _get_project_version() -> str:
    version = (
        os.popen('cat cicd/.bumpversion.cfg | grep current_version | cut -f 3 -d " "')
        .read()
        .replace("\n", "")
    )
    return version


def _search_files(file: dict, search_string: str) -> list:
    """Searches for a string and outputs the line.

    Search for a given string in a file and output the line where it is first
    found.

    :param file: path of the file to be searched.
    :param search_string: string that will be searched for.

    :returns: the number of the first line where a given search_string appears.
    """
    range_lines = []
    with open(file) as f:
        for num, line in enumerate(f, 1):
            if search_string in line:
                range_lines.append(num - 1)
    return range_lines[0]


def _get_dict_transformer(dict_to_search: dict, transformer: str) -> dict:
    """Searches for a transformer and returns the first dictionary occurrence.

    Search for a given transformer in a dictionary and return the first occurrence.

    :param dict_to_search: path of the file to be searched.
    :param transformer: string that will be searched for.

    :returns: first dictionary where a given transformer is found.
    """
    dict_transformer = []
    for spec in dict_to_search["transform_specs"]:
        for transformer_dict in spec["transformers"]:
            if transformer_dict["function"] == transformer:
                dict_transformer.append(transformer_dict)
    return json.dumps(dict_transformer[0], indent=4)


def _link_example(module_name: str) -> str or None:
    """Searches for a link in a dict.

    Searches for the link of a given module_name, in a specific config file and
    outputs it.

    :param module_name: name of the module to be searched for.

    :returns: None or the example link for the given module_name.
    """
    if module_name in list(link_dict.keys()):
        file_link = link_dict[str(module_name)]
        return link_dict["base_link"] + file_link if file_link != "" else None
    else:
        return None


def _highlight_examples(module_name: str) -> str or None:
    """Creates a code snippet.

    Constructs and exposes the code snippet of a given module_name.

    :param module_name: name of the module to be searched for.

    :returns: None or the code snippet wrapped in html tags.
    """
    transformers_to_ignore = [
        "UNSUPPORTED_STREAMING_TRANSFORMERS",
        "AVAILABLE_TRANSFORMERS",
        "__init__",
    ]
    if module_name.split(".")[1] == "transformers":
        if module_name not in list(link_dict.keys()):
            if module_name.split(".")[-1] not in list(transformers_to_ignore):
                warnings.warn(
                    "No entry provided for the following transformer: "
                    + module_name.split(".")[-1],
                    RuntimeWarning,
                    STACK_LEVEL,
                )
                return None

    for key, item in link_dict.items():
        if module_name == key:
            file_path = f"./{item}"
            transformer = key.split(".")[-1].lower()
            if file_path == "./":
                warnings.warn(
                    "No unit testing for the following transformer: " + transformer,
                    RuntimeWarning,
                    STACK_LEVEL,
                )
                return None

            first_line = _search_files(file_path, f'"function": "{transformer}"')
            with open(file_path) as json_file:
                acon_file = json.load(json_file)
            code_snippet = _get_dict_transformer(acon_file, transformer)
            # Defining the lexer which will parse through the snippet of code we want
            # to highlight
            lexer = pygments.lexers.JsonLexer()
            # Defining the format that will be outputted by the pygments library
            # (on our case it will output the code within html tags)
            formatter = pygments.formatters.html.HtmlFormatter(
                cssclass="pdoc-code codehilite",
                linenos="inline",
                anchorlinenos=True,
            )
            formatter.linenostart = first_line
            return Markup(pygments.highlight(code_snippet, lexer, formatter))


with open("./cicd/code_doc/examples.json") as json_file:
    link_dict = json.load(json_file)

# Adding our custom filters to jinja environment
env_jinja = render.env
env_jinja.filters["link_example"] = _link_example
env_jinja.filters["highlight_examples"] = _highlight_examples


root_path = Path(__file__).parents[2]
documentation_path = root_path / "artefacts" / "docs"
# Tell pdoc's render to use our jinja template
render.configure(
    template_directory=root_path / "cicd" / "code_doc" / ".",
    docformat="google",
    logo=logo_path,
    favicon=logo_path,
    footer_text=f"Lakehouse Engine v{_get_project_version()}",
    mermaid=True,
)
# Temporarily copy README file to be used in index.html page
shutil.copyfile("README.md", root_path / "cicd" / "code_doc" / "README.md")

# Render pdoc's documentation into artefacts/docs
pdoc(
    "./lakehouse_engine/",
    "./lakehouse_engine_usage/",
    output_directory=documentation_path,
)

# Copy the images used on the documentation, to the path where we have the rendered
# html pages.
shutil.copytree("./assets", documentation_path / "assets", dirs_exist_ok=True)

# Remove the temporary copy README file
os.remove(root_path / "cicd" / "code_doc" / "README.md")
