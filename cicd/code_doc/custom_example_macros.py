"""Macro methods to be used on Lakehouse Engine Docs."""
import warnings
import json
import pygments.formatters.html
from markupsafe import Markup

STACK_LEVEL = 2


def _search_files(file: dict, search_string: str) -> list:
    """Searches for a string and outputs the line.

    Search for a given string in a file and output the line where it is first
    found.

    Args:
        file: path of the file to be searched.
        search_string: string that will be searched for.

    Returns:
        The number of the first line where a given search_string appears.
    """
    range_lines = []
    with open(file) as f:
        for num, line in enumerate(f, 1):
            if search_string in line:
                range_lines.append(num - 1)
    return range_lines[0]


def _link_example(method_name: str) -> str or None:
    """Searches for a link in a dict.

    Searches for the link of a given method_name, in a specific config file and
    outputs it.

    Args:
        method_name: name of the method to be searched for.

    Returns:
        None or the example link for the given method_name.
    """
    if method_name in list(lakehouse_engine_examples.keys()):
        file_link = lakehouse_engine_examples[str(method_name)]

        return lakehouse_engine_examples["base_link"] + file_link if file_link != "" else None
    else:
        warnings.warn(
                "No entry provided for the following transformer: "
                + method_name,
                RuntimeWarning,
                STACK_LEVEL,
        )

        return None


def _get_dict_transformer(dict_to_search: dict, transformer: str) -> dict:
    """Searches for a transformer and returns the first dictionary occurrence.

    Search for a given transformer in a dictionary and return the first occurrence.

    Args:
        dict_to_search: path of the file to be searched.
        transformer: string that will be searched for.

    Returns:
        First dictionary where a given transformer is found.
    """
    dict_transformer = []
    for spec in dict_to_search["transform_specs"]:
        for transformer_dict in spec["transformers"]:
            if transformer_dict["function"] == transformer:
                dict_transformer.append(transformer_dict)

    return json.dumps(dict_transformer[0], indent=4)


def _highlight_examples(method_name: str) -> str or None:
    """Creates a code snippet.

    Constructs and exposes the code snippet of a given method_name.

    Args:
        method_name: name of the module to be searched for.

    Returns:
        None or the code snippet wrapped in html tags.
    """
    for key, item in lakehouse_engine_examples.items():
        if method_name == key:
            file_path = f"../../{item}"
            if file_path == "../../":
                warnings.warn(
                    "No unit testing for the following transformer: " + method_name,
                    RuntimeWarning,
                    STACK_LEVEL,
                    )
                return None

            first_line = _search_files(file_path, f'"function": "{method_name}"')
            with open(file_path) as json_file:
                acon_file = json.load(json_file)
            code_snippet = _get_dict_transformer(acon_file, method_name)

            # Defining the lexer which will parse through the snippet of code we want
            # to highlight
            lexer = pygments.lexers.JsonLexer()
            # Defining the format that will be outputted by the pygments library
            # (on our case it will output the code within html tags)
            formatter = pygments.formatters.html.HtmlFormatter(
                linenos="inline",
                anchorlinenos=True,
            )
            formatter.linenostart = first_line

            return Markup(pygments.highlight(code_snippet, lexer, formatter))


def get_example(method_name: str) -> str:
    """Get example based on given argument.

    Args:
        method_name: name of the module to be searched for.

    Returns:
        A example.
    """
    example_link = _link_example(method_name=method_name)
    json_example = _highlight_examples(method_name=method_name)

    if example_link:
        return (
            """<details class="example">\n"""
            f"""<summary>View Example of {method_name} (See full example <a href="{example_link}">here</a>)</summary>"""
            f"""<div class="language-json highlight"><pre><span></span><code>{json_example}</code></pre></div>\n"""
            """</details>"""
        )
    else:
        return ""


with open("./examples.json") as json_file:
    lakehouse_engine_examples = json.load(json_file)

def define_env(env):
    "Declare environment for jinja2 templates for markdown"

    for fn in [get_example]:
        env.macro(fn)

    # get mkdocstrings' Python handler
    python_handler = env.conf["plugins"]["mkdocstrings"].get_handler("python")

    # get the `update_env` method of the Python handler
    update_env = python_handler.update_env

    # override the `update_env` method of the Python handler
    def patched_update_env(md, config):
        update_env(md, config)

        # get the `convert_markdown` filter of the env
        convert_markdown = python_handler.env.filters["convert_markdown"]

        # build a chimera made of macros+mkdocstrings
        def render_convert(markdown: str, *args, **kwargs):
            return convert_markdown(env.render(markdown), *args, **kwargs)

        # patch the filter
        python_handler.env.filters["convert_markdown"] = render_convert

    # patch the method
    python_handler.update_env = patched_update_env
