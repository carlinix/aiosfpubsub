"""Sphinx configuration for the aiosfpubsub documentation."""

from importlib.metadata import version as distribution_version

project = "aiosfpubsub"
author = "Ricardo Carlini Sperandio"
copyright = "2026, Ricardo Carlini Sperandio"
release = distribution_version("aiosfpubsub")
version = release

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
]

autoclass_content = "both"
autodoc_typehints = "description"
exclude_patterns = []
html_static_path = ["_static"]
html_theme = "alabaster"
intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}
language = "en"
pygments_style = "sphinx"
root_doc = "index"
templates_path = ["_templates"]

htmlhelp_basename = "aiosfpubsubdoc"
latex_documents = [
    (
        root_doc,
        "aiosfpubsub.tex",
        "aiosfpubsub Documentation",
        author,
        "manual",
    ),
]
man_pages = [
    (
        root_doc,
        "aiosfpubsub",
        "aiosfpubsub Documentation",
        [author],
        1,
    ),
]
texinfo_documents = [
    (
        root_doc,
        "aiosfpubsub",
        "aiosfpubsub Documentation",
        author,
        "aiosfpubsub",
        "Salesforce Pub/Sub API client for asyncio.",
        "Miscellaneous",
    ),
]
