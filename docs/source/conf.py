"""Sphinx configuration for the simple-salesforce-pubsub documentation."""

from importlib.metadata import version as distribution_version

project = "simple-salesforce-pubsub"
author = "Ricardo Carlini Sperandio"
copyright = "2026, Ricardo Carlini Sperandio"  # noqa: A001
release = distribution_version("simple-salesforce-pubsub")
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

htmlhelp_basename = "simplesalesforcepubsubdoc"
latex_documents = [
    (
        root_doc,
        "simple-salesforce-pubsub.tex",
        "simple-salesforce-pubsub Documentation",
        author,
        "manual",
    ),
]
man_pages = [
    (
        root_doc,
        "simple-salesforce-pubsub",
        "simple-salesforce-pubsub Documentation",
        [author],
        1,
    ),
]
texinfo_documents = [
    (
        root_doc,
        "simple-salesforce-pubsub",
        "simple-salesforce-pubsub Documentation",
        author,
        "simple-salesforce-pubsub",
        "Salesforce Pub/Sub API client for asyncio.",
        "Miscellaneous",
    ),
]
