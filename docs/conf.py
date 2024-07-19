# Configuration file for the Sphinx documentation builder.

import os
import sys

sys.path.insert(0, os.path.abspath(".."))

import pg_upsert

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "pg_upsert"
copyright = "2024, Caleb Grant"
author = "Caleb Grant"

# The short X.Y version.
version = pg_upsert.__version__
# The full version, including alpha/beta/rc tags.
release = pg_upsert.__version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
add_module_names = True

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "sphinx_rtd_theme"

html_static_path = ["_static"]

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
html_show_sphinx = False
