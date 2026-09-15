# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

project = "REC"
extensions = ["sphinx.ext.autodoc"]
autodoc_mock_imports = ["mariadb", "dotenv"]
html_theme = "furo"
