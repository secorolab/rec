# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

from rec.observers.file_observer import FileObserver

__all__ = ["FileObserver", "MariaDBObserver"]


def __getattr__(name):
    if name == "MariaDBObserver":
        from rec.observers.mariadb_observer import MariaDBObserver

        return MariaDBObserver
    raise AttributeError(name)
