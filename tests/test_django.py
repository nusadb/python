"""The Django backend (``nusadb.django``) against a real server.

The tests live in ``tests/django`` with their settings and app; this module loads them so that
``python -m unittest discover -s tests`` runs them too. Skipped when Django is not installed.
Run them on their own with::

    NUSADB_SERVER_BIN=/path/to/nusadb-server python -m unittest discover -s tests/django
"""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

DJANGO_TESTS = Path(__file__).resolve().parent / "django"


class DjangoNotInstalled(unittest.TestCase):
    def test_django_backend(self) -> None:
        self.skipTest("Django is not installed")


def _import_django() -> bool:
    """Import the installed Django, if there is one.

    tests/ is on sys.path, and its django directory would be found as a namespace package before
    an installed Django that is located through an import hook. Leave tests/ out while importing.
    """
    tests_dir = DJANGO_TESTS.parent
    saved = sys.path[:]
    sys.path[:] = [p for p in sys.path if Path(p or ".").resolve() != tests_dir]
    try:
        sys.modules.pop("django", None)
        import django
    except ImportError:
        return False
    finally:
        sys.path[:] = saved
    return hasattr(django, "setup")


def load_tests(
    loader: unittest.TestLoader, standard_tests: unittest.TestSuite, pattern: str
) -> unittest.TestSuite:
    if not _import_django():
        return loader.loadTestsFromTestCase(DjangoNotInstalled)
    if str(DJANGO_TESTS) not in sys.path:
        sys.path.insert(0, str(DJANGO_TESTS))
    return loader.loadTestsFromModule(importlib.import_module("test_backend"))
