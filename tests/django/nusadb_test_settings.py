"""Django settings for the backend tests: one database on the server the tests start.

The test module sets ``NUSADB_TEST_PORT`` (and, for another server, ``NUSADB_TEST_HOST``)
before Django reads these settings.
"""

import os

SECRET_KEY = "nusadb-backend-tests"
USE_TZ = True
TIME_ZONE = "UTC"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
INSTALLED_APPS = ["bookshop"]
DATABASES = {
    "default": {
        "ENGINE": "nusadb.django",
        "NAME": "djapp",
        "USER": "nusadb-root",
        "PASSWORD": os.environ.get("NUSADB_TEST_PASSWORD", ""),
        "HOST": os.environ.get("NUSADB_TEST_HOST", "127.0.0.1"),
        "PORT": os.environ.get("NUSADB_TEST_PORT", "5678"),
        "TEST": {"NAME": "test_djapp"},
    }
}
