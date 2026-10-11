"""Django database backend for NusaDB, on the blocking :mod:`nusadb` driver.

Set ``ENGINE`` to ``"nusadb.django"``::

    DATABASES = {
        "default": {
            "ENGINE": "nusadb.django",
            "NAME": "app",
            "USER": "nusadb-root",
            "PASSWORD": "",
            "HOST": "127.0.0.1",
            "PORT": "5678",
            "OPTIONS": {"isolation_level": "read committed"},
        }
    }

Targets Django 4.2. See the "Django" section of the README for what is and is not supported.
"""
