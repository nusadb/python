"""A minimal CRUD example for the nusadb Python driver.

Start a server first::

    cargo run -p nusadb-server -- --listen 127.0.0.1:5678 --data-dir ./data

Then::

    python drivers/python/examples/crud.py
"""

from __future__ import annotations

import nusadb


def main() -> None:
    conn = nusadb.connect(host="127.0.0.1", port=5678, user="nusa-root", database="nusadb")
    cur = conn.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS people (id INT NOT NULL, name TEXT, age INT)")

    # Create.
    for person in [(1, "alice", 30), (2, "bob", 25), (3, "carol", 41)]:
        cur.execute("INSERT INTO people VALUES ($1, $2, $3)", list(person))

    # Read.
    cur.execute("SELECT id, name, age FROM people WHERE age >= $1", [30])
    print("people aged >= 30:")
    for row in cur:
        print(" ", row)

    # Update.
    cur.execute("UPDATE people SET age = $1 WHERE name = $2", [26, "bob"])
    print("updated rows:", cur.rowcount)

    # Delete.
    cur.execute("DELETE FROM people WHERE id = $1", [3])
    print("deleted rows:", cur.rowcount)

    conn.close()


if __name__ == "__main__":
    main()
