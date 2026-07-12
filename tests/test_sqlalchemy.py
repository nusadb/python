"""SQLAlchemy ORM integration tests for the NusaDB dialect, against a real server.

Run: python -m unittest discover -s drivers/python/tests   (after `cargo build -p nusadb-server`).
Skipped automatically if SQLAlchemy is not installed.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from sqlalchemy import Column, Integer, String, create_engine, func, select
    from sqlalchemy.dialects import registry
    from sqlalchemy.orm import Session, declarative_base

    HAVE_SQLALCHEMY = True
except ImportError:  # pragma: no cover - environment without SQLAlchemy
    HAVE_SQLALCHEMY = False

from test_driver import _Server  # reuse the server-boot harness

if HAVE_SQLALCHEMY:
    registry.register("nusadb", "nusadb.sqlalchemy", "NusaDialect")

    Base = declarative_base()

    class User(Base):
        __tablename__ = "sa_users"
        # autoincrement disabled: the server has no sequence default, so ids are explicit.
        id = Column(Integer, primary_key=True, autoincrement=False)
        name = Column(String)


@unittest.skipUnless(HAVE_SQLALCHEMY, "SQLAlchemy not installed")
class SqlAlchemyOrmTests(unittest.TestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()
        cls.engine = create_engine(
            f"nusadb://u@127.0.0.1:{cls.server.port}/nusadb", future=True
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.engine.dispose()
        cls.server.stop()

    def test_create_insert_query_via_orm(self) -> None:
        Base.metadata.create_all(self.engine)

        with Session(self.engine) as session:
            session.add_all([User(id=1, name="alice"), User(id=2, name="bob")])
            session.commit()

        with Session(self.engine) as session:
            names = session.execute(select(User.name).order_by(User.id)).scalars().all()
            self.assertEqual(names, ["alice", "bob"])

            alice = session.get(User, 1)
            self.assertEqual(alice.name, "alice")

    def test_reflection_reports_column_defaults(self) -> None:
        from sqlalchemy import inspect, text

        with self.engine.begin() as conn:
            conn.execute(
                text("CREATE TABLE sa_reflect (id SERIAL, status TEXT DEFAULT 'active', n INT)")
            )
        cols = {c["name"]: c for c in inspect(self.engine).get_columns("sa_reflect")}
        # A SERIAL column reflects as autoincrement with a nextval(...) server default.
        self.assertTrue(cols["id"]["autoincrement"])
        self.assertTrue(str(cols["id"]["default"]).startswith("nextval("))
        # A literal DEFAULT is reflected verbatim; a column with no default has None.
        self.assertEqual(cols["status"]["default"], "'active'")
        self.assertFalse(cols["status"]["autoincrement"])
        self.assertIsNone(cols["n"]["default"])

    def test_orm_update_and_delete(self) -> None:
        Base.metadata.create_all(self.engine)
        with Session(self.engine) as session:
            session.add(User(id=10, name="carol"))
            session.commit()

        with Session(self.engine) as session:
            carol = session.get(User, 10)
            carol.name = "caroline"
            session.commit()

        with Session(self.engine) as session:
            self.assertEqual(session.get(User, 10).name, "caroline")
            session.delete(session.get(User, 10))
            session.commit()

        with Session(self.engine) as session:
            self.assertIsNone(session.get(User, 10))

    def test_limit_offset_and_count_inline(self) -> None:
        # Regression: SQLAlchemy renders LIMIT/OFFSET as a bound parameter by default, which the
        # server rejects ("non-constant LIMIT"). The dialect's compiler must inline them as
        # constants. Filter on a private id range so the result is order-independent of other tests.
        Base.metadata.create_all(self.engine)
        with Session(self.engine) as session:
            session.add_all([User(id=i, name=f"u{i}") for i in range(201, 206)])
            session.commit()

        with Session(self.engine) as session:
            base = select(User.id).where(User.id >= 201).order_by(User.id)
            page = [int(x) for x in session.execute(base.limit(2).offset(1)).scalars().all()]
            self.assertEqual(page, [202, 203])
            first_two = [int(x) for x in session.execute(base.limit(2)).scalars().all()]
            self.assertEqual(first_two, [201, 202])
            tail = [int(x) for x in session.execute(base.offset(3)).scalars().all()]
            self.assertEqual(tail, [204, 205])
            n = session.execute(
                select(func.count()).select_from(User).where(User.id >= 201)
            ).scalar()
            self.assertEqual(int(n), 5)

    def test_transaction_rolls_back(self) -> None:
        Base.metadata.create_all(self.engine)
        session = Session(self.engine)
        session.add(User(id=99, name="temp"))
        session.flush()  # INSERT sent inside the transaction
        session.rollback()
        session.close()

        with Session(self.engine) as session:
            self.assertIsNone(session.get(User, 99))


if __name__ == "__main__":
    unittest.main()
