"""The SQLAlchemy dialects (``nusadb://`` and ``nusadb+asyncio://``) against a real server.

Run: ``python -m unittest discover -s tests``. Skipped when SQLAlchemy (or, for the asyncio
dialect, greenlet) is not installed.
"""

from __future__ import annotations

import datetime as dt
import enum
import sys
import unittest
import uuid
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_driver import _Server  # noqa: E402

try:
    import sqlalchemy as sa
    from sqlalchemy import (
        JSON,
        BigInteger,
        Boolean,
        CheckConstraint,
        Column,
        Date,
        DateTime,
        Enum,
        ForeignKey,
        Index,
        Integer,
        Interval,
        LargeBinary,
        Numeric,
        String,
        Text,
        Time,
        UniqueConstraint,
        Uuid,
        create_engine,
        func,
        inspect,
        select,
    )
    from sqlalchemy.dialects import registry
    from sqlalchemy.orm import DeclarativeBase, Session, relationship

    HAVE_SQLALCHEMY = True
except ImportError:  # pragma: no cover - environment without SQLAlchemy
    HAVE_SQLALCHEMY = False

try:
    import greenlet  # noqa: F401

    HAVE_GREENLET = True
except ImportError:  # pragma: no cover
    HAVE_GREENLET = False

if HAVE_SQLALCHEMY:
    registry.register("nusadb", "nusadb.sqlalchemy", "NusaDialect")
    registry.register("nusadb.asyncio", "nusadb.sqlalchemy", "NusaDialect_asyncio")

    class Mood(enum.Enum):
        happy = "happy"
        sad = "sad"

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "sa_authors"
        id = Column(Integer, primary_key=True)
        name = Column(String(40), nullable=False, unique=True)
        books = relationship("Book", back_populates="author", cascade="all, delete-orphan")

    class Book(Base):
        __tablename__ = "sa_books"
        __table_args__ = (
            Index("sa_books_title", "title"),
            CheckConstraint("pages > 0", name="sa_books_pages_positive"),
        )
        id = Column(BigInteger, primary_key=True)
        author_id = Column(Integer, ForeignKey("sa_authors.id", ondelete="CASCADE"), nullable=False)
        title = Column(Text, nullable=False)
        pages = Column(Integer, nullable=False)
        price = Column(Numeric(10, 2))
        published = Column(Date)
        added = Column(DateTime(timezone=True))
        read_time = Column(Interval)
        cover = Column(LargeBinary)
        ref = Column(Uuid)
        meta = Column(JSON)
        mood = Column(Enum(Mood))
        in_print = Column(Boolean, default=True)
        author = relationship("Author", back_populates="books")

    class Plain(Base):
        __tablename__ = "sa_plain"
        id = Column(Integer, primary_key=True, autoincrement=False)
        name = Column(String)


def _server_url(server: _Server, driver: str = "nusadb") -> str:
    return f"{driver}://u@127.0.0.1:{server.port}/nusadb"


@unittest.skipUnless(HAVE_SQLALCHEMY, "SQLAlchemy not installed")
class SqlAlchemyTests(unittest.TestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()
        cls.engine = create_engine(_server_url(cls.server), pool_pre_ping=True)
        Base.metadata.create_all(cls.engine)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.engine.dispose()
        cls.server.stop()

    def setUp(self) -> None:
        with Session(self.engine) as session:
            session.query(Book).delete()
            session.query(Author).delete()
            session.query(Plain).delete()
            session.commit()

    def test_identity_ids_come_back_through_returning(self) -> None:
        with Session(self.engine) as session:
            ann = Author(name="ann", books=[Book(title="a", pages=10), Book(title="b", pages=20)])
            bob = Author(name="bob")
            session.add_all([ann, bob])
            session.commit()
            self.assertIsNotNone(ann.id)
            self.assertNotEqual(ann.id, bob.id)
            self.assertEqual(sorted(b.id is not None for b in ann.books), [True, True])
            self.assertEqual(
                session.scalars(select(Book.title).where(Book.author_id == ann.id).order_by(Book.title)).all(),
                ["a", "b"],
            )

    def test_bulk_insert_returns_every_id_in_order(self) -> None:
        with Session(self.engine) as session:
            author = Author(name="bulk")
            session.add(author)
            session.flush()
            rows = session.scalars(
                sa.insert(Book).returning(Book.id, sort_by_parameter_order=True),
                [{"author_id": author.id, "title": f"t{i}", "pages": i + 1} for i in range(50)],
            ).all()
            self.assertEqual(len(rows), 50)
            self.assertEqual(rows, sorted(rows))
            session.commit()
            self.assertEqual(session.scalar(select(func.count()).select_from(Book)), 50)

    def test_types_round_trip(self) -> None:
        added = dt.datetime(2026, 10, 9, 8, 30, 15, 250000, tzinfo=dt.timezone.utc)
        ref = uuid.uuid4()
        with Session(self.engine) as session:
            book = Book(
                author=Author(name="types"),
                title="t",
                pages=3,
                price=Decimal("12.50"),
                published=dt.date(2026, 1, 2),
                added=added,
                read_time=dt.timedelta(hours=5, minutes=3),
                cover=b"\x00\xffbin",
                ref=ref,
                meta={"tags": ["x", "y"], "n": 1},
                mood=Mood.sad,
            )
            session.add(book)
            session.commit()
            book_id = book.id
        with Session(self.engine) as session:
            got = session.get(Book, book_id)
            self.assertEqual(got.price, Decimal("12.50"))
            self.assertEqual(got.published, dt.date(2026, 1, 2))
            self.assertEqual(got.added, added)
            self.assertEqual(got.read_time, dt.timedelta(hours=5, minutes=3))
            self.assertEqual(got.cover, b"\x00\xffbin")
            self.assertEqual(got.ref, ref)
            self.assertEqual(got.meta, {"tags": ["x", "y"], "n": 1})
            self.assertIs(got.mood, Mood.sad)
            self.assertIs(got.in_print, True)

    def test_json_elements_scalars_and_text_columns(self) -> None:
        with Session(self.engine) as session:
            author = Author(name="json")
            session.add(author)
            session.flush()
            docs = [{"n": 1, "tags": ["x", "y"]}, {"n": 2, "tags": ["z"]}, "hello", [1, 2], None]
            for i, doc in enumerate(docs):
                session.add(Book(author_id=author.id, title=f"j{i}", pages=1, meta=doc))
            session.commit()
            by_title = dict(session.execute(select(Book.title, Book.meta)).all())
            self.assertEqual([by_title[f"j{i}"] for i in range(5)], docs)
            self.assertEqual(
                session.scalars(
                    select(Book.title).where(Book.meta["n"].as_integer() == 2)
                ).all(),
                ["j1"],
            )
            self.assertEqual(
                session.scalar(select(Book.meta[("tags", 1)].as_string()).where(Book.title == "j0")),
                "y",
            )
            self.assertEqual(
                session.scalar(select(Book.meta["tags"]).where(Book.title == "j0")), ["x", "y"]
            )
            # A JSON value bound into an expression is JSON to the server, not text.
            bound = sa.bindparam("doc", {"a": {"b": 5}}, type_=JSON)
            self.assertEqual(session.scalar(select(bound[("a", "b")].as_integer())), 5)
        with self.engine.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE IF NOT EXISTS sa_raw (id INT PRIMARY KEY, doc TEXT)")
            conn.exec_driver_sql("DELETE FROM sa_raw")
            conn.exec_driver_sql("""INSERT INTO sa_raw VALUES (1, '{"k": [1, 2]}')""")
        raw = sa.Table("sa_raw", sa.MetaData(), Column("id", Integer), Column("doc", JSON))
        with self.engine.connect() as conn:
            self.assertEqual(conn.scalar(select(raw.c.doc)), {"k": [1, 2]})

    def test_update_delete_and_cascade(self) -> None:
        with Session(self.engine) as session:
            author = Author(name="c", books=[Book(title="x", pages=1)])
            session.add(author)
            session.commit()
            author.name = "c2"
            session.commit()
            self.assertEqual(session.scalar(select(Author.name).where(Author.id == author.id)), "c2")
            session.execute(sa.delete(Author).where(Author.id == author.id))
            session.commit()
            # The foreign key cascades on the server.
            self.assertEqual(session.scalar(select(func.count()).select_from(Book)), 0)

    def test_limit_offset_and_rollback(self) -> None:
        with Session(self.engine) as session:
            session.add_all([Plain(id=i, name=f"n{i}") for i in range(10)])
            session.commit()
            names = session.scalars(select(Plain.name).order_by(Plain.id).limit(3).offset(2)).all()
            self.assertEqual(names, ["n2", "n3", "n4"])
            session.add(Plain(id=100, name="gone"))
            session.flush()
            session.rollback()
            self.assertIsNone(session.get(Plain, 100))

    def test_reflection(self) -> None:
        insp = inspect(self.engine)
        self.assertIn("sa_books", insp.get_table_names())
        self.assertTrue(insp.has_table("sa_books"))
        self.assertFalse(insp.has_table("sa_nothing"))
        columns = {c["name"]: c for c in insp.get_columns("sa_books")}
        self.assertIsInstance(columns["id"]["type"], sa.BIGINT)
        self.assertTrue(columns["id"]["autoincrement"])
        self.assertFalse(columns["title"]["nullable"])
        self.assertIsInstance(columns["price"]["type"], sa.NUMERIC)
        self.assertEqual((columns["price"]["type"].precision, columns["price"]["type"].scale), (10, 2))
        self.assertTrue(columns["added"]["type"].timezone)
        self.assertEqual(insp.get_pk_constraint("sa_books")["constrained_columns"], ["id"])
        fks = insp.get_foreign_keys("sa_books")
        self.assertEqual(len(fks), 1)
        self.assertEqual(fks[0]["referred_table"], "sa_authors")
        self.assertEqual(fks[0]["referred_columns"], ["id"])
        self.assertEqual(fks[0]["constrained_columns"], ["author_id"])
        self.assertEqual(fks[0]["options"].get("ondelete"), "CASCADE")
        self.assertEqual(
            [i["name"] for i in insp.get_indexes("sa_books")], ["sa_books_title"]
        )
        self.assertEqual(
            [u["column_names"] for u in insp.get_unique_constraints("sa_authors")], [["name"]]
        )
        checks = insp.get_check_constraints("sa_books")
        self.assertEqual([c["name"] for c in checks], ["sa_books_pages_positive"])
        # A whole table reflected back can be used.
        meta = sa.MetaData()
        books = sa.Table("sa_books", meta, autoload_with=self.engine)
        self.assertIn("author_id", books.c)

    def test_text_that_looks_like_a_placeholder_is_left_alone(self) -> None:
        with self.engine.connect() as conn:
            self.assertEqual(conn.exec_driver_sql("SELECT '10:30'").scalar(), "10:30")
            self.assertEqual(
                conn.scalar(select(sa.literal_column("'a:1'"), sa.literal(5))), "a:1"
            )
        with Session(self.engine) as session:
            session.add(Plain(id=900, name="meet at 10:30, room :2"))
            session.commit()
            self.assertEqual(
                session.scalar(select(Plain.name).where(Plain.id == 900)), "meet at 10:30, room :2"
            )

    def test_check_constraints_belong_to_their_table(self) -> None:
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                "CREATE TABLE IF NOT EXISTS sa_check_other (v INT, CONSTRAINT sa_other_v CHECK (v < 9))"
            )
        insp = inspect(self.engine)
        self.assertEqual(
            [c["name"] for c in insp.get_check_constraints("sa_check_other")], ["sa_other_v"]
        )
        self.assertNotIn(
            "sa_other_v", [c["name"] for c in insp.get_check_constraints("sa_books")]
        )

    def test_which_errors_mean_a_lost_connection(self) -> None:
        import nusadb

        dialect = self.engine.dialect
        lost = nusadb.connection.from_sqlstate("08006", "connection failure")
        shutdown = nusadb.connection.from_sqlstate("57P01", "terminating connection")
        duplicate = nusadb.connection.from_sqlstate("23505", "duplicate key")
        conflict = nusadb.connection.from_sqlstate("40001", "could not serialize")
        self.assertTrue(dialect.is_disconnect(lost, None, None))
        self.assertTrue(dialect.is_disconnect(shutdown, None, None))
        self.assertFalse(dialect.is_disconnect(duplicate, None, None))
        self.assertFalse(dialect.is_disconnect(conflict, None, None))
        self.assertTrue(dialect.is_disconnect(nusadb.OperationalError("server closed"), None, None))
        self.assertFalse(
            dialect.is_disconnect(nusadb.TransactionRolledBack("rolled back"), None, None)
        )
        self.assertTrue(dialect.is_disconnect(nusadb.InterfaceError("connection is closed"), None, None))
        # A driver error on a connection that is still in step is not a lost connection.
        with self.engine.connect() as conn:
            raw = conn.connection.dbapi_connection
            failure = nusadb.OperationalError("COPY source read failed")
            self.assertFalse(dialect.is_disconnect(failure, raw, None))
            raw._fail()
            self.assertTrue(dialect.is_disconnect(failure, raw, None))
            conn.invalidate()

    def test_views_and_schemas(self) -> None:
        with self.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS sa_other")
            conn.exec_driver_sql("CREATE TABLE IF NOT EXISTS sa_other.t (id INT PRIMARY KEY)")
            conn.exec_driver_sql("CREATE OR REPLACE VIEW sa_view AS SELECT id FROM sa_plain")
        insp = inspect(self.engine)
        self.assertIn("sa_other", insp.get_schema_names())
        self.assertEqual(insp.get_table_names(schema="sa_other"), ["t"])
        self.assertTrue(insp.has_table("t", schema="sa_other"))
        self.assertFalse(insp.has_table("t"))
        self.assertIn("sa_view", insp.get_view_names())
        self.assertIn("sa_plain", insp.get_view_definition("sa_view"))

    def test_isolation_levels_and_autocommit(self) -> None:
        with self.engine.connect().execution_options(isolation_level="SERIALIZABLE") as conn:
            self.assertEqual(conn.exec_driver_sql("SHOW transaction_isolation").scalar(), "serializable")
            self.assertEqual(conn.get_isolation_level(), "SERIALIZABLE")
        with self.engine.connect() as conn:
            self.assertEqual(conn.get_isolation_level(), "READ COMMITTED")
        auto = self.engine.execution_options(isolation_level="AUTOCOMMIT")
        with auto.connect() as conn:
            conn.execute(sa.insert(Plain).values(id=500, name="auto"))
            # No commit: in autocommit mode the insert has already been saved.
        with Session(self.engine) as session:
            self.assertIsNotNone(session.get(Plain, 500))

    def test_a_lost_connection_is_replaced(self) -> None:
        with self.engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
            raw = conn.connection.dbapi_connection
        raw._sock.close()  # the pooled connection's socket dies behind the pool's back
        with self.engine.connect() as conn:
            self.assertEqual(conn.exec_driver_sql("SELECT 41 + 1").scalar(), 42)


@unittest.skipUnless(HAVE_SQLALCHEMY and HAVE_GREENLET, "SQLAlchemy asyncio not installed")
class SqlAlchemyAsyncTests(unittest.IsolatedAsyncioTestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    async def test_orm_through_the_asyncio_dialect(self) -> None:
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

        engine = create_async_engine(_server_url(self.server, "nusadb+asyncio"))
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            async with AsyncSession(engine, expire_on_commit=False) as session:
                author = Author(name="async", books=[Book(title="a", pages=1, price=Decimal("1.10"))])
                session.add(author)
                await session.commit()
                self.assertIsNotNone(author.id)
                titles = (await session.scalars(select(Book.title))).all()
                self.assertEqual(titles, ["a"])
                price = await session.scalar(select(Book.price))
                self.assertEqual(price, Decimal("1.10"))
                await session.execute(sa.update(Book).values(pages=5))
                await session.rollback()
                self.assertEqual(await session.scalar(select(Book.pages)), 1)
            async with engine.connect() as conn:
                tables = await conn.run_sync(lambda c: inspect(c).get_table_names())
                self.assertIn("sa_books", tables)
            auto = engine.execution_options(isolation_level="AUTOCOMMIT")
            async with auto.connect() as conn:
                await conn.execute(sa.insert(Plain).values(id=1, name="auto"))
            async with AsyncSession(engine) as session:
                self.assertIsNotNone(await session.get(Plain, 1))
        finally:
            await engine.dispose()


if __name__ == "__main__":
    unittest.main()
