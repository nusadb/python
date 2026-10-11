"""The Django backend against a real ``nusadb-server``: migrations, the ORM's queries, transactions
and introspection.

``setUpModule`` starts a server on a free port between 5700 and 5799 (set ``NUSADB_TEST_PORTS``
to ``first-last`` for another range), creates the test database with ``migrate`` and drops it at
the end. The server binary is found as in the driver's own tests (``NUSADB_SERVER_BIN``).
"""

from __future__ import annotations

import datetime
import io
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

HERE = Path(__file__).resolve().parent
# Import the installed Django before tests/ goes on sys.path: its django directory would otherwise
# be found as a namespace package.
_saved_path = sys.path[:]
sys.path[:] = [p for p in sys.path if Path(p or ".").resolve() != HERE.parent]
if getattr(sys.modules.get("django"), "setup", None) is None:
    sys.modules.pop("django", None)
import django  # noqa: E402

sys.path[:] = _saved_path
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parents[1]))

os.environ["DJANGO_SETTINGS_MODULE"] = "nusadb_test_settings"

django.setup()

from django.core.management import call_command  # noqa: E402
from django.db import (  # noqa: E402
    DatabaseError,
    IntegrityError,
    connection,
    connections,
    transaction,
)
from django.db.models import (  # noqa: E402
    Avg,
    Case,
    CharField,
    Count,
    Exists,
    ExpressionWrapper,
    F,
    IntegerField,
    JSONField,
    Max,
    Min,
    OuterRef,
    Q,
    Subquery,
    Sum,
    Value,
    When,
    Window,
)
from django.db.models.fields.json import KT  # noqa: E402
from django.db.models.functions import (  # noqa: E402
    Cast,
    Coalesce,
    Concat,
    ExtractHour,
    ExtractMonth,
    ExtractYear,
    Length,
    Lower,
    Now,
    Rank,
    RowNumber,
    StrIndex,
    Substr,
    TruncDate,
    TruncDay,
    TruncHour,
    TruncMonth,
    TruncYear,
    Upper,
)
from django.test import TestCase, TransactionTestCase  # noqa: E402
from django.test.utils import setup_test_environment, teardown_test_environment  # noqa: E402
from django.utils import timezone  # noqa: E402

from bookshop.models import Author, Book, Bookshelf, Tag  # noqa: E402
from test_driver import _server_binary, _wait_ready  # noqa: E402

UTC = datetime.timezone.utc
JAKARTA = datetime.timezone(datetime.timedelta(hours=7))


class _Server:
    """A ``nusadb-server`` child process with a temporary data directory."""

    def __init__(self) -> None:
        binary = str(_server_binary())
        first, last = (int(p) for p in os.environ.get("NUSADB_TEST_PORTS", "5700-5799").split("-"))
        for port in range(first, last + 1):
            if not _port_free(port):
                continue
            self.port = port
            self._tmp = tempfile.TemporaryDirectory()
            env = {k: v for k, v in os.environ.items() if k not in ("NUSADB_USER", "NUSADB_PASSWORD")}
            env["RUST_LOG"] = "error"
            self._proc = subprocess.Popen(
                [binary, "--listen", f"127.0.0.1:{port}", "--data-dir", self._tmp.name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
            )
            try:
                _wait_ready(port)
            except RuntimeError:
                self.stop()
                continue
            time.sleep(0.15)
            if self._proc.poll() is None:
                return
            self._tmp.cleanup()
        raise RuntimeError(f"could not start nusadb-server on a port in {first}-{last}")

    def stop(self) -> None:
        self._proc.terminate()
        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait()
        self._tmp.cleanup()


def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


_server: Optional[_Server] = None
_old_name: Optional[str] = None


def setUpModule() -> None:  # noqa: N802 - unittest's name
    global _server, _old_name
    if os.environ.get("NUSADB_TEST_HOST"):
        port = os.environ.get("NUSADB_TEST_PORT", "5678")
    else:
        _server = _Server()
        port = str(_server.port)
    for alias in connections:
        connections[alias].settings_dict["PORT"] = port
    setup_test_environment()
    _old_name = connection.creation.create_test_db(verbosity=0, autoclobber=True)


def tearDownModule() -> None:  # noqa: N802 - unittest's name
    try:
        if _old_name is not None:
            connection.creation.destroy_test_db(_old_name, verbosity=0)
        teardown_test_environment()
    finally:
        if _server is not None:
            _server.stop()


def _book(author: Author, title: str, **fields: Any) -> Book:
    return Book.objects.create(author=author, title=title, **fields)


class FieldTypeTests(TestCase):
    def test_every_field_type_round_trips(self) -> None:
        author = Author.objects.create(name="Ann", born=datetime.date(1970, 5, 17))
        created = datetime.datetime(2024, 2, 29, 23, 59, 58, 123456, tzinfo=UTC)
        meta = {"tags": ["a", "b"], "nested": {"n": 1.5, "ok": True, "none": None}, "s": "é"}
        book = _book(
            author,
            "All Types",
            summary="long text " * 50,
            pages=321,
            sales=2**40,
            rating=-3,
            price=Decimal("123456.78"),
            weight=0.1,
            in_print=False,
            published=datetime.date(2024, 1, 31),
            created=created,
            starts_at=datetime.time(13, 45, 1, 250000),
            reading_time=datetime.timedelta(days=2, hours=3, microseconds=7),
            cover=b"\x00\x01\xffbinary",
            meta=meta,
            server_ip="192.168.10.20",
        )
        self.assertIsInstance(book.pk, int)
        got = Book.objects.get(pk=book.pk)
        self.assertEqual(got.summary, "long text " * 50)
        self.assertEqual(got.pages, 321)
        self.assertEqual(got.sales, 2**40)
        self.assertEqual(got.rating, -3)
        self.assertEqual(got.price, Decimal("123456.78"))
        self.assertAlmostEqual(got.weight, 0.1)
        self.assertIs(got.in_print, False)
        self.assertEqual(got.published, datetime.date(2024, 1, 31))
        self.assertEqual(got.created, created)
        self.assertEqual(got.starts_at, datetime.time(13, 45, 1, 250000))
        self.assertEqual(got.reading_time, datetime.timedelta(days=2, hours=3, microseconds=7))
        self.assertIsInstance(got.uid, uuid.UUID)
        self.assertEqual(got.uid, book.uid)
        self.assertEqual(bytes(got.cover), b"\x00\x01\xffbinary")
        self.assertEqual(got.meta, meta)
        self.assertEqual(got.server_ip, "192.168.10.20")
        self.assertEqual(got.author.born, datetime.date(1970, 5, 17))

    def test_json_scalars_and_ipv6(self) -> None:
        author = Author.objects.create(name="Bea")
        for i, value in enumerate(["text", "1", 1, 2.5, True, [1, [2]], {}]):
            book = _book(author, f"json {i}", meta=value)
            self.assertEqual(Book.objects.get(pk=book.pk).meta, value)
        # A JSON null, as opposed to SQL NULL (which the column refuses).
        book = _book(author, "json null", meta=Value(None, JSONField()))
        self.assertIsNone(Book.objects.get(pk=book.pk).meta)
        self.assertEqual(Book.objects.filter(meta=Value(None, JSONField())).count(), 1)
        book = _book(author, "v6", server_ip="2001:db8::1")
        self.assertEqual(Book.objects.get(pk=book.pk).server_ip, "2001:db8::1")
        self.assertEqual(Book.objects.filter(server_ip="2001:db8::1").count(), 1)

    def test_null_values(self) -> None:
        author = Author.objects.create(name="Cid")
        book = _book(author, "Nulls")
        got = Book.objects.get(pk=book.pk)
        for name in ("rating", "price", "weight", "published", "starts_at", "reading_time",
                     "cover", "server_ip", "editor"):
            self.assertIsNone(getattr(got, name), name)


class CrudTests(TestCase):
    @classmethod
    def setUpTestData(cls) -> None:
        cls.ann = Author.objects.create(name="Ann")
        cls.bob = Author.objects.create(name="Bob")

    def test_create_read_update_delete(self) -> None:
        book = _book(self.ann, "Draft", pages=10)
        book.title = "Final"
        book.pages = F("pages") + 5
        book.save()
        book.refresh_from_db()
        self.assertEqual((book.title, book.pages), ("Final", 15))
        self.assertEqual(Book.objects.filter(pk=book.pk).update(pages=20), 1)
        self.assertEqual(Book.objects.get(pk=book.pk).pages, 20)
        deleted, per_model = Book.objects.filter(pk=book.pk).delete()
        self.assertEqual(deleted, 1)
        self.assertFalse(Book.objects.filter(pk=book.pk).exists())

    def test_cascade_and_set_null(self) -> None:
        author = Author.objects.create(name="Gone")
        book = _book(author, "Orphan", editor=self.bob)
        edited = _book(self.ann, "Edited", editor=author)
        author.delete()
        self.assertFalse(Book.objects.filter(pk=book.pk).exists())
        edited.refresh_from_db()
        self.assertIsNone(edited.editor)

    def test_unique_together_and_check_constraint(self) -> None:
        _book(self.ann, "Same")
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                _book(self.ann, "Same")
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                _book(self.ann, "Negative", pages=-1)
        # The same title for another author is fine.
        _book(self.bob, "Same")
        self.assertEqual(Book.objects.filter(title="Same").count(), 2)

    def test_partial_unique_constraint(self) -> None:
        Bookshelf.objects.create(label="a", code=0, place="hall")
        Bookshelf.objects.create(label="b", code=0, place="hall")  # code 0: outside the condition
        Bookshelf.objects.create(label="c", code=5, place="hall")
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                Bookshelf.objects.create(label="d", code=5, place="hall")
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                Bookshelf.objects.create(label="e", code=-1)

    def test_in_bulk_and_iterator(self) -> None:
        books = [_book(self.ann, f"b{i}") for i in range(5)]
        found = Book.objects.in_bulk([b.pk for b in books])
        self.assertEqual(set(found), {b.pk for b in books})
        self.assertEqual(
            [b.title for b in Book.objects.order_by("title").iterator(chunk_size=2)],
            [f"b{i}" for i in range(5)],
        )


class LookupTests(TestCase):
    @classmethod
    def setUpTestData(cls) -> None:
        cls.ann = Author.objects.create(name="Ann Lee")
        cls.bob = Author.objects.create(name="bob stone")
        cls.b1 = _book(
            cls.ann,
            "The Quick Fox",
            pages=100,
            published=datetime.date(2020, 3, 15),
            created=datetime.datetime(2020, 3, 15, 22, 30, tzinfo=UTC),
            price=Decimal("10.00"),
            meta={"lang": "en", "year": 2020, "dims": {"w": 10, "h": 20}, "list": [1, 2]},
        )
        cls.b2 = _book(
            cls.ann,
            "100% Pure_Data",
            pages=250,
            published=datetime.date(2021, 7, 1),
            created=datetime.datetime(2021, 7, 1, 1, 0, tzinfo=UTC),
            meta={"lang": "id", "year": 2021, "dims": {"w": 5}, "flag": None},
        )
        cls.b3 = _book(
            cls.bob,
            "slow turtle",
            pages=50,
            published=None,
            created=datetime.datetime(2022, 12, 31, 23, 0, tzinfo=UTC),
            meta={},
        )

    def titles(self, **lookups: Any) -> list:
        return sorted(Book.objects.filter(**lookups).values_list("title", flat=True))

    def test_text_lookups(self) -> None:
        self.assertEqual(self.titles(title__exact="slow turtle"), ["slow turtle"])
        self.assertEqual(self.titles(title__iexact="SLOW TURTLE"), ["slow turtle"])
        self.assertEqual(self.titles(title__contains="Quick"), ["The Quick Fox"])
        self.assertEqual(self.titles(title__icontains="quick"), ["The Quick Fox"])
        self.assertEqual(self.titles(title__startswith="The"), ["The Quick Fox"])
        self.assertEqual(self.titles(title__istartswith="the"), ["The Quick Fox"])
        self.assertEqual(self.titles(title__endswith="Fox"), ["The Quick Fox"])
        self.assertEqual(self.titles(title__iendswith="TURTLE"), ["slow turtle"])
        # LIKE wildcards in the value match themselves.
        self.assertEqual(self.titles(title__contains="100%"), ["100% Pure_Data"])
        self.assertEqual(self.titles(title__contains="e_D"), ["100% Pure_Data"])
        self.assertEqual(self.titles(title__contains="_"), ["100% Pure_Data"])
        self.assertEqual(self.titles(title__contains="%"), ["100% Pure_Data"])
        # An expression on the right: the wildcards are escaped on the server.
        self.assertEqual(
            sorted(Author.objects.filter(name__icontains=F("name")).values_list("name", flat=True)),
            ["Ann Lee", "bob stone"],
        )
        self.assertEqual(
            list(Book.objects.filter(title__startswith=Substr("title", 1, 3)).order_by("pk")
                 .values_list("title", flat=True)),
            ["The Quick Fox", "100% Pure_Data", "slow turtle"],
        )

    def test_regex_lookups(self) -> None:
        self.assertEqual(self.titles(title__regex=r"^[0-9]+%"), ["100% Pure_Data"])
        self.assertEqual(self.titles(title__iregex=r"quick\s+fox$"), ["The Quick Fox"])

    def test_comparison_and_membership(self) -> None:
        self.assertEqual(self.titles(pages__gt=100), ["100% Pure_Data"])
        self.assertEqual(self.titles(pages__gte=100), ["100% Pure_Data", "The Quick Fox"])
        self.assertEqual(self.titles(pages__lt=100), ["slow turtle"])
        self.assertEqual(self.titles(pages__lte=50), ["slow turtle"])
        self.assertEqual(self.titles(pages__in=[50, 250]), ["100% Pure_Data", "slow turtle"])
        self.assertEqual(self.titles(pages__in=[]), [])
        self.assertEqual(self.titles(pages__range=(60, 250)), ["100% Pure_Data", "The Quick Fox"])
        self.assertEqual(self.titles(published__isnull=True), ["slow turtle"])
        self.assertEqual(len(self.titles(published__isnull=False)), 2)
        self.assertEqual(self.titles(price__isnull=False), ["The Quick Fox"])
        self.assertEqual(
            self.titles(author__in=Author.objects.filter(name__startswith="bob")), ["slow turtle"]
        )

    def test_date_and_time_extraction(self) -> None:
        self.assertEqual(self.titles(published__year=2020), ["The Quick Fox"])
        self.assertEqual(self.titles(published__month=7), ["100% Pure_Data"])
        self.assertEqual(self.titles(published__day=15), ["The Quick Fox"])
        self.assertEqual(self.titles(published__week_day=1), ["The Quick Fox"])  # a Sunday
        self.assertEqual(self.titles(published__iso_week_day=7), ["The Quick Fox"])
        self.assertEqual(self.titles(published__quarter=3), ["100% Pure_Data"])
        self.assertEqual(self.titles(published__gte=datetime.date(2021, 1, 1)), ["100% Pure_Data"])
        self.assertEqual(self.titles(created__year=2022), ["slow turtle"])
        self.assertEqual(self.titles(created__month=3), ["The Quick Fox"])
        self.assertEqual(self.titles(created__hour=22), ["The Quick Fox"])
        self.assertEqual(self.titles(created__date=datetime.date(2021, 7, 1)), ["100% Pure_Data"])
        self.assertEqual(self.titles(created__iso_year=2020), ["The Quick Fox"])

    def test_extraction_in_another_time_zone(self) -> None:
        # 2022-12-31 23:00 UTC is already 2023 in Jakarta (+07:00).
        with timezone.override(JAKARTA):
            self.assertEqual(self.titles(created__year=2023), ["slow turtle"])
            self.assertEqual(self.titles(created__date=datetime.date(2020, 3, 16)), ["The Quick Fox"])
        # A zone with daylight saving time: 22:30 UTC on 2020-03-15 is 17:30 in Chicago (CDT).
        with timezone.override("America/Chicago"):
            self.assertEqual(self.titles(created__hour=17, created__month=3), ["The Quick Fox"])
            self.assertEqual(self.titles(created__date=datetime.date(2021, 6, 30)), ["100% Pure_Data"])
            self.assertEqual(self.titles(created__hour=17, created__day=31), ["slow turtle"])

    def test_json_key_lookups(self) -> None:
        self.assertEqual(self.titles(meta__lang="en"), ["The Quick Fox"])
        self.assertEqual(self.titles(meta__year__gte=2021), ["100% Pure_Data"])
        self.assertEqual(self.titles(meta__dims__w=5), ["100% Pure_Data"])
        self.assertEqual(self.titles(meta__list__1=2), ["The Quick Fox"])
        self.assertEqual(self.titles(meta__has_key="flag"), ["100% Pure_Data"])
        self.assertEqual(self.titles(meta__has_keys=["lang", "dims"]), ["100% Pure_Data", "The Quick Fox"])
        self.assertEqual(self.titles(meta__has_any_keys=["list", "flag"]), ["100% Pure_Data", "The Quick Fox"])
        self.assertEqual(self.titles(meta__contains={"lang": "id"}), ["100% Pure_Data"])
        self.assertEqual(self.titles(meta__contained_by={"lang": "en", "year": 2020, "dims": {"w": 10, "h": 20}, "list": [1, 2], "x": 1}), ["The Quick Fox", "slow turtle"])
        self.assertEqual(self.titles(meta__flag=None), ["100% Pure_Data"])
        self.assertEqual(self.titles(meta__flag__isnull=False), ["100% Pure_Data"])
        self.assertEqual(self.titles(meta__lang__isnull=True), ["slow turtle"])
        self.assertEqual(self.titles(meta__lang__in=["en", "id"]), ["100% Pure_Data", "The Quick Fox"])
        self.assertEqual(self.titles(meta__lang__icontains="E"), ["The Quick Fox"])
        self.assertEqual(self.titles(meta__dims__h__isnull=False), ["The Quick Fox"])
        values = Book.objects.order_by("pk").values_list("meta__lang", "meta__dims__w")
        self.assertEqual(list(values), [("en", 10), ("id", 5), (None, None)])
        self.assertEqual(
            list(Book.objects.filter(pk=self.b1.pk).annotate(lang=KT("meta__lang")).values_list("lang", flat=True)),
            ["en"],
        )


class RelationTests(TestCase):
    @classmethod
    def setUpTestData(cls) -> None:
        cls.ann = Author.objects.create(name="Ann")
        cls.bob = Author.objects.create(name="Bob")
        # The data migration adds these; a TransactionTestCase run before may have emptied the table.
        cls.fiction = Tag.objects.get_or_create(name="fiction")[0]
        cls.history = Tag.objects.get_or_create(name="history")[0]
        for i in range(3):
            book = _book(cls.ann, f"ann {i}", editor=cls.bob, pages=10 * (i + 1))
            book.tags.add(cls.fiction)
            if i:
                book.tags.add(cls.history)
        _book(cls.bob, "bob 0", pages=5)

    def test_select_related(self) -> None:
        with self.assertNumQueries(1):
            books = list(Book.objects.select_related("author", "editor").order_by("title"))
            self.assertEqual([b.author.name for b in books], ["Ann", "Ann", "Ann", "Bob"])
            self.assertEqual([b.editor and b.editor.name for b in books], ["Bob", "Bob", "Bob", None])

    def test_prefetch_related(self) -> None:
        with self.assertNumQueries(3):
            authors = list(Author.objects.prefetch_related("books__tags").order_by("name"))
            tags = {b.title: sorted(t.name for t in b.tags.all()) for a in authors for b in a.books.all()}
        self.assertEqual(tags["ann 0"], ["fiction"])
        self.assertEqual(tags["ann 2"], ["fiction", "history"])
        self.assertEqual(tags["bob 0"], [])

    def test_many_to_many(self) -> None:
        book = Book.objects.get(title="bob 0")
        book.tags.set([self.fiction, self.history])
        self.assertEqual(book.tags.count(), 2)
        book.tags.remove(self.fiction)
        self.assertEqual(list(book.tags.values_list("name", flat=True)), ["history"])
        book.tags.clear()
        self.assertEqual(book.tags.count(), 0)
        self.assertEqual(Tag.objects.filter(books__author=self.ann).distinct().count(), 2)

    def test_reverse_and_related_filters(self) -> None:
        self.assertEqual(
            list(Author.objects.filter(books__tags__name="history").distinct().values_list("name", flat=True)),
            ["Ann"],
        )
        self.assertEqual(Author.objects.filter(edited_books__isnull=False).distinct().count(), 1)
        shelf = Bookshelf.objects.create(label="s1", code=1, keeper=self.ann)
        shelf.books.add(*Book.objects.filter(author=self.ann))
        self.assertEqual(self.ann.shelves.get().books.count(), 3)


class AggregationTests(TestCase):
    @classmethod
    def setUpTestData(cls) -> None:
        cls.ann = Author.objects.create(name="Ann")
        cls.bob = Author.objects.create(name="Bob")
        cls.cid = Author.objects.create(name="Cid")
        rows = [
            (cls.ann, "a1", 100, Decimal("10.50"), 4, datetime.datetime(2023, 1, 5, 10, 15, tzinfo=UTC)),
            (cls.ann, "a2", 300, Decimal("20.00"), None, datetime.datetime(2023, 1, 20, 23, 50, tzinfo=UTC)),
            (cls.bob, "b1", 200, None, 2, datetime.datetime(2023, 2, 1, 0, 30, tzinfo=UTC)),
            (cls.bob, "b2", 50, Decimal("5.25"), 5, datetime.datetime(2024, 6, 1, 12, 0, tzinfo=UTC)),
        ]
        for author, title, pages, price, rating, created in rows:
            _book(author, title, pages=pages, price=price, rating=rating, created=created)

    def test_aggregate(self) -> None:
        result = Book.objects.aggregate(
            total=Sum("pages"), mean=Avg("pages"), top=Max("price"), low=Min("price"), n=Count("rating")
        )
        self.assertEqual(result["total"], 650)
        self.assertAlmostEqual(result["mean"], 162.5)
        self.assertEqual(result["top"], Decimal("20.00"))
        self.assertEqual(result["low"], Decimal("5.25"))
        self.assertEqual(result["n"], 3)
        self.assertEqual(Book.objects.filter(pages__gt=1000).aggregate(s=Sum("pages"))["s"], None)

    def test_annotate_with_f_q_case_when(self) -> None:
        authors = Author.objects.annotate(
            pages=Sum("books__pages"),
            big=Count("books", filter=Q(books__pages__gte=200)),
            size=Case(
                When(pages__gte=350, then=Value("large")),
                When(pages__gt=0, then=Value("small")),
                default=Value("none"),
                output_field=CharField(),
            ),
        ).order_by("name")
        self.assertEqual(
            [(a.name, a.pages, a.big, a.size) for a in authors],
            [("Ann", 400, 1, "large"), ("Bob", 250, 1, "small"), ("Cid", None, 0, "none")],
        )
        doubled = Book.objects.annotate(
            double=ExpressionWrapper(F("pages") * 2, output_field=IntegerField())
        ).filter(double__gt=F("pages") + 150)
        self.assertEqual(sorted(doubled.values_list("title", flat=True)), ["a2", "b1"])
        self.assertEqual(Book.objects.filter(Q(pages__lt=60) | Q(title="a1")).count(), 2)
        self.assertEqual(Book.objects.exclude(Q(rating__isnull=True)).count(), 3)

    def test_subquery_outerref_exists(self) -> None:
        newest = Book.objects.filter(author=OuterRef("pk")).order_by("-created")
        authors = Author.objects.annotate(
            newest_title=Subquery(newest.values("title")[:1]),
            has_cheap=Exists(Book.objects.filter(author=OuterRef("pk"), price__lt=6)),
        ).order_by("name")
        self.assertEqual(
            [(a.name, a.newest_title, a.has_cheap) for a in authors],
            [("Ann", "a2", False), ("Bob", "b2", True), ("Cid", None, False)],
        )

    def test_window_functions(self) -> None:
        books = Book.objects.annotate(
            rank=Window(Rank(), partition_by=F("author"), order_by=F("pages").desc()),
            row=Window(RowNumber(), order_by=F("title").asc()),
            running=Window(Sum("pages"), partition_by=F("author"), order_by=F("created").asc()),
        ).order_by("title")
        self.assertEqual(
            [(b.title, b.rank, b.row, b.running) for b in books],
            [("a1", 2, 1, 100), ("a2", 1, 2, 400), ("b1", 1, 3, 200), ("b2", 2, 4, 250)],
        )

    def test_values_values_list_distinct(self) -> None:
        self.assertEqual(
            list(Book.objects.order_by("title").values("title", "author__name")[:2]),
            [{"title": "a1", "author__name": "Ann"}, {"title": "a2", "author__name": "Ann"}],
        )
        self.assertEqual(
            list(Book.objects.order_by("title").values_list("title", "pages", named=True)[:1])[0].pages,
            100,
        )
        self.assertEqual(
            list(Book.objects.order_by("author__name").values_list("author__name", flat=True).distinct()),
            ["Ann", "Bob"],
        )
        first_per_author = Book.objects.order_by("author", "-pages").distinct("author")
        self.assertEqual(sorted(b.title for b in first_per_author), ["a2", "b1"])
        by_author = Book.objects.values("author__name").annotate(n=Count("id")).order_by("author__name")
        self.assertEqual(list(by_author), [{"author__name": "Ann", "n": 2}, {"author__name": "Bob", "n": 2}])

    def test_order_by_nulls(self) -> None:
        last = list(Book.objects.order_by(F("rating").asc(nulls_last=True)).values_list("title", flat=True))
        self.assertEqual(last, ["b1", "a1", "b2", "a2"])
        first = list(Book.objects.order_by(F("rating").desc(nulls_first=True)).values_list("title", flat=True))
        self.assertEqual(first, ["a2", "b2", "a1", "b1"])
        self.assertEqual(list(Book.objects.order_by("-pages").values_list("pages", flat=True)[1:3]), [200, 100])

    def test_functions(self) -> None:
        book = Book.objects.annotate(
            upper=Upper("title"),
            lower=Lower(Value("ABC")),
            length=Length("title"),
            label=Concat("title", Value("/"), "author__name", Value("/"), "pages", output_field=CharField()),
            fallback=Coalesce("price", Value(Decimal("0"))),
            as_text=Cast("pages", CharField()),
            at=StrIndex("author__name", Value("n")),
            now=Now(),
        ).get(title="b1")
        self.assertEqual((book.upper, book.lower, book.length), ("B1", "abc", 2))
        self.assertEqual(book.label, "b1/Bob/200")
        self.assertEqual(book.fallback, Decimal("0"))
        self.assertEqual(book.as_text, "200")
        self.assertEqual(book.at, 0)
        self.assertLess(abs((book.now - timezone.now()).total_seconds()), 60)

    def test_datetime_truncation(self) -> None:
        months = (
            Book.objects.annotate(month=TruncMonth("created"))
            .values("month")
            .annotate(n=Count("id"))
            .order_by("month")
        )
        self.assertEqual(
            [(m["month"], m["n"]) for m in months],
            [
                (datetime.datetime(2023, 1, 1, tzinfo=UTC), 2),
                (datetime.datetime(2023, 2, 1, tzinfo=UTC), 1),
                (datetime.datetime(2024, 6, 1, tzinfo=UTC), 1),
            ],
        )
        a2 = Book.objects.annotate(
            day=TruncDay("created"),
            hour=TruncHour("created"),
            year=TruncYear("created"),
            date=TruncDate("created"),
            y=ExtractYear("created"),
            m=ExtractMonth("created"),
            h=ExtractHour("created"),
        ).get(title="a2")
        self.assertEqual(a2.day, datetime.datetime(2023, 1, 20, tzinfo=UTC))
        self.assertEqual(a2.hour, datetime.datetime(2023, 1, 20, 23, tzinfo=UTC))
        self.assertEqual(a2.year, datetime.datetime(2023, 1, 1, tzinfo=UTC))
        self.assertEqual(a2.date, datetime.date(2023, 1, 20))
        self.assertEqual((a2.y, a2.m, a2.h), (2023, 1, 23))
        # In Jakarta, 23:50 UTC on 2023-01-20 is the morning of the 21st.
        local = Book.objects.annotate(
            day=TruncDay("created", tzinfo=JAKARTA), date=TruncDate("created", tzinfo=JAKARTA)
        ).get(title="a2")
        self.assertEqual(local.day, datetime.datetime(2023, 1, 21, tzinfo=JAKARTA))
        self.assertEqual(local.date, datetime.date(2023, 1, 21))
        published = Book.objects.annotate(month=TruncMonth("published")).filter(title="a1")
        self.assertEqual(list(published.values_list("month", flat=True)), [None])

    def test_time_and_duration_expressions(self) -> None:
        Book.objects.filter(title="a1").update(
            starts_at=datetime.time(9, 41, 30), reading_time=datetime.timedelta(hours=2)
        )
        book = Book.objects.annotate(
            hour=TruncHour("starts_at"),
            finish=ExpressionWrapper(F("created") + F("reading_time"), output_field=Book._meta.get_field("created")),
        ).get(title="a1")
        self.assertEqual(book.hour, datetime.time(9, 0))
        self.assertEqual(book.finish, datetime.datetime(2023, 1, 5, 12, 15, tzinfo=UTC))
        self.assertEqual(Book.objects.filter(reading_time__gt=datetime.timedelta(hours=1)).count(), 1)


class BulkTests(TestCase):
    @classmethod
    def setUpTestData(cls) -> None:
        cls.ann = Author.objects.create(name="Ann")

    def test_bulk_create_returns_ids(self) -> None:
        books = Book.objects.bulk_create(
            [Book(author=self.ann, title=f"bulk {i}", pages=i) for i in range(50)]
        )
        self.assertTrue(all(b.pk is not None for b in books))
        self.assertEqual(len({b.pk for b in books}), 50)
        self.assertEqual(Book.objects.filter(title__startswith="bulk").count(), 50)

    def test_bulk_create_conflicts(self) -> None:
        Tag.objects.bulk_create([Tag(name="fiction"), Tag(name="poetry")], ignore_conflicts=True)
        self.assertEqual(Tag.objects.filter(name__in=["fiction", "poetry"]).count(), 2)
        Author.objects.bulk_create(
            [Author(name="Ann", email="ann@example.com"), Author(name="New", email="new@example.com")],
            update_conflicts=True,
            unique_fields=["name"],
            update_fields=["email"],
        )
        self.assertEqual(Author.objects.get(name="Ann").email, "ann@example.com")
        self.assertTrue(Author.objects.filter(name="New").exists())

    def test_bulk_update(self) -> None:
        books = Book.objects.bulk_create(
            [Book(author=self.ann, title=f"u{i}", pages=i) for i in range(5)]
        )
        for book in books:
            book.pages += 100
            book.price = Decimal("1.50") * book.pages
            book.published = datetime.date(2020, 1, 1 + book.pages - 100)
            book.server_ip = f"10.0.0.{book.pages - 100}"
            book.meta = {"n": book.pages}
        updated = Book.objects.bulk_update(books, ["pages", "price", "published", "server_ip", "meta"], batch_size=2)
        self.assertEqual(updated, 5)
        got = Book.objects.get(title="u3")
        self.assertEqual(
            (got.pages, got.price, got.published, got.server_ip, got.meta),
            (103, Decimal("154.50"), datetime.date(2020, 1, 4), "10.0.0.3", {"n": 103}),
        )

    def test_get_or_create_and_update_or_create(self) -> None:
        tag, created = Tag.objects.get_or_create(name="history")
        self.assertFalse(created)
        tag, created = Tag.objects.get_or_create(name="drama")
        self.assertTrue(created)
        author, created = Author.objects.update_or_create(name="Ann", defaults={"email": "a@x"})
        self.assertFalse(created)
        self.assertEqual(author.email, "a@x")
        author, created = Author.objects.update_or_create(name="Zed", defaults={"email": "z@x"})
        self.assertTrue(created)


class TransactionTests(TransactionTestCase):
    available_apps = ["bookshop"]

    def setUp(self) -> None:
        self.ann = Author.objects.create(name="Ann")

    def test_atomic_commits_and_rolls_back(self) -> None:
        with transaction.atomic():
            _book(self.ann, "kept")
        with self.assertRaises(ZeroDivisionError):
            with transaction.atomic():
                _book(self.ann, "lost")
                1 / 0
        self.assertEqual(list(Book.objects.values_list("title", flat=True)), ["kept"])

    def test_nested_savepoints(self) -> None:
        with transaction.atomic():
            _book(self.ann, "outer")
            try:
                with transaction.atomic():
                    _book(self.ann, "inner")
                    raise ValueError
            except ValueError:
                pass
            with transaction.atomic():
                _book(self.ann, "second")
                with transaction.atomic():
                    _book(self.ann, "deepest")
            # A failed statement inside a savepoint leaves the transaction usable.
            try:
                with transaction.atomic():
                    _book(self.ann, "outer")
            except IntegrityError:
                pass
            self.assertEqual(Book.objects.count(), 3)
        self.assertEqual(
            sorted(Book.objects.values_list("title", flat=True)), ["deepest", "outer", "second"]
        )

    def test_manual_savepoint_rollback(self) -> None:
        with transaction.atomic():
            _book(self.ann, "one")
            sid = transaction.savepoint()
            _book(self.ann, "two")
            transaction.savepoint_rollback(sid)
            _book(self.ann, "three")
        self.assertEqual(sorted(Book.objects.values_list("title", flat=True)), ["one", "three"])

    def test_on_commit(self) -> None:
        called = []
        with transaction.atomic():
            transaction.on_commit(lambda: called.append("done"))
            self.assertEqual(called, [])
        self.assertEqual(called, ["done"])

    def test_select_for_update(self) -> None:
        book = _book(self.ann, "locked")
        other = _book(self.ann, "free")
        results = {}

        def contender() -> None:
            try:
                with transaction.atomic():
                    try:
                        with transaction.atomic():
                            list(Book.objects.select_for_update(nowait=True).filter(pk=book.pk))
                        results["nowait"] = "acquired"
                    except DatabaseError:
                        results["nowait"] = "refused"
                    skipped = Book.objects.select_for_update(skip_locked=True).order_by("pk")
                    results["skip_locked"] = list(skipped.values_list("title", flat=True))
            finally:
                connections.close_all()

        with transaction.atomic():
            locked = Book.objects.select_for_update().get(pk=book.pk)
            locked.pages = 7
            locked.save()
            thread = threading.Thread(target=contender)
            thread.start()
            thread.join(30)
        self.assertEqual(results["nowait"], "refused")
        self.assertEqual(results["skip_locked"], ["free"])
        self.assertEqual(Book.objects.get(pk=book.pk).pages, 7)
        self.assertEqual(Book.objects.select_for_update().filter(pk=other.pk).count(), 1)

    def test_isolation_level_option(self) -> None:
        settings_dict = dict(connection.settings_dict, OPTIONS={"isolation_level": "serializable"})
        wrapper = type(connections["default"])(settings_dict, alias="serializable")
        try:
            with wrapper.cursor() as cursor:
                cursor.execute("SHOW transaction_isolation")
                self.assertEqual(cursor.fetchone()[0], "serializable")
        finally:
            wrapper.close()


class ExpressionSqlTests(TestCase):
    @classmethod
    def setUpTestData(cls) -> None:
        cls.ann = Author.objects.create(name="Ann")
        _book(cls.ann, "short", pages=10, reading_time=datetime.timedelta(hours=1, microseconds=6))
        _book(cls.ann, "long", pages=17, reading_time=datetime.timedelta(days=1, minutes=30))
        _book(cls.ann, "unread", pages=3)

    def test_modulo_operator(self) -> None:
        # Django writes the modulo connector as `%%`; it must reach the server as `%`.
        rows = Book.objects.annotate(r=F("pages") % 3).order_by("pages").values_list("pages", "r")
        self.assertEqual(list(rows), [(3, 0), (10, 1), (17, 2)])
        self.assertEqual(Book.objects.filter(pages__gt=0).annotate(r=F("pages") % 3).filter(r=1).count(), 1)

    def test_sum_and_avg_over_durations(self) -> None:
        result = Book.objects.aggregate(total=Sum("reading_time"), mean=Avg("reading_time"))
        self.assertEqual(result["total"], datetime.timedelta(days=1, hours=1, minutes=30, microseconds=6))
        self.assertEqual(result["mean"], datetime.timedelta(hours=12, minutes=45, microseconds=3))
        empty = Book.objects.filter(pages__gt=100).aggregate(total=Sum("reading_time"))
        self.assertIsNone(empty["total"])
        per_author = Author.objects.annotate(total=Sum("books__reading_time")).get(pk=self.ann.pk)
        self.assertEqual(per_author.total, datetime.timedelta(days=1, hours=1, minutes=30, microseconds=6))


def _scratch_model(model_name: str, table: str, **fields: Any) -> Any:
    """A model in its own app registry, for the schema editor tests to create and alter."""
    from django.apps.registry import Apps
    from django.db import models

    meta = type("Meta", (), {"app_label": "bookshop", "apps": Apps(), "db_table": table})
    return type(model_name, (models.Model,), {"__module__": __name__, "Meta": meta, **fields})


class SchemaEditorTests(TransactionTestCase):
    available_apps = ["bookshop"]

    def tearDown(self) -> None:
        with connection.cursor() as cursor:
            for table in ("scratch_removed", "scratch_identity"):
                cursor.execute('DROP TABLE IF EXISTS "%s" CASCADE' % table)

    def test_remove_field_drops_its_indexes_and_constraints(self) -> None:
        from django.db import models

        model = _scratch_model(
            "Removed",
            "scratch_removed",
            name=models.CharField(max_length=20),
            rank=models.IntegerField(db_index=True),
            size=models.PositiveIntegerField(),
            owner=models.ForeignKey(Author, models.CASCADE, null=True),
        )
        with connection.schema_editor() as editor:
            editor.create_model(model)
        with connection.cursor() as cursor:
            before = connection.introspection.get_constraints(cursor, "scratch_removed")
        self.assertTrue(any(c["columns"] == ["rank"] and c["index"] for c in before.values()))
        self.assertTrue(any(c["columns"] == ["size"] and c["check"] for c in before.values()))
        with connection.schema_editor() as editor:
            for name in ("rank", "size", "owner"):
                editor.remove_field(model, model._meta.get_field(name))
        with connection.cursor() as cursor:
            after = connection.introspection.get_constraints(cursor, "scratch_removed")
            self.assertEqual(
                [c for c in after.values() if set(c["columns"]) & {"rank", "size", "owner_id"}], []
            )
            # A row without the removed columns is accepted.
            cursor.execute('INSERT INTO "scratch_removed" ("name") VALUES (%s)', ["kept"])
            cursor.execute('SELECT "name" FROM "scratch_removed"')
            self.assertEqual(cursor.fetchall(), [("kept",)])

    def test_remove_identity_column_drops_its_sequence(self) -> None:
        from django.db import models

        model = _scratch_model("Identity", "scratch_identity", name=models.CharField(max_length=20))
        with connection.schema_editor() as editor:
            editor.create_model(model)
        with connection.cursor() as cursor:
            sequences = connection.introspection.get_sequences(cursor, "scratch_identity")
        self.assertEqual([s["column"] for s in sequences], ["id"])
        with connection.schema_editor() as editor:
            editor.remove_field(model, model._meta.get_field("id"))
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT sequence_name FROM information_schema.sequences WHERE sequence_name = %s",
                [sequences[0]["name"]],
            )
            self.assertEqual(cursor.fetchall(), [])
        # A table of the same name gets a fresh identity column.
        with connection.schema_editor() as editor:
            editor.delete_model(model)
            editor.create_model(_scratch_model("Identity", "scratch_identity", name=models.CharField(max_length=20)))
        with connection.cursor() as cursor:
            cursor.execute('INSERT INTO "scratch_identity" ("name") VALUES (%s) RETURNING "id"', ["x"])
            self.assertEqual(cursor.fetchone()[0], 1)


class SequenceAndFlushTests(TransactionTestCase):
    available_apps = ["bookshop"]
    reset_sequences = True

    def test_ids_restart_after_flush(self) -> None:
        self.assertEqual(Author.objects.create(name="first").pk, 1)

    def test_sequence_reset_sql(self) -> None:
        Author.objects.create(pk=40, name="explicit")
        out = io.StringIO()
        call_command("sqlsequencereset", "bookshop", stdout=out, no_color=True)
        with connection.cursor() as cursor:
            for statement in out.getvalue().split(";"):
                if statement.strip().upper().startswith("SELECT"):
                    cursor.execute(statement)
        self.assertEqual(Author.objects.create(name="next").pk, 41)


class MigrationTests(TransactionTestCase):
    available_apps = ["bookshop"]
    serialized_rollback = False

    def tables(self) -> set:
        with connection.cursor() as cursor:
            return set(connection.introspection.table_names(cursor))

    def test_migrate_to_zero_and_back(self) -> None:
        self.assertIn("bookshop_bookshelf", self.tables())
        call_command("migrate", "bookshop", "zero", verbosity=0)
        self.assertFalse({t for t in self.tables() if t.startswith("bookshop_")})
        call_command("migrate", "bookshop", "0003", verbosity=0)
        self.assertIn("bookshop_shelf", self.tables())
        self.assertEqual(
            sorted(Tag.objects.values_list("name", flat=True)), ["fiction", "history", "science"]
        )
        call_command("migrate", "bookshop", verbosity=0)
        self.assertIn("bookshop_bookshelf", self.tables())
        self.assertNotIn("bookshop_shelf", self.tables())
        shelf = Bookshelf.objects.create(label="after", code=3, place="x", keeper=Author.objects.create(name="K"))
        self.assertEqual(Bookshelf.objects.get(pk=shelf.pk).keeper.name, "K")

    def test_inspectdb(self) -> None:
        out = io.StringIO()
        call_command("inspectdb", "bookshop_book", "bookshop_bookshelf", stdout=out)
        text = out.getvalue()
        for expected in (
            "class BookshopBook(models.Model):",
            "id = models.BigAutoField(primary_key=True)",
            "title = models.CharField(max_length=200)",
            "price = models.DecimalField(max_digits=8, decimal_places=2, blank=True, null=True)",
            "rating = models.SmallIntegerField(blank=True, null=True)",
            "created = models.DateTimeField()",
            "starts_at = models.TimeField(blank=True, null=True)",
            "reading_time = models.DurationField(blank=True, null=True)",
            "uid = models.UUIDField(unique=True)",
            "cover = models.BinaryField(blank=True, null=True)",
            "meta = models.JSONField()",
            "server_ip = models.GenericIPAddressField(blank=True, null=True)",
            "weight = models.FloatField(blank=True, null=True)",
            "in_print = models.BooleanField()",
            "author = models.ForeignKey('BookshopAuthor', models.DO_NOTHING)",
            "unique_together = (('title', 'author'),)",
            "class BookshopBookshelf(models.Model):",
            "label = models.CharField(unique=True, max_length=50)",
            "keeper = models.ForeignKey('BookshopAuthor', models.DO_NOTHING, blank=True, null=True)",
        ):
            self.assertIn(expected, text)

    def test_introspected_constraints(self) -> None:
        with connection.cursor() as cursor:
            constraints = connection.introspection.get_constraints(cursor, "bookshop_book")
            relations = connection.introspection.get_relations(cursor, "bookshop_book")
            sequences = connection.introspection.get_sequences(cursor, "bookshop_book")
        self.assertTrue(constraints["book_pages_gte_0"]["check"])
        self.assertEqual(constraints["book_published_idx"]["columns"], ["published"])
        self.assertTrue(any(c["primary_key"] and c["columns"] == ["id"] for c in constraints.values()))
        self.assertTrue(
            any(c["unique"] and c["columns"] == ["title", "author_id"] for c in constraints.values())
        )
        self.assertEqual(relations["author_id"], ("id", "bookshop_author"))
        self.assertEqual([s["column"] for s in sequences], ["id"])


if __name__ == "__main__":
    unittest.main()
