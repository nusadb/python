"""Models covering the field types, relations and constraints the backend has to handle."""

import uuid

from django.db import models
from django.utils import timezone


class Author(models.Model):
    name = models.CharField(max_length=100, unique=True)
    email = models.CharField(max_length=200, blank=True, default="")
    born = models.DateField(null=True)

    def __str__(self) -> str:
        return self.name


class Tag(models.Model):
    name = models.CharField(max_length=50, unique=True)


class Book(models.Model):
    title = models.CharField(max_length=200)
    summary = models.TextField(blank=True, default="")
    pages = models.IntegerField(default=0)
    sales = models.BigIntegerField(default=0)
    rating = models.SmallIntegerField(null=True)
    price = models.DecimalField(max_digits=8, decimal_places=2, null=True)
    weight = models.FloatField(null=True)
    in_print = models.BooleanField(default=True)
    published = models.DateField(null=True)
    created = models.DateTimeField(default=timezone.now)
    starts_at = models.TimeField(null=True)
    reading_time = models.DurationField(null=True)
    uid = models.UUIDField(default=uuid.uuid4, unique=True)
    cover = models.BinaryField(null=True)
    meta = models.JSONField(default=dict, blank=True)
    server_ip = models.GenericIPAddressField(null=True)
    author = models.ForeignKey(Author, models.CASCADE, related_name="books")
    editor = models.ForeignKey(
        Author, models.SET_NULL, null=True, related_name="edited_books"
    )
    tags = models.ManyToManyField(Tag, related_name="books", blank=True)

    class Meta:
        unique_together = [("title", "author")]
        constraints = [
            models.CheckConstraint(check=models.Q(pages__gte=0), name="book_pages_gte_0"),
        ]
        indexes = [models.Index(fields=["published"], name="book_published_idx")]

    def __str__(self) -> str:
        return self.title



class Bookshelf(models.Model):
    """Shelf after migration 0004: every field below was added, altered or renamed there."""

    label = models.CharField(max_length=50, unique=True)
    code = models.BigIntegerField(db_index=True)
    note = models.TextField(default="")
    old_code = models.IntegerField(null=True)
    place = models.CharField(max_length=30, null=True)
    keeper = models.ForeignKey(Author, models.SET_NULL, null=True, related_name="shelves")
    books = models.ManyToManyField(Book, related_name="shelves", blank=True)

    class Meta:
        indexes = [models.Index(fields=["place", "code"], name="shelf_place_code_idx")]
        constraints = [
            models.UniqueConstraint(
                fields=["place", "code"],
                condition=models.Q(code__gt=0),
                name="shelf_place_code_uniq",
            ),
            models.CheckConstraint(check=models.Q(code__gte=0), name="shelf_code_gte_0"),
        ]
