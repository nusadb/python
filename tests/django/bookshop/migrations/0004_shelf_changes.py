"""Schema changes on Shelf, one operation of each kind the backend has to run (and undo)."""

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("bookshop", "0003_shelf")]

    operations = [
        migrations.AddField("shelf", "location", models.CharField(max_length=30, null=True)),
        migrations.AddField(
            "shelf",
            "owner",
            models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="shelves",
                to="bookshop.author",
            ),
        ),
        migrations.AddField(
            "shelf",
            "books",
            models.ManyToManyField(blank=True, related_name="shelves", to="bookshop.book"),
        ),
        migrations.AlterField("shelf", "code", models.BigIntegerField(db_index=True)),
        migrations.AlterField("shelf", "name", models.CharField(max_length=50, unique=True)),
        migrations.AlterField("shelf", "note", models.TextField(default="")),
        migrations.RenameField("shelf", "legacy", "old_code"),
        migrations.RemoveField("shelf", "spare"),
        migrations.AddIndex(
            "shelf", models.Index(fields=["name", "note"], name="shelf_name_note_idx")
        ),
        migrations.RemoveIndex("shelf", "shelf_name_note_idx"),
        migrations.RenameField("shelf", "location", "place"),
        migrations.AddIndex(
            "shelf", models.Index(fields=["place", "code"], name="shelf_place_code_idx")
        ),
        migrations.AddConstraint(
            "shelf",
            models.UniqueConstraint(
                fields=["place", "code"],
                condition=models.Q(code__gt=0),
                name="shelf_place_code_uniq",
            ),
        ),
        migrations.AddConstraint(
            "shelf",
            models.CheckConstraint(check=models.Q(code__gte=0), name="shelf_code_gte_0"),
        ),
        migrations.RenameField("shelf", "name", "label"),
        migrations.RenameField("shelf", "owner", "keeper"),
        migrations.RenameModel("Shelf", "Bookshelf"),
    ]
