"""A data migration: the starting tags, removed again when migrating backwards."""

from django.db import migrations

TAGS = ["fiction", "history", "science"]


def add_tags(apps, schema_editor):
    Tag = apps.get_model("bookshop", "Tag")
    Tag.objects.using(schema_editor.connection.alias).bulk_create(
        [Tag(name=name) for name in TAGS]
    )


def remove_tags(apps, schema_editor):
    Tag = apps.get_model("bookshop", "Tag")
    Tag.objects.using(schema_editor.connection.alias).filter(name__in=TAGS).delete()


class Migration(migrations.Migration):
    dependencies = [("bookshop", "0001_initial")]

    operations = [migrations.RunPython(add_tags, remove_tags)]
