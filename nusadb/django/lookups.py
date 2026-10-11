"""The NusaDB forms of Django's JSON lookups and of the functions whose generic SQL the server
does not run.

Django compiles an expression with its ``as_<vendor>`` method when one exists; importing this
module adds ``as_nusadb`` to the classes below. The JSON operators (``->``, ``->>``, ``#>``,
``#>>``, ``?``, ``?|``, ``?&``, ``@>``, ``<@``) are the server's own.
"""

from __future__ import annotations

from typing import Any, Tuple

from django.conf import settings
from django.db import NotSupportedError
from django.db.models import Avg, DecimalField, Func, Sum, TextField
from django.db.models.fields.json import (
    ContainedBy,
    DataContains,
    HasAnyKeys,
    HasKey,
    HasKeyLookup,
    HasKeys,
    KeyTextTransform,
    KeyTransform,
)
from django.db.models.functions import (
    SHA1,
    SHA224,
    SHA256,
    SHA384,
    SHA512,
    Cast,
    JSONObject,
    Now,
    StrIndex,
)

_SQL = Tuple[str, Tuple[Any, ...]]


def _key_transform_sql(self: KeyTransform, compiler: Any, connection: Any) -> _SQL:
    """``doc -> key`` (``->>`` for text), or ``doc #> path`` for a nested key."""
    lhs, params, key_transforms = self.preprocess_lhs(compiler, connection)
    as_text = isinstance(self, KeyTextTransform)
    if len(key_transforms) > 1:
        operator = "#>>" if as_text else "#>"
        return "(%s %s %%s)" % (lhs, operator), tuple(params) + (key_transforms,)
    try:
        key: Any = int(self.key_name)
    except ValueError:
        key = self.key_name
    operator = "->>" if as_text else "->"
    return "(%s %s %%s)" % (lhs, operator), tuple(params) + (key,)


def _binary_operator(operator: str) -> Any:
    def as_nusadb(self: Any, compiler: Any, connection: Any) -> _SQL:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return "%s %s %s" % (lhs, operator, rhs), tuple(lhs_params) + tuple(rhs_params)

    return as_nusadb


def _has_key_operator(operator: str) -> Any:
    compile_operator = _binary_operator(operator)

    def as_nusadb(self: HasKeyLookup, compiler: Any, connection: Any) -> _SQL:
        if isinstance(self.rhs, KeyTransform):
            # has_key=KT("a__b"): test for "b" inside doc -> "a".
            *_, rhs_key_transforms = self.rhs.preprocess_lhs(compiler, connection)
            for key in rhs_key_transforms[:-1]:
                self.lhs = KeyTransform(key, self.lhs)
            self.rhs = rhs_key_transforms[-1]
        return compile_operator(self, compiler, connection)

    return as_nusadb


def _json_object_sql(self: JSONObject, compiler: Any, connection: Any, **extra: Any) -> _SQL:
    copy = self.copy()
    copy.set_source_expressions(
        [
            Cast(expression, TextField()) if index % 2 == 0 else expression
            for index, expression in enumerate(copy.get_source_expressions())
        ]
    )
    return super(JSONObject, copy).as_sql(
        compiler, connection, function="JSONB_BUILD_OBJECT", **extra
    )


def _str_index_sql(self: StrIndex, compiler: Any, connection: Any, **extra: Any) -> _SQL:
    return self.as_sql(compiler, connection, function="STRPOS", **extra)


def _now_sql(self: Now, compiler: Any, connection: Any, **extra: Any) -> _SQL:
    # The time of the statement, not of the start of its transaction. With USE_TZ = False,
    # Django works in naive local times: the local time in TIME_ZONE.
    if settings.USE_TZ:
        return self.as_sql(compiler, connection, template="STATEMENT_TIMESTAMP()", **extra)
    template, _params = connection.ops.local_time_sql("STATEMENT_TIMESTAMP()", (), settings.TIME_ZONE)
    return self.as_sql(compiler, connection, template=template.replace("%", "%%"), **extra)


def _duration_aggregate_sql(self: Any, compiler: Any, connection: Any, **extra: Any) -> _SQL:
    """``Sum`` and ``Avg`` over a ``DurationField``: the server adds up numbers, not intervals, so
    the aggregate runs over each interval's length in seconds."""
    source = self.get_source_expressions()[0]
    output = getattr(source, "output_field", None)
    if output is None or output.get_internal_type() != "DurationField":
        return self.as_sql(compiler, connection, **extra)
    copy = self.copy()
    seconds = Func(source, template="EXTRACT(EPOCH FROM %(expressions)s)", output_field=DecimalField())
    copy.set_source_expressions([seconds, *self.get_source_expressions()[1:]])
    sql, params = copy.as_sql(compiler, connection, **extra)
    return "(INTERVAL '1 second' * %s)" % sql, params


def _sha_sql(self: Any, compiler: Any, connection: Any, **extra: Any) -> _SQL:
    # The server's digest functions take bytes and return bytes.
    return self.as_sql(
        compiler,
        connection,
        template="ENCODE(%(function)s(CAST(%(expressions)s AS bytea)), 'hex')",
        **extra,
    )


def _sha1_sql(self: Any, compiler: Any, connection: Any, **extra: Any) -> _SQL:
    raise NotSupportedError("SHA1 is not supported on NusaDB.")


KeyTransform.as_nusadb = _key_transform_sql
DataContains.as_nusadb = _binary_operator("@>")
ContainedBy.as_nusadb = _binary_operator("<@")
HasKey.as_nusadb = _has_key_operator("?")
HasKeys.as_nusadb = _has_key_operator("?&")
HasAnyKeys.as_nusadb = _has_key_operator("?|")
JSONObject.as_nusadb = _json_object_sql
StrIndex.as_nusadb = _str_index_sql
Now.as_nusadb = _now_sql
Sum.as_nusadb = _duration_aggregate_sql
Avg.as_nusadb = _duration_aggregate_sql
for _sha in (SHA224, SHA256, SHA384, SHA512):
    _sha.as_nusadb = _sha_sql
SHA1.as_nusadb = _sha1_sql
