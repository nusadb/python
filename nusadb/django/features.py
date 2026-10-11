"""What the NusaDB server supports, as Django's feature flags.

Each flag below was checked against a running server; the comment next to a flag says what was
observed when it differs from Django's default.
"""

from __future__ import annotations

from django.db import InterfaceError
from django.db.backends.base.features import BaseDatabaseFeatures
from django.utils.functional import cached_property


class DatabaseFeatures(BaseDatabaseFeatures):
    minimum_database_version = None

    # Transactions and DDL. DDL runs inside a transaction and rolls back with it, so a migration
    # is atomic. Savepoints can be released.
    can_rollback_ddl = True
    supports_transactions = True
    uses_savepoints = True
    can_release_savepoints = True
    supports_atomic_references_rename = True
    # One action per ALTER TABLE statement.
    supports_combined_alters = False

    # Constraints are checked when each statement runs: DEFERRABLE is refused.
    can_defer_constraint_checks = False
    supports_deferrable_unique_constraints = False
    supports_forward_references = False

    # INSERT ... RETURNING, also for multi-row inserts, and ON CONFLICT.
    can_return_columns_from_insert = True
    can_return_rows_from_bulk_insert = True
    has_bulk_insert = True
    supports_ignore_conflicts = True
    supports_update_conflicts = True
    supports_update_conflicts_with_target = True
    # A CASE over parameters in a bulk update is cast to the column's type.
    requires_casted_case_in_updates = True

    # SELECT ... FOR UPDATE with NOWAIT and SKIP LOCKED, and with LIMIT. The server takes FOR
    # UPDATE only on a single table (no join), so FOR UPDATE OF and FOR NO KEY UPDATE are off.
    has_select_for_update = True
    has_select_for_update_nowait = True
    has_select_for_update_skip_locked = True
    has_select_for_update_of = False
    has_select_for_no_key_update = False
    supports_select_for_update_with_limit = True

    # Types.
    has_native_uuid_field = True
    has_native_duration_field = True
    has_native_json_field = True
    has_json_operators = True
    has_real_datatype = True
    supports_temporal_subtraction = True
    # With USE_TZ = False, DateTimeField is a plain timestamp and aware values are refused.
    supports_timezones = False
    # Only UTC and fixed offsets are known to the server; named zones are converted by the
    # backend (see DatabaseOperations), but the server has no zone database of its own.
    has_zoneinfo_database = False
    supports_unlimited_charfield = True
    supports_json_field = True
    supports_json_field_contains = True
    json_key_contains_list_matching_requires_list = True
    supports_primitives_in_json_field = True
    can_introspect_json_field = True
    has_json_object_function = True
    # COLLATE is accepted, but columns' collations cannot be read back.
    supports_collation_on_charfield = False
    supports_collation_on_textfield = False
    supports_non_deterministic_collations = False
    # COMMENT ON is accepted, but comments cannot be read back.
    supports_comments = False
    supports_comments_inline = False

    # Queries.
    can_distinct_on_fields = True
    supports_over_clause = True
    supports_frame_range_fixed_distance = True
    supports_aggregate_filter_clause = True
    supports_default_in_lead_lag = True
    supports_boolean_expr_in_select_clause = True
    supports_comparing_boolean_expr = True
    supports_order_by_nulls_modifier = True
    # NULL sorts after every value in ascending order.
    nulls_order_largest = True
    order_by_nulls_first = False
    greatest_least_ignores_nulls = True
    supports_slicing_ordering_in_compound = True
    supports_parentheses_in_compound = True
    # Every selected column must be grouped or aggregated, even when the primary key is grouped.
    allows_group_by_selected_pks = False
    supports_subqueries_in_group_by = True
    # The regular expression engine has no backreferences.
    supports_regex_backreferencing = False
    supports_logical_xor = False
    supports_paramstyle_pyformat = True
    supported_explain_formats = {"JSON", "TEXT"}

    # Schema.
    supports_tablespaces = False
    supports_sequence_reset = True
    can_introspect_default = True
    can_introspect_foreign_keys = True
    can_introspect_check_constraints = True
    can_create_inline_fk = True
    indexes_foreign_keys = True
    supports_index_on_text_field = True
    supports_partial_indexes = True
    supports_functions_in_partial_indexes = True
    supports_expression_indexes = True
    supports_covering_indexes = True
    # An index's column order (ASC/DESC) is not reported by information_schema.
    supports_index_column_ordering = False
    can_rename_index = False
    supports_column_check_constraints = True
    supports_table_check_constraints = True
    allows_multiple_constraints_on_same_fields = True
    closed_cursor_error_class = InterfaceError
    can_clone_databases = False
    test_db_allows_multiple_connections = True

    introspected_field_types = {
        **BaseDatabaseFeatures.introspected_field_types,
        "PositiveBigIntegerField": "BigIntegerField",
        "PositiveIntegerField": "IntegerField",
        "PositiveSmallIntegerField": "SmallIntegerField",
    }

    test_collations = {
        "ci": None,
        "cs": "C",
        "non_default": None,
        "swedish_ci": None,
    }
    test_now_utc_template = "STATEMENT_TIMESTAMP() AT TIME ZONE 'UTC'"

    @cached_property
    def supports_explaining_query_execution(self) -> bool:
        return True
