from enum import Enum
from typing import Callable
from dataclasses import dataclass
from generic_pipeline.config import ROW_HASH_COLUMN_NAME
from generic_pipeline.stages.ingestion import (
    upsert_append_only_table,
    upsert_full_update_table,
    upsert_drop_and_recreate_table,
)


class IngestionType(Enum):
    """Defines the way how tables are ingested."""

    # No updates, just append content
    APPEND_AND_REMOVE_ONLY = "append-and-remove-only"
    # Perform a full update (as intuitively understood)
    FULL_UPDATE = "full-update"
    # This way drops existing content and create a new one
    DROP_AND_RECREATE = "drop-and-recreate"
    # This skips the table completely
    NOT_IMPORT = "not-import"


class ColumnType(Enum):
    """Define column type that has a specific treatment during pseudonymisation."""

    FREE_TEXT = "free_text"
    OTHER_IDENTIFIABLE = "other_identifiable"
    CLIENT_ID = "client_id"
    DATE_TIME = "date_time"
    DATE = "date"


class DateTimeRoundOpt(Enum):
    """Define rounding options for the pyspark.sql.functions.date_trunc function"""

    MONTH = "month"
    HOUR = "hour"


class DatalakeZone(Enum):
    """Defines Datalake zones"""

    # Bronze contains only raw data.
    BRONZE = "bronze"
    # Silver layer is more secure (contains pseudonymised data).
    SILVER = "silver"
    # Gold layer contains enriched datasets from silver.
    GOLD = "gold"
    # Internal store for pipeline utilities (e.g. watermarks).
    INTERNAL = "internal"


@dataclass
class Table:
    """Describe a single table in the SQL"""

    name: str
    column_types: dict[ColumnType, tuple[str, ...]]
    analysed_columns: tuple[str, ...]
    update_logic: IngestionType
    table_primary_keys: tuple[str, ...] = tuple()

    # For special cases (mainly for special pipelines that treat bronze differently)
    _skip_import: bool = False
    _skip_columns: tuple[str, ...] = tuple()

    @property
    def columns_free_text(self) -> tuple[str, ...]:
        if ColumnType.FREE_TEXT not in self.column_types.keys():
            return tuple()
        return self.column_types[ColumnType.FREE_TEXT]

    @property
    def columns_identifiable(self) -> tuple[str, ...]:
        if ColumnType.OTHER_IDENTIFIABLE not in self.column_types.keys():
            return tuple()
        return self.column_types[ColumnType.OTHER_IDENTIFIABLE]

    @property
    def columns_datetime(self) -> tuple[str, ...]:
        if ColumnType.DATE_TIME not in self.column_types.keys():
            return tuple()
        return self.column_types[ColumnType.DATE_TIME]

    @property
    def columns_client_id(self) -> tuple[str, ...]:
        if ColumnType.CLIENT_ID not in self.column_types.keys():
            return tuple()
        return self.column_types[ColumnType.CLIENT_ID]

    @property
    def columns_date(self) -> tuple[str, ...]:
        if ColumnType.DATE not in self.column_types.keys():
            return tuple()
        return self.column_types[ColumnType.DATE]

    @property
    def columns_primary_keys(self) -> tuple[str, ...]:
        if self.update_logic == IngestionType.FULL_UPDATE:
            return (ROW_HASH_COLUMN_NAME,)
        return self.table_primary_keys

    @property
    def upsert_method(self) -> Callable:
        """Returns upsert method"""
        match self.update_logic:
            case IngestionType.APPEND_AND_REMOVE_ONLY:
                return upsert_append_only_table
            case IngestionType.FULL_UPDATE:
                return upsert_full_update_table
            case IngestionType.DROP_AND_RECREATE:
                return upsert_drop_and_recreate_table
        raise KeyError("Unsupported update method")

    @property
    def skip_pseudonymisation(self) -> bool:
        if len(self.column_types) == 0:
            return True
        for _val in self.column_types.values():
            if len(_val) > 0:
                return False
        return True

    @property
    def skip_feature_extraction(self) -> bool:
        if self.skip_pseudonymisation:
            return True
        if ColumnType.FREE_TEXT not in self.column_types.keys():
            return True
        return False
