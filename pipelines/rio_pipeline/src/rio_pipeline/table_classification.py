from generic_pipeline.common_types import ColumnType, IngestionType, Table

# Processing configuration for each table
TABLE_CONFIG: tuple[Table, ...] = (
    Table(
        name="FULL_SQL_TABLE_NAME",
        analysed_columns=(
            "COLUMN_1", "COLUMN_2"  # ... list all column in the table
        ),
        update_logic=IngestionType.FULL_UPDATE,  # Or whatever
        table_primary_keys=("COLUMN_1",),  # ... or empty = tuple()
        column_types={
            ColumnType.FREE_TEXT: (
                "COLUMN_1"  # ... or empty = tuple()
            ),
            ColumnType.OTHER_IDENTIFIABLE: (
                "COLUMN_1"  # ... or empty = tuple()
            ),
            ColumnType.CLIENT_ID: (
                "COLUMN_1"  # ... or empty = tuple()
            ),
            ColumnType.DATE_TIME: (
                "COLUMN_1"  # ... or empty = tuple()
            ),
            ColumnType.DATE: (
                "COLUMN_1"  # ... or empty = tuple()
            ),
        },
    ),
    # ... list all tables that you have in your SQL database
)
