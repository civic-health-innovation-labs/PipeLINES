import logging
from typing import Callable
from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, date_trunc, lit, sha2
from pyspark.sql.udf import UserDefinedFunction
from generic_pipeline.config import (
    CLIENT_ID_HASH_SUFFIX,
    HASH_SALT,
    RUN_FREE_TEXT_ANONYMISATION,
)
from generic_pipeline.common_types import ColumnType, DateTimeRoundOpt, Table


@dataclass
class DataframeTransformer:
    """Class for transforming data frames (for anonymisation purposes).

    Attributes:
        df (DataFrame): Actual data frame that is the subject of anonymisation.
        table_name (str): Name of the table (real SQL name).
        anonymise_udf (UserDefinedFunction): Function called for free-text anonymisation.
    """

    df: DataFrame
    table_name: str
    anonymise_udf: UserDefinedFunction

    def anonymise_free_text_columns(self, columns: tuple[str, ...]):
        """Pseudonymises free text using the pseudonymiser

        Raises:
            KeyError: If a column specified in 'columns' does not exist in the DataFrame.
        """
        for column in columns:
            if column in self.df.columns:
                # Perform free text anonymisation
                logging.info(
                    f"Anonymising free-text column: {column} in table: {self.table_name}"
                )
                self.df = self.df.withColumn(column, self.anonymise_udf(col(column)))
            else:
                raise KeyError(
                    f"Unable to pseudonymise column '{column}' as it does not exist in the"
                    f" source DataFrame ({self.table_name})."
                )

    def fake_anonymise_free_text_columns(self, columns: tuple[str, ...]):
        """Create a new column with the structure of the originally anonymised column but without
            anonymised data.

        Raises:
            KeyError: If a column specified in 'columns' does not exist in the DataFrame.
        """
        for column in columns:
            if column in self.df.columns:
                self.df = self.df.withColumn(column, col(column))
            else:
                raise KeyError(
                    f"Unable to pseudonymise column '{column}' as it does not exist in the"
                    f" source DataFrame ({self.table_name})."
                )

    def remove_columns(self, columns: tuple[str, ...]):
        """Remove specified columns from Spark DataFrame.

        Raises:
            KeyError: If a column specified in 'columns' does not exist in the DataFrame.
        """
        for column in columns:
            if column in self.df.columns:
                logging.info(f"Removing column: {column} in table: {self.table_name}")
                self.df = self.df.drop(column)
            else:
                raise KeyError(
                    f"Unable to drop column '{column}' as it does not exist in the source"
                    f" DataFrame ({self.table_name})."
                )

    def _round_datetime_columns(
        self, columns: tuple[str, ...], round_option: DateTimeRoundOpt
    ):
        """Rounds datetime values in specified DataFrame columns using the provided
        rounding option (most commonly hour).

        Args:
            round_option (DateTimeRoundOpt): An enum for the rounding options.

        Raises:
            KeyError: If a column specified in 'columns' does not exist in the DataFrame.
        """
        for column in columns:
            if column in self.df.columns:
                # Perform rounding with required precision defined by the value of enum
                logging.info(
                    f"Rounding datetime column: {column} in table: {self.table_name}"
                )
                self.df = self.df.withColumn(
                    column, date_trunc(round_option.value, self.df[column])
                )
            else:
                raise KeyError(
                    f"Unable to round datetime in column '{column}' as it does not exist in"
                    f" the source DataFrame ({self.table_name})."
                )

    def round_datetime_month_columns(self, columns: tuple[str, ...]):
        return self._round_datetime_columns(columns, DateTimeRoundOpt.MONTH)

    def round_datetime_hour_columns(self, columns: tuple[str, ...]):
        return self._round_datetime_columns(columns, DateTimeRoundOpt.HOUR)

    def hash_client_id_columns(self, columns: tuple[str, ...]):
        """Hashes the specified columns containing client IDs in the DataFrame.

        Raises:
            KeyError: If a column specified in 'columns' does not exist in the DataFrame.
        """
        for column in columns:
            if column in self.df.columns:
                logging.info(
                    f"Hashing clientID column: {column} in table: {self.table_name}"
                )

                # Merge ClientID with HASH_SALT (salting procedure)
                self.df = self.df.withColumn(
                    column, concat(self.df[column], lit(HASH_SALT))
                )
                # Compute hash value
                self.df = self.df.withColumn(
                    # Create a new column with name: ColumnNameSUFFIX with predefined SUFFIX
                    "".join([column, CLIENT_ID_HASH_SUFFIX]),
                    sha2(self.df[column].cast("Binary"), 256),
                )
                # Drop old column (one without prefix and without the hashed value)
                self.df = self.df.drop(column)
            else:
                raise KeyError(
                    f"Unable to hash column '{column}' as it does not exist in"
                    f" the source DataFrame ({self.table_name})."
                )


def pseudo_transform(
    df: DataFrame, table: Table, anonymise_udf: UserDefinedFunction
) -> DataFrame:
    """Apply pseudonymisation transform to the specified table based on the
    provided configuration.

    Args:
        df (DatatFrame): Dataframe that is the subject of transformation.
        table (Table): Configuration for the table.
        anonymise_udf (UserDefinedFunction): User defined function for anonymisation.

    Returns:
        DataFrame: Transformed DataFrame.
    """

    if table.skip_pseudonymisation:
        logging.info(
            f"No column types configured for {table.name}. Skipping pseudonymisation."
        )
        return df

    transformer: DataframeTransformer = DataframeTransformer(
        df, table.name, anonymise_udf
    )

    for column_type, columns in table.column_types.items():
        action: Callable
        match column_type:
            case ColumnType.FREE_TEXT:
                if RUN_FREE_TEXT_ANONYMISATION:
                    action = transformer.anonymise_free_text_columns
                else:
                    action = transformer.remove_columns
            case ColumnType.OTHER_IDENTIFIABLE:
                action = transformer.remove_columns
            case ColumnType.DATE_TIME:
                action = transformer.round_datetime_hour_columns
            case ColumnType.DATE:
                action = transformer.round_datetime_month_columns
            case ColumnType.CLIENT_ID:
                action = transformer.hash_client_id_columns
            case _:
                # TODO: Implement additional column types (if needed).
                raise KeyError(f"Unsupported column type: {column_type}.")
        action(columns)

    return transformer.df


def fake_pseudo_transform(
    df: DataFrame, table: Table, anonymise_udf: UserDefinedFunction
) -> DataFrame:
    """This methods returns the dataframe with the same structure as the original one, however
        outputs are not anonymised. It is usable for either testing or (non)anonymisation of
        entries for delete operation in upsert method.

    Args:
        df (DatatFrame): Dataframe that is the subject of transformation.
        table (Table): Configuration for the table.
        anonymise_udf (UserDefinedFunction): User defined function for anonymisation.

    Returns:
        DataFrame: Incorrectly transformed DataFrame.
    """

    if table.skip_pseudonymisation:
        logging.info(
            f"No column types configured for {table.name}. Skipping pseudonymisation."
        )
        return df

    transformer: DataframeTransformer = DataframeTransformer(
        df, table.name, anonymise_udf
    )

    def mocked_anonymise_function(columns):  # noqa
        pass

    for column_type, columns in table.column_types.items():
        action: Callable
        match column_type:
            case ColumnType.FREE_TEXT:
                if RUN_FREE_TEXT_ANONYMISATION:
                    action = transformer.fake_anonymise_free_text_columns
                else:
                    action = transformer.remove_columns
            case ColumnType.OTHER_IDENTIFIABLE:
                action = transformer.remove_columns
            case ColumnType.DATE_TIME:
                action = mocked_anonymise_function
            case ColumnType.DATE:
                action = mocked_anonymise_function
            case ColumnType.CLIENT_ID:
                action = transformer.hash_client_id_columns
            case _:
                # TODO: Implement additional column types (if needed).
                raise KeyError(f"Unsupported column type: {column_type}.")
        action(columns)

    return transformer.df
