"""Module with helper functions to prepare the JDBC tables used in the tests."""

import sqlite3
from os import makedirs, path

from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructType,
    TimestampNTZType,
    TimestampType,
)

SQLITE_URI_PREFIX = "jdbc:sqlite:"

_SPARK_TO_JDBC_TYPES = {
    IntegerType: "INTEGER",
    LongType: "BIGINT",
    DoubleType: "DOUBLE PRECISION",
    FloatType: "REAL",
    ShortType: "INTEGER",
    ByteType: "BYTE",
    BooleanType: "BIT(1)",
    StringType: "TEXT",
    BinaryType: "BLOB",
    TimestampType: "TIMESTAMP",
    TimestampNTZType: "TIMESTAMP",
    DateType: "DATE",
}


class JdbcHelpers(object):
    """Class with helper functions to interact with the test JDBC databases.

    Spark has no built-in dialect for SQLite, hence the default JDBC dialect is
    used. Since Spark 4.1.3 (SPARK-54800), that default dialect only assumes a
    table does not exist when the JDBC driver reports an SQL state of the `42`
    class. The SQLite driver does not report any SQL state, so Spark propagates
    the `no such table` error instead of creating the table on write. Therefore,
    the tests need to create the target table before writing into it.
    """

    @staticmethod
    def get_sqlite_file_path(uri: str) -> str:
        """Get the file path of a SQLite database from its JDBC uri.

        Args:
            uri: uri for the jdbc connection (e.g., `jdbc:sqlite:/tmp/tests.db`).

        Returns:
            The path of the file holding the SQLite database.
        """
        return uri[len(SQLITE_URI_PREFIX) :]

    @classmethod
    def get_create_table_statement(cls, schema: StructType, db_table: str) -> str:
        """Get the statement creating a table matching a spark schema.

        The column types are the ones the Spark JDBC data source would use to
        create the table itself, so that the schema read back from the database
        is the same as the one of the dataframe written into it.

        Args:
            schema: schema of the dataframe to write into the table.
            db_table: name of the table to create.

        Returns:
            The `CREATE TABLE` statement for the given schema.
        """
        columns = ", ".join(
            f'"{field.name}" {cls.get_jdbc_type(field.dataType)}'
            f'{"" if field.nullable else " NOT NULL"}'
            for field in schema.fields
        )

        return f'CREATE TABLE IF NOT EXISTS "{db_table}" ({columns})'  # nosec: B608

    @staticmethod
    def get_jdbc_type(data_type: DataType) -> str:
        """Get the JDBC type used by spark to represent a spark data type.

        Args:
            data_type: spark data type of a dataframe column.

        Returns:
            The JDBC type to use in the `CREATE TABLE` statement.
        """
        if isinstance(data_type, DecimalType):
            return f"DECIMAL({data_type.precision},{data_type.scale})"

        jdbc_type = _SPARK_TO_JDBC_TYPES.get(type(data_type))
        if not jdbc_type:
            raise NotImplementedError(
                f"Cannot get the JDBC type for the spark type: {data_type}."
            )

        return jdbc_type

    @classmethod
    def create_table_if_not_exists(
        cls, schema: StructType, uri: str, db_table: str
    ) -> None:
        """Create a SQLite table, matching a spark schema, if it does not exist.

        Args:
            schema: schema of the dataframe to write into the table.
            uri: uri for the jdbc connection.
            db_table: `database.table_name`.
        """
        if not uri.startswith(SQLITE_URI_PREFIX):
            return

        file_path = cls.get_sqlite_file_path(uri)
        makedirs(path.dirname(file_path), exist_ok=True)

        connection = sqlite3.connect(file_path)
        try:
            connection.execute(cls.get_create_table_statement(schema, db_table))
            connection.commit()
        finally:
            connection.close()
