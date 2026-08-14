from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_no_nulls(
    df: DataFrame,
    columns: list[str]
) -> dict:
    """
    Validate that specified columns contain no NULL values.

    Returns:
        Dictionary containing validation results.
    """

    results = {}

    for column in columns:
        null_count = df.filter(
            F.col(column).isNull()
        ).count()

        results[column] = {
            "null_count": null_count,
            "passed": null_count == 0
        }

    return results


def validate_no_duplicates(
    df: DataFrame,
    columns: list[str]
) -> dict:
    """
    Validate uniqueness based on the supplied key columns.
    """

    total_count = df.count()

    distinct_count = (
        df.select(*columns)
        .distinct()
        .count()
    )

    duplicate_count = total_count - distinct_count

    return {
        "columns": columns,
        "total_count": total_count,
        "distinct_count": distinct_count,
        "duplicate_count": duplicate_count,
        "passed": duplicate_count == 0
    }


def validate_referential_integrity(
    child_df: DataFrame,
    parent_df: DataFrame,
    child_column: str,
    parent_column: str
) -> dict:
    """
    Validate that every key in the child table
    exists in the parent table.
    """

    missing_count = (
        child_df
        .select(F.col(child_column).alias("key"))
        .dropDuplicates()
        .join(
            parent_df
            .select(F.col(parent_column).alias("key"))
            .dropDuplicates(),
            on="key",
            how="left_anti"
        )
        .count()
    )

    return {
        "child_column": child_column,
        "parent_column": parent_column,
        "missing_keys": missing_count,
        "passed": missing_count == 0
    }


def validate_row_count(
    df: DataFrame,
    expected_count: int
) -> dict:
    """
    Validate that the DataFrame contains the expected
    number of rows.
    """

    actual_count = df.count()

    return {
        "expected_count": expected_count,
        "actual_count": actual_count,
        "difference": actual_count - expected_count,
        "passed": actual_count == expected_count
    }


def validate_required_columns(
    df: DataFrame,
    required_columns: list[str]
) -> dict:
    """
    Validate that all required columns exist in the DataFrame.
    """

    actual_columns = set(df.columns)

    missing_columns = [
        column
        for column in required_columns
        if column not in actual_columns
    ]

    return {
        "required_columns": required_columns,
        "missing_columns": missing_columns,
        "passed": len(missing_columns) == 0
    }