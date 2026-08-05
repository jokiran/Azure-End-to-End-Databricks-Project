from pyspark.sql.functions import (
    col,
    when,
    expr,
    coalesce,
    to_date
)


def convert_mixed_date(df, column_name):
    """
    Converts mixed date formats into Spark DateType.

    Supported formats:
    - Excel serial numbers (44250)
    - yyyy-MM-dd HH:mm:ss
    - yyyy-MM-dd
    - M/d/yyyy
    - MM/dd/yyyy
    - d MMM yyyy
    - d-MMM-yy
    """

    return (
        df.withColumn(
            column_name,
            when(
                col(column_name).rlike("^[0-9]+$"),
                expr(
                    f"date_add('1899-12-30', CAST({column_name} AS INT))"
                )
            ).otherwise(
                coalesce(
                    to_date(col(column_name), "yyyy-MM-dd HH:mm:ss"),
                    to_date(col(column_name), "yyyy-MM-dd"),
                    to_date(col(column_name), "M/d/yyyy"),
                    to_date(col(column_name), "MM/dd/yyyy"),
                    to_date(col(column_name), "d MMM yyyy"),
                    to_date(col(column_name), "d-MMM-yy")
                )
            )
        )
    )