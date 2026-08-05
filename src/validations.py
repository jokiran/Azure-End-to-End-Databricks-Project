from pyspark.sql.functions import col, count, when


def duplicate_check(df, column_name):
    """
    Returns duplicate records based on a column.
    """
    return (
        df.groupBy(column_name)
          .count()
          .filter(col("count") > 1)
    )


def null_summary(df):
    """
    Returns null count for every column.
    """
    return df.select([
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ])