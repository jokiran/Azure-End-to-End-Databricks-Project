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

    exprs = []

    for c in df.columns:
        exprs.append(
            count(
                when(col(c).isNull(), c)
            ).alias(c)
        )

    return df.select(exprs)