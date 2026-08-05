from pyspark.sql.functions import (
    col,
    when,
    coalesce,
    try_to_timestamp,
    date_add,
    expr,
    lit
)

def convert_mixed_date(df, column_name):

    return df.withColumn(
        column_name,
        when(
            col(column_name).rlike("^[0-9]+$"),
            expr(f"date_add('1899-12-30', CAST({column_name} AS INT))")
        ).otherwise(
            coalesce(
                try_to_timestamp(col(column_name), lit("yyyy-MM-dd HH:mm:ss")).cast("date"),
                try_to_timestamp(col(column_name), lit("yyyy-MM-dd")).cast("date"),
                try_to_timestamp(col(column_name), lit("M/d/yyyy")).cast("date"),
                try_to_timestamp(col(column_name), lit("MM/dd/yyyy")).cast("date"),
                try_to_timestamp(col(column_name), lit("d MMM yyyy")).cast("date"),
                try_to_timestamp(col(column_name), lit("d-MMM-yy")).cast("date")
            )
        )
    )