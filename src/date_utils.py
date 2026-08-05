from pyspark.sql.functions import *

def convert_mixed_date(df, column_name):

    return df.withColumn(
        column_name,
        when(
            col(column_name).rlike("^[0-9]+$"),
            expr(f"date_add('1899-12-30', CAST({column_name} AS INT))")
        ).otherwise(
            coalesce(
                to_timestamp(col(column_name), "yyyy-MM-dd HH:mm:ss").cast("date"),
                to_timestamp(col(column_name), "yyyy-MM-dd").cast("date"),
                to_timestamp(col(column_name), "M/d/yyyy").cast("date"),
                to_timestamp(col(column_name), "MM/dd/yyyy").cast("date"),
                to_timestamp(col(column_name), "d MMM yyyy").cast("date"),
                to_timestamp(col(column_name), "d-MMM-yy").cast("date")
            )
        )
    )