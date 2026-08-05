from pyspark.sql.functions import *

def convert_mixed_date(df, column_name):

    return df.withColumn(
        column_name,
        when(
            col(column_name).rlike("^[0-9]+$"),
            expr(f"date_add('1899-12-30', CAST({column_name} AS INT))")
        ).otherwise(
            expr(f"""
            coalesce(
                try_to_date({column_name}, 'yyyy-MM-dd HH:mm:ss'),
                try_to_date({column_name}, 'yyyy-MM-dd'),
                try_to_date({column_name}, 'M/d/yyyy'),
                try_to_date({column_name}, 'MM/dd/yyyy'),
                try_to_date({column_name}, 'd MMM yyyy'),
                try_to_date({column_name}, 'd-MMM-yy')
            )
            """)
        )
    )