from pyspark.sql.functions import *

def trim_columns(df):

    for c in df.columns:
        df = df.withColumn(c, trim(col(c)))

    return df


def replace_blank_with_null(df):

    for c in df.columns:
        df = df.withColumn(
            c,
            when(col(c) == "", None).otherwise(col(c))
        )

    return df


def replace_nan_with_null(df):

    for c in df.columns:
        df = df.withColumn(
            c,
            when(
                col(c).isin("nan", "NaN", "None"),
                None
            ).otherwise(col(c))
        )

    return df


def add_audit_columns(df, source_file):

    return (
        df.withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("source_file", lit(source_file))
    )