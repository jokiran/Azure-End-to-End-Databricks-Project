from databricks.sdk.runtime import *
from pyspark.sql import functions as F

def get_watermark(
    pipeline_name,
    source_name
):
    """
    Return the last processed timestamp for a pipeline/source.
    Returns None if no watermark exists.
    """

    watermark_df = spark.table(
        "databricks_project1.silver.pipeline_watermark"
    )

    result = (
        watermark_df
        .filter(
            (F.col("pipeline_name") == pipeline_name) &
            (F.col("source_name") == source_name)
        )
        .select("last_processed_timestamp")
        .limit(1)
        .collect()
    )

    if not result:
        return None

    return result[0]["last_processed_timestamp"]


def update_watermark(
    pipeline_name,
    source_name,
    processed_timestamp
):
    """
    Insert or update the watermark for a pipeline/source.
    """

    spark.sql(f"""
        MERGE INTO databricks_project1.silver.pipeline_watermark AS target
        USING (
            SELECT
                '{pipeline_name}' AS pipeline_name,
                '{source_name}' AS source_name,
                TIMESTAMP('{processed_timestamp}') AS last_processed_timestamp,
                current_timestamp() AS updated_at
        ) AS source
        ON target.pipeline_name = source.pipeline_name
        AND target.source_name = source.source_name

        WHEN MATCHED THEN UPDATE SET
            target.last_processed_timestamp =
                source.last_processed_timestamp,
            target.updated_at =
                source.updated_at

        WHEN NOT MATCHED THEN INSERT (
            pipeline_name,
            source_name,
            last_processed_timestamp,
            updated_at
        )
        VALUES (
            source.pipeline_name,
            source.source_name,
            source.last_processed_timestamp,
            source.updated_at
        )
    """)