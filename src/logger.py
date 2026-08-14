import logging
import time


def get_logger(name="databricks_pipeline"):
    """
    Create and return a reusable pipeline logger.
    """

    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(logging.INFO)

    return logger


def log_pipeline_start(logger, pipeline_name):
    """
    Log pipeline start and return start time.
    """

    start_time = time.time()

    logger.info(
        f"Pipeline started: {pipeline_name}"
    )

    return start_time


def log_pipeline_end(
    logger,
    pipeline_name,
    start_time,
    status="SUCCESS"
):
    """
    Log pipeline completion and execution duration.
    """

    end_time = time.time()
    duration = end_time - start_time

    logger.info(
        f"Pipeline completed: {pipeline_name} | "
        f"Status: {status} | "
        f"Duration: {duration:.2f} seconds"
    )


def log_row_count(
    logger,
    table_name,
    row_count
):
    """
    Log row count for a table/DataFrame.
    """

    logger.info(
        f"Table: {table_name} | "
        f"Row count: {row_count}"
    )


def log_error(
    logger,
    pipeline_name,
    error
):
    """
    Log pipeline error.
    """

    logger.error(
        f"Pipeline failed: {pipeline_name} | "
        f"Error: {error}"
    )