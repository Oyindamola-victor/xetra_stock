"""
_summary_: File to store constants
"""

from enum import Enum


class S3FileTypes(Enum):
    """_summary_
    supported file types for S3BucketConnector
    """

    CSV = "csv"
    PARQUET = "parquet"


class MetaProcessFormat(Enum):
    """_summary_
    formation for MetaProcess class
    """

    META_DATE_FORMAT = "%Y-%m-%d"
    META_PROCESS_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    META_SOURCE_DATE_COLUMN = "source_date"
    META_PROCESS_COL = "datetime_of_processing"
    META_FILE_FORMAT = "csv"
