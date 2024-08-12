"""_summary_: Methods for processing the meta file
"""

from datetime import datetime
import pandas as pd
from xetra_code.common.constants import MetaProcessFormat
from xetra_code.common.s3 import S3BucketConnector
from xetra_code.common.custom_exceptions import WrongMetaFileException
import logging
from botocore.exceptions import ClientError


class MetaProcess:
    """_summary_: class for working and updating with the meta file"""

    @staticmethod
    def update_meta_file(
        meta_bucket: S3BucketConnector, meta_file_key: str, extract_date_list: list
    ):
        """
        This method updates the meta file with the processed Xetra dates
        and uses todays date as the processed date

        Args:
            meta_bucket (S3BucketConnector): S3BucketConnector for the bucket with the meta file
            meta_file_key (str): This is the key or filename of the meta file in the s3 bucket
            extract_date_list (list): This is a list of dates that are extracted from the source
        """
        _logger = logging.getLogger(__name__)

        # If the extract_date_list is empty, log the situation and return None
        if not extract_date_list:
            _logger.info("The DataFrame is empty! No file will be written")
            return None
        
        # Creating an empty Dataframe containing the new files that have been processed
        df_new = pd.DataFrame(
            columns=[
                MetaProcessFormat.META_SOURCE_DATE_COLUMN.value,
                MetaProcessFormat.META_PROCESS_COL.value,
            ]
        )
        df_new[MetaProcessFormat.META_SOURCE_DATE_COLUMN.value] = extract_date_list
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = datetime.today().strftime(
            MetaProcessFormat.META_PROCESS_DATE_FORMAT.value
        )
        try:
            # Reading the old records in the metafile
            # If the meta file exists then union old DataFrane and the new DataFrame
            df_old = meta_bucket.read_csv_to_df_ok(meta_file_key)

            # Now we need to first confirm if the columns in the old and new dataframe are the same
            if list(df_old.columns) != list(df_new.columns):
                raise WrongMetaFileException
            # Concatenating both records
            _logger.info(
                "Old and new metafile exists -> Updating meta key file -> %s to the s3 bucket",
                meta_file_key,
            )

            df_all = pd.concat([df_old, df_new])
        except ClientError as e:
            # Check if the exception is specifically about the key not existing
            if e.response['Error']['Code'] == 'NoSuchKey':
                # If the metafile doesnt exist -> We only use the new data
                df_all = df_new
            # Now we write the meta file back to s3
            _logger.info(
                "Old metafile does Not exist, creating and updating meta key file -> %s to the s3 bucket",
                meta_file_key,
            )
        meta_bucket.write_df_to_s3(
            df_all, meta_file_key, MetaProcessFormat.META_FILE_FORMAT.value
        )
        return True

    @staticmethod
    def return_date_list():
        pass
