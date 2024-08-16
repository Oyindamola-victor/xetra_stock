"""_summary_: Methods for processing the meta file
"""

from datetime import datetime, timedelta
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
            if e.response["Error"]["Code"] == "NoSuchKey":
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
    def return_date_list(
        meta_bucket: S3BucketConnector,
        first_date: str,
        meta_file_key: str
    ):
        """
        Creating a list of dates based on the input first_date and the already
        processed dates in the meta file

        :param: meta_bucket -> S3BucketConnector for the bucket with the meta file
        :param: first_date -> the earliest date Xetra data should be processed
        :param: meta_key -> key of the meta file on the S3 bucket
        # : todays_date_str -> I have this hard coded because I am not dealing with realtime data, but this value can be dynamic to suit different needs
        returns:
          min_date: first date that should be processed
          return_dates: list of all dates from min_date till today
        """

        # Now we want to get a list of dates that we are supposed to process

        start_date = datetime.strptime(
            first_date, MetaProcessFormat.META_DATE_FORMAT.value
        ).date() - timedelta(days=1)
        # today = datetime.strptime(
        #     todays_date_str, MetaProcessFormat.META_DATE_FORMAT.value
        # ).date()
        today = datetime.today().date()

        try:
            # If meta file exists create return_date_list using the content of the meta file
            # Reading meta file
            df_meta_file = meta_bucket.read_csv_to_df_ok(meta_file_key)
            # Creating a list of dates from first_date untill today
            date_list = [
                (start_date + timedelta(days=x))
                for x in range(0, (today - start_date).days + 1)
            ]
            # Retrieving all the processed dates from the meta file
            processed_dates = set(
                pd.to_datetime(
                    df_meta_file[MetaProcessFormat.META_SOURCE_DATE_COLUMN.value]
                ).dt.date
            )

            # Now lets get the dates that have not been processed
            # The subtraction operation removes any elements in processed_dates from the first set.
            dates_missing = set(date_list[1:]) - processed_dates
            # Please note the plan isnt to only process the missing dates because remember in our tansformation we need a percentage change in price
            # from the previous price, so we dont want the change in price from a day checking a day that has already been processed to be missing
            # The plan is just to see that we have missing dates from the minimum date(arg date) to the current date set, so we can process those data
            if dates_missing:
                # Determining the earliest date that should be extracted
                min_date = min(dates_missing) - timedelta(days=1)
                # Creating a list of dates from min_date until today
                return_dates = [
                    date.strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                    for date in date_list
                    if date >= min_date
                ]
                return_min_date = (min_date + timedelta(days=1)).strftime(
                    MetaProcessFormat.META_DATE_FORMAT.value
                )
            else:
                # If all the dates have been processed we just make the return min_date to be a date in the future
                return_dates = []
                return_min_date = (
                    datetime(9999, 12, 1)
                    .date()
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                )
        except ClientError as e:
            # Check if the exception is specifically about the key not existing
            # If there is no existing meta file then we create a date list from the first date -1 day AKA our Start date until today
            if e.response["Error"]["Code"] == "NoSuchKey":
                return_dates = [
                    (start_date + timedelta(days=x)).strftime(
                        MetaProcessFormat.META_DATE_FORMAT.value
                    )
                    for x in range(0, (today - start_date).days + 1)
                ]
                return_min_date = first_date
        return return_min_date, return_dates
