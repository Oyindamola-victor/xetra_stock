"""
Test the meta process method
"""

import unittest
import boto3
import pandas as pd

from moto import mock_aws
from datetime import datetime, timedelta
from xetra_code.common.s3 import S3BucketConnector
from xetra_code.common.meta_process import MetaProcess
from xetra_code.common.constants import MetaProcessFormat
from xetra_code.common.custom_exceptions import WrongMetaFileException
from io import StringIO


class TestMetaProcess(unittest.TestCase):
    """
    Testing the MetaProcess Class
    """

    def setUp(self):
        """
        Here we setup the environment and variables for the mock aws
        """
        # First: We initialize/start mocking the aws connection
        self.mock_s3 = mock_aws()
        self.mock_s3.start()

        # Second: We define the class arguments that will go into the S3BucketConnector
        # Hence imitating the real S3BucketConnector; you can check the arguments in the S3BucketConnector class to confirm

        self.s3_access_key = "AWS_ACCESS_KEY"
        self.s3_secret_key = "AWS_SECRET_KEY"
        self.s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
        self.s3_bucket_name = "test-bucket"

        # Third: Initialize an actual s3 service and use the mocked bucket on the mock_aws service

        self.s3 = boto3.resource(service_name="s3", endpoint_url=self.s3_endpoint_url)

        self.s3.create_bucket(
            Bucket=self.s3_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )

        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        # Creating a testing instance
        self.meta_s3_bucket_conn = S3BucketConnector(
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_endpoint_url,
            self.s3_bucket_name,
        )

        self.dates = [
            (datetime.today().date() - timedelta(days=day)).strftime(
                MetaProcessFormat.META_DATE_FORMAT.value
            )
            for day in range(8)
        ]

    def tearDown(self):
        # Stopping the mock s3 connection

        self.mock_s3.stop()

    def test_update_meta_file_no_meta_file(self):
        """
        Tests the update_meta_file method
        when there is no meta file
        """

        meta_file_key = "meta_file.csv"
        expected_source_date_list = ["2024-08-09", "2024-08-08"]
        expected_processed_date_list = [datetime.today().date()] * 2
        expected_log = f"Old metafile does Not exist, creating and updating meta key file -> {meta_file_key} to the s3 bucket"
        # Method Execution

        with self.assertLogs() as mocked_logs:
            MetaProcess.update_meta_file(
                self.meta_s3_bucket_conn, meta_file_key, expected_source_date_list
            )

            # Log test after method execution and confirm expected log is present
            self.assertIn(expected_log, mocked_logs.output[1])
            # self.assertTrue(
            #     any(expected_log in log for log in mocked_logs.output),
            #     f"Expected log '{expected_log}' not found in {mocked_logs.output}"
            # )

        # NOW WE HAVE A NEW META FILE THAT ONLY CONTAINS THE DATE PROCESSED NEWLY AND HAS TODAY AS THE PROCESSED DATE

        # Lets Read the Meta file and confirm the records written are correct
        written_data_object = (
            self.s3_bucket.Object(key=meta_file_key)
            .get()
            .get("Body")
            .read()
            .decode("utf-8")
        )
        output_buffer = StringIO(written_data_object)

        df_meta_result = pd.read_csv(output_buffer)

        # GETTING THE SOURCE DATE FROM THE JUST WRITTEN META FILE
        date_list_result = list(
            df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COLUMN.value]
        )

        # GETTING THE PROCESSED DATE FROM THE JUST WRITTEN META FILE
        proc_date_list_result = list(
            pd.to_datetime(
                df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]
            ).dt.date
        )

        # Test that the values expected and written are the same
        self.assertEqual(date_list_result, expected_source_date_list)
        self.assertEqual(proc_date_list_result, expected_processed_date_list)

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": meta_file_key}]})

    def test_update_meta_file_empty_date_list(self):
        """
        Tests the update_meta_file method
        when the argument extract_date_list is empty
        """
        # Expected results
        return_exp = None
        log_exp = "The DataFrame is empty! No file will be written"
        # Test init
        date_list = []
        meta_key = "meta_file.csv"
        # Method execution
        with self.assertLogs() as mocked_logs:
            result = MetaProcess.update_meta_file(
                self.meta_s3_bucket_conn, meta_key, date_list
            )
            # Log test after method execution
            self.assertIn(log_exp, mocked_logs.output[0])
        # Test after method execution
        self.assertEqual(return_exp, result)

    def test_update_meta_file_meta_file_wrong(self):
        """
        Tests the update_meta_file method
        when there is a wrong meta file
        """
        # Expected results
        date_list_old = ["2021-04-12", "2021-04-13"]
        date_list_new = ["2021-04-16", "2021-04-17"]
        # Test init
        meta_key = "meta_file.csv"
        meta_content = (
            f"wrong_column, {MetaProcessFormat.META_PROCESS_COL.value}\n"
            f"{date_list_old[0]},"
            f"{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n"
            f"{date_list_old[1]},"
            f"{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        # Method execution
        with self.assertRaises(WrongMetaFileException):
            MetaProcess.update_meta_file(
                self.meta_s3_bucket_conn, meta_key, date_list_new
            )
        # Cleanup after test
        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": meta_key}]})

    def test_update_meta_file_ok(self):
        """
        Test the update_meta_file method correctly by confirming metafile already exists
        and updating the metafile with the new records
        """

        # Expected results
        date_list_old = ["2021-04-12", "2021-04-13"]
        date_list_new = ["2021-04-16", "2021-04-17"]
        expected_combined_date_list = date_list_old + date_list_new
        expected_compined_processed_date = [datetime.today().date()] * 4
        
        # Test init
        meta_key = "meta_file"
        meta_file_content = (
          f'{MetaProcessFormat.META_SOURCE_DATE_COLUMN.value},'
          f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
          f'{date_list_old[0]},'
          f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
          f'{date_list_old[1]},'
          f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
        )
        expected_log = f"Old and new metafile exists -> Updating meta key file -> {meta_key} to the s3 bucket"
        result_exp = True

        # Lets insert/simulate an existing metafile with the metafile content above
        self.s3_bucket.put_object(Body=meta_file_content, Key=meta_key)

        # Method Execution

        with self.assertLogs() as mocked_logs:
            update_result = MetaProcess.update_meta_file(
                self.meta_s3_bucket_conn, meta_key, date_list_new
            )

            # Log test after method execution and confirm expected log is present
            self.assertIn(expected_log, mocked_logs.output[1])

        # Now to confirm that we successfully updated the metafile,
        # we need to crosscheck the final output after writing
        
        # Read Meta file
        meta_df = self.meta_s3_bucket_conn.read_csv_to_df_ok(meta_key)
        meta_date_list_result = list(meta_df[MetaProcessFormat.META_SOURCE_DATE_COLUMN.value])
        meta_processed_date_result = list(pd.to_datetime(meta_df[MetaProcessFormat.META_PROCESS_COL.value]).dt.date)
        
        # Now lets Crosscheck with our expectations
        self.assertEqual(expected_combined_date_list, meta_date_list_result)
        self.assertEqual(expected_compined_processed_date, meta_processed_date_result)

        # Cleanup after test
        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": meta_key}]})


if __name__ == "__main__":
    unittest.main()
