"""
Test S3BucketConnectorMethods
"""

import unittest
import boto3
from io import StringIO, BytesIO
import pandas as pd
from moto import mock_aws

from xetra_code.common.s3 import S3BucketConnector
from xetra_code.common.custom_exceptions import WrongFormatException

class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector Class
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
        self.s3_bucket_conn = S3BucketConnector(
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_endpoint_url,
            self.s3_bucket_name,
        )
        

    def tearDown(self):
        # Stopping the mock s3 connection

        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """
        This tests the list_files_in_prefix method for getting 2 file keys as
        list on the mocked s3 bucket
        """
        # Expected results
        prefix_expected = "prefix/"
        key1_exp = f"{prefix_expected}-test1.csv"
        key2_exp = f"{prefix_expected}-test2.csv"

        # Test init
        csv_content = """ col1, col2\nvalA, valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)

        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_expected)

        # Tests after method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)

        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": key1_exp}, {"Key": key2_exp}]}
        )

    def test_list_files_in_prefix_wrong_prefix(self):
        """
        This tests the list_files_in_prefix method in case of a wrong or non-existing prefix
        """
        # Test init
        prefix = "no-prefix/"
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix)
        # Test after method execution
        self.assertTrue(not list_result)

    def test_read_csv_to_df(self):
        """
        This test the read_csv_to_df method for
        reading 1 .csv file and confirms that it is able to return a Dataframe
        """

        # Expected results
        key_exp = "test.csv"
        col1 = "col1"
        col2 = "col2"
        val1_exp = "val1"
        val2_exp = "val2"

        expected_log = f"Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}"

        # Test init
        csv_content = f"{col1},{col2}\n{val1_exp},{val2_exp}"
        self.s3_bucket.put_object(Body=csv_content, Key=key_exp)

        # Method execution
        with self.assertLogs() as mocked_logs:
            """
            This line is using Python's unittest framework's assertLogs context manager. 
            The purpose is to capture any log messages that are generated 
            during the execution of the code block within the with statement.
            """
            df_result = self.s3_bucket_conn.read_csv_to_df_ok(key=key_exp)
            
            # Log test after method execution and confirm expected log is present
            self.assertIn(expected_log, mocked_logs.output[0])
        
        
        # Now lets confirm the records are actually present in the dataframe
        # This line asserts that the DataFrame df_result has exactly 1 row.
        self.assertEqual(df_result.shape[0], 1)
        # This line asserts that the DataFrame df_result has exactly 2 columns.
        self.assertEqual(df_result.shape[1], 2)
        self.assertEqual(val1_exp, df_result[col1][0])
        self.assertEqual(val2_exp, df_result[col2][0])
        
        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": key_exp}]}
        )
    
    def test_write_df_to_s3_empty(self):
        """
        This tests the write_df_to_s3 method by trying to add an empty dataframe
        """   
        result_exp = None    
        expected_log = "The DataFrame is empty! No file will be written"
        empty_dataframe = pd.DataFrame()
        expected_key = "emp_file"
        file_format = ".csv"
        # Method execution
        with self.assertLogs() as mocked_logs:
            df_result = self.s3_bucket_conn.write_df_to_s3(data_frame=empty_dataframe, key=expected_key, file_format=file_format)

            # Log test after method execution and confirm expected log is present
            self.assertIn(expected_log, mocked_logs.output[0])
        
        self.assertEqual(result_exp, df_result)
        
    def test_write_df_to_s3_csv(self):
        """
        This tests the write_df_to_s3 method by adding 1 csv file
        and writing to an s3 bucket
        """
        # Expected results
        return_exp = True
        key_exp = "testfile"
        file_format = "csv"
        expected_log = f"Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}.{file_format}"
        test_data = {'col1': [1, 2], 'col2': [3, 4]}
        csv_df = pd.DataFrame(data = test_data)
        
        # Method Execution
        with self.assertLogs() as mocked_logs:
            csv_result = self.s3_bucket_conn.write_df_to_s3(data_frame=csv_df, key=f"{key_exp}.{file_format}", file_format=file_format)
            
            # Log test after method execution and confirm expected log is present
            self.assertIn(expected_log, mocked_logs.output[0])
        
        # Testing the Method Execution
        self.assertEqual(return_exp, csv_result)
        
        # Testing that the dataframe written equals the original
        written_df_object = self.s3_bucket.Object(key=f"{key_exp}.{file_format}").get().get("Body").read().decode("utf-8")
        output_buffer = StringIO(written_df_object)
        written_df = pd.read_csv(output_buffer, delimiter = ',')
        
        self.assertTrue(csv_df.equals(written_df))
        
        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": f"{key_exp}.{file_format}"}]}
        )
    
    def test_write_df_to_s3_parquet(self):
        """
        This tests the write_df_to_s3 method by adding 1 csv file
        and writing to an s3 bucket
        """
        # Expected results
        return_exp = True
        key_exp = "testfile"
        file_format = "parquet"
        expected_log = f"Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}.{file_format}"
        test_data = {'col1': [1, 2], 'col2': [3, 4]}
        parquet_df = pd.DataFrame(data = test_data)
        
        # Method Execution
        with self.assertLogs() as mocked_logs:
            parquet_result = self.s3_bucket_conn.write_df_to_s3(data_frame=parquet_df, key=f"{key_exp}.{file_format}", file_format=file_format)
            
            # Log test after method execution and confirm expected log is present
            self.assertIn(expected_log, mocked_logs.output[0])
        
        # Testing the Method Execution
        self.assertEqual(return_exp, parquet_result)
        
        # Testing that the dataframe written equals the original
        written_df_object = self.s3_bucket.Object(key=f"{key_exp}.{file_format}").get().get("Body").read()
        output_buffer = BytesIO(written_df_object)
        written_df = pd.read_parquet(output_buffer)
        
        self.assertTrue(parquet_df.equals(written_df))
        
        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": f"{key_exp}.{file_format}"}]}
        )
    
    def test_write_df_to_s3_wrong_format(self):
        """
        This tests the write_df_to_s3 method by providing the wrong format
        """
        
        key_exp = "wrong_file"
        file_format = "wrong_format"
        expected_log = f"The file format {file_format} is not supported to be written to s3!"
        test_data = {'col1': [1, 2], 'col2': [3, 4]}
        wrong_dataframe = pd.DataFrame(data=test_data)
        exception_exp = WrongFormatException
        
        # Method Execution
        with self.assertLogs() as mocked_logs:
            with self.assertRaises(exception_exp):
                self.s3_bucket_conn.write_df_to_s3(data_frame=wrong_dataframe, key=f"{key_exp}.{file_format}", file_format=file_format)

                # Log test after method execution and confirm expected log is present
                self.assertIn(expected_log, mocked_logs.output[0])

if __name__ == "__main__":
    unittest.main()
