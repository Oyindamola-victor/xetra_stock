import unittest
from unittest.mock import patch
from moto import mock_aws
import boto3
import pandas as pd
from io import StringIO, BytesIO
from xetra_code.transformers.xetra_transformers import (
    XetraETL,
    XetraSourceConfig,
    XetraTargetConfig,
)
from xetra_code.common.s3 import S3BucketConnector
from xetra_code.common.meta_process import MetaProcess


class TestXetraETL(unittest.TestCase):
    """
    Testing the XetraETL Class
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
        self.s3_bucket_name_src = "src-bucket"
        self.s3_bucket_name_trg = "trg-bucket"
        self.meta_key = "meta_key"

        # Third: Initialize an actual s3 service and use the mocked bucket on the mock_aws service

        self.s3 = boto3.resource(service_name="s3", endpoint_url=self.s3_endpoint_url)

        self.s3.create_bucket(
            Bucket=self.s3_bucket_name_src,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )

        self.s3.create_bucket(
            Bucket=self.s3_bucket_name_trg,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )

        self.src_s3_bucket = self.s3.Bucket(self.s3_bucket_name_src)
        self.trg_s3_bucket = self.s3.Bucket(self.s3_bucket_name_trg)

        # Creating a testing instance for both src & trg bucket
        self.s3_bucket_src = S3BucketConnector(
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_endpoint_url,
            self.s3_bucket_name_src,
        )

        self.s3_bucket_trg = S3BucketConnector(
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_endpoint_url,
            self.s3_bucket_name_trg,
        )

        # Creating source and target configuration
        conf_dict_src = {
            "src_first_extract_date": "2021-04-01",
            "src_columns": [
                "ISIN",
                "Mnemonic",
                "Date",
                "Time",
                "StartPrice",
                "EndPrice",
                "MinPrice",
                "MaxPrice",
                "TradedVolume",
            ],
            "src_col_date": "Date",
            "src_col_isin": "ISIN",
            "src_col_time": "Time",
            "src_col_start_price": "StartPrice",
            "src_col_min_price": "MinPrice",
            "src_col_max_price": "MaxPrice",
            "src_col_traded_vol": "TradedVolume",
        }
        conf_dict_trg = {
            "trg_col_isin": "isin",
            "trg_col_date": "date",
            "trg_col_op_price": "opening_price_eur",
            "trg_col_clos_price": "closing_price_eur",
            "trg_col_min_price": "minimum_price_eur",
            "trg_col_max_price": "maximum_price_eur",
            "trg_col_dail_trad_vol": "daily_traded_volume",
            "trg_col_ch_prev_clos": "change_prev_closing_%",
            "trg_key": "report1/xetra_daily_report1_",
            "trg_key_date_format": "%Y%m%d_%H%M%S",
            "trg_format": "parquet",
        }
        self.source_config = XetraSourceConfig(**conf_dict_src)
        self.target_config = XetraTargetConfig(**conf_dict_trg)
        # Creating source files on mocked s3
        columns_src = [
            "ISIN",
            "Mnemonic",
            "Date",
            "Time",
            "StartPrice",
            "EndPrice",
            "MinPrice",
            "MaxPrice",
            "TradedVolume",
        ]
        data = [
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-15",
                "12:00",
                20.19,
                18.45,
                18.20,
                20.33,
                877,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-16",
                "15:00",
                18.27,
                21.19,
                18.27,
                21.34,
                987,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-17",
                "13:00",
                20.21,
                18.27,
                18.21,
                20.42,
                633,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-17",
                "14:00",
                18.27,
                21.19,
                18.27,
                21.34,
                455,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-18",
                "07:00",
                20.58,
                19.27,
                18.89,
                20.58,
                9066,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-18",
                "08:00",
                19.27,
                21.14,
                19.27,
                21.14,
                1220,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-19",
                "07:00",
                23.58,
                23.58,
                23.58,
                23.58,
                1035,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-19",
                "08:00",
                23.58,
                24.22,
                23.31,
                24.34,
                1028,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-19",
                "09:00",
                24.22,
                22.21,
                22.21,
                25.01,
                1523,
            ],
        ]
        # Creating Source DataFrame
        self.df_src = pd.DataFrame(data, columns=columns_src)

        # Adding files to s3 and ensuring the prefix are aligned for test
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[0:0], "2021-04-15/2021-04-15_TEST_FILE12.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[1:1], "2021-04-16/2021-04-16_TEST_FILE15.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[2:2], "2021-04-17/2021-04-17_TEST_FILE13.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[3:3], "2021-04-17/2021-04-17_TEST_FILE14.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[4:4], "2021-04-18/2021-04-18_TEST_FILE07.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[5:5], "2021-04-18/2021-04-18_TEST_FILE08.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[6:6], "2021-04-19/2021-04-19_TEST_FILE07.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[7:7], "2021-04-19/2021-04-19_TEST_FILE08.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[8:8], "2021-04-19/2021-04-19_TEST_FILE09.csv", "csv"
        )

        # For the target table
        columns_report = [
            "ISIN",
            "Date",
            "opening_price_eur",
            "closing_price_eur",
            "minimum_price_eur",
            "maximum_price_eur",
            "daily_traded_volume",
            "change_prev_closing_%",
        ]
        data_report = [
            ["AT0000A0E9W5", "2021-04-17", 20.21, 18.27, 18.21, 21.34, 1088, 0.00],
            ["AT0000A0E9W5", "2021-04-18", 20.58, 19.27, 18.89, 21.14, 10286, 5.47],
            ["AT0000A0E9W5", "2021-04-19", 23.58, 24.22, 22.21, 25.01, 3586, 25.69],
        ]
        self.df_report = pd.DataFrame(data_report, columns=columns_report)

    def tearDown(self):
        # Stopping the mock s3 connection

        self.mock_s3.stop()

    def test_extract_ok(self):
        """
        This method tests the extract method
        when there are files to be extracted
        """

        # Expected results
        expected_df = self.df_src.loc[1:8].reset_index(drop=True)
        extract_date = "2021-04-17"
        extract_date_list = [
            "2021-04-16",
            "2021-04-17",
            "2021-04-18",
            "2021-04-19",
            "2021-04-20",
        ]

        # Method execution
        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            df_result = xetra_etl.extract()

        # Test after method execution
        self.assertTrue(expected_df.equals(df_result))

    def test_extract_no_files(self):
        """
        Tests the extract method when
        there are no files to be extracted
        """
        # Test init
        extract_date = "9999-12-31"
        extract_date_list = []
        # Method execution
        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            df_return = xetra_etl.extract()
        # Test after method execution
        self.assertTrue(df_return.empty)

    def test_transform_report1_ok(self):
        """
        Tests the transform_report1 method with
        an DataFrame as input argument
        """
        # Expected results
        log1_exp = (
            "Applying transformations to Xetra source data for report 1 started..."
        )
        log2_exp = "Applying transformations to Xetra source data finished..."
        df_exp = self.df_report
        # Test init
        extract_date = "2021-04-17"
        extract_date_list = ["2021-04-16", "2021-04-17", "2021-04-18", "2021-04-19"]
        df_input = self.df_src.loc[1:8].reset_index(drop=True)
        # Method execution
        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            with self.assertLogs() as mocked_logs:
                df_result = xetra_etl.transform_report1(df_input)
                # Log test after method execution
                self.assertIn(log1_exp, mocked_logs.output[0])
                self.assertIn(log2_exp, mocked_logs.output[1])

        # #Print out the DataFrames for comparison
        # print("Expected DataFrame:")
        # print(df_exp)
        # print("Resulting DataFrame:")
        # print(df_result)

        # Test after method execution
        self.assertTrue(df_exp.equals(df_result))

    def test_transform_report1_empty_dataframe(self):
        """
        Tests the transform_report1 method with
        an empty DataFrame as input argument
        """
        # Expected results
        expected_log = "The DataFrame is empty. No Transformation will be applied!"
        # Test init
        extract_date = "2021-04-17"
        extract_date_list = ["2021-04-16", "2021-04-17", "2021-04-18"]
        df_input = pd.DataFrame()
        # Method execution
        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            with self.assertLogs() as mocked_log:
                df_result = xetra_etl.transform_report1(df_input)
                # Log test after method execution
                self.assertIn(expected_log, mocked_log.output[0])

        # Test after method execution
        self.assertTrue(df_result.empty)

    def test_load(self):
        """
        Tests the load method
        """
        # Expected results
        log1_exp = "Xetra target data successfully written."
        log2_exp = "Xetra meta file successfully updated."
        df_exp = self.df_report
        meta_exp = ["2021-04-17", "2021-04-18", "2021-04-19"]
        # Test init
        extract_date = "2021-04-17"
        extract_date_list = ["2021-04-16", "2021-04-17", "2021-04-18", "2021-04-19"]
        df_input = self.df_report
        # Method execution
        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            with self.assertLogs() as mocked_logs:
                xetra_etl.load_to_s3(df_input)
                # Log test after method execution
                self.assertIn(log1_exp, mocked_logs.output[1])
                self.assertIn(log2_exp, mocked_logs.output[5])
        # Test after method execution
        trg_file = self.s3_bucket_trg.list_files_in_prefix(self.target_config.trg_key)[
            0
        ]
        data = self.trg_s3_bucket.Object(key=trg_file).get().get("Body").read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)

        self.assertTrue(df_exp.equals(df_result))
        meta_file = self.s3_bucket_trg.list_files_in_prefix(self.meta_key)[0]
        df_meta_result = self.s3_bucket_trg.read_csv_to_df_ok(meta_file)
        self.assertEqual(list(df_meta_result["source_date"]), meta_exp)
        # Cleanup after test
        self.trg_s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": trg_file}, {"Key": trg_file}]}
        )

    def test_etl_report1(self):
        """
        This tests the etl_report1 method and will be very similar to the test_load
        """
        # Expected results
        df_exp = self.df_report
        meta_exp = ["2021-04-17", "2021-04-18", "2021-04-19"]
        # Test init
        extract_date = "2021-04-17"
        extract_date_list = ["2021-04-16", "2021-04-17", "2021-04-18", "2021-04-19"]
        # Method execution
        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            xetra_etl.etl_report1()
        # Test after method execution
        trg_file = self.s3_bucket_trg.list_files_in_prefix(self.target_config.trg_key)[
            0
        ]
        data = self.trg_s3_bucket.Object(key=trg_file).get().get("Body").read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_exp.equals(df_result))
        meta_file = self.s3_bucket_trg.list_files_in_prefix(self.meta_key)[0]
        df_meta_result = self.s3_bucket_trg.read_csv_to_df_ok(meta_file)
        self.assertEqual(list(df_meta_result["source_date"]), meta_exp)
        # Cleanup after test
        self.trg_s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": trg_file}, {"Key": trg_file}]}
        )


if __name__ == "__main__":
    unittest.main()
