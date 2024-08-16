import unittest
from unittest.mock import patch
from moto import mock_aws
import boto3
import pandas as pd
from io import StringIO
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
        self.s3_bucket_name_src = 'src-bucket'
        self.s3_bucket_name_trg = 'trg-bucket'
        self.meta_key = 'meta_key'
        
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
            'src_first_extract_date': '2021-04-01',
            'src_columns': ['ISIN', 'Mnemonic', 'Date', 'Time',
            'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'],
            'src_col_date': 'Date',
            'src_col_isin': 'ISIN',
            'src_col_time': 'Time',
            'src_col_start_price': 'StartPrice',
            'src_col_min_price': 'MinPrice',
            'src_col_max_price': 'MaxPrice',
            'src_col_traded_vol': 'TradedVolume'
        }
        conf_dict_trg = {
            'trg_col_isin': 'isin',
            'trg_col_date': 'date',
            'trg_col_op_price': 'opening_price_eur',
            'trg_col_clos_price': 'closing_price_eur',
            'trg_col_min_price': 'minimum_price_eur',
            'trg_col_max_price': 'maximum_price_eur',
            'trg_col_dail_trad_vol': 'daily_traded_volume',
            'trg_col_ch_prev_clos': 'change_prev_closing_%',
            'trg_key': 'report1/xetra_daily_report1_',
            'trg_key_date_format': '%Y%m%d_%H%M%S',
            'trg_format': 'parquet'
        }
        self.source_config = XetraSourceConfig(**conf_dict_src)
        self.target_config = XetraTargetConfig(**conf_dict_trg)
        # Creating source files on mocked s3
        columns_src = ['ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice',
        'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume']
        data = [['AT0000A0E9W5', 'SANT', '2021-04-15', '12:00', 20.19, 18.45, 18.20, 20.33, 877],
                ['AT0000A0E9W5', 'SANT', '2021-04-16', '15:00', 18.27, 21.19, 18.27, 21.34, 987],
                ['AT0000A0E9W5', 'SANT', '2021-04-17', '13:00', 20.21, 18.27, 18.21, 20.42, 633],
                ['AT0000A0E9W5', 'SANT', '2021-04-17', '14:00', 18.27, 21.19, 18.27, 21.34, 455],
                ['AT0000A0E9W5', 'SANT', '2021-04-18', '07:00', 20.58, 19.27, 18.89, 20.58, 9066],
                ['AT0000A0E9W5', 'SANT', '2021-04-18', '08:00', 19.27, 21.14, 19.27, 21.14, 1220],
                ['AT0000A0E9W5', 'SANT', '2021-04-19', '07:00', 23.58, 23.58, 23.58, 23.58, 1035],
                ['AT0000A0E9W5', 'SANT', '2021-04-19', '08:00', 23.58, 24.22, 23.31, 24.34, 1028],
                ['AT0000A0E9W5', 'SANT', '2021-04-19', '09:00', 24.22, 22.21, 22.21, 25.01, 1523]
            ]
        # Creating Source DataFrame
        self.df_src = pd.DataFrame(data, columns=columns_src)
        
        # Adding files to s3 and ensuring the prefix are aligned for test
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[0:0],
        '2021-04-15/2021-04-15_TEST_FILE12.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[1:1],
        '2021-04-16/2021-04-16_TEST_FILE15.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[2:2],
        '2021-04-17/2021-04-17_TEST_FILE13.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[3:3],
        '2021-04-17/2021-04-17_TEST_FILE14.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[4:4],
        '2021-04-18/2021-04-18_TEST_FILE07.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[5:5],
        '2021-04-18/2021-04-18_TEST_FILE08.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[6:6],
        '2021-04-19/2021-04-19_TEST_FILE07.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[7:7],
        '2021-04-19/2021-04-19_TEST_FILE08.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[8:8],
        '2021-04-19/2021-04-19_TEST_FILE09.csv','csv')
        
        # For the target table
        columns_report = ['ISIN', 'Date', 'opening_price_eur', 'closing_price_eur',
        'minimum_price_eur', 'maximum_price_eur', 'daily_traded_volume', 'change_prev_closing_%']
        data_report = [['AT0000A0E9W5', '2021-04-17', 20.21, 18.27, 18.21, 21.34, 1088, 10.62],
                       ['AT0000A0E9W5', '2021-04-18', 20.58, 19.27, 18.89, 21.14, 10286, 1.83],
                       ['AT0000A0E9W5', '2021-04-19', 23.58, 24.22, 22.21, 25.01, 3586, 14.58]]
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
        extract_date = '2021-04-17'
        extract_date_list = ['2021-04-16', '2021-04-17', '2021-04-18', '2021-04-19', '2021-04-20']
        
        # Method execution
        with patch.object(MetaProcess, "return_date_list",return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_bucket_src, self.s3_bucket_trg, self.meta_key, self.source_config, self.target_config)
            df_result = xetra_etl.extract()
            
        # Test after method execution
        self.assertTrue(expected_df.equals(df_result))
    
    def test_extract_no_files(self):
        """
        Tests the extract method when
        there are no files to be extracted
        """
        # Test init
        extract_date = '9999-12-31'
        extract_date_list = []
        # Method execution
        with patch.object(MetaProcess, "return_date_list", return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_bucket_src, self.s3_bucket_trg,
                         self.meta_key, self.source_config, self.target_config)
            df_return = xetra_etl.extract()
        # Test after method execution
        self.assertTrue(df_return.empty)
        
    # ALTERNATIVE METHOD TO test_extract
    # @mock_aws
    # def test_extract(self):
    #     # Setup the mock S3 environment
    #     s3 = boto3.resource("s3", region_name="us-east-1")
    #     bucket_name = "test-bucket"
    #     s3.create_bucket(Bucket=bucket_name)

    #     # Populate the bucket with some mock data
    #     s3.Object(bucket_name, "2023-01-01/file1.csv").put(Body="col1,col2\n1,3\n2,4\n")
    #     s3.Object(bucket_name, "2023-01-01/file2.csv").put(Body="col1,col2\n5,7\n6,8\n")

    #     # Create a mock S3BucketConnector
    #     s3_bucket_src = S3BucketConnector(
    #         AWS_ACCESS_KEY="AWS_ACCESS_KEY",
    #         AWS_SECRET_KEY="AWS_SECRET_KEY",
    #         endpoint_url="https://s3.eu-central-1.amazonaws.com",
    #         bucket=bucket_name,
    #     )
    #     s3_bucket_trg = S3BucketConnector(
    #         AWS_ACCESS_KEY="AWS_ACCESS_KEY",
    #         AWS_SECRET_KEY="AWS_SECRET_KEY",
    #         endpoint_url="https://s3.eu-central-1.amazonaws.com",
    #         bucket=bucket_name,
    #     )

    #     # Mock return_date_list method from MetaProcess to control the test scenario
    #     MetaProcess.return_date_list = lambda self, x, y: ("2023-01-01", ["2023-01-01"])

    #     # Instantiate XetraETL with the mocked S3 bucket connectors
    #     etl = XetraETL(
    #         s3_bucket_src=s3_bucket_src,
    #         s3_bucket_trg=s3_bucket_trg,
    #         meta_key="meta_key",
    #         src_args=XetraSourceConfig(
    #             src_first_extract_date="2023-01-01",
    #             src_columns=[],
    #             src_col_date="date",
    #             src_col_isin="isin",
    #             src_col_time="time",
    #             src_col_start_price="start_price",
    #             src_col_min_price="min_price",
    #             src_col_max_price="max_price",
    #             src_col_traded_vol="traded_vol",
    #         ),
    #         trg_args=XetraTargetConfig(
    #             trg_col_isin="isin",
    #             trg_col_date="date",
    #             trg_col_op_price="open_price",
    #             trg_col_clos_price="close_price",
    #             trg_col_min_price="min_price",
    #             trg_col_max_price="max_price",
    #             trg_col_dail_trad_vol="daily_traded_volume",
    #             trg_col_ch_prev_clos="change_prev_close",
    #             trg_key="key",
    #             trg_key_date_format="%Y-%m-%d",
    #             trg_format="csv",
    #         ),
    #     )

    #     # Call the extract method
    #     extracted_df = etl.extract()

    #     # Assertions to validate the correct behavior
    #     self.assertEqual(len(extracted_df), 4)  # Since two DataFrames are concatenated
    #     pd.testing.assert_frame_equal(
    #         extracted_df, pd.DataFrame({"col1": [1, 2, 5, 6], "col2": [3, 4, 7, 8]})
    #     )


if __name__ == "__main__":
    unittest.main()
