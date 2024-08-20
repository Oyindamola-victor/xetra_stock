"""
Xetra ETL Component
"""

from typing import NamedTuple
from xetra_code.common.s3 import S3BucketConnector
import logging
from xetra_code.common.meta_process import MetaProcess
import pandas as pd
from datetime import datetime


class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data

    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volumne in source
    """

    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


class XetraTargetConfig(NamedTuple):
    """
    Class for target configuration data

    trg_col_isin: column name for isin in target
    trg_col_date: column name for date in target
    trg_col_op_price: column name for opening price in target
    trg_col_clos_price: column name for closing price in target
    trg_col_min_price: column name for minimum price in target
    trg_col_max_price: column name for maximum price in target
    trg_col_dail_trad_vol: column name for daily traded volume in target
    trg_col_ch_prev_clos: column name for change to previous day's closing price in target
    trg_key: basic key of target file
    trg_key_date_format: date format of target file key
    trg_format: file format of the target file
    """

    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str


class XetraETL:
    """
    Reads the Xetra data, transforms and writes the transformed to target
    """

    def __init__(
        self,
        s3_bucket_src: S3BucketConnector,
        s3_bucket_trg: S3BucketConnector,
        meta_key: str,
        src_args: XetraSourceConfig,
        trg_args: XetraTargetConfig,
    ):
        """
        Constructor for XetraTransformer

        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_trg: connection to target S3 bucket
        :param meta_key: used as self.meta_key -> key of meta file
        :param src_args: NamedTuple class with source configuration data
        :param trg_args: NamedTuple class with target configuration data
        """

        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(
            self.s3_bucket_src, self.src_args.src_first_extract_date, self.meta_key
        )

        self.meta_update_list = [date for date in self.extract_date_list\
            if date >= self.extract_date]

    def extract(self):
        """
        This method reads the source data and extracts all the files based off the date
        and concatenates them to a Pandas DataFrame

        :returns:
            data_frame: Pandas DataFrame with the extracted data
        """
        self._logger.info("Extracting Xetra source files started...")
        files = [key for date in self.extract_date_list\
                    for key in self.s3_bucket_src.list_files_in_prefix(date)
                ]
        # print(f"ALL FILES:\n{files}")

        # Check if files were extracted
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat(
                [self.s3_bucket_src.read_csv_to_df_ok(file) for file in files],
                ignore_index=True,
            )
        self._logger.info("Finished Extracting Xetra source files.")
        # print(f"Dataframe from all files:\n{data_frame}")
        return data_frame

    
    def transform_report1(self, dataframe: pd.DataFrame):
        """
        Transform the source dataframe to generate the report with the required calculations.
        """
        if dataframe.empty:
            self._logger.info("The DataFrame is empty. No Transformation will be applied!")
            return dataframe

        self._logger.info('Applying transformations to Xetra source data for report 1 started...')
        
        # Filtering necessary source columns
        dataframe = dataframe.loc[:, self.src_args.src_columns]
        # Removing rows with missing values
        dataframe.dropna(inplace=True)
        
        # Calculating opening price per ISIN and day
        dataframe[self.trg_args.trg_col_op_price] = dataframe.sort_values(by=[self.src_args.src_col_time])\
        .groupby([self.src_args.src_col_isin, self.src_args.src_col_date])[self.src_args.src_col_start_price]\
        .transform("first")
        # print(f"Dataframe after WOP:\n{dataframe.head(8)}")
        # Calculating closing price per ISIN and day
        dataframe[self.trg_args.trg_col_clos_price] = dataframe.sort_values(by=[self.src_args.src_col_time])\
        .groupby([self.src_args.src_col_isin, self.src_args.src_col_date])[self.src_args.src_col_start_price]\
        .transform("last")
        # print(f"Dataframe after CP:\n{dataframe.head(8)}")
        
        # Renaming columns
        dataframe.rename(columns={
            self.src_args.src_col_min_price: self.trg_args.trg_col_min_price,
            self.src_args.src_col_max_price: self.trg_args.trg_col_max_price,
            self.src_args.src_col_traded_vol: self.trg_args.trg_col_dail_trad_vol
            }, inplace=True)
        
        # Aggregating per ISIN and day
        dataframe = dataframe.groupby([self.src_args.src_col_isin, self.src_args.src_col_date], as_index=False)\
        .agg({
        self.trg_args.trg_col_op_price: 'min', 
        self.trg_args.trg_col_clos_price: 'min',
        self.trg_args.trg_col_min_price: 'min',
        self.trg_args.trg_col_max_price: 'max',
        self.trg_args.trg_col_dail_trad_vol: 'sum'})
        # print(f"Dataframe after Aggregation:\n{dataframe.head(8)}")
        
        # Calculating the percentage change in closing prices compared to the previous day
        dataframe["prev_closing_price"] = dataframe\
            .sort_values(by=[self.src_args.src_col_date])\
                .groupby([self.src_args.src_col_isin])[self.trg_args.trg_col_clos_price]\
                    .shift(1)
        # print(f"Dataframe after percent_change:\n{dataframe.head(8)}")
        
        # Calculating percentage change
        dataframe[self.trg_args.trg_col_ch_prev_clos] = (
            (dataframe[self.trg_args.trg_col_clos_price] - dataframe["prev_closing_price"])
            / dataframe["prev_closing_price"] * 100
        )
        
        dataframe.drop(columns=['prev_closing_price'], inplace=True)
        # Rounding to 2 decimals
        dataframe = dataframe.round(decimals=2)
        
        # Removing the days before extract_date
        dataframe = dataframe[dataframe.Date >= self.extract_date].reset_index(drop=True)
        
        self._logger.info('Applying transformations to Xetra source data finished...')
        return dataframe



    def load_to_s3(self, dataframe: pd.DataFrame):
        """
        This method writes the transformed report to the target bucket
        """
        # Creating target key
        target_key =  (
            f'{self.trg_args.trg_key}'
            f'{datetime.today().strftime(self.trg_args.trg_key_date_format)}.'
            f'{self.trg_args.trg_format}'
        )
        # Writing to target
        self.s3_bucket_trg.write_df_to_s3(dataframe, target_key, self.trg_args.trg_format)
        self._logger.info('Xetra target data successfully written.')
        
        # Updating meta file
        MetaProcess.update_meta_file(self.s3_bucket_trg, self.meta_key, self.meta_update_list)
        self._logger.info('Xetra meta file successfully updated.')
        return True

    def etl_report1(self):
        """
        This method streamlines the creation of the etl_report1 by 
        taking all the required methods of extract, transform_report1, load_to_s3
        """
        
        # Running the extraction using extract method
        data_frame = self.extract()
        # Performing the transformation by using the dataframe returned from the extraction
        transformed_data_frame = self.transform_report1(data_frame)
        self.load_to_s3(transformed_data_frame)
        
        return True
        
        
