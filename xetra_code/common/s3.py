"""_summary_: Connectors and methods accessing s3
"""

import os
import logging

import boto3
from configs.config import configuration


class S3BucketConnector():
    
    """_summary_: Class for interacting with s3 Buckets
    """
    
    def __init__(self, AWS_ACCESS_KEY:str, AWS_SECRET_KEY:str, endpoint_url:str, bucket:str ):
        """_summary_: Constructor for S3BucketConnector

        Args:
            AWS_ACCESS_KEY (str): access key for accessing s3
            AWS_SECRET_KEY (str): secret key for accessing s4
            endpoint_url (str): endpoint url to s3 e.g s3
            bucket (str): S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id = configuration[AWS_ACCESS_KEY],
                                     aws_secret_access_key=configuration[AWS_SECRET_KEY]
                                     )
        self._s3 = self.session.resource(service_name="s3", endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)
    
    def list_files_in_prefix(self, prefix:str):
        """_summary_: Listing all files with a prefix on the S3 Bucket 

        Args:
            prefix (str): prefix on the S3 buckrt that should be filtered with
        
        returns: 
            files: list of all the file names containing the prefix in the key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files
   
   
    def read_csv_to_df(self):
        pass
   
    def write_df_to_s3(self):
        pass
