import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
from botocore.exceptions import ClientError
import logging
import pandas as pd

load_dotenv()

class Aws:

    def __init__(self):
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        )


    def write_s3(self, bucket_name : str, folder_path : str, file_name : str, data : object, file_format : str) -> bool:

        """
            Upload a file to an S3 bucket.

            :param bucket_name: The name of the S3 bucket where the file will be saved.
            :type bucket_name: str

            :param folder_path: The path inside the bucket where the files will be saved.
            :type folder_path: str

            :param file_name: The name of the file that will be written to the S3 bucket.
            :type file_name: str
            disclaimer:: This name will undergo a modification during the process that will add the execution date of this function and a file extension.

            :param data: The object that contains the data to be stored in the S3 bucket.
            :type data: bytes

            :return: True if file was uploaded, else False
            :rtype: bool
        """

        date = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = file_name + '_' + date + file_format
        file_path = folder_path + file_name

        try:
            self.s3.put_object(Bucket=bucket_name, Key=file_path, Body=data)
            print(f"Data was written to S3://{bucket_name}/{folder_path}")

        except Exception as e:

            print(f"Error: {e}")
        
            return False

        return True
    

    def read_s3(self, bucket_name: str, folder_path: str) -> pd.DataFrame:

        """
        Reads all CSV files in a folder in an S3 bucket and returns a list of pandas DataFrames.

        :param bucket_name: The name of the S3 bucket.
        :type bucket_name: str

        :param folder_path: The path to the folder in the bucket that contains the CSV files.
        :type folder_path: str

        :return: A list of pandas DataFrames containing the data from the CSV files in the folder.
        :rtype: bool

        """

        try:

            objects = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)["Contents"]
            data = self.s3.get_object(Bucket = bucket_name, Key = objects["Key"])

            return data, objects

        except ClientError as e:

            logging.error(e)

        except ValueError as e:

            raise(e)


    def read_s3_v2(self, bucket_name: str, folder_path: str) -> pd.DataFrame:

        """
        Reads all CSV files in a folder in an S3 bucket and returns a list of pandas DataFrames.

        :param bucket_name: The name of the S3 bucket.
        :type bucket_name: str

        :param folder_path: The path to the folder in the bucket that contains the CSV files.
        :type folder_path: str
        """

        try:
   
            data = self.s3.get_object(Bucket = bucket_name, Key = folder_path)

            return data

        except ClientError as e:

            logging.error(e)

        except ValueError as e:

            raise(e)
        
    def list_objetcs_s3(self, bucket_name: str, folder_path: str) -> pd.DataFrame:

        """
        Reads all CSV files in a folder in an S3 bucket and returns a list of pandas DataFrames.

        :param bucket_name: The name of the S3 bucket.
        :type bucket_name: str

        :param folder_path: The path to the folder in the bucket that contains the CSV files.
        :type folder_path: str

        :return: A list of pandas DataFrames containing the data from the CSV files in the folder.
        :rtype: list
        """

        try:

            objects = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)["Contents"]
            return objects

        except ClientError as e:

            logging.error(e)

        except ValueError as e:
            
            raise(e)