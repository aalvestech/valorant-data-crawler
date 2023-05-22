from src.crawler import Crawler
from src.aws.aws import Aws
from src.data_clean.data_cleaner import DataCleaner
import pandas as pd
import io
from selenium.common import exceptions
from time import sleep

aws = Aws()


#TODO: Olhar a doc sphinx

        
def match_detail(match_id_list):
    """"""
    
    count = 0
    for matchId in match_id_list:
        len_match_id_list = len(match_id_list)
        print('Loading the match: {} {} / {}'.format(matchId, count, len_match_id_list))
        write_match_detail(matchId)
        count = count + 1 

def wrtite_match_summary(userId):
    """"""

    try:

        matches_summary_data = Crawler.get_matches(userId)
        aws.write_s3(bucket_name = 's3-tcc-fia-valorant', folder_path = 'valorant/raw/summary/matches/', file_name = 'matches_summary', data = matches_summary_data, file_format = '.csv')

    except exceptions.NoSuchElementException as e:
        
        print(e.msg)
        sleep(120)
        wrtite_match_summary(userId)

    except ValueError as e:
        raise(e)
            # TODO: Pegar value error https://docs.python.org/3/tutorial/errors.html


def write_match_detail(matchId):
    """"""

    try:      

        matches_details_data = Crawler.get_matches_details(matchId)
        aws.write_s3(bucket_name = 's3-tcc-fia-valorant', folder_path = 'valorant/raw/details/matches/', file_name = 'match_details_{}'.format(matchId), data =  matches_details_data, file_format = '.json')
    
    except exceptions.NoSuchElementException as e:

        print(e.msg)
        sleep(120)
        write_match_detail(matchId)

    except ValueError as e:

        raise(e)
            # TODO: Pegar value error https://docs.python.org/3/tutorial/errors.html


def concat_files_s3(objects):
    """"""

    json_files = [obj['Key'] for obj in objects]

    for file in json_files:

        response = aws.read_s3_v2(bucket_name='s3-tcc-fia-valorant', folder_path=file)
        json_data = response['Body'].read().decode('utf-8')

    return io.StringIO(json_data)

def read_pandas(data_io):
    """"""

    return pd.read_csv(data_io)

def run_raw_data():
    """"""

    wrtite_match_summary('RayzenSama%236999')
    
    objects = aws.list_objetcs_s3(bucket_name = 's3-tcc-fia-valorant', folder_path = 'valorant/raw/summary/matches/')
    data_io = concat_files_s3(objects)
    df = read_pandas(data_io)
    match_detail(df['matchId'])