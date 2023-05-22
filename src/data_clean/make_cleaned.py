from src.crawler import Crawler
from src.aws.aws import Aws
from src.data_clean.data_cleaner import DataCleaner
import pandas as pd
import json
import io

aws = Aws()
dc = DataCleaner()

objects = aws.list_objetcs_s3(bucket_name = 's3-tcc-fia-valorant', folder_path = 'valorant/raw/details/matches/')

def clean_data(list_s3_objetcs : list) -> list:

    """
    
        The mission of this function is to execute the primary data cleaning function, for example dc.clean_match_metadata(data_dict), in order to obtain the already cleaned data and concatenate it into a list.
        This list will then be passed to the dc.clean_match_metadata(data_dict) function, which will further concatenate it into final dataframes.
        
        :param list_s3_objetcs: A list that receives a list of objects containing match data, player data, round data, and so on...
        :type list_s3_objetcs: LIST

        :return: A set of lists with the data already processed and concatenated. According to the processing of this function.
        :rtype: list
    """

    json_files = [obj['Key'] for obj in list_s3_objetcs]

    match_metadata_data = []
    team_summary_data = []
    round_summary_data = []
    player_round_data = []
    player_round_damage_data = []
    player_loadout_data = []
    player_round_kills_data = []
    player_summary_data = []

    for file in json_files:

        response = aws.s3.get_object(Bucket='s3-tcc-fia-valorant', Key=file)
        json_data = response['Body'].read().decode('utf-8')

        if json_data:

            data_dict = json.loads(json_data)
            match_metadata_row = dc.clean_match_metadata(data_dict)
            match_metadata_data.append(match_metadata_row)
            team_summary_row = dc.clean_team_summary(data_dict)
            team_summary_data.append(team_summary_row)
            round_summary_row = dc.clean_round_summary(data_dict)
            round_summary_data.append(round_summary_row)
            player_round_row = dc.clean_player_round(data_dict)
            player_round_data.append(player_round_row)
            player_round_damage_row = dc.clean_player_round_damage(data_dict)
            player_round_damage_data.append(player_round_damage_row)
            player_loadout_row = dc.clean_player_loadout(data_dict)
            player_loadout_data.append(player_loadout_row)
            player_round_kills_row = dc.clean_player_round_kills(data_dict)
            player_round_kills_data.append(player_round_kills_row)
            player_summary_row = dc.clean_player_summary(data_dict)
            player_summary_data.append(player_summary_row)

    return match_metadata_data, team_summary_data, round_summary_data, player_round_data, player_round_damage_data, player_loadout_data, player_round_kills_data, player_summary_data
   

def create_df_list(match_metadata_data : list, team_summary_data : list, round_summary_data : list, player_round_data : list, player_round_damage_data : list, player_loadout_data : list, player_round_kills_data : list, player_summary_data : list) -> list:
        
    """

        The mission of this function is to execute the primary data cleaning function, for example dc.clean_match_metadata(data_dict), in order to obtain the already cleaned data and concatenate it into a list.
        This list will then be passed to the dc.clean_match_metadata(data_dict) function, which will further concatenate it into final dataframes.
        
        :param match_metadata_data: A list that receives the metadata of the matches.
        :type match_metadata_data: list

        :param team_summary_data: A list that receives the summarized data of matches in the blue team and red team views.
        :type team_summary_data: list

        :param round_summary_data: A list that receives the summarized data of rounds in a match.
        :type round_summary_data: list

        :param player_round_data: A list that receives the data of rounds from a player's perspective.
        :type player_round_data: list

        :param player_round_damage_data: A list that receives the damage data caused by a player in a specific round.
        :type player_round_damage_data: list

        :param player_loadout_data: A list that receives the equipment data used by a player in a match.
        :type player_loadout_data: list

        :param player_round_kills_data: A list that receives the kill data made by a player in a round.
        :type player_round_kills_data: list

        :param player_summary_data: A list that receives the summarized data of a player per match.
        :type player_summary_data: list

        :return: A list of dataframes that contains player data, match data, equipment data, and so on... . According to the processing of this function.
        :rtype: list
    """

    df_match_metadata_data = pd.concat(match_metadata_data, ignore_index=True)
    df_team_summary_data = pd.concat(team_summary_data, ignore_index=True)
    df_round_summary_data = pd.concat(round_summary_data, ignore_index=True)
    df_player_round_data = pd.concat(player_round_data, ignore_index=True)
    df_player_round_damage_data = pd.concat(player_round_damage_data, ignore_index=True)
    df_player_loadout_data = pd.concat(player_loadout_data, ignore_index=True)
    df_player_round_kills_data = pd.concat(player_round_kills_data, ignore_index=True)
    df_player_summary_data = pd.concat(player_summary_data, ignore_index=True)
    dfs_list = [df_match_metadata_data, df_team_summary_data, df_round_summary_data, df_player_round_data, df_player_round_damage_data, df_player_loadout_data, df_player_round_kills_data, df_player_summary_data]
    
    return dfs_list


def write_dfs(dfs_list : list, file_names : list, folders: list) -> None:

    """
    
        The mission of this function is to group the information of a list of dataframes, file names, and folders in order to simplify the writing of this data to an S3 bucket.
        
        :param dfs_list: A list that contains the dataframes to be written to the S3 bucket.
        :type dfs_list: list

        :param file_names: A list that contains the file names to be written to the S3 bucket.
        :type file_names: list

        :param folders: A list that contains the folders names to be written to the S3 bucket.
        :type folders: list
    """

    for dfi, file_name, folder in zip(dfs_list, file_names, folders):

        csv_buffer = io.StringIO()
        dfi.to_csv(csv_buffer, index=False)
        csv_buffer_value = csv_buffer.getvalue()
        aws.write_s3(bucket_name='s3-tcc-fia-valorant', folder_path='valorant/cleaned/details/{}/'.format(folder), file_name=file_name, data=csv_buffer_value, file_format='.csv')


def run_clean() -> None:

    """
    
        The mission of this function is to centralize key functions to facilitate the writing of data with their names and respective folders in the S3 bucket.
    """

    match_metadata_data, team_summary_data,round_summary_data,player_round_data,player_round_damage_data,player_loadout_data,player_round_kills_data,player_summary_data = clean_data(objects)
    dfs_list = create_df_list(match_metadata_data, team_summary_data,round_summary_data,player_round_data,player_round_damage_data,player_loadout_data,player_round_kills_data,player_summary_data)
    file_names = ['matches_metadata', 'team_summary', 'round_summary', 'player_round', 'player_round_damage', 'player_loadout', 'player_round_kills', 'player_summary']
    folders = ['metadata', 'team_summary', 'round_summary', 'player_round', 'player_round_damage','player_loadout', 'player_round_kills', 'player_summary']
    
    write_dfs(dfs_list, file_names, folders)