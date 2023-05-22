import pandas as pd

class DataCleaner:

    def __init__(self) -> None:
        pass
        

    def clean_match_metadata(self, json_dict : dict) -> pd.DataFrame:
        
        """
            The mission of this function is to clean and organize the detailed metadata of a Valorant match, making it easier to read and write this data.

            :param json_dict: A dictionary containing the metadata of a Valorant match:
            :type json_dict: dict

            :return: A clean and organized dataframe containing the metadata of a match according to the processing of this function.
            :rtype: pd.Dataframe
        """

        json_obj = json_dict['data']

        rows = {}
        rows['matchId'] = json_obj['attributes']['id']
        rows['expiryDate'] = json_obj['expiryDate']
        rows['modeKey'] = json_obj['metadata']['modeKey']
        rows['modeName'] = json_obj['metadata']['modeName']
        rows['modeImageUrl'] = json_obj['metadata']['modeImageUrl']
        rows['modeMaxRounds'] = json_obj['metadata']['modeMaxRounds']
        rows['duration'] = json_obj['metadata']['duration']
        rows['dateStarted'] = json_obj['metadata']['dateStarted']
        rows['rounds'] = json_obj['metadata']['rounds']
        rows['isRanked'] = json_obj['metadata']['isRanked']
        rows['queueId'] = json_obj['metadata']['queueId']
        rows['seasonId'] = json_obj['metadata']['seasonId']
        rows['map'] = json_obj['metadata']['map']
        rows['mapName'] = json_obj['metadata']['mapName']
        rows['mapImageUrl'] = json_obj['metadata']['mapImageUrl']

        df_match_metadata = pd.DataFrame([rows], index=[0])
        
        return df_match_metadata
    
    def clean_team_summary(self, json_dict : dict) -> pd.DataFrame:

        """
            The mission of this function is to clean and organize the summarized data of a team in Valorant match, making it easier to read and write this data.
            
            :param json_dict: A dictionary containing the summary of a team in Valorant match:
            :type json_dict: dict

            :return: A clean and organized dataframe containing the summary of a team according to the processing of this function.
            :rtype: pd.Dataframe
        """

        json_obj = json_dict['data']
        rows = []

        for segment in json_obj['segments']:

            if segment['type'] == "team-summary":

                team_name = segment["attributes"]["teamId"]
                team_dict = {
                    'matchId': json_obj['attributes']['id'],
                    'teamId': team_name,
                    'hasWon': segment['metadata']['hasWon'],
                }

                for stat_name, stat_value in segment['stats'].items():

                    stat_name = stat_name.title()
                    team_dict[f"{stat_name}Value"] = stat_value['value']
                    team_dict[f"{stat_name}DisplayValue"] = stat_value['displayValue']

                rows.append(team_dict)

        df_team_summary = pd.DataFrame(rows)
        
        return df_team_summary
    
    def clean_round_summary(self, json_dict : dict) -> pd.DataFrame:

        """
            The mission of this function is to clean and organize the summarized data of rounds in a Valorant match, making it easier to read and write this data.
            
            :param json_dict: A dictionary containing the summary of rounds in Valorant match:
            :type json_dict: dict

            :return: A clean and organized dataframe containing the summary of rounds according to the processing of this function.
            :rtype: pd.Dataframe
        """

        json_obj = json_dict['data']
        rows = []

        for segment in json_obj['segments']:

            if segment['type'] == "round-summary":
                
                round = segment['attributes']['round']
                team_dict = {
                    'matchId': json_obj['attributes']['id'],
                    'Round': round
                }

                for stat_name, stat_value in segment['stats'].items():

                    stat_name = stat_name.title()
                    team_dict[f"{stat_name}Value"] = stat_value['value']
                    team_dict[f"{stat_name}DisplayValue"] = stat_value['displayValue']

                rows.append(team_dict)

        df_round_summary = pd.DataFrame(rows)

        return df_round_summary
    

    def clean_player_round(self, json_dict : dict) -> pd.DataFrame:

        """
            The mission of this function is to clean and organize player data in a round-by-round view of a Valorant match, facilitating the reading and writing of this data.
            
            :param json_dict: A dictionary containing the data of players in a round-by-round view of a Valorant match:
            :type json_dict: dict

            :return: A clean and organized dataframe containing the data of players in a round-by-round view of a Valorant match.
            :rtype: pd.Dataframe
        """

        json_obj = json_dict['data']
        rows = []

        for segment in json_obj['segments']:

            if segment['type'] == "player-round":

                platform_user_id = segment['attributes']['platformUserIdentifier']
                round_num = segment['attributes']['round']
                stats = segment['stats']

                for stat_name, stat_value in stats.items():

                    stat_name = stat_name.title()
                    row = {
                        'MatchId': json_obj['attributes']['id'],
                        'platformUserIdentifier': platform_user_id,
                        'Round': round_num,
                        'StatName': stat_name,
                        'StatValue': stat_value['value'],
                        'StatDisplayValue': stat_value['displayValue'].replace(',', '.'),
                    }

                    rows.append(row)

        df_player_round = pd.DataFrame(rows)
        df_player_round = df_player_round.pivot_table(index=['MatchId', 'platformUserIdentifier', 'Round'], columns='StatName', values=['StatValue', 'StatDisplayValue'])
        df_player_round = df_player_round.reset_index()
        df_player_round.columns = [f'{col[0]}_{col[1]}' if col[1] else col[0] for col in df_player_round.columns]

        return df_player_round


    def clean_player_round_damage(self, json_dict : dict) -> pd.DataFrame:

        """
            The mission of this function is to clean and organize the damage data inflicted on players in a round-by-round view of a Valorant match, facilitating the reading and writing of this data.
            
            :param json_dict: A dictionary containing the data damage inflicted on players in a round-by-round view of a Valorant match:
            :type json_dict: dict

            :return: A clean and organized dataframe containing the data damage inflicted on players in a round-by-round view of a Valorant match.
            :rtype: pd.Dataframe
        """

        json_obj = json_dict['data']
        rows = []

        for segment in json_obj['segments']:

            if segment['type'] == "player-round-damage":

                platform_user_id = segment['attributes']['platformUserIdentifier']
                round_num = segment['attributes']['round']
                opponent_platform_user_id = segment['attributes']['opponentPlatformUserIdentifier']
                stats = segment['stats']

                for stat_name, stat_value in stats.items():

                    stat_name = stat_name.title()
                    row = {
                        'MatchId': json_obj['attributes']['id'],
                        'platformUserIdentifier': platform_user_id,
                        'opponentPlatformUserIdentifier': opponent_platform_user_id,
                        'Round': round_num,
                        f"{stat_name}Value": stat_value['value'],
                        f"{stat_name}DisplayValue": stat_value['displayValue']
                    }

                    rows.append(row)

        df_player_round_damage = pd.DataFrame(rows)
        
        return df_player_round_damage
    

    def clean_player_loadout(self, json_dict : dict) -> pd.DataFrame:

        """
            The mission of this function is to clean and organize the equipment data of a player, making it easier to read and write this data.
            
            :param json_dict: A dictionary containing the data organized the equipment data of a player, making it easier to read and write this data:
            :type json_dict: dict

            :return: A clean and organized dataframe containing the data organized the equipment data of a player, making it easier to read and write this data.
            :rtype: pd.Dataframe
        """

        json_obj = json_dict['data']
        rows = []

        for segment in json_obj['segments']:

            if segment['type'] == "player-loadout":

                platform_user_id = segment['attributes']['platformUserIdentifier']
                match_id = json_obj['attributes']['id']
                loadout = segment['attributes']['loadout']

                for stat_name, stat_value in segment['stats'].items():

                    stat_name = stat_name.title()
                    row = {
                        'MatchId': match_id,
                        'PlatformUserIdentifier': platform_user_id,
                        'Loadout': loadout,
                        'StatName': stat_name,
                        'StatValue': stat_value['value'],
                        'StatDisplayValue': stat_value['displayValue']
                    }

                    rows.append(row)

        df_player_loadout = pd.DataFrame(rows)

        return df_player_loadout
    
    
    def clean_player_round_kills(self, json_dict : dict) -> pd.DataFrame:

        """
            The mission of this function is to clean and organize the kill data of a player in a round-by-round view, making it easier to read and write this data.
            
            :param json_dict: A dictionary containing the the kill data of a player in a round-by-round view, making it easier to read and write this data:
            :type json_dict: dict

            :return: A clean and organized dataframe containing the the kill data of a player in a round-by-round view, making it easier to read and write this data.
            :rtype: pd.Dataframe
        """
    
        json_obj = json_dict['data']
        rows = []

        for segment in json_obj['segments']:

            if segment['type'] == "player-round-kills":

                row = dict()
                platform_user_id = segment['attributes']['platformUserIdentifier']

                if platform_user_id not in row.keys():

                    row.update({'MatchId':json_obj['attributes']['id']})
                    row.update({'platformUserIdentifier': platform_user_id})
                    opponent_platform_user_id = segment['attributes']['opponentPlatformUserIdentifier']
                    row.update({'opponentPlatformUserIdentifier': opponent_platform_user_id})

                for stat_name, stat_value in segment['stats'].items():

                    stat_name = stat_name.title()
                    round_num = segment['attributes']['round']
                    row.update({f"Round": round_num})
                    row.update({f"{stat_name}Value":stat_value['value']})
                    row.update({f"{stat_name}DisplayValue":stat_value['displayValue']})
                    
                for metadata_key, metadata_value in segment['metadata'].items():

                    if isinstance(metadata_value, dict):

                        for inner_key, inner_value in metadata_value.items():

                            row.update({f"{metadata_key.title()}_{inner_key.title()}": inner_value})

                    else:

                        row.update({f"{metadata_key.title()}": metadata_value})

                rows.append(row)

        df_player_round_kills = pd.DataFrame(rows)
        df_player_round_kills = df_player_round_kills.join(df_player_round_kills.pop('Assistants').apply(pd.Series))
        
        return df_player_round_kills


    def clean_player_summary(self, json_dict : dict) -> pd.DataFrame:

        """
            The mission of this function is to clean and organize the summarized player data in a match view, making it easier to read and write this data.
            
            :param json_dict: A dictionary containing the summarized player data in a match view, making it easier to read and write this data:
            :type json_dict: dict

            :return: A clean and organized dataframe containing the summarized player data in a match view, making it easier to read and write this data.
            :rtype: pd.Dataframe
        """

        json_obj = json_dict['data']
        rows = []

        for segment in json_obj['segments']:

            if segment['type'] == "player-summary":

                row = dict()
                platform_user_id = segment['attributes']['platformUserIdentifier']

                if platform_user_id not in row.keys():

                    row.update({'MatchId': json_obj['attributes']['id']})
                    row.update({'platformUserIdentifier': platform_user_id})

                for stat_name, stat_value in segment['stats'].items():
                    stat_name = stat_name.title()

                    if stat_value is not None:

                        row.update({f"{stat_name}Value": stat_value.get('value')})
                        row.update({f"{stat_name}DisplayValue": stat_value.get('displayValue')})

                    else:

                        row.update({f"{stat_name}Value": None})
                        row.update({f"{stat_name}DisplayValue": None})

                for metadata_key, metadata_value in segment['metadata'].items():

                    if isinstance(metadata_value, dict):

                        for inner_key, inner_value in metadata_value.items():

                            row.update({f"{metadata_key.title()}_{inner_key.title()}": inner_value})

                    else:

                        row.update({f"{metadata_key.title()}": metadata_value})

                rows.append(row)

        player_summary_df = pd.DataFrame(rows)

        return player_summary_df