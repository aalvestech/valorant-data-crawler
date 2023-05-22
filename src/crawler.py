from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import json
import pandas as pd
import pathlib

class Crawler():

    abs_path = pathlib.Path.cwd()
    globals() ["webdriver_path"] = pathlib.Path(abs_path).joinpath('chromedriver.exe')

    def get_matches(userId : str) -> bytes:

        '''
            The purpose of this function is to fetch all matches of the player entered in the userId parameter.
            These matches are divided into pages.

            :param userId: A variable that receives a player. For example: 'RayzenSama%236999'.
            :type userId: str

            :return: A variable that receives a string with the structure of a json.
            :rtype: bytes

            disclaimer::There is a possibility of using the crawler in a Docker environment as mentioned in line 31
            ( #driver = webdriver.Remote('http://crawler:4444/wd/hub', options = options) ).
            In this case, we would need to comment out lines 32 and 33 and uncomment line 31.
            I am not currently using it because the TrackGG servers have enhanced security measures with Cloudflare,
            which can detect requests made by Selenium Docker images.
        '''
    
        data = []
        next_page = 0
        while next_page is not None:

            print('Loading the page: {} from user: {}'.format(next_page, userId))
            options = webdriver.ChromeOptions()
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            # driver = webdriver.Remote('http://crawler:4444/wd/hub', options = options)
            global webdriver_path
            driver = webdriver.Chrome(webdriver_path, options = options)
            driver.get('https://api.tracker.gg/api/v2/valorant/standard/matches/riot/{}?type=competitive&next={}'.format(userId, next_page))
            data_pre = driver.find_element('xpath', '//pre').text
            data_json = json.loads(data_pre)

            if 'data' in data_json.keys() and data_json['data']['metadata']['next'] != 'null':

                next_page = data_json['data']['metadata']['next']
                
                matches = data_json['data']['matches']

                for match in matches:

                    dataframe_row = dict()
                    dataframe_row['userId'] = userId
                    dataframe_row['matchId'] = match['attributes']['id']
                    dataframe_row['mapId'] = match['attributes']['mapId']
                    dataframe_row['modeId'] = match['attributes']['modeId']
                    dataframe_row['modeKey'] = match['metadata']['modeKey']
                    dataframe_row['modeName'] = match['metadata']['modeName']
                    dataframe_row['modeImageUrl'] = match['metadata']['modeImageUrl']
                    dataframe_row['modeMaxRounds'] = match['metadata']['modeMaxRounds']
                    dataframe_row['isAvailable'] = match['metadata']['isAvailable']
                    dataframe_row['timestamp'] = match['metadata']['timestamp']
                    dataframe_row['metadataResult'] = match['metadata']['result']
                    dataframe_row['map'] = match['metadata']['map']
                    dataframe_row['mapName'] = match['metadata']['mapName']
                    dataframe_row['mapImageUrl'] = match['metadata']['mapImageUrl']
                    dataframe_row['seasonName'] = match['metadata']['seasonName']
                    dataframe_row['userId'] = match['segments'][0]['attributes']['platformUserIdentifier']
                    dataframe_row['hasWon'] = match['segments'][0]['metadata']['hasWon']
                    dataframe_row['result'] = match['segments'][0]['metadata']['result']
                    dataframe_row['agentName'] = match['segments'][0]['metadata']['agentName']
                    
                    for stat_name, stat_value in match['segments'][0]['stats'].items():

                        if stat_value is not None:
                            dataframe_row[f"{stat_name}Value"] = stat_value['value']
                            dataframe_row[f"{stat_name}DisplayValue"] = stat_value['displayValue']

                    data.append(dataframe_row)
                driver.quit()

        df = pd.DataFrame.from_dict(data)
        csv_string = df.to_csv(index=False)
        json_data_bytes = csv_string.encode('utf-8')

        return json_data_bytes
    
    def get_matches_details(matchId):
        
        '''
            The purpose of this function is to retrieve all the details of a Valorant match.
            To execute this function, we need to have previously executed the "get_matches" function.
            It is responsible for retrieving the match IDs that we use in this function.

            :param matchId: A variable that receives a matchId. For example: '4040a59c-9062-4478-85bd-609c0ed07c4b'.
            :type matchId: str

            :return: A variable that receives a string with the structure of a json.
            :rtype: bytes

            disclaimer::There is a possibility of using the crawler in a Docker environment as mentioned in line 31
            ( #driver = webdriver.Remote('http://crawler:4444/wd/hub', options = options) ).
            In this case, we would need to comment out lines 32 and 33 and uncomment line 31.
            I am not currently using it because the TrackGG servers have enhanced security measures with Cloudflare,
            which can detect requests made by Selenium Docker images.
        '''

        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        # driver = webdriver.Remote('http://crawler:4444/wd/hub', options = options)
        global webdriver_path
        driver = webdriver.Chrome(webdriver_path, options = options)

        #TODO: exception 403
        driver.get('https://api.tracker.gg/api/v2/valorant/standard/matches/{}'.format(matchId))
        data_pre = driver.find_element('xpath', '//pre').text
        data_json = json.loads(data_pre)

        driver.quit()

        json_data_bytes = json.dumps(data_json).encode('utf-8')

        return json_data_bytes