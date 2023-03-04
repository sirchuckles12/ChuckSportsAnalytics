import pandas as pd
import requests

BASE = "http://statsapi.web.nhl.com/api/v1"

def parse_schedule(b):
    game_pk = b['gamePk']
    game_type = b['gameType']
    game_date = b['gameDate']
    game_final = b['status']['detailedState'] == 'Final'
    home_team = b['teams']['home']['team']['name']
    away_team = b['teams']['away']['team']['name']
    
    if game_final:
        home_score = b['teams']['home']['score']
        away_score = b['teams']['away']['score']
    else:
        home_score = -1
        away_score = -1
     
    return(pd.DataFrame({"game_pk": game_pk
                         , "game_type": game_type
                         , "game_date": game_date
                         , "game_final": game_final
                         , "home_team": home_team
                         , "away_team": away_team
                         , "home_score": home_score
                         , "away_score": away_score
                         }, index = [1]))
    

def get_schedule(startdate, enddate):
    s = requests.get('{}/schedule/'.format(BASE)
                     , params = {'startDate': startdate, 'endDate': enddate}).json()
    
    dat = pd.concat([parse_schedule(a) for d in s['dates'] for a in d['games']]).reset_index()
    del dat['index']
    return (dat)