import statsapi
import pandas as pd
import plotly.express as px

def clean_columns(c):
    return ([x.lower().replace(' ', '_').replace('.', '_') for x in c])

def get_schedule(season, filter_final = True):
    b = statsapi.get('schedule', {'sportId': 1, 'startDate': '{}-01-01'.format(season), 'endDate': '{}-12-31'.format(season), "hydrate": "probablePitcher(note)"})
    c = pd.json_normalize([i for i in b['dates']], 'games')
    c.columns = clean_columns(c.columns)
    c = c[c['gametype'].isin(['R'])]
    if filter_final:
        c = c[c['status_statuscode'] == 'F']
    return(c)

def get_historical_lineup(gamepk, home_away = 'home'):
    game_lineup = statsapi.get("game", {"gamePk": gamepk})['liveData']['boxscore']['teams'][home_away]
    l = pd.DataFrame([(v['person']['id'], v['person']['fullName'], v.get('battingOrder') or None) for v in game_lineup['players'].values()], columns = ['id', 'name', 'order'])
    l = l.astype({'order': 'float'})
    l = l[~pd.isnull(l['order'])]
    l = l[l['order'] % 100 == 0]
    l = l.sort_values('order')
    return (l)

def get_upcoming_lineup(gamepk, home_away):
    #lineup consists of the nine batters, plus pitcher?
    game_lineup = statsapi.get("game", {"gamePk": gamepk})['liveData']['boxscore']['teams'][home_away]

    batting_lineup = game_lineup['batters']
    
    if len(batting_lineup) != 10 or batting_lineup is None:
        #print(gamepk)
        return None
    
    print('adding {}'.format(game_lineup['team']['name']))
    batting_lineup = [game_lineup['players'].get('ID{}'.format(x))['person']['fullName'] for x in batting_lineup]
    batting_lineup = pd.DataFrame({'id': range(100, 1000, 100), 'name': batting_lineup[:9]})
    
    return {game_lineup['team']['name']: batting_lineup}

#def summarize_game(game):
#    return (pd.DataFrame({'away_score': game.away_score_start
#                          , 'home_score': game.home_score_start
#                          , 'away_starter_name': game.away_pitching_lineup[0].name
#                          , 'home_starter_name': game.home_pitching_lineup[0].name}
#                         , index = [1]))

def summarize_batter(batter):
    return pd.DataFrame({'name': batter.name, 'real_name' :batter.real_name
                         , 'hits': batter.hits
                         , 'singles': batter.singles
                         , 'doubles': batter.doubles
                         , 'triples': batter.triples
                         , 'hrs': batter.hrs
                         , 'rbis': batter.rbis
                         , 'walks': batter.walks}
                        , index = [1])

def summarize_pitcher(pitcher):
    return pd.DataFrame({'name': pitcher.name
                         , 'hits': pitcher.hits
                         , 'outs': pitcher.outs
                         , 'pitches': pitcher.pitches
                         , 'strikeouts': pitcher.strikeouts
                         , 'walks': pitcher.walks}
                         , index = [1])

def convert_prob_to_american(p):
    if p == 0:
        return 99999
    
    p = p * 100
    if p == 100:
        return (-10000)
    if p > 50:
        return ((-p / (1 - p / 100)))
    else:
        return(100 / (p / 100) - 100)
    
def process_yrfi(games):
    yrfi = pd.concat([pd.DataFrame(g.at_bats) for g in games]).query('half_inning == 2 & inning_end == True')
    yrfi['yrfi'] = (yrfi['home_score_start'] + yrfi['away_score_start']) >= 1
    yrfi = yrfi.groupby('yrfi')['pitcher'].count().to_frame()
    yrfi = yrfi.rename({'pitcher': 'count'}, axis = 1)
    yrfi['odds'] = (yrfi['count'] / yrfi['count'].sum()).apply(convert_prob_to_american)
    return (yrfi)

def process_fh_total(games):
    fh_total = pd.concat([pd.DataFrame(g.at_bats) for g in games]).query('half_inning == 10 & inning_end == True')
    fh_total['total'] = (fh_total['home_score_start'] + fh_total['away_score_start'])
    return (fh_total)