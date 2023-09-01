import streamlit as st
import pandas as pd
import seaborn as sns
import copy
from credentials import user, passwd
from utils import get_schedule, get_historical_lineup, summarize_game, summarize_batter, summarize_pitcher, convert_prob_to_american, get_upcoming_lineup
import os
import pandas as pd
from db_connect import get_connection
from game import Game, AtBat
from player import Pitcher, Bullpen, Batter
import plotly.express as px
import numpy as np


st.set_page_config(layout="wide")

cm = sns.light_palette("green", as_cmap=True)

st.title("MLB Game Simulations")

todays_games = get_schedule(season = 2023, filter_final=False).query('officialdate == "2023-07-26"')
todays_games['gamedate'] = pd.to_datetime(todays_games['gamedate'])
todays_games['gamedate'] = todays_games['gamedate'].dt.tz_convert("US/Eastern")
todays_games['starttime'] = todays_games['gamedate'].dt.strftime('%I:%M %p')
todays_games['gameid'] = todays_games['teams_away_team_name'] + '_' + todays_games['teams_home_team_name']
display = todays_games[['gamepk', 'officialdate', 'starttime', 'gameid', 'teams_away_probablepitcher_fullname'
                        , 'teams_home_probablepitcher_fullname']]

historical_games = get_schedule(season = 2023, filter_final=True)
last_home_game = historical_games.groupby('teams_home_team_name')['gamepk'].apply(lambda x: x.tail(1)).reset_index()

last_lineups = {}
for i, r in last_home_game.iterrows():
    h = get_historical_lineup(r['gamepk'], 'home')
    h['name'] = h['name'].str.replace('\'', '')
    last_lineups.update({r['teams_home_team_name']: h})
    
st.session_state['last_lineups'] = last_lineups


engine = get_connection()
all_batters = pd.read_sql('SELECT DISTINCT batter_name FROM mlb_play_simulator_data', engine)
all_pitchers = pd.read_sql('SELECT DISTINCT pitcher_name FROM mlb_play_simulator_data', engine)

def clear_state():
    st.session_state['games'] = None
    st.session_state['batters'] = None
    st.session_state['pitchers'] = None
    
#clear_state()

def run_simulation():
    
    print(n_games)
#    st.session_state['num_games'] = n_games
    away_starting_pitcher = Pitcher(away_pitcher[0])
    home_starting_pitcher = Pitcher(home_pitcher[0])
    away_pitching_lineup = [away_starting_pitcher, Bullpen('away')]
    home_pitching_lineup = [home_starting_pitcher, Bullpen('home')]
    
    away_batting_lineup = [Batter(a) for a in away_lineup]
    home_batting_lineup = [Batter(b) for b in home_lineup]

    games = []
    batters = []
    pitchers = []

    for i in range(n_games):
        g = Game(home_batting_lineup = copy.deepcopy(home_batting_lineup)
             , away_batting_lineup = copy.deepcopy(away_batting_lineup)
             , home_pitching_lineup = copy.deepcopy(home_pitching_lineup)
             , away_pitching_lineup = copy.deepcopy(away_pitching_lineup))
        g.play_game()
        games = games + [g]
        batters = batters + g.away_batting_lineup + g.home_batting_lineup 
        pitchers = pitchers + g.away_pitching_lineup + g.home_pitching_lineup

    st.session_state['games'] = games
    st.session_state['batters'] = batters 
    st.session_state['pitchers'] = pitchers
    return True

def check_new_lineups(gamepk):
    away = get_upcoming_lineup(gamepk, 'away')
    if away is not None:
        last_lineups.update(away)
    home = get_upcoming_lineup(gamepk, 'home')
    if home is not None:
        last_lineups.update(home)
    return True
    
    
    

with st.sidebar:
    teams = todays_games['gameid']
    team_choice = st.selectbox('Which game?', (teams), on_change = clear_state)
    
    this_game = todays_games[todays_games['gameid'] == team_choice]
    
    check_new_lineups(this_game['gamepk'].iloc[0])

    
    away_lineup = st.multiselect('Away lineup?',
                                 np.append(all_batters['batter_name'].values, last_lineups[this_game['teams_away_team_name'].iloc[0]]['name']),
                                 last_lineups[this_game['teams_away_team_name'].iloc[0]]['name'],
                                 on_change = clear_state)
    
    away_probable = this_game['teams_away_probablepitcher_fullname'].fillna('Missing').iloc[0]
    away_pitcher = st.multiselect('Away pitcher?',
                                  np.append(all_pitchers['pitcher_name'].values, ['Missing', away_probable]),
                                  away_probable,
                                  on_change = clear_state)

    home_lineup = st.multiselect('Home lineup?',
                                 np.append(all_batters['batter_name'].values, last_lineups[this_game['teams_home_team_name'].iloc[0]]['name']),
                                 last_lineups[this_game['teams_home_team_name'].iloc[0]]['name'], 
                                 on_change = clear_state)
    
    home_probable = this_game['teams_home_probablepitcher_fullname'].fillna('Missing').iloc[0]
    
    home_pitcher = st.multiselect('Home pitcher?',
                                  np.append(all_pitchers['pitcher_name'].values, ['Missing', home_probable]),
                                  home_probable,
                                  on_change = clear_state)

    n_games = st.number_input('How many simulations?', value = 100, on_change = clear_state)

    run_sim = st.button('Run Simulation', on_click = run_simulation)

col1, col2 = st.columns(2)

with col1:
    st.header('Run in First Inning?')
    if st.session_state['games']:
        yrfi = pd.concat([pd.DataFrame(g.at_bats) for g in st.session_state['games']]).query('half_inning == 2 & inning_end == True')
        yrfi['yrfi'] = (yrfi['home_score'] + yrfi['away_score']) >= 1
        yrfi = yrfi.groupby('yrfi')['pitcher'].count().to_frame()
        yrfi = yrfi.rename({'pitcher': 'count'}, axis = 1)
        yrfi['odds'] = (yrfi['count'] / yrfi['count'].sum()).apply(convert_prob_to_american)
        yrfi
    
    st.header('First Half Total?')
    if st.session_state['games']:
        fh_total = pd.concat([pd.DataFrame(g.at_bats) for g in st.session_state['games']]).query('half_inning == 10 & inning_end == True')
        fh_total['total'] = (fh_total['home_score'] + fh_total['away_score'])
        fh = px.histogram(x = 'total', data_frame = fh_total)
        fh.add_vline(x=fh_total['total'].median(), annotation_text = fh_total['total'].median())
        st.plotly_chart(fh, use_container_width=True)
    
    st.header('First Half Spread?')
    if st.session_state['games']:
        fh_total['home_lead'] = (fh_total['home_score'] - fh_total['away_score'])
        fh_diff = px.histogram(x = 'home_lead', data_frame = fh_total)
        fh_diff.add_vline(x=fh_total['home_lead'].median(), annotation_text = fh_total['home_lead'].median())
        st.plotly_chart(fh_diff, use_container_width=True)

with col2:
    st.header('Pitcher Stats')
    if st.session_state['pitchers']:
        p = pd.concat([summarize_pitcher(x) for x in st.session_state['pitchers']]).groupby('name').median()
        p
    
    st.header('Batter Stats')
    if st.session_state['batters']:
        b = pd.concat([summarize_batter(x) for x in st.session_state['batters']]).groupby(['name', 'real_name']).apply(lambda x: (x >= 1).mean())
        b
        
#    [x.real_name for x in st.session_state['games'][0].home_batting_lineup]
#    [x.real_name for x in st.session_state['games'][0].away_batting_lineup]
    
    h = pd.concat([summarize_batter(x) for x in st.session_state['batters']])
    h
#    px.histogram(x = 'strikeouts', data_frame = pd.concat([summarize_pitcher(x) for x in pitchers]).query('name == "Dean Kremer"'))

#    fh_total = fh_total.groupby('total')['pitcher'].count().to_frame()
#    fh_total = fh_total.rename({'pitcher': 'count'}, axis = 1)
#    fh_total['odds'] = (fh_total['count'] / fh_total['count'].sum()).apply(convert_prob_to_american)
#    fh_total

#display
#if confirm_only:
#    dat['is_confirmed'] = dat['Player'].apply(lambda x: x in lineups)
#    a, b = show_data(dat, league_choice, team_choice, opp_choice)
#    c = summarize_data(dat, league_choice, team_choice, opp_choice)
#    g, sog, y = top_data(dat, league_choice, team_choice, opp_choice)

#col1, col2, col3 = st.columns(3)

#with col1:
#    st.dataframe(a.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)
#    st.dataframe(g.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)

#with col2:
#    st.dataframe(b.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)
#    st.dataframe(sog.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)
    
#with col3:
#    st.dataframe(c.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)
#    st.dataframe(y.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)