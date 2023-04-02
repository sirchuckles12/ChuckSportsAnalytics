import streamlit as st
import pandas as pd
import seaborn as sns
from credentials import user, passwd
import os

st.set_page_config(layout="wide")

cm = sns.light_palette("green", as_cmap=True)

st.title("Soccer Projections and Lineups")

def show_data(dat, league, team, opp):
    a = dat[(dat['league'] == league) & (dat['Team'] == team) & (dat['Opp'] == opp)][['Player', 'Team', 'Pos', 'is_confirmed', 'G', 'A', 'SOG', 'Y', 'R', 'FC']]

    b = dat[(dat['league'] == league) & (dat['Team'] == opp) & (dat['Opp'] == team)][['Player', 'Team', 'Pos', 'is_confirmed', 'G', 'A', 'SOG', 'Y', 'R', 'FC']]
    
    return (a, b)

def summarize_data(dat, league, team, opp):
    c = dat[(dat['league'] == league) & (((dat['Team'] == team) & (dat['Opp'] == opp)) | ((dat['Team'] == opp) & (dat['Opp'] == team)))].groupby('Team')[['G', 'A', 'SOG', 'Y', 'R', 'FC']].sum()
    return (c)

def top_data(dat, league, team, opp):
    a = dat[(dat['league'] == league) & (dat['Team'] == team) & (dat['Opp'] == opp)][['Player', 'Team', 'Pos', 'is_confirmed', 'G', 'A', 'SOG', 'Y', 'R', 'FC']]
    b = dat[(dat['league'] == league) & (dat['Team'] == opp) & (dat['Opp'] == team)][['Player', 'Team', 'Pos', 'is_confirmed', 'G', 'A', 'SOG', 'Y', 'R', 'FC']]
    dat = pd.concat([a, b])
    g = dat[['Player', 'Team', 'is_confirmed', 'G']].sort_values(['is_confirmed', 'G'], ascending = False)
    sog = dat[['Player', 'Team', 'is_confirmed', 'SOG']].sort_values(['is_confirmed', 'SOG'], ascending = False)
    y = dat[['Player', 'Team', 'is_confirmed', 'Y']].sort_values(['is_confirmed', 'Y'], ascending = False)
    return (g, sog, y)

recent_date = os.listdir('projections/')[-1]
dat = pd.concat([pd.read_csv('C:/Users/Chuck/Desktop/GitRepos/ChuckSportsAnalytics/Soccer/projections/{}/{}'.format(recent_date, x)) for x in os.listdir('projections/{}/'.format(recent_date))])

dat['is_confirmed'] = False

with st.sidebar:
    leagues = dat['league'].unique()
    leagues.sort()
    league_choice = st.selectbox('Which league?', (leagues))
    
    first_team = dat.query('league == "{}"'.format(league_choice))['Team'].unique()
    first_team.sort()
    team_choice = st.selectbox('Which team?', (first_team))
    
    second_team = dat.query('league == "{}" & Team == "{}"'.format(league_choice, team_choice))['Opp'].unique()
    second_team.sort()
    opp_choice = st.selectbox('Which opponent?', (second_team))
    
    a, b = show_data(dat, league_choice, team_choice, opp_choice)
    c = summarize_data(dat, league_choice, team_choice, opp_choice)
    g, sog, y = top_data(dat, league_choice, team_choice, opp_choice)

    players = a['Player'].tolist() + b['Player'].tolist()
    players.sort()
    
    lineups = st.multiselect('Confirmed starters?', players)    
    confirm_only = st.checkbox("Filter for confirmed players")
        
if confirm_only:
    dat['is_confirmed'] = dat['Player'].apply(lambda x: x in lineups)
    a, b = show_data(dat, league_choice, team_choice, opp_choice)
    c = summarize_data(dat, league_choice, team_choice, opp_choice)
    g, sog, y = top_data(dat, league_choice, team_choice, opp_choice)

col1, col2, col3 = st.columns(3)

with col1:
    st.dataframe(a.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)
    st.dataframe(g.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)

with col2:
    st.dataframe(b.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)
    st.dataframe(sog.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)
    
with col3:
    st.dataframe(c.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)
    st.dataframe(y.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)