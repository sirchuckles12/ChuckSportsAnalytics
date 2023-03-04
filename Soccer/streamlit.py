import streamlit as st
import pandas as pd
import seaborn as sns
from roto import RotowireLineup, process_game
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from credentials import user, passwd

st.set_page_config(layout="wide")

cm = sns.light_palette("green", as_cmap=True)

#df2.style.background_gradient(cmap=cm)

# Title
st.title("Soccer Projections and Lineups")

def show_data(dat, league, team, opp):
    a = dat[(dat['league'] == league) & (dat['Team'] == team) & (dat['Opp'] == opp)][['Player', 'Team', 'Pos', 'G', 'A', 'SOG', 'Y', 'R', 'FC']]

    b = dat[(dat['league'] == league) & (dat['Team'] == opp) & (dat['Opp'] == team)][['Player', 'Team', 'Pos', 'G', 'A', 'SOG', 'Y', 'R', 'FC']]
    return (a, b)

def summarize_data(dat, league, team, opp):
    c = dat[(dat['league'] == league) & (((dat['Team'] == team) & (dat['Opp'] == opp)) | ((dat['Team'] == opp) & (dat['Opp'] == team)))].groupby('Team')[['G', 'A', 'SOG', 'Y', 'R', 'FC']].sum()
    return (c)

def fetch_lineups(dat, league):
    options = Options()
#    options.headless = True
    driver = webdriver.Chrome(options=options)
    
    if league == 'EPL':
        add_on = ''
    else:
        add_on = 'league={}'.format(league)
    driver.get('https://www.rotowire.com/soccer/lineups.php?{}'.format(add_on))

    games = [x for x in driver.find_elements(by = By.CLASS_NAME, value = 'lineup__box')]
    lineups = pd.concat([process_game(game) for game in games])
    return (lineups)



dat = pd.concat([pd.read_csv(x) for x in ['liga_projections_2023-02-28.csv', 'serie_a_projections_2023-02-28.csv', 'epl_projections_2023-02-28.csv', 'bund_projections_2023-02-28.csv', 'mls_projections_2023-02-28.csv']])


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
l = fetch_lineups(dat, league_choice)

col1, col2, col3 = st.columns(3)

with col1:
    st.dataframe(a.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)

with col2:
    st.dataframe(b.style.background_gradient(cmap = cm, axis = 0).format(precision=2), height = 425)
    
with col3:
    st.dataframe(c.style.background_gradient(cmap = cm, axis = 0).format(precision=2))