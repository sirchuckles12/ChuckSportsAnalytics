import pandas as pd

def process_player_boxscore(b):
    
    person = b['person']
    position = b['position']
    is_goalie = position['name'] == "Goalie"
    skater_stats = b['stats'].get('skaterStats', {})
    goalie_stats = b['stats'].get('goalieStats', {})
    
    return(pd.DataFrame({"player_id": person['id']
                         , "player_name": person['fullName']
                         , "position": position['name']
                         , "is_goalie": position['name'] == 'Goalie'
                         , "timeOnIce": skater_stats.get('timeOnIce') or goalie_stats.get('timeOnIce')
                         , "assists": skater_stats.get('assists', 0)
                         , "goals": skater_stats.get('goals', 0 )
                         , "shots": skater_stats.get('shots', 0)
                         , "shot_attempts": skater_stats.get('shots', 0) + skater_stats.get('blocked', 0)
                         , "plus_minus": skater_stats.get('plusMinus', 0)
                         , "shots_against": goalie_stats.get('shots')
                         , "saves": goalie_stats.get('saves')
                         , 'save_percentage': goalie_stats.get('savePercentage')
                         , 'power_play_save_percentage': goalie_stats.get('powerPlaySavePercentage')
                         , 'short_handed_save_percentage': goalie_stats.get('shortHandedSavePercentage')
                         , 'even_strength_save_percentage': goalie_stats.get('evenStrengthSavePercentage')
                        }, index = [1]))

def process_team_boxscore(b):
    
    away = b['away']
    home = b['home']
    
    return(pd.DataFrame({"away_team": away['team']['name']
                         , "away_goals": away['teamStats']['teamSkaterStats']['goals']
                         , "away_shots": away['teamStats']['teamSkaterStats']['shots']
                         , "away_power_play_goals": away['teamStats']['teamSkaterStats']['powerPlayGoals']
                         , "away_power_play_opportunities": away['teamStats']['teamSkaterStats']['powerPlayOpportunities']
                         , "away_face_off_win_percentage": away['teamStats']['teamSkaterStats']['faceOffWinPercentage']
                         , "away_takeaways": away['teamStats']['teamSkaterStats']['takeaways']
                         , "away_giveaways": away['teamStats']['teamSkaterStats']['giveaways']
                         
                         , "home_team": home['team']['name']
                         , "home_goals": home['teamStats']['teamSkaterStats']['goals']
                         , "home_shots": home['teamStats']['teamSkaterStats']['shots']
                         , "home_power_play_goals": home['teamStats']['teamSkaterStats']['powerPlayGoals']
                         , "home_power_play_opportunities": home['teamStats']['teamSkaterStats']['powerPlayOpportunities']
                         , "home_face_off_win_percentage": home['teamStats']['teamSkaterStats']['faceOffWinPercentage']
                         , "home_takeaways": home['teamStats']['teamSkaterStats']['takeaways']
                         , "home_giveaways": home['teamStats']['teamSkaterStats']['giveaways']
                        }, index = [1]))