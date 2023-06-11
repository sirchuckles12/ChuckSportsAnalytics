try(dbRemoveTable(con, 'mlb_play_by_play'))
dbExecute(con, "
          CREATE TABLE mlb_play_by_play AS (
          SELECT game_pk, game_date::DATE, \"startTime\"::TIMESTAMP AS start_time, home_team
          , \"matchup_batter_fullName\" AS batter, batting_team
          , \"matchup_pitcher_fullName\" AS pitcher, fielding_team
          , about_inning AS inning, \"about_isTopInning\" AS is_top_inning
          , count_outs_start, count_outs_end, count_outs_end - count_outs_start AS outs_recorded
          , \"pitchNumber\" AS pitches_thrown
          , count_strikes_start AS count_strikes_start
          , COALESCE(LAG(\"result_awayScore\") OVER (PARTITION BY game_pk ORDER BY \"about_atBatIndex\"), 0) AS away_score_start
          , COALESCE(LAG(\"result_homeScore\") OVER (PARTITION BY game_pk ORDER BY \"about_atBatIndex\"), 0) AS home_score_start
          , \"result_awayScore\" AS away_score_end, \"result_homeScore\" AS home_score_end
          , CASE WHEN result_event IN ('Single', 'Double', 'Home Run', 'Triple') THEN 1 ELSE 0 END AS hit_recorded
          , CASE WHEN result_event IN ('Walk', 'Intent Walk') THEN 1 ELSE 0 END AS walk_recorded
          , CASE WHEN result_event = 'Strikeout' THEN 1 ELSE 0 END AS strikeout_recorded
          , CASE WHEN \"matchup_postOnFirst_fullName\" IS NOT NULL THEN 1 ELSE 0 END AS first_base_runner
          , CASE WHEN \"matchup_postOnSecond_fullName\" IS NOT NULL THEN 1 ELSE 0 END AS second_base_runner
          , CASE WHEN \"matchup_postOnThird_fullName\" IS NOT NULL THEN 1 ELSE 0 END AS third_base_runner
          , \"about_captivatingIndex\" AS captivating_index 
          
          FROM mlb_pitch_level WHERE last_index_of_pa = TRUE 
          );")


try(dbRemoveTable(con, 'mlb_game_statistics'))

dbExecute(con, "CREATE TABLE mlb_game_statistics AS (
          SELECT game_pk, game_date
          , home_team, batting_team, fielding_team
          , SUM(pitches_thrown) AS pitches_thrown
          , SUM(hit_recorded) AS hits_recorded
          , SUM(walk_recorded) AS walks_recorded
          , SUM(1) AS abs_recorded
          , SUM(strikeout_recorded) AS strikeouts_recorded
          , AVG(count_strikes_start) AS avg_strikes
          , SUM(CASE WHEN home_team = batting_team THEN home_score_end - home_score_start ELSE away_score_end - away_score_start END) AS runs_scored

          , SUM(CASE WHEN inning <= 5 THEN pitches_thrown ELSE 0 END) AS fh_pitches_thrown
          , SUM(CASE WHEN inning <= 5 THEN hit_recorded ELSE 0 END ) AS fh_hits_recorded
          , SUM(CASE WHEN inning <= 5 THEN walk_recorded ELSE 0 END) AS fh_walks_recorded
          , SUM(CASE WHEN inning <= 5 THEN 1 ELSE 0 END) AS fh_abs_recorded
          , SUM(CASE WHEN inning <= 5 THEN strikeout_recorded ELSE 0 END) AS fh_strikeouts_recorded
          , AVG(CASE WHEN inning <= 5 THEN count_strikes_start ELSE 0 END) AS fh_avg_strikes
          , SUM(CASE WHEN inning >= 6 THEN 0
                     WHEN home_team = batting_team THEN home_score_end - home_score_start ELSE away_score_end - away_score_start END) AS fh_runs_scored

          
          FROM mlb_play_by_play
          
          GROUP BY 1, 2, 3, 4, 5
          ORDER BY 2, 1, 3, 4, 5
);")


try(dbRemoveTable(con, 'mlb_pitcher_statistics'))

dbExecute(con, "CREATE TABLE mlb_pitcher_statistics AS (
          SELECT game_pk, game_date
          , fielding_team, batting_team, pitcher
          , CASE WHEN fielding_team = home_team THEN 1 ELSE 0 END AS is_home_team
          
          , MIN(CASE WHEN fielding_team = home_team THEN away_score_start
                     ELSE home_score_start END) AS opp_score_start
          , MIN(CASE WHEN fielding_team = home_team THEN home_score_start
                     ELSE away_score_start END) AS score_start

          , MAX(CASE WHEN fielding_team = home_team THEN away_score_start
                     ELSE home_score_start END) AS opp_score_end
          , MAX(CASE WHEN fielding_team = home_team THEN home_score_start
                     ELSE away_score_start END) AS score_end

          , SUM(pitches_thrown) AS pitches_thrown
          , SUM(hit_recorded) AS hits_recorded
          , SUM(CASE WHEN inning = 1 THEN hit_recorded ELSE 0 END) AS fi_hits_recorded
          , SUM(walk_recorded) AS walks_recorded
          , SUM(1) AS abs_recorded
          , SUM(strikeout_recorded) AS strikeouts_recorded
          , SUM(outs_recorded) AS outs_recorded
          , AVG(count_strikes_start) AS avg_strikes
          
          FROM mlb_play_by_play
          
          GROUP BY 1, 2, 3, 4, 5, 6
          ORDER BY 2, 1, 3, 4, 5, 6);")


try(dbRemoveTable(con, 'mlb_starting_pitcher'))

dbExecute(con, "CREATE TABLE mlb_starting_pitcher AS (
SELECT DISTINCT game_pk, game_date, fielding_team
, FIRST_VALUE(pitcher) OVER (PARTITION BY game_pk, game_date, fielding_team ORDER BY start_time ASC) AS starting_pitcher

FROM mlb_play_by_play)
;")

