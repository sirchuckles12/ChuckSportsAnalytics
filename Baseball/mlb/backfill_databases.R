library(baseballr)
library(RPostgres)
library(dplyr)
library(tidyr)
library(data.table)

source('../Desktop/GitRepos/ChuckSportsAnalytics/Baseball/mlb/data_functions.R')
source('../Desktop/GitRepos/ChuckSportsAnalytics/Baseball/mlb/connect-database.R')



#Backfilling, so remove old tables
#try(dbRemoveTable(con, 'mlb_schedule'))
#try(dbRemoveTable(con, 'mlb_pitch_level'))
#try(dbRemoveTable(con, 'mlb_box_scores'))
#try(dbRemoveTable(con, 'mlb_play_by_play'))
#try(dbRemoveTable(con, 'mlb_game_details'))

for(season in seq(2018, 2022, 1)) {
  print(season) 
  print(Sys.time())
  s = mlb_schedule(season = season, level_ids = "1")
  s = s %>% filter(series_description == 'Regular Season' |
                 series_description == 'Wild Card Game' |
                 series_description == 'Division Series'|
                 series_description == 'League Championship Series' |
                 series_description == 'World Series')
  s = s %>% filter(status_coded_game_state == 'F')
  s = data.table(s)
  dbWriteTable(con, 'mlb_schedule', value = s, append = TRUE)
  
  game_pks = dbGetQuery(con, paste0("SELECT DISTINCT game_pk FROM mlb_schedule WHERE season = '", season, "\';"))
  game_pks = data.table(game_pks)
  game_pks = game_pks$game_pk
  
  payloads = get_payloads(game_pks)
  write_data(payloads)
  
#  write_pitch_by_pitch(unique(game_pks))
#  print(Sys.time())
#  write_box_scores(unique(game_pks))
#  print(Sys.time())
#  write_game_details(unique(game_pks))
#  print(Sys.time())
}

source('create_convenience_tables.R')
