library(baseballr)
library(RPostgres)
library(dplyr)
library(tidyr)
library(data.table)

source('../Desktop/GitRepos/ChuckSportsAnalytics/Baseball/mlb/data_functions.R')
source('../Desktop/GitRepos/ChuckSportsAnalytics/Baseball/mlb/connect-database.R')


games_added = 0

for(season in seq(2023, 2023, 1)) {
  print(season)
  s = mlb_schedule(season = season, level_ids = "1")
  s = s %>% filter(series_description == 'Regular Season' |
                     series_description == 'Wild Card Game' |
                     series_description == 'Division Series'|
                     series_description == 'League Championship Series' |
                     series_description == 'World Series')
  s = s %>% filter(status_coded_game_state == 'F')
  
  known_pks = dbGetQuery(con, paste0("SELECT DISTINCT game_pk FROM mlb_schedule WHERE season = '", season, "\';"))
  known_pks = data.table(known_pks)$game_pk

  all_games = s$game_pk
  missing_games = all_games[!(all_games %in% known_pks)]
  
  if (length(missing_games) > 0) {
    payloads = get_payloads(missing_games)
    write_data(payloads)

    new_schedule = data.table(s)
    new_schedule = new_schedule[game_pk %in% missing_games]
    dbWriteTable(con, 'mlb_schedule', value = new_schedule, append = TRUE)
    
    games_added = games_added + length(missing_games)
  }
}

source('../Desktop/GitRepos/ChuckSportsAnalytics/Baseball/mlb/create_convenience_tables.R')


print(paste0('Games added: ', games_added))