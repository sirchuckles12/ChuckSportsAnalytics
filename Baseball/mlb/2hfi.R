library(baseballr)
library(RPostgres)
library(dplyr)
library(tidyr)
library(data.table)

source('../Desktop/GitRepos/ChuckSportsAnalytics/Baseball/mlb/data_functions.R')
source('../Desktop/GitRepos/ChuckSportsAnalytics/Baseball/mlb/connect-database.R')

s = mlb_schedule(season = 2023, level_ids = "1")
s = s %>% filter(series_description == 'Regular Season' |
                   series_description == 'Wild Card Game' |
                   series_description == 'Division Series'|
                   series_description == 'League Championship Series' |
                   series_description == 'World Series')
s = s %>% filter(official_date >= Sys.Date()) %>% filter(official_date <= Sys.Date() + 1)
s = s %>% filter(is.na(reschedule_game_date))

probable_starters = rbindlist(lapply(unique(s$game_pk), function(x) {mlb_probables(x)}))
probable_starters = probable_starters[!is.na(fullName)]

schedule = data.table(s)


#### 2HFI / YRFI models?
team_stats = dbGetQuery(con, "(SELECT b.game_pk, s.game_date, b.home_team_name AS team_name, 'Home' AS home_away
                        , SUM(CASE WHEN num = 1 THEN home_hits ELSE 0 END) AS first_inning_hits
                        , SUM(CASE WHEN num = 1 THEN home_runs ELSE 0 END) AS first_inning_runs
                        , SUM(home_hits) AS hits, SUM(home_runs) AS runs
                        , SUM(CASE WHEN num = 1 THEN away_hits ELSE 0 END) AS first_inning_hits_given
                        , SUM(CASE WHEN num = 1 THEN away_runs ELSE 0 END) AS first_inning_runs_given
                        , SUM(away_hits) AS hits_given
                        , SUM(away_runs) AS runs_given
                        FROM mlb_box_scores b
                        INNER JOIN mlb_schedule s ON b.game_pk = s.game_pk
                        GROUP BY 1, 2, 3
                        ORDER BY 2, 4 ASC)
                        
                        UNION
                                
                        (SELECT b.game_pk, s.game_date, b.away_team_name AS team_name, 'Away' AS home_away
                        , SUM(CASE WHEN num = 1 THEN away_hits ELSE 0 END) AS first_inning_hits
                        , SUM(CASE WHEN num = 1 THEN away_runs ELSE 0 END) AS first_inning_runs
                        , SUM(away_hits) AS hits, SUM(away_runs) AS runs
                        , SUM(CASE WHEN num = 1 THEN home_hits ELSE 0 END) AS first_inning_hits_given
                        , SUM(CASE WHEN num = 1 THEN home_runs ELSE 0 END) AS first_inning_runs_given
                        , SUM(home_hits) AS hits_given
                        , SUM(home_runs) AS runs_given
                        FROM mlb_box_scores b
                        INNER JOIN mlb_schedule s ON b.game_pk = s.game_pk
                        GROUP BY 1, 2, 3
                        ORDER BY 2, 4 ASC)")

synth_one = data.table(s[, c('game_pk', 'game_date', 'teams_home_team_name'), with = FALSE])
synth_one[, home_away := 'Home']
setnames(synth_one, 'teams_home_team_name', 'team_name')
synth_two = data.table(s[, c('game_pk', 'game_date', 'teams_away_team_name'), with = FALSE])
synth_two[, home_away := 'Away']
setnames(synth_two, 'teams_away_team_name', 'team_name')
synth = rbind(synth_one, synth_two)

team_stats = data.table(team_stats)
team_stats = rbind(team_stats, synth, fill = TRUE)
team_stats = team_stats[order(game_date, home_away)]
team_stats[, c('team_fi_hits_l10', 'team_fi_runs_l10', 'team_hits_l10', 'team_runs_l10',
               'team_fi_hits_given_l10', 'team_fi_runs_given_l10', 'team_hits_given_l10', 'team_runs_given_l10') := 
             list(frollmean(shift(first_inning_hits, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE),
                  frollmean(shift(first_inning_runs, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE),
                  frollmean(shift(hits, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE),
                  frollmean(shift(runs, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE),
                  frollmean(shift(first_inning_hits_given, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE),
                  frollmean(shift(first_inning_runs_given, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE),
                  frollmean(shift(hits_given, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE),
                  frollmean(shift(runs_given, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE)
             )
           , list(team_name)]


team_stats = dcast(team_stats, game_pk + game_date ~ home_away
                   , value.var = c('team_fi_hits_l10', 'team_fi_runs_l10', 'team_hits_l10', 'team_runs_l10'
                                  , 'team_fi_hits_given_l10', 'team_fi_runs_given_l10'
                                  , 'team_hits_given_l10', 'team_runs_given_l10'), fun.aggregate = sum)


stadium_stats = dbGetQuery(con, "(SELECT b.game_pk, s.game_date, b.home_team_name AS home_team
                        , SUM(CASE WHEN num = 1 THEN home_hits + away_hits ELSE 0 END) AS first_inning_hits
                        , SUM(CASE WHEN num = 1 THEN home_runs + away_runs ELSE 0 END) AS first_inning_runs
                        , SUM(home_hits + away_hits) AS hits, SUM(home_runs + away_runs) AS runs
                        FROM mlb_box_scores b
                        INNER JOIN mlb_schedule s ON b.game_pk = s.game_pk
                        GROUP BY 1, 2, 3
                        ORDER BY 2 ASC)")

synth = synth_one
setnames(synth, 'team_name', 'home_team')

stadium_stats = data.table(stadium_stats)
stadium_stats = rbind(stadium_stats, synth_one, fill = TRUE)[order(game_date)]
stadium_stats[, c('stadium_fi_hits_l10', 'stadium_fi_runs_l10', 'stadium_hits_l10', 'stadium_runs_l10') := 
             list(frollmean(shift(first_inning_hits, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE),
                  frollmean(shift(first_inning_runs, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE),
                  frollmean(shift(hits, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE),
                  frollmean(shift(runs, n = 1, type = 'lag'), n = 10, align = 'right', na.rm = TRUE))
           , list(home_team)]
stadium_stats = stadium_stats[, c('game_pk', 'game_date', 'stadium_fi_hits_l10', 'stadium_fi_runs_l10', 'stadium_hits_l10', 'stadium_runs_l10'), with = FALSE]


pitcher_stats = dbGetQuery(con, "SELECT p.*, opp_score_end - opp_score_start AS runs_scored
FROM mlb_pitcher_statistics p
INNER JOIN mlb_starting_pitcher s ON p.game_pk = s.game_pk AND p.fielding_team = s.fielding_team AND p.pitcher = s.starting_pitcher
")

pitcher_stats = data.table(pitcher_stats)[order(game_date)]

p = probable_starters[, c('game_pk', 'game_date', 'team', 'fullName'), with = FALSE]
p = merge(p, data.table(s[, c('game_pk', 'teams_away_team_name', 'teams_home_team_name')]), by.x = c('game_pk', 'team'), by.y = c('game_pk', 'teams_away_team_name'), all.x = TRUE)
p = merge(p, data.table(s[, c('game_pk', 'teams_away_team_name', 'teams_home_team_name')]), by.x = c('game_pk', 'team'), by.y = c('game_pk', 'teams_home_team_name'), all.x = TRUE)
p[, batting_team := fcoalesce(teams_home_team_name, teams_away_team_name)]
p[, home_team := fcoalesce(teams_home_team_name, team)]
p[, is_starting_pitcher := 1]
p = p[, c('team', 'game_pk', 'game_date', 'fullName', 'batting_team', 'home_team', 'is_starting_pitcher'), with = FALSE]
p[, game_date := as.Date(game_date)]
setnames(p, c('team', 'fullName'), c('fielding_team', 'pitcher'))
p[, is_home_team := ifelse(fielding_team == home_team, 1, 0)]



pitcher_stats = rbind(pitcher_stats, p, fill = TRUE)
pitcher_stats = pitcher_stats[order(game_date)]
pitcher_stats[, c('pitcher_hits_l6', 'pitcher_fi_hits_l6', 'pitcher_runs_l6', 'pitcher_outs_l6') := 
                list(frollmean(shift(hits_recorded, n = 1, type = 'lag'), n = 6, align = 'right'),
                     frollmean(shift(fi_hits_recorded, n = 1, type = 'lag'), n = 6, align = 'right'),
                     frollmean(shift(runs_scored, n = 1, type = 'lag'), n = 6, align = 'right'),
                     frollmean(shift(outs_recorded, n = 1, type = 'lag'), n = 6, align = 'right')
                ), pitcher]
pitcher_stats[, home_away := ifelse(is_home_team, 'home', 'away')]
pitcher_stats[, pitcher_hits_per_out_l6 := pitcher_hits_l6 / pitcher_outs_l6]


pitcher_stats = dcast(pitcher_stats, game_pk ~ home_away
                      , value.var = c('pitcher_hits_l6', 'pitcher_fi_hits_l6'
                                      , 'pitcher_runs_l6', 'pitcher_outs_l6', 'pitcher_hits_per_out_l6')
                      , fun.aggregate = sum, fill = NA)




labels = dbGetQuery(con, "(SELECT b.game_pk, s.game_date, b.home_team_name AS home_team, b.away_team_name AS away_team
                        , SUM(CASE WHEN num = 1 THEN home_hits + away_hits ELSE 0 END) AS first_inning_hits
                        , SUM(CASE WHEN num = 1 THEN home_runs + away_runs ELSE 0 END) AS first_inning_runs
                        , SUM(home_hits + away_hits) AS hits, SUM(home_runs + away_runs) AS runs
                        FROM mlb_box_scores b
                        INNER JOIN mlb_schedule s ON b.game_pk = s.game_pk
                        GROUP BY 1, 2, 3, 4
                        ORDER BY 2, 3 ASC) ")

synth = s[, c('game_pk', 'game_date', 'teams_away_team_name', 'teams_home_team_name')]
setnames(synth, c('teams_away_team_name', 'teams_home_team_name'), c('away_team', 'home_team'))

labels = data.table(labels)
labels = rbind(labels, synth, fill = TRUE)
labels = labels[order(game_date)]

labels = merge(labels, team_stats, by = c('game_pk', 'game_date'), sort = FALSE, suffixes = c('_label', '_team'))
labels = merge(labels, stadium_stats, by = c('game_pk', 'game_date'), sort = FALSE, suffixes = c('', '_stadium'))
labels = merge(labels, pitcher_stats, by = c('game_pk'), sort = FALSE, suffixes = c('', '_pitcher'))

labels[, dow := weekdays(as.Date(game_date))]
labels[, month := month(as.Date(game_date))]
labels[, year := year(as.Date(game_date))]



library(glmnet)

training_data = labels[complete.cases(labels)]
inference_data = labels[is.na(first_inning_hits)]


two_hit_m = glm(first_inning_hits >= 2 ~ 
          #team_fi_hits_l10_Away + team_fi_hits_l10_Home + 
          #team_fi_runs_l10_Away + team_fi_runs_l10_Home + 
          team_hits_l10_Away + team_hits_l10_Home + 
          #team_runs_l10_Away + team_runs_l10_Home + 
          #team_fi_hits_given_l10_Away + team_fi_hits_given_l10_Home +
          #team_fi_runs_given_l10_Away + team_fi_runs_given_l10_Home + 
          team_hits_given_l10_Away + team_hits_given_l10_Home + 
          #team_runs_given_l10_Away + team_runs_given_l10_Home +
          #stadium_fi_hits_l10 + stadium_fi_runs_l10 + 
          stadium_hits_l10 + stadium_runs_l10 +
          #pitcher_hits_l6_away + pitcher_hits_l6_home + 
          #pitcher_fi_hits_l6_away + pitcher_fi_hits_l6_home +
          pitcher_hits_per_out_l6_away + pitcher_hits_per_out_l6_home +
          #pitcher_runs_l6_away + pitcher_runs_l6_home + 
          #pitcher_outs_l6_away + pitcher_outs_l6_home + 
          as.factor(month) + as.factor(year), data = training_data, family = 'binomial')

training_data[, two_hit_pred := predict(two_hit_m, newdata = .SD, type = 'response')]
inference_data[, two_hit_pred := predict(two_hit_m, newdata = .SD, type = 'response')]

training_data[, mean((two_hit_pred - I(first_inning_hits >= 2)) ^ 2, na.rm = TRUE), year(game_date)]
training_data[, mean(first_inning_hits >= 2, na.rm = TRUE), list(dow, year(game_date))]




#features = c('team_fi_hits_l10_Away', 'team_fi_hits_l10_Home'
#             , 'team_fi_runs_l10_Away', 'team_fi_runs_l10_Home'
#             , 'team_hits_l10_Away', 'team_hits_l10_Home'
#             , 'team_runs_l10_Away', 'team_runs_l10_Home'
#             , 'team_fi_hits_given_l10_Away', 'team_fi_hits_given_l10_Home'
#             , 'team_fi_runs_given_l10_Away', 'team_fi_runs_given_l10_Home'
#             , 'team_hits_given_l10_Away', 'team_hits_given_l10_Home' 
#             , 'team_runs_given_l10_Away', 'team_runs_given_l10_Home'
#             , 'stadium_fi_hits_l10', 'stadium_fi_runs_l10'
#             , 'stadium_hits_l10', 'stadium_runs_l10'
#             , 'pitcher_hits_l6_away', 'pitcher_hits_l6_home'
#             , 'pitcher_fi_hits_l6_away', 'pitcher_fi_hits_l6_home'
#             , 'pitcher_runs_l6_away', 'pitcher_runs_l6_home'
#             , 'pitcher_outs_l6_away', 'pitcher_outs_l6_home'
#             , 'pitcher_hits_per_out_l6_away', 'pitcher_hits_per_out_l6_home' 
#             , 'month', 'year')

#two_hit_m = cv.glmnet(x = as.matrix(training_data[, features, with = FALSE])
#                   , y = training_data$first_inning_hits >= 2, data = labels)


#summary(two_hit_m)
#coef(two_hit_m, s = 'lambda.1se')
#coef(two_hit_m, s = 'lambda.min')

#training_data[, two_hit_pred_cv := predict(two_hit_m, type = 'response', s = 'lambda.min', newx = as.matrix(.SD[, features, with = FALSE]))]
#inference_data[, two_hit_pred_cv := predict(two_hit_m, type = 'response', s = 'lambda.min', newx = as.matrix(.SD[, features, with = FALSE]))]



run_m = glm(first_inning_runs >= 1 ~ team_fi_hits_l10_Away + team_fi_hits_l10_Home + 
                  team_fi_runs_l10_Away + team_fi_runs_l10_Home + 
                  team_hits_l10_Away + team_hits_l10_Home + 
                  #team_runs_l10_Away + team_runs_l10_Home + 
                  #team_fi_hits_given_l10_Away + team_fi_hits_given_l10_Home +
                  #team_fi_runs_given_l10_Away + team_fi_runs_given_l10_Home + 
                  #team_hits_given_l10_Away + team_hits_given_l10_Home + 
                  #team_runs_given_l10_Away + team_runs_given_l10_Home +
                  #stadium_fi_hits_l10 + stadium_fi_runs_l10 + 
                  stadium_hits_l10 + stadium_runs_l10 +
                  #pitcher_hits_l6_away + pitcher_hits_l6_home + 
                  pitcher_fi_hits_l6_away + pitcher_fi_hits_l6_home +
                  #pitcher_runs_l6_away + pitcher_runs_l6_home + 
                  pitcher_outs_l6_away + pitcher_outs_l6_home, data = training_data, family = 'binomial')

summary(run_m)

training_data[, run_pred := predict(run_m, newdata = .SD, type = 'response')]
inference_data[, run_pred := predict(run_m, newdata = .SD, type = 'response')]



training_data[, mean((two_hit_pred - I(first_inning_hits >= 2)) ^ 2, na.rm = TRUE), year(game_date)]



inference_data


inference_data[, c('game_pk', 'game_date',  'home_team', 'first_inning_hits', 'first_inning_runs', 'two_hit_pred', 'run_pred'), with = FALSE]


#write.csv(file = 'test_20230506.csv', x = labels[is.na(first_inning_hits)])
#write.csv(file = 'starters_20230506.csv', x = probable_starters)

tmp = fread('test.csv')

