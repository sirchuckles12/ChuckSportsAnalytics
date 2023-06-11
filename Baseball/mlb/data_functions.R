library(parallel)
library(doParallel)
library(pbapply)
library(future.apply)

plan(multisession)

get_single_payload <- function(game_pk) {
  mlb_endpoint <- baseballr:::mlb_stats_endpoint(glue::glue("v1.1/game/{game_pk}/feed/live"))
  payload <- mlb_endpoint %>% baseballr:::mlb_api_call() %>% jsonlite::toJSON() %>% 
    jsonlite::fromJSON(flatten = TRUE)
  
  return(payload)
}

get_payloads <- function(games) {
  print(length(games))
  payloads = future_lapply(games,FUN=get_single_payload)
  return (payloads)
}


payload_to_pbp <- function(payload) {
  tryCatch(expr = {
  plays <- payload$liveData$plays$allPlays$playEvents %>% 
    dplyr::bind_rows()
  at_bats <- payload$liveData$plays$allPlays
  current <- payload$liveData$plays$currentPlay
  game_status <- payload$gameData$status$abstractGameState
  home_team <- payload$gameData$teams$home$name
  home_level <- payload$gameData$teams$home$sport
  home_league <- payload$gameData$teams$home$league
  away_team <- payload$gameData$teams$away$name
  away_level <- payload$gameData$teams$away$sport
  away_league <- payload$gameData$teams$away$league
  columns <- lapply(at_bats, function(x) class(x)) %>% 
    dplyr::bind_rows(.id = "variable")
  cols <- c(colnames(columns))
  classes <- c(t(unname(columns[1, ])))
  df <- data.frame(cols, classes)
  list_columns <- df %>% dplyr::filter(.data$classes == 
                                         "list") %>% dplyr::pull(.data$cols)
  if (length(list_columns) > 0) {
    at_bats <- at_bats %>% dplyr::select(-c(tidyr::one_of(list_columns)))
  }
  pbp <- plays %>% dplyr::left_join(at_bats, by = c(endTime = "playEndTime"))
  pbp <- pbp %>% tidyr::fill(.data$atBatIndex:.data$matchup.splits.menOnBase, 
                             .direction = "up") %>% dplyr::mutate(game_pk = payload$gamePk, 
                                                                  game_date = substr(payload$gameData$datetime$dateTime, 
                                                                                     1, 10)) %>% dplyr::select(.data$game_pk, .data$game_date, 
                                                                                                               tidyr::everything())
  pbp <- pbp %>% dplyr::mutate(matchup.batter.fullName = factor(.data$matchup.batter.fullName), 
                               matchup.pitcher.fullName = factor(.data$matchup.pitcher.fullName), 
                               atBatIndex = factor(.data$atBatIndex)) %>% dplyr::mutate(home_team = home_team, 
                                                                                        home_level_id = home_level$id, home_level_name = home_level$name, 
                                                                                        home_parentOrg_id = payload$gameData$teams$home$parentOrgId, 
                                                                                        home_parentOrg_name = payload$gameData$teams$home$parentOrgName, 
                                                                                        home_league_id = home_league$id, home_league_name = home_league$name, 
                                                                                        away_team = away_team, away_level_id = away_level$id, 
                                                                                        away_level_name = away_level$name, away_parentOrg_id = payload$gameData$teams$away$parentOrgId, 
                                                                                        away_parentOrg_name = payload$gameData$teams$away$parentOrgName, 
                                                                                        away_league_id = away_league$id, away_league_name = away_league$name, 
                                                                                        batting_team = factor(ifelse(.data$about.halfInning == 
                                                                                                                       "bottom", .data$home_team, .data$away_team)), 
                                                                                        fielding_team = factor(ifelse(.data$about.halfInning == 
                                                                                                                        "bottom", .data$away_team, .data$home_team)))
  pbp <- pbp %>% dplyr::arrange(desc(.data$atBatIndex), 
                                desc(.data$pitchNumber))
  pbp <- pbp %>% dplyr::group_by(.data$atBatIndex) %>% 
    dplyr::mutate(last.pitch.of.ab = ifelse(.data$pitchNumber == 
                                              max(.data$pitchNumber), "true", "false"), last.pitch.of.ab = factor(.data$last.pitch.of.ab)) %>% 
    dplyr::ungroup()
  pbp <- dplyr::bind_rows(baseballr::stats_api_live_empty_df, 
                          pbp)
  check_home_level <- pbp %>% dplyr::distinct(.data$home_level_id) %>% 
    dplyr::pull()
  pbp <- pbp %>% dplyr::rename(count.balls.start = .data$count.balls.x, 
                               count.strikes.start = .data$count.strikes.x, count.outs.start = .data$count.outs.x, 
                               count.balls.end = .data$count.balls.y, count.strikes.end = .data$count.strikes.y, 
                               count.outs.end = .data$count.outs.y) #%>% make_baseballr_data("MLB Play-by-Play data from MLB.com", 
                                                                    #                        Sys.time())
  pbp = data.frame(pbp)
  names(pbp) = gsub(pattern = '.', replacement = '_', x = names(pbp), fixed = TRUE)
  return(pbp)
  }, error = function(e) {})
}

payload_to_game_details <- function(payload) {
  tryCatch(expr = {lookup_table <- payload$liveData$boxscore$info %>% as.data.frame() %>% 
    tidyr::spread(.data$label, .data$value)
  year <- stringr::str_sub(payload$gameData$game$calendarEventID, 
                           -10, -7)
  game_table <- tibble(game_date = stringr::str_sub(payload$gameData$game$calendarEventID, 
                                                    -10, -1), game_pk = payload$gamePk, venue_name = payload$gameData$venue$name, 
                       venue_id = payload$gameData$venue$id, temperature = payload$gameData$weather$temp, 
                       other_weather = payload$gameData$weather$condition, 
                       wind = payload$gameData$weather$wind, attendance = ifelse("Att" %in% 
                                                                                   names(lookup_table) == TRUE, as.character(lookup_table$Att) %>% 
                                                                                   stringr::str_remove_all("\\."), NA), start_time = as.character(lookup_table$`First pitch`) %>% 
                         stringr::str_remove_all("\\."), elapsed_time = as.character(lookup_table$T) %>% 
                         stringr::str_remove_all("\\."), game_id = payload$gameData$game$id, 
                       game_type = payload$gameData$game$type, home_sport_code = "mlb", 
                       official_scorer = payload$gameData$officialScorer$fullName, 
                       date = names(lookup_table)[1], status_ind = payload$gameData$status$statusCode, 
                       home_league_id = payload$gameData$teams$home$league$id, 
                       gameday_sw = payload$gameData$game$gamedayType) #%>% 
#    make_baseballr_data("MLB Game Info data from MLB.com", 
#                        Sys.time())
  game_table = data.frame(game_table)
  names(game_table) = gsub(pattern = '.', replacement = '_', x = names(game_table), fixed = TRUE)
  return(game_table)
  }, error = function(e) {})
}

payload_to_game_linescore <- function(payload) {
  tryCatch(expr = {
    home_team <- unlist(payload$gameData$teams$home) %>% 
    dplyr::bind_rows() %>% janitor::clean_names() %>% 
    dplyr::select(-starts_with("spring_venue"), -starts_with("spring_league"))
  away_team <- unlist(payload$gameData$teams$away) %>% 
    dplyr::bind_rows() %>% janitor::clean_names() %>% 
    dplyr::select(-starts_with("spring_venue"), -starts_with("spring_league"))
  colnames(home_team) <- paste0("home_team_", colnames(home_team))
  colnames(away_team) <- paste0("away_team_", colnames(away_team))
  teams <- home_team %>% dplyr::bind_cols(away_team)
  linescore <- payload$liveData$linescore$innings
  linescore_table <- linescore %>% dplyr::bind_cols(teams) %>% 
    janitor::clean_names() %>% dplyr::mutate(game_pk = payload$gamePk) %>% 
    dplyr::select(.data$game_pk, .data$home_team_id, 
                  .data$home_team_name, .data$away_team_id, .data$away_team_name, 
                  tidyr::everything()) #%>% make_baseballr_data("MLB Game Linescore data from MLB.com", 
                                      #                         Sys.time())
  linescore_table = data.frame(linescore_table)
  names(linescore_table) = gsub(pattern = '.', replacement = '_', x = names(linescore_table), fixed = TRUE)
  return(linescore_table)
  }, error = function(e) {})
}


write_data <- function(payloads) {
  #Pitch by pitch
  print('Writing PBP')
  pbp_logs = lapply(payloads, FUN = payload_to_pbp)
  full = data.table(dplyr::bind_rows(pbp_logs))
  
  full[, last_index_of_pa := max(index) == index, list(game_pk, atBatIndex)]
  full[, is_hit := result_event %in% c('Single', 'Double', 'Home Run', 'Triple')]
  full[, reviewDetails_additionalReviews := NULL]
  full[, details_violation_type := NULL]
  full[, details_violation_description := NULL]
  full[, details_violation_player_id := NULL]
  full[, details_violation_player_fullName := NULL]
  
  dbWriteTable(con, 'mlb_pitch_level', value = full, append = TRUE)
  print(paste0('Wrote ', full[, .N]))
  rm(pbp_logs)
  rm(full)
  
  print('Writing game details')
  #Game details
  details_logs = lapply(payloads, FUN = payload_to_game_details)
  full = data.table(dplyr::bind_rows(details_logs))
  
  dbWriteTable(con, 'mlb_game_details', value = full, append = TRUE)
  
  rm(details_logs)
  rm(full)
  
  #Boxscore
  print('Writing boxscores')
  boxscore_logs = lapply(payloads, FUN = payload_to_game_linescore)
  full = data.table(dplyr::bind_rows(boxscore_logs))
  
  dbWriteTable(con, 'mlb_box_scores', value = full, append = TRUE)
  
  rm(boxscore_logs)
  rm(full)
  rm(payloads)
}

#write_data(games = game_pks[1:100])














# write_pitch_by_pitch <- function(payloads) {
# 
#   game_logs = lapply(payloads,FUN=payload_to_pbp)
#   print('done')
# 
# }
# 
# write_box_scores <- function(games) {
#   box_scores = parLapply(cl,games,fun=process_box_scores)
# #  box_scores = list()
# #  
# #  for(i in seq_along(games)) {
# #    box_scores[[i]] = process_box_scores(games[i])
# #    
# #    print(i / length(games))
# #  }
#   
#   full = data.table(dplyr::bind_rows(box_scores))
#   
#   dbWriteTable(con, 'mlb_box_scores', value = full, append = TRUE)
#   
#   rm(box_scores)
#   rm(full)
# }
# 
# write_game_details <- function(games) {
# 
#   game_details = parLapply(cl,games,fun=process_game_details)
#   
# #  for(i in seq_along(games)) {
# #    game_details[[i]] = process_game_details(games[i])
# #    
# #    print(i / length(games))
# #  }
#   
#   full = data.table(dplyr::bind_rows(game_details))
#   
#   dbWriteTable(con, 'mlb_game_details', value = full, append = TRUE)
#   
#   rm(game_details)
#   rm(full)
# }
# 
# no_cores <- detectCores(logical = TRUE)
# 
# cl <- makeCluster(no_cores-1)  
# registerDoParallel(cl)  
# 
# 
# mlb_pbp <- baseballr::mlb_pbp
# mlb_game_info <- baseballr::mlb_game_info
# mlb_game_linescore <- baseballr::mlb_game_linescore
# 
# clusterExport(cl, list('mlb_pbp', 'mlb_game_info', 'mlb_game_linescore'))
# 
# 
# 
# 
# mlb_pbp <- function (game_pk) 
# {
#   mlb_endpoint <- mlb_stats_endpoint(glue::glue("v1.1/game/{game_pk}/feed/live"))
#   tryCatch(expr = {
#     payload <- mlb_endpoint %>% mlb_api_call() %>% jsonlite::toJSON() %>% 
#       jsonlite::fromJSON(flatten = TRUE)
#   }, error = function(e) {
#     message(glue::glue("{Sys.time()}: Invalid arguments provided"))
#   }, warning = function(w) {
#   }, finally = {
#   })
# }
# 
# mlb_game_linescore <- function (game_pk) 
# {
#   mlb_endpoint <- mlb_stats_endpoint(glue::glue("v1.1/game/{game_pk}/feed/live"))
#   tryCatch(expr = {
#     payload <- mlb_endpoint %>% mlb_api_call() %>% jsonlite::toJSON() %>% 
#       jsonlite::fromJSON(flatten = TRUE)
#   }, error = function(e) {
#     message(glue::glue("{Sys.time()}: Invalid arguments provided"))
#   }, warning = function(w) {
#   }, finally = {
#   })
# }
# 
# mlb_game_info <- function (game_pk) 
# {
#   api_call <- paste0("http://statsapi.mlb.com/api/v1.1/game/", 
#                      game_pk, "/feed/live")
#   tryCatch(expr = {
#     payload <- jsonlite::fromJSON(api_call)
# 
#     
#   }, error = function(e) {
#     message(glue::glue("{Sys.time()}: Invalid arguments provided"))
#   }, warning = function(w) {
#   }, finally = {
#   })
# }

# process_pbp <- function(game_pk) {
#   dat = data.frame(mlb_pbp(game_pk = game_pk))
#   names(dat) = gsub(pattern = '.', replacement = '_', x = names(dat), fixed = TRUE)
#   return(dat)
# }
# 
# process_box_scores <- function(game_pk) {
#   dat = data.frame(mlb_game_linescore(game_pk = game_pk))
#   names(dat) = gsub(pattern = '.', replacement = '_', x = names(dat), fixed = TRUE)
#   return(dat)
# }
# 
# process_game_details <- function(game_pk) {
#   dat = data.frame(mlb_game_info(game_pk))
#   names(dat) = gsub(pattern = '.', replacement = '_', x = names(dat), fixed = TRUE)
#   return(dat)
# }
