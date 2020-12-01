library(tidyverse)
library(nflfastR)
library(furrr)
library(gt)
library(glue)
library(here)

schedule_2020 <- 
  nflfastR::fast_scraper_schedules(2020)

save(schedule_2020, file = "C:\\Users\\tom\\Google Drive\\R\\BricBets\\schedule_2020.rds")

rosters_2020 <- 
  nflfastR::fast_scraper_roster(2020) %>% 
  mutate(team = if_else(team == "LAR", "LA", team)
  )

save(rosters_2020, file = here("rosters_2020.rds"))
save(rosters_2020, file = "C:\\Users\\tom\\Google Drive\\R\\BricBets\\rosters_2020.rds")

nfl_pbp_2020 <-
  schedule_2020 %>%
  filter(as.POSIXct(paste(gameday, gametime), format = "%Y-%m-%d %H:%M") < Sys.time()) %>%
  pull(game_id) %>%
  nflfastR::build_nflfastR_pbp(pp = TRUE)

nfl_stats_2020 <- 
  nfl_pbp_2020 %>%
  ### For Stat Tracking, the end runner of the lateral gets the receiving yards.
  mutate(
    receiver_player_name = 
      case_when(lateral_reception == 1 ~lateral_receiver_player_name,
                TRUE ~receiver_player_name
      ),
    receiver_player_id = 
      case_when(lateral_reception == 1 ~lateral_receiver_player_name,
                TRUE ~receiver_player_id
      ),
  ) %>% 
  select(
    season,
    play_id,
    game_date,
    game_id,
    yardline_100,
    posteam,
    play_type,
    play_type_nfl,
    passer,
    passer_player_id,
    passer_player_name,
    rusher,
    rusher_player_id,
    rusher_player_name,
    receiver,
    receiver_player_id,
    receiver_player_name,
    punt_returner_player_name,
    punt_returner_player_id,
    kickoff_returner_player_name,
    kickoff_returner_player_id,
    pass,
    pass_attempt,
    pass_interception = interception,
    incomplete_pass,
    complete_pass,
    rush_attempt,
    sack,
    rush,
    yards_gained,
    desc,
    td_team,
    touchdown,
    pass_touchdown,
    rush_touchdown,
    return_touchdown,
    two_point_attempt,
    two_point_conv_result,
    yards_after_catch,
    air_yards,
    extra_point_attempt,
    extra_point_result,
    field_goal_attempt,
    field_goal_result,
    kicker_player_id,
    kicker_player_name,
    ep,
    epa
  ) %>% 
  left_join(schedule_2020) %>% 
  # remove pass attempts from Sacks
  mutate(
    pass = case_when(play_type_nfl == "SACK" ~ as.double(0),
                     TRUE ~ pass),
    # code kneels as runs
    play_type = case_when(play_type == "qb_kneel" ~ "run",
                          TRUE ~ play_type),
    # ensure that kneels are counted as rush attempts
    rush = case_when(play_type == "qb_kneel" ~ as.double(1),
                     TRUE ~ rush),
    # remove NAs in numerics and replace with 0s
    across(where(is.numeric), .fns = ~ replace_na(., 0))
  ) %>%
  pivot_longer(
    cols = c("rusher_player_id", "passer_player_id", "receiver_player_id", "kicker_player_id"),
    names_to = "player_type",
    values_to = "player_id"
  ) %>%
  mutate(
    player_type = str_remove(
      player_type,
      "_player_id"
    )
  ) %>%
  filter(
    !is.na(player_id)
  ) %>%
  # not accounting for returns as of now
  mutate(
    player_name =
      case_when(
        play_type %in% c("pass", "qb_spike") & player_type == "passer" ~ passer_player_name,
        play_type == "pass" & player_type == "receiver" ~ receiver_player_name,
        play_type == "run" & player_type == "rusher" ~ rusher_player_name,
        play_type %in% c("field_goal", "extra_point") & player_type == "kicker" ~ kicker_player_name
      ),
    rusher_attempt = case_when(
      player_type == "rusher" ~ 1,
      TRUE ~ 0
    ),
    rusher_yards_gained = case_when(
      player_type == "rusher" ~ yards_gained,
      TRUE ~ 0
    ),
    rusher_touchdown = case_when(
      player_type == "rusher" ~ touchdown,
      TRUE ~ 0
    ),
    rusher_two_pt_success = case_when(
      player_type == "rusher" & two_point_conv_result == 1 ~ 1,
      TRUE ~ 0
    ),
    rusher_two_pt_att = case_when(
      player_type == "rusher" & two_point_attempt == 1 ~ 1,
      TRUE ~ 0
    ),
    receiver_reception = case_when(
      player_type == "receiver" & complete_pass == 1 ~ 1,
      TRUE ~ 0
    ),
    receiver_yards_gained = case_when(
      player_type == "receiver" ~ yards_gained,
      TRUE ~ 0
    ),
    receiver_incomplete_target = case_when(
      player_type == "receiver" & complete_pass == 0 ~ 1,
      TRUE ~ 0
    ),
    receiver_target = case_when(
      player_type == "receiver" ~ 1,
      TRUE ~ 0
    ),
    receiver_touchdown = case_when(
      player_type == "receiver" ~ touchdown,
      TRUE ~ 0
    ),
    receiver_air_yards = case_when(
      player_type == "receiver" ~ air_yards,
      TRUE ~ 0
    ),
    receiver_yac = case_when(
      player_type == "receiver" ~ yards_after_catch,
      TRUE ~ 0
    ),
    receiver_two_pt_success = case_when(
      player_type == "receiver" & two_point_conv_result == 1 ~ 1,
      TRUE ~ 0
    ),
    receiver_two_pt_att = case_when(
      player_type == "receiver" & two_point_attempt == 1 ~ 1,
      TRUE ~ 0
    ),
    pass_complete = case_when(
      player_type == "passer" & complete_pass == 1 ~ 1,
      TRUE ~ 0
    ),
    pass_incomplete = case_when(
      player_type == "passer" & complete_pass == 0 ~ 1,
      TRUE ~ 0
    ),
    pass_attempt = case_when(
      player_type == "passer" ~ 1,
      TRUE ~ 0
    ),
    pass_air_yards = case_when(
      player_type == "passer" ~ air_yards,
      TRUE ~ 0
    ),
    pass_touchdown = case_when(
      player_type == "passer" ~ touchdown,
      TRUE ~ 0
    ),
    pass_yac = case_when(
      player_type == "passer" ~ yards_after_catch,
      TRUE ~ 0
    ),
    pass_yards_gained = case_when(
      player_type == "passer" ~ yards_gained,
      TRUE ~ 0
    ),
    pass_two_pt_success = case_when(
      player_type == "passer" & two_point_conv_result == 1 ~ 1,
      TRUE ~ 0
    ),
    pass_two_pt_att = case_when(
      player_type == "passer" & two_point_attempt == 1 ~ 1,
      TRUE ~ 0
    ),
    pass_sack_yards = case_when(
      player_type == "passer" & sack == 1 ~ yards_gained,
      TRUE ~ 0
    )
  ) %>%
  select(
    season,
    week,
    play_id,
    game_date,
    game_id,
    team = posteam,
    play_type,
    player_type,
    player_name,
    player_id,
    pass,
    rush,
    rusher_attempt,
    rusher_touchdown,
    rusher_yards_gained,
    rusher_two_pt_att,
    rusher_two_pt_success,
    pass_attempt,
    pass_complete,
    pass_incomplete,
    pass_air_yards,
    pass_touchdown,
    pass_yac,
    pass_yards_gained,
    pass_two_pt_success,
    pass_two_pt_att,
    pass_interception,
    pass_sack_yards,
    pass_sack = sack,
    receiver_reception,
    receiver_incomplete_target,
    receiver_target,
    receiver_touchdown,
    receiver_air_yards,
    receiver_yards_gained,
    receiver_yac,
    receiver_two_pt_success,
    receiver_two_pt_att,
    yards_gained,
    desc,
    touchdown,
    two_point_attempt,
    two_point_conv_result,
    yardline_100,
    extra_point_attempt,
    extra_point_result,
    field_goal_attempt,
    field_goal_result,
    ep,
    epa
  ) %>%
  group_by(
    season,
    week,
    game_id,
    game_date,
    team,
    player_id,
    player_name
  ) %>%
  summarize(
    xp_atts = sum(extra_point_attempt, 0),
    xp_made = sum(if_else(!is.na(extra_point_result) & extra_point_result == "good", 1, 0)),
    fg_atts = sum(replace_na(field_goal_attempt, 0)),
    fg_made_0_39 = sum(if_else(field_goal_result == "made" & !is.na(field_goal_result) & yardline_100 < 40, 1, 0)),
    fg_made_40_49 = sum(if_else(field_goal_result == "made" & !is.na(field_goal_result) & yardline_100 < 50 & yardline_100 > 39, 1, 0)),
    fg_made_50_plus = sum(if_else(field_goal_result == "made" & !is.na(field_goal_result) & yardline_100 > 49, 1, 0)),
    rz_rush_atts = sum(if_else(yardline_100 <= 20 , rusher_attempt, 0)),
    rz_rec_targ = sum(if_else(yardline_100 <= 20, receiver_target, 0)),
    gz_rush_atts = sum(if_else(yardline_100 <= 10, rusher_attempt, 0)),
    gz_rec_targ = sum(if_else(yardline_100 <= 10, receiver_target, 0)),
    rz_pass_atts = sum(if_else(yardline_100 <= 20, pass_attempt, 0)),
    gz_pass_atts = sum(if_else(yardline_100 <= 10, pass_attempt, 0)),
    rz_ep = sum(if_else(yardline_100 <= 20, ep, 0)),
    gz_ep = sum(if_else(yardline_100 <= 10, ep, 0)),
    ep = sum(ep),
    epa = sum(epa),
    pass_cmp = sum(pass_complete),
    pass_atts = sum(pass_attempt),
    pass_yards = sum(pass_yards_gained),
    pass_air_yards = sum(pass_air_yards),
    pass_td = sum(pass_touchdown),
    pass_two_pt_att = sum(pass_two_pt_att),
    pass_two_pt_success = sum(pass_two_pt_success),
    pass_interception = sum(pass_interception),
    pass_sack = sum(pass_sack),
    pass_sack_yards = sum(pass_sack_yards),
    pass_long = max(pass_yards_gained),
    rush_yards = sum(rusher_yards_gained),
    rush_atts = sum(rusher_attempt),
    rush_td = sum(rusher_touchdown),
    rush_two_pt_att = sum(rusher_two_pt_att),
    rush_two_pt_success = sum(rusher_two_pt_success),
    rush_long = max(rusher_yards_gained),
    rec_yards = sum(receiver_yards_gained),
    rec_td = sum(receiver_touchdown),
    rec_targ = sum(receiver_target),
    rec_incomplete_target = sum(receiver_incomplete_target),
    rec_cmp = sum(receiver_reception),
    rec_long = max(receiver_yards_gained),
    rec_airyards = sum(receiver_air_yards),
    rec_yac = sum(receiver_yac), 
    rec_two_pt_success = sum(receiver_two_pt_success),
    rec_two_pt_att = sum(receiver_two_pt_att),
    rec_adot = sum(receiver_air_yards) / sum(receiver_target)
  ) %>% 
  mutate(DKPassing =
           (pass_td * 4) + 
           if_else(pass_yards >= 300, 3, 0) +
           (pass_yards * .04) + 
           (pass_interception * -1),
         DKRushing = 
           (rush_td * 6) + 
           (rush_yards * .1) +
           if_else(rush_yards >= 100, 3, 0),
         DKReceiving = 
           (rec_td * 6) + 
           (rec_yards * .1) + 
           if_else(rec_yards >= 100, 3, 0) + 
           (rec_cmp * 1),
         DKKicking = xp_made + (fg_made_0_39 * 3) + (fg_made_40_49 * 4) + (fg_made_50_plus * 5)
  ) %>% 
  group_by(season,
           week,
           game_id,
           game_date) %>% 
  mutate(team_targets = sum(rec_targ),
         team_airyards = sum(rec_airyards),
         team_carries = sum(rush_atts),
         team_rz_rec_targ = sum(rz_rec_targ),
         team_rz_rush_atts = sum(rz_rush_atts),
         team_gz_rec_targ = sum(gz_rec_targ),
         team_gz_rush_atts = sum(gz_rush_atts)) %>% 
  ungroup() %>% 
  mutate(rec_targ_share = rec_targ / team_targets,
         rec_airyards_share = rec_airyards / team_airyards,
         rec_wopr = (1.5 * rec_targ_share) + (.7 * rec_airyards_share),
         rec_racr = rec_yards / rec_airyards,
         rush_carry_share = rush_atts / team_carries,
         rz_carry_share = rz_rush_atts / team_rz_rush_atts,
         gz_carry_share = gz_rush_atts / team_gz_rush_atts,
         rz_rec_targ_share = rz_rec_targ / team_rz_rec_targ,
         gz_rec_targ_share = gz_rec_targ / team_gz_rec_targ
         ) %>% 
  decode_player_ids() %>% 
  rename( "gsis_id" = "player_id") %>% 
  left_join(rosters_2020) %>% 
  inner_join(nfl_team_list %>% select(
    team_abbr,
    team_name, 
    team_logo_wikipedia,
    team_wordmark
  ), by = c("team" = "team_abbr"))

## Thielen in week 1 or so has a player name of NA on a 2pt

saveRDS(nfl_stats_2020, file = "nfl_stats_2020.rds")

espn_qbr_2020 <- 
  nfl_stats_2020 %>% 
  distinct(season, week) %>% 
  {map2_df(.$season, .$week, espnscrapeR::get_nfl_qbr)} %>% 
  rename(week = game_week) %>% 
  mutate(week = as.double(week))

save(espn_qbr_2020, file = "espn_qbr_2020.rds")

## Team List ##
nfl_team_list <-
  nflfastR::teams_colors_logos %>% 
  filter(!team_abbr %in% c("STL", "OAK", "LAR", "SD" ))

name_list <-
  set_names(nfl_team_list$team_abbr, nfl_team_list$team_name)

saveRDS(nfl_team_list, file = here("nfl_team_list.rds"))