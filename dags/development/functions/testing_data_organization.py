
# Using the projected schemas listed within the documentation, the organization of column names and data types are clear.
expected_df_dates = {
    'column_name': ['Date', 'date_id', 'Year', 'Month','Day','Month_Name','Weekday','dayofyear','Quarter','days_in_month','is_month_start','is_month_end','is_year_start','is_year_end','is_leap_year']
    , 'data_type': ['object','int64','int64','int64','int64','object','int64','int64','int64','int64','object','object','object','object','object']
}

expected_df_gamelogs = {
    'column_name': ['date', 'opponent_team', 'opponent_league', 'team','league','team_game_no','opponent_score','team_score','win_loss','is_home']
    , 'data_type': ['int64','object','object','object','object','int64','int64','int64','int64','int64']
}

expected_df_statistics = {
    'column_name': ['team','wins','losses','team_id','league','ab','h','dbl','trpl','hr','rbi','sh','sf','hbp','bb','bb_int','so','sb','cs','gidp','ci','lob','ptchrs','er','er_team','wp','balk','po','asst','err','pb','dbl_def','trpl_def']
    , 'data_type': ['object','int64','int64','int64','object','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64']
}

expected_df_events = {
    'column_name': ['index','event_type','inning_no','home_team','player_id','count_on_batter','pitches_to_batter','event_describer','team','date_id','PA','AB','H','Double','Triple','HR','RBI','BB','BB_int','HBP','SH','SF','SB','CS','GIDP','SO']
    , 'data_type': ['int64','object','int64','int64','object','float64','object','object','object','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64','int64']
}

expected_df_rosters = {
    'column_name': ['player_id','lastname','firstname','handedness_batting','handedness_throwing','team','position']
    , 'data_type': ['object','object','object','object','object','object','object']
}