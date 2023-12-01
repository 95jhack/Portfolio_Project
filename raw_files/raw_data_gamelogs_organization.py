gamelogs_df_column_names = ["date","game_no","dow","visiting_team","visiting_league","visiting_league_game_no","home_team","home_league","home_league_game_no","visiting_team_score"   ,"home_team_score","length_outs","day_night"
,"completion_info","forfeit_info","protest_info","park_info","attendance_info","time_min","visiting_line","home_line","v_AB","v_h","v_dbl","v_trpl","v_hr","v_rbi","v_sh","v_sf","v_hbp","v_bb","v_bb_int","v_so"
,"v_sb","v_cs","v_gidp","v_ci","v_lob","v_ptchrs","v_er","v_er_team","v_wp","v_balk","v_po","v_asst","v_err","v_pb","v_dbl_def","v_trpl_def","h_AB","h_h","h_dbl","h_trpl"
,"h_hr","h_rbi","h_sh","h_sf","h_hbp","h_bb","h_bb_int","h_so","h_sb","h_cs","h_gidp","h_ci","h_lob","h_ptchrs","h_er","h_er_team"
,"h_wp","h_balk","h_po","h_asst","h_err","h_pb","h_dbl_def","h_trpl_def","home_ump_id","home_ump_name","1b_ump_id","1b_ump_name","2b_ump_id","2b_ump_name","3b_ump_id"
,"3b_ump_name","lf_ump_id","lf_ump_name","rf_ump_id","rf_ump_name","v_manager_id","v_manager_name","h_manager_id","h_manager_name","w_pitcher_id","w_pitcher_name","l_pitcher_id","l_pitcher_name","sv_pitcher_id","sv_pitcher_name"
,"gw_rbi_player_id","gw_rbi_player_name","v_sp_id","v_sp_name","h_sp_id","h_sp_name","v_1_id","v_1_name","v_1_pos","v_2_id","v_2_name"
,"v_2_pos","v_3_id","v_3_name","v_3_pos","v_4_id","v_4_name","v_4_pos","v_5_id","v_5_name","v_5_pos","v_6_id","v_6_name","v_6_pos","v_7_id","v_7_name","v_7_pos","v_8_id","v_8_name","v_8_pos"
,"v_9_id","v_9_name","v_9_pos","h_1_id","h_1_name","h_1_pos","h_2_id","h_2_name","h_2_pos","h_3_id","h_3_name","h_3_pos","h_4_id","h_4_name","h_4_pos","h_5_id","h_5_name","h_5_pos"
,"h_6_id","h_6_name","h_6_pos","h_7_id","h_7_name","h_7_pos","h_8_id","h_8_name","h_8_pos","h_9_id","h_9_name","h_9_pos","additional_info","acquisition_info"
]

fact_gamelogs_cols = ["date","game_no","dow","visiting_team","visiting_league","visiting_league_game_no"
 ,"home_team","home_league","home_league_game_no","visiting_team_score","home_team_score"]

dim_team_stats_home_col = ["home_team","home_league","h_AB","h_h","h_dbl","h_trpl","h_hr","h_rbi","h_sh"
,"h_sf","h_hbp","h_bb","h_bb_int","h_so","h_sb","h_cs","h_gidp","h_ci","h_lob","h_ptchrs"
,"h_er","h_er_team","h_wp","h_balk","h_po","h_asst","h_err","h_pb","h_dbl_def","h_trpl_def"]

dim_team_stats_visitors_col = ["visiting_team","visiting_league","v_AB","v_h","v_dbl","v_trpl","v_hr","v_rbi","v_sh"
,"v_sf","v_hbp","v_bb","v_bb_int","v_so","v_sb","v_cs","v_gidp","v_ci","v_lob","v_ptchrs"
,"v_er","v_er_team","v_wp","v_balk","v_po","v_asst","v_err","v_pb","v_dbl_def","v_trpl_def"]


merged_columns = ["team","league","AB","h","dbl","trpl","hr","rbi","sh","sf","hbp","bb","bb_int"
,"so","sb","cs","gidp","ci","lob","ptchrs","er","er_team","wp","balk","po","asst","err","pb","dbl_def","trpl_def"]

