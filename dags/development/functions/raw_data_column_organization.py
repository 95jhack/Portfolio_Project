"""
Purpose:
This file is used to de-clutter the individual ETL files as the width of the original table
and some subsequent objects is difficult to read.

These lists are called during the creation / manipulation of numerous objects. 
"""

# This list is the original column list used for the Raw Data for this project.
gamelogs_df_column_names = ["date","game_no","dow","visiting_team","visiting_league","visiting_league_game_no","home_team","home_league","home_league_game_no","visiting_team_score"   ,"home_team_score","length_outs","day_night"
,"completion_info","forfeit_info","protest_info","park_info","attendance_info","time_min","visiting_line","home_line","v_ab","v_h","v_dbl","v_trpl","v_hr","v_rbi","v_sh","v_sf","v_hbp","v_bb","v_bb_int","v_so"
,"v_sb","v_cs","v_gidp","v_ci","v_lob","v_ptchrs","v_er","v_er_team","v_wp","v_balk","v_po","v_asst","v_err","v_pb","v_dbl_def","v_trpl_def","h_ab","h_h","h_dbl","h_trpl"
,"h_hr","h_rbi","h_sh","h_sf","h_hbp","h_bb","h_bb_int","h_so","h_sb","h_cs","h_gidp","h_ci","h_lob","h_ptchrs","h_er","h_er_team"
,"h_wp","h_balk","h_po","h_asst","h_err","h_pb","h_dbl_def","h_trpl_def","home_ump_id","home_ump_name","one_b_ump_id","one_b_ump_name","two_b_ump_id","two_b_ump_name","three_b_ump_id"
,"three_b_ump_name","lf_ump_id","lf_ump_name","rf_ump_id","rf_ump_name","v_manager_id","v_manager_name","h_manager_id","h_manager_name","w_pitcher_id","w_pitcher_name","l_pitcher_id","l_pitcher_name","sv_pitcher_id","sv_pitcher_name"
,"gw_rbi_player_id","gw_rbi_player_name","v_sp_id","v_sp_name","h_sp_id","h_sp_name","v_one_id","v_one_name","v_one_pos","v_two_id","v_two_name"
,"v_two_pos","v_three_id","v_three_name","v_three_pos","v_four_id","v_four_name","v_four_pos","v_five_id","v_five_name","v_five_pos","v_six_id","v_six_name","v_six_pos","v_seven_id","v_seven_name","v_seven_pos","v_eight_id","v_eight_name","v_eight_pos"
,"v_nine_id","v_nine_name","v_nine_pos","h_one_id","h_one_name","h_one_pos","h_two_id","h_two_name","h_two_pos","h_three_id","h_three_name","h_three_pos","h_four_id","h_four_name","h_four_pos","h_five_id","h_five_name","h_five_pos"
,"h_six_id","h_six_name","h_six_pos","h_seven_id","h_seven_name","h_seven_pos","h_eight_id","h_eight_name","h_eight_pos","h_nine_id","h_nine_name","h_nine_pos","additional_info","acquisition_info"
]

# This is the column list used when beginning to build out the fact_gamelogs object.
fact_gamelogs_cols = ["date","game_no","dow","visiting_team","visiting_league","visiting_league_game_no"
 ,"home_team","home_league","home_league_game_no","visiting_team_score","home_team_score"]

# This is the column list used for building out the dim_team_statistics dimension, for the home team.
dim_team_stats_home_col = ["home_team","home_league","h_ab","h_h","h_dbl","h_trpl","h_hr","h_rbi","h_sh"
,"h_sf","h_hbp","h_bb","h_bb_int","h_so","h_sb","h_cs","h_gidp","h_ci","h_lob","h_ptchrs"
,"h_er","h_er_team","h_wp","h_balk","h_po","h_asst","h_err","h_pb","h_dbl_def","h_trpl_def"]
# This is the column list used for building out the dim_team_statistics dimension, for the visiting team.
dim_team_stats_visitors_col = ["visiting_team","visiting_league","v_ab","v_h","v_dbl","v_trpl","v_hr","v_rbi","v_sh"
,"v_sf","v_hbp","v_bb","v_bb_int","v_so","v_sb","v_cs","v_gidp","v_ci","v_lob","v_ptchrs"
,"v_er","v_er_team","v_wp","v_balk","v_po","v_asst","v_err","v_pb","v_dbl_def","v_trpl_def"]

# This is the column list used for building out the dim_team_statistics dimension, this is used for the rename & union process.
unioned_columns = ["team","league","ab","h","dbl","trpl","hr","rbi","sh","sf","hbp","bb","bb_int"
,"so","sb","cs","gidp","ci","lob","ptchrs","er","er_team","wp","balk","po","asst","err","pb","dbl_def","trpl_def"]

