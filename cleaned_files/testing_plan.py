'''
This document focuses on the numerous objects within this project, 
the objective is to understand the schemas and the required testing to prove out there design.
'''

#########################################################
########### Object: dim_date ############################
#########################################################

'''
Column Name             Proposed Data Type           Nullable            FK / PK
Date                    Date                            No                  PK
date_id                 Number(10,0)                    No                  FK (fact_gamelogs - date)
Year                    Number(5,0)                     No
Month                   Number(2,0)                     No                  
Day                     Number(2,0)                     No 
Month_Name              Varchar(10)                     No 
Weekday                 Varchar(10)                     No 
dayofyear               Number(3,0)                     No 
Quarter                 Number(1,0)                     No 
days_in_month           Number(2,0)                     No 
is_month_start          BOOLEAN                         No 
is_month_end            BOOLEAN                         No
is_year_start           BOOLEAN                         No
is_year_end             BOOLEAN                         No
is_leap_year            BOOLEAN                         No

Tests:
* Test the column set listed above.
* There should be no duplicated rows.
* Not Null Tests ---> All columns
* Accepted Values Tests [T/F] ---> All Boolean columns
* Logic Based Testing:
    - Day between 1 and 31
    - Month between 1 and 12
    - Quarter between 1 and 4
    - dayofyear between 1 and 365, if is_leap_year = False, else between 1 and 366
    - days_in_month between 28 and 31
    - if is_month_start = TRUE, Day must equal 1
    - if is_year_start = TRUE, Day must equal 1, dayofyear must equal 1, Month = 1, is_month_start = TRUE
    - if is_year_end = True, Day must equal 31, Month = 12
    - if is_month_end = True, Day must be between 28 and 31.

'''


#########################################################
########### Object: dim_team_statistics #################
#########################################################

'''
Column Name             Proposed Data Type           Nullable            FK / PK
team                    Varchar(5)                      No                  PK, FK (fact_gamelogs - team)                    
wins                    Number(3,0)                     No
losses                  Number(3,0)                     No
league                  Varchar(5)                      No
AB                      Number(5,0)                     No
h                       Number(5,0)                     No
dbl                     Number(5,0)                     No
trpl                    Number(5,0)                     No
hr                      Number(5,0)                     No
rbi                     Number(5,0)                     No
sh                      Number(5,0)                     No                    
sf                      Number(5,0)                     No
hbp                     Number(5,0)                     No
bb                      Number(5,0)                     No
bb_int                  Number(5,0)                     No
so                      Number(5,0)                     No
sb                      Number(5,0)                     No
cs                      Number(5,0)                     No
gidp                    Number(5,0)                     No
ci                      Number(5,0)                     No
lob                     Number(5,0)                     No
ptchrs                  Number(5,0)                     No
er                      Number(5,0)                     No
er_team                 Number(5,0)                     No
wp                      Number(5,0)                     No
balk                    Number(5,0)                     No
po                      Number(5,0)                     No
asst                    Number(5,0)                     No
err                     Number(5,0)                     No
pb                      Number(5,0)                     No
dbl_def                 Number(5,0)                     No
trpl_def                Number(5,0)                     No

Tests:
* Test the column set listed above.
* There should be no duplicated rows.
* Not Null Tests ---> All columns
* Accepted Values Tests:
    - league in (AL, NL)
* Logic Based Testing:
    - Use baseball logic to populate.

'''

#########################################################
########### Object: fact_gamelogs #######################
#########################################################

'''
Column Name             Proposed Data Type           Nullable            FK / PK
date                    Number(10,0)                    No                  FK (dim_date - date_id)
opponent_team           Varchar(5)                      No
opponent_league         Varchar(3)                      No
team                    Varchar(5)                      No                  FK (dim_team_statistics - team)
league                  Varchar(3)                      No
team_game_no            Number(3,0)                     No
opponent_score          Number(3,0)                     No
team_score              Number(3,0)                     No
win_loss                Number(1,0)                     No
is_home                 Number(1,0)                     No

Tests:
* Test the column set listed above.
* There should be no duplicated rows.
* Not Null Tests ---> All columns
* Accepted Values Tests:
    - is_home in (0,1)
    - win_loss in (0,1)
    - league in (AL, NL)
* Logic Based Testing:
    - if win_loss = 1 then team_score > opponent_score, else 0.
    - team_game_no between 1 and 162.
'''