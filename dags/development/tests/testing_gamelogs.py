from ..functions.cleaned_functions import CsvToSqlite

csv_link = "C:/Users/95jha/Documents/Learning/Portfolio_Project/dags/development/data/production/fact_gamelogs.csv"

# Example usage
csv_to_sqlite = CsvToSqlite(database_name='testing_gamelogs.db')
csv_to_sqlite.create_table_from_csv(csv_link, "fact_gamelogs")

# Execute a query to retrieve data
# select_query1 = '''
#                 SELECT 
#                     COUNT(date) AS game_count
#                 FROM fact_gamelogs
#                 WHERE TRUE
#                     AND opponent_team = 'TOR'
#                 GROUP BY 
#                     opponent_team
#             '''
# result_away = csv_to_sqlite.fetch_all_rows(select_query1)
# result_away = result_away[0]

# # this lines UNPACKS values
# (rows_away,) = result_away  
# # print(rows_away)

# # Execute a query to retrieve data
# select_query2 = '''
#                 SELECT 
#                     COUNT(date) AS game_count
#                 FROM fact_gamelogs
#                 WHERE TRUE
#                     AND team = 'TOR'
#                 GROUP BY 
#                     team
#             '''
# result_home = csv_to_sqlite.fetch_all_rows(select_query2)
# result_home = result_home[0] 

# # this lines UNPACKS values
# (rows_home,) = result_home  
# # print(rows_home)

# Execute a query to retrieve data
select_query_duplicate_check = '''
                SELECT 
                    COALESCE(sq.game_count,0)
                FROM(
                    SELECT 
                        date
                        , opponent_team
                        , team
                        , team_game_no
                        , COUNT(*) AS game_count
                    FROM fact_gamelogs
                    GROUP BY 
                        date
                        , opponent_team
                        , team
                        , team_game_no
                ) AS sq
                WHERE game_count > 1
            '''
dup_check_result = len(csv_to_sqlite.fetch_all_rows(select_query_duplicate_check))

csv_to_sqlite.close_connection()