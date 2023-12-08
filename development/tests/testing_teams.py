from ..functions.cleaned_functions import CsvToSqlite

# Example usage
csv_to_sqlite = CsvToSqlite(database_name='testing_teams.db')
csv_to_sqlite.create_table_from_csv("C:/Users/95jha/Documents/Learning/JHack_Portfolio/development/data/production/dim_team_statistics.csv", "dim_team_stats")

# Execute a query to retrieve data
select_query = '''
                SELECT 
                    COUNT(team) AS team_count 
                FROM dim_team_stats
            '''
result = csv_to_sqlite.fetch_all_rows(select_query)
result = result[0]
 
# this lines UNPACKS values
(rows,) = result  
# print(rows)

# # Execute a query to retrieve data
# select_query_duplicate_check = '''
#                 SELECT 
#                     team
#                     , league
#                     , COUNT(*) AS team_count
#                 FROM dim_team_stats
#                 GROUP BY 
#                     team
#                     , league
#                 HAVING COUNT(*) > 1
#             '''
# dup_check_result = csv_to_sqlite.fetch_all_rows(select_query_duplicate_check)
# dup_check_result = dup_check_result[0] 

# # this lines UNPACKS values
# (dup_check_row,) = dup_check_result  
# print(rows_home)

# Execute a query to retrieve data
select_query_duplicate_check = '''
                SELECT 
                    COALESCE(sq.team_count,0)
                FROM(
                    SELECT 
                        team
                        , team_id
                        , COUNT(*) AS team_count
                    FROM dim_team_stats
                    GROUP BY 
                        team
                        , team_id
                ) AS sq
                WHERE team_count > 1
            '''
dup_check_result = len(csv_to_sqlite.fetch_all_rows(select_query_duplicate_check))

csv_to_sqlite.close_connection()

