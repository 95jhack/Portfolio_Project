from cleaned_functions import CsvToSqlite

# Example usage
csv_to_sqlite = CsvToSqlite(database_name='testing_teams.db')
csv_to_sqlite.create_table_from_csv("C:/Users/95jha/Documents/Learning/JHack_Portfolio/cleaned_files/dim_team_statistics.csv", "dim_team_stats")

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

csv_to_sqlite.close_connection()

