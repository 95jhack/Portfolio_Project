from cleaned_functions import CsvToSqlite

# Example usage
csv_to_sqlite = CsvToSqlite(database_name='testing_gamelogs.db')
csv_to_sqlite.create_table_from_csv("C:/Users/95jha/Documents/Learning/JHack_Portfolio/cleaned_files/fact_gamelogs.csv", "fact_gamelogs")

# Execute a query to retrieve data
select_query1 = '''
                SELECT 
                    COUNT(date) AS game_count
                FROM fact_gamelogs
                WHERE TRUE
                    AND visiting_team = 'TOR'
                GROUP BY 
                    visiting_team
            '''
result_away = csv_to_sqlite.fetch_all_rows(select_query1)
result_away = result_away[0]

# this lines UNPACKS values
(rows_away,) = result_away  
# print(rows_away)

# Execute a query to retrieve data
select_query2 = '''
                SELECT 
                    COUNT(date) AS game_count
                FROM fact_gamelogs
                WHERE TRUE
                    AND home_team = 'TOR'
                GROUP BY 
                    home_team
            '''
result_home = csv_to_sqlite.fetch_all_rows(select_query2)
result_home = result_home[0] 

# this lines UNPACKS values
(rows_home,) = result_home  
# print(rows_home)

csv_to_sqlite.close_connection()