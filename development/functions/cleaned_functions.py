import csv
import sqlite3

class CsvToSqlite:

    def __init__(self, database_name='mydatabase.db'):
        self.database_name = database_name
        self.conn = sqlite3.connect(self.database_name)
        self.cursor = self.conn.cursor()

    def create_table_from_csv(self, csv_file, table_name):

        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
        self.cursor.execute(drop_table_query)

        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)

            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(header)});"
            self.cursor.execute(create_table_query)

            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?']*len(header))});"
            for row in reader:
                self.cursor.execute(insert_query, row)

        self.conn.commit()

    def execute_query(self, query, values=None):
        # Execute a query
        if values:
            self.cursor.execute(query, values)
        else:
            self.cursor.execute(query)
        # Commit the changes
        self.conn.commit()

    def fetch_all_rows(self, query):
        # Execute a query to retrieve data
        self.cursor.execute(query)
        # Fetch all rows from the result set
        rows = self.cursor.fetchall()
        return rows

    def close_connection(self):
        self.conn.close()


