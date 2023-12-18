create_tbl =        '''
        CREATE TABLE IF NOT EXISTS date_df(
            Date DATE NOT NULL
            ,date_id INTEGER NOT NULL
            ,Year INTEGER NOT NULL
            ,Month INTEGER NOT NULL
            ,Day INTEGER NOT NULL
            ,Month_Name TEXT NOT NULL
            ,Weekday TEXT NOT NULL
            ,dayofyear INTEGER NOT NULL
            ,Quarter INTEGER NOT NULL
            ,days_in_month INTEGER NOT NULL
            ,is_month_start BOOLEAN NOT NULL
            ,is_month_end BOOLEAN NOT NULL
            ,is_year_start BOOLEAN NOT NULL
            ,is_year_end BOOLEAN NOT NULL
            ,is_leap_year BOOLEAN NOT NULL
        );
        '''
