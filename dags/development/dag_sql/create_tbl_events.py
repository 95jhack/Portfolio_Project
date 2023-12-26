create_tbl =        '''
        CREATE TABLE IF NOT EXISTS file_df(
            team TEXT NOT NULL
            ,league TEXT NOT NULL
            ,city TEXT NOT NULL
            ,teamlogo TEXT NOT NULL
        );
        '''

create_tbl_list =        '''
        CREATE TABLE IF NOT EXISTS file_list_df(
            file TEXT NOT NULL
        );
        '''