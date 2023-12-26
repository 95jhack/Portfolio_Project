create_tbl =        '''
        CREATE TABLE IF NOT EXISTS pre_load_df(
            team TEXT NOT NULL
            ,wins INTEGER NOT NULL
            ,losses INTEGER NOT NULL
            ,team_id INTEGER NOT NULL
            ,league TEXT NOT NULL
            ,ab INTEGER NOT NULL
            ,h INTEGER NOT NULL
            ,dbl INTEGER NOT NULL
            ,trpl INTEGER NOT NULL
            ,hr INTEGER NOT NULL
            ,rbi INTEGER NOT NULL
            ,sh INTEGER NOT NULL
            ,sf INTEGER NOT NULL
            ,hbp INTEGER NOT NULL
            ,bb INTEGER NOT NULL
            ,bb_int INTEGER NOT NULL
            ,so INTEGER NOT NULL
            ,sb INTEGER NOT NULL
            ,cs INTEGER NOT NULL
            ,gidp INTEGER NOT NULL
            ,ci INTEGER NOT NULL
            ,lob INTEGER NOT NULL
            ,ptchrs INTEGER NOT NULL
            ,er INTEGER NOT NULL
            ,er_team INTEGER NOT NULL
            ,wp INTEGER NOT NULL
            ,balk INTEGER NOT NULL
            ,po INTEGER NOT NULL
            ,asst INTEGER NOT NULL
            ,err INTEGER NOT NULL
            ,pb INTEGER NOT NULL
            ,dbl_def INTEGER NOT NULL
            ,trpl_def INTEGER NOT NULL
        );
        '''