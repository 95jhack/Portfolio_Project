create_tbl =        '''
        CREATE TABLE IF NOT EXISTS pre_load_df(
            team TEXT NOT NULL
            ,wins TEXT NOT NULL
            ,losses TEXT NOT NULL
            ,team_id TEXT NOT NULL
            ,league TEXT NOT NULL
            ,ab TEXT NOT NULL
            ,h TEXT NOT NULL
            ,dbl TEXT NOT NULL
            ,trpl TEXT NOT NULL
            ,hr TEXT NOT NULL
            ,rbi TEXT NOT NULL
            ,sh TEXT NOT NULL
            ,sf TEXT NOT NULL
            ,hbp TEXT NOT NULL
            ,bb TEXT NOT NULL
            ,bb_int TEXT NOT NULL
            ,so TEXT NOT NULL
            ,sb TEXT NOT NULL
            ,cs TEXT NOT NULL
            ,gidp TEXT NOT NULL
            ,ci TEXT NOT NULL
            ,lob TEXT NOT NULL
            ,ptchrs TEXT NOT NULL
            ,er TEXT NOT NULL
            ,er_team TEXT NOT NULL
            ,wp TEXT NOT NULL
            ,balk TEXT NOT NULL
            ,po TEXT NOT NULL
            ,asst TEXT NOT NULL
            ,err TEXT NOT NULL
            ,pb TEXT NOT NULL
            ,dbl_def TEXT NOT NULL
            ,trpl_def TEXT NOT NULL
        );
        '''