#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 09:51:34 2022

@author: chrisgonzalez
"""
import os
import sqlalchemy as sa
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import numpy as np

os.chdir('/Users/chrisgonzalez/Documents/projects/cfb_to_nfl_performance/')

engine = create_engine('postgresql://postgres:estarguars@localhost:5432/postgres')
sql_query='select n.college_id,n.player_id,n."Player",n.first_year from nfl_player_snaps_table n where n.snapcounts > 49 and n.college_id is not null'

samples=pd.read_sql_query(sql=sql_query, con=engine)
#samples.groupby(['first_year']).count()


sql_query='select * from cfb_player_query_table p where p."year" >= 2008'
samp=pd.read_sql_query(sql=sql_query, con=engine)


x_numerical_cols=['Pts','Opp','G','wins_so_far','losses_so_far','Passing_Att',
            'Passing_Cmp', 'Passing_Yds', 'Passing_TD', 'Passing_Int',
            'Rushing_Att', 'Rushing_Yds', 'Rushing_TD', 'Receiving_Rec',
            'Receiving_Yds', 'Receiving_TD', 'Tackles_Solo', 'Tackles_Ast',
            'Tackles_Tot', 'Tackles_Loss', 'Tackles_Sk', 'Def Int_Int',
            'Def Int_Yds', 'Def Int_TD', 'Def Int_PD', 'Fumbles_FR', 
            'Fumbles_Yds','Fumbles_TD', 'Fumbles_FF','height_inches','weight']
means=samp.loc[:,x_numerical_cols].mean(axis=0)
stds=samp.loc[:,x_numerical_cols].std(axis=0)

## view smaller data sample
view_data=samp.iloc[1:1000,:]

x_categoricals=['team_rank','conference_href','home_away','w_l','Class','Pos']
samp.loc[:,'team_rank'][samp.loc[:,'team_rank']=='']='NR'


'height_inches', 'weight', 







engine.dispose()
del(engine)



