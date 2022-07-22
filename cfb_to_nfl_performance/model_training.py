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

## y data
sql_query='select n.* from nfl_player_snaps_table n where n.snapcounts > 49 and n.college_id is not null'
samples=pd.read_sql_query(sql=sql_query, con=engine)
cols=['Passing_Cmp', 'Passing_Att', 'Passing_Yds', 'Passing_TD',
      'Passing_Int', 'Passing_Sk', 'Passing_Sk_Yds', 'Rushing_Att',
      'Rushing_Yds', 'Rushing_TD', 'Receiving_Tgt', 'Receiving_Rec',
      'Receiving_Yds', 'Receiving_TD', 'Fumbles_Fmb', 'Fumbles_FL',
      'Def Interceptions_Int', 'Def Interceptions_Yds','Def Interceptions_TD',
      'Def Interceptions_PD', 'Sk', 'Tackles_Comb','Tackles_Solo', 
      'Tackles_Ast', 'Tackles_TFL', 'Tackles_QBHits','Fumbles_FR', 
      'Fumbles_Yds', 'Fumbles_TD', 'Fumbles_FF','Kick Returns_Rt',
      'Kick Returns_Yds', 'Kick Returns_TD','Punt Returns_Ret', 
      'Punt Returns_Yds', 'Punt Returns_TD','Scoring_XPM', 'Scoring_XPA',
      'Scoring_FGM', 'Scoring_FGA','Punting_Pnt', 'Punting_Yds']

temp=samples.loc[:,cols]
temp[temp.isna()]=0
samples.loc[:,cols]=temp
del(temp)
samples.loc[:,cols]=samples.loc[:,cols].div(samples.snapcounts,axis=0)
samples.loc[:,'snapcounts']=samples.loc[:,'snapcounts'].div(samples.years_played,axis=0)
###################################
#samples.groupby(['first_year']).count()

## x data
sql_query='select * from cfb_player_query_table p where p."year" >= 2008'
samp=pd.read_sql_query(sql=sql_query, con=engine)

## view smaller data sample
view_data=samp.iloc[1:1000,:]


x_numerical_cols=['Pts','Opp','G','wins_so_far','losses_so_far','Passing_Att',
            'Passing_Cmp', 'Passing_Yds', 'Passing_TD', 'Passing_Int',
            'Rushing_Att', 'Rushing_Yds', 'Rushing_TD', 'Receiving_Rec',
            'Receiving_Yds', 'Receiving_TD', 'Tackles_Solo', 'Tackles_Ast',
            'Tackles_Tot', 'Tackles_Loss', 'Tackles_Sk', 'Def Int_Int',
            'Def Int_Yds', 'Def Int_TD', 'Def Int_PD', 'Fumbles_FR', 
            'Fumbles_Yds','Fumbles_TD', 'Fumbles_FF','height_inches','weight']
means=samp.loc[:,x_numerical_cols].mean(axis=0)
stds=samp.loc[:,x_numerical_cols].std(axis=0)


x_categoricals=['team_rank','conference_href','home_away','w_l','Class','Pos']
samp.loc[:,'team_rank'][samp.loc[:,'team_rank']=='']='NR'




###################################


engine.dispose()
del(engine)
