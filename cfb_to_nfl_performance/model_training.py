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

y_means=samples.loc[:,cols+['snapcounts']].mean(axis=0)
y_stds=samples.loc[:,cols+['snapcounts']].std(axis=0)
samples.loc[:,cols+['snapcounts']]=( samples.loc[:,cols+['snapcounts']] - y_means )/y_stds

###################################
#samples.groupby(['first_year']).count()

## x data
sql_query='select * from cfb_player_query_table p where p."year" >= 2008 and p."year" < 2021'
samp=pd.read_sql_query(sql=sql_query, con=engine)
engine.dispose()
del(engine)


samp['team_rank_NR']=0
samp.loc[samp.team_rank=='','team_rank_NR']=1
#samp['team_rank_NR'][samp.loc[:,'team_rank']=='']=1
samp.loc[samp.team_rank=='','team_rank']=0
#samp.loc[:,'team_rank'][samp.loc[:,'team_rank']=='']=0
samp.loc[ samp.wins_so_far.isna(),'wins_so_far' ]=0
samp.loc[ samp.losses_so_far.isna(),'losses_so_far' ]=0
samp.loc[ samp.Pts.isna(),'Pts' ]=0
samp.loc[ samp.Opp.isna(),'Opp' ]=0

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

samp.loc[samp.height_inches.isna(),'height_inches']=0
#samp.loc[:,'height_inches'][samp.loc[:,'height_inches'].isna()]=0
samp.loc[samp.weight.isna(),'weight']=0
#samp.loc[:,'weight'][samp.loc[:,'weight'].isna()]=0


""" normalize data """ 
samp.loc[:,x_numerical_cols]=(samp.loc[:,x_numerical_cols]-means)/stds

""" categoricals """
x_categoricals=['conference_href','home_away','w_l','Class','Pos']
cate=pd.get_dummies(samp.loc[:,x_categoricals])
x_categoricals=list(cate.columns)
samp=pd.concat([samp,cate],axis=1)
del(cate)

## view smaller data sample
view_data=samp.iloc[1:1000,:]


""" Create Tensor for signle player here """
# 2 own team and opponent by   48 games  by  180 rows/players   by 89 columns boxscores
import torch

def make_x_data_tensor(player_id,debug_flag=''):
    
    x_data=torch.zeros([48,2,180,89])
    college_id=player_id
    #college_id='corey-thompson-3'
    
    
    # example
    boxscores=list(samp.loc[samp.player_href==college_id,'boxscore_href'])
    boxscoresteam=list(samp.loc[samp.player_href==college_id,'boxscore_href']+'_'+samp.loc[ samp.player_href==college_id,'college_href'])
    
    same_team=samp[samp.boxscore_href.isin(boxscores)]
    opp_team=same_team[~(same_team.boxscore_href+'_'+same_team.college_href).isin(boxscoresteam)]
    same_team=same_team[(same_team.boxscore_href+'_'+same_team.college_href).isin(boxscoresteam)]
    
    same_team['is_player']=0
    same_team.loc[same_team.player_href==college_id,'is_player']=1
    #same_team['is_player'][same_team.player_href==college_id]=1
    opp_team['is_player']=0
    
    same_team['na_player']=0
    opp_team['na_player']=0
    same_team['na_game']=0
    opp_team['na_game']=0
    
    
    boxscores.sort(reverse=True)
    boxscoresteam.sort(reverse=True)
    
    for i in range(x_data.shape[0]):
        
        if i < len(boxscores):
            if debug_flag=='y':
                print(i)
        
            ### same team logic 
            same_team_temp=same_team.loc[same_team.boxscore_href==boxscores[i],:]
            same_team_temp=np.array(same_team_temp.loc[:,x_numerical_cols+x_categoricals+['team_rank','team_rank_NR','is_player','na_player','na_game']])            
            
            """ put into x_data tensor """
            if same_team_temp.shape[0] < x_data.shape[2]:
                fill=pd.DataFrame( np.zeros([x_data.shape[2]-same_team_temp.shape[0],x_data.shape[3]]), columns=x_numerical_cols+x_categoricals+['team_rank','team_rank_NR','is_player','na_player','na_game'] )
                fill['na_player']=1
                same_team_temp=np.concatenate((same_team_temp,fill),axis=0)
                np.random.shuffle(same_team_temp)
            else:
                same_team_temp=same_team_temp[0:x_data.shape[2],:]
                np.random.shuffle(same_team_temp)
                        
            x_data[i][0]=torch.from_numpy(same_team_temp.astype('float64'))  
            if debug_flag=='y':
                print(torch.isnan(x_data[i][0]).any())
            
            ### opp team logic 
            opp_team_temp=opp_team.loc[opp_team.boxscore_href==boxscores[i],:]
            opp_team_temp=np.array(opp_team_temp.loc[:,x_numerical_cols+x_categoricals+['team_rank','team_rank_NR','is_player','na_player','na_game']])
            """ put into x_data tensor """
            if opp_team_temp.shape[0] < x_data.shape[2]:
                fill=pd.DataFrame( np.zeros([x_data.shape[2]-opp_team_temp.shape[0],x_data.shape[3]]), columns=x_numerical_cols+x_categoricals+['team_rank','team_rank_NR','is_player','na_player','na_game'] )
                fill['na_player']=1
                opp_team_temp=np.concatenate((opp_team_temp,fill),axis=0)
                np.random.shuffle(opp_team_temp)                
            else:
                opp_team_temp=opp_team_temp[0:x_data.shape[2],:]
                np.random.shuffle(opp_team_temp)                
                        
            x_data[i][1]=torch.from_numpy(opp_team_temp.astype('float64'))  
            if debug_flag=='y':
                print(torch.isnan(x_data[i][1]).any())
            
        else:
            same_team_temp=pd.DataFrame(np.zeros([180,89]),columns=x_numerical_cols+x_categoricals+['team_rank','team_rank_NR','is_player','na_player','na_game'])        
            same_team_temp['na_game']=1
            same_team_temp['na_player']=1
            same_team_temp=np.array(same_team_temp)
            
            opp_team_temp=pd.DataFrame(np.zeros([180,89]),columns=x_numerical_cols+x_categoricals+['team_rank','team_rank_NR','is_player','na_player','na_game'])        
            opp_team_temp['na_game']=1
            opp_team_temp['na_player']=1
            opp_team_temp=np.array(opp_team_temp)
            
            x_data[i][0]=torch.from_numpy(same_team_temp.astype('float64'))        
            x_data[i][1]=torch.from_numpy(opp_team_temp.astype('float64'))        
    
    return x_data


### model training 
import nn_model
nn=nn_model.NeuralNetwork(4,4,4,16)
loss = torch.nn.MSELoss()
optimizer = torch.optim.Adam(nn.parameters(), lr=1e-3)

iters=100000
for k in range(iters):
    samples=samples.sample(frac=1)
    train_ids=list(samples.college_id[samples.first_year!=2021])
    test_ids=list(samples.college_id[samples.first_year==2021])
    nn.train()
    
    for j in range(len(train_ids)):
        x_train=make_x_data_tensor(train_ids[j])
        y_train=torch.tensor(samples.loc[samples.college_id==train_ids[j],cols+['snapcounts']].values)
        y_train=y_train.flatten().float()
        
        ## predict
        x_vals=nn(x_train).float()
        loss_val=loss(x_vals,y_train)
        
        ## run sgd
        optimizer.zero_grad()
        loss_val.backward()
        optimizer.step()
        
        print(train_ids[j])
        print(loss_val)
        print(torch.isnan(x_train).any())
        #print(nn.layer5.weight)


    



