#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  8 19:16:41 2022

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
sql_query='select n.* from nfl_player_snaps_table_yr1 n where n.snapcounts > 49 and n.college_id is not null'
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

""" normalize maybe? """
y_means=samples.loc[:,cols+['snapcounts']].mean(axis=0)
y_stds=samples.loc[:,cols+['snapcounts']].std(axis=0)
samples.loc[:,cols+['snapcounts']]=( samples.loc[:,cols+['snapcounts']] - y_means )/y_stds

###################################
#samples.groupby(['first_year']).count()

## x data
sql_query='select * from cfb_player_query_table p where p."year" >= 2008 and p."year" < 2021'
samp=pd.read_sql_query(sql=sql_query, con=engine)

samp['height_weight_na']=0
samp.loc[samp.height_inches.isna(),'height_weight_na']=1
samp.loc[samp.weight.isna(),'height_weight_na']=1
samp.loc[samp.height_inches.isna(),'height_inches']=0
samp.loc[samp.weight.isna(),'weight']=0

player_x_categoricals=['Class','Pos']
cate=pd.get_dummies(samp.loc[:,player_x_categoricals])
player_x_categoricals=list(cate.columns)
samp=pd.concat([samp,cate],axis=1)

engine.dispose()
del(engine,cate)


view_data=samp.iloc[1:1000,:]

""" outcome formatting """
team_cols=['boxscore_href','college_href','conference_href','home_away','w_l','team_rank','Pts','Opp','G','wins_so_far','losses_so_far']
team_data=samp.loc[:,team_cols].drop_duplicates()

team_data['team_rank_NR']=0
team_data.loc[team_data.team_rank=='','team_rank_NR']=1
team_data.loc[team_data.team_rank=='','team_rank']=0
team_data.loc[team_data.wins_so_far.isna(),'wins_so_far' ]=0
team_data.loc[team_data.losses_so_far.isna(),'losses_so_far' ]=0
team_data.loc[team_data.Pts.isna(),'Pts' ]=0
team_data.loc[team_data.Opp.isna(),'Opp' ]=0

x_categoricals=['conference_href','home_away','w_l']
cate=pd.get_dummies(team_data.loc[:,x_categoricals])
x_categoricals=list(cate.columns)
team_data=pd.concat([team_data,cate],axis=1)
del(cate)

team_data_means=team_data.loc[:,['Pts','Opp','G','wins_so_far','losses_so_far']].mean(axis=0)
team_data_stds=team_data.loc[:,['Pts','Opp','G','wins_so_far','losses_so_far']].std(axis=0)
team_data.loc[:,['Pts','Opp','G','wins_so_far','losses_so_far']]=(team_data.loc[:,['Pts','Opp','G','wins_so_far','losses_so_far']] - team_data_means)/team_data_stds

""" by class and pos """
class_pos=samp.loc[:,['Class','Pos']].drop_duplicates().reset_index(drop=True)
x_numerical_cols=['Passing_Att',
            'Passing_Cmp', 'Passing_Yds', 'Passing_TD', 'Passing_Int',
            'Rushing_Att', 'Rushing_Yds', 'Rushing_TD', 'Receiving_Rec',
            'Receiving_Yds', 'Receiving_TD', 'Tackles_Solo', 'Tackles_Ast',
            'Tackles_Tot', 'Tackles_Loss', 'Tackles_Sk', 'Def Int_Int',
            'Def Int_Yds', 'Def Int_TD', 'Def Int_PD', 'Fumbles_FR', 
            'Fumbles_Yds','Fumbles_TD', 'Fumbles_FF','Kick Ret_Ret',
            'Kick Ret_Yds', 'Kick Ret_TD', 'Punt Ret_Ret', 'Punt Ret_Yds',
            'Punt Ret_TD', 'Kicking_XPM', 'Kicking_XPA', 'Kicking_FGM',
            'Kicking_FGA', 'Punting_Punts', 'Punting_Yds','height_inches','weight','height_weight_na']


by_game_data=samp.loc[:,['Class','Pos','boxscore_href','college_href']+x_numerical_cols].groupby(by=['Class','Pos','boxscore_href','college_href'],as_index=False).sum() 
by_game_data_mean=by_game_data.loc[:,['Class','Pos']+x_numerical_cols].groupby(by=['Class','Pos'],as_index=False).mean() 
by_game_data_std=by_game_data.loc[:,['Class','Pos']+x_numerical_cols].groupby(by=['Class','Pos'],as_index=False).std() 
view_bygame_data=by_game_data.loc[by_game_data.boxscore_href=='2021-01-11-alabama',:]
## need a for loop to standardize
for i in range(class_pos.shape[0]):
    temp_class=class_pos.Class[i]
    temp_pos=class_pos.Pos[i]
    temp_std=by_game_data_std.loc[(temp_class==by_game_data_std.Class) & (temp_pos==by_game_data_std.Pos) , :]   
    temp_std=temp_std.loc[:,(temp_std!=0).any(axis=0)]
    temp_std=temp_std.squeeze()
    
    temp_mean=by_game_data_mean.loc[(temp_class==by_game_data_mean.Class) & (temp_pos==by_game_data_mean.Pos) , :]       
    temp_mean=temp_mean.squeeze()
        
    temp_cols=temp_std.index
    temp_cols=temp_cols[temp_cols!='Class']
    temp_cols=temp_cols[temp_cols!='Pos']
    if len(temp_cols)==0:
        continue
    else:
        by_game_data.loc[(temp_class==by_game_data.Class) & (temp_pos==by_game_data.Pos) ,temp_cols]= (by_game_data.loc[(temp_class==by_game_data.Class) & (temp_pos==by_game_data.Pos) ,temp_cols] - temp_mean[temp_cols])/temp_std[temp_cols]
    

means=samp.loc[:,x_numerical_cols].mean(axis=0)
stds=samp.loc[:,x_numerical_cols].std(axis=0)
samp.loc[:,x_numerical_cols]=(samp.loc[:,x_numerical_cols]-means)/stds


""" player mean std by pos/class"""
"""
player_data_mean=samp.loc[:,['Class','Pos']+x_numerical_cols].groupby(by=['Class','Pos'],as_index=False).mean() 
player_data_std=samp.loc[:,['Class','Pos']+x_numerical_cols].groupby(by=['Class','Pos'],as_index=False).std() 
## need a for loop to standardize
for i in range(class_pos.shape[0]):
    temp_class=class_pos.Class[i]
    temp_pos=class_pos.Pos[i]
    temp_std=player_data_std.loc[(temp_class==player_data_std.Class) & (temp_pos==player_data_std.Pos) , :]   
    temp_std=temp_std.loc[:,(temp_std!=0).any(axis=0)]
    temp_std=temp_std.squeeze()
    
    temp_mean=player_data_mean.loc[(temp_class==player_data_mean.Class) & (temp_pos==player_data_mean.Pos) , :]       
    temp_mean=temp_mean.squeeze()
        
    temp_cols=temp_std.index
    temp_cols=temp_cols[temp_cols!='Class']
    temp_cols=temp_cols[temp_cols!='Pos']
    if len(temp_cols)==0:
        continue
    else:
        samp.loc[(temp_class==samp.Class) & (temp_pos==samp.Pos) ,temp_cols]= (samp.loc[(temp_class==samp.Class) & (temp_pos==samp.Pos) ,temp_cols] - temp_mean[temp_cols])/temp_std[temp_cols]
"""

view_data=samp.iloc[1:1000,:]



import torch
def make_x_data_tensor(player_id,debug_flag=''):
    
    own_class_pos_data=torch.zeros([48,130,39])
    opp_class_pos_data=torch.zeros([48,130,39])
        
    college_id=player_id
    #college_id='greg-newsome-ii-1'
    print(college_id)
        
    # example
    boxscores=list(samp.loc[samp.player_href==college_id,'boxscore_href'])
    boxscoresteam=list(samp.loc[samp.player_href==college_id,'boxscore_href']+'_'+samp.loc[ samp.player_href==college_id,'college_href'])
    boxscores.sort(reverse=True)
    boxscoresteam.sort(reverse=True)
    print(boxscores)

    ## individual player data per game
    player_x_data=samp.loc[samp.player_href==college_id,]
    player_x_data=player_x_data.sort_values('boxscore_href',ascending=False)
    player_x_data['na_game']=0
    player_x_data=np.array(player_x_data.loc[:,x_numerical_cols+player_x_categoricals+['na_game']])
    
    if player_x_data.shape[0] < 48:
        fill=pd.DataFrame( np.zeros([48-player_x_data.shape[0],len(x_numerical_cols+player_x_categoricals+['na_game'])]), columns=x_numerical_cols+player_x_categoricals+['na_game'] )
        fill['na_game']=1
        player_x_data=np.concatenate((player_x_data,fill),axis=0)
    else:
        player_x_data=player_x_data[0:48,:]
    
    player_x_data=torch.from_numpy(player_x_data.astype('float64'))  
    ###################################

    ### class position data per game
    for i in range(48):
        
        if i < len(boxscores): 
            ## own team class pos
            temp=by_game_data.loc[by_game_data.boxscore_href==boxscores[i],:]        
            temp=temp[(temp.boxscore_href+'_'+temp.college_href).isin(boxscoresteam)]
            temp=class_pos.merge(temp.loc[:,['Class','Pos']+x_numerical_cols],on=['Class','Pos'],how='left')
            temp=temp.loc[:,x_numerical_cols]
            temp[temp.isna()]=0
            temp=np.array(temp)            
            own_class_pos_data[i]=torch.from_numpy(temp.astype('float64'))  
        
            ## opp team class pos
            temp=by_game_data.loc[by_game_data.boxscore_href==boxscores[i],:]        
            temp=temp[~(temp.boxscore_href+'_'+temp.college_href).isin(boxscoresteam)]
            temp=class_pos.merge(temp.loc[:,['Class','Pos']+x_numerical_cols],on=['Class','Pos'],how='left')
            temp=temp.loc[:,x_numerical_cols]
            temp[temp.isna()]=0
            temp=np.array(temp)            
            opp_class_pos_data[i]=torch.from_numpy(temp.astype('float64'))  
    ###################################        
    
    ### own team data per game
    own_team_sched=torch.zeros([48,27])
    
    temp=team_data.loc[team_data.boxscore_href.isin(boxscores),:]
    temp=temp[(temp.boxscore_href+'_'+temp.college_href).isin(boxscoresteam)]
    temp=temp.sort_values('boxscore_href',ascending=False)
    temp=temp.loc[:,['team_rank','team_rank_NR','Pts','Opp','G','wins_so_far','losses_so_far']+x_categoricals]
    temp=temp.astype('float')
    temp[temp.isna()]=0
    temp=np.array(temp)            
    temp=torch.from_numpy(temp.astype('float64'))  
            
    if temp.shape[0] > 48:
        own_team_sched[0:48]=temp[0:48]
    else:
        own_team_sched[0:temp.shape[0]]=temp
    
    ### opp team data per game
    opp_team_sched=torch.zeros([48,27])
    
    temp=team_data.loc[team_data.boxscore_href.isin(boxscores),:]
    temp=temp[~(temp.boxscore_href+'_'+temp.college_href).isin(boxscoresteam)]
    temp=temp.sort_values('boxscore_href',ascending=False)
    temp=temp.loc[:,['team_rank','team_rank_NR','Pts','Opp','G','wins_so_far','losses_so_far']+x_categoricals]
    temp=temp.astype('float')
    temp[temp.isna()]=0
    temp=np.array(temp)            
    temp=torch.from_numpy(temp.astype('float64'))  
            
    if temp.shape[0] > 48:
        opp_team_sched[0:48]=temp[0:48]
    else:
        opp_team_sched[0:temp.shape[0]]=temp
    ###################################            
 
    return player_x_data, own_class_pos_data, opp_class_pos_data, own_team_sched, opp_team_sched
 

### save tensors 
s=list(samples.college_id)
exclude=[]
for j in range(len(s)):
    print(j)
    boxscores=list(samp.loc[samp.player_href==s[j],'boxscore_href'])
    if len(boxscores)==0:
        exclude.append(s[j])
        continue
    
    if s[j]=='blake-lynch-1':
        continue
    player_x_data, own_class_pos_data, opp_class_pos_data, own_team_sched, opp_team_sched=make_x_data_tensor(s[j])
    y_train=torch.tensor(samples.loc[samples.college_id==s[j],cols+['snapcounts']].values)
    y_train=y_train.flatten().float()
    
    torch.save(player_x_data, 'saved_tensors_model2/x_player_data_'+s[j]+'.pt')
    torch.save(own_class_pos_data, 'saved_tensors_model2/x_own_class_pos_'+s[j]+'.pt')
    torch.save(opp_class_pos_data, 'saved_tensors_model2/x_opp_class_pos_'+s[j]+'.pt')
    torch.save(own_team_sched, 'saved_tensors_model2/x_own_team_sched_'+s[j]+'.pt')
    torch.save(opp_team_sched, 'saved_tensors_model2/x_opp_team_sched_'+s[j]+'.pt')    
    torch.save(y_train, 'saved_tensors_model2/y_'+s[j]+'.pt')
    
##player_x_data, own_class_pos_data, opp_class_pos_data, own_team_sched, opp_team_sched = make_x_data_tensor(player_id='eric-reid-1')
###################################           



### model training 
samples=samples.loc[samples.college_id!='blake-lynch-1',:]
samples=samples.loc[~samples.college_id.isin(exclude),:]

import nn_model
nn=nn_model.NeuralNetwork_v2(64,32,16)
loss = torch.nn.MSELoss()

#loss = torch.nn.GaussianNLLLoss()

optimizer = torch.optim.SGD(nn.parameters(), lr=1e-7)
scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=50, gamma=0.9)
#scheduler=torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer,mode='min',patience=5)

iters=500
for k in range(iters):
    loss_list=[]
    
    samples=samples.sample(frac=1)
    train_ids=list(samples.college_id[samples.first_year!=2021])
    test_ids=list(samples.college_id[samples.first_year==2021])
    nn.train()
    
    for j in range(len(train_ids)):
        
        player_x_data=torch.load('saved_tensors_model2/x_player_data_'+train_ids[j]+'.pt')
        own_class_pos_data=torch.load('saved_tensors_model2/x_own_class_pos_'+train_ids[j]+'.pt')
        opp_class_pos_data=torch.load('saved_tensors_model2/x_opp_class_pos_'+train_ids[j]+'.pt')
        own_team_sched=torch.load('saved_tensors_model2/x_own_team_sched_'+train_ids[j]+'.pt')
        opp_team_sched=torch.load('saved_tensors_model2/x_opp_team_sched_'+train_ids[j]+'.pt')
        y_train=torch.load('saved_tensors_model2/y_'+train_ids[j]+'.pt')
        
        player_x_data=player_x_data.float()
        own_class_pos_data=own_class_pos_data.float()
        opp_class_pos_data=opp_class_pos_data.float()
        own_team_sched=own_team_sched.float()
        opp_team_sched=opp_team_sched.float()
        
        ## predict
        x_vals=nn(player_x_data,own_class_pos_data,opp_class_pos_data,own_team_sched,opp_team_sched).float()
        loss_val=loss(x_vals,y_train)
        
        ## run sgd
        optimizer.zero_grad()
        loss_val.backward()
        optimizer.step()
        
        #print(train_ids[j])
        #print(loss_val)
        loss_list.append(loss_val.item())
        
    print('iteration: '+str(k)+' loss average: '+str(np.mean(loss_list)))
    scheduler.step()
    
        
    if (k+1) % 10 ==0:
        compare=pd.DataFrame(columns=cols+['snapcounts','player_id','type'])
        
        """ evaluate test data sets """    
        losses_test=[]
        
        loss_eval = torch.nn.MSELoss()
        #loss_eval = torch.nn.GaussianNLLLoss()
        
        nn.eval()
        for t in range(len(test_ids)):
            
            player_x_data_test=torch.load('saved_tensors_model2/x_player_data_'+test_ids[t]+'.pt')
            own_class_pos_data_test=torch.load('saved_tensors_model2/x_own_class_pos_'+test_ids[t]+'.pt')
            opp_class_pos_data_test=torch.load('saved_tensors_model2/x_opp_class_pos_'+test_ids[t]+'.pt')
            own_team_sched_test=torch.load('saved_tensors_model2/x_own_team_sched_'+test_ids[t]+'.pt')
            opp_team_sched_test=torch.load('saved_tensors_model2/x_opp_team_sched_'+test_ids[t]+'.pt')
            y_test=torch.load('saved_tensors_model2/y_'+test_ids[t]+'.pt')
            
            player_x_data_test=player_x_data_test.float()
            own_class_pos_data_test=own_class_pos_data_test.float()
            opp_class_pos_data_test=opp_class_pos_data_test.float()
            own_team_sched_test=own_team_sched_test.float()
            opp_team_sched_test=opp_team_sched_test.float()

            x_vals_test=nn(player_x_data_test,own_class_pos_data_test,opp_class_pos_data_test,own_team_sched_test,opp_team_sched_test).float()
            loss_val_eval=loss_eval(x_vals_test,y_test)
            losses_test.append(loss_val_eval.item())
            
            ## put together preds/actuals
            gg=x_vals_test.detach().numpy()
            gg=gg.reshape(-1, len(gg))
            x_pred=pd.DataFrame(gg,columns=cols+['snapcounts'])
            
            x_pred=x_pred*y_stds+y_means            
            x_pred['player_id']=test_ids[t]
            x_pred['type']='prediction'
            
            gg=y_test.detach().numpy()
            gg=gg.reshape(-1, len(gg))
            x_actual=pd.DataFrame(gg,columns=cols+['snapcounts'])
            
            x_actual=x_actual*y_stds+y_means            
            x_actual['player_id']=test_ids[t]
            x_actual['type']='actual'
            
            compare=pd.concat([compare,x_pred,x_actual])
            #############################
                        
        print('TEST DATA SET LOSS:')
        print(np.mean(losses_test))


