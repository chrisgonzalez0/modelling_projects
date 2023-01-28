#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 12 11:57:26 2022

@author: chrisgonzalez
"""

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
pd.options.mode.chained_assignment = None
import os
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from textwrap import wrap
from IPython.display import HTML

## load data and join for training ##
games=pd.read_csv('data/games.csv')
scouting=pd.read_csv('data/pffScoutingData.csv')
players=pd.read_csv('data/players.csv')
plays=pd.read_csv('data/plays.csv')
plays=plays.loc[plays.foulName1.isna(),:]
plays.loc[:,'normed_playResult']=plays.loc[:,'playResult']
y_means=plays.normed_playResult.mean()
y_std=plays.normed_playResult.std()
plays.loc[:,'normed_playResult']=(plays.loc[:,'normed_playResult']-y_means)/y_std
plays.loc[:,'key_value']=plays.loc[:,'playId'].astype(str)+plays.loc[:,'gameId'].astype(str)

week=pd.read_csv('data/week1.csv')
for i in range(2,9):
    week=pd.concat([week,
                    pd.read_csv('data/week'+str(i)+'.csv')],
                   axis=0)
week=week.loc[(week.x >= 0) & (week.x <= 120) & (week.y >= 0) & (week.y <= 53.3),:]

colors=pd.read_csv('supporting_files/team_colors.csv')

week=pd.merge(week,colors,'left',left_on='team',right_on='team')
week=pd.merge(week,players.loc[:,['nflId','weight','officialPosition']],'left',left_on='nflId', right_on='nflId')
week.loc[week.officialPosition.isna(),'officialPosition']=''

week=pd.merge(week,scouting.loc[:,['nflId','gameId','playId','pff_positionLinedUp']],'left',
              left_on=['nflId','gameId','playId'], 
              right_on=['nflId','gameId','playId'])
week.loc[week.pff_positionLinedUp.isna(),'pff_positionLinedUp']=''


## make all plays go the same direction
week.x.loc[week.playDirection=='left']=120-week.x.loc[week.playDirection=='left']
week.y.loc[week.playDirection=='left']=53.3-week.y.loc[week.playDirection=='left']
week.dir.loc[week.playDirection=='left']=((week.dir.loc[week.playDirection=='left']-360)-180)%360

## make pixel lengths and widths smaller for faster processing
week.x=round(week.x,0)
week.y=round(week.y,0)


print(week.shape)
week.head()



## get tensor x, y data function ##
import torch 
def get_tensors(gameId,playId,frameId):
    
    #gameId=2021101703
    #playId=1779
    #frameId=53
    
    temp=week.loc[(week.gameId==gameId) & (week.playId==playId) & (week.frameId==frameId),:]
    temp.loc[temp.team=='football','weight']=1
    temp.loc[:,'force']=temp.weight*temp.a
    temp.loc[temp.team=='football','force']=1
    temp.loc[temp.team!='football','force']=(temp.loc[temp.team!='football','force']-temp.force[temp.team!='football'].mean())/temp.force[temp.team!='football'].std()
    
    layers=['offense','football','defense']
    x_data=torch.zeros([3,54,121])
    
    for j in range(len(layers)):
        tens_temp=x_data[j]
        
        if layers[j]=='offense':
            temp_j=temp.loc[temp.team==plays.possessionTeam[(plays.gameId==gameId) & (plays.playId==playId)].item(),:]
            temp_j=temp_j.reset_index(drop=True)
        if layers[j]=='defense':
            temp_j=temp.loc[temp.team==plays.defensiveTeam[(plays.gameId==gameId) & (plays.playId==playId)].item(),:]
            temp_j=temp_j.reset_index(drop=True)        
        if layers[j]=='football':
            temp_j=temp.loc[temp.team=='football',:]
            temp_j=temp_j.reset_index(drop=True)
        
        for i in range(temp_j.shape[0]):
            tens_temp[int(temp_j.y[i].item()),int(temp_j.x[i].item())]=temp_j.force[i]
            #print(tens_temp[int(temp_j.y[i].item()*10),int(temp_j.x[i].item()*10)])
        
        x_data[j]=tens_temp
    
    y_data=torch.tensor([plays.normed_playResult[(plays.gameId==gameId) & (plays.playId==playId)].item()])
    
    return x_data,y_data

##############################################


### LeNet Function for Image Classification 
from torch.nn import Module
from torch.nn import Conv2d
from torch.nn import Linear
from torch.nn import MaxPool2d
from torch.nn import ReLU
from torch.nn import LogSoftmax
from torch import flatten
from torch import squeeze, unsqueeze


class LeNet(Module):
	def __init__(self, numChannels, classes):
		# call the parent constructor
		super(LeNet, self).__init__()
		# initialize first set of CONV => RELU => POOL layers
		self.conv1 = Conv2d(in_channels=numChannels, out_channels=20,
			kernel_size=(2, 2))
		self.relu1 = ReLU()
		self.maxpool1 = MaxPool2d(kernel_size=(2, 2), stride=(2, 2))
		# initialize second set of CONV => RELU => POOL layers
		self.conv2 = Conv2d(in_channels=20, out_channels=40,
			kernel_size=(2, 2))
		self.relu2 = ReLU()
		self.maxpool2 = MaxPool2d(kernel_size=(2, 2), stride=(3, 3))
		# initialize first (and only) set of FC => RELU layers
		self.fc1 = Linear(in_features=6400, out_features=500)
		self.relu3 = ReLU()
		# initialize our softmax classifier
		self.fc2 = Linear(in_features=500, out_features=classes)
		self.logSoftmax = LogSoftmax(dim=1)
        
	def forward(self, x):
		# pass the input through our first set of CONV => RELU =>
		# POOL layers
		x = self.conv1(x)
		x = self.relu1(x)
		x = self.maxpool1(x)
		# pass the output from the previous layer through the second
		# set of CONV => RELU => POOL layers
		x = self.conv2(x)
		x = self.relu2(x)
		x = self.maxpool2(x)
		# flatten the output from the previous layer and pass it
		# through our only set of FC => RELU layers
		x = flatten(x)
		x = self.fc1(x)
		x = self.relu3(x)
		# pass the output to our softmax classifier to get our output
		# predictions
		x = self.fc2(x)
		output = x
		# return the output predictions
		return output        

##############################################


## save training tensors locally ##
play_frames=week.loc[:,['gameId','playId','frameId'] ].drop_duplicates().reset_index(drop=True)
play_frames.loc[:,'key_value']=play_frames.loc[:,'playId'].astype(str)+play_frames.loc[:,'gameId'].astype(str)
play_frames=play_frames.loc[play_frames.key_value.isin(plays.key_value),:]
play_frames=play_frames.sample(frac=1).reset_index(drop=True)
#play_frames=play_frames.iloc[0:1000,:]
train_samp=200000
for i in range(train_samp):
    x,y=get_tensors( play_frames.gameId[i] , play_frames.playId[i] , play_frames.frameId[i] )
    torch.save(x,'data/tensors/x_'+str(play_frames.gameId[i])+str(play_frames.playId[i])+str(play_frames.frameId[i])+'.pt')
    torch.save(y,'data/tensors/y_'+str(play_frames.gameId[i])+str(play_frames.playId[i])+str(play_frames.frameId[i])+'.pt')
train_frames=play_frames.iloc[0:train_samp,:]
test_frames=play_frames.iloc[train_samp:play_frames.shape[0],:]

train_frames.to_csv('train_frames.csv',index=False)
test_frames.to_csv('test_frames.csv',index=False)

##############################################



## run gradient descent on model, analyze training/testing loss ##
n_iters=10
model = LeNet(numChannels=3,classes=1)

loss = torch.nn.MSELoss()
optimizer = torch.optim.SGD(model.parameters(), lr=1e-5)

for i in range(n_iters):
    model.train()
    print('i = ' + str(i) )
    loss_list=[]
    
    train_frames=train_frames.sample(frac=1).reset_index(drop=True)
    
    for j in range( train_frames.shape[0] ):
                    
        #x,y=get_tensors( play_frames.gameId[j] , play_frames.playId[j] , play_frames.frameId[j] )
        x=torch.load('data/tensors/x_'+str(play_frames.gameId[j])+str(play_frames.playId[j])+str(play_frames.frameId[j])+'.pt')
        y=torch.load('data/tensors/y_'+str(play_frames.gameId[j])+str(play_frames.playId[j])+str(play_frames.frameId[j])+'.pt')
            
        pred=model(x)
        loss_val=loss(pred*y_std+y_means,y*y_std+y_means)
        
        ## run sgd
        optimizer.zero_grad()
        loss_val.backward()
        optimizer.step()
        
        loss_list.append(loss_val.item())
                    
    print('iteration: '+str(i)+' total loss average: '+str(np.mean(loss_list)))
    torch.save(model,'lenet.pt')
    
    test_frames=test_frames.sample(frac=1).reset_index(drop=True)
    test_loss=[]
    model.eval()
    for k in range(1000):
        x,y=get_tensors( test_frames.gameId[k] , test_frames.playId[k] , test_frames.frameId[k] )
                
        pred=model(x)
        test_loss.append(loss(pred*y_std+y_means,y*y_std+y_means).item())
        
    print('test iteration: '+str(k)+' total loss average: '+str(np.mean(test_loss)))
    
##############################################


import pickle
with open('y_means.pkl', 'wb') as outp:
    pickle.dump(y_means, outp)
    
with open('y_std.pkl', 'wb') as outp:
    pickle.dump(y_std, outp)
    

## Integrated Gradients Testing ##
from captum.attr import IntegratedGradients
ig = IntegratedGradients(model)
zz=torch.rand(1,3,54,121)
imp_m = ig.attribute(inputs=x,
                     baselines=zz,
                     n_steps=15,internal_batch_size=3)


b = imp_m != 0
indices = b.nonzero()
imp_m[imp_m!=0]    
    
##############################################
    


## define the Animation function ## 

def animate(gameid,playid):
    fig, ax = plt.subplots(dpi=4000/16)
    im = plt.imread('/kaggle/input/nflbigdatabowl/football_field.jpg')
    _ = plt.imshow(im,cmap='gray',zorder=1,origin='lower',extent=[0,120,0,54])
    
    s=ax.scatter([],[])
    
    ax.set_title("\n".join(wrap(
        plays.loc[(plays.gameId==gameid) & (plays.playId==playid), 'playDescription'].item()
                                ,50)),fontsize=6)
    ax.set_xlabel("\n".join(wrap(        
        str(plays.loc[(plays.gameId==gameid) & (plays.playId==playid), 'down'].item())+' and '
        +str(plays.loc[(plays.gameId==gameid) & (plays.playId==playid), 'yardsToGo'].item())+' at '
        +str(plays.loc[(plays.gameId==gameid) & (plays.playId==playid), 'yardlineSide'].item())+' '
        +str(plays.loc[(plays.gameId==gameid) & (plays.playId==playid), 'yardlineNumber'].item())        
                                ,50)),fontsize=6
                 )

    ax.set_ylabel("\n".join(wrap(        
        'Offense: '+str(plays.loc[(plays.gameId==gameid) & (plays.playId==playid), 'possessionTeam'].item())+'\n'
        +'Defense: '+str(plays.loc[(plays.gameId==gameid) & (plays.playId==playid), 'defensiveTeam'].item())
                                ,50)),fontsize=6
                 )
    
    dataset=week.loc[(week.playId==playid) & (week.gameId==gameid),:]
    #dataset.loc[:,'x']=dataset.loc[:,'x']*883/120
    #dataset.loc[:,'y']=dataset.loc[:,'y']*359/53.3
    dataset.loc[:,'sizes']=90
    dataset.loc[dataset.team=='football','sizes']=7
    
    dataset['dir1']=np.sin(dataset.dir*np.pi/180)
    dataset['dir2']=np.cos(dataset.dir*np.pi/180)
    qr = ax.quiver(dataset.loc[dataset.frameId==(1),'x'], 
                   dataset.loc[dataset.frameId==(1),'y'], 
                   dataset.loc[dataset.frameId==(1),'dir1'], 
                   dataset.loc[dataset.frameId==(1),'dir2'], 
                   color='black')
        
    frames=dataset.frameId.unique()
    fe=dataset.loc[:,['frameId','event']].drop_duplicates()  ## frame by event that occurred
    fe=fe.reset_index(drop=True)
    
    title = ax.text(0.5,0.85, "", bbox={'facecolor':'w', 'alpha':0.5, 'pad':5},
                transform=ax.transAxes, ha="center")
    
    init_df=dataset.loc[dataset.frameId==1,:].reset_index(drop=True)
    
    annotation = [ax.annotate(
        np.array(init_df.loc[j,'officialPosition']).item(), 
        xy=(np.array(init_df.loc[j,['x','y'] ])), va='center', ha='center',fontsize=4,color='white') for j in range(init_df.shape[0])]

    def init():
        #plt.xlim(0, 120)
        #plt.ylim(0, 53.3) 
        ax.axis([min(dataset.x), max(dataset.x), 0 , 54])        
        s=ax.scatter([],[])        
        return s,

    def plot_play(i):
        frame_df=np.array(dataset.loc[dataset.frameId==(i),['x','y']])
        s.set_offsets(dataset.loc[dataset.frameId==(i),['x','y']])
        s.set_color(dataset.loc[dataset.frameId==(i), 'color_1' ])
        s.set_edgecolors(dataset.loc[dataset.frameId==(i), 'color_2' ])
        s.set_sizes(dataset.loc[dataset.frameId==(i), 'sizes' ])
        
        [annotation[j].set_position(tuple(frame_df[j]) ) for j in range(len(frame_df))]
        event=fe.loc[fe.frameId==(i),'event'].item()        
        if event!='None':
            title.set_text(fe.loc[fe.frameId==(i),'event'].item() )
            
        qr.set_offsets(dataset.loc[dataset.frameId==(i),['x','y']])
        qr.set_UVC( dataset.loc[dataset.frameId==(i),'dir1'] ,
                    dataset.loc[dataset.frameId==(i),'dir2'] )
        qr.set_color(dataset.loc[dataset.frameId==(i),'color_1'])
        return s,title,qr,

    return animation.FuncAnimation(fig,
                                  plot_play, 
                                  frames=frames,
                                  blit=True,
                                  init_func=init,
                                  interval=350
                                  )
##############################################    



