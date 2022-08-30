#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 31 13:09:44 2022

@author: chrisgonzalez
"""
import os
import torch
from torch import nn
from torch.utils.data import DataLoader
from torchvision import datasets, transforms


class NeuralNetwork(nn.Module):
    def __init__(self,games_out,opps_out,rows_out,cols_out):
        super(NeuralNetwork, self).__init__()
        
        self.layer1=nn.Linear(89, cols_out)
        self.layer2=nn.Linear(180, rows_out)
        self.layer3=nn.Linear(2, opps_out)
        self.layer4=nn.Linear(48, games_out)
        
        self.flatten = nn.Flatten()
        self.activation=nn.LogSoftmax(dim=3)
        self.dropout=nn.Dropout(0.1)
        
        self.layer5=nn.Linear(games_out*opps_out*rows_out*cols_out , 43)
        
    def forward(self, x):
        x=self.layer1(x)
        x=self.activation(x)
        x=self.dropout(x)
        
        x=x.reshape( [ x.shape[0],x.shape[1],x.shape[3],x.shape[2] ] )
        x=self.layer2(x)
        x=self.activation(x)
        x=self.dropout(x)
        
        x=x.reshape( [ x.shape[0],x.shape[3],x.shape[2],x.shape[1] ] )
        x=self.layer3(x)
        x=self.activation(x)
        x=self.dropout(x)
        
        x=x.reshape( [ x.shape[3],x.shape[1],x.shape[2],x.shape[0] ] )        
        x=self.layer4(x)
        x=self.activation(x)
        x=self.dropout(x)
        #print(x.shape)
        
        x=x.flatten()
        #print(x.shape)
        x=self.layer5(x)
        x=self.dropout(x)
        
        return x
        
"""    
layer1=nn.Linear(89, 16)
layer2=nn.Linear(180, 8)
layer3=nn.Linear(2, 5)
layer4=nn.Linear(48, 3)
activation=nn.Softmax()
"""


class NeuralNetwork_v2(nn.Module):
    def __init__(self,player_out,class_pos_out,team_sched_out):
        super(NeuralNetwork_v2, self).__init__()
        
        self.player_layer1=nn.Linear(73, player_out)
        self.class_pos_layer1=nn.Linear(39, class_pos_out)
        self.team_sched_layer1=nn.Linear(27, team_sched_out)
        
        
        self.flatten = nn.Flatten()
        self.activation=nn.LogSoftmax(dim=0)
        self.dropout=nn.Dropout(0.1)
        
        self.layer_final=nn.Linear( (player_out*48)+ (class_pos_out*48*130)*2 + (team_sched_out*48)*2 , 43)

        
    def forward(self, player_x_data, own_class_pos_data, opp_class_pos_data, own_team_sched, opp_team_sched):
        
        player=self.player_layer1(player_x_data)
        player=self.activation(player)
        player=self.dropout(player)
        player=player.flatten()
        
        own_class_pos=self.class_pos_layer1(own_class_pos_data)
        own_class_pos=self.activation(own_class_pos)
        own_class_pos=self.dropout(own_class_pos)
        own_class_pos=own_class_pos.flatten()
        
        opp_class_pos=self.class_pos_layer1(opp_class_pos_data)
        opp_class_pos=self.activation(opp_class_pos)
        opp_class_pos=self.dropout(opp_class_pos)
        opp_class_pos=opp_class_pos.flatten()

        own_team=self.team_sched_layer1(own_team_sched)
        own_team=self.activation(own_team)
        own_team=self.dropout(own_team)
        own_team=own_team.flatten()
        
        opp_team=self.team_sched_layer1(opp_team_sched)
        opp_team=self.activation(opp_team)
        opp_team=self.dropout(opp_team)
        opp_team=opp_team.flatten()

        result=self.layer_final( torch.cat( (player,own_class_pos,opp_class_pos,own_team,opp_team)  )  )
        
        return result





