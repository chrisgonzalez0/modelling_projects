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
        self.activation=nn.Softmax()
        
        self.layer5=nn.Linear(games_out*opps_out*rows_out*cols_out , 43)
        
    def forward(self, x):
        x=self.layer1(x)
        x=self.activation(x)
        
        x=x.reshape( [ x.shape[0],x.shape[1],x.shape[3],x.shape[2] ] )
        x=self.layer2(x)
        x=self.activation(x)
        
        x=x.reshape( [ x.shape[0],x.shape[3],x.shape[2],x.shape[1] ] )
        x=self.layer3(x)
        x=self.activation(x)
        
        x=x.reshape( [ x.shape[3],x.shape[1],x.shape[2],x.shape[0] ] )        
        x=self.layer4(x)
        x=self.activation(x)
        print(x.shape)
        
        x=x.flatten()
        print(x.shape)
        x=self.layer5(x)
        
        return x
        
"""    
layer1=nn.Linear(89, 16)
layer2=nn.Linear(180, 8)
layer3=nn.Linear(2, 5)
layer4=nn.Linear(48, 3)
activation=nn.Softmax()
"""