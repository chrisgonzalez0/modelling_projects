#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 14 09:03:27 2022

@author: chrisgonzalez
"""


import torch

x=torch.ones(1,20,17)
y=torch.zeros(1,20,17)


z=torch.cat([x,y],dim=0)

func=torch.nn.Linear(17, 6)
activ=torch.nn.Softmax(dim=0)

func2=torch.nn.Linear(20, 1)

result=func(z)

result_reshape=result.reshape( result.shape[0],result.shape[2],result.shape[1] )

result2=func2(result_reshape)



