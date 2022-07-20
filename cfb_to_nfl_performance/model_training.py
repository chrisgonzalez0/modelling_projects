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


engine.dispose()
del(engine)



