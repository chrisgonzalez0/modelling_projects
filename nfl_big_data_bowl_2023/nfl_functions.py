#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 12 11:57:26 2022

@author: chrisgonzalez
"""

import pandas as pd 
import matplotlib.pyplot as plt



games=pd.read_csv('data/games.csv')
scouting=pd.read_csv('data/pffScoutingData.csv')
players=pd.read_csv('data/players.csv')
plays=pd.read_csv('data/plays.csv')


week=pd.read_csv('data/week1.csv')


gg=week.groupby('playId').max('frameId')

gg=week.loc[week.nflId.isna(),:]



events 
'None', 'ball_snap', 'autoevent_passforward', 'pass_forward',
       'autoevent_ballsnap', 'line_set', 'play_action', 'pass_arrived',
       'autoevent_passinterrupted', 'fumble', 'fumble_offense_recovered',
       'qb_sack', 'run', 'man_in_motion', 'pass_outcome_caught',
       'pass_outcome_incomplete', 'pass_tipped', 'qb_strip_sack', 'shift',
       'first_contact', 'huddle_break_offense', 'lateral', 'handoff']




from PIL import Image
image = Image.open('supporting_files/football_field.jpg')
new_image = image.resize((120, 54))
new_image.save('supporting_files/myimage_500.jpg')

#plt.figure(figsize = (120,53.3),dpi=80)
im = plt.imread('supporting_files/football_field.jpg')

implot = plt.imshow(im,cmap='gray',zorder=1,origin='lower')

# put a blue dot at (10, 20)
#plt.scatter([10,10], [20,50],zorder=2)
plt.scatter((week.x[(week.playId==97) & (week.frameId==6) & (week.team=='TB')]/120)*883,
            (week.y[(week.playId==97) & (week.frameId==6) & (week.team=='TB')]/53.3)*359,
            zorder=2,c='#D50A0A',edgecolors='#FF7900')
plt.scatter((week.x[(week.playId==97) & (week.frameId==6) & (week.team=='DAL')]/120)*883,
            (week.y[(week.playId==97) & (week.frameId==6) & (week.team=='DAL')]/53.3)*359,
            zorder=2,c='b')

plt.show()

# put a red dot, size 40, at 2 locations:
plt.scatter(x=[30, 40], y=[30, 200], c='r', s=40,zorder=2)
plt.show()



(week.x[(week.playId==97) & (week.frameId==6)]/120)*883
(week.y[(week.playId==97) & (week.frameId==6)]/53.3)*359


53.3  359

120  883

