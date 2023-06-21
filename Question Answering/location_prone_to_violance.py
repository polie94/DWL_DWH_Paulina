#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 23:36:07 2023

@author: paulina
"""
""" this script is a simple data analysis for finding regions where the gun violance o
    occurce more often than in others"""

import pandas as pd
import statsmodels.api as sm
import numpy as np
from patsy import dmatrices
import matplotlib.pyplot as plt
import seaborn as sn

def bool_incident(x):
    if  x >0:
        return 1
    else:
        return 0  

def incidents(x):
    if not x >0:
        return 0
    else:
        return x
    
    
    
### read data downloaded from lake 
gun=pd.read_csv("Gun_Renamed.csv")
cities=pd.read_csv("uscities.csv")
weather=pd.read_csv("weather_data.csv")
air=pd.read_csv("air_new.csv")
air.county=air.county.apply(lambda x: x.strip())
cities=cities[cities["state_id"]=="TX"]
gun_cols=gun.columns


#join data
gun=gun.rename(columns={'Incident_Id':"incident id", 'Incident Date':'incident date', 'city-county':"county_city", 'killed':'# killed', 'injured':'# injured'})

gun=pd.merge(left=gun,right=cities,how="inner",left_on="county_city",right_on="city",)
gun=gun[["incident id",'incident date', '# killed', '# injured',"county_name"]]
gun=gun.groupby(['incident date',"county_name"])["incident id"].count().reset_index() # takenumber of incidents for lacationa and date

gun=pd.merge(left=weather,right=gun,how="left",left_on=["county","date"],right_on=["county_name","incident date"])
gun=gun.drop(['county_name', 'incident date'],axis=1)

gun=pd.merge(left=air,right=gun,how="left",left_on=["county","Date"],right_on=["county","date"],)
#gun=gun.drop(['lat', 'lon',  'unix_dt', 'Datetime', 'Hour'],axis=1)
gun=gun.drop(['lat', 'lon'],axis=1)

gun=gun.rename(columns={"incident id": "incident"})

gun['incident'] = gun.incident.apply(lambda x:incidents(x)) #### wenn kein incident then dann 0

#analysis for 2022
gun = gun[gun["Date"]<'2023-01-01']

temp=gun.dropna(subset=['temp_min', 'temp_max', 'app_temp_min', 'app_temp_max', 'precipitation','precipitation_hours', 'snowfall'])


# convert number of incidents to 0 or 1 - wheather there was an incident or not
temp["bool_incident"]=temp.incident.apply(lambda x :bool_incident(x))

#avg over date for date
mean=np.mean(temp.groupby(["Date"])["bool_incident"].mean())
#group data 
mean_county_date=temp.groupby([ "county","date"])['bool_incident'].agg(['mean']).reset_index()
mean_county=mean_county_date.groupby([ "county"])['mean'].agg(['mean']).reset_index()
# find counties where the mean is higher than the average
over_mean=mean_county[mean_county["mean"]>mean].sort_values(by="mean",ascending=False)
over_mean.set_index("county", inplace=True)
ax=over_mean.plot(kind="bar")
ax.set_ylabel("incident per day")
plt.show()
