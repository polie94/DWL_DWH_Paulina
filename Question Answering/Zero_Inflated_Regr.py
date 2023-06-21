#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 20:02:51 2023

@author: paulina
"""

import pandas as pd
import statsmodels.api as sm
import numpy as np
from patsy import dmatrices
import matplotlib.pyplot as plt
import seaborn as sn
import datetime

def incidents(x):
    if not x >0:
        return 0
    else:
        return x
##read data
gun=pd.read_csv("Gun_Renamed.csv")
cities=pd.read_csv("uscities.csv")
weather=pd.read_csv("weather_data.csv")
air=pd.read_csv("air_new.csv")
air.county=air.county.apply(lambda x: x.strip())
cities=cities[cities["state_id"]=="TX"]
gun_cols=gun.columns

#prepare data

gun=gun.rename(columns={'Incident_Id':"incident id", 'Incident Date':'incident date', 'city-county':"county_city", 'killed':'# killed', 'injured':'# injured'})

gun=pd.merge(left=gun,right=cities,how="inner",left_on="county_city",right_on="city",)
gun=gun[["incident id",'incident date', '# killed', '# injured',"county_name"]]
gun=gun.groupby(['incident date',"county_name"])["incident id"].count().reset_index() #calculate nr of incidents
gun=pd.merge(left=weather,right=gun,how="left",left_on=["county","date"],right_on=["county_name","incident date"])
gun=gun.drop(['county_name', 'incident date'],axis=1)
gun=pd.merge(left=air,right=gun,how="left",left_on=["county","Date"],right_on=["county","date"],)
gun=gun.drop(['lat', 'lon', ],axis=1)
gun=gun.rename(columns={"incident id": "incident"})
gun['incident'] = gun.incident.apply(lambda x:incidents(x))
gun = gun[gun["Date"]<'2023-01-01']



#Zero inflated regression for  air pollution  research question 2

y=gun[['incident']]
x=gun[['aqi','co', 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3']]

zip_model=sm.ZeroInflatedPoisson(y, x,x,inflation='logit').fit()
print("Zero Inflated Regression Air Polution")
print(zip_model.summary())



####Zero inflated regression weather research question 1 
temp=gun.dropna(subset=['temp_min', 'temp_max', 'app_temp_min', 'app_temp_max', 'precipitation','precipitation_hours', 'snowfall'])
#temp1=temp[temp["county"]=="Cameron"]

y=temp[['incident']]
x=temp[['temp_min', 'temp_max', 'app_temp_min', 'app_temp_max', 'precipitation','precipitation_hours', 'snowfall']]

zip_model=sm.ZeroInflatedPoisson(y, x,x).fit()
print("Zero Inflated Regression Weather")
print(zip_model.summary())
