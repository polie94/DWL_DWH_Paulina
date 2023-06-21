#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 23:39:32 2023

@author: paulina
"""


import pandas as pd
import statsmodels.api as sm
import numpy as np
from patsy import dmatrices
import matplotlib.pyplot as plt
import seaborn as sn
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import numpy as np
from sklearn.metrics import mean_squared_error

def incidents(x):
    if not x >0:
        return 0
    else:
        return x

def temp_range(x):
    if x<0:
        return 0
    if x>=0 and x<10:
        return 1
    if x>=10 and x<20:
        return 2
    if x>=20 and x<30:
        return 3
    if x>=30 and x<40:
        return 4
    if x>=40 :
        return 5
        
def rein_dur(x):
    
    if x>=0 and x<4:
        return 0
    if x>=4 and x<8:
        return 1
    if x>=8 and x<12:
        return 2
    if x>=12 and x<16:
        return 3
    if x>=16 and x<20:
        return 4
    if x>=20 and x<24:
        return 5
def bool_incident(x):
    if  x >0:
        return 1
    else:
        return 0               

gun=pd.read_csv("Gun_Renamed.csv")
cities=pd.read_csv("uscities.csv")
weather=pd.read_csv("weather_data.csv")
air=pd.read_csv("air_new.csv")
air.county=air.county.apply(lambda x: x.strip())
cities=cities[cities["state_id"]=="TX"]
gun_cols=gun.columns

gun=gun.rename(columns={'Incident_Id':"incident id", 'Incident Date':'incident date', 'city-county':"county_city", 'killed':'# killed', 'injured':'# injured'})
gun=pd.merge(left=gun,right=cities,how="inner",left_on="county_city",right_on="city",)
gun=gun[["incident id",'incident date', '# killed', '# injured',"county_name"]]
gun=gun.groupby(['incident date',"county_name"])["incident id"].count().reset_index()

gun=pd.merge(left=weather,right=gun,how="left",left_on=["county","date"],right_on=["county_name","incident date"])
gun=gun.drop(['county_name', 'incident date'],axis=1)

gun=pd.merge(left=air,right=gun,how="left",left_on=["county","Date"],right_on=["county","date"],)
gun=gun.drop(['lat', 'lon'],axis=1)

gun=gun.rename(columns={"incident id": "incident"})
gun = gun[gun["Date"]<'2023-01-01']

gun['incident'] = gun.incident.apply(lambda x:incidents(x)) #### wenn NAN dann 0

gun["temp_range"]=gun.temp_max.apply(lambda x: temp_range(x))
gun["rein_dur"]=gun.precipitation_hours.apply(lambda x: rein_dur(x))
gun=gun[['county', 'aqi','incident', 'temp_range', 'rein_dur']]
from sklearn.tree import DecisionTreeRegressor

# Create a random mask
mask = np.random.rand(len(gun)) < 0.6
# Split the data based on the mask
train_data = gun[mask]
test_data = gun[~mask]
train_data["was_incident"]=train_data.incident.apply(lambda x :bool_incident(x))
test_data["was_incident"]=test_data.incident.apply(lambda x :bool_incident(x))

#group train and train data
train_data = train_data.groupby(['aqi', "county","temp_range","rein_dur"])['was_incident'].agg(['sum', 'count']).reset_index()
train_data["probability"] = train_data["sum"]/train_data["count"]
test_data = test_data.groupby(['aqi', "county","temp_range","rein_dur"])['was_incident'].agg(['sum', 'count']).reset_index()
test_data["probability"] = test_data["sum"]/test_data["count"]


# Define the categorical columns
categorical_columns = ["county"]

# Define the numerical columns (if any)
numerical_columns = [ 'aqi', 'temp_range', 'rein_dur']

# Define the target variable column
target_column = "probability"

# Define the preprocessing steps
preprocessor = ColumnTransformer(
    transformers=[
        ('cat', OneHotEncoder(), categorical_columns),
        ('num', 'passthrough', numerical_columns)
    ])

# Create the pipeline with the preprocessing and regression steps
pipeline = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('regressor', DecisionTreeRegressor())
])

X_train=train_data[['aqi', 'county', 'temp_range', 'rein_dur']]
y_train=train_data[['probability']]

X_test=test_data[['aqi', 'county', 'temp_range', 'rein_dur']]
y_test=test_data[['probability']]
# Fit the pipeline on the data
pipeline.fit(X_train, y_train)

# Predict using the fitted model
y_pred = pipeline.predict(X_test)

mean_squared_error(y_test,y_pred)


