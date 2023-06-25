#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 06:05:25 2023

@author: paulina
"""
                            
import json 
import requests
import pandas as pd
import boto3
import csv
import psycopg2
import os 
from sqlalchemy import create_engine
from time import sleep
import configs
import datetime


class S3Client:
    def __init__(self, ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN, S3_BUCKET_NAME):
        self.client = boto3.client("s3", aws_access_key_id=ACCESS_KEY_ID,
                                   aws_secret_access_key=SECRET_ACCESS_KEY,
                                   aws_session_token=SESSION_TOKEN)
        self.ACCESS_KEY_ID=ACCESS_KEY_ID
        self.SECRET_ACCESS_KEY=SECRET_ACCESS_KEY
        self.SESSION_TOKEN=SESSION_TOKEN
        self.S3_BUCKET_NAME=S3_BUCKET_NAME
        
    
    def read_s3_bucket(self, object_key):
        file_content = self.client.get_object(
            Bucket=self.S3_BUCKET_NAME, Key=object_key)["Body"]
        location = pd.read_csv(file_content)
        loc_info = location[['CNTY_NM', 'X (Lat)', 'Y (Long)']]
        loc_info["lat_round"] = loc_info['X (Lat)'].apply(lambda x: round(x, 2))
        loc_info["lng_round"] = loc_info['Y (Long)'].apply(lambda x: round(x, 2))
        return loc_info
    
    def write_s3(self, df, object_key):
        df.to_csv(f's3://{self.S3_BUCKET_NAME}/{object_key}', index=False, storage_options={
                  'key': self.ACCESS_KEY_ID, 'secret': self.SECRET_ACCESS_KEY, "token": self.SESSION_TOKEN})

class OpenWeatherMap:
    def __init__(self, API_KEY):
        self.API_KEY = API_KEY
    
    def read_api(self, lat, lon, county):
        """ read data from API for a given county, the response from API for a given location between start 
        and end timestamps return a list with JSON for every hour in this time range. This needs to be flattened out. 
        The data are then stored in a common list of dictionaries"""
      
        query = {'lat': lat, 'lon': lon, 'appid': self.API_KEY, "start": "1640991600", "end": "1681116257"}
        sleep(1.1)
        dictionaries = []
        try:
            response = requests.get('http://api.openweathermap.org/data/2.5/air_pollution/history', params=query)
            json = response.json()
            for i in range(0, len(json["list"])):
                dictionary_temp = dict()
                aqi = json['list'][i]["main"]
                conditions = json['list'][i]["components"]
                unix_dt = json['list'][i]["dt"]
                location = {"lat": lat, "lon": lon, "county": county}
                dictionary_temp.update(location)
                dictionary_temp.update(aqi)
                dictionary_temp.update(conditions)
                dictionary_temp.update({"unix_dt": unix_dt})
                dictionaries.append(dictionary_temp)
        except:
            print("No Data")
        return dictionaries
    
    def call_api(self, location):
        """ iterate over counties coordinates and get data using the function read_api
          and write data to pandas data frame """
      
        dictionaries = []
        for index, row in location.iterrows():
            position = self.read_api(row["lat_round"], row["lng_round"], row["CNTY_NM"])
            dictionaries.extend(position)
        df_airpolution = pd.DataFrame(dictionaries)
        return df_airpolution
    
    @staticmethod
    def filter_time(df):
                df["Datetime"] = pd.to_datetime(df["unix_dt"], unit="s", utc=True)
                df["Datetime"] = df.Datetime.dt.tz_convert('America/Chicago')
                df["Datetime"] = df.Datetime.apply(lambda x: x.replace(tzinfo=None))
                df['Date'] = df['Datetime'].dt.date
                df['Hour'] = df['Datetime'].dt.time
                df = df[(df["Hour"] == datetime.time(1, 00)) |(df["Hour"] == datetime.time(4, 00)) | 
                        (df["Hour"] == datetime.time(7, 00)) |(df["Hour"] == datetime.time(10, 00)) |
                        (df["Hour"] == datetime.time(13, 00))|(df["Hour"] == datetime.time(16, 00)) |
                        (df["Hour"] == datetime.time(19, 00))|(df["Hour"] == datetime.time(22, 00))]
                return df #df.drop(["Datetime","Date","Hour"],axis=1)
        

def main():   
   API_KEY=configs.API_KEY
   S3_BUCKET_NAME = configs.S3_BUCKET_NAME
   ACCESS_KEY_ID=configs.ACCESS_KEY_ID
   SECRET_ACCESS_KEY=configs.SECRET_ACCESS_KEY
   SESSION_TOKEN=configs.SESSION_TOKEN
   OBJECT_KEY = configs.OBJECT_KEY
    
   S3Class=S3Client( ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN,S3_BUCKET_NAME)
   OpenWeatherMapClass= OpenWeatherMap(API_KEY)
   location=S3Class.read_s3_bucket(OBJECT_KEY) # read coordinates of all counties
   data=OpenWeatherMapClass.call_api(location) # send requests to API for coordinates
   #df=OpenWeatherMapClass.filter_time(data) #not used
   S3Class.write_s3(data,"air_polution_cleaned_history_new.csv")  # write as CSV file in an S3 bucket
   print(data)
    
if __name__ == "__main__":
    main()
    
