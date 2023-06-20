import json 
import requests
import pandas as pd  
import boto3
import csv
import psycopg2
import os
from sqlalchemy import create_engine #sqlalchemy==1.4.46                                                       
import datetime


class S3ClientClass:
    def __init__(self, S3_BUCKET_NAME):
        self.client = boto3.client("s3")
        self.S3_BUCKET_NAME=S3_BUCKET_NAME
        
    
    def read_s3_bucket(self,object_key): 
    
        file_content = self.client.get_object(
            Bucket=self.S3_BUCKET_NAME, Key=object_key)["Body"]
        airpol_history = pd.read_csv(file_content)
        
        return airpol_history


class RDSClass:
    def __init__(self, RDS_USER,RDS_PASSWORD,RDS_HOST):
        self.RDS_USER =RDS_USER
        self.RDS_PASSWORD=RDS_PASSWORD
        self.RDS_HOST=RDS_HOST
    def read_rds(self,db,query_read):
        conn_string = "postgresql://"+self.RDS_USER+":"+self.RDS_PASSWORD+"@"+self.RDS_HOST+"/"+db
        db_sqlalchemy = create_engine(conn_string)
        conn_sqlalchemy = db_sqlalchemy.connect()
        connection = psycopg2.connect(user=self.RDS_USER,
                                      password=self.RDS_PASSWORD,
                                      host=self.RDS_HOST,
                                      port="5432",
                                      database=db) 
        cursor= connection.cursor()
    
       
        airpol_current= pd.read_sql(query_read,db_sqlalchemy)
        conn_sqlalchemy.invalidate()
        db_sqlalchemy.dispose()
        connection.close()
        return airpol_current 
    def write_rds(self,df, db,query,table):
        conn_string = "postgresql://"+self.RDS_USER+":"+self.RDS_PASSWORD+"@"+self.RDS_HOST+"/"+db
        db_sqlalchemy = create_engine(conn_string)
        conn_sqlalchemy = db_sqlalchemy.connect()

        connection = psycopg2.connect(user=self.RDS_USER,
                                      password=self.RDS_PASSWORD,
                                      host=self.RDS_HOST,
                                      port="5432",
                                      database=db) 
       
        cursor= connection.cursor()
      

        sql=query
        cursor.execute(query)
        connection.commit()
        cursor.execute("delete from "+table)
        connection.commit()
        
        df.to_sql(table, db_sqlalchemy,  if_exists= 'append', index=False)
        conn_sqlalchemy.invalidate()
        db_sqlalchemy.dispose()
        connection.close()
       
def clean(data_2022,gun_scraped,data_gap):
     

    
    data_gap=data_gap.rename(columns={"Incident ID":"Incident_Id","Incident Date":"Incident Date","State":"state", "City Or County":"city-county","Address":"address",'# Killed':'killed', '# Injured':'injured', 'Operations':'operations'})#
    gun_scraped=gun_scraped.rename(columns={"incident_id":"Incident_Id","incident_date":"Incident Date","city_or_county":"city-county",'killed':'killed', 'injured':'injured', 'Operations':'operations'})
  # data_2022=data_2022.rename(columns={"Incident_Id":"incident id","Incident Date":"incident date","city-county":"county_city",'killed':'# killed', 'injured':'# injured'})


    data_gap["Incident Date"]=data_gap["Incident Date"].apply(lambda x: datetime.datetime.strptime(x, "%B %d, %Y").date())
    
    data=pd.concat([data_gap,gun_scraped],axis=0)
    data_tx=data[data["state"]=="Texas"]
   # print(data_tx.state.unique())
    data_tx=data_tx[["Incident_Id","Incident Date","city-county",'killed','injured']]
    guns=pd.concat([data_2022,data_tx],axis=0)
    guns.drop_duplicates(inplace=True)

    
    return guns

def lambda_handler(event, context):
 
    S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
   
##rds
    RDS_USER = os.environ["RDS_USER"]
    RDS_PASSWORD = os.environ["RDS_PASSWORD"]
    RDS_HOST=os.environ["RDS_HOST"]
    RDS_DB = os.environ["RDS_DB"] 
   
    S3=S3ClientClass(S3_BUCKET_NAME)
    RDS=RDSClass(RDS_USER,RDS_PASSWORD,RDS_HOST)
    
    data_2022=S3.read_s3_bucket("texas_clean_gun_cw.csv")
    data_gap=S3.read_s3_bucket("Gun_Violance_gap.csv") 
    query_read="select * from gun_violance"
    gun_scraped=RDS.read_rds(RDS_DB,query_read)
    
    guns=clean(data_2022,gun_scraped,data_gap)
    print(guns)
    sql = '''CREATE TABLE IF NOT EXISTS gun_violance_texas ("Incident_Id" int, "Incident Date" date,  "city-county" varchar(40),
        "killed" int , "injured" int );'''

    RDS.write_rds(guns,RDS_DB,sql,"gun_violance_texas")

    return { 
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
