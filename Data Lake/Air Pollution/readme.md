All code authored by: Paulina Zal

To run this pieces of code you need to log in AWS account class 40209, Account ID: 3355-3129-7715 Federated user: voclabs/user2478657=paulina.zal94


The respective lambda functions (python code in Github:lambda name):<br>
  - Current_Airpollution.py:Current_Airpollution<br>
  - Air_Pollution_All_Data_.py:Air_Pollution_All_Data_<br>
  - Air_Pollution_Aggregated.py:Air_Pollution_Aggregated<br>

Every Lambda function has a layer containing: pandas, sqlalchemy==1.4.46, psycopg2-binary,pytz 

File History_AirPollution.py:
- requires Texas_Counties_Centroid_Map.csv in S3 bucket for reading coordinates
- run locally
- collect and insert historical data from api into S3 bucket
- credentials configurated in file config.py ( ###placeholder should be changed with credentials to an S3 bucket)

The order of running code: 

History_AirPollution.py > air_pollution_current.py > Air_Pollution_All_Data.py >Air_Pollution_Aggregated.py

air_pollution_current.py:
- the new data is collected from api
- requires Texas_Counties_Centroid_Map.csv in S3 bucket for reading coordinates
- credentials configurated in Environment variables for lambda function
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46
   
Air_Pollution_All_Data.py:
- requires a air_polution_cleaned_history_new.csv in S3 buket and run previously air_pollution_current.py (data to be found in the Data folder)
- concatenate the data from
- credentials configurated in Environment variables for lambda function
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46
  
Air_Pollution_Aggregated.py:
- requires previous run of Air_Pollution_All_Data.py
- aggregates the data set to one datapoint per day and calculates average value of measurements
- credentials configurated in Environment variables for lambda function
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46
