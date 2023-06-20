All code authored by: Paulina Zal

To run this pieces of code you need to log in AWS account class 40209, user ...

The respective lambda functions (python code in Github:lambda name):<br>
  - air_pollution_current.py:Current_Airpollution<br>
  - Air_Pollution_All_Data.py:Air_Pollution_All_Data_<br>
  - Air_Pollution_Aggregated.py:Air_Pollution_Aggregated<br>

Every Lambda function has a layer containing: pandas, sqlalchemy==1.4.46, psycopg2-binary,pytz 

File History_with_class.py:
- run locally
- collect and insert historical data from api into S3 bucket

The order of running code: 
history_with_class.py > air_pollution_current.py > Air_Pollution_All_Data.py >Air_Pollution_Aggregated.py

air_pollution_current.py:
- the new data is collected from api
  
Air_Pollution_All_Data.py:
- requires a .csv in S3 buket and run previously air_pollution_current.py (data to be found in the Data folder)
- concatenate the data from 
  
Air_Pollution_Aggregated.py:
- requires previous run of Air_Pollution_All_Data.py
- aggregates the data set to one datapoint per day and calculates average value of measurements
  
