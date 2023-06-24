All code authored by: Mirko Dimitrijevic

To run this pieces of code you need to log in AWS account class 40209, User: voclabs/user2428376=Dimitrijevic,_Mirko, Account ID: 550319664291

The Lambda function in the DailyWeatherAPI.py file has a layer containing: pandas, sqlalchemy==1.4.46

File CREATE TABLE weather_data:
- run in order to create table before running python files

File HistoricalWeatherAPI.py:
- run locally
- collect and insert historical data from api into Data Lake

File DailyWeatherAPI.py:
- run in AWS as a Lambda function
- credentials configurated in Environment variables for Lambda function
