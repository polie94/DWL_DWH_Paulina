![image](https://github.com/polie94/DL-DWH_Project/assets/40672835/48ffbf03-935a-4661-bd65-0439fa45838a)### star_dim_date, star_dim_city, star_fact_gun
authored by: Paulina Zal
Here the data from data lake are transformed to  DWH built in star schema.
All files are run after the processes in the Data Lake are done:

star_dim_date.py
- authored by: Paulina Zal
- dimension table for dates
- lambda function star_dim_date
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46
- To run this pieces of code you need to log in AWS account class 40209, Account ID: 3355-3129-7715 Federated user: voclabs/user2478657=paulina.zal94


star_dim_city.py  
- authored by: Paulina Zal
- lambda function star_dim_city
- dimension table for locations
- requires Texas_Counties_Centroid_Map.csv and uscities.csv
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46
- To run this pieces of code you need to log in AWS account class 40209, Account ID: 3355-3129-7715 Federated user: voclabs/user2478657=paulina.zal94


star_fact_gun.py
- authored by: Paulina Zal
- lambda function star_fact_gun
- fact table for gun violence incidents
- requires Texas_Counties_Centroid_Map.csv and uscities.csv
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46
- To run this pieces of code you need to log in AWS account class 40209, Account ID: 3355-3129-7715 Federated user: voclabs/user2478657=paulina.zal94


### star_dim_air, star_dim_weather
authored by: Mirko Dimitrijevic

All files are run after the processes in the Data Lake are done:

star_dim_air.py
- authored by: Mirko Dimitrijevic
- lambda function star_dim_air
- dimension table for air pollution measurements
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46

star_dim_weather.py
- authored by: Mirko Dimitrijevic
- lambda function star_dim_weather
- dimension table for weather measurements
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46

### final table
Authored by: Paulina Zal

final_table.py:
- authored by: Paulina Zal
- lambda function : Final_table
- combine all tables in star schema into one, as in database part for visualization
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46
- To run this pieces of code you need to log in AWS account class 40209, Account ID: 3355-3129-7715 Federated user: voclabs/user2478657=paulina.zal94

