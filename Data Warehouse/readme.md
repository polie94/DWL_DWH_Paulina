### star_dim_date, star_dim_city, star_fact_gun
authored by: Paulina Zal

All files are run after the processes in the Data Lake are done:

star_dim_date.py
- authored by: Paulina Zal
- lambda function star_dim_date
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46

star_dim_city.py  
- authored by: Paulina Zal
- lambda function star_dim_city
- requires Texas_Counties_Centroid_Map.csv and uscities.csv
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46

star_fact_gun.py
- authored by: Paulina Zal
- lambda function star_fact_gun
- requires Texas_Counties_Centroid_Map.csv and uscities.csv
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46

### star_dim_air, star_dim_weather
authored by: Mirko Dimitrijevic

All files are run after the processes in the Data Lake are done:

star_dim_air.py
- authored by: Mirko Dimitrijevic
- lambda function star_dim_air
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46

star_dim_weather.py
- authored by: Mirko Dimitrijevic
- lambda function star_dim_weather
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46

### final table
Authored by: Paulina Zal

final_table.py:
- authored by: Paulina Zal
- lambda function : Final_table
- combine all tables into one, as in database part
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46
