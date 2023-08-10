### star_dim_date, star_dim_city, star_fact_gun
authored by: Paulina Zal
Here the data from data lake are transformed to  DWH built in star schema. The credentials needs to be specified in lambda function environmental variables.
All files are run after the processes in the Data Lake are done:

star_dim_date.py
- authored by: Paulina Zal
- dimension table for dates
- lambda function star_dim_date
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46



star_dim_city.py  
- authored by: Paulina Zal
- lambda function star_dim_city
- dimension table for locations
- requires Texas_Counties_Centroid_Map.csv and uscities.csv
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46



star_fact_gun.py
- authored by: Paulina Zal
- lambda function star_fact_gun
- fact table for gun violence incidents
- requires Texas_Counties_Centroid_Map.csv and uscities.csv
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46


### final table
Authored by: Paulina Zal

final_table.py:
- authored by: Paulina Zal
- lambda function : Final_table
- combine all tables in star schema into one, as in database part for visualization
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46


