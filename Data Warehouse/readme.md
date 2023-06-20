### star_dim_date, star_dim_city, star_fact_gun
authored by: Paulina Zal

All files are run after the processes in the Data Lake are done:

star_dim_date.py
- lambda function star_dim_date
- requires a layer with pandas, psycopg2-binary and sqlalchemy==1.4.46

star_dim_city.py  
- lambda function star_dim_city
- requires Texas_Counties_Centroid_Map.csv and uscities.csv

star_fact_gun.py
- lambda function star_fact_gun
- requires Texas_Counties_Centroid_Map.csv and uscities.csv

  ### Mirkos tables


### final table
Authored by: Paulina Zal
