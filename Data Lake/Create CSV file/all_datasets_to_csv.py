import pandas as pd

# Load the gun violence incident data into a pandas DataFrame
gun_violence_df = pd.read_csv('texas22-unique.csv')

# Load the weather data into a pandas DataFrame
weather_df = pd.read_csv('weather_data_2022-cw.csv')

# Load the air pollution data into a pandas DataFrame
air_pollution_df = pd.read_csv('TexasAirpollutioncleaned_aggregate_v2.csv')
air_pollution_df = air_pollution_df.round(4)

# Merge the gun violence dataframe with the weather and pollution dataframes using a left join
merged_df_large = pd.merge(weather_df, gun_violence_df, how='left', on=['date', 'county'])
merged_df_large = pd.merge(merged_df_large, air_pollution_df, how='left', on=['date', 'county'])

# Drop unnecessary columns
merged_df_large = merged_df_large.drop(['state', 'county_city', 'city', 'index', 'Datetime', 'Hour', 'Unnamed: 0', 'operations',
                                        'state_id', 'state_name', 'lat_x', 'lng', 'lat_y', 'lon'], axis=1)

#change injured and killed to integers
merged_df_large['# killed'] = pd.to_numeric(merged_df_large['# killed'], errors='coerce').fillna(0).astype(int)
merged_df_large['# injured'] = pd.to_numeric(merged_df_large['# injured'], errors='coerce').fillna(0).astype(int)

# Create a new column with the temperature factors
merged_df_large['temperature_factor'] = pd.cut(merged_df_large['temperature_2m_max'], bins=range(-20, 50, 5),
                                  labels=['< -15', '-15 to -10', '-10 to -5', '-5 to 0', '0 to 5',
                                          '5 to 10', '10 to 15', '15 to 20', '20 to 25', '25 to 30',
                                          '30 to 35', '35 to 40', '>40'])

print(merged_df_large.head(10))

# Save the merged dataframe to a new csv file
merged_df_large.to_csv('merged_data_large.csv', index=False)