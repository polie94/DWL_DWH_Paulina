CREATE TABLE weather_data (
    county VARCHAR(255) NOT NULL,
    date DATE NOT NULL,
    temp_min FLOAT,
    temp_max FLOAT,
    app_temp_min FLOAT,
    app_temp_max FLOAT,
    precipitation FLOAT,
    precipitation_hours FLOAT,
    snowfall FLOAT,
    sunrise TIME,
    sunset TIME,
    PRIMARY KEY (county, date)
);