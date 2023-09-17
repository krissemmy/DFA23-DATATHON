import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

user=os.getenv("USER")
password=os.getenv("PASSWORD")
account=os.getenv("ACCOUNT")
role=os.getenv("ROLE")


con = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    role=role
    )


query_1 = ["""
-- Create a TABLE for SENSORDATARAW
CREATE OR REPLACE TABLE DFA23RAWDATA.THEDATACREW.SensorDataStaging AS
SELECT
    SENSOR_ID::STRING AS Sensor_ID,
    TIMESTAMP AS Timestamp,
    TRY_CAST(TEMPERATURE AS FLOAT) AS Temperature,
    TRY_CAST(HUMIDITY AS FLOAT) AS Humidity,
    TRY_CAST(SOIL_MOISTURE AS FLOAT) AS Soil_Moisture,
    TRY_CAST(LIGHT_INTENSITY AS INTEGER) AS Light_Intensity,
    TRY_CAST(BATTERY_LEVEL AS FLOAT) AS Battery_Level
FROM DFA23RAWDATA.RAWDATA.SENSORDATARAW
WHERE
    Sensor_ID IS NOT NULL
    OR Timestamp IS NOT NULL;
""",

"""
-- Create a TABLE for WEATHERDATARAW
CREATE OR REPLACE TABLE DFA23RAWDATA.THEDATACREW.WeatherDataStaging AS
SELECT
    TIMESTAMP AS Timestamp,
    WEATHER_CONDITION::STRING AS Weather_Condition,
    TRY_CAST(WIND_SPEED AS FLOAT) AS Wind_Speed,
    TRY_CAST(PRECIPITATION AS FLOAT) AS Precipitation
FROM DFA23RAWDATA.RAWDATA.WEATHERDATARAW
WHERE
    Timestamp IS NOT NULL;
""",

"""
-- Create a TABLE for PESTDATARAW
CREATE OR REPLACE TABLE DFA23RAWDATA.THEDATACREW.PestDataStaging AS
SELECT
    TIMESTAMP AS Timestamp,
    PEST_TYPE::STRING AS Pest_Type,
    PEST_DESCRIPTION::STRING AS Pest_Description,
    PEST_SEVERITY::STRING AS Pest_Severity
FROM DFA23RAWDATA.RAWDATA.PESTDATARAW
WHERE
    Timestamp IS NOT NULL;
""",

"""
-- Create a TABLE for SOILDATARAW
CREATE OR REPLACE TABLE DFA23RAWDATA.THEDATACREW.SoilDataStaging AS
SELECT
    TIMESTAMP AS Timestamp,
    TRY_CAST(SOIL_COMP AS FLOAT) AS Soil_Comp,
    TRY_CAST(SOIL_MOISTURE AS FLOAT) AS Soil_Moisture,
    TRY_CAST(SOIL_PH AS FLOAT) AS Soil_PH,
    TRY_CAST(NITROGEN_LEVEL AS FLOAT) AS Nitrogen_Level,
    TRY_CAST(PHOSPHORUS_LEVEL AS FLOAT) AS Phosphorus_Level,
    TRY_CAST(ORGANIC_MATTER AS FLOAT) AS Organic_Matter
FROM DFA23RAWDATA.RAWDATA.SOILDATARAW
WHERE
    Timestamp IS NOT NULL;
""",

"""
-- Create or replace the TABLE without rows where all columns have NULL values
CREATE OR REPLACE TABLE DFA23RAWDATA.THEDATACREW.LocationDataStaging AS
SELECT
    SPLIT_PART(SENSOR_ID, '_', 2)::STRING AS Sensor_ID,
    LOCATION_NAME::STRING AS Location_Name,
    TRY_CAST(LATITUDE AS FLOAT) AS Latitude,
    TRY_CAST(LONGITUDE AS FLOAT) AS Longitude,
    TRY_CAST(ELEVATION AS FLOAT) AS Elevation,
    COALESCE(REGION::STRING, 'Unknown') AS Region
FROM DFA23RAWDATA.RAWDATA.LOCATIONDATARAW
WHERE
    Sensor_ID IS NOT NULL
    OR LATITUDE IS NOT NULL
    OR LONGITUDE IS NOT NULL
    OR ELEVATION IS NOT NULL
    OR REGION IS NOT NULL;
""",

"""
-- Create a TABLE for IRRIGATIONDATARAW
CREATE OR REPLACE TABLE DFA23RAWDATA.THEDATACREW.IrrigationDataStaging AS
SELECT
    SPLIT_PART(SENSOR_ID, '_', 2)::STRING AS Sensor_ID,
    TIMESTAMP AS Timestamp,
    IRRIGATION_METHOD::STRING AS Irrigation_Method,
    WATER_SOURCE::STRING AS Water_Source,
    TRY_CAST(IRRIGATION_DURATION_MIN AS INTEGER) AS Irrigation_Duration_Min
FROM DFA23RAWDATA.RAWDATA.IRRIGATIONDATARAW
WHERE
    Sensor_ID IS NOT NULL
    OR Timestamp IS NOT NULL;
""",

"""
-- Create a TABLE for CROPDATARAW
CREATE OR REPLACE TABLE DFA23RAWDATA.THEDATACREW.CropDataStaging AS
SELECT
    TIMESTAMP AS Timestamp,
    CROP_TYPE::STRING AS Crop_Type,
    TRY_CAST(CROP_YIELD AS FLOAT) AS Crop_Yield,
    GROWTH_STAGE::STRING AS Growth_Stage,
    PEST_ISSUE::STRING AS Pest_Issue
FROM DFA23RAWDATA.RAWDATA.CROPDATARAW;
"""
]


cur = con.cursor()
cur.execute("USE WAREHOUSE DFA23")
cur.execute("USE DATABASE DFA23RAWDATA")
cur.execute("USE SCHEMA DFA23RAWDATA.RAWDATA")

for sql in query_1:
    try:
        cur.execute(sql)
        con.commit()
    except Exception as e:
        print("Error during data insertion:", str(e))
        con.rollback()
    finally:
        con.close()

