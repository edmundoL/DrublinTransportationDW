
# Dublin Transportation Data Warehouse 
## Developed by: Edmundo Aarón Llaza Miranda

To create the database I worked with "SnowFlake" [app.snowflake.com](https://app.snowflake.com/), the continue code is the creation of the Database and the schema with I'll work to the ingest of the data.

~~~
create database PublicTransportDublin
use PublicTransportDublin
create schema Ingest
~~~

### Luas Passenger Numbers
Then was the creation of the tables that would be ingested with the data from the csv
~~~
CREATE OR REPLACE TABLE LUASPASSENGERS (
    statistic STRING,
    statistic_label STRING,
    year INT,
    month STRING,
    value FLOAT
);
~~~

### Dublin Bus Passenger Numbers
And executed the program [LuasPassenger.py](LuasPassenger.py) wich use all columns less TLIST, C01885V02316 and unit, because there year contains the same value with TLIST and month with C01885V02316 are the same value in diferent expressions and UNIT just is the definition of the next column VALUE.

I made the same with the csv of the buss passengers, here I created the table with the columns that I think are nessesary:
~~~
CREATE OR REPLACE TABLE BUSSPASSENGER (
    statistic_label STRING,
    year INT,
    month STRING,
    value FLOAT
);
~~~
Continue with the execution of the program [BussPassenger.py](BussPassenger.py) where I used the value of all columns less UNIT.

### Weather Data - Met  Éireann
With the weather data I had to made a fix with the headers because the column ind it's repeating the name so I add a number to differenciate in the others having the headers like: *"date,ind,rain,ind1,maxt,ind2,mint,gmin,soil"*  and deleting the first 13 rows because it was just a leyend of the data, the loaded of the data was with the program [WeatherData.py](WeatherData.py).

~~~
CREATE OR REPLACE TABLE WEATHER_DATA (
    date DATE,
    ind INT,
    rain FLOAT,
    ind1 INT,
    maxt FLOAT,
    ind2 INT,
    mint FLOAT,
    gmin FLOAT,
    soil FLOAT
);
~~~

### Dublin Bikes
To get access to the data from Dublinbike I enter to the link [Dublin Bikes](https://data.smartdublin.ie/dublinbikes-api/bikes/openapi.json), the pipeline is the program [DublinBikes.py](DublinBikes.py) and consume the endpoint /{system_id}/historical/station.csv because the other /{system_id}/historical/station wont work like it should be.
~~~
CREATE OR REPLACE TABLE DUBLINBIKES (
    system_id STRING,
    last_reported TIMESTAMP_NTZ,
    station_id STRING,
    num_bikes_available INT,
    num_docks_available INT,
    is_installed BOOLEAN,
    is_renting BOOLEAN,
    is_returning BOOLEAN,
    name STRING,
    short_name STRING,
    address STRING,
    lat FLOAT,
    lon FLOAT,
    region_id STRING,
    capacity INT
);
~~~
Here was worked with the dates "2024-06-15T00:00:00" and "2024-09-15T00:00:00" by the instructions from the document.

### Cycle Counts
For the last document I loaded manually because it contains massive data to work, here I just used the program [CycleCount.py](CycleCounts.py) to clean the name from the headers wich had special characters like coma or parentheses and its saved with the name [CycleCountsClean.csv](CycleCountsClean.csv).