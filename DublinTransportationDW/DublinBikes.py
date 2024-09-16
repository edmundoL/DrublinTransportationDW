import pandas as pd
import snowflake.connector

csv_file = "stations_dublin_bikes_2024-06-15T00_00_00.csv"  # Cambia el nombre del archivo según corresponda

# Leer el archivo CSV con la cabecera especificada
df = pd.read_csv(csv_file, encoding='utf-8', skiprows=0)

# Verificar los nombres de las columnas y los primeros registros
print("Columnas en el DataFrame:", df.columns)
print(df.head())

# Convertir la columna 'last_reported' al formato TIMESTAMP_NTZ
df['last_reported'] = pd.to_datetime(df['last_reported'], format='%Y-%m-%d %H:%M:%S')

# Conectar a Snowflake
conn = snowflake.connector.connect(
    user='Edmundo2210',
    password='Shunashi973!',
    account='ypxqcvp-pp44707',
    warehouse='COMPUTE_WH',
    database='PUBLICTRANSPORTDUBLIN',
    schema='INGEST'
)

cursor = conn.cursor()

# Crear la tabla en Snowflake
create_table_query = """
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
"""
cursor.execute(create_table_query)
print("Tabla creada o reemplazada exitosamente")

# Insertar los datos en Snowflake
for index, row in df.iterrows():
    insert_query = f"""
    INSERT INTO DUBLINBIKES (
        system_id, last_reported, station_id, num_bikes_available, num_docks_available,
        is_installed, is_renting, is_returning, name, short_name, address, lat, lon, region_id, capacity
    ) VALUES (
        '{row['system_id']}',
        TO_TIMESTAMP_NTZ('{row['last_reported'].strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS'),
        '{row['station_id']}',
        {row['num_bikes_available'] if pd.notna(row['num_bikes_available']) and row['num_bikes_available'] != '' else 'NULL'},
        {row['num_docks_available'] if pd.notna(row['num_docks_available']) and row['num_docks_available'] != '' else 'NULL'},
        {str(row['is_installed']).upper() if pd.notna(row['is_installed']) and row['is_installed'] != '' else 'NULL'},
        {str(row['is_renting']).upper() if pd.notna(row['is_renting']) and row['is_renting'] != '' else 'NULL'},
        {str(row['is_returning']).upper() if pd.notna(row['is_returning']) and row['is_returning'] != '' else 'NULL'},
        '{row['name'].replace("'", "''") if pd.notna(row['name']) and row['name'] != '' else 'NULL'}',
        '{row['short_name'].replace("'", "''") if pd.notna(row['short_name']) and row['short_name'] != '' else 'NULL'}',
        '{row['address'].replace("'", "''") if pd.notna(row['address']) and row['address'] != '' else 'NULL'}',
        {row['lat'] if pd.notna(row['lat']) and row['lat'] != '' else 'NULL'},
        {row['lon'] if pd.notna(row['lon']) and row['lon'] != '' else 'NULL'},
        '{row['region_id'].replace("'", "''") if pd.notna(row['region_id']) and row['region_id'] != '' else 'NULL'}',
        {row['capacity'] if pd.notna(row['capacity']) and row['capacity'] != '' else 'NULL'}
    );
    """
    # Comentario para depuración: print(f"Ejecutando consulta: {insert_query}")
    cursor.execute(insert_query)

cursor.close()
conn.close()

print("Datos cargados exitosamente")
