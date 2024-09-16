import pandas as pd
import snowflake.connector

csv_file = "dly3923.csv"

# Leer el CSV, asumiendo que ahora las cabeceras están en la primera línea
df = pd.read_csv(csv_file)

# Convertir la columna 'date' al formato YYYY-MM-DD
df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')

# Reemplazar espacios en blanco con None
df.replace(r'^\s*$', None, regex=True, inplace=True)

print(df.head())

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

# Crear o reemplazar la tabla
create_table_query = """
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
"""
cursor.execute(create_table_query)
print("Tabla creada o reemplazada exitosamente")

# Insertar datos fila por fila
for index, row in df.iterrows():
    # Construir la lista de valores
    values = [
        f"'{row['date']}'",
        f"{row['ind']}" if row['ind'] is not None else 'NULL',
        f"{row['rain']}" if row['rain'] is not None else 'NULL',
        f"{row['ind1']}" if row['ind1'] is not None else 'NULL',
        f"{row['maxt']}" if row['maxt'] is not None else 'NULL',
        f"{row['ind2']}" if row['ind2'] is not None else 'NULL',
        f"{row['mint']}" if row['mint'] is not None else 'NULL',
        f"{row['gmin']}" if row['gmin'] is not None else 'NULL',
        f"{row['soil']}" if row['soil'] is not None else 'NULL'
    ]
    
    # Unir los valores con comas
    values_str = ', '.join(values)
    
    # Crear la consulta SQL
    insert_query = f"""
    INSERT INTO WEATHER_DATA (date, ind, rain, ind1, maxt, ind2, mint, gmin, soil)
    VALUES ({values_str});
    """
    
    print(insert_query)  # Imprime la consulta para depuración
    cursor.execute(insert_query)

cursor.close()
conn.close()

print("Datos cargados exitosamente")
