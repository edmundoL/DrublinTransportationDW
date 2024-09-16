import pandas as pd
import snowflake.connector

csv_file = "TOA11.20240915195232.csv"

df = pd.read_csv(csv_file)

df = df[df['Month'] != 'All months']

print(df.head())

conn = snowflake.connector.connect(
    user='Edmundo2210',
    password='Shunashi973!',
    account='ypxqcvp-pp44707',
    warehouse='COMPUTE_WH',
    database='PUBLICTRANSPORTDUBLIN',
    schema='INGEST'
)

cursor = conn.cursor()

create_table_query = """
CREATE OR REPLACE TABLE LUASPASSENGERS (
    statistic STRING,
    statistic_label STRING,
    year INT,
    month STRING,
    value FLOAT
);
"""
cursor.execute(create_table_query)
print("Tabla creada o reemplazada exitosamente")

for index, row in df.iterrows():
    insert_query = f"""
    INSERT INTO LUASPASSENGERS (statistic, statistic_label, year, month, value)
    VALUES ('{row['STATISTIC']}', '{row['Statistic Label']}', {row['Year']}, '{row['Month']}', {row['VALUE']});
    """
    cursor.execute(insert_query)

cursor.close()
conn.close()

print("Datos cargados exitosamente")
