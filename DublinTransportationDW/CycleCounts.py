import pandas as pd
import re

archivo_csv = 'cycle-counts-1-jan-31-december-2023.csv'

df = pd.read_csv(archivo_csv)

def limpiar_titulo(titulo):
    return re.sub(r'[().]', '', titulo)

df.columns = [limpiar_titulo(col) for col in df.columns]

df.columns = [col.replace(' ', '_') for col in df.columns]

df.to_csv('CycleCountsClean.csv', index=False)
