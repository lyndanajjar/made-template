import os
import urllib.request
import zipfile
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String, Float, Text, BigInteger
import shutil

local_zip_path = 'mowesta-dataset.zip'
local_extracted_path = 'mowesta-dataset'
database_path = 'sqlite:///temperatures.sqlite'
csv_filename = 'data.csv'

url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
urllib.request.urlretrieve(url, local_zip_path)

with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
    zip_ref.extractall(local_extracted_path)
    
csv_file_path = os.path.join(local_extracted_path, csv_filename)
use_cols = [
    "Geraet", "Hersteller", "Model", "Monat",
    "Temperatur in Â°C (DWD)", "Batterietemperatur in Â°C", "Geraet aktiv"
]
df = pd.read_csv(csv_file_path, delimiter=';', usecols=use_cols, encoding='ISO-8859-1')

# Rename columns
df.rename(columns={
    "Temperatur in Â°C (DWD)": "Temperatur",
    "Batterietemperatur in Â°C": "Batterietemperatur"
}, inplace=True)

df['Temperatur'] = pd.to_numeric(df['Temperatur'], errors='coerce')
df['Batterietemperatur'] = pd.to_numeric(df['Batterietemperatur'], errors='coerce')

# Apply the conversion formula, skipping NaN values
df['Temperatur'] = df['Temperatur'].apply(lambda x: (x * 9/5) + 32 if pd.notna(x) else x)
df['Batterietemperatur'] = df['Batterietemperatur'].apply(lambda x: (x * 9/5) + 32 if pd.notna(x) else x)



# Validate data
df = df[df['Geraet'] > 0]
sql_dtypes = {
    "Geraet": BigInteger(),
    "Hersteller": Text(),
    "Model": Text(),
    "Monat": Integer(),
    "Temperatur": Float(),
    "Batterietemperatur": Float(),
    "Geraet aktiv": String()
}

df['Geraet'] = df['Geraet'].astype('int64') 
df['Monat'] = pd.to_numeric(df['Monat'], errors='coerce').astype('Int64')  
df['Geraet aktiv'] = df['Geraet aktiv'].astype(str)  
df['Temperatur'] = pd.to_numeric(df['Temperatur'], errors='coerce')  
df['Batterietemperatur'] = pd.to_numeric(df['Batterietemperatur'], errors='coerce')  

engine = create_engine(database_path)
df.to_sql('temperatures', con=engine, if_exists='replace', index=False, dtype=sql_dtypes)

os.remove(local_zip_path)
if os.path.exists(local_extracted_path):
    shutil.rmtree(local_extracted_path)

print("Data processing complete and stored in SQLite database.")
