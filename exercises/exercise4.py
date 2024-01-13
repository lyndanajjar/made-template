import os
import urllib.request
import zipfile
import pandas as pd
from sqlalchemy import create_engine, Integer, Float, String
from sqlalchemy.types import BigInteger
import shutil

# Constants for the file paths
local_zip_path = 'mowesta-dataset.zip'
local_extracted_path = 'mowesta-dataset'
database_path = 'sqlite:///temperatures.sqlite'
csv_filename = 'data.csv'

# Download the dataset
url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
urllib.request.urlretrieve(url, local_zip_path)

# Unzip the dataset
with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
    zip_ref.extractall(local_extracted_path)

# Full path to the CSV file after extraction
csv_file_path = os.path.join(local_extracted_path, csv_filename)

# Define the columns to use based on the column names in the file
use_cols = [
    "Geraet", "Hersteller", "Model", "Monat",
    "Temperatur in Â°C (DWD)", "Batterietemperatur in Â°C", "Geraet aktiv"
]

# Read the CSV file
df = pd.read_csv(csv_file_path, delimiter=';', usecols=use_cols, encoding='ISO-8859-1')

# Rename columns
df.rename(columns={
    "Temperatur in Â°C (DWD)": "Temperatur",
    "Batterietemperatur in Â°C": "Batterietemperatur"
}, inplace=True)

# Convert temperatures from Celsius to Fahrenheit
df['Temperatur'] = pd.to_numeric(df['Temperatur'], errors='coerce')
df['Batterietemperatur'] = pd.to_numeric(df['Batterietemperatur'], errors='coerce')
df['Temperatur'] = df['Temperatur'].apply(lambda x: (x * 9/5) + 32 if pd.notna(x) else x)
df['Batterietemperatur'] = df['Batterietemperatur'].apply(lambda x: (x * 9/5) + 32 if pd.notna(x) else x)

# Validate data
df['Geraet'] = pd.to_numeric(df['Geraet'], errors='coerce').astype('Int64')
df['Monat'] = pd.to_numeric(df['Monat'], errors='coerce').astype('Int64')
df = df[df['Geraet'] > 0]

# Assuming 'Geraet aktiv' should be a boolean, convert 'Ja'/'Nein' to True/False
df['Geraet aktiv'] = df['Geraet aktiv'].map({'Ja': True, 'Nein': False})


# Define SQL data types
sql_dtypes = {
    "Geraet": BigInteger(),
    "Hersteller": String(),
    "Model": String(),
    "Monat": Integer(),
    "Temperatur": Float(),
    "Batterietemperatur": Float(),
    "Geraet aktiv": String()
}

engine = create_engine(database_path)
df.to_sql('temperatures', con=engine, if_exists='replace', index=False, dtype=sql_dtypes)


os.remove(local_zip_path)
shutil.rmtree(local_extracted_path)

print("Data processing complete and stored in SQLite database.")

