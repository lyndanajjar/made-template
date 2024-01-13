import os
import urllib.request
import zipfile
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Float, String, BigInteger
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

# Convert temperatures from Celsius to Fahrenheit ensuring all values are numeric
df['Temperatur'] = pd.to_numeric(df['Temperatur'], errors='coerce')
df['Batterietemperatur'] = pd.to_numeric(df['Batterietemperatur'], errors='coerce')
df['Temperatur'] = (df['Temperatur'] * 9/5) + 32
df['Batterietemperatur'] = (df['Batterietemperatur'] * 9/5) + 32

# Validate data
df['Geraet'] = pd.to_numeric(df['Geraet'], errors='coerce').fillna(0).astype(int)
df['Monat'] = pd.to_numeric(df['Monat'], errors='coerce').fillna(0).astype(int)

df = pd.read_csv(csv_file_path, delimiter=';', usecols=use_cols, encoding='ISO-8859-1')


# Validate data
df['Geraet'] = pd.to_numeric(df['Geraet'], errors='coerce').fillna(0).astype(int)
df = df[df['Geraet'] > 0]
print(f"Row count after 'Geraet' validation: {len(df)}")

# Inspect 'Monat' values before conversion
print(f"Unique 'Monat' values before conversion: {df['Monat'].unique()}")

# Convert 'Monat' to numeric, fill NaNs with a placeholder (e.g., -1) for debugging
df['Monat'] = pd.to_numeric(df['Monat'], errors='coerce').fillna(-1).astype(int)

print(f"Row count after 'Monat' validation: {len(df)}")


df['Geraet aktiv'] = df['Geraet aktiv'].replace({'Ja': True, 'Nein': False})
print(f"Row count after 'Geraet aktiv' validation: {len(df)}")  # Check if rows are dropped here


# Check if the processed number of rows is as expected
if len(df) != 4872:
    raise ValueError(f"Processed row count mismatch. Expected 4872, got {len(df)}")

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

