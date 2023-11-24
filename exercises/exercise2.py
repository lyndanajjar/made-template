
import pandas as pd
from sqlalchemy import create_engine, Float, Text 

url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
df = pd.read_csv(url, encoding='utf-8', sep=';')

df = df.drop(columns=['Status'])
df['Laenge'] = df['Laenge'].str.replace(',', '.').astype(float)
df['Breite'] = df['Breite'].str.replace(',', '.').astype(float)
df['Laenge'] = pd.to_numeric(df['Laenge'], errors='coerce')
df['Breite'] = pd.to_numeric(df['Breite'], errors='coerce')


df = df[df['Verkehr'].isin(['FV', 'RV', 'nur DPN'])]
df = df[df['Laenge'].between(-90, 90) & df['Breite'].between(-90, 90)]
df = df[df['IFOPT'].str.match(r'^[A-Za-z]{2}:\d+:\d+(\:\d+)?$') | df['IFOPT'].notna()]

df = df.dropna()
column_types = {
    'EVA_NR': Text(),
    'DS100': Text(),
    'IFOPT': Text(),
    'NAME': Text(),
    'Verkehr': Text(),
    'Laenge': Float(),
    'Breite': Float(),
    'Betreiber_Name': Text(),
    'Betreiber_Nr': Text(),
}

engine = create_engine('sqlite:///trainstops.sqlite', echo=False)
df.to_sql('trainstops', con=engine, index=False, if_exists='replace', dtype=column_types)
engine.dispose()
