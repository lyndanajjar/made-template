import os
import zipfile
from pathlib import Path
import shutil
from typing import Callable, Any
import pandas as pd
from sqlalchemy import BIGINT, FLOAT, TEXT
import requests
from io import BytesIO
from sqlalchemy import create_engine

def filter_data(df: pd.DataFrame, column_name: str, validation_rule: Callable[[Any], bool]) -> pd.DataFrame:
    valid_rows = df[column_name].apply(validation_rule)
    return df[valid_rows]

def celsius_to_fahrenheit(c: float) -> float:
    return (c * 9/5) + 32

if __name__ == '__main__':
    zip_url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
    data_filename = 'data.csv'
    response = requests.get(zip_url)
    data_path = Path(zip_url).stem
    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
        zip_file.extractall(data_path)
    df = pd.read_csv(os.path.join(data_path, data_filename),
                     sep=';',
                     index_col=False,
                     usecols=['Geraet', 'Hersteller', 'Model', 'Monat', 'Temperatur in °C (DWD)', 'Batterietemperatur in °C', 'Geraet aktiv'],
                     decimal=',')
    df.rename(columns={
        'Temperatur in °C (DWD)': 'Temperatur',
        'Batterietemperatur in °C':'Batterietemperatur'
        }, inplace=True)
    
    df['Temperatur'] = celsius_to_fahrenheit(df['Temperatur'])
    df['Batterietemperatur'] = celsius_to_fahrenheit(df['Batterietemperatur'])
    
    df = filter_data(df, 'Geraet', lambda x: x > 0)
    df = filter_data(df, 'Monat', lambda x: x in range(1, 13))
    df = filter_data(df, 'Temperatur', lambda x: -459.67 < x < 212)
    df = filter_data(df, 'Batterietemperatur', lambda x: -459.67 < x < 212)
    df = filter_data(df, 'Geraet aktiv', lambda x: x in ['Ja', 'Nein'])
    

    sql_dtypes = {
        'Geraet': BIGINT,
        'Hersteller': TEXT,
        'Model': TEXT,
        'Monat': BIGINT,
        'Temperatur': FLOAT,
        'Batterietemperatur': FLOAT,
        'Geraet aktiv': TEXT
}


    engine = create_engine('sqlite:///temperatures.sqlite')
    df.to_sql('temperatures', con=engine, if_exists='replace', index=False, dtype=sql_dtypes)
    

    shutil.rmtree(data_path)
    print('Data processing complete and stored in SQLite database.')
