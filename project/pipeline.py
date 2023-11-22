import pandas as pd
import requests
from pyexcel_ods import get_data
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine
import os
from io import StringIO


# Get the current script direc
script_dir = os.path.dirname(os.path.abspath(__file__))
data_folder = os.path.join(script_dir, 'data')


# create engine globally
db_path = os.path.join(data_folder, 'Linda_data.db')
engine = create_engine(f'sqlite:///{db_path}')

try:
    def process_startup_data():
        url = "https://www.wirtschaft.nrw/system/files/media/document/file/nrw-startup-report-2020.ods"
        response = requests.get(url)
        file_name = os.path.join(data_folder, 'nrw-startup-report-2020.ods')
        with open(file_name, "wb") as file:
            file.write(response.content)
        ods_data = get_data(file_name)
        sheet_name = 'S__20_-_Finanzierungen_BL'
        df = pd.DataFrame(ods_data[sheet_name])
        column_names = df.iloc[0]
        data = df[1:].copy()
        data.columns = column_names
        data[['Bundesland', 'Typ', 'Startups']] = data['Bundesland,Typ,Startups'].str.split(',', expand=True)
        data = data[data['Bundesland'].notna()]
        data['Startups'] = data['Startups'].apply(lambda x: 0 if x is None or not str(x).isnumeric() else int(x))
        data.to_sql('startup_data', con=engine, index=False, if_exists='replace')
        print("Startup data processed and saved to SQLite.")

    def process_combined_gdp_data():
        # Load the GDP and GDP per capita Excel files
        url_gdp = "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=excel"
        url_gdp_per_capita = "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.CD?downloadformat=excel"

        df_gdp = pd.read_excel(url_gdp, sheet_name="Data", skiprows=3)  # Specify the sheet name
        df_gdp_per_capita = pd.read_excel(url_gdp_per_capita, sheet_name="Data", skiprows=3)  # Specify the sheet name

        # Extract data for Germany
        germany_data_gdp = df_gdp[df_gdp['Country Name'] == 'Germany']
        years_gdp = germany_data_gdp.columns[4:]
        gdp_values_gdp = germany_data_gdp.iloc[:, 4:].values.flatten()

        # Extract data for Germany (GDP per capita)
        germany_data_gdp_per_capita = df_gdp_per_capita[df_gdp_per_capita['Country Name'] == 'Germany']
        gdp_values_gdp_per_capita = germany_data_gdp_per_capita.iloc[:, 4:].values.flatten()

        # Create a table for combined GDP data
        combined_db_table = 'germany_combined_gdp'
        combined_df = pd.DataFrame({'Year': years_gdp, 'GDP Growth Rate': gdp_values_gdp, 'GDP per Capita (Current US Dollars)': gdp_values_gdp_per_capita})
        combined_df.to_sql(combined_db_table, con=engine, index=False, if_exists='replace')
        print("Combined GDP data processed and saved to SQLite.")


    def process_startup_Business_data():
        
        url_startup_business = "https://www.destatis.de/DE/Themen/Branchen-Unternehmen/Unternehmen/_Grafik/_Interaktiv/Daten/betriebsgruendungen-insgesamt.csv?__blob=value"

        response_startup_business = requests.get(url_startup_business)
        csv_data_startup_business = StringIO(response_startup_business.text)
        df_startup_business = pd.read_csv(csv_data_startup_business, delimiter=';')
        df_startup_business.columns = ['Year', 'Business Startups']
        table_name_startup_business = 'startup_data_business'
        df_startup_business.to_sql(table_name_startup_business, con=engine, index=False, if_exists='replace')
        print("Startup data from CSV processed and saved to SQLite.")
    
    def extract_and_add_employment_growth_data():
        csv_url = "https://www.destatis.de/DE/Themen/Arbeit/Arbeitsmarkt/_Grafik/_Interaktiv/Daten/erwerbstaetigkeit-wz-bereiche-jahr.csv?__blob=value"
        response = requests.get(csv_url)
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data, delimiter=';', encoding='unicode_escape') 
        df.columns = [col.strip('"\t ') for col in df.columns]
        column_mapping = {
        'Jahr': 'Year',
        'Insgesamt': 'Total',
        'Land- und Forstwirtschaft, Fischerei': 'Agriculture, Forestry, and Fishing',
        'Produzierendes Gewerbe ohne Baugewerbe': 'Manufacturing Industry excl. Construction',
        'Baugewerbe': 'Construction',
        'Dienstleistungsbereiche': 'Service Industries'
    }

        df.rename(columns=column_mapping, inplace=True)
        required_columns = ['Year', 'Total', 'Agriculture, Forestry, and Fishing',
                        'Manufacturing Industry excl. Construction', 'Construction',
                        'Service Industries']

    # Check if all  col are present
        if all(col in df.columns for col in required_columns):
            employment_growth_table = 'employment_growth_data'
            df[required_columns].to_sql(employment_growth_table, con=engine, index=False, if_exists='replace')
            print("Employment growth data processed and saved to SQLite.")
        else:
            print("Error")

    def process_insolvency_data():
        csv_url = "https://www.destatis.de/DE/Themen/Branchen-Unternehmen/Unternehmen/_Grafik/_Interaktiv/Daten/insolvenzen-unternehmen-insgesamt.csv?__blob=value"
        response = requests.get(csv_url)
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data, delimiter=';')
        df.columns = ['Year', 'Insolvencies']
        table_name = 'insolvency_data'
        df.to_sql(table_name, con=engine, index=False, if_exists='replace')
        print("Insolvency data from CSV processed and saved to SQLite.")



    process_startup_data()
    process_combined_gdp_data()
    process_startup_Business_data()
    extract_and_add_employment_growth_data()
    process_insolvency_data()


except Exception as e:
    print(f"Error: {str(e)}")

# Close the database connection
finally:
    engine.dispose()
