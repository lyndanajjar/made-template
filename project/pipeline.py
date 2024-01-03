import pandas as pd
import requests
from pyexcel_ods import get_data
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine
import os
from io import StringIO
import xml.etree.ElementTree as ET



# Get the current script direc
script_dir = os.path.dirname(os.path.abspath(__file__))
data_folder = os.path.join(os.path.dirname(script_dir), 'data')



# create engine globally
db_filename = 'Linda_data.db'
db_path = os.path.join(data_folder, db_filename)
engine = create_engine(f'sqlite:///{db_path}')

try:

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
        if response.status_code != 200:
            print(f"Error fetching data: HTTP {response.status_code}")
            return

        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data, delimiter=';', encoding='unicode_escape') 
        df.columns = [col.strip().replace('"', '') for col in df.columns]  

        column_mapping = {
    'Jahr': 'Year',
    'Insgesamt': 'Total',
    'Land- u. Forstwirtschaft, Fischerei': 'Agriculture, Forestry, and Fishing', 
    'Produzierendes Gewerbe ohne Baugewerbe': 'Manufacturing Industry excl. Construction',
    'Baugewerbe': 'Construction',
    'Dienstleistungsbereiche': 'Service Industries'}

        df.rename(columns=column_mapping, inplace=True)
        required_columns = ['Year', 'Total', 'Agriculture, Forestry, and Fishing',
                        'Manufacturing Industry excl. Construction', 'Construction',
                        'Service Industries']

        if all(col in df.columns for col in required_columns):
            employment_growth_table = 'employment_growth_data'
            df[required_columns].to_sql(employment_growth_table, con=engine, index=False, if_exists='replace')
            print("Employment growth data processed and saved to SQLite.")
        else:
            missing_columns = [col for col in required_columns if col not in df.columns]
            print(f"Error: Missing columns in DataFrame: {missing_columns}")

        
    def process_venture_capital_data():
        xml_url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/VC_INVEST/DEU.VC_INV.VC_T+SEED+START+LATER.USD_V+SH_GDP/all?startTime=2002&endTime=2022"
        response = requests.get(xml_url)
        xml_data = response.content
        root = ET.fromstring(xml_data)
        ns = {
        'ns0': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message',
        'ns2': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic',
    }

        data = []
        for series in root.findall('.//ns2:Series', namespaces=ns):
            series_key = series.find('ns2:SeriesKey', namespaces=ns)
            values = [value.attrib['value'] for value in series_key.findall('ns2:Value', namespaces=ns)]
            attributes = series.find('ns2:Attributes', namespaces=ns)
            attributes_dict = {value.attrib['concept']: value.attrib['value'] for value in attributes.findall('ns2:Value', namespaces=ns)}

            for obs in series.findall('ns2:Obs', namespaces=ns):
                row = {'Time': obs.find('ns2:Time', namespaces=ns).text, 'Value': obs.find('ns2:ObsValue', namespaces=ns).attrib['value']}
                row.update(dict(zip(['LOCATION', 'SUBJECT', 'STAGES', 'MEASURE'], values)))
                row.update(attributes_dict)
                data.append(row)

        df_venture_capital = pd.DataFrame(data)
        table_name_venture_capital = 'venture_capital_data'
        df_venture_capital.to_sql(table_name_venture_capital, con=engine, index=False, if_exists='replace')
        print("Venture capital data processed and saved to SQLite.")



    #process_startup_data()
    process_combined_gdp_data()
    process_startup_Business_data()
    extract_and_add_employment_growth_data()
    process_venture_capital_data()


except Exception as e:
    print(f"Error: {str(e)}")

# Close the database connection
finally:
    engine.dispose()
