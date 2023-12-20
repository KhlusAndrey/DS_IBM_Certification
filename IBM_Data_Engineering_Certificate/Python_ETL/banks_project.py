from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 


def log_progress(message: str) -> None:
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')  

def extract(url: str, table_attribs: list[str]) -> pd.DataFrame:
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows[1:]:
        col = row.find_all('td')
        if col:
            data_dict = {
                "Rank": col[0].text.strip(),
                "Bank_name": col[1].text.strip(),
                "Market_cap": col[2].text.strip()
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df
   
def transform(df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv(csv_path).set_index('Currency').to_dict()['Rate']
    df = df.rename(columns={'Market_cap': 'MC_USD_Billion'})
    df['MC_GBP_Billion'] = [np.round(x * float(exchange_rate['GBP']), 2) for x in df['MC_USD_Billion'].astype(float)]
    df['MC_EUR_Billion'] = [np.round(x * float(exchange_rate['EUR']), 2) for x in df['MC_USD_Billion'].astype(float)]
    df['MC_INR_Billion'] = [np.round(x * float(exchange_rate['INR']), 2) for x in df['MC_USD_Billion'].astype(float)]
    return df

def load_to_csv(df: pd.DataFrame, output_path: str) -> None:
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df: pd.DataFrame, sql_connection: sqlite3.Connection, table_name: str) -> None:
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement: str, sql_connection: sqlite3.Connection) -> None:
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)

def log_progress(message: str) -> None:
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt", "a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Rank", "Bank_name", "Market_cap"]

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

transform(df, '/home/project/exchange_rate.csv')
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, 'banks_csv.csv')

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, 'Largest_banks')
log_progress('Data loaded to Database as table. Running the query')

run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', sql_connection)
run_query('SELECT * FROM Largest_banks', sql_connection)
run_query('SELECT Bank_name from Largest_banks LIMIT 5', sql_connection)

log_progress('Process Complete.')

sql_connection.close()