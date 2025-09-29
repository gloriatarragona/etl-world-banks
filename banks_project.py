#Acquiring and Processing Information on the World's Largest Banks

#Importing libraries

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy 
from datetime import datetime
import sqlite3

def log_progress(message):
    time = datetime.now()
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file,'a') as f:
        f.write(f"{time} : {message}\n")


def extract(url, table_attrib):

    #Extraction
    request=requests.get(url).text
    bs=BeautifulSoup(request,'html.parser')
    df=pd.DataFrame(columns=table_attrib)
    tables=bs.find_all('tbody')
    rows=tables[0].find_all('tr')
    
    for row in rows:
        cols=row.find_all('td')
        if len(cols)!=0:
            column1=cols[1].find_all('a')
            data_dict={table_attrib[0]:column1[1]['title'],table_attrib[1]:cols[2].contents[0]}
            data_frame=pd.DataFrame([data_dict])
            df=pd.concat([df,data_frame],ignore_index=True)
    
    #Converting to float the column MC
    for i in range (0,len(df[table_attrib[1]])):
        valores=df[table_attrib[1]][i].split('\n')[0].split('.')
        nuevo_item=''
        for valor in valores:
            nuevo_item+=valor
        nuevo_item=float(nuevo_item)/100
        df[table_attrib[1]][i]=nuevo_item
    log_progress('Data extraction complete. Initiating Transformation process')
    return df
def transform(df, csv_path):
    df_exchange=pd.read_csv(csv_path)
    keys=df_exchange['Currency']
    values=df_exchange['Rate']
    dict_exchange=dict(zip(keys,values))
    list_USD=list(df['MC_USD_Billion'])
    list_GBP=[round(item*dict_exchange['GBP'],2) for item in list_USD ]
    list_EUR=[round(item*dict_exchange['EUR'],2) for item in list_USD ]
    list_INR=[round(item*dict_exchange['INR'],2) for item in list_USD ]
    df['MC_GBP_Billion']=list_GBP
    df['MC_EUR_Billion']=list_EUR
    df['MC_INR_Billion']=list_INR
    log_progress('Data transformation complete. Initiating Loading process')
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)
    log_progress('Data saved to CSV file')
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name,sql_connection,if_exists='replace')
    log_progress('Data loaded to Database as a table, Executing queries')

def run_query(query_statement, sql_connection):
    print(query_statement)
    print(pd.read_sql(query_statement,sql_connection))

    log_progress('Process Complete')




''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url='https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_csv='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attrib=['Name','MC_USD_Billion']
csv_path='Largest_banks_data.csv'
db_name='Banks.db'
table_name='Largest_banks'
log_file='code_log.txt'
log_progress("Preliminaries complete. Initiating ETL process")
load_to_csv(transform(extract(url, table_attrib),exchange_csv),csv_path)
conn=sqlite3.connect(db_name)
log_progress('SQL Connection initiated')
load_to_db(transform(extract(url, table_attrib),exchange_csv),conn,table_name)
query_st1=f'SELECT * FROM Largest_banks'
query_st2=f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query_st3=f'SELECT Name from Largest_banks LIMIT 5'
run_query(query_st1,conn)
run_query(query_st2,conn)
run_query(query_st3,conn)
conn.close()
log_progress('Server Connection closed')