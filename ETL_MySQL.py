import numpy as np
import pandas as pd
import mysql.connector
import mysql.connector as connection

from mysql.connector import Error
from IPython.display import Image
import tqdm.notebook as tq
tq.tqdm_notebook.pandas()

# Data Ingestion
df  = pd.read_csv ('DATA.csv')   
df.head()

# EDA of Dataset
len(df)
df.isna().sum()
dtype_pd = pd.DataFrame(df.dtypes, columns = ['data_type']).reset_index()
unique_records = pd.DataFrame(df.nunique(), columns = ['unique_records']).reset_index()
info_df = pd.merge(dtype_pd, unique_records, on = 'index')
info_df

# Transformations
df['domain'] = df['email'].progress_apply(lambda x: x.split('@')[1])
df['full_name'] = df['first_name']+' '+df['last_name']
df.rename(columns = {'id':'user_id'}, inplace = True)
df[df.duplicated(['full_name'], keep=False)]

# Loading in the database
try:
    connection = mysql.connector.connect(
        database = 'etl_database',
        host='localhost',
        user='root',
        password='Password',
        port='3306',auth_plugin='mysql_native_password')


    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        prod_cursor = connection.cursor()
        prod_cursor.execute("select database();")
        record = prod_cursor.fetchone()
        print("You're connected to database ")


except Exception as e:
    print(e)
    print("Error while connecting to MySQL")

df.columns

sql_table_name= 'user'
initial_sql = "CREATE TABLE IF NOT EXISTS " +str(sql_table_name)+ "(id INT AUTO_INCREMENT PRIMARY KEY"

def rename_df_cols(df):
    col_no_space =  dict((i, i.replace(' ','')) for i in list(df.columns))
    df.rename(columns= col_no_space, index= str, inplace= True)
    return df

def dtype_mapping():
    return {'object' : 'VARCHAR(50)',
        'int64' : 'BIGINT',
        'float64' : 'FLOAT',
        'datetime64' : 'DATETIME',}

def create_sql( df, sql = initial_sql):

    df = rename_df_cols(df)

    col_list_dtype = [(i, str(df[i].dtype)) for i in list(df.columns)]

    map_data= dtype_mapping()

    for i in col_list_dtype:
        key = str(df[i[0]].dtypes)
        sql += ", " + str(i[0])+ ' '+ map_data[key]
    sql = sql+",inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
    sql= sql + str(')')
    
    print('\n', sql, '\n')
    

    cursor = connection.cursor()
    try:
        cursor.execute(sql)
    except ValueError: 
        print("Check Sql again")
    
    cursor.close()

create_sql(df= df)

def load_csv_in_db(df):
    cursor = connection.cursor()
    for index, data in df.iterrows():
        query = f"""INSERT INTO user (user_id, first_name, last_name, email, gender, ip_address,domain, full_name) """ \
        f"""VALUES({data['user_id']},"{data['first_name']}","{data['last_name']}","{data['email']}",
        "{data['gender']}","{data['ip_address']}","{data['domain']}","{data['full_name']}");"""

        cursor.execute(query)
        connection.commit()
        if (int(index)+1)%100 == 0:
            print(str(int(index)+1)+" Records inserted successfully")
            
load_csv_in_db(df)