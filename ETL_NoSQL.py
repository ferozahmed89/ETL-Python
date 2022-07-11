import pandas as pd 
from datetime import datetime
from pymongo import MongoClient
from pprint import pprint

ENV = 'WEB'
MONGO_HOST = ''
USERNAME = ''
PASSWORD = ''

if ENV == 'local': 
    dbConfig = {
        'USERNAME' : '',
        'PASSWORD' : '',
        'MONGO_HOST' : 'localhost:27017/',
        'MONGO_URL' : 'mongodb://127.0.0.1:27017',
        'DB_NAME' : 'testDB',
        'DB_COLLECTION' : 'testCollection',
    }
else:
    dbConfig = {
        'USERNAME' : '',
        'PASSWORD' : '',
        'MONGO_HOST' : 'localhost:27017/',
        'MONGO_URL' : 'mongodb+srv://'+USERNAME+':'+PASSWORD+'@'+MONGO_HOST+'/?retryWrites=true&w=majority',
        'DB_NAME' : 'testDB',
        'DB_COLLECTION' : 'testCollection',
    }   


def load_data(dataFrame=None, noOfRecords=0):
    if (dataFrame is None or noOfRecords == 0):
        raise Exception('Cannot Load Empty Data into database')
    else:
        log('Inserting Data into Mongo')
        client = MongoClient(dbConfig['MONGO_URL'])
        db = client[dbConfig['DB_NAME']]
        collection=db[dbConfig['DB_COLLECTION']]
        dataFrame.reset_index(inplace=True)
        data_dict = dataFrame.to_dict("records")
        # Insert collection
        collection.insert_many(data_dict)


def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process) 
    return dataframe

def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    pprint(timestamp + ' - ' + message)

def main():
    filePath = '.\DATA.csv'
    try:
        # ETL Process
        log('ETL Pipeline | Execution Started')
        dataFrame = extract_from_csv(filePath)
        dataLength = len(dataFrame)
        log('ETL Pipeline | Extraction Complete with '+str(dataLength)+' records')
        log('ETL Pipeline | Transform Complete')
        log('ETL Pipeline | Loading to Mongo Start')
        # insert data into mongo
        load_data(dataFrame, dataLength)
        log('ETL Pipeline | Loading to Mongo Complete')
        log('ETL Pipeline | Execution Complete')
    except Exception as error:
        log('ETL Pipeline encountered error')
        log('Error message: {}'.format(error))

if __name__ == "__main__":
  main()