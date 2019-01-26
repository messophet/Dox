import time
import json
import requests
import MySQLdb
import threading
import sqlalchemy
import glob
import sys
import tables
import queue
import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from pandas.io.json import json_normalize

"""
This program takes records from a RESTful web api and stitches them with a remote SQL db.
It does this whilst chunking, using threading techniques.
"""


##########################
#    GLOBAL VARIABLES    #
##########################

api_url_base = 'http://de-tech-challenge-api.herokuapp.com/api/v1/'
lock = threading.Lock() # for each block in database
write_lock = threading.Lock() # to prevent segmentation fault
date = datetime.now().date()
threadLimit = None
time = None
matches = 0
DDL = '''CREATE TABLE IF NOT EXISTS FriendlyUsers (
    id INT,
    firstname VARCHAR(255) NOT NULL,
    lastname VARCHAR(255) NOT NULL,
    specialty VARCHAR(255) NOT NULL,
    last_active_date DATE,
    active VARCHAR(255) NOT NULL,
    practice_location VARCHAR(255) NOT NULL,
    user_type_classification VARCHAR(255) NOT NULL,
    dox_active VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (doxid) REFERENCES user(id)
);'''
output = 'output'


##########################
#   UTILITY FUNCTIONS    #
##########################

def get_user_files():
    for f in sorted(glob.glob('*.h5')):
        yield f

##########################
# DB CONNECTION SETTINGS #
# EDITED TO REMOVE CREDS #
##########################

db_host = ''
db_user = ''
db_password = '' # better to decrypt encrypted password or SSL if supported
db_port = 1337
db_name = ''
db_table = ''

db = None

##########################
#     SQL FUNCTIONS      #
#      (deprecated)      #
##########################

#def mysql_connect(host, port, user, pw):
#    DB = MySQLdb.connect(host=host,port=port,user=user,password=pw)
#    return DB

#def mysql_execute(db, query):
#    c = db.cursor()
#    c.execute(query)
#    return c.fetchall()

##########################
#     API FUNCTIONS      #
##########################

def get_user_info(page):
    api_url = '{0}users?page={1}'.format(api_url_base, page)
    response = requests.get(api_url, headers=None)
    if response.status_code == 200:
        return response.content.decode('utf-8')
    else:
        return None

def fetch(i):
    """ Fetches user info from API by Page Number, and caches result to disk in HDF5 format
        Along the way, it calculates if the user is active
    """
    threadLimit.acquire()
    try:
        print("[+] Fetching user info for PAGE {}".format(i))
        user_info = json_normalize(json.loads(get_user_info(i))['users'])
        user_info['last_active_date'] = pd.to_datetime(user_info['last_active_date']) 
    
        def f(row):
            return 'N' if (date - row['last_active_date']).days > 30 else 'Y'

        user_info['active'] = user_info.apply(f, axis=1)
    
        table = 'users' + str(i)

        with write_lock:
            user_info.to_hdf(table + '.h5', table)

        print("[+] Finished fetching user info for PAGE {}".format(i))
    finally:
        threadLimit.release()

##########################
#       MAIN CLASS       #
##########################

############################################################ 
# Note: Could use Map-Reduce to solve chunking portion &   #
#       store pandas SQL results in HDF5 format if needed. #
############################################################

class FVFetcher(threading.Thread):
    """ This function will combine chunked SQL blocks
        with page-indexed user info from friendly vendor
    
    """
    
    def __init__(self, block_start, block_end, engine):
        self.__start = block_start
        self.__end = block_end
        self.__mysql_cn = engine
        threading.Thread.__init__(self) 

    def run(self):
        """
            MySQL does not support window functions, thus we chunk by id.
            
            Based on the data, a match is determined by firstname, lastname and specialty

            A lock is used in case we can eventually load more than 10% of the data OR
            we decide to chunk <10% and choose to parallelize the problem such that 
            the partitions add up to 10%.

        """
        with lock:
            print("[+] Executing Thread. Merging with SQL BLOCK IDs ({} - {})".format(self.__start, self.__end))
#           Another approach would be to use SQLAlchemy and the session object:
#           ie. users = self.__session.query(User).filter(User.id.between(self.__start, self.__end)).all()
            dox_data = pd.read_sql_query('SELECT id as doxid, firstname, lastname, specialty, last_active_date FROM {} WHERE id BETWEEN {} AND {};'.format(db_table, self.__start, self.__end), self.__mysql_cn) 
           
            def f(row):
                return 'N' if (date - row['last_active_date']).days > 30 else 'Y'

            dox_data['last_active_date'] = pd.to_datetime(dox_data['last_active_date'])
            dox_data['dox_active'] = dox_data.apply(f, axis=1)
            dox_data.drop('last_active_date', axis=1)

            files = get_user_files()
            matches = []
            
            for file in [x for x in files if x.startswith('users')]:
                data = pd.read_hdf(file)
                columns = ['firstname', 'lastname', 'specialty']
                try:
                    data = pd.merge(data, dox_data[columns + ['doxid', 'dox_active']], how='inner', left_on=columns, right_on=columns)
                    data.to_hdf(output + '.h5', output, format='t', mode='a', append=True)
                    matches.append(len(data))
                except TypeError:
                    print('[+] Failed to load file {}.'.format(file))
                    return 0 
            
            return matches

if __name__ == "__main__":
    """ This could use some tidying-up
        output: we are saving an hdf5 file with all of the matches,
                this would be changed in production code as the records
                are pushed to the database on each loop!
    """
    if len(sys.argv) > 1:
        date = sys.argv[1]
        try:
            date = datetime.strptime(date, '%Y%m%d')
        except ValueError:
            print('Unable to parse date provided - please use the following format:\nYYYYMMDD')
            sys.exit()
        finally:
            if len(sys.argv) == 3:
                output = sys.argv[2]
                try:
                    os.remove(output + '.h5') # delete if it already exists
                except OSError:
                    pass

    engine = create_engine('mysql://{}:{}@{}:{}/{}'.format(db_user, db_password, db_host, db_port, db_name))
 
    num_records = pd.read_sql_query('SELECT COUNT(*) FROM %s;' % db_table, engine)
    num_records = num_records.iloc[0][0] 

    block_size = num_records // 10

    #in case there are less than 10 records in the SQL database (someone call security!)
    if not block_size:
        block_size = 1

    threads = []
    page_count = json.loads(get_user_info(0))['total_pages']
    
    threadLimit = threading.BoundedSemaphore(page_count // 10)
    
    for page in range(1, page_count + 1):
        t = threading.Thread(target=fetch, args=(page,))
        threads.append(t)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    queue = []
    for db_chunk in range(0,10):
        queue.append(FVFetcher(1 + block_size * db_chunk, block_size * (db_chunk + 1), engine).run())
    
    matches = sum(map(sum, queue))


with open('output.txt', 'w') as f:
    f.write('Total Matches: {}\n\n'.format(str(matches)))
    sample = pd.HDFStore(output + '.h5')
    sample = sample[output].iloc[:10, :]
    f.write('Sample Output: {}\n\n'.format(sample.to_json(orient='records')))
    f.write('SQL DDL: {}'.format(DDL))
