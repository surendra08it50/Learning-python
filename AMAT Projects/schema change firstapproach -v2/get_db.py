import pandas as pd
import os,getpass,traceback,pytz
from sqlalchemy import create_engine
from datetime import datetime,timedelta


def get_DB_connection_from_config_dict(config_dict):
    '''
    Getting the db connection
    '''
    try:
        host = config_dict['db_host']
        port = config_dict['db_port']
        database = config_dict['db_database']
        user = config_dict['db_user']
        password = config_dict['db_pass']

        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
                               executemany_mode='batch')  # sqlalchemy version 1.3.21

        connection = engine.connect()
        print(f'[PID:{os.getpid()}] DB is connected')

        return connection
    except Exception as ex:
        raise ex



# config_dict_dev= {
#             "db_host": "localhost",
#             "db_port": "5432",
#             "db_database": "surendra",
#             "db_user": "postgres",
#             "db_pass": "admin"
#     }

# config_dict_qa= {
#             "db_host": "localhost",
#             "db_port": "5432",
#             "db_database": "surendra",
#             "db_user": "postgres",
#             "db_pass": "admin"
#     }

# con_dev = get_DB_connection_from_config_dict(config_dict_dev)
# con_qa = get_DB_connection_from_config_dict(config_dict_qa)

# query = f"select * from company"
# data = con.execute(query)

# for id, name, age, address, salary, join_date in data:
#     print(name , "-", age)



