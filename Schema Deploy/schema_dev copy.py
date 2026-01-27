import pandas as pd
import os,getpass,traceback,pytz
from sqlalchemy import create_engine
from datetime import datetime,timedelta
from get_db import con_dev

# def get_DB_connection_from_config_dict(config_dict):
#     '''
#     Getting the db connection
#     '''
#     try:
#         host = config_dict['db_host']
#         port = config_dict['db_port']
#         database = config_dict['db_database']
#         user = config_dict['db_user']
#         password = config_dict['db_pass']

#         engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
#                                executemany_mode='batch')  # sqlalchemy version 1.3.21

#         connection = engine.connect()
#         print(f'[PID:{os.getpid()}] DB is connected')

#         return connection
#     except Exception as ex:
#         raise ex



# config_dict= {
#             "db_host": "localhost",
#             "db_port": "5432",
#             "db_database": "surendra",
#             "db_user": "postgres",
#             "db_pass": "admin"
#     }


config_dict= {
            "db_host": "dcaldd1016",
            "db_port": "5432",
            "db_database": "learning_rate",
            "db_user": "lr_pguser",
            "db_pass": "lr_pgpass"
    }

# con = get_DB_connection_from_config_dict(config_dict)
# query = f"select * from company"
# data = con.execute(query)

# for id, name, age, address, salary, join_date in data:
#     print(name , "-", age)

#################################################################################
original_table_name = 'suri_drift_op_39cols'

query = "select column_name, data_type from information_schema.columns where table_name = '"+ original_table_name+"'"
data = con_dev.execute(query)
result =data.fetchall()
dict_cols_dev= dict(result)
set_cols_dev=set(dict_cols_dev.keys())
# for r in data:
#     print(r)


# print(dict_cols_dev)
# print(set_cols_dev)


########### create backup table ################
backup_table_name = original_table_name + "_backup"
query_create_backup_table = "create table IF NOT EXISTS "+backup_table_name +" as select * from " + original_table_name
con_dev.execute(query_create_backup_table)




#################################################################################










# original_table_name = "suri_drift_op"
# backup_table_name = original_table_name + "_backup"
# temp_table_name = original_table_name+ "_temp"

########### before deploy new version################
# query_create_backup_table = "create table IF NOT EXISTS "+backup_table_name +" as select * from " + original_table_name
# con.execute(query_create_backup_table)

############ post deploy new version #################
# query_create_temp_table = "create table "+temp_table_name +" as select * from "+ original_table_name+" where 1=2"
# con.execute(query_create_temp_table)


# query_insert_temp_table = "insert into "+ temp_table_name +" select * from " + backup_table_name
# con.execute(query_insert_temp_table)


# query_insert_original_table = "insert into "+ original_table_name +" select * from " + temp_table_name
# con.execute(query_insert_original_table)



