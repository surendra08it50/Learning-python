import pandas as pd
import os,getpass,traceback,pytz
from sqlalchemy import create_engine
from datetime import datetime,timedelta
from get_db import con_qa

from schema_dev import set_cols_dev, dict_cols_dev
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

####################################################################################
original_table_name = 'suri_drift_op_25cols'

query = "select column_name, data_type from information_schema.columns where table_name = '"+ original_table_name+"'"
data = con_qa.execute(query)
result =data.fetchall()
dict_cols_deploy= dict(result)
set_cols_deploy=set(dict_cols_deploy.keys())
# for r in data:
#     print(r)

new_cols_to_add= list(set_cols_dev-set_cols_deploy)
# print(new_cols_to_add)

# print(set_cols_deploy)

# col1='chamber_slot'
# print(dict_cols_dev[col1])


### add new columns on deployment server

for col in new_cols_to_add:
    query_add_cols = "ALTER TABLE " + original_table_name+" ADD COLUMN "+ col+ " "+ dict_cols_dev[col]
    con_qa.execute(query_add_cols)


##### find out the data type difference
data_type_to_update={}
for col_name,data_type in dict_cols_deploy.items():
    try:
        if dict_cols_deploy[col_name]!=dict_cols_dev[col_name]:
            data_type_to_update[col_name] = dict_cols_dev[col_name]
    except Exception as e:
        # print("error:", str(e))
        pass

print(data_type_to_update)

# ######update data types on deployment server
for col_name,data_type in data_type_to_update.items():
    query_update_data_type = "ALTER TABLE "+ original_table_name+" ALTER COLUMN "+ col_name +" TYPE "+ data_type +" USING "+ col_name +"::"+ data_type +""  
    con_qa.execute(query_update_data_type)






















###################################################################################

# backup_table_name = original_table_name + "_backup"
# temp_table_name = original_table_name+ "_temp"

############ before deploy new version################
# query_create_backup_table = "create table "+backup_table_name +" as select * from " + original_table_name
# con.execute(query_create_backup_table)

############ post deploy new version #################
# query_create_temp_table = "create table "+temp_table_name +" as select * from "+ original_table_name+" where 1=2"
# con.execute(query_create_temp_table)


# query_insert_temp_table = "insert into "+ temp_table_name +" select * from " + backup_table_name
# con.execute(query_insert_temp_table)


# query_insert_original_table = "insert into "+ original_table_name +" select * from " + temp_table_name
# con.execute(query_insert_original_table)



