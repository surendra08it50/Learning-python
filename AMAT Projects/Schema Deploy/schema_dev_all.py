import pandas as pd
import os,getpass,traceback,pytz
from sqlalchemy import create_engine
from datetime import datetime,timedelta
from get_db import get_DB_connection_from_config_dict


config_dict_dev= {
            "db_host": "dcaldd1016",
            "db_port": "5432",
            "db_database": "learning_rate",
            "db_user": "lr_pguser",
            "db_pass": "lr_pgpass"
    }

con_dev = get_DB_connection_from_config_dict(config_dict_dev)

#################################################################################
# original_table_name = 'suri_drift_op_39cols'
original_table_name_dev = 'suri_drift_ip_58cols'

query = "select column_name, data_type from information_schema.columns where table_name = '"+ original_table_name_dev+"'"
data = con_dev.execute(query)
result =data.fetchall()
dict_cols_dev= dict(result)
set_cols_dev=set(dict_cols_dev.keys())

print("columns od dev server table", dict_cols_dev)
# print(set_cols_dev)







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



