import pandas as pd
import os,getpass,traceback,pytz
from sqlalchemy import create_engine
from datetime import datetime,timedelta
from get_db import get_DB_connection_from_config_dict
from schema_dev import set_cols_dev, dict_cols_dev



config_dict= {
            "db_host": "dcaldd1016",
            "db_port": "5432",
            "db_database": "learning_rate",
            "db_user": "lr_pguser",
            "db_pass": "lr_pgpass"
    }

con = get_DB_connection_from_config_dict(config_dict)

####################################################################################
original_table_name = 'suri_drift_op_25cols'

query = "select column_name, data_type from information_schema.columns where table_name = '"+ original_table_name+"'"
data = con.execute(query)
result =data.fetchall()
dict_cols_deploy= dict(result)
set_cols_deploy=set(dict_cols_deploy.keys())
# for r in data:
#     print(r)

new_cols_to_add= list(set_cols_dev-set_cols_deploy)
# print(new_cols_to_add)

print(set_cols_deploy)

# col1='chamber_slot'
# print(dict_cols_dev[col1])


### add new columns on deployment server

for col in new_cols_to_add:
    query_add_cols = "ALTER TABLE " + original_table_name+" ADD COLUMN "+ col+ " "+ dict_cols_dev[col]
    con.execute(query_add_cols)


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
    con.execute(query_update_data_type)



# #insert into new original table from temp table
table_39cols = 'suri_drift_op_39cols'
query_insert_original_table2 = "insert into "+ original_table_name +" select * from " + table_39cols
con.execute(query_insert_original_table2)




















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



