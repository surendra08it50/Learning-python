import pandas as pd
import os,getpass,traceback,pytz
from sqlalchemy import create_engine
from datetime import datetime,timedelta
from get_db import get_DB_connection_from_config_dict


config_dict= {
            "db_host": "dcaldd1016",
            "db_port": "5432",
            "db_database": "learning_rate",
            "db_user": "lr_pguser",
            "db_pass": "lr_pgpass"
    }

con = get_DB_connection_from_config_dict(config_dict)

###################################################################################
original_table_name = 'suri_drift_op_21cols'
new_table_name = "suri_drift_op_39cols"
backup_table_name = original_table_name + "_backup"
temp_table_name = new_table_name+ "_temp"


########### before deployment of new version################

# take backup of original table before deployment of new version
query_create_backup_table = "create table IF NOT EXISTS "+backup_table_name +" as select * from " + original_table_name
con.execute(query_create_backup_table)

#drop original table 
# query_drop_orig_table = "drop table " + original_table_name 
# con.execute(query_drop_orig_table)

########### post deployment of new version #################

#'''' run the recipe of new version to create new tables 

#create blank temp table from new version table
query_create_temp_table = "create table IF NOT EXISTS "+temp_table_name +" as select * from "+ new_table_name+" where 1=2"
con.execute(query_create_temp_table)

##############################################################################
# getting data
query = "select column_name, data_type from information_schema.columns where table_name = '"+ temp_table_name+"'"
data = con.execute(query)
result =data.fetchall()
dict_cols_temp= dict(result)
set_cols_temp=set(dict_cols_temp.keys())



query = "select column_name, data_type from information_schema.columns where table_name = '"+ backup_table_name+"'"
data = con.execute(query)
result =data.fetchall()
dict_cols_backup= dict(result)
set_cols_backup=set(dict_cols_backup.keys())
# for r in data:
#     print(r)

new_cols_to_add= list(set_cols_temp-set_cols_backup)
common_columns = tuple(set_cols_temp.intersection(set_cols_backup))

new_cols_to_add_dict ={}
for col_name in new_cols_to_add:
    new_cols_to_add_dict[col_name]="TEXT"


print("---new columns to add--")
print(new_cols_to_add)

######################################
#date type column in temp table that is not present in original table
date_type_cols={}
for col_name in new_cols_to_add:
    try:
        if dict_cols_temp[col_name]=="timestamp without time zone":
            date_type_cols[col_name] = 'text'
    except Exception as e:
        # print("error:", str(e))
        pass 


print("date type column ", date_type_cols)


# ### add new columns in temp table

# for col in new_cols_to_add:
#     query_add_cols = "ALTER TABLE " + temp_table_name+" ADD COLUMN "+ col+ " "+ dict_cols_temp[col]
#     con.execute(query_add_cols)


##### find out the data type difference between temp table and old backup table
data_type_to_update={}
for col_name,data_type in dict_cols_backup.items():
    try:
        if dict_cols_backup[col_name]!=dict_cols_temp[col_name]:
            data_type_to_update[col_name] = dict_cols_backup[col_name]
    except Exception as e:
        # print("error:", str(e))
        pass

print(data_type_to_update)

# ######update data types of temp table

data_type_to_update.update(new_cols_to_add_dict)
print("final columns to update data types:", data_type_to_update)
for col_name,data_type in data_type_to_update.items():
    query_update_data_type = "ALTER TABLE "+ temp_table_name+" ALTER COLUMN "+ col_name +" TYPE "+ data_type +" USING "+ col_name +"::"+ data_type +""  
    con.execute(query_update_data_type)


##############################################################################
print(common_columns)
#insert into temp table from backup table, old data 
query_insert_temp_table = "insert into "+ temp_table_name +" "+ str(common_columns) +" select "+ str(common_columns) +" from " + backup_table_name
query_insert_temp_table=query_insert_temp_table.replace("'","")
query_insert_temp_table=query_insert_temp_table.replace("select (","select ")
query_insert_temp_table=query_insert_temp_table.replace(") from", " from")

con.execute(query_insert_temp_table)


# ######update data types of temp table to its original format

for col_name,data_type in dict_cols_temp.items():
    query_update_data_type = "ALTER TABLE "+ temp_table_name+" ALTER COLUMN "+ col_name +" TYPE "+ data_type +" USING "+ col_name +"::"+ data_type +""  
    con.execute(query_update_data_type)

# #insert into new original table from temp table
query_insert_original_table = "insert into "+ new_table_name +" select * from " + temp_table_name
con.execute(query_insert_original_table)


con.close()









































