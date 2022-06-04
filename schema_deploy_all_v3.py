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

config_dict_deploy= {
            "db_host": "dcaldd1016",
            "db_port": "5432",
            "db_database": "learning_rate",
            "db_user": "lr_pguser",
            "db_pass": "lr_pgpass"
    }

con = get_DB_connection_from_config_dict(config_dict_deploy)

###*********************** DEV SERVER *******************************************************************
def schema_change(old_table, new_table):
    #try:
    
    # # original_table_name_dev = 'suri_drift_op_39cols'
    original_table_name_dev = new_table

    query = "select column_name, data_type from information_schema.columns where table_name = '"+ original_table_name_dev+"'"
    data = con_dev.execute(query)
    result =data.fetchall()
    dict_cols_dev= dict(result)
    set_cols_dev=set(dict_cols_dev.keys())

    print("columns on dev server table", dict_cols_dev)
    # print(set_cols_dev)



    # ###************************ DEPLOYMENT SERVER ******************************************************************

    # ########### create backup table ################
    # original_table_name = 'suri_drift_op_21cols'
    original_table_name = old_table

    backup_table_name = original_table_name + "_backup"
    query_create_backup_table = "create table IF NOT EXISTS "+backup_table_name +" as select * from " + original_table_name
    con.execute(query_create_backup_table)
    ####################################################################################


    query = "select column_name, data_type from information_schema.columns where table_name = '"+ original_table_name+"'"
    data = con.execute(query)
    result =data.fetchall()
    dict_cols_deploy= dict(result)
    set_cols_deploy=set(dict_cols_deploy.keys())

    new_cols_to_add= list(set_cols_dev-set_cols_deploy)
    
    new_cols_to_add_dict ={}
    for col_name in new_cols_to_add:
        new_cols_to_add_dict[col_name]=dict_cols_dev[col_name]

    print("new columns to add : ",new_cols_to_add)

    print("columns of deployment server tables :", dict_cols_deploy)


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
            print("error:", str(e))
            # pass

    print(data_type_to_update)

    # ######update data types on deployment server
    for col_name,data_type in data_type_to_update.items():
        query_update_data_type = "ALTER TABLE "+ original_table_name+" ALTER COLUMN "+ col_name +" TYPE "+ data_type +" USING "+ col_name +"::"+ data_type +""  
        con.execute(query_update_data_type)



    #######################################################################
    input_table_name="suri_drift_ip_34cols"

    query = "select column_name, data_type from information_schema.columns where table_name = '"+ input_table_name+"'"
    data = con.execute(query)
    result =data.fetchall()
    dict_cols_input= dict(result)
    set_cols_input=set(dict_cols_input.keys())

    common_columns = tuple(set(new_cols_to_add).intersection(set_cols_input))

    print(common_columns)

    for col_name in common_columns:

        if col_name =="stepname":
            # pass    
            query_update =f"""update {original_table_name}
            set {col_name} = t2.{col_name}
            from
            (
            select distinct cast({col_name} as {dict_cols_dev[col_name]}), step, partitioned_by
            from {input_table_name}
            where step not in ('PR', 'PS', 'PQ')
            ) t2
            where
            cast("{original_table_name}".step as FLOAT) = cast(t2.step as FLOAT) and
            "{original_table_name}".partitioned_by = t2.partitioned_by"""

            # print(query_update)
            con.execute(query_update)
        else:
            query_update =f"""update {original_table_name}
            set {col_name} = t2.{col_name}
            from
            (
            select distinct cast({col_name} as {dict_cols_dev[col_name]}), partitioned_by
            from {input_table_name}
            ) t2
            where
            {original_table_name}.partitioned_by = t2.partitioned_by"""

            # print(query_update)
            con.execute(query_update)
    
    print("Done!")
    con.close()
    con_dev.close()

    # except Exception as e:
    #     print(str(e))
    # finally:
    #     con.close()
    #     con_dev.close()


#### calling function here

# schema_change("suri_drift_ip_34cols", "suri_drift_ip_58cols")

schema_change("suri_drift_op_21cols", "suri_drift_op_39cols")
















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



