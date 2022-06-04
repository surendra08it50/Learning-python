import json
from get_db import get_DB_connection_from_config_dict

with open('schema3.json') as json_file:
    ENV_VARIABLES = json.load(json_file)

con = get_DB_connection_from_config_dict(ENV_VARIABLES['postgres_details'])


###################################################################################
def run_post_deployment(table_name_new, table_name_old):

    original_table_name = table_name_old
    new_table_name = table_name_new
    backup_table_name = original_table_name + "_backup"
    temp_table_name = new_table_name+ "_temp"


    ########### post deployment of new version #################

    ######## run the recipe of new version to create new tables 

    #create blank temp table from new version table
    query_create_temp_table = "create table IF NOT EXISTS "+temp_table_name +" as select * from "+ new_table_name+" where 1=2"
    con.execute(query_create_temp_table)

    ##############################################################################
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

    ##### find out the data type difference between temp table and old backup table
    data_type_to_update={}
    for col_name,data_type in dict_cols_backup.items():
        try:
            if dict_cols_backup[col_name]!=dict_cols_temp[col_name]:
                data_type_to_update[col_name] = dict_cols_backup[col_name]
        except Exception as e:
            print("error:", str(e))
            # pass

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

    #drop temp table 
    # query_drop_temp_table = "drop table " + temp_table_name 
    # con.execute(query_drop_temp_table)


run_post_deployment("suri_drift_op_39cols", "suri_drift_op_21cols")







































