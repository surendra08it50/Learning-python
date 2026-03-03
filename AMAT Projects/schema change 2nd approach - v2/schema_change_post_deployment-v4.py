import json

from sqlalchemy import false
from get_db import get_DB_connection_from_config_dict

with open('schema-v4.json') as json_file:
    ENV_VARIABLES = json.load(json_file)

con = get_DB_connection_from_config_dict(ENV_VARIABLES['postgres_details'])

output_table_name=ENV_VARIABLES['output_table_name']
output_table_name_new=ENV_VARIABLES['output_table_name_new']
input_table_name=ENV_VARIABLES['input_table_name']
input_table_name_new=ENV_VARIABLES['input_table_name_new']
data_type_mapping=ENV_VARIABLES['data_type_mapping']
data_type_mapping_v4=ENV_VARIABLES['data_type_mapping_v4']
def check_data_type(new_data_type, old_data_type):
    if data_type_mapping_v4[new_data_type] < data_type_mapping_v4[old_data_type]:
        return True
    else:
        return False


print(data_type_mapping_v4)
###################################################################################
def run_post_deployment(table_name_new, table_name_old):

    original_table_name = table_name_old
    new_table_name = table_name_new
    backup_table_name = original_table_name + "_backup"
    
    ########### post deployment of new version #################

    # query = "select column_name, data_type from information_schema.columns where table_name = '"+ new_table_name+"'"
    # data = con.execute(query)
    # result =data.fetchall()
    # dict_cols_temp= dict(result)
    # set_cols_temp=set(dict_cols_temp.keys())
    dict_cols_temp= ENV_VARIABLES[table_name_old]
    set_cols_temp=set(dict_cols_temp.keys())

    # print('set_cols_temp---', set_cols_temp)


    query = "select column_name, data_type from information_schema.columns where table_name = '"+ backup_table_name+"'"
    data = con.execute(query)
    result =data.fetchall()
    dict_cols_backup= dict(result)
    set_cols_backup=set(dict_cols_backup.keys())
    
    # print('set_cols_backup--', set_cols_backup)

    new_cols_to_add= list(set_cols_temp-set_cols_backup)
    common_columns = tuple(set_cols_temp.intersection(set_cols_backup))

    # print('----common columns in both of the tables:--', common_columns)

    common_columns_dict={}
    for col in common_columns:
        common_columns_dict[col]=dict_cols_temp[col]

    print("--------------------------------------------")
    print("----common columns in both of the tables--:", common_columns_dict)
   
    ##### find out the data type difference between new table and old backup table
    data_type_to_update={}
    for col_name,data_type in dict_cols_backup.items():
        try:
            if dict_cols_backup[col_name]!=dict_cols_temp[col_name]:
                data_type_to_update[col_name] = dict_cols_temp[col_name]
        except Exception as e:
            print("error:", str(e))
            # pass

    print('-----data type difference between two tables---:', data_type_to_update)

    ########################## prepare the insert query in fly####################################################
    list_cast_columns=[]
    for col,dtype in common_columns_dict.items():
        st =f"""cast({col} as {dtype})"""
        if ((col in data_type_to_update.keys()) and check_data_type(dict_cols_temp[col], dict_cols_backup[col]) ):
                list_cast_columns.append(st)
        else:
             list_cast_columns.append(col)   
    
    tuple_cast_columns=tuple(list_cast_columns)   

    ############# insert into new table from backup table, old data ##########################
    query_insert_temp_table = f"""insert into public."{new_table_name}" {str(common_columns)} select {str(tuple_cast_columns)} from public."{backup_table_name}" """
    query_insert_temp_table=query_insert_temp_table.replace("'","").replace("select (","select ").replace(") from", " from")
    print("-------insert query -----: ", query_insert_temp_table)
    con.execute(query_insert_temp_table)

    

run_post_deployment(output_table_name_new, output_table_name)
# run_post_deployment(input_table_name_new, input_table_name)
