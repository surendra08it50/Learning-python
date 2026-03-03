import json
from get_db import get_DB_connection_from_config_dict

with open('schema3.json') as json_file:
    ENV_VARIABLES = json.load(json_file)

con = get_DB_connection_from_config_dict(ENV_VARIABLES['postgres_details'])

output_table_name=ENV_VARIABLES['output_table_name']
output_table_name_new=ENV_VARIABLES['output_table_name_new']
input_table_name=ENV_VARIABLES['input_table_name']
input_table_name_new=ENV_VARIABLES['input_table_name_new']
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

    print('set_cols_temp---', set_cols_temp)


    query = "select column_name, data_type from information_schema.columns where table_name = '"+ backup_table_name+"'"
    data = con.execute(query)
    result =data.fetchall()
    dict_cols_backup= dict(result)
    set_cols_backup=set(dict_cols_backup.keys())
    
    print('set_cols_backup--', set_cols_backup)

    new_cols_to_add= list(set_cols_temp-set_cols_backup)
    common_columns = tuple(set_cols_temp.intersection(set_cols_backup))

    print('common_columns--', common_columns)

    common_columns_dict={}
    for col in common_columns:
        common_columns_dict[col]=dict_cols_temp[col]

    print("--------------------------------------------")
    print("common_columns_dict", common_columns_dict)
   
    ##### find out the data type difference between new table and old backup table
    data_type_to_update={}
    for col_name,data_type in dict_cols_backup.items():
        try:
            if dict_cols_backup[col_name]!=dict_cols_temp[col_name]:
                data_type_to_update[col_name] = dict_cols_temp[col_name]
        except Exception as e:
            print("error:", str(e))
            # pass

    print('data_type_to_update---', data_type_to_update)

    ##############################################################################
    list_cast_columns=[]
    for k,v in common_columns_dict.items():
        st =f"""cast({k} as {v})"""
        if k in data_type_to_update.keys():
            list_cast_columns.append(st)
        else:
             list_cast_columns.append(k)   
    print(list_cast_columns)
    tuple_cast_columns=tuple(list_cast_columns)
    #insert into new table from backup table, old data 
    query_insert_temp_table = f"""insert into public."{new_table_name}" {str(common_columns)} select {str(tuple_cast_columns)} from public."{backup_table_name}" """
    query_insert_temp_table=query_insert_temp_table.replace("'","")
    query_insert_temp_table=query_insert_temp_table.replace("select (","select ")
    query_insert_temp_table=query_insert_temp_table.replace(") from", " from")
    print(query_insert_temp_table)
    con.execute(query_insert_temp_table)

    # ########################## update columns of output table from input table###############################
    input_table_name="OWPS_DRIFT_ait_owps_summ_ip_58"

    query = "select column_name, data_type from information_schema.columns where table_name = '"+ input_table_name+"'"
    data = con.execute(query)
    result =data.fetchall()
    dict_cols_input= dict(result)
    set_cols_input=set(dict_cols_input.keys())

    common_columns = tuple(set(new_cols_to_add).intersection(set_cols_input))

    print(common_columns)

    for col_name in common_columns:        
        if col_name =="stepname":
            query_update =f"""update public."{new_table_name}"
            set {col_name} = cast(t2.{col_name} as {dict_cols_temp[col_name]})
            FROM public."{new_table_name}" t1 INNER JOIN "{input_table_name}" t2
            ON t1.partitioned_by = t2.partitioned_by and t1.step not in ('PR', 'PS', 'PQ')            
            """  
        else:
            query_update =f"""update public."{new_table_name}"
            set {col_name} = cast(t2.{col_name} as {dict_cols_temp[col_name]})
            FROM public."{new_table_name}" t1 INNER JOIN "{input_table_name}" t2
            ON t1.partitioned_by = t2.partitioned_by"""       
        
        

        print(query_update)
        con.execute(query_update)


run_post_deployment(output_table_name_new, output_table_name)
# run_post_deployment(input_table_name_new, input_table_name)







































