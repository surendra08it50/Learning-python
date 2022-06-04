import json
from get_db import get_DB_connection_from_config_dict

with open('schema3.json') as json_file:
    ENV_VARIABLES = json.load(json_file)

con = get_DB_connection_from_config_dict(ENV_VARIABLES['postgres_details'])

print(ENV_VARIABLES['output_table_name'])
output_table_name=ENV_VARIABLES['output_table_name']
input_table_name=ENV_VARIABLES['input_table_name']

###################################################################################
def run_b4_deployment(table_name_old):

    original_table_name = table_name_old           
    backup_table_name = original_table_name + "_backup"

    print(backup_table_name)
    
    ########### before deployment of new version################

    # take backup of original table before deployment of new version
    query_create_backup_table = f"""create table IF NOT EXISTS public."{backup_table_name}" as select * from public."{original_table_name}" """
    con.execute(query_create_backup_table)

    #drop original table 
    # query_drop_orig_table = "drop table " + original_table_name 
    # con.execute(query_drop_orig_table)


run_b4_deployment(output_table_name)
# run_b4_deployment(input_table_name)
con.close()