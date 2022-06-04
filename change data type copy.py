import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime,timedelta
import json
from get_db import get_DB_connection_from_config_dict

with open('schema2.json') as json_file:
    ENV_VARIABLES = json.load(json_file)



print(ENV_VARIABLES['postgres_details'])
con_dev = get_DB_connection_from_config_dict(ENV_VARIABLES['postgres_details'])


######################################33
original_table_name_dev='''public."OWPS_DRIFT_ait_owps_summ_ip"'''

backup_table_name = original_table_name_dev.replace("_ip","_ip_backup10may")
query_create_backup_table = "create table IF NOT EXISTS "+backup_table_name +" as select * from " + original_table_name_dev
con_dev.execute(query_create_backup_table)
 #---------------------------------------------------------------

dict_cols_dev= ENV_VARIABLES["suri_drift_ip_58cols"]
set_cols_dev=set(dict_cols_dev.keys())

######update data types on deployment server
for col_name,data_type in dict_cols_dev.items():
    query_update_data_type = "ALTER TABLE "+ original_table_name_dev+" ALTER COLUMN "+ col_name +" TYPE "+ data_type +" USING "+ col_name +"::"+ data_type +""  
    con_dev.execute(query_update_data_type)
















