
# import necessary libraries
import pandas as pd
import os
import glob
  
  
# use glob to get all the csv files 
# in the folder
path = os.getcwd()
parquet_files = glob.glob(os.path.join(path, "*.parquet"))

f ='output_EL11_CHC2_owps-algorithm_EL11_CHC2_Slot1_Side1_2022-03-30_20-55-46_MNguyen%N3 LA%PE-LA-M0EA530PD42-non-actinium.parquet'
  
df = pd.read_parquet(f)

df.Loop = pd.to_numeric(df.Loop, errors='coerce').astype('Int64')
# df['Loop'] = df['Loop'].astype(int)
print(df.Loop.sum())
# if (df['Loop'].notnull().sum() != 0) :
#         print('File Name:', f.split("\\")[-1])
#         print(df['Loop'].notnull().sum())





# loop over the list of csv files
# for f in parquet_files:
      
#     # read the csv file
#     df = pd.read_parquet(f)

#     #print(df.StartTime)
#     #print(df.error_event)
#     #print(df.loc[~df.error_event.isnull()], 'error_event')

#     #print(df[~df.error_event.isnull()].error_event)

#     file_name = f.split("\\")[-1]
#     file_name2 = file_name.split(".")[0]

#     # df.to_csv(file_name2+".csv" )

#     # print('File Name:', file_name2)
#     #print('File Name:', f.split("\\")[-1])
    
#     # if (df['error_event'].notna().sum() == 0) :
#     #     print('File Name:', f.split("\\")[-1])

    
#     df['Loop'] = df['Loop'].astype(int)
#     # if (df['Loop'].notnull().sum() != 0) :
#     #     print('File Name:', f.split("\\")[-1])
#     #     print(df['Loop'].notnull().sum())

#     # print(df['Loop'].notna().sum())

#     # # print the location and filename
#     # print('Location:', f)
    
      
#     # # print the content
#     # print('Content:')
#     # #display(df)
#     # print()