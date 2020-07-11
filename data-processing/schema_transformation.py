import pandas as pd
from datetime import datetime
import numpy as np
import json

'''
transform the original electricity usage data (accumulative) to 'timestamp','household_id','machine_id','usage'
Unit kwh/min
'''

original_data = pd.read_csv('./data/opsd-household_data-2020-04-15/household_data_1min_singleindex.csv')
#pd.set_option('display.max_columns', None)

original_data = original_data.drop(['cet_cest_timestamp','interpolated'],axis=1)
original_data['starttime'] = pd.to_datetime(original_data['utc_timestamp'])
original_data = original_data.set_index('starttime')
original_data = original_data.drop(['utc_timestamp'],axis=1)
'''original_data is cumulative energy consumption/generation over time, find the difference to get the consumption in minute'''
difference = original_data.diff(axis=0) # the difference between rows
difference = difference.iloc[1:]  # get rid of the first row(empty)


data = difference.to_json(orient='index') # dataframe to json
data = json.loads(data)

# extract machine id, household id from column name
the_list = list()
for dt,dd in data.items():
    for name, usage in dd.items():
        if usage is not None:
            name_list = name.split('_')
            machine_id = '_'.join(name_list[3:])
            household_id = name_list[2]
            date_time=datetime.fromtimestamp(int(dt)/1000.) # change the format of timestamp
            the_list.append([date_time,household_id,machine_id,usage])

new_df=pd.DataFrame(the_list,columns=['timestamp','household_id','machine_id','usage'])
new_df.to_csv('./data/usage_newschema.csv',index=False)

'''
Outliers 
50454793  2017-03-06 06:28:00           pv   industrial2  333.337
50454855  2017-03-06 06:29:00           pv   industrial2  333.338
58765795  2017-07-08 09:01:00  grid_import  residential3  401.970
58765820  2017-07-08 09:02:00  grid_import  residential3  401.966
'''
# remove the outliers
new_df.drop([50454793 , 50454855, 58765795, 58765820], inplace=True)
new_df.to_csv('./data/usage_newschema_no_outlier.csv',index=False)

def columns_adjustment(stat):
    stat = stat.T
    stat = stat.rename(columns={"area_room":"area_room_1","machine":"machine_1",'pv':'pv_3'})
    stat['pv']=stat[['pv_1','pv_2','pv_3']].mean(axis = 1) 
    stat['machine']=stat[['machine_1','machine_2','machine_3','machine_4','machine_5']].mean(axis = 1) 
    stat['area_room']=stat[['area_room_1','area_room_2','area_room_3','area_room_4']].mean(axis = 1) 
    return stat

def get_stat_in_kwh_min(df):
    # change usage to df
    stat_kwh_min=df[df.usage>0].groupby(['machine_id'])['usage'].describe(percentiles=[.03, .1, .25, .5, .75, .9, .97])
    return columns_adjustment(stat_kwh_min)


def get_stat_in_wh_sec(df):
    df['usage']=df['usage'].apply(lambda x: x*1000/60)
    stat_wh_sec=df[df.usage>0].groupby(['machine_id'])['usage'].describe(percentiles=[.03, .1, .25, .5, .75, .9, .97])
    return columns_adjustment(stat_wh_sec)

def get_stats(df):
    stat_kwh_min = get_stat_in_kwh_min(df)
    stat_kwh_min.to_json('./initial_setup/stat_kwh_min_0629.json',orient='columns')
    stat_wh_sec = get_stat_in_wh_sec(df)
    stat_wh_sec.to_json('./initial_setup/stat_wh_sec_0629.json',orient='columns')
   

get_stats(new_df)



