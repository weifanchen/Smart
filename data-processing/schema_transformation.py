import pandas as pd
from datetime import datetime
import numpy as np
import json

'''
transform the original electricity usage data (accumulative) to 'timestamp','household_id','machine_id','usage'

'''

house = pd.read_csv('./data/opsd-household_data-2020-04-15/household_data_1min_singleindex.csv')
#pd.set_option('display.max_columns', None)

#house.shape = (2307133, 71)

house_small = house
house_small = house_small.drop(['cet_cest_timestamp'],axis=1)
house_small = house_small.drop(['interpolated'],axis=1)
#house_small.sum(axis=1)
house_small['starttime'] = pd.to_datetime(house_small['utc_timestamp'])
house_small = house_small.set_index('starttime')
house_small = house_small.drop(['utc_timestamp'],axis=1)
difference = house_small.diff(axis=0)
difference = difference.iloc[1:]

# get rid of the first row
# if the whole row is zero, return 0, not None
#checking = difference.sum(axis=1)

# at least one column in each row is not None
#test = difference.isnull().all(axis=1) 
# test.sum() = 0, 

# fill in NaN ? 

# difference.iloc[:100000].to_json('./house_newschema.json',orient='index')
# with open('./house_newschema.json') as j:
#    data = json.load(j)
data = difference.to_json(orient='index')
data = json.loads(data)

the_list = list()
for dt,dd in data.items():
    for name, usage in dd.items():
        if usage is not None:
            #print(name,usage)
            name_list = name.split('_')
            machine_id = '_'.join(name_list[3:])
            household_id = name_list[2]
            date_time=datetime.fromtimestamp(int(dt)/1000.) 
            the_list.append([date_time,household_id,machine_id,usage])

new_df=pd.DataFrame(the_list,columns=['timestamp','household_id','machine_id','usage'])
new_df.to_csv('./usage_newschema.csv',index=False)
#test=new_df[(new_df.machine_id=='residential6') & (new_df.timestamp.between('2015-01-15 04:00:00', '2015-01-15 05:00:00'))]

#data.rename(columns={'household_id':'machine_id','machine_id':'household_id'}, inplace=True)

'''
stat = pd.read_json('./stat_old.json')
new_stat = stat.apply(lambda x:x*1000/60)
new_stat = new_stat.rename(columns={"area_room":"area_room_1","machine":"machine_1",'pv':'pv_3'})
new_stat
new_stat['pv']=new_stat[['pv_1','pv_2','pv_3']].mean(axis = 1) 
new_stat['machine']=new_stat[['machine_1','machine_2','machine_3','machine_4','machine_5']].mean(axis = 1) 
new_stat['area_room']=new_stat[['area_room_1','area_room_2','area_room_3','area_room_4']].mean(axis = 1) 

new_stat.to_json('./stat.json',orient='columns')

'''