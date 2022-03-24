import ast
import pandas as pd
import glob
import os
import pyodbc 
import numpy as np


conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-ESEEIKP;'
                      'Database=VeReMi;'
                      'Trusted_Connection=yes;')
path = "C:/Users/Mahshid/Desktop/ensemble/veremi/AttackType4/density5/results15/" 
table_name_ref='at4_dens5_res15'  
files = glob.glob(path)
dense=5
attack_type=4
print(table_name_ref)

# =====================only one time=============================================
cursor = conn.cursor()
create_table_truth='''CREATE TABLE [{}] (
        	type int,
        	time float,
            sender int,
            attackerType int,
            messageID int,
        	[pos-x2] float,
        	[pos-y2] float,
        	[pos-z2] float,
        	[spd-x2] float,
        	[spd-y2] float,
        	[spd-z2] float
    			)'''.format('GroundTruthJSONlog_'+table_name_ref)
cursor.execute(create_table_truth)
conn.commit()

create_table='''CREATE TABLE [{}] (
        	type int,
        	rcvTime float,
        	[pos-x] float,
        	[pos-y] float,
        	[pos-z] float,
        	[spd-x] float,
        	[spd-y] float,
        	[spd-z] float,
        	sendTime float,
        	sender int,
        	messageID int,
        	RSSI float,
            dense int,
            [my_attackType] int)'''.format(table_name_ref)
cursor.execute(create_table)
conn.commit()
# =============================================================================

txtfiles = []
for file in glob.glob(path + "\\*.json"):
    txtfiles.append(file)

for f in txtfiles:
    raw_data = [ ast.literal_eval(line) for line in open(f) ]
    df=pd.DataFrame(raw_data)
    cursor = conn.cursor()
    if 'noise' in df :
        df.drop(['noise'], inplace=True, axis = 1)
    if 'spd_noise' in df:
        df.drop(['spd_noise'], inplace=True, axis = 1)
    if  'pos_noise' in df:
        df.drop(['pos_noise'], inplace=True, axis = 1)

        
    if  'GroundTruthJSONlog' in  os.path.basename(f):
        data=pd.concat([df.iloc[:,:5], df.pos.apply(pd.Series,index= ['pos-x','pos-y','pos-z']),df.spd.apply(pd.Series,index= ['spd-x','spd-y','spd-z'])],axis=1)
        data.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        data = data.fillna(0)
        if 'messageID' in data:
            insert_value='''INSERT INTO [{}] (
        	type ,
        	time ,
            sender ,
            attackerType,
            messageID ,
        	[pos-x2] ,
        	[pos-y2] ,
        	[pos-z2] ,
        	[spd-x2] ,
        	[spd-y2] ,
        	[spd-z2] )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            '''.format('GroundTruthJSONlog_'+ table_name_ref)
            cursor.executemany(insert_value, data.values.tolist())
            conn.commit()
        
       
    else:
        data=pd.concat([df.iloc[:,:2], df.pos.apply(pd.Series,index= ['pos-x','pos-y','pos-z']),df.spd.apply(pd.Series,index= ['spd-x','spd-y','spd-z']),df.iloc[:,4:]],axis=1)
        data.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        data = data.fillna(0)
        data['dense']=dense
        data['my_attackType']=attack_type
        if 'messageID' in data:
            insert_value='''INSERT INTO [{}] (
        	type ,
        	rcvTime ,
        	[pos-x] ,
        	[pos-y] ,
        	[pos-z] ,
        	[spd-x] ,
        	[spd-y] ,
        	[spd-z] ,
        	sendTime ,
        	sender ,
        	messageID ,
        	RSSI,
            dense,
            [my_attackType])
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            '''.format(table_name_ref)
            cursor.executemany(insert_value, data.values.tolist())
            conn.commit()




