'''
    Run this Script as a cron job for the required frequency i.e hourly, every minute, every hour, etc
    see cron manual for setting up
    Proprietory code of Bishwa Bikash Das
'''

import mysql.connector

con = mysql.connector.connect(user='IOT_master', password='IOT_pass1234##', host='127.0.0.1', database='iot_data')
cursor = con.cursor()

query = (
    "SELECT DISTINCT a.device_ID AS 'dev1', b.device_ID AS 'dev2',abs(timestampdiff(SECOND,a.time_stamp,b.time_stamp)) AS 'devdiff', b.time_stamp AS 'dev2_ts',a.time_stamp AS 'dev1_ts',a.cluster_ID AS 'cluster' FROM sensor_data a,sensor_data b WHERE abs(timestampdiff(SECOND,a.time_stamp,b.time_stamp)) <3 AND a.device_ID <> b.device_ID  AND a.time_stamp < b.time_stamp AND a.cluster_ID=b.cluster_ID ")  # and a.time_stamp>= Date_SUB(NOW(),INTERVAL 1 HOUR)")
'''
query=("SELECT DISTINCT a.device_ID AS 'First Device to be triggered',a.RFID + b.RFID as "subject RFID",a.time_stamp AS 'event timestamp',a.cluster_ID AS 'cluster' FROM sensor_data a,sensor_data b WHERE abs(timestampdiff(SECOND,a.time_stamp,b.time_stamp)) <3 AND a.device_ID <> b.device_ID  AND a.time_stamp < b.time_stamp AND a.cluster_ID=b.cluster_ID") # and a.time_stamp>= Date_SUB(NOW(),INTERVAL 1 HOUR)")
'''
cursor.execute(query)

'''
 Iterate through all the columns satisfying the query and return the number of entries or exits in the past hour
'''
data_dict = {}
for (dev1, dev2, devdiff, dev1_ta, dev2_ta, cluster) in cursor:
    if int(dev1[-1]) % 2 == 0:
        if cluster in data_dict:
            data_dict[cluster] += 1
        else:
            data_dict[cluster] = 1
    else:
        if cluster in data_dict:
            data_dict[cluster] -=1
        else:
            data_dict[cluster] = -1

print(data_dict)
cursor.close()
con.close()
