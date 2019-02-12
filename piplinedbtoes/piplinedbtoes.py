import pyodbc 
import json, os
import requests
import collections
import time

start = time.time()

server = 'vminformdev01' 
database = 'GI_VS_SC_Test' 
es_base_url = 'http://10.11.1.70:9200/'
es_index_name = 'mostrecentcfr'
headers = {'Content-Type':'application/json'}

#delete and create index in ES

requests.delete(es_base_url + es_index_name)
requests.put(es_base_url + es_index_name)

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')
cursor = cnxn.cursor()

cursor.execute("SELECT top(100) * FROM [InformaticsLoad].[dbo].[Most Recent CFR activity];") 

objects_list = []
for row in cursor:
    d = collections.OrderedDict()
    d['cfr'] = row.myCFR
    d['Country Code'] = row.__getattribute__('Country Code')
    d['Vessel Name'] = row.__getattribute__('Vessel Name')
    d['Port Code'] = row.__getattribute__('Port Code')
    d['Port Name'] = row.__getattribute__('Port Name')
    d['Loa'] = row.Loa
    d['Lbp'] = row.Lbp
    objects_list.append(d)
    j = json.dumps(objects_list[0])
    r = requests.post(es_base_url + es_index_name + '/_doc?pretty',headers = headers, data = j)  
    objects_list.clear()

cnxn.close()

end = time.time()
print(end - start)
