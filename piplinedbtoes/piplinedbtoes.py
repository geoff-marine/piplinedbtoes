import pyodbc 
import json, os
import requests
import collections

server = 'vminformdev01' 
database = 'GI_VS_SC_Test' 
es_base_url = 'http://10.11.1.70:9200/'
es_index_name = 'foo'

#delete and create index in ES

requests.delete(es_base_url + es_index_name)
requests.put(es_base_url + es_index_name)

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')
cursor = cnxn.cursor()

cursor.execute("SELECT * FROM FOO;") 

objects_list = []
for row in cursor:
    d = collections.OrderedDict()
    d['fookey'] = row.fooKey
    d['foovalue'] = row.foofooValue
    d['anotherValue'] = row.anotherValue
    objects_list.append(d)
    j = json.dumps(objects_list[0])
    requests.post(es_base_url + '/_doc?pretty',json = j)   
    objects_list.clear()

cnxn.close()
