import pyodbc 
import json, os
import requests
import collections
import time

start = time.time()

server = 'vminformdev01' 
database = 'GI_VS_SC_Test' 
es_base_url = 'http://10.11.1.70:9200/'
es_index_name = 'vessels/'
es_type_name = 'mostrecentcfr/'
headers = {'Content-Type':'application/json'}
es_input_open = '{"input":['
es_input_close = ']}' 

es_init_for_auto_complete = '{"mappings":{"mostrecentcfr":{"properties":{"Vessel Name":{"type":"completion","contexts":[{"name":"Country Code","type":"category","path":"Country Code"}]},"cfr":{"type":"keyword"},"Country Code":{"type": "keyword"},"Port Code" : {"type": "keyword"},"Port Name" : {"type": "keyword"},"Loa" : {"type": "keyword"},"Lbp" : {"type": "keyword"}}}}}'
#requests.delete(es_base_url + es_index_name)
requests.put(es_base_url + es_index_name, headers = headers, data = es_init_for_auto_complete)

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')
cursor = cnxn.cursor()

cursor.execute("SELECT [myCFR], [Country Code], [Vessel Name], [Port Code], [Port Name], [Loa], [Lbp] FROM [InformaticsLoad].[dbo].[Most Recent CFR activity];") 
#cursor.execute("SELECT top(10) * FROM [InformaticsLoad].[dbo].[Most Recent CFR activity];") 


objects_list = []
collection_objects =[]

while True:
    results = cursor.fetchmany(20000)
    if not results:
        break
    for row in results:
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
        start_vess_name = j.find('"Vessel Name":')
        end_vess_name = j.find( ', "Port Code":')
        open_mod_j = j[:start_vess_name + 14] + es_input_open + j[start_vess_name + 15:end_vess_name] + es_input_close + j[end_vess_name:]
        t = '{"index":{"_index":"vessels", "_type":"mostrecentcfr", "_id":"' + row.myCFR + '"} }\n'
        n = '\n'
        p = t + open_mod_j + n
        collection_objects.append(p)
        objects_list.clear()
    send_to_es = ''.join(collection_objects) + n
    r = requests.put(es_base_url + es_index_name + '_bulk',headers = headers, data = send_to_es)   
    collection_objects.clear()

#below is start of bulk post code - not finished
#objects_list = []


#for row in cursor:
#    d = collections.OrderedDict()
#    d['cfr'] = row.myCFR
#    d['Country Code'] = row.__getattribute__('Country Code')
#    d['Vessel Name'] = row.__getattribute__('Vessel Name')
#    d['Port Code'] = row.__getattribute__('Port Code')
#    d['Port Name'] = row.__getattribute__('Port Name')
#    d['Loa'] = row.Loa
#    d['Lbp'] = row.Lbp
#    objects_list.append(d)
#    j = json.dumps(objects_list[0])
#    t = '{"index":{"_index":"vessels", "_type":"mostrecentcfr", "_id":"' + row.myCFR + '"} }\n'
#    n = '\n'
#    p = t + j + n
#    r = requests.put(es_base_url + es_index_name + '_bulk',headers = headers, data = p)  
#    print(r.content)
#    objects_list.clear()

#below is put with cfr as id

#objects_list = []

#for row in cursor:
#    d = collections.OrderedDict()
#    d['cfr'] = row.myCFR
#    d['Country Code'] = row.__getattribute__('Country Code')    
#    d['Vessel Name'] = row.__getattribute__('Vessel Name')
#    d['Port Code'] = row.__getattribute__('Port Code')
#    d['Port Name'] = row.__getattribute__('Port Name')
#    d['loa'] = row.Loa
#    d['lbp'] = row.Lbp
#    objects_list.append(d)
#    j = json.dumps(objects_list[0])
#    r = requests.put(es_base_url + es_index_name + es_type_name + row.myCFR ,headers = headers, data = j)  
#    #print(r.content)
#    objects_list.clear()

cnxn.close()

end = time.time()
print(end - start)
