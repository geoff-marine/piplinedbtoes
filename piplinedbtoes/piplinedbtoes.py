import pyodbc 
import json, os
import requests
import collections
import time

start = time.time()

#Variables

server = 'vminformdev01' 
database = 'GI_VS_SC_Test' 
es_base_url = 'http://10.11.1.70:9200/'
es_index_name_all = 'allvessels/'
es_index_name_one_cfr = 'vessel/'
es_index_name_by_vesselName = 'vesselname/'
es_type_name_all = 'allevents'
es_type_name_cfr ='mostrecentcfr'
es_type_name_vesselName ='allnames'
headers = {'Content-Type':'application/json'}
es_input_open = '{"input":['
es_input_close = ']}' 

es_init_for_all_events = '''
{
    "mappings":{
        "allevents": {
            "properties": {
                "VesselName" : {
                    "type": "keyword"
                },
                "cfr" : {
                    "type": "keyword"
                },
                "CountryCode" : {
                    "type": "keyword"
                },
                "PortCode" : {
                    "type": "keyword"
                },
                "PortName" : {
                    "type": "keyword"
                },
                "Loa" : {
                    "type": "keyword"
				},
                "Lbp" : {
                    "type": "keyword"
                },
                "EventCode" :{
                    "type" : "keyword"
                },
                "EventStartDate" : {
                    "type" : "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "EventEndDate": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                }
                
                }
            }
        }
}
'''

es_init_for_all_most_recent_cfrs = '''
{
	  "settings": {
    "index": {
      "number_of_shards": 1,
      "analysis": {
        "analyzer": {
          "trigram": {
            "type": "custom",
            "tokenizer": "standard",
            "filter": ["shingle"]
          },
          "reverse": {
            "type": "custom",
            "tokenizer": "standard",
            "filter": ["reverse"]
          }
        },
        "filter": {
          "shingle": {
            "type": "shingle",
            "min_shingle_size": 2,
            "max_shingle_size": 3
          }
        }
      }
    }
  },

    "mappings": {
        "mostrecentcfr" : {
            "properties" : {
                "VesselName" : {
                    "type" : "text",
					"fields": {
						"trigram": {
						  "type": "text",
						  "analyzer": "trigram"
						},					        
                    "reverse": {
                      "type": "text",
                      "analyzer": "reverse" 
                },
                "ExactName" : {
                    "type": "keyword"
                },
                "cfr" : {
                    "type": "keyword"
                },
                "CountryCode" : {
                    "type": "keyword"
                },
                "PortCode" : {
                    "type": "keyword"
                },
                "PortName" : {
                    "type": "keyword"
                },
                "Loa" : {
                    "type": "keyword"
				},
                "Lbp" : {
                    "type": "keyword"
                }
                
				}
			
            }
        }
    }
}
}
'''


es_init_for_all_vesslNames = '''
{
	  "settings": {
    "index": {
      "number_of_shards": 1,
      "analysis": {
        "analyzer": {
          "trigram": {
            "type": "custom",
            "tokenizer": "standard",
            "filter": ["shingle"]
          },
          "reverse": {
            "type": "custom",
            "tokenizer": "standard",
            "filter": ["reverse"]
          }
        },
        "filter": {
          "shingle": {
            "type": "shingle",
            "min_shingle_size": 2,
            "max_shingle_size": 3
          }
        }
      }
    }
  },

    "mappings": {
        "allnames" : {
            "properties" : {
                "VesselName" : {
                    "type" : "text",
					"fields": {
						"trigram": {
						  "type": "text",
						  "analyzer": "trigram"
						},					        
                    "reverse": {
                      "type": "text",
                      "analyzer": "reverse" 
                },
                "ExactName" : {
                    "type": "keyword"
                },
                "cfr" : {
                    "type": "keyword"
                },
                "CountryCode" : {
                    "type": "keyword"
                },
                "PortCode" : {
                    "type": "keyword"
                },
                "PortName" : {
                    "type": "keyword"
                },
                "Loa" : {
                    "type": "keyword"
				},
                "Lbp" : {
                    "type": "keyword"
                }
                
				}
			
            }
        }
    }
}
}
'''
#trasnfer data from sql server to es for all records in mastervessel
#delete needed as we have to use auto generated es ids for this index
#no es init needed as each field will be a keyword

requests.delete(es_base_url + es_index_name_all)
requests.put(es_base_url + es_index_name_all, headers = headers, data = es_init_for_all_events)

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')
cursor = cnxn.cursor()

cursor.execute("SELECT [CFR], [Country Code], [Vessel Name], [Port Code], [Port Name], [Loa], [Lbp], [Event Code],[Event Start Date],[Event End Date] FROM [InformaticsLoad].[dbo].[MasterVessel];") 

objects_list = []
collection_objects =[]

while True:
    results = cursor.fetchmany(20000)
    if not results:
        break
    for row in results:
        d = collections.OrderedDict()
        d['cfr'] = row.CFR
        d['CountryCode'] = row.__getattribute__('Country Code')
        d['VesselName'] = row.__getattribute__('Vessel Name')
        d['PortCode'] = row.__getattribute__('Port Code')
        d['PortName'] = row.__getattribute__('Port Name')
        d['Loa'] = row.Loa
        d['Lbp'] = row.Lbp
        d['EventCode'] = row.__getattribute__('Event Code')
        d['EventStartDate'] = row.__getattribute__('Event Start Date')
        d['EventEndDate'] = row.__getattribute__('Event End Date')
        objects_list.append(d)
        j = json.dumps(objects_list[0], default = str)
        t = '{"index":{"_index":"' + es_index_name_all[:-1] +'", "_type":"' + es_type_name_all + '"} }\n'
        n = '\n'
        p = t + j + n
        collection_objects.append(p)
        objects_list.clear()
    send_to_es = ''.join(collection_objects) + n
    r = requests.put(es_base_url + es_index_name_all + '_bulk',headers = headers, data = send_to_es) 
    collection_objects.clear()


cnxn.close()

#trasnfer data from sql server to es for all records in most recent cfrs view

#vessel_cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')

#requests.put(es_base_url + es_index_name_one_cfr, headers = headers, data = es_init_for_all_most_recent_cfrs)

#cursor = vessel_cnxn.cursor()
#cursor.execute("SELECT [CFR], [Country Code], [Vessel Name], [Port Code], [Port Name], [Loa], [Lbp] FROM [InformaticsLoad].[dbo].[Most Recent CFR activity]")

#objects_list = []
#collection_objects =[]

#while True:
#    results = cursor.fetchmany(20000)
#    if not results:
#        break
#    for row in results:
#        d = collections.OrderedDict()
#        d['cfr'] = row.CFR
#        d['CountryCode'] = row.__getattribute__('Country Code')
#        d['VesselName'] = row.__getattribute__('Vessel Name')
#        d['ExactName'] = row.__getattribute__('Vessel Name')
#        d['PortCode'] = row.__getattribute__('Port Code')
#        d['PortName'] = row.__getattribute__('Port Name')
#        d['Loa'] = row.Loa
#        d['Lbp'] = row.Lbp
#        objects_list.append(d)
#        j = json.dumps(objects_list[0], default = str)
#        t = '{"index":{"_index":"' + es_index_name_one_cfr[:-1] +'", "_type":"' + es_type_name_cfr + '", "_id":"' + row.CFR + '"} }\n'
#        n = '\n'
#        p = t + j + n
#        collection_objects.append(p)
#        objects_list.clear()
#    send_to_es = ''.join(collection_objects) + n
#    r = requests.put(es_base_url + es_index_name_one_cfr + '_bulk',headers = headers, data = send_to_es) 
#    collection_objects.clear()

#vessel_cnxn.close()

#vessel names

vesselname_cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')

requests.delete(es_base_url + es_index_name_by_vesselName)
requests.put(es_base_url + es_index_name_by_vesselName, headers = headers, data = es_init_for_all_vesslNames)

cursor = vesselname_cnxn.cursor()
cursor.execute("SELECT [CFR], [Country Code], [Vessel Name], [Port Code], [Port Name], [Loa], [Lbp] FROM [InformaticsLoad].[dbo].[Most Recent Vessel Name Activity]")

objects_list = []
collection_objects =[]

while True:
    results = cursor.fetchmany(20000)
    if not results:
        break
    for row in results:
        d = collections.OrderedDict()
        d['cfr'] = row.CFR
        d['CountryCode'] = row.__getattribute__('Country Code')
        d['VesselName'] = row.__getattribute__('Vessel Name')
        d['ExactName'] = row.__getattribute__('Vessel Name')
        d['PortCode'] = row.__getattribute__('Port Code') 
        d['PortName'] = row.__getattribute__('Port Name')
        d['Loa'] = row.Loa
        d['Lbp'] = row.Lbp
        objects_list.append(d)
        j = json.dumps(objects_list[0], default = str)
        t = '{"index":{"_index":"' + es_index_name_by_vesselName[:-1] +'", "_type":"' + es_type_name_vesselName + '"} }\n'
        n = '\n'
        p = t + j + n
        collection_objects.append(p)
        objects_list.clear()
    send_to_es = ''.join(collection_objects) + n
    r = requests.put(es_base_url + es_index_name_by_vesselName + '_bulk',headers = headers, data = send_to_es) 
    collection_objects.clear()

vesselname_cnxn.close()


end = time.time()
print(end - start)
