import pyodbc 
import json, os
import requests
import collections
import time
import argparse

start = time.time()

#server and es url from run command

parser = argparse.ArgumentParser()
parser.add_argument("-s","--sql", help="Name of SQL server to query")
parser.add_argument("-e","--ESurl", help="URL of ES server")
parser.add_argument("-u","--user", help="SQL server username")
parser.add_argument("-p","--password", help="SQL server password")
args = parser.parse_args()

#Variables

if args.sql:
    server = str(args.sql)
else:
    raise SystemExit('No sql server name')

if args.ESurl:
    es_base_url = str(args.ESurl)
else:
    raise SystemExit('No url for elasticsearch')

if args.user:
    username = str(args.user)
else:
    raise SystemExit('No user name')

if args.password:
    password = str(args.password)
else:
    raise SystemExit('No password')

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
                },
                "TonRef" : {
                    "type": "keyword"
                },
                "PowerMain" : {
                    "type": "keyword"
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
                },
                "TonRef" : {
                    "type": "keyword"
                },
                "PowerMain" : {
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

requests.delete(es_base_url + es_index_name_all)
requests.put(es_base_url + es_index_name_all, headers = headers, data = es_init_for_all_events)

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';UID='+username+';PWD='+password)
cursor = cnxn.cursor()

cursor.execute("SELECT [CFR], [Country Code], [Vessel Name], [Port Code], [Port Name], [Loa], [Lbp], [Event Code],[Event Start Date],[Event End Date], [Power Main], [Ton Ref] FROM [InformaticsLoad].[dbo].[MasterVessel];") 

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
        d['TonRef'] = row.__getattribute__('Ton Ref')
        d['PowerMain'] = row.__getattribute__('Power Main')
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

#populate index for first search, vessel by name or cfr

vesselname_cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';UID='+username+';PWD='+password)

requests.delete(es_base_url + es_index_name_by_vesselName)
requests.put(es_base_url + es_index_name_by_vesselName, headers = headers, data = es_init_for_all_vesslNames)

cursor = vesselname_cnxn.cursor()
cursor.execute("SELECT [CFR], [Country Code], [Vessel Name], [Port Code], [Port Name], [Loa], [Lbp], [Power Main], [Ton Ref] FROM [InformaticsLoad].[dbo].[Most Recent Vessel Name Activity]")

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
        d['TonRef'] = row.__getattribute__('Ton Ref')
        d['PowerMain'] = row.__getattribute__('Power Main')
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
