# In case you need to update the idMappings.json file with the counts in the current data sets 
import json
import time

idMappingsFileName = 'idMappings.json'
with open(idMappingsFileName, 'r') as idMappings:
    mappings=idMappings.read()
institutions = json.loads(mappings)

for i in institutions:
    ror = i["ror"].split("/")[-1]
    outputFileName = "./data/"+ror+".json"
    with open(outputFileName, 'r') as output:
        o=output.read()
    oj = json.loads(o)
    totalOrcids = len(oj)
    i["lastUpdate"] = time.time()
    i["lastOrcidCount"] = totalOrcids
print(institutions)
with open(idMappingsFileName, 'w', encoding='utf-8') as idMappings:
    json.dump(institutions, idMappings, ensure_ascii=False, indent=4)
