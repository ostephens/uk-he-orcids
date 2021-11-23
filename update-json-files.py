import requests
import json
import time
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo
from concurrent.futures import ThreadPoolExecutor, as_completed

THREAD_POOL = 16
session = requests.Session()
session.mount(
    'https://',
    requests.adapters.HTTPAdapter(pool_maxsize=THREAD_POOL,
                                  max_retries=8,
                                  pool_block=True)
)

@on_exception(expo, RateLimitException, max_tries=8, jitter="backoff.full_jitter")
@limits(calls=50, period=1)
def call_orcidAPI(url) :
    orcidResponse = session.get(
        url,
        headers={
            "Accept": "application/vnd.orcid+json"
        },
    )
    if orcidResponse.status_code != 200:
        raise Exception('API response: {}'.format(orcidResponse.status_code))
    return orcidResponse.json()


def build_orcidAPIURL(base = "https://pub.orcid.org",version = "v3.0",type = "search") :
    return base + '/' + version + '/' + type

def generate_downloadURLs(query,totalOrcids,pageSize=1000) :
    baseUrl = build_orcidAPIURL()
    queryUrl = baseUrl+ '/?q=' + query;
    urls = []
    remainingOrcids = totalOrcids
    sortOptions = 'orcid%20asc'
    i = 0
    while(remainingOrcids > 0 and totalOrcids < 22000) :
      if(i >= 11000) :
          sortOptions = "orcid%20desc"
          i = 0
      if(remainingOrcids <= pageSize) :
          pageSize = remainingOrcids
      u = queryUrl + "&start=" + str(i) + "&rows=" + str(pageSize) + "&sort=" + sortOptions
      urls.append(u)
      remainingOrcids -= pageSize
      i += pageSize
    return urls;

def generate_orcidQuery(institution) :
    query = []
    if institution["ringgold"] != None :
        for r in institution["ringgold"].split("|") :
            query.append("ringgold-org-id:"+r)
    if institution["grid"] != None :
        for g in institution["grid"].split("|") :
            query.append("grid-org-id:"+g)
    if institution["domains"] != None :
        for d in institution["domains"].split("|") :
            query.append("email:*@"+d)
    queryString = "%20OR%20".join(query)
    return queryString

def get_fullOrcid(orcid) :
    url = "https://pub.orcid.org/v3.0/" + orcid["path"]
    try :
        orcidResponse = call_orcidAPI(url)
    except :
        orcid["anon"] = True
        orcidResponse = orcid
    return orcidResponse;

def get_orcidList(query, rows, sortOptions) :
    url = "https://pub.orcid.org/v3.0/search/?q="+query+"&rows=" + rows + "&sort=" + sortOptions;
    orcidResponse = call_orcidAPI(url)
    return orcidResponse

def get_orcidCount(query) :
    baseUrl = build_orcidAPIURL()
    queryUrl = baseUrl+ '/?q=' + query + "&rows=0"
    orcidResponse = call_orcidAPI(queryUrl)
    return orcidResponse["num-found"]

def get_orcidUrls(url) :
    orcidUrls = []
    orcidResponse = call_orcidAPI(url)
    for o in orcidResponse["result"] :
        orcidUrls.append(o["orcid-identifier"])
    return orcidUrls

idMappingsFileName = 'idMappings.json'
with open(idMappingsFileName, 'r') as idMappings:
    mappings=idMappings.read()
institutions = json.loads(mappings)
startScrapeAll = time.time()
print("Starting updates: "+str(startScrapeAll))
for i in institutions:
    startScrapeInst = time.time()
    lastOrcidCount = i.get("lastOrcidCount", 0)
    lastUpdate = i.get("lastUpdate", 0)
    print("Starting: "+i["name"]+" at "+str(startScrapeInst))
    print("lastUpdate: " + str(lastUpdate))
    print("lastOrcidCount: " + str(lastOrcidCount))
    ror = i["ror"].split("/")[-1]
    outputFileName = "./data/json/"+ror+".json"
    query = generate_orcidQuery(i)
    totalOrcids = get_orcidCount(query)
    print("newOrcidCount: " + str(totalOrcids))
    try :
        if abs(totalOrcids-lastOrcidCount)<totalOrcids/1000 and startScrapeInst-lastUpdate < 60*60*24*7 :
            print("Skipping: " + i["name"] + " at " + str(time.time()) + " because count changed by <" + str(totalOrcids/1000) + " and data less than a week old")
            continue
    except :
        print("Processing " + i["name"] + " at " + str(time.time()) + " because cannot be sure if it should skip or not")
    if totalOrcids > 22000 :
        print("Skipping: " + i["name"] + " at " + str(time.time()) + " because they have too many orcids: " + str(totalOrcids))
        continue
    downloadList = generate_downloadURLs(query,totalOrcids)
    orcidList = []
    for url in downloadList :
        orcidList.extend(get_orcidUrls(url))
    fileContent = []
    with ThreadPoolExecutor(max_workers=THREAD_POOL) as executor:
        for orcidResponse in list(executor.map(get_fullOrcid, orcidList)) :
            fileContent.append(orcidResponse)
    with open(outputFileName, 'w', encoding='utf-8') as outputFile:
        json.dump(fileContent, outputFile, ensure_ascii=False, separators=(',', ':'))
    endScrapeInst = time.time()
    i["lastUpdate"] = endScrapeInst
    i["lastOrcidCount"] = totalOrcids
    print("Ending: "+i["name"]+" at "+str(endScrapeInst))
    if endScrapeInst-startScrapeAll > 60*60*5:
        break
endScrapeAll = time.time()
print("Ending updates: "+str(endScrapeAll))
with open(idMappingsFileName, 'w', encoding='utf-8') as idMappings:
    json.dump(institutions, idMappings, ensure_ascii=False, indent=4)
