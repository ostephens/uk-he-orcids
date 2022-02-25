import json
import time
import datetime
import os
import csv

jsonDir = "./data/json"
csvDir = "./data/csv"

def convertToCsv(jsonFileName,csvFileName) :
    with open(csvFileName, 'w', newline='') as csvfile :
        with open(jsonFileName, 'r') as orcidJson :
            orcidRecords = json.load(orcidJson)
            fieldnames = ['orcid', 'lastUpdated','name','educations','employments','ids','emails','workCount']
            csvWriter = csv.writer(csvfile)
            csvWriter.writerow(fieldnames)
            for orcidRecord in orcidRecords :
                csvWriter.writerow(generateCsvRow(orcidRecord))

def generateCsvRow(orcid) :
    csvRow = []
    csvRow.append(getOrcidId(orcid))
    csvRow.append(getLastUpdated(orcid))
    csvRow.append(getName(orcid))
    csvRow.append(getEmployments(orcid))
    csvRow.append(getEducations(orcid))
    csvRow.append(getIds(orcid))
    csvRow.append(getEmails(orcid))
    csvRow.append(getWorkCount(orcid))
    return csvRow

def getOrcidId(orcidRecord) :
    orcidId = orcidRecord.get("orcid-identifier").get("path")
    return orcidId

def getLastUpdated(orcidRecord) :
    try :
        lastUpdated = orcidRecord.get("history").get("last-modified-date").get("value")
        lastUpdated = datetime.date.fromtimestamp( lastUpdated/1000 )
    except :
        lastUpdated = ""
    return lastUpdated

def getName(orcidRecord) :
    try :
        givenName = orcidRecord.get("person").get("name").get("given-names").get("value")
        name = givenName
    except :
        name = ""
    try :
        familyName = orcidRecord.get("person").get("name").get("family-name").get("value")
        name = name + " " + familyName
    except :
        name = name
    if len(name.strip()) == 0 :
        try :
            name = orcidRecord.get("person").get("other-names").get("other-name")[0].get("content")
        except :
            name = "Anonymous"
    return name

def getEmployments(orcidRecord) :
    employmentsList = []
    noJob = "No job title given"
    noOrg = "No organization name"
    noYear = "*"
    try :
        employmentAGs = orcidRecord.get("activities-summary").get("employments").get("affiliation-group")
        for group in employmentAGs:
            for summary in group["summaries"] :
                try :
                    org = summary.get("employment-summary").get("organization").get("name")
                    org = org if org else noOrg
                except :
                    org =  noOrg
                try :
                    role = summary.get("employment-summary").get("role-title")
                    role = role if role else noJob
                except :
                    role =  noJob
                try :
                    startYear = summary.get("employment-summary").get("start-date").get("year").get("value")
                    startYear = startYear if startYear else noYear
                except :
                    startYear = noYear
                try :
                    endYear = summary.get("employment-summary").get("end-date").get("year").get("value")
                    endYear = endYear if endYear else noYear
                except :
                    endYear = noYear
                e = org + ": " + role + " " + str(startYear) + " -> " + str(endYear)
                employmentsList.append(e)
    except :
        employmentsList = []
    employments = ";".join(employmentsList)
    return employments

def getEducations(orcidRecord) :
    educationsList = []
    noCourse = "No course of study given"
    noOrg = "No organization name"
    noYear = "*"
    try :
        edcuationAGs = orcidRecord.get("activities-summary").get("educations").get("affiliation-group")
        for group in edcuationAGs:
            for summary in group["summaries"] :
                try :
                    org = summary.get("education-summary").get("organization").get("name")
                    org = org if org else noOrg
                except :
                    org =  noOrg
                try :
                    role = summary.get("education-summary").get("role-title")
                    role = role if role else noCourse
                except :
                    role =  noCourse
                try :
                    startYear = summary.get("education-summary").get("start-date").get("year").get("value")
                    startYear = startYear if startYear else noYear
                except :
                    startYear = noYear
                try :
                    endYear = summary.get("education-summary").get("end-date").get("year").get("value")
                    endYear = endYear if endYear else noYear
                except :
                    endYear = noYear
                e = org + ": " + role + " " + str(startYear) + " -> " + str(endYear)
                educationsList.append(e)
    except :
        educationsList = []
    educations = ";".join(educationsList)
    return educations

def getEmails(orcidRecord) :
    try :
        emailList = [e.get("email") for e in orcidRecord.get("person").get("emails").get("email")]
    except :
        emailList = []
    emails = ";".join(emailList)
    return emails

def getIds(orcidRecord) :
    try :
        idList = [i.get("external-id-type") + ": "+ i.get("external-id-value") for i in orcidRecord.get("person").get("external-identifiers").get("external-identifier")]
    except :
        idList = []
    ids = ";".join(idList)
    return ids

def getWorkCount(orcidRecord) :
    try :
        workCount = len(orcidRecord.get("activities-summary").get("works").get("group"))
    except :
        workCount = 0
    return workCount

idMappingsFileName = 'idMappings.json'
with open(idMappingsFileName, 'r') as idMappings:
    mappings=idMappings.read()
institutions = json.loads(mappings)
for i in institutions :
    ror = i["ror"].split("/")[-1]
    jsonFileName = os.path.join(jsonDir, ror + '.json')
    csvFileName = os.path.join(csvDir, ror + '.csv')
    try :
        convertToCsv(jsonFileName,csvFileName)
    except :
        continue
