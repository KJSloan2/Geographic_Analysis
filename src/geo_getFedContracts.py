'''This program aggregates federal contract data by CBSA (core base statistical area).
   Currently, this code is set up to extract contract data from the
   department of transportation's award datasets.'''
#####################################################################################
import numpy as np
import csv
import math
import pandas as pd
import time
import os
from os import listdir
from os.path import isfile, join
import json
import re
import geopy.distance

def format_zipcode(zipcode,targetLen):
	zipcode=str(zipcode)
	zcLen = len(zipcode)
	if zcLen != targetLen:
		if zcLen <=5:
			lenDelta = abs(zcLen-targetLen)
			chars_ = []
			for i in range(lenDelta):
				chars_.append(str(0))
			for c in zipcode:
				chars_.append(c)
			return("".join(chars_))
		elif zcLen > 5:
			zipcode=zipcode[0:int(len(zipcode)-4)]
			lenDelta = abs(len(zipcode)-targetLen)
			chars_ = []
			for i in range(lenDelta):
				chars_.append(str(0))
			for c in zipcode:
				chars_.append(c)
			return("".join(chars_))
	else:
		return(zipcode)

def get_gaz(tab,labels,fnames_):
	'''retreives geographic data from Gazzetter files using the arguments to specify the 
	file to read and columns to extract.
	'tab': the tabulation type (cnty, cbsa, ztca, etc.), used to grab the file name from
	 the 'gazFileNames_' dictionary.
	'labels': a list of column labels to get from the file ('GEOID', 'NAME', 'INTPTLAT', etc.)'''
	gaz_zcta = {}
	for l in labels:
		gaz_zcta[l] = []
	with open(str("%s%s" % (dirpaths_["gaz"],fnames_[tab])),encoding="utf-8") as read_gaz:
		gaz_lines = read_gaz.readlines()
		headers_ = list(map(lambda h: str(h).strip(),gaz_lines[0].split("	")))
		#print(headers_)
		idx_ = []
		for l in labels:
			idx_.append(headers_.index(l))
		for i in range(1,len(gaz_lines),1):
			gaz_line = gaz_lines[i].split("	")
			for l,idx in zip(labels,idx_):
				gaz_zcta[l].append(str(gaz_line[idx]))
	read_gaz.close()
	return gaz_zcta
#####################################################################################
dirpaths_ = {
	"00_resources":r"PATH TO WORKSPACE RESOURCES",
	"01_data":r"PATH TO WORKSPACE DATA",
	"02_output":r"PATH TO WORKSPACE OUTPUT",
	}
gazFileNames_ = {
	"elsd":"2021_Gaz_elsd_national.txt",
	"place":"2021_Gaz_place_national.txt",
	"cnty": "2021_Gaz_counties_national.txt",
	"zcta": "2021_Gaz_zcta_national.txt",
	"cd115":"2017_Gaz_115CDs_national.txt",
	"cbsa": "2021_Gaz_cbsa_national.txt",
	}
#####################################################################################
gaz_zcta = get_gaz("zcta",["GEOID","INTPTLAT","INTPTLONG"],gazFileNames_)
gaz_cbsa = get_gaz("cbsa",["GEOID","NAME","INTPTLAT","INTPTLONG"],gazFileNames_)
#####################################################################################
zipcodeRef_ = json.load(open("%s%s" % (dirpaths_["00_resources"],"zipcode_state_ref.json")))
naicsRef_ = json.load(open("%s%s" % (dirpaths_["00_resources"],"dot_referenceCodes.json")))
#####################################################################################
fName_funds_contract = "DOT_FY2023_069_Contracts_Full_20230511.csv"
funds_contract = pd.read_csv(str("%s%s" % (dirpaths_["01_data"],fName_funds_contract)),encoding="utf-8", low_memory=False)
d_zipcodes = list(funds_contract["primary_place_of_performance_zip_4"])
d_stateCodes = list(funds_contract["primary_place_of_performance_state_code"])
d_permalinks = list(funds_contract["usaspending_permalink"])
d_descriptions = list(funds_contract["transaction_description"])
d_actionDates  = list(funds_contract["action_date"])
d_startDates  = list(funds_contract["period_of_performance_start_date"])
d_endDates  = list(funds_contract["period_of_performance_current_end_date"])
d_naicsDesc  = list(funds_contract["naics_description"])
d_naicsCodes  = list(funds_contract["naics_code"])
d_recipient = list(funds_contract["recipient_name"])
d_fedActionObligations  = list(funds_contract["federal_action_obligation"])
d_totDolars = list(funds_contract["current_total_value_of_award"])
d_subAgency  = list(funds_contract["funding_sub_agency_name"])
#####################################################################################
geoCrosswalk_ = json.load(open("%s%s" % (dirpaths_["00_resources"],"GAZ_Crosswalk.json")))
cbsaKeys_ = list(geoCrosswalk_.keys())
#####################################################################################
cbsa_stateCodes = []
for i in range(len(gaz_cbsa["GEOID"])):
	stateCode = str(list(gaz_cbsa["NAME"][i].split(", "))[-1]).split(" ")[0]
	cbsa_stateCodes.append(stateCode)

get_locid = list(gaz_cbsa["GEOID"])
get_lat = list(gaz_cbsa["INTPTLAT"])
get_lon = list(gaz_cbsa["INTPTLONG"])
get_stateCode = cbsa_stateCodes
#####################################################################################
zcRef_stateCodes = []
for obj in zipcodeRef_:
	zcRef_stateCodes.append(obj["state_code"])

naicsRef_ncais = list(naicsRef_["ncais"].keys())

ui_maxDist = input("input the maximum distance (in miles) from the inertpolated center of each CBSA to get contract data: ")
with open("%s%s" % (dirpaths_["02_output"],"dot_contracts_byCbsa.csv"),'w',newline='', encoding='utf-8') as write_dataOut:
	writer_dataOut = csv.writer(write_dataOut)
	writer_dataOut.writerow([
		"LOCID","LAT","LON","STATE_CODE","ZIPCODE",
		"DIST","FUND_TYPE","NAICS_CODE","RECIPIENT",
		"ACTION_DATE","START_DATE","END_DATE","PERMALINK",
		"FED_ATION_OBL","CURRENT_TOTAL_VALUE","SUBAGENCY","DESCRIPTION"
		])
	
	for i in range(len(get_locid)):
		query_ = {
			"locid":get_locid[i],
			"coords":[get_lat[i],get_lon[i]],
			"maxdist":ui_maxDist,
			"state_code":get_stateCode[i],
			"geoids":[],
			"dist":[]
		}

	for i in range(len(d_zipcodes)):
		if str(d_zipcodes[i]).strip() != "" and len(str(d_zipcodes[i])) >=5:
			zipcode = zipcode = format_zipcode(d_zipcodes[i],5)
			for cbsaKey in cbsaKeys_:
				cbsaObj_ = geoCrosswalk_[cbsaKey]
				cbsaLat = cbsaObj_["cbsa_lat"]
				cbsaLon = cbsaObj_["cbsa_lon"]
				if zipcode in cbsaObj_["zcta"]["geoid"]:
					idx_zipcode = cbsaObj_["zcta"]["geoid"].index(zipcode)
					dist = round((float(geopy.distance.geodesic((cbsaObj_["zcta"]["lat"][idx_zipcode],cbsaObj_["zcta"]["lon"][idx_zipcode]),(cbsaLat,cbsaLon)).miles)),2)
					naicsCode = str(d_naicsCodes[i])
					if naicsCode in naicsRef_ncais:
						writer_dataOut.writerow([
							cbsaObj_["cbsa_name"],cbsaLat,cbsaLon,
							query_["state_code"],zipcode,dist,"contract",naicsRef_["ncais"][naicsCode],d_recipient[i],
							d_actionDates[i],
							d_startDates[i],
							d_endDates[i],
							d_permalinks[i],
							d_fedActionObligations[i],
							d_totDolars[i],
							d_subAgency[i],
							d_naicsDesc[i]
							])
						
write_dataOut.close()
print("done")
