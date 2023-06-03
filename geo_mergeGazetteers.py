'''This python program merges Gazetteer, geographic reference 
   files from the US Census Bureau into a single GeoJson for easier access
   and for use in visualization tools like Deck.GL'''
import numpy as np
import json
import geopy.distance
import datetime
from datetime import datetime, timezone
from os import listdir
from os.path import isfile, join
import os

##############################DIRECTORY PATHS########################################
dirpaths_ = {
	"gaz":r"PATH TO GAZETTEER FILES",
	"output" :r"PATH TO WRITE OUTPUT"
	}
#####################################################################################
'''geoCatchBasin: the type of geographic tabulation area to serve as the base 
location to pool surrounding tabulation areas with'''
##############################GLOBAL FUNCTIONS#######################################
def check_encoding(file2check):
	file_encoding = ""
	with open(file2check) as file_info:
		file_encoding = file_info.encoding
	file_info.close()
	print(file_encoding)
	return str(file_encoding)

def get_gaz(tab,fname,labels):
	'''retreives geographic data from Gazzetter files using the arguments to specify the 
	file to read and columns to extract.
	'tab': the tabulation type (cnty, cbsa, ztca, etc.), used to grab the file name from
	 the 'gazFileNames_' dictionary.
	'labels': a list of column labels to get from the file ('GEOID', 'NAME', 'INTPTLAT', etc.)'''
	objs_ = []
	with open(str("%s%s" % (dirpaths_["gaz"],fname)),encoding="utf-8") as read_gaz:
		gaz_lines = read_gaz.readlines()
		headers_ = list(map(lambda h: str(h).strip(),gaz_lines[0].split("	")))
		print(headers_)
		idx_ = []
		for l in labels:
			idx_.append(headers_.index(l))
		for i in range(1,len(gaz_lines),1):
			gaz_line = gaz_lines[i].split("	")
			geoid = str(gaz_line[idx_[0]])
			obj_ = {
				"type": "Feature",
				"properties": {
				"featureclass": tab,
				"geoid": geoid
				},
				"geometry": {
				"type": "Point",
				"coordinates": [float(str(gaz_line[idx_[2]]).strip()),float(str(gaz_line[idx_[1]]).strip())]
				}
			}
			'''for l,idx in zip(labels,idx_):
				objs_[geoid][l] = str(gaz_line[idx]).strip()'''
			objs_.append(obj_)
	read_gaz.close()
	return objs_

#####################################################################################
'''The args_ dictionary contains instructions that tell the get_gaz function what Gazetteer files
    to process and what labels to extract. This allows you to control what files to merge and what data to extract from each file.'''

args_ = {
	"cbsa":{"f_name":"2021_Gaz_cbsa_national.txt","process":True,"labels":["GEOID","INTPTLAT","INTPTLONG"]},
	"cd116":{"f_name":"2017_Gaz_115CDs_national.txt","process":True,"labels":["GEOID","INTPTLAT","INTPTLONG"]},
	"cnty":{"f_name":"2021_Gaz_counties_national.txt","process":True,"labels":["GEOID","INTPTLAT","INTPTLONG"]},
	"zcta":{"f_name":"2021_Gaz_zcta_national.txt","process":True,"labels":["GEOID","INTPTLAT","INTPTLONG"]},
	"place":{"f_name":"2021_Gaz_place_national.txt","process":True,"labels":["GEOID","INTPTLAT","INTPTLONG"]},
	"elsd":{"f_name":"2021_Gaz_elsd_national.txt","process":True,"labels":["GEOID","INTPTLAT","INTPTLONG"]}
}
#####################################################################################
gazCombined_ = {"type": "FeatureCollection","features":[]}

for tab in list(args_.keys()):
	if args_[tab]["process"] == True:
		objs_ = get_gaz(tab,args_[tab]["f_name"],args_[tab]["labels"])
		for obj_ in objs_:
			gazCombined_["features"].append(obj_)

with open(str("%s%s" % (dirpaths_["output"],"GazzettersCombined.geojson")), "w") as json_gazCombined:
	geoCrosswalk_pretty = json.dumps(gazCombined_, indent=4)
	json_gazCombined.write(geoCrosswalk_pretty)
