# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 12:59:06 2020

@author: ltrask
Edited by: Apoorba
Purpose: Get similar geojson for SHSP Cats
"""

import json
import geopandas as gpd
gj_data=None
geojson_dict = {'type': 'FeatureCollection', 'features': []}
geometries = {}
properties = {}
import os

os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\Safety_Data_Analysis')
os.getcwd()


for root, dirs, files in os.walk("HSIPData/SHSP/Fatalities"):
    for file in files:
        if file.endswith(".geojson"):
             feats = json.load(open(root + "/" + file, 'r'))['features']
             for feat in feats:
                 cnty_nm = feat['properties']['CountyNm']
                 if not cnty_nm in geometries:
                     geometries[cnty_nm] = feat['geometry']
                 if not cnty_nm in properties:
                     properties[cnty_nm] = {'Crashes': {'Fatalities':{}, 'SSI':{}}}
                 properties[cnty_nm]['CountyNm'] = cnty_nm
                 properties[cnty_nm]['DISTRICT_N'] = feat['properties']['DISTRICT_N']
                 properties[cnty_nm]['PLANNING_P'] = feat['properties']['PLANNING_P']
                 properties[cnty_nm]['District'] = feat['properties']['District']
                 properties[cnty_nm]['TotalLinearMiles'] = feat['properties']['TotalLinearMiles']
                 properties[cnty_nm]['TotalDVMT'] = feat['properties']['TotalDVMT']
                 CheckSHSPCatLab = "Fatalities in "+ feat['properties']['SHSP_Focus_Cat']
                 if not CheckSHSPCatLab in properties[cnty_nm]['Crashes']['Fatalities']:
                     if cnty_nm == 'Adams':
                         print('Fatality-' + feat['properties']['SHSP_Focus_Cat'] + '-' + cnty_nm)
                     crash_dict = {
                                 'SHSP_Focus_Cat': feat['properties']['SHSP_Focus_Cat'],
                             }
                     for yr in range(1999, 2019, 1):
                         crash_dict['Yr-'+str(yr)] = feat['properties']['Yr-'+str(yr)]
                     SHSPCatLab = "Fatalities in "+ feat['properties']['SHSP_Focus_Cat']
                     properties[cnty_nm]['Crashes']["Fatalities"][SHSPCatLab] = crash_dict
                     
                     
for root, dirs, files in os.walk("HSIPData/SHSP/SSI"):
    for file in files:
        if file.endswith(".geojson"):
             feats = json.load(open(root + "/" + file, 'r'))['features']
             for feat in feats:
                 cnty_nm = feat['properties']['CountyNm']
                 if not cnty_nm in geometries:
                     geometries[cnty_nm] = feat['geometry']
                 if not cnty_nm in properties:
                     properties[cnty_nm] = {'Crashes': {'Fatalities':{}, 'SSI':{}}}
                 properties[cnty_nm]['CountyNm'] = cnty_nm
                 properties[cnty_nm]['DISTRICT_N'] = feat['properties']['DISTRICT_N']
                 properties[cnty_nm]['PLANNING_P'] = feat['properties']['PLANNING_P']
                 properties[cnty_nm]['District'] = feat['properties']['District']
                 properties[cnty_nm]['TotalLinearMiles'] = feat['properties']['TotalLinearMiles']
                 properties[cnty_nm]['TotalDVMT'] = feat['properties']['TotalDVMT']
                 CheckSHSPCatLab = "Suspected Serious Injuries in "+ feat['properties']['SHSP_Focus_Cat']
                 if not CheckSHSPCatLab in properties[cnty_nm]['Crashes']['SSI']:
                     if cnty_nm == 'Adams':
                         print('SSI-' + feat['properties']['SHSP_Focus_Cat'] + '-' + cnty_nm)
                     crash_dict = {
                                 'SHSP_Focus_Cat': feat['properties']['SHSP_Focus_Cat'],
                             }
                     for yr in range(1999, 2019, 1):
                         crash_dict['Yr-'+str(yr)] = feat['properties']['Yr-'+str(yr)]
                     SHSPCatLab = "Suspected Serious Injuries in "+ feat['properties']['SHSP_Focus_Cat']
                     properties[cnty_nm]['Crashes']["SSI"][SHSPCatLab] = crash_dict
                     

                 
for cnty in geometries:
    cnty_feat = {'type': "Feature",
                 'geometry': geometries[cnty],
                 'properties': properties[cnty]
                 }
    geojson_dict['features'].append(cnty_feat)
             
             
             

f = open("HSIPData/SHSP/delete.json","w")
f.write(json.dumps(geojson_dict))
f.close()
                 
             
