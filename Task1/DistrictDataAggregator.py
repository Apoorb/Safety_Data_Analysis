# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 14:42:56 2020

@author: abibeka
#Output data for Leaflet format for districts
"""

#**********************************************
import os
import pandas as pd
import geopandas as gpd
import json
import ast
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\HSIP\Crash Analsys - Apoorba\DistrictLeafletData')

IS_SHSP = True

# Import fatalities and SSI data 
#***********************************************
if IS_SHSP:
    FatDat = pd.read_json('SHSP_District_Fatality_Statistics_Processed.json',orient="records")
    SSI_Dat = pd.read_json('SHSP_District_Suspected_Serious_Injury_Statistics_Processed.json',orient="records")

else:
    FatDat = pd.read_json('District_Fatality_Statistics_Processed.json',orient="records")
    SSI_Dat = pd.read_json('District_Suspected_Serious_Injury_Statistics_Processed.json',orient="records")

FatDat.rename(columns = {"Comb_SingleDictYr1":"Fatalities"},inplace=True)
FatDat.drop(columns = "TotalLinearMiles",inplace=True)

SSI_Dat.rename(columns = {"Comb_SingleDictYr1":"SSI"},inplace=True)
CombDAt = pd.merge(FatDat,SSI_Dat,on="District", how='inner')

# Convert a list of dict to dict
def Res_To_Dict(List_Dicts):
    result= {}
    for d in List_Dicts:
        result.update(d)
    return(result)
    
CombDAt.loc[:,'Crashes'] = CombDAt[['Fatalities','SSI']].apply(Res_To_Dict,axis=1)
CombDAt.drop(columns = ['Fatalities',"SSI"],inplace =True)
# CombDAt.loc[:,"properties"] = CombDAt.to_dict('records')
# CombDAt = CombDAt[['District','properties']]

CombDAt.columns

# Get the District Shape File
#***********************************************
DistrictShapeFile = '../PennDOT-CountyDistrictShp/PennDOT_Engineering_Districts.shp'
DistrictShape = gpd.read_file(DistrictShapeFile)[['DISTRICT_N', 'geometry']]
DistrictShape.rename(columns = {"DISTRICT_N":"District"},inplace=True)
DistrictShape.District = DistrictShape.District .apply(lambda x: "District {}".format(x))
CombDAt = CombDAt.merge(DistrictShape, on = "District", how = "inner")
CombDAt.columns

# Create Geopandas data then saves as geoJSON
#***********************************************
CombDAt = gpd.GeoDataFrame(CombDAt)
CombDAt["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon \
    else feature for feature in CombDAt["geometry"]]
    
if IS_SHSP:
    CombDAt.to_file("Delete_District_SHSP.geojson", driver='GeoJSON')
else:
    CombDAt.to_file("Delete_District.geojson", driver='GeoJSON')


#**********************************************

