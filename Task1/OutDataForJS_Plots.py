# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 12:12:53 2020
Purpose: Output data for Leaflet Plots
@author: abibeka
"""

import os
import glob
import pandas as pd
import re
import geopandas as gpd
import json
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon


os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\HSIP\Crash Analsys - Apoorba')
os.getcwd()


#*****************************************************************************************************************************
# Read the Data
#*****************************************************************************************************************************
# Read the mapping for output Json files.
x3 = pd.ExcelFile('RawData/OutputGeoJsonKeys.xlsx')
OutputCatDat = x3.parse()
OutputCatDat.columns
OutputCatDat = OutputCatDat.applymap(lambda x : x.strip())
# Read the County and District Shape Files
Countyshapefile = 'PennDOT-CountyDistrictShp/County_Boundary.shp'
#Read shapefile using Geopandas
gdf = gpd.read_file(Countyshapefile)[['COUNTY_NAM', 'DISTRICT_N','PLANNING_P', 'geometry']]
gdf.head()
gdf.plot()
gdf.loc[:,'COUNTY_NAM'] = gdf.COUNTY_NAM.str.capitalize()
gdf.loc[:,'COUNTY_NAM'] = gdf.COUNTY_NAM.str.capitalize()
gdf.loc[:,'COUNTY_NAM']= gdf.loc[:,'COUNTY_NAM'].str.capitalize().str.strip().str.replace('Mckean','McKean')
gdf.rename(columns = {'COUNTY_NAM': 'CountyNm'},inplace=True)


DistrictShapeFile = 'PennDOT-CountyDistrictShp/PennDOT_Engineering_Districts.shp'
DistrictShape = gpd.read_file(DistrictShapeFile)[['DISTRICT_N', 'geometry']]
DistrictShape.to_file("District.geojson", driver='GeoJSON')


#*****************************************************************************************************************************
# Output Geojsons
#*****************************************************************************************************************************
def OutputGeoJson(CrashCat, OutputCatDat,x1, gdf, Tag = "Fatalities"):
    df = x1.parse(CrashCat) 
    YearCols = df.columns.tolist()
    YearCols.remove('CountyNm')
    YearCols.remove('CrashCategory')
    YearCols.remove('District')
    YearCols.remove('TotalLinearMiles')
    YearCols.remove('TotalDVMT')
    NewYearCols = []
    for i in YearCols:
        df.rename(columns = {i:"Yr-{}".format(i)},inplace=True)
        NewYearCols.append("Yr-{}".format(i))
    #Merge with Jesus's categories
    SHSP_Focus_Cat = OutputCatDat.loc[CrashCat == OutputCatDat.CrashAbb,'SHSP Focus Area'].values[0]
    Crash_Focus_Cat = OutputCatDat.loc[CrashCat == OutputCatDat.CrashAbb,'Crash Data Focus Areas'].values[0]
    df.loc[:,'SHSP_Focus_Cat'] = SHSP_Focus_Cat 
    df.loc[:,'Crash_Focus_Cat'] = Crash_Focus_Cat
    #Merge With County Data
    OutDat = gdf.merge(df, left_on = 'CountyNm', right_on = 'CountyNm', how = 'right')
    OutDat= gpd.GeoDataFrame(OutDat)
    OutDat["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon \
        else feature for feature in gdf["geometry"]]
    OutDat.to_file("Leaflet_Input/{}/{}.geojson".format(Tag, CrashCat), driver='GeoJSON')
    
def OutputGeoJsonAggreateCat (SHSP_Dat, SHSP_Cat, gdf, Tag = "Fatalities"):
    df = SHSP_Dat[SHSP_Dat.SHSP_Focus_Cat == SHSP_Cat] 
    df.rename(columns={"2016*":2016},inplace=True)
    YearCols = df.columns.tolist()
    YearCols.remove('CountyNm')
    YearCols.remove('District')
    YearCols.remove('TotalLinearMiles')
    YearCols.remove('TotalDVMT')
    YearCols.remove('SHSP_Focus_Cat')
    NewYearCols = []
    for i in YearCols:
        df.rename(columns = {i:"Yr-{}".format(i)},inplace=True)
        NewYearCols.append("Yr-{}".format(i))
    #Merge With County Data
    OutDat = gdf.merge(df, left_on = 'CountyNm', right_on = 'CountyNm', how = 'right')
    OutDat= gpd.GeoDataFrame(OutDat)
    OutDat["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon \
        else feature for feature in gdf["geometry"]]
    OutDat.to_file("Leaflet_Input/SHSP/{}/{}.geojson".format(Tag, SHSP_Cat), driver='GeoJSON')
    
    
for CrashType, WB in zip(['Fatalities','SSI'],["Fatality_Statistics_Processed","Suspected_Serious_Injury_Statistics_Processed"]):
    Wb_Name = WB 
    x1 = pd.ExcelFile(Wb_Name+'.xlsx')
    x1.sheet_names
    CrashCat = "Drinking Driver"
    
    AllData = pd.DataFrame()
    for index, row in OutputCatDat.iterrows():
        Cat = row['CrashAbb']
        OutputGeoJson(Cat, OutputCatDat,x1, gdf, CrashType)
        SHSPCats = row['SHSP Focus Area']
        if SHSPCats in ['Reducing Impaired Driving','Lane Departures','Reducing Speeding & Aggressive Driving']:
            tempDat = x1.parse(Cat)
            tempDat.loc[:,"SHSP_Focus_Cat"] = SHSPCats
            AllData = pd.concat([AllData,tempDat])
    
    CHECK = AllData[['CountyNm','District','TotalLinearMiles','TotalDVMT']].groupby("CountyNm").std()
    CommonAttributes = AllData[['CountyNm','District','TotalLinearMiles','TotalDVMT']].groupby("CountyNm").first().reset_index()
    
    AllData.drop(columns = ['CrashCategory','District','TotalLinearMiles','TotalDVMT'],inplace=True)
    AllData.isna().sum()
    AllData = AllData.applymap(lambda x: 0 if x == "-" else x)
    Dat1  = AllData.groupby(['SHSP_Focus_Cat','CountyNm']).sum().reset_index()
    Dat1.SHSP_Focus_Cat.value_counts()
    Dat1 = Dat1.merge(CommonAttributes, left_on = "CountyNm", right_on = "CountyNm", how = "left" )
    
    for SHSPCat in ['Reducing Impaired Driving','Lane Departures','Reducing Speeding & Aggressive Driving']:
        OutputGeoJsonAggreateCat (SHSP_Dat=Dat1, SHSP_Cat=SHSPCat, gdf = gdf, Tag = CrashType)