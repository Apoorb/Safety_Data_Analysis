# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 15:42:48 2020

@author: abibeka
"""
#*************************************************************************************************************************
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import os
import geopandas as gpd
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon

#Read the data for fatalities
#*************************************************************************************************************************
os.chdir(r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\HSIP\functional class and jurisdiction")
x1= ExcelFile("Fatalities by Jurisdiction.xls")
x1.sheet_names
x2= ExcelFile("Serious Injuries by Jurisdiction.xls")
# Num drivers by county
DriverDat  = pd.read_excel('DriverByCounty.xlsx')

def ReadData (x1, Var2014_2015):
    '''
    Read fatalities and SSI data and sum 2014 to 2018 data
    '''
    Dat = x1.parse(skiprows=2)
    Dat.rename(columns = {"County ": "County"}, inplace = True)
    Dat.loc[:,"County"] = Dat.loc[:,"County"].fillna(method ='ffill')
    Dat = Dat[["Jurisdiction Type","County","2014","2015","2016","2017","2018"]]
    Dat.loc[:,Var2014_2015] = Dat[["2014","2015","2016","2017","2018"]].apply(sum, axis=1)
    Dat = Dat [["Jurisdiction Type", "County", Var2014_2015]]
    Dat.rename(columns = {"Jurisdiction Type":"JurisTy"},inplace=True)
    Dat = Dat[~((Dat.County =='Total')|(Dat.JurisTy =='Total') )]
    return(Dat)

fatDf = ReadData(x1, "buf1")
ssiDf = ReadData(x2, "buf2")
CombDf = pd.merge(fatDf, ssiDf, on = ['County','JurisTy'], how = 'inner')

#Check
CombDf.groupby(['County']).count()
# Some counties have less than 5 cats

#Combine Categories to Local and State
#*************************************************************************************************************************
def combinCat(x):
    broad_cat = -9999
        
    if x == "County Highway Agency":
        broad_cat = "Local"
    elif x == "Local Municipal Roadway":
        broad_cat = "Local"
    elif x == "Private Road":
        broad_cat = "Local"
    elif x == "State Highway Agency":
        broad_cat = "State"
    elif x == "State Toll Authority (Turnpike)":
        broad_cat = "State"
    else: 
        broad_cat = "Incorrect Cat"
    return broad_cat

CombDf.loc[:,"NewCat"] = CombDf.loc[:,"JurisTy"].apply(combinCat)
if "Incorrect Cat" in CombDf.NewCat: raise("Something is Wrong")

#Sum the SSI and Fat Crashes 
CombDf.loc[:,'Avg2014_2018'] = CombDf.buf1 + CombDf.buf2
#Average by Local 
CombDfAvg = CombDf.groupby(["County","NewCat"])["Avg2014_2018"].mean().reset_index()
#Factor in # of drivers by County 
CombDfAvg = pd.merge(CombDfAvg, DriverDat, on = "County", how  = "left")
assert(CombDfAvg.NumDrivers.isna().sum() == 0 )
# Get Fat+SSI/ 1000 drivers
CombDfAvg.loc[:,"Avg2014_2018_per1000Driver"] = round(1000* CombDfAvg.Avg2014_2018/ CombDfAvg.NumDrivers,2)
CombDfAvg.loc[:,"Avg2014_2018"] = round(CombDfAvg.loc[:,"Avg2014_2018"],2)


# Reshape the data to Lake's format 
#*************************************************************************************************************************
# Createa dict for crashes for 'NumDrivers','Avg2014_2018','Avg2014_2018_per1000Driver']
FinData = CombDfAvg.copy()
FinData.loc[:,"SingleDictYr"] = FinData[['Avg2014_2018','Avg2014_2018_per1000Driver']].to_dict('records')
FinData.drop(columns=['NumDrivers','Avg2014_2018','Avg2014_2018_per1000Driver'],inplace=True)
# Make as bigger dict
FinData.loc[:,"Comb_SingleDictYr"]  = FinData[['NewCat','SingleDictYr']].apply(lambda x: {x[0]:x[1]},axis=1)
FinData.drop(columns=['NewCat','SingleDictYr'],inplace=True)
# Convert a list of dict to dict
def Res_To_Dict(List_Dicts,tag):
    result= {}
    for d in List_Dicts:
        result.update(d)
    return({tag:result})
#Collapse multiple rows for same district
FinData1 = FinData.groupby(['County'])['Comb_SingleDictYr'].apply(list).reset_index()
tag = "Fatalities_SSI"
FinData1.loc[:,"Comb_SingleDictYr1"] = FinData1.Comb_SingleDictYr.apply(lambda x: Res_To_Dict(x,tag))
FinData1.drop(columns="Comb_SingleDictYr",inplace=True)
# Add common attributes
FinData1 = FinData1.merge(DriverDat, how='inner',on = "County")

def Res_To_Dict2(List_Dicts):
    result= {}
    for d in List_Dicts:
        result.update(d)
    return(result)
#Add a redundant Dictionary to match the preivous format
FinData1.loc[:,'Crashes'] = FinData1[['Comb_SingleDictYr1']].apply(Res_To_Dict2,axis=1)
FinData1.drop(columns = 'Comb_SingleDictYr1',inplace=True)
FinData1.rename(columns = {'County':'CountyNm'},inplace=True)

# Add the County Shapes and get the data in geojson for plotting
#*************************************************************************************************************************
CountyShapeFile = '../Crash Analsys - Apoorba/PennDOT-CountyDistrictShp/County_Boundary.shp'
CountyGdf = gpd.read_file(CountyShapeFile)[['COUNTY_NAM','DISTRICT_N','geometry']]
CountyGdf.rename(columns = {'COUNTY_NAM':"CountyNm",'DISTRICT_N':'District'},inplace=True)

CountyGdf.CountyNm = CountyGdf.CountyNm.str.upper()
CountyGdf.District = CountyGdf.District .apply(lambda x: "District {}".format(x))
FinData1 = FinData1.merge(CountyGdf, on = "CountyNm", how = "inner")


# Create Geopandas data then saves as geoJSON
#*************************************************************************************************************************
FinData1 = gpd.GeoDataFrame(FinData1)
FinData1["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon \
else feature for feature in FinData1["geometry"]]
        
FinData1.to_file("Data2014_2018_Delete.geojson", driver='GeoJSON')

