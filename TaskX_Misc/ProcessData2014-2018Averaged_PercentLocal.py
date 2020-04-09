# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 16:59:15 2020

@author: abibeka
"""
#0.0 Housekeeping. Clear variable space
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
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
CombDf = pd.merge(fatDf, ssiDf, on = ['County','JurisTy'], how = 'outer')

#Check
CombDf.groupby(['County']).count()
# Some counties have less than 5 cats

#Combine Categories to Local and State
#*************************************************************************************************************************
def combinCat(x):
    broad_cat = -9999
    x = x.strip()    
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
CombDf.loc[:,'Sum2014_2018'] = CombDf.buf1 + CombDf.buf2
#Sum by Local 
CombDfAvg = CombDf.groupby(["County","NewCat"])["Sum2014_2018"].sum().reset_index()
TempDa = CombDf.groupby(["County"])["Sum2014_2018"].sum().reset_index()
TempDa.rename(columns = {"Sum2014_2018":"Tot"},inplace=True)
CombDfAvg = CombDfAvg.merge(TempDa, on ="County", how = "inner")
CombDfAvg = CombDfAvg[CombDfAvg.NewCat=="Local"]
CombDfAvg.rename(columns = {"Sum2014_2018":"Local"},inplace=True)

# Use County ShapeFile to get Districts
#*************************************************************************************************************************
CountyShapeFile = '../Crash Analsys - Apoorba/PennDOT-CountyDistrictShp/County_Boundary.shp'
CountyGdf = gpd.read_file(CountyShapeFile)[['COUNTY_NAM','DISTRICT_N']]
CountyGdf.rename(columns = {'COUNTY_NAM':"County",'DISTRICT_N':'District'},inplace=True)
CountyGdf.County = CountyGdf.County.str.upper()
CountyGdf.District = CountyGdf.District .apply(lambda x: "District {}".format(x))
CombDfAvg = CombDfAvg.merge(CountyGdf, on = "County", how = "inner")

#Get the District Average Summary 
DistrictDat = CombDfAvg.groupby(['District','NewCat'])[['Local',"Tot"]].sum().reset_index()

# Remove extra columns
CombDfAvg.drop(columns = 'NewCat',inplace=True)
DistrictDat.drop(columns = 'NewCat',inplace=True)
DistrictDat.loc[:,"County"] = "District Average"
FinDat = pd.concat ([CombDfAvg,DistrictDat])
FinDat.loc[:,"PercentLocalRoad2014_2018"] = round(100*FinDat.Local/FinDat.Tot,2)
SortOrderCounty = CountyGdf.County.values.tolist() +["District Average"]
FinDat.County = pd.Categorical(FinDat.County, SortOrderCounty)
FinDat.District.unique()
SortOrderDistricts = ['District 01','District 02','District 03', 'District 04',
                      'District 05','District 06','District 08','District 09',
                      'District 10',  'District 11', 'District 12']
FinDat.District = pd.Categorical(FinDat.District, SortOrderDistricts)
FinDat = FinDat.sort_values(['District','County'])
FinDatSum = FinDat.groupby(['District','County']).agg({'PercentLocalRoad2014_2018':"first",'Local':"mean",'Tot':"mean"})
FinDatSum.to_excel("Results/PercentFatal_SSI_2014_2018_Local.xlsx")
