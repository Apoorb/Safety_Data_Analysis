# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 18:06:52 2020

@author: abibeka
Manipulate data by functional classification
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
x1= ExcelFile("Fatalities by Function Class.xls")
x1.sheet_names
x2= ExcelFile("Serious Injuries by Function Class.xls")
# Pete's Mapping for functional class
PeteMap  = pd.read_excel('PeteFunctionalClass_Mapping.xlsx')
PeteMap = PeteMap.applymap(lambda x: x.strip())
# PeteMap.FuncCls = PeteMap.FuncCls.str.upper()
PeteMap.FuncCls =  PeteMap.FuncCls.apply(lambda x: x.replace(" ",""))
def ReadData (x1, Var2014_2018):
    '''
    Read fatalities and SSI data and sum 2014 to 2018 data
    '''
    Dat = x1.parse(skiprows=2)
    Dat.rename(columns = {"County ": "County","Function Class":"FuncCls"}, inplace = True)
    Dat.loc[:,"County"] = Dat.loc[:,"County"].fillna(method ='ffill')
    Dat = Dat[["FuncCls","County","2014","2015","2016","2017","2018"]]
    Dat.loc[:,Var2014_2018] = Dat[["2014","2015","2016","2017","2018"]].apply(sum, axis=1)
    Dat = Dat[["FuncCls", "County", Var2014_2018]]
    Dat = Dat[~((Dat.County =='Total')|(Dat.FuncCls =='Total') )]
    Dat[["FuncCls", "County"]] = Dat[["FuncCls", "County"]].applymap(lambda x: x.strip())
    # Dat.FuncCls = Dat.FuncCls.str.upper()
    Dat.FuncCls = Dat.FuncCls.apply(lambda x: x.replace(" ",""))
    return(Dat)

fatDf = ReadData(x1, "buf1")
ssiDf = ReadData(x2, "buf2")
CombDf = pd.merge(fatDf, ssiDf, on = ['County','FuncCls'], how = 'outer')
CombDf.loc[:,'buf1'] = CombDf.loc[:,'buf1'].fillna(0) 
CombDf.loc[:,'buf2'] = CombDf.loc[:,'buf2'].fillna(0) 
#Check
CombDf.groupby(['County']).count()
# Merge with Pete's Data to get New Categories 
CombDfClear = CombDf.merge(PeteMap, on = ['FuncCls'],how = 'right')

Check = CombDf.merge(PeteMap, on = ['FuncCls'],how = 'outer')
Check = Check[Check.NewCat.isna()]



#Sum by NewCat categories
CombDfClear.loc[:,'Sum2014_2018'] = CombDfClear.buf1 + CombDfClear.buf2 
CombDfClear = CombDfClear.groupby(["County","NewCat"])["Sum2014_2018"].sum().reset_index()
TempDa = CombDfClear.groupby(["County"])["Sum2014_2018"].sum().reset_index()
TempDa.rename(columns = {"Sum2014_2018":"Tot"},inplace=True)
CombDfClear = CombDfClear.merge(TempDa, on ="County", how = "inner")
assert(sum(CombDfClear.Tot < CombDfClear.Sum2014_2018) == 0)
# Use County ShapeFile to get Districts
#*************************************************************************************************************************
CountyShapeFile = '../Crash Analsys - Apoorba/PennDOT-CountyDistrictShp/County_Boundary.shp'
CountyGdf = gpd.read_file(CountyShapeFile)[['COUNTY_NAM','DISTRICT_N']]
CountyGdf.rename(columns = {'COUNTY_NAM':"County",'DISTRICT_N':'District'},inplace=True)
CountyGdf.County = CountyGdf.County.str.upper()
CountyGdf.District = CountyGdf.District .apply(lambda x: "District {}".format(x))
CombDfClear = CombDfClear.merge(CountyGdf, on = "County", how = "inner")
#Get the District Average Summary 
TempDat2 = TempDa.merge(CountyGdf, on = "County", how = "inner")
TempDat2 = TempDat2.groupby(['District'])[['Tot']].sum().reset_index()
# Need to use TempDat. can't use groupby. If a counties is missing a category. Then 
# other categories value is not getting used to get district total 
DistrictDat = CombDfClear.groupby(['District','NewCat'])[['Sum2014_2018']].sum().reset_index()
DistrictDat = DistrictDat.merge(TempDat2,on = "District",how = "inner")
CheckDistrict = DistrictDat.groupby('District').agg({'Sum2014_2018':'sum','Tot':'mean'})

DistrictDat.loc[:,"County"] = "District Average"
FinDat = pd.concat ([CombDfClear,DistrictDat])
FinDat.loc[:,"PercentLocalRoad2014_2018"] = round(100*FinDat.Sum2014_2018/FinDat.Tot,2)

SortOrderCounty = CountyGdf.County.values.tolist() +["District Average"]
FinDat.County = pd.Categorical(FinDat.County, SortOrderCounty)
FinDat.District.unique()
SortOrderDistricts = ['District 01','District 02','District 03', 'District 04',
                      'District 05','District 06','District 08','District 09',
                      'District 10',  'District 11', 'District 12']
FinDat.District = pd.Categorical(FinDat.District, SortOrderDistricts)

FinDat.NewCat = pd.Categorical(FinDat.NewCat, ['Local', 'Collector', 'Arterial', 'Freeway','Ramp'])

FinDat = FinDat.sort_values(['District','County','NewCat'])
FinDatSum = FinDat.groupby(['District','County','NewCat']).agg({'PercentLocalRoad2014_2018':"first",'Sum2014_2018':"mean",'Tot':"mean"})

FinDatSum = FinDatSum.unstack()
idx = pd.IndexSlice

FinDatSum.to_excel("Results/PercentFatal_SSI_2014_2018_FuncClass.xlsx", na_rep = '-')
CheckSeries = FinDatSum.loc[:,idx['PercentLocalRoad2014_2018',:]].sum(axis = 1)