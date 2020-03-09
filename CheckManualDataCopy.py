# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 15:35:25 2020

@author: abibeka
Check if Check if I copy pasted the  data correctly.
"""


#0.0 Housekeeping. Clear variable space
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()

# Load Libraries
#************************************************************************************************************
import pandas as pd
import os 
import geopandas as gpd
import pandasql as ps
import numpy as np
import xlsxwriter
from geopandas import GeoDataFrame
from shapely.geometry import Point, LineString, MultiLineString
import geopandas as gpd
import folium
import pyepsg
from folium import IFrame
from folium.plugins import MarkerCluster
import fiona 
os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\HSIP\DataMapping')

# Read the Data
#************************************************************************************************************
# Get the Project and Seg data...
x1 = pd.ExcelFile("TestFile.xlsx")
x2 = pd.ExcelFile('DataSummary.xlsx')
x2.sheet_names
Years = x1.sheet_names
writer = pd.ExcelWriter('DataSummary.xlsx')
Years.remove('Functional Classifications'); Years.remove('2016'); Years.remove('Summary (Injuries)');Years.remove('Summary (Crashes)')
 #Years.remove('Long Narrative')
yr = "2015"
yr= "2007-2002"
Data = x1.parse(yr,skiprows=4)
NumProjDat = pd.DataFrame()
Data_AllYear = pd.DataFrame()

for yr in Years:
    Data = x1.parse(yr,skiprows=4,dtype={"HSIP Project ID":str})
    Data.rename(columns={"HSIP Project ID":"HSIP Proj. ID"},inplace=True)
    #Fix 2002-2007
    if yr == "2007-2002":
        Data = x1.parse(yr,skiprows=3,dtype={"HSIP Project ID":str})
        Data.rename(columns={"HSIP Project ID":"HSIP Proj. ID"},inplace=True)
        Cols = Data.columns.to_list()
        ColRename = Data.columns[[Cols.index("From"),Cols.index("From")+1,Cols.index("To"), Cols.index("To")+1]]
        ColRenameDict = dict(zip(ColRename, ["Beg Seg","Beg Off","End Seg","End Off"])) 
        Data = Data.rename(columns = ColRenameDict)
        
        Data = Data.drop(0,axis=0)
        Data["Beg Seg"] =Data["Beg Seg"].astype("int64");Data["Beg Off"] = Data["Beg Off"].astype("int64");Data["End Seg"] = Data["End Seg"].astype("int64");Data["End Off"] = Data["End Off"].astype("int64")
        Data.loc[Data["Proj. ID"] == "80076\n80077","HSIP Proj. ID"] = "NotDefined"
    Data.rename(columns = {'Length (ft.)':"CorSegLen","AADT":"CurAADT"},inplace=True)
    Data = Data[['Proj. ID','HSIP Proj. ID','County','SR','Beg Seg','Beg Off', 'End Seg','End Off','CorSegLen','CurAADT']]
    Data1 = Data.fillna(method='ffill',axis= 0 )
    Data1.loc[:,"County"] = Data1.loc[:,"County"].str.upper()
    
    def CorrectMessyHSIP_Labels(x):
        Ret1 = ""
        if len(x.split('.'))==2:
               Ret1 = x[0:x.find(".")+5]
               Ret1 = round(float(Ret1),3)
               Ret1 = str(Ret1)
        else:
            Ret1 =x+".000"
        return(Ret1)
    
    Data1.loc[:,"HSIP Proj. ID"] = Data1.loc[:,"HSIP Proj. ID"].apply(lambda x: CorrectMessyHSIP_Labels(x))

    Data1.loc[:,"HSIP Proj. ID"] = Data1.loc[:,"HSIP Proj. ID"].apply(lambda x: x[0:x.find(".")+4] if len(x.split('.'))==2 else x+".000")
   
    if yr == "2007-2002":
        Data1.dtypes
        Data1.SR.value_counts()
        Data1.SR = Data1.SR.astype('int64')
        Data1.loc[Data1["Proj. ID"] == "80076\n80077","Proj. ID"] = "80076"
        Data1["Proj. ID"] = Data1["Proj. ID"].astype('int64')
    # Get County name and code from shapefile
    # Read the County and District Shape Files
    Countyshapefile = 'PennDOT-CountyDistrictShp/County_Boundary.shp'
    #Read shapefile using Geopandas
    CountyNmCode = gpd.read_file(Countyshapefile)[["COUNTY_NAM","COUNTY_COD"]]
    CountyNmCode.to_csv('CountyNameCodeMap.csv',index=False)
    # Add County Name and code to the Project and Seg data
    Data1 = pd.merge(Data1, CountyNmCode,left_on = "County", right_on = "COUNTY_NAM", how  = "left")
    Data1.drop(columns = "COUNTY_NAM",inplace=True)
    Data1.isna().sum()
    Data1 = Data1.rename(columns = {'Proj. ID': "ProjID",
                                    "HSIP Proj. ID":"HSIP_Proj_ID",
                                    "COUNTY_COD":"CountyCode",
                                    "Beg Seg":"BegSeg",
                                    "Beg Off":"BegOff",
                                    "End Seg":"EndSeg",
                                    "End Off":"EndOff"})
    Data1.dtypes
    Data1.ProjID =Data1.ProjID.astype(int)
    Data1.SR =Data1.SR.astype(int)
    CheckData = x2.parse(yr,dtype={"HSIP_Proj_ID":str})
    CheckData = CheckData.fillna(method='ffill',axis= 0 )
    CheckData1 = pd.merge(Data1,CheckData,on = 
             ['ProjID','HSIP_Proj_ID','County','SR','BegSeg','BegOff','EndSeg','EndOff'],
             how = 'inner')
    assert(Data1.shape[0]==CheckData1.shape[0])
    assert(sum(CheckData1.CorSegLen_x!=CheckData1.CorSegLen_y)==0)    
    assert(sum(CheckData1.CurAADT_x!=CheckData1.CurAADT_y)==0)    
    print("Check Stat for Year {}".format(yr))
    print("Shape before and after join: {} & {}".format(Data1.shape[0],CheckData1.shape[0]) )
    print("Diff in Cor Len b/w two data: {}".format(sum(CheckData1.CorSegLen_x!=CheckData1.CorSegLen_y)) )
    print("Diff in Cor AADT b/w two data: {} \n".format(sum(CheckData1.CurAADT_x!=CheckData1.CurAADT_y)) )


    