# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 15:35:25 2020

@author: abibeka
Get Single Table
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
from datetime import datetime

os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\HSIP\DataMapping\April09-DataProcessing\RawData')


# Read the Data
#************************************************************************************************************
# Get the Project and Seg data...
x1 = pd.ExcelFile("2019 HSIP Program Benefit Cost Analysis 5 Year - Master - v8.2 Prep.xlsx")
x1.sheet_names
Years = x1.sheet_names
writer = pd.ExcelWriter('DataSummary.xlsx')
Years.remove('Functional Classifications'); Years.remove('2016'); Years.remove('Summary (Injuries)');Years.remove('Summary (Crashes)')
Years.remove('Year_AnalysisPeriod'); Years.remove('RemovedProjects_Changes'); Years.remove('Project_ImpType_Planning_P_Map')
Years.remove('CostofCrashes')try:
    Years.remove('Year_AnalysisPeriod'); Years.remove('RemovedProjects_Changes'); 
except: print('')
#Years.remove('Long Narrative')
yr = "2014"
# yr= "2007-2002"
Data = x1.parse(yr,skiprows=4,dtype={"HSIP Project ID":str})
list(Data.columns)
Data.rename(columns = {"Project Title":"Title","Constr. Completion Date":"Constr. Comp. Date",
                       "Notice to Proceed Date":"NTP or Let Date"},inplace=True)



KeepCols = ['Proj. ID',
 'HSIP Project ID',
 'NTP or Let Date',
 'Constr. Comp. Date',
 'Title',
 'PennDOT Description',
 'Alternate Description',
 'Functional Class',
 'Cost Distribution',
 'Improvement Category',
 'Improvement Type',
 'Emphasis Area Related Improv. Type',
 'Project Cost',
 'Funds for Safety',
 'Funds for Safety Related Proj_Ed',
 'County',
 'SR',
 'Beg Seg',
 'Beg Off',
 'End Seg',
 'End Off',
 'Length (ft.)',
 'AADT',
 'Method for Site Selection',
 'Date Updated'
 ]

Data = Data[KeepCols]
NumProjDat = pd.DataFrame()
Data_AllYear = pd.DataFrame()
Error = pd.DataFrame()

# Get Functional class data
FunctionalClassDat = x1.parse('Functional Classifications')
# Get County name and code from shapefile
# Read the County and District Shape Files
Countyshapefile = 'PennDOT-CountyDistrictShp/County_Boundary.shp'
#Read shapefile using Geopandas
CountyNmCode = gpd.read_file(Countyshapefile)[["COUNTY_NAM","COUNTY_COD","PLANNING_P","DISTRICT_N"]]
# Add County Name and code to the Project and Seg data
tp = pd.ExcelFile('DriverByCounty_DVMT.xlsx')
tp.sheet_names
CountyDriver_DVMT_Dat = pd.read_excel('DriverByCounty_DVMT.xlsx','NumDriver_DVMT')
TempDat = CountyDriver_DVMT_Dat.merge(CountyNmCode,left_on="County", right_on ="COUNTY_NAM")

DistrictDat = TempDat.groupby('DISTRICT_N')[['NumDrivers','DVMT']].sum().reset_index()
DistrictDat.rename(columns = {'NumDrivers':'NumDrivers_District',"DVMT":'DVMT_District'},inplace=True)

sum(DistrictDat.DVMT_District)
PLANNING_PDat = TempDat.groupby('PLANNING_P')[['NumDrivers','DVMT']].sum().reset_index()
PLANNING_PDat.rename(columns = {'NumDrivers':'NumDrivers_PlanningP',"DVMT":'DVMT_PlanningP'},inplace=True)


for yr in Years:
    #Read the DAta
    Data = x1.parse(yr,skiprows=4,dtype={"HSIP Project ID":str},parse_cols ="A:DI")
    Data = Data[~Data['Beg Seg'].isna()]
    Data.rename(columns = {"Project Title":"Title","Constr. Completion Date":"Constr. Comp. Date",
                       "Notice to Proceed Date":"NTP or Let Date",
                       "HSIP Project ID_GTE":"HSIP Project ID"
                       },inplace=True)
    #Fix 2002-2007
    if yr in ["2007-2002","2004-2007"]:
        Data.loc[Data["Proj. ID"] == "80076\n80077","HSIP Project ID"] = "NotDefined"
    Data = Data[KeepCols]
    Data1 = Data.copy()
    FillCols = KeepCols[1:]
    Data1.loc[:,"Proj. ID"] = Data1.loc[:,"Proj. ID"].fillna(method='ffill',axis= 0 )
    Data1.loc[:,FillCols] = Data1.groupby('Proj. ID')[FillCols].fillna(method='ffill',axis= 0 )
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
    Data1.loc[:,"HSIP Project ID"] = Data1.loc[:,"HSIP Project ID"].astype(str)
    Data1.loc[:,"HSIP Project ID"] = Data1.loc[:,"HSIP Project ID"].apply(lambda x: CorrectMessyHSIP_Labels(x))
    Data1.loc[:,"HSIP Project ID"] = Data1.loc[:,"HSIP Project ID"].apply(lambda x: x[0:x.find(".")+4] if len(x.split('.'))==2 else x+".000")
   
    if yr in ["2007-2002","2004-2007"]:
        Data1.dtypes
        Data1.SR.value_counts()
        Data1.SR = Data1.SR.astype('int64')
        Data1.loc[Data1["Proj. ID"] == "80076\n80077","Proj. ID"] = "80076"
        Data1["Proj. ID"] = Data1["Proj. ID"].astype('int64')

    #Add County Information
    Data1 = pd.merge(Data1, CountyNmCode,left_on = "County", right_on = "COUNTY_NAM", how  = "left")
    Data1.drop(columns = "COUNTY_NAM",inplace=True)
    Data1.isna().sum()
    #Correct Data type
    Data1.dtypes
    Data1.loc[:,"Proj. ID"]  =Data1.loc[:,"Proj. ID"] .astype(int)
    Data1.SR =Data1.SR.astype(int)
    #Impute Missing Length
    Mask = (Data1.loc[:,"Length (ft.)"]=="NoData") & (Data1['Beg Seg'] ==Data1['End Seg'])
    Data1.loc[Mask,'Length (ft.)'] = Data1.loc[Mask, 'End Off']-Data1.loc[Mask, 'Beg Off']
    #Need to make some assumption for 2 rows 
    Error = pd.concat([Error,Data1.loc[Data1.loc[:,"Length (ft.)"]=="NoData"]])
    Error.loc[:,'ImputedLength'] = Error['End Off']+ 500
    Data1.loc[Data1.loc[:,"Length (ft.)"]=="NoData", "Length (ft.)"] = Data1.loc[Data1.loc[:,"Length (ft.)"]=="NoData", 'End Off'] + 500 # Assume Beg Seg is 500 ft.
    
    def GetLenPerRow(SmallDf):
        TotalLen = sum(SmallDf)
        FracLen = SmallDf/TotalLen
        return(FracLen)
    #Get Cost
    Data1.loc[:,"FracLen"] = Data1.groupby("Proj. ID")["Length (ft.)"].apply(GetLenPerRow)
    Data1.loc[:,"CostPerRow"] = Data1.FracLen * Data1['Project Cost'].astype(float)
    Data1.loc[:,"Temp"] = 1
    Data1.loc[:,"Temp"] = Data1.groupby('Proj. ID')["Temp"].transform('count')
    Data1.loc[:,"Temp2"] = Data1["Project Cost"]/Data1.loc[:,"Temp"]
    Data1.loc[:,'Tp_Cd'] = Data1['Cost Distribution'].str.strip()
    Data1['Tp_Cd'] = Data1['Tp_Cd'].str.upper()
    assert(0==sum(~Data1['Tp_Cd'].isin(['PROPORTIONAL','EQUAL'])))
    Data1.loc[Data1['Tp_Cd']=="EQUAL","CostPerRow"] = Data1.loc[Data1['Tp_Cd']=="EQUAL","Temp2"] 
    Data1.loc[Data1['Tp_Cd']=="EQUAL","FracLen"] = np.NaN
    Data1.drop(columns = ["Temp","Temp2","Tp_Cd"],inplace=True)
    Data1.loc[:,"Year"] = yr
    maskOldDateUpd = Data1["Date Updated"].isna()
    Data1.loc[maskOldDateUpd,"Date Updated"]  = "Old"
    Data_AllYear = pd.concat([Data_AllYear,Data1])
    

# Add District data and Functional Class
Data_AllYear = Data_AllYear.merge(DistrictDat,on="DISTRICT_N", how='left')
Data_AllYear = Data_AllYear.merge(PLANNING_PDat,on="PLANNING_P", how='left')
Data_AllYear.loc[Data_AllYear['Functional Class'].isna(),'Functional Class'] = -999
Data_AllYear.loc[:,'Functional Class'] = Data_AllYear['Functional Class'].astype(int)
FunctionalClassDat.dtypes
Data_AllYear = Data_AllYear.merge(FunctionalClassDat,left_on= 'Functional Class', right_on = "PennDOT FC",how="left")
maskOldData = Data_AllYear["Date Updated"]  == "Old"
NewData = Data_AllYear[~maskOldData]

Data_AllYear_Sort = pd.concat([Data_AllYear[maskOldData], Data_AllYear[~maskOldData]])
Data_AllYear_Sort.loc[Data_AllYear_Sort["Beg Off"]==-999, "Length (ft.)"] = "NoData"
#Reorder Columns 
KeepCols1 = KeepCols.copy()
UpdatedColNms = list(Data_AllYear_Sort.columns)
UpdatedColNms.remove('Method for Site Selection')
UpdatedColNms.append('Method for Site Selection')
UpdatedColNms.remove('Date Updated')
UpdatedColNms.append('Date Updated')
Data_AllYear_Sort = Data_AllYear_Sort.loc[:,UpdatedColNms]
list(Data_AllYear_Sort.columns)

# Issue = Data_AllYear[Data_AllYear.loc[:,'Functional Class'].isna()]

# Mask = (Data_AllYear.loc[:,"Length (ft.)"]=="NoData") & (Data_AllYear['Beg Seg'] ==Data_AllYear['End Seg'])
# sum(Mask) 
# Data_AllYear.loc[Mask,'Length (ft.)'] = Data_AllYear.loc[Mask, 'Beg Off'] - Data_AllYear.loc[Mask, 'End Off']
# Need to make an assumption
# Data_AllYear.loc[Data_AllYear.loc[:,"Length (ft.)"]=="NoData", "Length (ft.)"] = Data_AllYear.loc[Data_AllYear.loc[:,"Length (ft.)"], 'End Off']
# FinddIssues = Data_AllYear[Data_AllYear.loc[:,"Length (ft.)"]=="NoData"]Error.loc[:,"ImputedLength"] = Error["End Off"] + 500

DataHSIPProjSum =Data_AllYear.groupby(['Year','Proj. ID','HSIP Project ID']).agg({'Proj. ID':{'NumHSIP_ProjRows':'count'}})
DataHSIPProjSum.columns = DataHSIPProjSum.columns.droplevel(0); DataHSIPProjSum.reset_index(inplace=True)

Data_AllYear.columns
Data_AllYear2 = Data_AllYear.copy(); Data_AllYear2.drop(columns=['DISTRICT_N','PLANNING_P'],inplace=True)
Data_AllYear2 = Data_AllYear2.merge(TempDat,on="County",how='left')
Data_AllYear2.loc[:,'NumProjRows'] = 1


GrpBy = {'NumProjRows':'sum',
 'Cost Distribution':'first',
 'Improvement Category':'first',
 'Improvement Type':'first',
 'Emphasis Area Related Improv. Type':'first',
 'Project Cost':'first',
 'Funds for Safety':'first',
 'County': lambda x: "_".join(set(x)),
 'DISTRICT_N': lambda x: "_".join(set(x)),
 'PLANNING_P': lambda x: "_".join(set(x))}

DataProjSum =Data_AllYear2.groupby(['Year','Proj. ID']).agg(GrpBy).reset_index()
#DataProjSum =Data_AllYear.groupby(['Year','Proj. ID']).agg({'Proj. ID':{'NumProjRows':'count'},"","","",""})
# DataProjSum.columns = DataProjSum.columns.droplevel(1); DataProjSum.reset_index(inplace=True)

now = datetime.now()
d4 = now.strftime("%b-%d-%Y %H_%M")

OutFi = "../SingleTable_BC_AADT_Len_Data_{}.xlsx".format(d4)

writer = pd.ExcelWriter(OutFi)
Data_AllYear_Sort.to_excel(writer,"AllYear",index=False)
DataProjSum.to_excel(writer,"Project_by_Year",index=False)
DataHSIPProjSum.to_excel(writer,"HSIP_Project_by_Year",index=False)
DistrictDat.to_excel(writer,"DistrictDat",index=False)
PLANNING_PDat.to_excel(writer,"PLANNING_PDat",index=False)
FunctionalClassDat.to_excel(writer,"FunctionalClassDat",index=False)
TempDat.to_excel(writer,"CountyNmCode",index=False)
Error.to_excel(writer,"MissingLengthData",index=False)
writer.save()