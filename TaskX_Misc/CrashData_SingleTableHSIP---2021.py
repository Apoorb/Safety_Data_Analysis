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
x1 = pd.ExcelFile("Task 2.7 Project Analysis_2020-04-30 - Copy.xlsx")
x1.sheet_names
Years = x1.sheet_names
writer = pd.ExcelWriter('DataSummary.xlsx')
Years.remove('Sheet1')
yr= "2021"
Data = x1.parse(yr,skiprows=3)
list(Data.columns)
Data.rename(columns = {"Project Title":"Title","Constr. Completion Date":"Constr. Comp. Date",
                       "Notice to Proceed Date":"NTP or Let Date","FY2021 Obligation":"Project Cost"},inplace=True)



KeepCols = ['Proj. ID',
 'HSIP Project ID',
 'Est Let Date',
 'Title', 'ADT',
 'PennDOT Description',
 'Functional Class',
 'Cost Distribution',
 'Improvement Category',
 'Improvement Type',
 'SHSP Emphasis Area',
 'Project Cost',
 'County',
 'SR',
 'Beg Seg',
 'Beg Off',
 'End Seg',
 'End Off',
 'Length (ft.)',
 'AADT',
 'Method for Site Selection']

CrashColumnsTemp = ["CrashType1","CrashType2","CrashType3",
 'Before_Fatal','Before_SSI','After_Fatal', 'After_SSI',
 'Type1_Before_Fatal','Type1_Before_SSI','Type1_After_Fatal', 'Type1_After_SSI',
'Type2_Before_Fatal','Type2_Before_SSI','Type2_After_Fatal' , 'Type2_After_SSI',
'Type3_Before_Fatal','Type3_Before_SSI','Type3_After_Fatal' , 'Type3_After_SSI']
Tp = list(set(CrashColumnsTemp) - set(Data.columns))
[CrashColumnsTemp.remove(i) for i in Tp]

CrashColumnsNew =["Before_Total Crashes","Before_Fatal","Before_SSI","Before_Susp Min Inj Crashes",
"Before_Possible Inj Crashes","Before_Unk Inj Crashes","Before_PDO Crashes",	
"After_Total Crashes","After_Fatal","After_SSI","After_Susp Min Inj Crashes","After_Possible Inj Crashes",
"After_Unk Inj Crashes","After_PDO Crashes",	
"CrashType1","Type1_Before_Total Crashes","Type1_Before_Fatal","Type1_Before_SSI",
"Type1_Before_Susp Min Inj Crashes","Type1_Before_Possible Inj Crashes","Type1_Before_Unk Inj Crashes",
"Type1_Before_PDO_Crashes","Type1_After_Total Crashes","Type1_After_Fatal","Type1_After_SSI",
"Type1_After_Susp Min Inj Crashes","Type1_After_Possible Inj Crashes","Type1_After_Unk Inj Crashes"	,
"Type1_After_PDO Crashes","CrashType2","Type2_Before_Total Crashes","Type2_Before_Fatal","Type2_Before_SSI",
"Type2_Before_Susp Min Inj Crashes","Type2_Before_Possible Inj Crashes","Type2_Before_Unk Inj Crashes",
"Type2_Before_PDO_Crashes","Type2_After_Total Crashes","Type2_After_Fatal","Type2_After_SSI",
"Type2_After_Susp Min Inj Crashes","Type2_After_Possible Inj Crashes","Type2_After_Unk Inj Crashes",
"Type2_After_PDO Crashes","CrashType3","Type3_Before_Total Crashes","Type3_Before_Fatal","Type3_Before_SSI",	
"Type3_Before_Susp Min Inj Crashes","Type3_Before_Possible Inj Crashes","Type3_Before_Unk Inj Crashes",
"Type3_Before_PDO_Crashes","Type3_After_Total Crashes","Type3_After_Fatal","Type3_After_SSI",
"Type3_After_Susp Min Inj Crashes","Type3_After_Possible Inj Crashes","Type3_After_Unk Inj Crashes",
"Type3_After_PDO Crashes"]
Tp = list(set(CrashColumnsNew) - set(Data.columns))
[CrashColumnsNew.remove(i) for i in Tp]

CrashCols = [ 'Before_Fatal','Before_SSI','After_Fatal', 'After_SSI',
 'Type1_Before_Fatal','Type1_Before_SSI','Type1_After_Fatal', 'Type1_After_SSI',
'Type2_Before_Fatal','Type2_Before_SSI','Type2_After_Fatal' , 'Type2_After_SSI',
'Type3_Before_Fatal','Type3_Before_SSI','Type3_After_Fatal' , 'Type3_After_SSI']
Tp = list(set(CrashCols) - set(Data.columns))
[CrashCols.remove(i) for i in Tp]

Data.rename(columns = {"Project Title":"Title",
                   "HSIP Project ID_GTE":"HSIP Project ID"
                   },inplace=True)
    #Fix 2002-2007
Data = Data[KeepCols+CrashColumnsNew]
NumProjDat = pd.DataFrame()
Data_AllYear = pd.DataFrame()
Error = pd.DataFrame()


x2 = pd.ExcelFile("2019 HSIP Program Benefit Cost Analysis 5 Year - Master - v8.2 Prep.xlsx")
#Crash Cost Data
# CrashCost = x2.parse('CostofCrashes')
#Get Functional class data
FunctionalClassDat = x2.parse('Functional Classifications')
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
# 
DistrictDat = TempDat.groupby('DISTRICT_N')[['NumDrivers','DVMT']].sum().reset_index()
DistrictDat.rename(columns = {'NumDrivers':'NumDrivers_District',"DVMT":'DVMT_District'},inplace=True)

sum(DistrictDat.DVMT_District)
PLANNING_PDat = TempDat.groupby('PLANNING_P')[['NumDrivers','DVMT']].sum().reset_index()
PLANNING_PDat.rename(columns = {'NumDrivers':'NumDrivers_PlanningP',"DVMT":'DVMT_PlanningP'},inplace=True)

yr = "2021"
for yr in Years:
    #Read the DAta
    Data = x1.parse(yr,skiprows=3,dtype={"HSIP Project ID":str},parse_cols ="A:DI")
    Data = Data[~Data['Beg Seg'].isna()]
    Data.rename(columns = {"Project Title":"Title","Constr. Completion Date":"Constr. Comp. Date",
                       "Notice to Proceed Date":"NTP or Let Date",
                       "HSIP Project ID_GTE":"HSIP Project ID","FY2021 Obligation":"Project Cost"
                       },inplace=True)
    #Fix 2002-2007
    if yr == "2004-2007":
        Data.loc[Data["Proj. ID"] == "80076\n80077","HSIP Project ID"] = "80076.000"
        Data.loc[Data["Proj. ID"] == "80076\n80077","Proj. ID"] = "80076"
    Data = Data[KeepCols+CrashColumnsNew]
    Data1 = Data.copy()
    
    def Diff(li1, li2): 
        return (list(set(li1) - set(li2))) 
    
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
    if yr == "2004-2007":
        Data1["Proj. ID"] = Data1["Proj. ID"].astype('int64')
        Data1.SR = Data1.SR.astype('int64')

    #Data1.loc[:,CrashCols] = Data1.groupby(['Proj. ID',"HSIP Project ID"])[CrashCols].fillna(method='ffill',axis= 0 )    
    #Add County Information ##############################################################################################
    Data1 = pd.merge(Data1, CountyNmCode,left_on = "County", right_on = "COUNTY_NAM", how  = "left")
    Data1.drop(columns = "COUNTY_NAM",inplace=True)
    #Correct Data type ###################################################################################################
    Data1.loc[:,"Proj. ID"]  =Data1.loc[:,"Proj. ID"] .astype(int)
    Data1.SR =Data1.SR.astype(int)
    #Impute Missing Length ###############################################################################################
    Mask = (Data1.loc[:,"Length (ft.)"]=="NoData") & (Data1['Beg Seg'] ==Data1['End Seg'])
    Data1.loc[Mask,'Length (ft.)'] = Data1.loc[Mask, 'End Off']-Data1.loc[Mask, 'Beg Off']
    #Need to make some assumption for 2 rows ############################################################################
    Error = pd.concat([Error,Data1.loc[Data1.loc[:,"Length (ft.)"]=="NoData"]])
    Data1.loc[Data1.loc[:,"Beg Seg"]==-999, "Length (ft.)"] = -9999
    #Get Cost ############################################################################################################
    def GetLenPerRow(SmallDf):
        TotalLen = sum(SmallDf)
        FracLen = SmallDf/TotalLen
        return(FracLen)
    Data1.loc[:,"FracLen"] = Data1.groupby("Proj. ID")["Length (ft.)"].apply(GetLenPerRow) #Len Proportions
    Data1.loc[:,"CostPerRow"] = Data1.FracLen * Data1['Project Cost'].astype(float) #Cost PerRow
    Data1.loc[:,"Temp"] = 1 # Placeholder
    Data1.loc[:,"Temp"] = Data1.groupby('Proj. ID')["Temp"].transform('count') #Row count by project
    Data1.loc[:,"Temp2"] = Data1["Project Cost"]/Data1.loc[:,"Temp"] #Equal Distribution of project cost
    Data1.loc[:,'Tp_Cd'] = Data1['Cost Distribution'].str.strip()  #Cost dist clean up
    Data1['Tp_Cd'] = Data1['Tp_Cd'].str.upper()
    assert(0==sum(~Data1['Tp_Cd'].isin(['PROPORTIONAL','EQUAL']))) #Sanity check
    Data1.loc[Data1['Tp_Cd']=="EQUAL","CostPerRow"] = Data1.loc[Data1['Tp_Cd']=="EQUAL","Temp2"] #Handle Equal cost dist
    Data1.loc[Data1['Tp_Cd']=="EQUAL","FracLen"] = np.NaN # Length not relevant
    Data1.drop(columns = ["Temp","Temp2","Tp_Cd"],inplace=True) # Drop temp columns
    Data1.loc[:,"Year"] = yr # Create Year Column
    Data1.loc[:,"Date Updated"]  = "May05"
    Data_AllYear = pd.concat([Data_AllYear,Data1])

len(Data_AllYear['HSIP Project ID'].unique())
len(Data_AllYear['Proj. ID'].unique())

Data_AllYear.Year.value_counts()

Data_AllYear["Improvement Category"] = Data_AllYear["Improvement Category"].str.capitalize()
Data_AllYear["Improvement Category"].value_counts()
CrashColumnsNewv_1 = CrashColumnsNew.copy()
CrashColumnsNewv_1.remove('CrashType1');CrashColumnsNewv_1.remove('CrashType2'); CrashColumnsNewv_1.remove('CrashType3')


Data_AllYear[CrashColumnsNew] = Data_AllYear[CrashColumnsNew].replace('---',np.NaN)
Data_AllYear[CrashColumnsNewv_1] = Data_AllYear[CrashColumnsNewv_1].astype('Int64')
Data_AllYear1 = Data_AllYear.copy()
Data_AllYear1.loc[:,'ProjectSNo'] = Data_AllYear1.groupby(['Proj. ID']).cumcount()+1
Data_AllYear1.loc[:,'HsipProjectSNo'] = Data_AllYear1.groupby(['Proj. ID',"HSIP Project ID"])['Proj. ID'].cumcount()+1
    
Data_AllYear1.loc[Data_AllYear1.ProjectSNo!=1,'Project Cost'] = np.NaN

Year_AnalysisPeriod = x1.parse('Sheet1',skiprows=0)
Year_AnalysisPeriod.Year =Year_AnalysisPeriod.Year.astype(str)
Data_AllYear1 = Data_AllYear1.merge(Year_AnalysisPeriod, on='Year',how='left')
# Add District data and Functional Class
Data_AllYear1 = Data_AllYear1.merge(DistrictDat,on="DISTRICT_N", how='left')
Data_AllYear1 = Data_AllYear1.merge(PLANNING_PDat,on="PLANNING_P", how='left')
Data_AllYear1['Functional Class'] = Data_AllYear1['Functional Class'].replace('---',np.NaN)
Data_AllYear1.loc[Data_AllYear1['Functional Class'].isna(),'Functional Class'] = -999
Data_AllYear1.loc[:,'Functional Class'] = Data_AllYear1['Functional Class'].astype(int)
FunctionalClassDat.dtypes
Data_AllYear1 = Data_AllYear1.merge(FunctionalClassDat,left_on= 'Functional Class', right_on = "PennDOT FC",how="left")
Data_AllYear1.drop(columns=['DISTRICT_N','PLANNING_P'],inplace=True)
try:
    TempDat.drop(columns="COUNTY_COD",inplace=True)
except: pass
Data_AllYear1 = Data_AllYear1.merge(TempDat,on="County",how='left')
Data_AllYear1.loc[:,'DISTRICT_N'] = Data_AllYear1.loc[:,'DISTRICT_N'].astype(str)

# CrashCost.Year = CrashCost.Year.astype(str).str.strip()
# Data_AllYear1 = Data_AllYear1.merge(CrashCost,on="Year",how="left")
# Data_AllYear1.loc[:,"Benefits"] = (Data_AllYear1.Before_Fatal-Data_AllYear1.After_Fatal)*Data_AllYear1.FatalCost\
#     + (Data_AllYear1.Before_SSI-Data_AllYear1.After_SSI)*Data_AllYear1.SSICost

Data_AllYear1.loc[:,'Before_FatalSSI'] = Data_AllYear1.Before_Fatal + Data_AllYear1.Before_SSI
# Data_AllYear1.loc[:,'After_FatalSSI'] = Data_AllYear1.After_Fatal + Data_AllYear1.After_SSI
# Data_AllYear1.loc[:,"DiffBeforeAfter_FatalSSI"] = Data_AllYear1.loc[:,'Before_FatalSSI']-Data_AllYear1.loc[:,'After_FatalSSI'] 
# Data_AllYear1.drop(columns=['FatalCost',"SSICost"],inplace=True)

MinFunctionalClassHSIP = Data_AllYear1.groupby(['Year','Proj. ID','HSIP Project ID'])['Functional Class'].min()

Data_AllYear1= Data_AllYear1.sort_values(['Year','Proj. ID','ProjectSNo','HSIP Project ID','HsipProjectSNo'])
Data_AllYear1 = Data_AllYear1.set_index(['Year','Proj. ID','ProjectSNo','HSIP Project ID','HsipProjectSNo'])
Data_AllYear1.rename(columns = {"Project Cost":"FY2021 Obligation"
                       },inplace=True)
    #Fix 2002-2007

now = datetime.now()
d4 = now.strftime("%b-%d-%Y %H_%M")
OutFi = "../ProcessedData/AllRowsCrashData-2021{}.xlsx".format(d4)
writer = pd.ExcelWriter(OutFi)
Data_AllYear1.to_excel(writer,"MasterTableHSIP",index=True,merge_cells=False)
MinFunctionalClassHSIP.to_excel(writer,"MinFunctionalClassHSIP",index=True,merge_cells=False)
DistrictDat.to_excel(writer,"DistrictDat",index=False)
PLANNING_PDat.to_excel(writer,"PLANNING_PDat",index=False)
FunctionalClassDat.to_excel(writer,"FunctionalClassDat",index=False)
TempDat.to_excel(writer,"CountyNmCode",index=False)
Error.to_excel(writer,"MissingLengthData",index=False)
# CrashCost.to_excel(writer,"CrashCost",index=False)
writer.close()



Check= Data_AllYear1[Data_AllYear1.PLANNING_P.isna()]
