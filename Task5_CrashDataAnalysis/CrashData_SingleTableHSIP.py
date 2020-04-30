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
Years.remove('CostofCrashes')
yr = "2015"
# yr= "2007-2002"
Data = x1.parse(yr,skiprows=4)
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
 'Date Updated']

CrashColumnsTemp = ["CrashType1","CrashType2","CrashType3",
 'Before_Fatal','Before_SSI','After_Fatal', 'After_SSI',
 'Type1_Before_Fatal','Type1_Before_SSI','Type1_After_Fatal', 'Type1_After_SSI',
'Type2_Before_Fatal','Type2_Before_SSI','Type2_After_Fatal' , 'Type2_After_SSI',
'Type3_Before_Fatal','Type3_Before_SSI','Type3_After_Fatal' , 'Type3_After_SSI']

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

CrashCols = [ 'Before_Fatal','Before_SSI','After_Fatal', 'After_SSI',
 'Type1_Before_Fatal','Type1_Before_SSI','Type1_After_Fatal', 'Type1_After_SSI',
'Type2_Before_Fatal','Type2_Before_SSI','Type2_After_Fatal' , 'Type2_After_SSI',
'Type3_Before_Fatal','Type3_Before_SSI','Type3_After_Fatal' , 'Type3_After_SSI']

Data.rename(columns = {"Project Title":"Title","Constr. Completion Date":"Constr. Comp. Date",
                   "Notice to Proceed Date":"NTP or Let Date",
                   "HSIP Project ID_GTE":"HSIP Project ID"
                   },inplace=True)
    #Fix 2002-2007
Data = Data[KeepCols+CrashColumnsNew]
NumProjDat = pd.DataFrame()
Data_AllYear = pd.DataFrame()
Error = pd.DataFrame()

#Crash Cost Data
CrashCost = x1.parse('CostofCrashes')
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
# 
DistrictDat = TempDat.groupby('DISTRICT_N')[['NumDrivers','DVMT']].sum().reset_index()
DistrictDat.rename(columns = {'NumDrivers':'NumDrivers_District',"DVMT":'DVMT_District'},inplace=True)

sum(DistrictDat.DVMT_District)
PLANNING_PDat = TempDat.groupby('PLANNING_P')[['NumDrivers','DVMT']].sum().reset_index()
PLANNING_PDat.rename(columns = {'NumDrivers':'NumDrivers_PlanningP',"DVMT":'DVMT_PlanningP'},inplace=True)

yr = "2004-2007"
for yr in Years:
    #Read the DAta
    Data = x1.parse(yr,skiprows=4,dtype={"HSIP Project ID":str},parse_cols ="A:DI")
    Data = Data[~Data['Beg Seg'].isna()]
    Data.rename(columns = {"Project Title":"Title","Constr. Completion Date":"Constr. Comp. Date",
                       "Notice to Proceed Date":"NTP or Let Date",
                       "HSIP Project ID_GTE":"HSIP Project ID"
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
    Error.loc[:,'ImputedLength'] = Error['End Off']+ 500
    Data1.loc[Data1.loc[:,"Length (ft.)"]=="NoData", "Length (ft.)"] = Data1.loc[Data1.loc[:,"Length (ft.)"]=="NoData", 'End Off'] + 500 # Assume Beg Seg is 500 ft.
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
    maskOldDateUpd = Data1["Date Updated"].isna() #Add some value to Date updated 
    Data1.loc[maskOldDateUpd,"Date Updated"]  = "Old"
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

Year_AnalysisPeriod = x1.parse('Year_AnalysisPeriod',skiprows=0)
Year_AnalysisPeriod.Year =Year_AnalysisPeriod.Year.astype(str)
Data_AllYear1 = Data_AllYear1.merge(Year_AnalysisPeriod, on='Year',how='left')
# Add District data and Functional Class
Data_AllYear1 = Data_AllYear1.merge(DistrictDat,on="DISTRICT_N", how='left')
Data_AllYear1 = Data_AllYear1.merge(PLANNING_PDat,on="PLANNING_P", how='left')
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

x1.sheet_names
PlanningP_ImpTy= x1.parse('Project_ImpType_Planning_P_Map',skiprows=2)
PlanningP_ImpTy.dtypes
PlanningP_ImpTy.drop(columns="PLANNING_P",inplace=True) #Don't want to add it this way
# check with Jesus about where he got the mapping for county by Planning_P
Data_AllYear1.drop(columns = ['Improvement Type'],inplace=True)
Data_AllYear1 = Data_AllYear1.merge(PlanningP_ImpTy,on=['Proj. ID','HSIP Project ID'],how='left')
Data_AllYear1.PLANNING_P.value_counts().sum()
Data_AllYear1['Improvement Type'].value_counts().sum()

CrashCost.Year = CrashCost.Year.astype(str).str.strip()
Data_AllYear1 = Data_AllYear1.merge(CrashCost,on="Year",how="left")
Data_AllYear1.loc[:,"Benefits"] = (Data_AllYear1.Before_Fatal-Data_AllYear1.After_Fatal)*Data_AllYear1.FatalCost\
    + (Data_AllYear1.Before_SSI-Data_AllYear1.After_SSI)*Data_AllYear1.SSICost

Data_AllYear1.loc[:,'Before_FatalSSI'] = Data_AllYear1.Before_Fatal + Data_AllYear1.Before_SSI
Data_AllYear1.loc[:,'After_FatalSSI'] = Data_AllYear1.After_Fatal + Data_AllYear1.After_SSI
Data_AllYear1.loc[:,"DiffBeforeAfter_FatalSSI"] = Data_AllYear1.loc[:,'Before_FatalSSI']-Data_AllYear1.loc[:,'After_FatalSSI'] 
Data_AllYear1.drop(columns=['FatalCost',"SSICost"],inplace=True)

MinFunctionalClassHSIP = Data_AllYear1.groupby(['Year','Proj. ID','HSIP Project ID'])['Functional Class'].min()

Data_AllYear1= Data_AllYear1.sort_values(['Year','Proj. ID','ProjectSNo','HSIP Project ID','HsipProjectSNo'])
Data_AllYear1 = Data_AllYear1.set_index(['Year','Proj. ID','ProjectSNo','HSIP Project ID','HsipProjectSNo'])


now = datetime.now()
d4 = now.strftime("%b-%d-%Y %H_%M")
OutFi = "../ProcessedData/AllRowsCrashData{}.xlsx".format(d4)
writer = pd.ExcelWriter(OutFi)
Data_AllYear1.to_excel(writer,"MasterTableHSIP",index=True,merge_cells=False)
MinFunctionalClassHSIP.to_excel(writer,"MinFunctionalClassHSIP",index=True,merge_cells=False)
DistrictDat.to_excel(writer,"DistrictDat",index=False)
PLANNING_PDat.to_excel(writer,"PLANNING_PDat",index=False)
FunctionalClassDat.to_excel(writer,"FunctionalClassDat",index=False)
TempDat.to_excel(writer,"CountyNmCode",index=False)
Error.to_excel(writer,"MissingLengthData",index=False)
CrashCost.to_excel(writer,"CrashCost",index=False)
writer.close()



Check= Data_AllYear1[Data_AllYear1.PLANNING_P.isna()]

x2 = pd.ExcelFile('../ProcessedData/CrashDataAggByHSIP_ProjApr-22-2020 19_30.xlsx')
x2.sheet_names
TestData = x2.parse('HSIP_Project_by_Year')
A = TestData.loc[TestData.Year=="2004-2007","HSIP Project ID"].values
B = Data_AllYear.loc[Data_AllYear.Year=="2004-2007","HSIP Project ID"].values
np.setdiff1d(A,B)

Data_AllYear.columns
Data_AllYear.dtypes
Data_AllYear.loc[:,'NumProjRows'] = 1
Data_AllYear.loc[Data_AllYear.AADT=="NoData","AADT"] =  -9999
Data_AllYear.AADT= Data_AllYear.AADT.astype(int)
GrpBy ={
        'NTP or Let Date': 'first', 'Constr. Comp. Date': 'first',
       'Title':'first', 'PennDOT Description': 'first', 'Alternate Description':'first',
       'Functional Class':'min', 'Cost Distribution':'first', 'Improvement Category':'first',
       'Improvement Type':'first', 'Emphasis Area Related Improv. Type':'first',
       'Project Cost':'first', 'Funds for Safety':'first'
       , 'Funds for Safety Related Proj_Ed':'first',
       'County': lambda x: "_".join(set(x)),
       'DISTRICT_N': lambda x: "_".join(set(x)),
       'PLANNING_P': lambda x: "_".join(set(x)),
        'Method for Site Selection':'first', 'Date Updated':'first',
        "CrashType1":"first",
        "CrashType2":"first",
        "CrashType3":"first",
       'Before_Fatal':'first', 'Before_SSI':'first', 'After_Fatal':'first',
       'After_SSI':'first',
       'Type1_Before_Fatal':'first', 'Type1_Before_SSI':'first',
       'Type1_After_Fatal':'first','Type1_After_SSI':'first',
       'Type2_Before_Fatal':'first', 'Type2_Before_SSI':'first',
       'Type2_After_Fatal':'first', 'Type2_After_SSI':'first',
       'Type3_Before_Fatal':'first',
       'Type3_Before_SSI':'first', 'Type3_After_Fatal':'first', 'Type3_After_SSI':'first',
       'FracLen':'sum', 'CostPerRow':'sum',
        'NumProjRows':'sum',
        'AADT':'max'
 }

DataHSIPProjSum =Data_AllYear.groupby(['Year','Proj. ID','HSIP Project ID']).agg(GrpBy).reset_index()
DataHSIPProjSum.DISTRICT_N.str.split('_',expand=True)[0].value_counts()

DataHSIPProjSum.County.str.split('_',expand=True)[1].value_counts()
tp3 = DataHSIPProjSum[~DataHSIPProjSum.County.str.split('_',expand=True)[1].isnull()]


try:
    if (DataHSIPProjSum.PLANNING_P.str.split('_',expand=True).shape[1]>=2):
        tp = DataHSIPProjSum[~DataHSIPProjSum.PLANNING_P.str.split('_',expand=True)[1].isnull()]
        if (tp['Proj. ID'].values!= 80076): raise AssertionError('Need to handle multiple Planning Org')
        tp2 = DataHSIPProjSum[~DataHSIPProjSum.County.str.split('_',expand=True)[1].isnull()]
        if (tp2['Proj. ID'].values!= 80076): raise AssertionError('Need to handle multiple County')
except AssertionError:
    raise

# Get the Sum of Fatal and SSI 

DataHSIPProjSum[CrashCols] = DataHSIPProjSum[CrashCols].replace('---',np.NaN)
DataHSIPProjSum[CrashCols] = DataHSIPProjSum[CrashCols].astype('Int64')
DataHSIPProjSum.loc[:,'Before_FatalSSI'] = DataHSIPProjSum.Before_Fatal + DataHSIPProjSum.Before_SSI
DataHSIPProjSum.loc[:,'After_FatalSSI'] = DataHSIPProjSum.After_Fatal + DataHSIPProjSum.After_SSI
DataHSIPProjSum.loc[:,'Type1_Before_FatalSSI'] = DataHSIPProjSum.Type1_Before_Fatal + DataHSIPProjSum.Type1_Before_SSI
DataHSIPProjSum.loc[:,'Type1_After_FatalSSI'] = DataHSIPProjSum.Type1_After_Fatal + DataHSIPProjSum.Type1_After_SSI
DataHSIPProjSum.loc[:,'Type2_Before_FatalSSI'] = DataHSIPProjSum.Type2_Before_Fatal + DataHSIPProjSum.Type2_Before_SSI
DataHSIPProjSum.loc[:,'Type2_After_FatalSSI'] = DataHSIPProjSum.Type2_After_Fatal + DataHSIPProjSum.Type2_After_SSI
DataHSIPProjSum.loc[:,'Type3_Before_FatalSSI'] = DataHSIPProjSum.Type3_Before_Fatal + DataHSIPProjSum.Type3_Before_SSI
DataHSIPProjSum.loc[:,'Type3_After_FatalSSI'] = DataHSIPProjSum.Type3_After_Fatal + DataHSIPProjSum.Type3_After_SSI
Crash_FatalSSI_Col = ['Before_FatalSSI','After_FatalSSI','Type1_Before_FatalSSI','Type1_After_FatalSSI','Type2_Before_FatalSSI',
                      'Type2_After_FatalSSI','Type3_Before_FatalSSI','Type3_After_FatalSSI']


DataHSIPProjSum.dtypes
Year_AnalysisPeriod = x1.parse('Year_AnalysisPeriod',skiprows=0)
Year_AnalysisPeriod.Year =Year_AnalysisPeriod.Year.astype(str)
DataHSIPProjSum = DataHSIPProjSum.merge(Year_AnalysisPeriod, on='Year',how='left')
DataHSIPProjSum.loc[:,Crash_FatalSSI_Col] = DataHSIPProjSum[Crash_FatalSSI_Col].div(DataHSIPProjSum["Analysis Period"], axis=0)


#DataHSIPProjSum.drop(columns = CrashCols, inplace=True)
# Add District data and Functional Class
DataHSIPProjSum = DataHSIPProjSum.merge(DistrictDat,on="DISTRICT_N", how='left')
DataHSIPProjSum = DataHSIPProjSum.merge(PLANNING_PDat,on="PLANNING_P", how='left')
DataHSIPProjSum.loc[DataHSIPProjSum['Functional Class'].isna(),'Functional Class'] = -999
DataHSIPProjSum.loc[:,'Functional Class'] = DataHSIPProjSum['Functional Class'].astype(int)
FunctionalClassDat.dtypes
DataHSIPProjSum = DataHSIPProjSum.merge(FunctionalClassDat,left_on= 'Functional Class', right_on = "PennDOT FC",how="left")

DataHSIPProjSum.drop(columns=['DISTRICT_N','PLANNING_P'],inplace=True)
DataHSIPProjSum = DataHSIPProjSum.merge(TempDat,on="County",how='left')
DataHSIPProjSum.loc[:,'DISTRICT_N'] = DataHSIPProjSum.loc[:,'DISTRICT_N'].astype(str)

x1.sheet_names
PlanningP_ImpTy= x1.parse('Project_ImpType_Planning_P_Map',skiprows=2)
PlanningP_ImpTy.dtypes
DataHSIPProjSum.drop(columns = ['PLANNING_P','Improvement Type'],inplace=True)
DataHSIPProjSum = DataHSIPProjSum.merge(PlanningP_ImpTy,on=['Proj. ID','HSIP Project ID'],how='left')
DataHSIPProjSum.PLANNING_P.value_counts().sum()
DataHSIPProjSum['Improvement Type'].value_counts().sum()

# GrpBy2 = {'NumProjRows':'sum',
#  'Cost Distribution':'first',
#  'Improvement Category':'first',
#  'Improvement Type':'first',
#  'Emphasis Area Related Improv. Type':'first',
#  'Project Cost':'first',
#  'Funds for Safety':'first',
#  'County': lambda x: "_".join(set(x)),
#  'DISTRICT_N': lambda x: "_".join(set(x)),
#  'PLANNING_P': lambda x: "_".join(set(x))}
# DataProjSum =DataHSIPProjSum.groupby(['Year','Proj. ID']).agg(GrpBy2).reset_index()
#DataProjSum =Data_AllYear.groupby(['Year','Proj. ID']).agg({'Proj. ID':{'NumProjRows':'count'},"","","",""})
# DataProjSum.columns = DataProjSum.columns.droplevel(1); DataProjSum.reset_index(inplace=True)

now = datetime.now()
d4 = now.strftime("%b-%d-%Y %H_%M")

OutFi = "../ProcessedData/CrashDataAggByHSIP_Proj{}.xlsx".format(d4)

writer = pd.ExcelWriter(OutFi)
DataHSIPProjSum.to_excel(writer,"HSIP_Project_by_Year",index=False)
DistrictDat.to_excel(writer,"DistrictDat",index=False)
PLANNING_PDat.to_excel(writer,"PLANNING_PDat",index=False)
FunctionalClassDat.to_excel(writer,"FunctionalClassDat",index=False)
TempDat.to_excel(writer,"CountyNmCode",index=False)
Error.to_excel(writer,"MissingLengthData",index=False)
writer.save()