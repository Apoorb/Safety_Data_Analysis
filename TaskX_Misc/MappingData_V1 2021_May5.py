# -*- coding: utf-8 -*-
"""
Created on Thu Feb 13 10:55:08 2020

@author: abibeka
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
os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\HSIP\DataMapping\April09-DataProcessing\RawData')

# Read the Data
#************************************************************************************************************
# Get the Project and Seg data...
x1 = pd.ExcelFile("Task 2.7 Project Analysis_2020-04-30.xlsx")
Years = x1.sheet_names
writer = pd.ExcelWriter('../ProcessedData/DataSummary2021.xlsx')
yr = "FY2021 Projects"
Data = x1.parse(yr,skiprows=4)
list(Data.columns)
NumProjDat = pd.DataFrame()
Data_AllYear = pd.DataFrame()

for yr in Years:
    Data = x1.parse(yr,skiprows=3,dtype={"HSIP Project ID":str})
    Data.rename(columns={"HSIP Project ID":"HSIP Proj. ID"},inplace=True)
    Data = Data[['Proj. ID','HSIP Proj. ID','County','SR','Beg Seg','Beg Off', 'End Seg','End Off']]
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
    Data1.dtypes
    
    Data1 = Data1.rename(columns = {'Proj. ID': "ProjID",
                                    "COUNTY_COD":"CountyCode",
                                    "Beg Seg":"BegSeg",
                                    "Beg Off":"BegOff",
                                    "End Seg":"EndSeg",
                                    "End Off":"EndOff"})
    Data1.loc[~(Data1.BegSeg.astype(str).str.isnumeric()),['SR','BegSeg','EndSeg','BegOff','EndOff']] =-999
    Data1[['SR','BegSeg','EndSeg','BegOff','EndOff']] = Data1[['SR','BegSeg','EndSeg','BegOff','EndOff']].astype('int')

    #Num Project summary
    temp1 = pd.DataFrame({"Year":[yr], "NumProjects":[Data1.ProjID.unique().shape[0]] } )
    NumProjDat = pd.concat([temp1, NumProjDat])
    # Get the lookup data for Segment length and AADT
    SegInfoData = pd.read_csv("RMSSEG_State_Roads.csv", 
                              usecols = ["CTY_CODE","ST_RT_NO","SEG_NO","SEG_LNGTH_FEET","CUR_AADT","X_VALUE_BGN",'Y_VALUE_BGN','X_VALUE_END',
                                         'Y_VALUE_END'])
    SegInfoData = SegInfoData.rename(columns = {'CTY_CODE':"CountyCode",
                                                'ST_RT_NO':"SR",
                                                'SEG_NO':"SegNo",
                                                'SEG_LNGTH_FEET':"SegLenFt",
                                                "CUR_AADT":"CurAADT"})
    SegInfoData.columns
    SegInfoData.head()
    
    # Join data based on the begin and end segments 
    #************************************************************************************************************
    sqlcode = '''
    select Data1.ProjID, Data1.CountyCode, Data1.SR, Data1.BegSeg, Data1.BegOff, SegInfoData.SegNo, Data1.EndSeg
    from Data1
    left join SegInfoData on SegInfoData.CountyCode = Data1.CountyCode and SegInfoData.SR = Data1.SR
    and SegInfoData.SegNo = Data1.BegSeg
    '''
    
    TestDf = ps.sqldf(sqlcode,locals())
    TestDf.isna().sum()
    TestDf.groupby(['ProjID','CountyCode','SR','BegSeg','BegOff'])['BegSeg'].first().shape
    
    
    sqlcode = '''
    select Data1.ProjID, Data1.CountyCode, Data1.SR, Data1.BegSeg, SegInfoData.SegNo, Data1.EndSeg, 
    SegInfoData.SegLenFt, Data1.BegOff, Data1.EndOff, SegInfoData.CurAADT, SegInfoData.X_VALUE_BGN,
    SegInfoData.Y_VALUE_BGN, SegInfoData.X_VALUE_END, SegInfoData.Y_VALUE_END
    from Data1
    left join SegInfoData on SegInfoData.CountyCode = Data1.CountyCode and SegInfoData.SR = Data1.SR
    and SegInfoData.SegNo between Data1.BegSeg and Data1.EndSeg
    '''
    NewDf = ps.sqldf(sqlcode,locals())
    NewDf.isna().sum()
    NewDf.groupby(['ProjID','CountyCode','SR','BegSeg','BegOff'])['BegSeg'].first().shape

    #Compute length of the segments  ---Fun
    #************************************************************************************************************
    def CalcSegDist(row):
        SegLength = 0
        if(row['BegSeg'] == row['EndSeg']): #Directly use offsets
            SegLength = row['EndOff'] - row['BegOff'] 
        else:
            if(row['BegSeg'] == row['SegNo']): # Lenght of our section = Begin segment len - offset
                SegLength = row['SegLenFt']- row['BegOff']
            elif(row['EndSeg'] == row['SegNo']): # Lenght of our section = offset
                SegLength = row['EndOff']
            elif( row['SegNo'] > row['BegSeg']) & (row['SegNo'] <row['EndSeg']) :# Lenght of our section = Seg len
                SegLength = row['SegLenFt']
            else:
                SegLength = np.nan
                #raise AssertionError("Something is wrong")
        return(SegLength)
    

    #Compute length of the segments 
    #************************************************************************************************************
    NewDf.loc[:,'CorSegLen'] = NewDf.apply(CalcSegDist, axis=1)
    # ErrorMask = (NewDf['BegSeg'] != NewDf['EndSeg']) & ~((NewDf['BegSeg'] == NewDf['SegNo']) | (NewDf['EndSeg'] == NewDf['SegNo']) | (( NewDf['SegNo'] > NewDf['BegSeg']) & (NewDf['SegNo'] <NewDf['EndSeg'])))
    # ErrorCheck = dict(NewDf[ErrorMask].T)
    NewDf = NewDf.sort_values(['CountyCode','SR','BegSeg']).reset_index()
    # Only use odd segment when the begin segment is odd and vice versa
    Mask = ((NewDf.BegSeg %2 == 0) & (NewDf.SegNo %2 == 0)) |((NewDf.BegSeg %2 != 0) & (NewDf.SegNo %2 != 0))  
    NewDfClean = NewDf[Mask]
    
    # sum(NewDfClean.Y_VALUE_BGN.isna())
    # NewDfClean = NewDfClean[NewDfClean.Y_VALUE_BGN.isna()]
    NewDfClean.loc[:,"BegGeom"] = NewDfClean[["X_VALUE_BGN","Y_VALUE_BGN"]].apply(lambda x: Point((x[0],x[1])),axis=1)
    NewDfClean.loc[:,"EndGeom"] = NewDfClean[["X_VALUE_END","Y_VALUE_END"]].apply(lambda x: Point((x[0],x[1])),axis=1)
    NewDfClean.loc[:,"LineSeg"] = NewDfClean[["BegGeom","EndGeom"]].apply(lambda x: LineString(x.tolist()), axis=1)
    #https://gis.stackexchange.com/questions/202190/turning-geodataframe-of-x-y-coordinates-into-linestrings-using-groupby
    NewDfClean = NewDfClean.groupby(['ProjID','CountyCode','SR','BegSeg','BegOff']).agg({'CorSegLen':'sum','CurAADT':'first',
                                                                                         'LineSeg': lambda x:MultiLineString(x.tolist()) }).reset_index()
    NewDfClean.dtypes
    Data1.dtypes
    
    # NewDfClean.loc[:,['ProjID','CountyCode','SR','BegSeg','BegOff']] = NewDfClean.loc[:,['ProjID','CountyCode','SR','BegSeg','BegOff']].applymap(lambda x: int(x))
    # Data1.loc[:,['ProjID','CountyCode','SR','BegSeg','BegOff']] = Data1.loc[:,['ProjID','CountyCode','SR','BegSeg','BegOff']].applymap(lambda x: int(x))
    
    CmList = ['ProjID','CountyCode','SR','BegSeg','BegOff']
    Data1 = Data1.rename(columns ={"HSIP Proj. ID":"HSIP_Proj_ID"})
    FinDat = pd.merge(Data1, NewDfClean, left_on = CmList, right_on = CmList, how = 'left')
    FinDat1 = FinDat.set_index(['ProjID','HSIP_Proj_ID','County','CountyCode','SR','BegSeg','BegOff','EndSeg','EndOff'])
    FinDat1.drop(columns = 'LineSeg',inplace=True)
    
    #Write the output
    #***********************************************************************************************************
    FinDat1.to_excel(writer,yr,na_rep = "NoData", engine='xlsxwriter')
    FinDat1.loc[:,"Year"] = yr
    Data_AllYear = pd.concat([Data_AllYear,FinDat1])
    
    Workbook = writer.book
    format1 = Workbook.add_format({'bg_color':   '#FFC7CE',
                               'font_color': '#9C0006'})
    worksheet = writer.sheets[yr]
    worksheet.conditional_format('A1:J1000', {'type':'text',
                                              'criteria':'containing',
                                              'value': 'NoData',
                                              'format' : format1})

writer.save()











