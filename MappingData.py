# -*- coding: utf-8 -*-
"""
Created on Thu Feb 13 10:55:08 2020

@author: abibeka
"""


# Load Libraries
#************************************************************************************************************
import pandas as pd
import os 
import geopandas as gpd
import pandasql as ps
import numpy as np
os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\HSIP\DataMapping')

# Read the Data
#************************************************************************************************************
# Get the Project and Seg data...
x1 = pd.ExcelFile("2019 HSIP Program Benefit Cost Analysis 5 Year - Kittelson Master - v1.xlsx")
Years = x1.sheet_names
writer = pd.ExcelWriter('Text.xlsx')
Years.remove('Summary (Injuries)'); Years.remove('Summary (Crashes)');Years.remove('Long Narrative');Years.remove('Functional Classifications')
for yr in Years:
    Data = x1.parse(yr,skiprows=4)
    #Fix 2002-2007
    if yr == "2002-2007":
        Cols = Data.columns.to_list()
        ColRename = Data.columns[[Cols.index("From"),Cols.index("From")+1,Cols.index("To"), Cols.index("To")+1]]
        ColRenameDict = dict(zip(ColRename, ["Beg Seg","Beg Off","End Seg","End Off"])) 
        Data = Data.rename(columns = ColRenameDict)
        

    
    Data = Data[['Proj. ID','County','SR','Beg Seg','Beg Off', 'End Seg','End Off']]
    Data1 = Data.fillna(method='ffill',axis= 0 )
    Data1.loc[:,"County"] = Data1.loc[:,"County"].str.upper()
    
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
                                    "COUNTY_COD":"CountyCode",
                                    "Beg Seg":"BegSeg",
                                    "Beg Off":"BegOff",
                                    "End Seg":"EndSeg",
                                    "End Off":"EndOff"})
    
    # Get the lookup data for Segment length and AADT
    SegInfoData = pd.read_csv("RMSSEG_State_Roads.csv", 
                              usecols = ["CTY_CODE","ST_RT_NO","SEG_NO","SEG_LNGTH_FEET","CUR_AADT"])
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
    SegInfoData.SegLenFt, Data1.BegOff, Data1.EndOff, SegInfoData.CurAADT
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
    NewDfClean = NewDfClean.groupby(['ProjID','CountyCode','SR','BegSeg','BegOff']).agg({'CorSegLen':'sum','CurAADT':'first'}).reset_index()
    
    
    CmList = ['ProjID','CountyCode','SR','BegSeg','BegOff']
    FinDat = pd.merge(Data1, NewDfClean, left_on = CmList, right_on = CmList, how = 'left')
    FinDat1 = FinDat.set_index(['ProjID','County','CountyCode','SR','BegSeg','BegOff'])
    
    #Write the output
    #***********************************************************************************************************
    FinDat1.to_excel(writer,yr)

writer.save()
