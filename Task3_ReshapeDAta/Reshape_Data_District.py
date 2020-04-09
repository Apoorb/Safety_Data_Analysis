# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 17:32:48 2020

Purpose: Aggregate Data By District
@author: abibeka
"""
import os
import glob
import pandas as pd
import re

# Read the files
# os.chdir(r'H:\21\21659 - E03983 - PennDOT BOMO Safety\WO8 - HSIP Implementation Plan\task 2.1\annual crashes\Crash Analsys - Apoorba\RawData')
os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\HSIP\Crash Analsys - Apoorba')
os.getcwd()
Wb_Name = 'Crash_Statistics_Processed'
Wb_Name = 'Fatality_Statistics_Processed'
Wb_Name = 'Suspected_Serious_Injury_Statistics_Processed'

x1 = pd.ExcelFile(Wb_Name+'.xlsx')
x1.sheet_names
CrashTypes = x1.sheet_names
CrashTypes.remove('Check')

# Get SHSP key val map
x3 = pd.ExcelFile('RawData/OutputGeoJsonKeys.xlsx')
OutputCatDat = x3.parse()
OutputCatDat.columns
OutputCatDat = OutputCatDat.applymap(lambda x : x.strip())

# Get common attributes
dat = x1.parse("Bicyclist")
dat.columns
CommonAttributes = dat[['District','TotalLinearMiles']].groupby("District").agg({'District':'first','TotalLinearMiles':'sum'}).reset_index(drop=True)

# Out file
outFi = Wb_Name+ "_District.xlsx"
writer=pd.ExcelWriter(outFi)

# Groupby and Write Csv
FinData = pd.DataFrame()
for shNm in CrashTypes:
    dat = x1.parse(shNm)
    dat.drop(columns = ['TotalLinearMiles','TotalDVMT'],inplace=True)
    Dat1  = dat.groupby(['District','CrashCategory']).sum().reset_index()
    Dat1.loc[:,"CrashAbb"] = shNm
    Dat1 = pd.merge(Dat1,OutputCatDat,how="left",on="CrashAbb")
    Dat1.District.value_counts()
    Dat1 = Dat1.merge(CommonAttributes, left_on = "District", right_on = "District", how = "left" )
    FinData = pd.concat([FinData,Dat1])
    Dat1.to_excel(writer,shNm,index=False)
writer.save()

#Fix column names 
FinData.rename(columns= {"2016*":2016,"SHSP Focus Area":"SHSP_Focus_Cat","Crash Data Focus Areas":"Crash_Focus_Cat"},inplace=True)
#Remove all cat except for the one SHSP data
FinData = FinData[~FinData.Crash_Focus_Cat.isna()]
FinData.columns
#Get columns for year
YearCols = FinData.columns.tolist()
YearCols.remove('CrashAbb')
YearCols.remove('CrashCategory')
YearCols.remove('District')
YearCols.remove('TotalLinearMiles')
YearCols.remove('SHSP_Focus_Cat')
YearCols.remove('Crash_Focus_Cat')

# Get new year column name
NewYearCols = []
for i in YearCols:
    FinData.rename(columns = {i:"Yr-{}".format(i)},inplace=True)
    NewYearCols.append("Yr-{}".format(i))
    
    NewYearCols

# Keep data aside for SHSP Categories 
maskSHSP = FinData.SHSP_Focus_Cat.isin(['Reducing Impaired Driving','Lane Departures','Reducing Speeding & Aggressive Driving'])
FinData_SHSP = FinData[maskSHSP].groupby(['District','SHSP_Focus_Cat'])[NewYearCols].sum().reset_index()
    

FinData[NewYearCols].isna().sum()
FinData[NewYearCols] = FinData[NewYearCols].fillna("-")

# Createa dict for crashes for years and SHSP cat and crash focus cat
FinData.loc[:,"SingleDictYr"] = FinData[['SHSP_Focus_Cat','Crash_Focus_Cat']+NewYearCols].to_dict('records')
FinData.drop(columns=NewYearCols+['SHSP_Focus_Cat','Crash_Focus_Cat'],inplace=True)
# Make as bigger dict
FinData.loc[:,"Comb_SingleDictYr"]  = FinData[['CrashCategory','SingleDictYr']].apply(lambda x: {x[0]:x[1]},axis=1)
FinData.drop(columns=['CrashAbb','CrashCategory','SingleDictYr'],inplace=True)
# Convert a list of dict to dict
def Res_To_Dict(List_Dicts,tag):
    result= {}
    for d in List_Dicts:
        result.update(d)
    return({tag:result})
#Collapse multiple rows for same district
FinData1 = FinData.groupby(['District'])['Comb_SingleDictYr'].apply(list).reset_index()
tag = ''
if Wb_Name =='Crash_Statistics_Processed':
    tag = "Crash"
elif Wb_Name =='Fatality_Statistics_Processed':
    tag = "Fatalities"
elif Wb_Name =='Suspected_Serious_Injury_Statistics_Processed':
    tag = "SSI"
    
FinData1.loc[:,"Comb_SingleDictYr1"] = FinData1.Comb_SingleDictYr.apply(lambda x: Res_To_Dict(x,tag))
FinData1.drop(columns="Comb_SingleDictYr",inplace=True)
# Add common attributes
Dat2 = Dat1[['District','TotalLinearMiles']]
FinData1 = FinData1.merge(Dat2, how='inner',on = "District")
FinData1.to_json("DistrictLeafletData/District_{}.json".format(Wb_Name),orient = 'records')



# Output Data for SHSP Cats
#********************************************************************************************************************
# Createa dict for crashes for years and SHSP cat and crash focus cat

tag2 = ''
if Wb_Name =='Crash_Statistics_Processed':
    tag2 = "Crash in "
elif Wb_Name =='Fatality_Statistics_Processed':
    tag2 = "Fatalities in "
elif Wb_Name =='Suspected_Serious_Injury_Statistics_Processed':
    tag2 = "Suspected Serious Injury in "
    
    
FinData_SHSP.loc[:,"SingleDictYr"] = FinData_SHSP[['SHSP_Focus_Cat']+NewYearCols].to_dict('records')
FinData_SHSP.drop(columns=NewYearCols,inplace=True)

FinData_SHSP.SHSP_Focus_Cat = FinData_SHSP.SHSP_Focus_Cat.apply(lambda x: tag2+x)
# Make as bigger dict
FinData_SHSP.loc[:,"Comb_SingleDictYr"]  = FinData_SHSP[['SHSP_Focus_Cat','SingleDictYr']].apply(lambda x: {x[0]:x[1]},axis=1)
FinData_SHSP.drop(columns=['SHSP_Focus_Cat','SingleDictYr'],inplace=True)
# Convert a list of dict to dict
def Res_To_Dict(List_Dicts,tag):
    result= {}
    for d in List_Dicts:
        result.update(d)
    return({tag:result})
#Collapse multiple rows for same district
FinData_SHSP1 = FinData_SHSP.groupby(['District'])['Comb_SingleDictYr'].apply(list).reset_index()
tag = ''
if Wb_Name =='Crash_Statistics_Processed':
    tag = "Crash"
elif Wb_Name =='Fatality_Statistics_Processed':
    tag = "Fatalities"
elif Wb_Name =='Suspected_Serious_Injury_Statistics_Processed':
    tag = "SSI"
    
FinData_SHSP1.loc[:,"Comb_SingleDictYr1"] = FinData_SHSP1.Comb_SingleDictYr.apply(lambda x: Res_To_Dict(x,tag))
FinData_SHSP1.drop(columns="Comb_SingleDictYr",inplace=True)
# Add common attributes
Dat2 = Dat1[['District','TotalLinearMiles']]
FinData_SHSP1 = FinData_SHSP1.merge(Dat2, how='inner',on = "District")
FinData_SHSP1.to_json("DistrictLeafletData/SHSP_District_{}.json".format(Wb_Name),orient = 'records')