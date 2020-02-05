# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 17:32:48 2020

Purpose: Reshape data by County and add districts to the data
@author: abibeka
"""
import os
import glob
import pandas as pd
import re

os.chdir(r'H:\21\21659 - E03983 - PennDOT BOMO Safety\WO8 - HSIP Implementation Plan\task 2.1\annual crashes\Crash Analsys - Apoorba\RawData')


Wb_Name = 'Crash_Statistics'
Wb_Name = 'Fatality_Statistics'
Wb_Name = 'Suspected_Serious_Injury_Statistics'

x1 = pd.ExcelFile(Wb_Name+'.xlsx')
x1.sheet_names

x2 = pd.ExcelFile('Name-KeyMap.xlsx')
NmKeys = x2.parse()

x3 = pd.ExcelFile('County_District_KeyMap.xlsx')
DistrictKeys = x3.parse('Data')


NameDict = {}
NmKeys.loc[:,'Acnm'] = NmKeys.Acnm.str.strip()
NmKeys.loc[:,'Acnm'] = NmKeys.loc[:,'Acnm'].str.replace('/','_')
NmKeys.loc[:,'Acnm'] = NmKeys.loc[:,'Acnm'].str.replace('*','+')
NameDict['Crash_Statistics'] = NmKeys[['Crashes','Acnm']].dropna().set_index('Crashes').to_dict()['Acnm']
NameDict['Fatality_Statistics'] = NmKeys[['Fatalities','Acnm']].dropna().set_index('Fatalities').to_dict()['Acnm']
NameDict['Suspected_Serious_Injury_Statistics'] = NmKeys[['Suspected Serious Injuries','Acnm']].dropna().set_index('Suspected Serious Injuries').to_dict()['Acnm']



#**********************************************************************************
#**********************************************************************************


FinalDat = pd.DataFrame()
AllCountyDatDict = {}
ShNm = x1.sheet_names[1]
for ShNm in x1.sheet_names:
    if ShNm != 'Statewide':
        CountyNm = ShNm.split('(')[0]
        tempDat = x1.parse(ShNm, skiprows=2,nrows=35)
        tempDat.rename(columns = {"Unnamed: 0":"CrashCategory"},inplace=True)
        tempDat.loc[tempDat.CrashCategory == "Major Injuries in Vehicle Failure Related Crashes (any factor)", "CrashCategory"] = "Suspected Serious Injuries in Vehicle Failure Related Crashes (any factor)"
        tempDat.loc[:,'CountyNm'] = CountyNm
        AllCountyDatDict[CountyNm] = tempDat
        FinalDat  = pd.concat([FinalDat,tempDat])
        
#**********************************************************************************
#**********************************************************************************

   
StateWide_From_Data = x1.parse('Statewide',skiprows=2,nrows=35)
StateWide_From_Data.rename(columns = {"Unnamed: 0":"CrashCategory"},inplace=True)
StateWide_From_Data.set_index("CrashCategory",inplace=True)
StateWide_From_Data.columns

Col = list(FinalDat.columns)
Col2 = list(StateWide_From_Data.columns)

Col.remove('CrashCategory')
Col.remove('CountyNm')
StateWideCHECK = FinalDat.groupby('CrashCategory')[Col].sum()
StateWide_From_Data.rename(columns= {'2012':2012},inplace=True)


CHECK_dat = StateWide_From_Data.merge(StateWideCHECK,left_index=True,right_index=True,suffixes=("_RawData","_Satewide"),how='right')
ColLab = ['{}_Diff'.format(i) for i in Col]
CHECK_dat[ColLab] = StateWide_From_Data[Col] - StateWideCHECK[Col]
CHECK_dat.columns = pd.MultiIndex.from_tuples([tuple(x.split('_')) for x in CHECK_dat.columns],sortorder =1, names=['year','data'])
midx = CHECK_dat.columns
CHECK_dat = CHECK_dat.sort_index(axis =1, level = [0,1], ascending = [True,False])


def highlight_zero(val):
    '''
    highlight the zero
    '''
    is_zero = val != 0
    return ('background-color: yellow' if is_zero else '' )

idx = pd.IndexSlice

CHECK_dat.loc[:,idx[:,'Diff']].style.applymap(highlight_zero)
#**********************************************************************************
#**********************************************************************************


os.chdir('..')
os.getcwd()
def ReplaceValue(key, NameDict):
    RetVal = NameDict[key]
    return(RetVal)

FinalDat.loc[:,'CountyNm']= FinalDat.loc[:,'CountyNm'].str.capitalize().str.strip().str.replace('Mckean','McKean')
FinalDat.loc[:,'CountyNm'] = FinalDat.loc[:,'CountyNm'].str.replace('Huntington','Huntingdon')
FinalDat =FinalDat.merge(DistrictKeys,left_on='CountyNm',right_on = 'CountyNm', how = 'left')

sum(FinalDat.DistrictA.isna())
GpBy = FinalDat.groupby('CrashCategory')  
outFi = Wb_Name+ "_Processed.xlsx"
writer=pd.ExcelWriter(outFi)
CHECK_dat.to_excel(writer,"Check",na_rep='-',index=True)
for CrshCat, group in GpBy:
    CrshCat = NameDict[Wb_Name][CrshCat]
    group = group.set_index('CountyNm')
    group.to_excel(writer,CrshCat,na_rep='-',index=True)
writer.save() 






   
