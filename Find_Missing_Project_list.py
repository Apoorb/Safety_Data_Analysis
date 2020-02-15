# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 13:48:48 2020

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
os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\HSIP\DataMapping\MissingProjCompareData')


# Read the Data
#************************************************************************************************************
# Get the Project and Seg data...
x1 = pd.ExcelFile("2019 HSIP Program Benefit Cost Analysis 5 Year.xlsx")
Years = x1.sheet_names
writer = pd.ExcelWriter('DataSummary.xlsx')
Years.remove('Summary (Injuries)'); Years.remove('Summary (Crashes)');Years.remove('Functional Classifications')
yr = "2002-2007"
UniqueProjDat = pd.DataFrame()
for yr in Years:
    Data = x1.parse(yr,skiprows=4)

    Data = Data[['Proj. ID','County','SR','Beg Seg','Beg Off', 'End Seg','End Off']]
    Data1 = Data.fillna(method='ffill',axis= 0 )
    Data1.loc[:,"County"] = Data1.loc[:,"County"].str.upper()
    if yr == "2002-2007":
        Data1.dtypes
        Data1.SR.value_counts()
        Data1.SR = Data1.SR.astype('int64')
        Data1.loc[Data1["Proj. ID"] == "80076\n80077","Proj. ID"] = "80076"
        Data1["Proj. ID"] = Data1["Proj. ID"].astype('int64')
        tempData = Data1.loc[Data1["Proj. ID"] == 80076].reset_index(drop=True)
        tempData.loc[tempData["Proj. ID"] == 80076,"Proj. ID"] = 80077
        Data1 = pd.concat([Data1,tempData])

    Data1.isna().sum()
    Data1 = Data1.rename(columns = {'Proj. ID': "ProjID",
                                    "Beg Seg":"BegSeg",
                                    "Beg Off":"BegOff",
                                    "End Seg":"EndSeg",
                                    "End Off":"EndOff"})
   
    UnqPrj = Data1.ProjID.unique()
    Entries = len(UnqPrj)
    YearRep = [yr]*Entries
    tempDa1 = pd.DataFrame({"ProjID" : UnqPrj, "Year":YearRep})
    UniqueProjDat = pd.concat([UniqueProjDat,tempDa1])
    
    
UniqueProjDat[UniqueProjDat.duplicated("ProjID")]
# Read the Master list of projects 
    
x2 = pd.ExcelFile("HSIP obligations since SAFETEA-LU-1-30-2020.xlsx")
x2.sheet_names    
MasterDat  =x2.parse()
MasterDat = MasterDat[~MasterDat.Project.isna()]

UniqueProjDat.ProjID = UniqueProjDat.ProjID.astype("int64") 
MasterDat.Project = MasterDat.Project.astype("int64") 


MasterDat1 =  pd.merge(MasterDat,UniqueProjDat, left_on ="Project",right_on="ProjID",how="left")
sum(~MasterDat1.ProjID.isna())

OurProj = set(UniqueProjDat.ProjID)
AllProj = set(MasterDat.Project)
MissingFromMaster = OurProj.difference(AllProj)
ProjectsMissingFromMasterList = UniqueProjDat[UniqueProjDat.ProjID.isin(MissingFromMaster)]


CheckProj = set(MasterDat1.Project)
GetZeroProj = MissingFromMaster.intersection(CheckProj)

writer = pd.ExcelWriter('ListMissingProjects.xlsx')
MasterDat1[["ProjID","Year"]] = MasterDat1[["ProjID","Year"]].fillna('NoData')
MasterDat1.to_excel(writer,"MissData", engine='xlsxwriter',index=False)
ProjectsMissingFromMasterList.to_excel(writer,"ProjectsMissingFromMasterList", engine='xlsxwriter',index=False)
UniqueProjDat.to_excel(writer,"ListOfAllProjectsInBCA", engine='xlsxwriter',index=False)

Workbook = writer.book
format1 = Workbook.add_format({'bg_color':   '#FFC7CE',
                           'font_color': '#9C0006'})
worksheet = writer.sheets['MissData']
worksheet.conditional_format('N1:O625', {'type':'text',
                                          'criteria':'containing',
                                          'value': 'NoData',
                                          'format' : format1})


writer.save()

