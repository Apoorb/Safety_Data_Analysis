# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 18:03:45 2020

@author: abibeka
"""

#0.0 Housekeeping. Clear variable space
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()
#1 Libraries
######################################################################################################
import os, pandas as pd,  numpy as np, seaborn as sns, re, glob,shutil
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import matplotlib 
import plotly.express as px, plotly.graph_objects as go
#Using Plotly with Spyder
#https://community.plot.ly/t/plotly-for-spyder/10527/2
from plotly.offline import plot
import plotly.figure_factory as ff
from palettable.cartocolors.diverging import Temps_7_r
Temps_7_r.hex_colors
Temps_7_r.show_discrete_image()
#2 Read Data. Rename Columns. Fix Incorrect Values
######################################################################################################
Year= ['2002', '2003', '2004-2007', '2008', '2009', '2010', '2011',
       '2012', '2013', '2014', '2015','2005.5']
CostFatal= [6685345]*7+[11295400]*4 +[6685345]
CostSSI =[1478907]*7+[655000]*4 + [1478907]
CostCrash = pd.DataFrame({'Year':Year,"CostFatal":CostFatal,"CostSSI":CostSSI})



os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\HSIP\DataMapping\May22-Processing')
CrashCols = [ 'Before_Fatal','Before_SSI','After_Fatal', 'After_SSI',
 'Type1_Before_Fatal','Type1_Before_SSI','Type1_After_Fatal', 'Type1_After_SSI',
'Type2_Before_Fatal','Type2_Before_SSI','Type2_After_Fatal' , 'Type2_After_SSI',
'Type3_Before_Fatal','Type3_Before_SSI','Type3_After_Fatal' , 'Type3_After_SSI']

Crash_FatalSSI_Col = ['Before_FatalSSI','After_FatalSSI','Type1_Before_FatalSSI','Type1_After_FatalSSI','Type2_Before_FatalSSI',
                      'Type2_After_FatalSSI','Type3_Before_FatalSSI','Type3_After_FatalSSI']

x1 = pd.ExcelFile('Master-HSIP-ProjectList-Apr4-v1.xlsx')
x1.sheet_names
FunClass = x1.parse('FunctionalClassDat')
data= x1.parse('MasterTableHSIP')
TempDa = data.groupby('HSIP Project ID')['CostPerRow'].sum().reset_index()

data =data.query('HsipProjectSNo==1'); data.drop(columns="CostPerRow",inplace=True)
data = data.merge(TempDa,on="HSIP Project ID")
data = data.assign(Type1_Before_FatalSSI=lambda x: x.Type1_Before_SSI+x.Type1_Before_Fatal,
            Type2_Before_FatalSSI=lambda x: x.Type2_Before_SSI+x.Type2_Before_Fatal,
            Type3_Before_FatalSSI=lambda x: x.Type3_Before_SSI+x.Type3_Before_Fatal,
            Type1_After_FatalSSI=lambda x: x.Type1_After_SSI+x.Type1_After_Fatal,
            Type2_After_FatalSSI=lambda x: x.Type2_After_SSI+x.Type2_After_Fatal,
            Type3_After_FatalSSI=lambda x: x.Type3_After_SSI+x.Type3_After_Fatal)

data1= data[["HSIP Project ID",'Year','PLANNING_P','DISTRICT_N','Improvement Type','Improvement Category',
             'Emphasis Area Related Improv. Type',"Roadway Type","Cost Distribution","Urban/Rural",
      'Method for Site Selection','CostPerRow',
      'CrashType1','CrashType2','CrashType3',
      'Before_FatalSSI', 'After_FatalSSI',
       'Type1_Before_FatalSSI', 'Type1_After_FatalSSI',
       'Type2_Before_FatalSSI', 'Type2_After_FatalSSI',
       'Type3_Before_FatalSSI', 'Type3_After_FatalSSI', 'Analysis Period',
        'NumDrivers_District', 'DVMT_District', 'NumDrivers_PlanningP',
       'DVMT_PlanningP','AADT']+CrashCols]
data1.loc[:,'Roadway Type'] =data1.loc[:,'Urban/Rural'] + " "+ data1.loc[:,'Roadway Type']

CrashSumColMap = { "Before_FatalSSI":"FatalSSI_Before","After_FatalSSI":"FatalSSI_After",
                      "Type1_Before_FatalSSI": "Type1_Before", "Type1_After_FatalSSI": "Type1_After",
                      "Type2_Before_FatalSSI": "Type2_Before", "Type2_After_FatalSSI": "Type2_After",
                      "Type3_Before_FatalSSI": "Type3_Before", "Type3_After_FatalSSI": "Type3_After",
                      }

data1.rename(columns={"HSIP Project ID":"HSIP_ProjID",
    'Improvement Type':"ImpType",
    "DISTRICT_N":"District",
                      'Improvement Category':"ImpCat",
                      "Method for Site Selection":"Method for Site Selection",
                      "Emphasis Area Related Improv. Type":"EmphArea", "Roadway Type":"Roadway Type",
                      "Urban/Rural":"Urban-Rural","Cost Distribution":"Cost Distribution",
                      'Analysis Period':"AnalysisPeriod"
                      },inplace=True)
RenameColumnMap = {'Before_Fatal':"Fatal_Before",'Before_SSI':"SSI_Before",
                   'After_Fatal':"Fatal_After", 'After_SSI':"SSI_After",
 'Type1_Before_Fatal':"Type1-Fatal_Before",'Type1_Before_SSI':"Type1-SSI_Before",
 'Type1_After_Fatal':"Type1-Fatal_After", 'Type1_After_SSI':"Type1-SSI_After",
'Type2_Before_Fatal':"Type2-Fatal_Before",'Type2_Before_SSI':"Type2-SSI_Before",
'Type2_After_Fatal':"Type2-Fatal_After" , 'Type2_After_SSI':"Type2-SSI_After",
'Type3_Before_Fatal':"Type3-Fatal_Before",'Type3_Before_SSI':"Type3-SSI_Before",
'Type3_After_Fatal':"Type3-Fata_Before" , 'Type3_After_SSI':"Type3-SSI_After"}
data1.rename(columns =RenameColumnMap,inplace=True )
data1["Roadway Type"].replace(regex="Principle",value="Principal",inplace=True)
data1.rename(columns =CrashSumColMap,inplace=True )
data1.loc[:,RenameColumnMap.values()] = data1[RenameColumnMap.values()].divide(data1.AnalysisPeriod,axis=0)
data1.loc[:,CrashSumColMap.values()] = data1[CrashSumColMap.values()].divide(data1.AnalysisPeriod,axis=0)
data1.loc[:,'TempIndex'] = data1.index.values
data1.ImpCat.value_counts()
# mask = data1.ImpCat.str.contains \
# ('Intersection.* traffic control',na=False,flags=re.IGNORECASE|re.DOTALL,regex=True)
# sum(mask)
# data1.loc[mask,'ImpCat'] = "Intersection traffic control"
data1.ImpCat = data1.ImpCat.str.capitalize()
data1.ImpCat.replace({'Intersection traffic control/\npedestrians and bicyclists':'Int traffic controll- Ped & Bike',
                      'Intersection traffic control/\nroadside':"Int traffic control- roadside",
                      "Intersection traffic control\nroadside": "Int traffic control- roadside",
                      "Intersection traffic control / geometry":"Int traffic control- geometry",
                      "Intersection geometry/\n traffic control":"Int traffic control- geometry",
                      "Intersection traffic control/\nroadway":"Int traffic control- roadway"},inplace=True)
data1.District = data1.District.astype("Int32").astype(str)
data1.District = "District "+data1.District
data1.groupby('District').count()
data1.District = pd.Categorical(data1.District,
                                             ["District 1","District 2","District 3","District 4","District 5",
                                              "District 6","District 8","District 9","District 10","District 11",
                                              "District 12"],ordered=True)

# data1.District.replace("District nan","NoData",inplace=True)
data1.Year= pd.Categorical(data1.Year,['2002','2003','2004-2007','2008','2009','2010','2011','2012','2013','2014','2015'],ordered=True)

#2.1 Reshape Data
######################################################################################################
data2 = pd.wide_to_long(data1,["FatalSSI",'Fatal','SSI',"Type1","Type2","Type3"],
                sep="_",suffix="\w+",i=['TempIndex'],j="B_A")
data2.reset_index(inplace=True)
data2.loc[:,'DollorSpentMil'] = data2.CostPerRow/10**6
data2.B_A = pd.Categorical(data2.B_A,["Before","After"],ordered=True)

data1.columns
data1.ImpCat.value_counts()
data1["Roadway Type"].value_counts()
data1.District.value_counts()
data1["Urban-Rural"].value_counts()
data1['Cost Distribution'].value_counts()
data1["Method for Site Selection"].value_counts()
data1.EmphArea.value_counts()

data1_TotalCrash =data1.copy()
data1_TotalCrash.loc[:,RenameColumnMap.values()] = data1_TotalCrash[RenameColumnMap.values()].multiply(data1_TotalCrash.AnalysisPeriod,axis=0)
data1_TotalCrash.loc[:,CrashSumColMap.values()] = data1_TotalCrash[CrashSumColMap.values()].multiply(data1_TotalCrash.AnalysisPeriod,axis=0)
data1_TotalCrash.groupby('ImpCat')[['FatalSSI_Before','FatalSSI_After']].sum()
data1_TotalCrash.groupby('Urban-Rural')[['FatalSSI_Before','FatalSSI_After']].sum()
data1_TotalCrash.groupby('Roadway Type')[['FatalSSI_Before','FatalSSI_After']].sum()
data1_TotalCrash.groupby('Cost Distribution')[['FatalSSI_Before','FatalSSI_After']].sum()
data1_TotalCrash.groupby('Method for Site Selection')[['FatalSSI_Before','FatalSSI_After']].sum()
data1_TotalCrash.groupby(['District','EmphArea'])[['FatalSSI_Before','FatalSSI_After']].sum()

data1_TotalCrash[RenameColumnMap.values()].sum()
data1_TotalCrash[CrashSumColMap.values()].sum()

data1.loc[:,'DollorSpentMil'] = data1.CostPerRow/10**6
data1.loc[:,"ChangeInCrashes"] = data1.FatalSSI_Before - data1.FatalSSI_After
data1_TotalCrash.loc[:,"ChangeInCrashes"] = data1_TotalCrash.FatalSSI_Before - data1_TotalCrash.FatalSSI_After
data1_TotalCrash.loc[:,"ChangeInFatal"] = data1_TotalCrash.Fatal_Before - data1_TotalCrash.Fatal_After
data1_TotalCrash.loc[:,"ChangeInSSI"] = data1_TotalCrash.SSI_Before - data1_TotalCrash.SSI_After

#3 Plots
######################################################################################################

# Focus on Satewide Trends
#Plot 1
# try: 
#     shutil.rmtree('./StateWidePlots') 
# except: 
#     print("close files")
##############################################################################
data3 = data2.groupby(['Year','B_A'])['FatalSSI','Fatal','SSI','DollorSpentMil'].sum().reset_index()
data3.loc[data3.Year=="2004-2007",['FatalSSI','Fatal','DollorSpentMil']] =data3.loc[data3.Year=="2004-2007",['FatalSSI','Fatal','DollorSpentMil']]/4

current_palette = sns.color_palette()
sns.set(font_scale=1.5,style="ticks")
dims = (7, 5)
fig, ax = plt.subplots(figsize=dims)
sns.barplot(x="Year",y='FatalSSI',data=data3, hue="B_A",ci=None, ax=ax)
sns.barplot(x="Year",y='Fatal',data=data3, hue="B_A",ci=None, ax=ax, palette=current_palette[3:])
ax.set_xticklabels(ax.get_xticklabels(), rotation=45,ha="right")
ax.set(xlabel = "Year",
       ylabel="Fatal + Suspected Serious Injury\n Crashes per year")
ax2 = ax.twinx()
ax2.plot("Year",'DollorSpentMil',data=data3,linestyle='dashed',
         linewidth=2,c='black')
l = ax.legend(["Before SSI","After SSI","Before Fatal","After Fatal"]);l.set_title('')

ax2.set(ylabel="HSIP Spending (million $)")
if (not os.path.isdir('./StateWidePlots')): os.makedirs('./StateWidePlots')
fig.savefig('./StateWidePlots/Crashes_by_Funding.png',bbox_inches="tight")
#Plot 2
##############################################################################
def GroupData(data1,data1_TotalCrash,CostCrash, Cat):
    data4 = data1.groupby(['Year',Cat])[['DollorSpentMil']].sum().round(1).reset_index()
    data1_tp = data1.groupby(['Year',Cat])[['ChangeInCrashes']].sum().round(1).reset_index()
    AllYearDat = data1.groupby(Cat)[['DollorSpentMil','ChangeInCrashes']].sum().round(1).reset_index()
    AllYearDat.loc[:,'Year'] = "All Years"
    data1_tp = pd.concat([data1_tp,AllYearDat])
    data5 = data1_tp.pivot(Cat,'Year','ChangeInCrashes')
    data6 = data4.pivot(Cat,'Year','DollorSpentMil')
    ######################
    data7 = data1_TotalCrash.groupby(['Year',Cat])[['ChangeInFatal','ChangeInSSI','CostPerRow']].sum().round(2).reset_index()
    data7= data7.merge(CostCrash,on="Year")    
    tp =pd.DataFrame(data7[['ChangeInFatal','ChangeInSSI']].values* data7[['CostFatal','CostSSI']].values,columns=["CostFatal_T","CostSSI_T"],index=data7.index)
    tp.loc[:,'FatalSSI_CostT'] = tp.CostFatal_T +tp.CostSSI_T
    data7.loc[:,'FatalSSI_CostT'] = tp.FatalSSI_CostT
    data7.drop(columns=['ChangeInFatal','ChangeInSSI'],inplace=True)
    AllYearDat_1= data7.groupby(Cat).sum().reset_index()
    AllYearDat_1.loc[:,'Year'] = "All Years"
    data7 = pd.concat([data7,AllYearDat_1])
    data7.loc[:,'BC'] = (data7.FatalSSI_CostT /data7.CostPerRow).round(1)
    data7 = data7.pivot(Cat,'Year','BC')
    return({"ChngCrash":data5,"Funds":data6,"BC":data7})


def HeatmapByCat(data,cbarLab,Ylab,SaveFig,dim=[10,10],cbar_ticks="",color_map=""):
    sns.set(font_scale=1.4,style="ticks")
    dims = (10,14)
    fig3, ax3 = plt.subplots(figsize=dims)
    if cbar_ticks=="":
        sns.heatmap(data, ax=ax3,cmap='viridis_r',annot=True,linewidth=.5,fmt='g',
                  cbar_kws={'label':cbarLab , 'orientation': 'horizontal'} ,xticklabels=1,yticklabels=1)
    else:
        # colPalette= sns.xkcd_palette(["blush"])*int(3/0.5)+[sns.color_palette('viridis_r')[0]]*2 +[sns.color_palette('viridis_r')[1]]*int(3/0.5)+\
        # [sns.color_palette('viridis_r')[2]]*int(3/0.5)+[sns.color_palette('viridis_r')[3]]*int(3/0.5)+[sns.color_palette('viridis_r')[4]]*int(3/0.5)
        Colors= Temps_7_r.hex_colors
        colPalette =[Colors[1]]*int(3/0.5)+[Colors[3]]*2+[Colors[4]]*int(3/0.5)+[Colors[5]]*int(3/0.5)+[Colors[6]]*int(3/0.5)
        # sns.heatmap(data, ax=ax3,cmap=color_map,annot=True,linewidth=.5,fmt='g',
        #           cbar_kws={'label':cbarLab , 'orientation': 'horizontal'},vmin=0,vmax=1)       
         
        sns.heatmap(data, ax=ax3,cmap=colPalette,annot=True,linewidth=.5,fmt='g',
                  cbar_kws={'label':cbarLab , 'orientation': 'horizontal','ticks':cbar_ticks,'format':'%d'},vmin=-3,vmax=10)   
        cbar = ax3.collections[0].colorbar
        cbar.set_ticks([0,1,4,7])
        cbar.set_ticklabels(["< 0", 1,4,"> 7"])
    ax3.set(ylabel =Ylab )
    plt.yticks(rotation=45)
    plt.xticks(rotation=90)
    fig3.savefig(SaveFig,bbox_inches="tight")
    plt.close()
    return(fig3)

def HeatmapByCat2(data,cbarLab,Ylab,SaveFig,dim=[10,10],cbar_ticks=""):
    fig=None
    x = list(data.columns)
    y = list(data.index.values)
    if cbar_ticks=="":
        fig = ff.create_annotated_heatmap(data.values,x=x,y=y,colorscale='viridis_r',showscale=True)
    else:
        colPalette =[px.colors.sequential.Reds[0]] +px.colors.sequential.Viridis_r
        fig = ff.create_annotated_heatmap(data.values,x=x,y=y,colorscale=colPalette,\
                                          showscale=True,zmin=-10,zmax=70,colorbar = {'tickvals':cbar_ticks})   
    #the magic happens here
    layout = plotly.graph_objs.Layout(xaxis={'type': 'category'})
    fig.update_layout(layout,title =cbarLab)
    return(fig)

data1_TotalCrash.columns

import plotly
check = data1.groupby(['Year','ImpCat'])[['Fatal_Before','Fatal_After',"SSI_Before",'SSI_After']].sum().round(2).reset_index()


"" if(os.path.isdir("./StateWidePlots")) else os.makedirs("./StateWidePlots") 
"" if(os.path.isdir("./StateWidePlots/BC")) else os.makedirs("./StateWidePlots/BC") 
"" if(os.path.isdir("./StateWidePlots/AuxPlot")) else os.makedirs("./StateWidePlots/AuxPlot") 


# data1_TotalCrash.District = data1_TotalCrash.District.cat.remove_categories('NoData')
data1_TotalCrash.District.isna().sum()
data1.District.isna().sum()
da_Tot = data1_TotalCrash.copy()
da_temp = data1.copy()
da_Tot = da_Tot[~da_Tot.District.isna()]
da_temp = da_temp[~da_temp.District.isna()]

# def NonLinCdict(steps, hexcol_array):
#     cdict = {'red': (), 'green': (), 'blue': ()}
#     for s, hexcol in zip(steps, hexcol_array):
#         rgb =matplotlib.colors.hex2color(hexcol)
#         cdict['red'] = cdict['red'] + ((s, rgb[0], rgb[0]),)
#         cdict['green'] = cdict['green'] + ((s, rgb[1], rgb[1]),)
#         cdict['blue'] = cdict['blue'] + ((s, rgb[2], rgb[2]),)
#     return cdict

# rgb_col = sns.xkcd_palette(["pale red"])+sns.xkcd_palette(["yellow"]) +sns.color_palette('viridis_r')[0:4]
# maxVal = TempDict['BC'].values.max()
# minVal= TempDict['BC'].values.min()
# th = ([minVal,0,1,1.5,5,maxVal] - minVal)/(maxVal-minVal)
# cdict = NonLinCdict(th, rgb_col)
# cm = LinearSegmentedColormap('test', cdict)

for Cat1 in ['District','PLANNING_P','ImpCat','EmphArea','Urban-Rural','Roadway Type','Cost Distribution','Method for Site Selection']:
    if Cat1=="District":
        TempDict= GroupData(da_temp,da_Tot,CostCrash, Cat=Cat1)
    else:
        TempDict= GroupData(data1,data1_TotalCrash,CostCrash, Cat=Cat1)
    fig1 = HeatmapByCat(TempDict['ChngCrash'],
                 cbarLab='Before-After Fatal + Suspected Serious Injury\n Crashes (per year)',
                 Ylab =Cat1,
                 SaveFig=f'./StateWidePlots/AuxPlot/{Cat1}_CrashHeatMap.png')
    fig2 = HeatmapByCat(TempDict['Funds'],
                 cbarLab='HSIP Spending (million $)',
                 Ylab =Cat1,
                 SaveFig=f'./StateWidePlots/AuxPlot/{Cat1}_FundingHeatMap.png')
    fig3 = HeatmapByCat(data=TempDict['BC'],
                 cbarLab='Benefit Cost Ratio',
                 Ylab =Cat1,
                 SaveFig=f'./StateWidePlots/BC/{Cat1}_BenefitCosts.png',
                 cbar_ticks= [-3,0,1,4,7,10])


data1.Year
TempDict= GroupData(data1,data1_TotalCrash,CostCrash, Cat=Cat1)
fig1 = HeatmapByCat2(TempDict['ChngCrash'],
             cbarLab='Before-After Fatal + Suspected Serious Injury\n Crashes (per year)',
             Ylab =Cat1,
             SaveFig=f'./StateWidePlots/AuxPlot/{Cat1}_CrashHeatMap.png')
fig2 = HeatmapByCat2(TempDict['Funds'],
             cbarLab='HSIP Spending (million $)',
             Ylab =Cat1,
             SaveFig=f'./StateWidePlots/AuxPlot/{Cat1}_FundingHeatMap.png')
fig3 = HeatmapByCat2(TempDict['BC'],
             cbarLab='Benefit Cost Ratio',
             Ylab =Cat1,
             SaveFig=f'./StateWidePlots/BC/{Cat1}_BenefitCosts.png',
             cbar_ticks= [-10,0,1,1.5,5,10,20,30,40,50,60])
data = TempDict['Funds']



figs =[fig1,fig2,fig3]
def figures_to_html(figs, filename="dashboard.html"):
    dashboard = open(filename, 'w')
    dashboard.write("<html><head></head><body>" + "\n")
    for fig in figs:
        inner_html = fig.to_html().split('<body>')[1].split('</body>')[0]
        dashboard.write(inner_html)
    dashboard.write("</body></html>" + "\n")

figures_to_html([fig1, fig2, fig3])


def BC_Calc(data,CostCrash):
        data= data.merge(CostCrash,on="Year")    
        tp =pd.DataFrame(data[['ChangeInFatal','ChangeInSSI']].values* data[['CostFatal','CostSSI']].values,
                         columns=["CostFatal_T","CostSSI_T"],index=data.index)
        tp.loc[:,'FatalSSI_CostT'] = tp.CostFatal_T +tp.CostSSI_T
        data.loc[:,'FatalSSI_CostT'] = tp.FatalSSI_CostT
        data.loc[:,'BC'] = (data.FatalSSI_CostT /data.CostPerRow).round(2)
        return(data)
 
def GroupCat3(da,CostCrash,Cat="EmphArea",CatValue = "Lane Departures"):
    da1 = da.copy()
    da1 = da1[da1[Cat]==CatValue]
    da1 = da1[['HSIP_ProjID','Year','AnalysisPeriod','FatalSSI_Before','FatalSSI_After','ImpType','ImpCat','EmphArea','AADT','Urban-Rural','Roadway Type','Cost Distribution','Method for Site Selection',
                                                               'ChangeInFatal','ChangeInSSI','CostPerRow',"District","PLANNING_P"]]
    da1.AADT.describe()
    da1.loc[:,'AADT_Lab'] = pd.cut(da1.AADT,[-10000,0,10000,20000,30000,120000],
                labels=['NoData','(0,10000]',"(10000,20000]","(20000,30000]","(30000,120000]"])
    da1 = BC_Calc(da1,CostCrash)
    da1.loc[:,"DollorSpent1000"] = (da1.CostPerRow/ 10**3).round(3)
    da1.loc[:,"DollarSpent1000_Bin"] = pd.cut(da1.loc[:,"DollorSpent1000"], [0,100,500,1000,2000,3000,4000,10000],
                labels=["(0,100]","(100,500]","(500,1000]","(1000,2000]","(2000,3000]","(3000,4000]",">4000"])
    da1.loc[:,'ChangeInFatalSSI'] = (da1.ChangeInFatal+ \
    da1.ChangeInSSI).div(da1.AnalysisPeriod,axis=0)
    da1[["ChangeInSSI","ChangeInFatal",'FatalSSI_Before','FatalSSI_After']]= da1[["ChangeInSSI","ChangeInFatal",
   'FatalSSI_Before','FatalSSI_After']].div(da1.AnalysisPeriod,axis=0)
    da1[["ChangeInSSI","ChangeInFatal",'FatalSSI_Before','FatalSSI_After','ChangeInFatalSSI']] = \
    da1[["ChangeInSSI","ChangeInFatal",'FatalSSI_Before','FatalSSI_After','ChangeInFatalSSI']].round(3)
    return(da1)
        
def PlotlyScatter1(Data,Cat,SaveNm="",Yvar="ChangeInFatalSSI",colorSym="ImpType",title1="",hover_data1=[]):
    fig3 = px.scatter(Data, x = "Year", y = Yvar,
                  hover_data = hover_data1
                  , color =colorSym,
                  facet_col =Cat, facet_col_wrap=4,
                  symbol=colorSym
     ,template="plotly_white",title=title1)
    fig3.add_annotation(
        x=0.5,
        y=-0.18,
        text="")
    fig3.update_annotations(dict(
                xref="paper",
                yref="paper",
                showarrow=False,))
    # fig3.update_layout(showlegend=True,xaxis={  'tickangle': 35,
    #                                              "showticklabels":True,
    #                                             'type': 'category'})
    plot(fig3, filename=f"{SaveNm}_{Cat}.html",auto_open=False)
    
data1_TotalCrash.EmphArea.value_counts()
data1_TotalCrash.ImpCat.value_counts()

data1_TotalCrash.Year = data1_TotalCrash.Year.cat.rename_categories({"2004-2007":"2005.5"})

Dat_LaneDepart= GroupCat3(data1_TotalCrash,CostCrash)
Dat_RoadSignTrafficControl= GroupCat3( data1_TotalCrash,CostCrash,"ImpCat",'Roadway signs and traffic control')


dataDict ={"Emph_LaneDepart":Dat_LaneDepart,"ImpCat_RoadSignTrafficControl":Dat_RoadSignTrafficControl}
Cats= ['DollarSpent1000_Bin','EmphArea','ImpCat','AADT_Lab','Urban-Rural','Roadway Type','District','PLANNING_P']

os.getcwd()
for SaveNm, Dat in dataDict.items():
    # Cats =['District']
    "" if(os.path.isdir(f"./StateWidePlots/{SaveNm}")) else os.makedirs(f"./StateWidePlots/{SaveNm}") 
    "" if(os.path.isdir(f"./StateWidePlots/{SaveNm}/BC")) else os.makedirs(f"./StateWidePlots/{SaveNm}/BC") 
    "" if(os.path.isdir(f"./StateWidePlots/{SaveNm}/Crash")) else os.makedirs(f"./StateWidePlots/{SaveNm}/Crash")
    for Cat in Cats:
        if Cat!= "DollarSpent1000_Bin": 
            Dat[Cat].fillna("NoData",inplace=True)
        else:
            Dat[Cat] = Dat[Cat].cat.add_categories("NoData").fillna("NoData")
        Dat.sort_values(by=['Year',Cat],inplace=True)
        PlotlyScatter1(Dat,Cat,SaveNm=f"./StateWidePlots/{SaveNm}/Crash/{SaveNm}",Yvar="ChangeInFatalSSI",title1="Before-After Yearly Crash Rate",
                   hover_data1=     ['HSIP_ProjID','Year','FatalSSI_Before','FatalSSI_After','District','PLANNING_P','AnalysisPeriod','ImpType','ImpCat','EmphArea','AADT','Urban-Rural','Roadway Type','Cost Distribution','Method for Site Selection',
                                                           'ChangeInFatal','ChangeInSSI','DollorSpent1000','BC'])    
        PlotlyScatter1(Dat,Cat,SaveNm=f"./StateWidePlots/{SaveNm}/BC/{SaveNm}",Yvar="BC",title1="Benfit Cost Ratio" ,
                       hover_data1=     ['HSIP_ProjID','Year','FatalSSI_Before','FatalSSI_After','District','PLANNING_P',
                                         'AnalysisPeriod','ImpType','ImpCat','EmphArea','AADT','Urban-Rural','Roadway Type','Cost Distribution',
                                         'Method for Site Selection','ChangeInFatal','ChangeInSSI','DollorSpent1000','BC'])    



def GroupFilter_Cat(da1,CostCrash,filter1="EmphArea",filterVal = "Lane Departures",Cat2="ImpCat",colorSym="ImpType"):
    if Cat2!="District":da1[Cat2].fillna("NoData",inplace=True)
    #da1 = da1[da1[filter1]==filterVal]
    da1 = da1.groupby(['Year',colorSym,Cat2]).agg({"CostPerRow":"sum","AnalysisPeriod":"first","ChangeInFatal":"sum","ChangeInSSI":"sum",'FatalSSI_Before':'sum','FatalSSI_After':'sum'}).reset_index()
    da1.sort_values(by=['Year',colorSym],inplace=True)
    da1 = BC_Calc(da1,CostCrash)
    da1.loc[:,"DollorSpent1000"] = (da1.CostPerRow/ 10**3).round(3)
    da1.loc[:,"DollarSpent1000_Bin"] = pd.cut(da1.loc[:,"DollorSpent1000"], [0,100,500,1000,2000,3000,4000,10000],
                labels=["(0,100]","(100,500]","(500,1000]","(1000,2000]","(2000,3000]","(3000,4000]",">4000"])
    da1.loc[:,'ChangeInFatalSSI'] = (da1.ChangeInFatal+ \
    da1.ChangeInSSI).div(da1.AnalysisPeriod,axis=0)
    da1[["ChangeInSSI","ChangeInFatal",'FatalSSI_Before','FatalSSI_After']]= da1[["ChangeInSSI","ChangeInFatal",
   'FatalSSI_Before','FatalSSI_After']].div(da1.AnalysisPeriod,axis=0)
    da1[["ChangeInSSI","ChangeInFatal",'FatalSSI_Before','FatalSSI_After','ChangeInFatalSSI']] = \
    da1[["ChangeInSSI","ChangeInFatal",'FatalSSI_Before','FatalSSI_After','ChangeInFatalSSI']].round(3)
    return(da1)


Cats2= ['EmphArea','ImpCat','Urban-Rural','Roadway Type','District','PLANNING_P']
zip(['EmphArea','ImpCat'],["Lane Departures",'Roadway signs and traffic control'])

for filter1,filterVal in zip(["Plot1"],["Plot1"]):
    # Cats =['District']
    "" if(os.path.isdir(f"./StateWidePlots/{filterVal}/BC")) else os.makedirs(f"./StateWidePlots/{filterVal}/BC") 
    "" if(os.path.isdir(f"./StateWidePlots/{filterVal}/Crash")) else os.makedirs(f"./StateWidePlots/{filterVal}/Crash")
    for Cat2 in Cats2:
        Dat1 = GroupFilter_Cat( data1_TotalCrash,CostCrash,filter1,filterVal,Cat2)

        Dat1.sort_values(Cat2,inplace=True)
        PlotlyScatter1(Dat1,Cat2,SaveNm=f"./StateWidePlots/{filterVal}/Crash/{filterVal}",Yvar="ChangeInFatalSSI",title1="Before-After Yearly Crash Rate",
                       hover_data1=[Cat2,'Year','DollorSpent1000',"ChangeInFatalSSI","ChangeInSSI","ChangeInFatal",'FatalSSI_Before','FatalSSI_After'])    
        PlotlyScatter1(Dat1,Cat2,SaveNm=f"./StateWidePlots/{filterVal}/BC/{filterVal}",Yvar="BC",title1="Benfit Cost Ratio",
                        hover_data1=[Cat2,'Year','DollorSpent1000',"ChangeInFatalSSI","ChangeInSSI","ChangeInFatal",'FatalSSI_Before','FatalSSI_After'])    


def RunDiffPlots(Var,Cats_):
    for filter1,filterVal in zip([Var],[Var]):
        # Cats =['District']
        "" if(os.path.isdir(f"./StateWidePlots/{filterVal}/BC")) else os.makedirs(f"./StateWidePlots/{filterVal}/BC") 
        "" if(os.path.isdir(f"./StateWidePlots/{filterVal}/Crash")) else os.makedirs(f"./StateWidePlots/{filterVal}/Crash")
        colorSym=Var
        for Cat2 in Cats_:
            Dat1 = GroupFilter_Cat( data1_TotalCrash,CostCrash,filter1,filterVal,Cat2,colorSym=colorSym)
            Dat1.sort_values(Cat2,inplace=True)
            PlotlyScatter1(Dat1,Cat2,SaveNm=f"./StateWidePlots/{filterVal}/Crash/{filterVal}",Yvar="ChangeInFatalSSI",colorSym=colorSym,title1="Before-After Yearly Crash Rate",
                           hover_data1=[Cat2,'Year','DollorSpent1000',"ChangeInFatalSSI","ChangeInSSI",
                                        "ChangeInFatal",'FatalSSI_Before','FatalSSI_After','BC'])    
            PlotlyScatter1(Dat1,Cat2,SaveNm=f"./StateWidePlots/{filterVal}/BC/{filterVal}",Yvar="BC",colorSym=colorSym,title1="Benfit Cost Ratio",
                            hover_data1=[Cat2,'Year','DollorSpent1000',"ChangeInFatalSSI","ChangeInSSI","ChangeInFatal",'FatalSSI_Before','FatalSSI_After','BC'])    
Var = 'EmphArea'



Cats2= ['District','PLANNING_P','EmphArea','ImpCat','Urban-Rural','Roadway Type']
for Var in Cats2:
    Cats_ = Cats2.copy()
    Cats_.remove(Var)
    if Var!= "District": Cats_.remove('District')
    RunDiffPlots(Var, Cats_)