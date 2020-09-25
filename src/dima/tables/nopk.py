from utils.arcnah import arcno
import pandas as pd
import numpy as np
from projects.dima.tables.lpipk import lpi_pk
from projects.dima.tables.bsnepk import bsne_pk

def no_pk(tablefam:str=None,dimapath:str=None,tablename:str= None):
    """

    """
    arc = arcno()
    fam = {
        'plantprod':['tblPlantProdDetail','tblPlantProdHeader'],
        'soilstab':['tblSoilStabDetail','tblSoilStabHeader'],
        'soilpit':['tblSoilPits', 'tblSoilPitHorizons']
        }
    try:
        if tablefam is not None and ('plantprod' in tablefam):

            header = arcno.MakeTableView(fam['plantprod'][1],dimapath)
            detail = arcno.MakeTableView(fam['plantprod'][0],dimapath)
            head_det = pd.merge(header,detail,how="inner", on="RecKey")
            head_det = arc.CalculateField(head_det,"PrimaryKey","PlotKey","FormDate")
            return head_det


        elif tablefam is not None and ('soilstab' in tablefam):
            header = arcno.MakeTableView(fam['soilstab'][1],dimapath)
            detail = arcno.MakeTableView(fam['soilstab'][0],dimapath)
            head_det = pd.merge(header,detail,how="inner", on="RecKey")
            head_det = arc.CalculateField(head_det,"PrimaryKey","PlotKey","FormDate")
            return head_det

        elif tablefam is not None and ('soilpit' in tablefam):
            print("soilpit")
            pits = arcno.MakeTableView(fam['soilpit'][0], dimapath)
            horizons = arcno.MakeTableView(fam['soilpit'][1], dimapath)
            merge = pd.merge(pits, horizons, how="inner", on="SoilKey")

            allpks = lpi_pk(dimapath)
            iso = allpks.loc[:,["PrimaryKey", "EstablishDate", "FormDate", "DateModified"]].copy()
            merge['FormDate2'] = pd.to_datetime(merge.DateRecorded.apply(lambda x: pd.Timestamp(x).date()))

            mergepk = date_column_chooser(merge,iso) if "tetonAIM" not in dimapath else pd.merge(merge, iso, how="left", left_on="FormDate2", right_on="FormDate").drop_duplicates('HorizonKey')
            if "DateModified2" in mergepk.columns:
                mergepk.drop(['EstablishDate',"FormDate",'FormDate2',"DateModified2"], axis=1, inplace=True)
            else:
                mergepk.drop(['EstablishDate',"FormDate",'FormDate2'], axis=1, inplace=True)

            return mergepk

        else:
            no_pk_df = arcno.MakeTableView(tablename, dimapath)
            # print('netdima in path')
            if ('Network_DIMAs' in dimapath) and (tablefam==None):
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    print("lines,plots; networkdima in the path")
                    fulldf = bsne_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["LineKey","PrimaryKey"]) if "tblLines" in tablename else pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["PrimaryKey"])
                    return no_pk_df
                else:
                    print("network, but not line or plot, no pk")
                    if 'Sites' in tablename:
                        no_pk_df = no_pk_df[(no_pk_df.SiteKey!='888888888') & (no_pk_df.SiteKey!='999999999')]
                        return no_pk_df
                    else:
                        return no_pk_df

            elif ('Network_DIMAs' in dimapath) and ('fake' in tablefam):
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()

                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["LineKey","PrimaryKey"]) if "tblLines" in tablename else pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["PrimaryKey"])
                    return no_pk_df
                else:
                    print("network, but not line or plot, no pk --fakebranch")
                    if 'Sites' in tablename:
                        no_pk_df = no_pk_df[(no_pk_df.SiteKey!='888888888') & (no_pk_df.SiteKey!='999999999')]
                        return no_pk_df
                    else:
                        return no_pk_df

            else:
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    print('not network, no tablefam')
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["LineKey","PrimaryKey"]) if "tblLines" in tablename else pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["PrimaryKey"])
                    return no_pk_df
                else:
                    print("not network, not line or plot, no pk")
                    if 'Sites' in tablename:
                        no_pk_df = no_pk_df[(no_pk_df.SiteKey!='888888888') & (no_pk_df.SiteKey!='999999999')]
                        return no_pk_df
                    else:
                        return no_pk_df
            # return no_pk_df
    except Exception as e:
        print(e)

def date_column_chooser(df,iso):
    df_establish = pd.merge(df, iso, how="left", left_on="FormDate2", right_on="EstablishDate").drop_duplicates('HorizonKey')
    df_formdate = pd.merge(df, iso, how="left", left_on="FormDate2", right_on="FormDate").drop_duplicates('HorizonKey')
    if np.nan not in [i for i in df_formdate.PrimaryKey]:
        return df_formdate
    elif np.nan not in [i for i in df_establish.PrimaryKey]:
        return df_establish
    else:
        iso["DateModified2"] = pd.to_datetime(iso.DateModified.apply(lambda x: pd.Timestamp(x).date()))
        df_datemod = pd.merge(df, iso, how="left", left_on="FormDate2", right_on="DateModified2").drop_duplicates('HorizonKey')
        return df_datemod
