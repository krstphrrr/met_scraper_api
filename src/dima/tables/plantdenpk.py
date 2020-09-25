from utils.arcnah import arcno
import pandas as pd
from projects.dima.tabletools import fix_fields

def plantden_pk(dimapath):
    """
    returns a dataframe with tblplots, tbllines, tblplantdenheader and tblplantdenDetail
    joined. PrimaryKey field is made using formdate and plotkey

    """
    arc = arcno()
    pla_header = arcno.MakeTableView('tblPlantDenHeader', dimapath)
    pla_detail = arcno.MakeTableView('tblPlantDenDetail', dimapath)
    lines = arcno.MakeTableView('tblLines', dimapath)
    plots = arcno.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = pd.merge(plots,lines, how="inner" ,on='PlotKey')
    plahead_detail = pd.merge(pla_header,pla_detail, how="inner" ,on='RecKey')

    plot_line_det = pd.merge(plot_line, plahead_detail,how="inner", on='LineKey')

    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk
