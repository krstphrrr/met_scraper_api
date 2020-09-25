from utils.arcnah import arcno
import pandas as pd
from projects.dima.tabletools import fix_fields

def qual_pk(dimapath):
    """
    returns a dataframe with tblplots, tbllines, tblgapheader and tblgapDetail
    joined. PrimaryKey field is made using formdate and plotkey
    no dup fields

    """
    arc = arcno()
    header = arcno.MakeTableView('tblQualHeader', dimapath)
    detail = arcno.MakeTableView('tblQualDetail', dimapath)
    # joins
    head_detail = pd.merge(header,detail, how="inner", on="RecKey")

    plot_pk = arc.CalculateField(head_detail, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk
