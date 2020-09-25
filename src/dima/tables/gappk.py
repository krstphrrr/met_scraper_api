from utils.arcnah import arcno
import pandas as pd
from projects.dima.tabletools import fix_fields

def gap_pk(dimapath):
    """
    returns a dataframe with tblplots, tbllines, tblgapheader and tblgapDetail
    joined. PrimaryKey field is made using formdate and plotkey

    """
    arc = arcno()
    gap_header = arcno.MakeTableView('tblGapHeader', dimapath)
    gap_detail = arcno.MakeTableView('tblGapDetail', dimapath)
    lines = arcno.MakeTableView('tblLines', dimapath)
    plots = arcno.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = pd.merge(plots,lines,how="inner", on="PlotKey")
    gaphead_detail = pd.merge(gap_header,gap_detail, how="inner", on="RecKey")
    plot_line_det = pd.merge(plot_line,gaphead_detail,how="inner", on="LineKey")
    # fixing dup fields
    # tmp1 = fix_fields(plot_line_det, 'DateModified')
    # tmp2 = fix_fields(tmp1, 'ElevationType')

    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk
