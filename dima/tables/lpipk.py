from utils.arcnah import arcno
import pandas as pd
from projects.dima.tabletools import fix_fields
# path = r"C:\Users\kbonefont\Desktop\DIMA 5.4_BlueMountains_2020-01-06.accdb"
# lpi_pk(path)
def lpi_pk(dimapath):
    """
    returns a dataframe with tblplots, tbllines, tbllpiheader and tblLPIDetail
    joined. PrimaryKey field is made using formdate and plotkey

    """

    lpi_header = arcno.MakeTableView('tblLPIHeader', dimapath)
    lpi_detail = arcno.MakeTableView('tblLPIDetail', dimapath)
    lines = arcno.MakeTableView('tblLines', dimapath)
    plots = arcno.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = pd.merge(plots, lines, how="inner", on="PlotKey")
    lpihead_detail = pd.merge(lpi_header, lpi_detail, how="inner", on="RecKey")
    plot_line_det = pd.merge(plot_line, lpihead_detail, how="inner", on="LineKey")
    arc = arcno()
    #
    # tmp1 = fix_fields(plot_line_det, 'DateModified').copy()
    # tmp2 = fix_fields(tmp1,'ElevationType').copy()
    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")
    return plot_pk
