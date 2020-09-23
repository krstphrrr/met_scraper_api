from utils.arcnah import arcno
import pandas as pd
from projects.dima.tabletools import fix_fields

def bsne_pk(dimapath):
    """
    returns a dataframe with tblplots, tblBSNE_Box, tblBSNE_Stack and
    tblBSNE_BoxCollection and tblBSNE_TrapCollection joined. if tblBSNE_TrapCollection
    does not exist in networkdima, skip it and join: box, stack and boxcollection.
    if it exists, join trapcollection and stack.

    PrimaryKey field is made using formdate and plotkey

    """
    ddt = arcno.MakeTableView("tblBSNE_TrapCollection",dimapath)
    arc = arcno()
    if ddt.shape[0]>0:
        ddt = arcno.MakeTableView("tblBSNE_TrapCollection",dimapath)

        stack = arc.MakeTableView("tblBSNE_Stack", dimapath)

        # df = arc.AddJoin(stack, ddt, "StackID", "StackID")
        df = pd.merge(stack,ddt, how="inner", on="StackID")
        df2 = arc.CalculateField(df,"PrimaryKey","PlotKey","collectDate")
        df2tmp = fix_fields(df2,"Notes")
        df2tmp2 = fix_fields(df2tmp,"DateModified")
        df2tmp3 = fix_fields(df2tmp2,"DateEstablished")
        return df2tmp3
    else:

        box = arcno.MakeTableView("tblBSNE_Box",dimapath)
        stack = arcno.MakeTableView("tblBSNE_Stack", dimapath)
        boxcol = arcno.MakeTableView('tblBSNE_BoxCollection', dimapath)
        # differences 1

        # dfx = pd.merge(stack, box[cols_dif1], left_index=True, right_index=True, how="outer")
        df = pd.merge(box,stack, how="inner", on="StackID")
        df2 = pd.merge(df,boxcol, how="inner", on="BoxID")
        # fix
        df2 = arc.CalculateField(df2,"PrimaryKey","PlotKey","collectDate")
        df2tmp = fix_fields(df2,"Notes")
        df2tmp2 = fix_fields(df2tmp,"DateModified")
        df2tmp3 = fix_fields(df2tmp2,"DateEstablished")

        return df2tmp3
