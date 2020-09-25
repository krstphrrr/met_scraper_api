# from utils.arcnah import arcno
# import pandas as pd
# from utils.tools import db
# from projects.dima.tables.bsnepk import bsne_pk
# from projects.dima.tables.lpipk import lpi_pk
# from projects.dima.tables.nopk import no_pk
# from projects.dima.tables.gappk import gap_pk
# from projects.dima.tables.sperichpk import sperich_pk
# from projects.dima.tables.plantdenpk import plantden_pk
# from projects.dima.tables.qualpk import qual_pk
#
# switcher = {
#     'tblPlots':no_pk,
#     'tblLines':no_pk,
#     'tblLPIDetail':lpi_pk ,
#     'tblLPIHeader':lpi_pk ,
#     'tblGapDetail':gap_pk ,
#     'tblGapHeader':gap_pk ,
#     'tblQualHeader':qual_pk ,
#     'tblQualDetail':qual_pk ,
#     'tblSoilStabHeader':no_pk ,
#     'tblSoilStabDetail':no_pk ,
#     'tblSoilPitHorizons':no_pk ,
#     'tblSoilPits':no_pk ,
#     'tblSpecRichHeader':sperich_pk ,
#     'tblSpecRichDetail':sperich_pk ,
#     'tblPlantProdHeader':no_pk,
#     'tblPlantProdDetail':no_pk,
#     'tblPlotNotes':no_pk,
#     'tblPlantDenHeader':plantden_pk ,
#     'tblPlantDenDetail':plantden_pk ,
#     'tblSpecies':no_pk,
#     'tblSpeciesGeneric':no_pk,
#     'tblSites':no_pk,
#     'tblBSNE_Box':bsne_pk ,
#     'tblBSNE_BoxCollection':bsne_pk ,
#     'tblBSNE_Stack':bsne_pk ,
#     'tblBSNE_TrapCollection':bsne_pk
# }
# tableswitch ={
#     'tblPlots':"no pk",
#     'tblLines':"no pk",
#     'tblLPIDetail':"RecKey",
#     'tblLPIHeader':"LineKey",
#     'tblGapDetail':"RecKey",
#     'tblGapHeader':"LineKey",
#     'tblQualHeader':"PlotKey",
#     'tblQualDetail':"RecKey",
#     'tblSoilStabHeader':"PlotKey",
#     'tblSoilStabDetail':"RecKey",
#     'tblSoilPitHorizons':"no pk",
#     'tblSoilPits':"no pk",
#     'tblSpecRichHeader':"LineKey",
#     'tblSpecRichDetail':"RecKey",
#     'tblPlantProdHeader':"PlotKey",
#     'tblPlantProdDetail':"RecKey",
#     'tblPlotNotes':"no pk",
#     'tblPlantDenHeader':"LineKey",
#     'tblPlantDenDetail':"RecKey",
#     'tblSpecies':"no pk",
#     'tblSpeciesGeneric':"no pk",
#     'tblSites':"no pk",
#     'tblBSNE_Box':"BoxID",
#     'tblBSNE_BoxCollection':"BoxID",
#     'tblBSNE_Stack':"PlotKey",
#     'tblBSNE_TrapCollection':"StackID"
# }
