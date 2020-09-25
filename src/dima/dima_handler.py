from src.utils.arcnah import arcno
import os
from os.path import normpath, split, splitext, join
from src.utils.tools import db, config
from sqlalchemy import create_engine

from datetime import datetime
from psycopg2 import sql
import pandas as pd

from src.dima.handler import switcher, tableswitch
from src.dima.tabletools import fix_fields, new_tablename, table_create, \
tablecheck, csv_fieldcheck, blank_fixer, significant_digits_fix_pandas, \
float_field, openingsize_fixer, datetime_type_assert
from src.tall_tables.talltables_handler import ingesterv2


def main_translate(tablename:str, dimapath:str, debug=None):
    """ Translates between tables and different function argument strategies

    This function will map the tablename to an appropriate tuple of arguments.
    first block of 'a' argument tuple will check if the dima has 'BSNE' tables
    within the DIMA. It could still be a vegetation dima however, that will be
    checked further along on the no_pk function.

    Parameters
    ----------
    tablename : str
        Name of the table in DIMA. example: 'tblLines'

    dimapath : str
        Physical path to DIMA Access file. example: 'c://Folder/dima_file.mdb'

    debug : any character or None (default value: None)
        Prints out how trayectory of the mapping process throughout the function.

    """

    no_primary_key = ['tblPlots', 'tblLines', 'tblSpecies','tblSpeciesGeneric',\
                      'tblSites','tblPlotNotes', 'tblSites']
    soil_stab_primary_key = ['tblSoilStabDetail', 'tblSoilStabHeader']
    soil_pit_primary_key = ['tblSoilPits','tblSoilPitHorizons']
    plant_prod_primary_key = ['tblPlantProdDetail', 'tblPlantProdHeader']
    bsne_primary_keys = ['tblBSNE_Box', 'tblBSNE_Stack','tblBSNE_BoxCollection',\
                         'tblBSNE_TrapCollection']

    switcher_arguments= {
        'no_pk': (None, dimapath, tablename),
        'no_pk_soilstab': ('soilstab',dimapath, None),
        'no_pk_soilpits': ('soilpits',dimapath, None),
        'no_pk_plantprod': ('plantprod',dimapath, None),
        'yes_pk': dimapath,
        'f': ('fake', dimapath, tablename)
        }
    # first check if tablename exists in the particular dima
    if table_check(tablename, dimapath):
        if tablename in no_primary_key:
            # no_pk branch
            network_check = 0
            inst = arcno(dimapath)

            for i,j in inst.actual_list.items():
                if any([True for i,j in inst.actual_list.items() if 'BSNE' in i]):
                    network_check = 2
                else:
                    network_check = 1

            while network_check!=0:

                if network_check==1:
                    print('no_pk; netdima in path; line or plot') if debug else None
                    df = switcher[tablename](*switcher_arguments['f'])
                    network_check=0
                    df = blank_fixer(df)
                    df = significant_digits_fix_pandas(df)
                    return df

                elif network_check==2:
                    print('no_pk; netdima in path; line or plot') if debug else None
                    df = switcher[tablename](*switcher_arguments['no_pk'])
                    network_check=0
                    df = blank_fixer(df)
                    df = significant_digits_fix_pandas(df)
                    return df

        elif tablename in soil_stab_primary_key:
            # no_pk + soilstab branch
            print('no_pk; soilstab') if debug else None
            df = switcher[tablename](*switcher_arguments['no_pk_soilstab'])
            df = blank_fixer(df)
            df = significant_digits_fix_pandas(df)
            return df

        elif tablename in soil_pit_primary_key:
            # no_pk + soilpits branch
            print('no_pk; soilpits') if debug else None
            df = switcher[tablename](*switcher_arguments['no_pk_soilpits'])
            df = blank_fixer(df)
            df = significant_digits_fix_pandas(df)
            return df

        elif tablename in plant_prod_primary_key:
            # no_pk + plantprod branch
            print('no_pk; plantprod') if debug else None
            df = switcher[tablename](*switcher_arguments['no_pk_plantprod'])
            df = blank_fixer(df)
            df = significant_digits_fix_pandas(df)
            return df

        else:
            # lpi_pk, gap_pk, sperich_pk, plantden_pk, bsne_pk branch
            if tablename in bsne_primary_keys:
                print('bsne collection') if debug else None
                retdf = switcher[tablename](switcher_arguments['yes_pk'])
                retdf = blank_fixer(retdf)
                retdf = significant_digits_fix_pandas(retdf)
                retdf = openingsize_fixer(retdf)
                return retdf
            else:
                print('hmmm?') if debug else None
                df = switcher[tablename](switcher_arguments['yes_pk'])
                arc = arcno()
                iso = arc.isolateFields(df,tableswitch[tablename],"PrimaryKey").copy()
                iso.drop_duplicates([tableswitch[tablename]],inplace=True)

                target_table = arcno.MakeTableView(tablename, dimapath)
                retdf = pd.merge(target_table, iso, how="inner", on=tableswitch[tablename])
                retdf = blank_fixer(retdf)
                retdf = significant_digits_fix_pandas(retdf)
                retdf = openingsize_fixer(retdf)
                return retdf
    else:

        print(f'table not in {os.path.basename(dimapath)}')
        pass


def pg_send(table:str, path:str, csv=None, debug=None):
    plot = None
    """ Sends dataframe to postgres or prints out CSV.

    Given a Dima path and tablename, uses the function main_translate to create
    a dataframe and either send it to a postgres database, or print it out to
    a CSV in the same directory where the DIMA file is in.

    Parameters
    ----------

    table : str
        Name of the table in DIMA. example: 'tblLines'

    path : str
        Physical path to DIMA Access file. example: 'c://Folder/dima_file.mdb'

    csv : None or any character. (default value: None)
        If not None, it will print out the table dataframe to CSV.

    debug : None or any character. (default value: None)
        If not None, it will print out each of the steps the function's processes
        take.
    """
    d = db('dima')
    df = main_translate(table,path)
    print("STARTING INGEST")
    df['DateLoadedInDB'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    # dbkey add calibration HERE
    if ('calibration' in path) or ('Calibration' in path):
            df['DBKey'] = os.path.join(split(splitext(path)[0])[1].replace(" ",""),'calibration')
    else:
        # creates non-calibration DBKey here
        df['DBKey'] = split(splitext(path)[0])[1].replace(" ","")

    if 'ItemType' in df.columns:
        # if one of the non-vegetation bsne tables, use 'new_tablename' ,
        # function to produce a new tablename: 'tblHorizontalFlux' or
        # 'tblDustDeposition'
        newtablename = new_tablename(df)
        if tablecheck(newtablename):
            print('MWACK')
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else csv_fieldcheck(df,path,table)
        else:
            table_create(df, newtablename, 'dima')
            print('llegue a 2')
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else csv_fieldcheck(df,path,table)

    else:
        print("NOT A HORFLUX TABLE")
        newtablename = table
        if tablecheck(table):
            print("FOUND THE TABLE IN PG")
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else csv_fieldcheck(df,path,table)

        else:
            print("DID NOT FIND TABLE IN PG, CREATING...")
            table_create(df, table, 'dima')
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else csv_fieldcheck(df,path,table)


def batch_looper(dimacontainer, pg=False):

    """
    addition
    creates an exhaustive list of tables across all dimas in a folder
    and then uses looper to gothrough the list of tables and create csv's for
    each.
    """
    d = db('dima')
    tablelist = None
    while tablelist is None:
        print('gathering tables within dimas..')
        tablelist = table_collector(dimacontainer)
    else:
        print('creating csvs for each table..')
        for table in tablelist:
            if pg!=True:
                looper(dimacontainer, table, csv=True)
            else:
                df = looper(dimacontainer,table,csv=False)
                if 'ItemType' in df.columns:
                    # if one of the non-vegetation bsne tables, use 'new_tablename' ,
                    # function to produce a new tablename: 'tblHorizontalFlux' or
                    # 'tblDustDeposition'
                    newtablename = new_tablename(df)
                    if tablecheck(newtablename):
                        print('MWACK')
                        ingesterv2.main_ingest(df, newtablename, d.str, 10000)
                    else:
                        table_create(df, newtablename, 'dima')
                        print('llegue a 2')
                        ingesterv2.main_ingest(df, newtablename, d.str, 10000)

                else:
                    print("NOT A HORFLUX TABLE")
                    newtablename = table
                    if tablecheck(table):
                        print("FOUND THE TABLE IN PG")
                        ingesterv2.main_ingest(df, newtablename, d.str, 10000)

                    else:
                        print("DID NOT FIND TABLE IN PG, CREATING...")
                        table_create(df, table, 'dima')
                        ingesterv2.main_ingest(df, newtablename, d.str, 10000)


def looper(path2mdbs, tablename, csv=False):
    """
    goes through all the files(.mdb or .accdb extensions) inside a folder,
    create a dataframe of the chosen table using the 'main_translate' function,
    adds the dataframe into a dictionary,
    finally appends all the dataframes
    and returns the entire appended dataframe
    """
    containing_folder = path2mdbs
    contained_files = os.listdir(containing_folder)
    df_dictionary={}

    count = 1
    basestring = 'file_'
    for i in contained_files:
        if os.path.splitext(os.path.join(containing_folder,i))[1]=='.mdb' or os.path.splitext(os.path.join(containing_folder,i))[1]=='.accdb':
            countup = basestring+str(count)
            # df creation/manipulation starts here
            print(i)
            df = main_translate(tablename,os.path.join(containing_folder,i))
            if df is not None:
                if 'DateLoadedInDB' in df.columns:
                    df['DateLoadedInDB']=df['DateLoadedInDB'].astype('datetime64')
                    df['DateLoadedInDB'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                else:
                    df['DateLoadedInDB'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

                df['DBKey'] = os.path.split(os.path.splitext(i)[0])[1].replace(" ","")
                # df add to dictionary list
                df_dictionary[countup] = df.copy()
            else:
                pass
            count+=1
    final_df = pd.concat([j for i,j in df_dictionary.items()], ignore_index=True).drop_duplicates()

    return final_df if csv==False else final_df.to_csv(os.path.join(containing_folder,tablename+'.csv'))


def table_check(tablename, path):
    """
    returns true if table is present in a particular dima
    uses "arcno.actual_list" to compile a list of tables
    then checks if the chosen table is present in that list
    """
    instance = arcno(path)
    tablelist = [i for i,j in instance.actual_list.items()]
    return True if tablename in tablelist else False

def table_collector(path2mdbs):
    """
    returns a list of all tables present in a folder of dimas
    because dimas may each have a different set of tables, this function
    goes through the list of tables per dima and appends any table not previously
    seen into an internal list which is ultimately returned.
    """
    containing_folder = path2mdbs
    contained_files = os.listdir(containing_folder)
    table_list = []
    for mdb_path in contained_files:
        if os.path.splitext(mdb_path)[1]=='.mdb' or os.path.splitext(mdb_path)[1]=='.accdb':
            instance = arcno(os.path.join(containing_folder,mdb_path))
            for tablename, size in instance.actual_list.items():
                if tablename not in table_list:
                    table_list.append(tablename)
    return table_list


def has_duplicate_pks(df,tablename):
    try:
        con = d.str
        cur = con.cursor()
        exists_query = f'''
        select "PrimaryKey" from
        postgres.public."{tablename}"
        '''
        cur.execute (exists_query)
        df_pg_keys = list(set([i for i in cur.fetchall()]))

        if 'PrimaryKey' in df.columns:
            for i in df.PrimaryKey:
                if i in df_pg_keys:
                    return True
                else:
                    return False
        else:
            print('table has no primary key. aborting check')

    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()

def single_pg_send(df, tablename):
    """
    uses: new_tablename, tablecheck, ingesterv2.main_ingest
    table_create, datetime_type_assert
    """
    d = db('dima')
    print("STARTING INGEST")
    df = blank_fixer(df)
    df = significant_digits_fix_pandas(df)
    df = datetime_type_assert(df)
    df = openingsize_fixer(df) if "openingSize" in df.columns else df
    if 'ItemType' in df.columns:
        newtablename = new_tablename(df)
        if tablecheck(newtablename):
            print(f'network table "{newtablename}" exists, ingesting..')
            ingesterv2.main_ingest(df, newtablename, d.str, 10000)
        else:
            table_create(df, newtablename, 'dima')
            print(f'created network table: {newtablename}')
            ingesterv2.main_ingest(df, newtablename, d.str, 10000)

    else:
        print("not a network table")
        newtablename = table
        if tablecheck(table):
            print("FOUND THE TABLE IN PG")
            ingesterv2.main_ingest(df, newtablename, d.str, 10000)

        else:
            print("DID NOT FIND TABLE IN PG, CREATING...")
            table_create(df, table, 'dima')
            ingesterv2.main_ingest(df, newtablename, d.str, 10000)


















# modcheck+=1
