import pandas as pd
from utils.tools import db
from utils.arcnah import arcno
import os
from psycopg2 import sql
import numpy as np

def fix_fields(df : pd.DataFrame, keyword: str, debug=None):
    """ Checks for duplicate fields produced by primarykey joins


    """
    df = df.copy()
    done=False
    while done!=True:
        if len([i for i in df.columns if keyword in i])>=3:
            print(f'0.1, {keyword} field occurs 3 times, dropping both additional iterations') if debug else None
            df.drop([f'{keyword}_y',f'{keyword}_x'], axis=1, inplace=True)
            done=True
            return df
        else:
            if (f'{keyword}_x' in df.columns) or (f'{keyword}_y' in df.columns):
                if df[f'{keyword}_x'].equals(df[f'{keyword}_y']):
                    # if the two notes are the same, keep one of them.
                    print(f'1. {keyword}_x equals y (drops y)') if debug else None
                    df.drop([f'{keyword}_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                    done=True
                    return df


                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and ((None not in df[f'{keyword}_x']) or (None not in df[f'{keyword}_x'])) and (len(df[f'{keyword}_x'].unique())>len(df[f'{keyword}_y'].unique())):
                    # if the two notes are different AND the x is none, keep the y
                    print(f'2. {keyword}_x does not equal y, and \'None\' is not in column x or y, and the length of x.unique is larger than y.unique (deletes y)') if debug else None
                    df.drop([f'{keyword}_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                    done=True
                    return df

                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and ((None not in df[f'{keyword}_x']) or (None not in df[f'{keyword}_x'])) and (len(df[f'{keyword}_x'].unique())<len(df[f'{keyword}_y'].unique())):
                    # if the two notes are different AND the x is none, keep the y
                    print(f'3. {keyword}_x does not equal y, and \'None\' is not in column x or y, and the length of x.unique is smaller than y.unique (deletes x)') if debug else None
                    df.drop([f'{keyword}_x'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_y':f'{keyword}'}, inplace=True)

                    done=True
                    return df

                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])>len([i for i in df[f'{keyword}_y'] if i==None])):
                    # if the two notes are different AND the x is none, keep the y
                    print(f'4. {keyword}_x does not equal y, and the length of Nones in x is larger than the length of Nones in y') if debug else None
                    df.drop([f'{keyword}_x'], axis=1, inplace=True)
                    # df.rename(columns={f'{keyword}_y':f'{keyword}'}, inplace=True)

                    done=True
                    return df

                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])<len([i for i in df[f'{keyword}_y'] if i==None])):
                    # if the two notes are different AND the y is none, keep the x
                    print(f'5. {keyword}_x does not equal y, and the length of Nones in x is smaller than the length of Nones in y') if debug else None
                    df.drop(['Notes_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                    done=True
                    return df

            else:
                return df

def new_tablename(df:pd.DataFrame):
    """
    if dataframe has an 'ItemType' field,
    return a new tablename string depending on what's present in that field.
    """
    if 'ItemType' in df.columns:
        if (any(df.ItemType=='M')) or (any(df.ItemType=='m')):
            newtablename = 'tblHorizontalFlux'
            return newtablename

        elif (any(df.ItemType=='T')) or (any(df.ItemType=='t')):
            newtablename = 'tblDustDeposition'
            return newtablename

def table_create(df: pd.DataFrame, tablename: str, conn:str=None):
    """
    pulls all fields from dataframe and constructs a postgres table schema;
    using that schema, create new table in postgres.
    """

    table_fields = {}

    try:
        for i in df.columns:
            if tablename!='aero_runs':
                table_fields.update({f'{i}':f'{type_translate[df.dtypes[i].name]}'})
            else:
                table_fields.update({f'{i}':f'{aero_translate[df.dtypes[i].name]}'})


        if table_fields:
            comm = sql_command(table_fields, tablename, conn) if conn!='nri' else sql_command(table_fields, tablename, 'nritest')
            d = db(f'{conn}')
            con = d.str
            cur = con.cursor()
            # return comm
            cur.execute(comm)
            con.commit()

    except Exception as e:
        print(e)
        d = db(f'{conn}')
        con = d.str
        cur = con.cursor()

def sql_command(typedict:{}, name:str, db:str=None):
    """
    create a string for a psycopg2 cursor execute command to create a new table.
    it receives a dictionary with fields and fieldtypes, and builds the string
    using them.

    """
    db_choice={
    # dimas in postgres
    "dima":"postgres",
    # met data in gisdb
    "met":"n_dats",
    # tall tables
    "gisdb": "gisdb",
    # nri
    "nri": "nritest"
    }
    schema_choice={
    "dima":"public",
    "met":"public",
    "gisdb":"public",
    "nri":"public"
    }
    inner_list = [f"\"{k}\" {v}" for k,v in typedict.items()]
    part_1 = f""" CREATE TABLE {db_choice[db]}.{schema_choice[db]}.\"{name}\" \
     (""" if db==None else f""" CREATE TABLE {db_choice[db]}.{schema_choice[db]}.\"{name}\" ("""
    try:
        for i,x in enumerate(inner_list):
            if i==len(inner_list)-1:
                part_1+=f"{x}"
            else:
                part_1+=f"{x},"
    except Exception as e:
        print(e)
    finally:
        part_1+=");"
        return part_1

def tablecheck(tablename, conn="dima"):
    """
    receives a tablename and returns true if table exists in postgres table
    schema, else returns false

    """
    try:
        d = db(f'{conn}')
        con = d.str
        cur = con.cursor()
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (f'{tablename}',))
        if cur.fetchone()[0]:
            return True
        else:
            return False

    except Exception as e:
        print(e)
        d = db('conn')
        con = d.str
        cur = con.cursor()

def csv_fieldcheck(df: pd.DataFrame, path: str, table: str):
    checked = 0
    try:
        escaped = {'\\': '\\\\', '\n': r'\n', '\r': r'\r', '\t': r'\t',}
        for col in df.columns:
            if df.dtypes[col] == 'object':
                for v, e in escaped.items():
                    df[col] = df[col].apply(lambda x: x.replace(v, '') if (x is not None) and (isinstance(x,str)) else x)
                    checked = 1
    except Exception as e:
        print(e)

    finally:
        if checked==1:
            df.to_csv(os.path.join(os.path.dirname(path),table.replace('tbl','')+'.csv'))
        else:
            print('fields not fixed; csv export aborted')

def drop_dbkey(table, path):
    squished_path = os.path.split(os.path.splitext(path)[0])[1].replace(" ","")
    d = db('dima')
    try:
        # print(f'"DELETE FROM postgres.public.{table} WHERE \"DBKey\"=\'{squished_path}\';"')
        con = d.str
        cur = con.cursor()
        cur.execute(
            sql.SQL("DELETE FROM postgres.public.{0} WHERE \"DBKey\"= '%s';" % squished_path).format(
                sql.Identifier(table))
        )
        con.commit()
        print(f'successfully dropped \'{squished_path}\' from table \'{table}\'')
    except Exception as e:
        con = d.str
        cur = con.cursor()
        print(e)


def blank_fixer(df):
    for i in df.columns:
        if df[i].dtype=='object':
            df[i].replace('',np.nan,inplace=True)
    return df

def significant_digits_fix_pandas(df):
    colist = ['sedimentGperDay','emptyWeight', 'recordedWeight', 'sedimentWeight']
    for i in df.columns:
        if i in colist:
            df[i] = df[i].apply(lambda x: round(x,4))
    return df


def float_field(df, field):
    temp_series = df[field].astype('float64')
    return temp_series

def openingsize_fixer(df):
    for i in df.columns:
        if 'openingSize' in i:
            df[i] = df[i].astype('float64')
    return df

def datetime_type_assert(df):
    for i in df.columns:
        if df.dtypes[i]=="datetime64[ns]" or "date" in i.lower():
            df[i] = df[i].apply(lambda x: pd.NaT if x is None else x).astype("datetime64[ns]")
    return df

type_translate = {
    'int64':'int',
    'Int64':'int',
    "object":'text',
    'datetime64[ns]':'timestamp',
    'bool':'boolean',
    'float64':'float(5)'
}

aero_translate = {
    'int64':'int',
    'Int64':'int',
    "object":'text',
    'datetime64[ns]':'timestamp',
    'bool':'boolean',
    'float64':'float'
}
