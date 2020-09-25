import datetime as dt
from psycopg2 import sql
from tqdm import tqdm
from io import StringIO
import psycopg2, re, os, os.path, pandas as pd
from sqlalchemy import *
from sqlalchemy import TEXT, INTEGER, NUMERIC, VARCHAR, DATE
from pandas import read_sql_query
# from geoalchemy2 import Geometry, WKTElement, WKBElement
# from shapely.geometry import Point
# import geopandas as gpd

#local imports
from src.utils.tools import db
# from projects.tall_tables.models.header import dataHeader


"""
TODO:
    - include header-primarykey check before ingesting
        - for rows in new-dataframe, if row not in header stop ingestion and
        store rows.

    - describe at a very high level how data flows from csv to pg for debugging

    - need to implement graceful closing of pg session
    - fix DateLoadedInDb before consulting hardcoded model/schema
        (add field further along?)

    - fix tomcat headers + check if the layer reflects newly ingested data

"""
def bring_PKlist(table):
    """ returns a list of the primary keys of a given table
    """
    con = create_engine(os.environ.get('DBSTR'))
    df = pd.read_sql_query(f'select * from gisdb.public."{table}"',con=con)
    return [i for i in df.PrimaryKey.unique()]

def fix_header_geom(df):
    # find rows that are not currently in (deprecated)
     # = splitPK(df,'dataHeader')
    df2 = df.copy()
    # fix missing geoms
    df2.loc[pd.isna(df.Latitude_NAD83), 'wkb_geometry'] = None
    return df2

def splitPK(dataframe, tablename):
    """ takes a dataframe and tablename and returns dictionary with two object

    the 'df' object returns a dataframe without the conflicting PK's.
    the 'conflicts' object returns a list with the conflicting PK's.
    deprecated
    """
    df = dataframe
    all = bring_PKlist(tablename)

    conflicts = [i for i in df.PrimaryKey if i in all]
    package = {}

    package['df'] = df[~df.PrimaryKey.isin([i for i in df.PrimaryKey if i in all])]
    package['conflicts'] = conflicts
    return package

def filterDF(dataframe, conflictlist):
    return dataframe[~dataframe.PrimaryKey.isin(conflictlist)]

# a = splitPK(m.checked_df, 'dataHeader')
# a['df'][a['df'].SpeciesState=="WY"]

# class model_handler:
#     engine = None
#     initial_dataframe = None
#     checked_df = None
#     typedict = None
#     sqlalchemy_types = None
#     pandas_dtypes = None
#     psycopg2_types = None
#     psycopg2_command = None
#     geo_dataframe = None
#     table_name = None
#     conflict_list = None
#
#     def __init__(self,path, name2dictionary, tablename):
#         geosp = ['geoSpecies', 'geoIndicators']
#         """ needs to match name to model and pull dictionary """
#
#         """ clearing attributes & setting engine """
#         self.engine = create_engine(os.environ.get('DBSTR'))
#         [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
#
#         """ creating type dictionaries """
#         self.typedict = name2dictionary
#         self.sqlalchemy_types = field_parse('sqlalchemy', self.typedict)
#         self.pandas_dtypes = field_parse('pandas', self.typedict)
#         self.psycopg2_types = field_parse('pg', self.typedict)
#         self.psycopg2_command = sql_command(self.psycopg2_types,tablename)
#
#         """ prepping a geodf from path """
#         self.tablename = tablename
#
#         if 'dataHeader' in tablename:
#
#             try:
#                 self.initial_dataframe = pd.read_csv(path,encoding='utf-8', low_memory=False)
#                 self.initial_dataframe["DateLoadedInDb"] = dt.date.today().isoformat()
#                 self.initial_dataframe.drop(['PLOTKEY'], inplace=True, axis=1)
#             except Exception as e:
#                 print(e)
#                 print('Continuing...')
#                 self.initial_dataframe = pd.read_csv(path,encoding='cp1252', low_memory=False)
#                 self.initial_dataframe["DateLoadedInDb"] = dt.date.today().isoformat()
#                 self.initial_dataframe.drop(['PLOTKEY'], inplace=True, axis=1)
#
#             self.geo_dataframe = gpd.GeoDataFrame(
#                 self.initial_dataframe,
#                 crs='epsg:4326',
#                 geometry = [
#                     Point(xy) for xy in zip(self.initial_dataframe.Longitude_NAD83,
#                     self.initial_dataframe.Latitude_NAD83)
#                     ]
#                 )
#             self.geo_dataframe['wkb_geometry'] = self.geo_dataframe['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
#             self.geo_dataframe.drop('geometry', axis=1, inplace=True)
#             checked = self.check(self.geo_dataframe)
#
#             if any(~pd.isna(checked.wkb_geometry)): # originally, they geom wont have null values
#                 # temp = splitPK(checked, 'dataHeader') # split pk with the rows in pg
#                 self.checked_df = fix_header_geom(checked.copy())
#             else:
#                 # temp = splitPK(checked, 'dataHeader')
#                 self.checked_df = checked.copy()
#         else:
#             try:
#
#                 self.initial_dataframe = pd.read_csv(path,encoding='utf-8', low_memory=False) if 'geoSpecies' not in tablename else pd.read_csv(path, low_memory=False, encoding='utf-8', index_col=False, usecols=[i for i in range(0,19)])
#                 self.initial_dataframe["DateLoadedInDb"] = dt.date.today().isoformat()
#                 # self.initial_dataframe.drop(['PLOTKEY'], inplace=True, axis=1)
#             except Exception as e:
#                 print(e)
#                 print('Continuing..')
#                 self.initial_dataframe = pd.read_csv(path,encoding='cp1252', low_memory=False) if 'geoSpecies' not in tablename else pd.read_csv(path, low_memory=False, encoding='cp1252', index_col=False, usecols=[i for i in range(0,19)])
#                 self.initial_dataframe["DateLoadedInDb"] = dt.date.today().isoformat()
#                 # self.initial_dataframe.drop(['PLOTKEY'], inplace=True, axis=1)
#             checked = self.check(self.initial_dataframe)
#             self.checked_df = checked.copy()
#
#
#
#
#     def clear(self,var):
#         var = None
#         return var
#
#
#     def check(self, df):
#         """ fieldtype check """
#         for i in df.columns:
#             # print(i)
#             if df[i].dtype!=self.pandas_dtypes[i]:
#                 df[i] = self.typecast(df=df,field=i,fieldtype=self.pandas_dtypes[i])
#
#         return df
#
#     def typecast(self,df,field,fieldtype):
#         data = df
#         datetypes = ["DateModified","FormDate","DateLoadedInDb","created_date","last_edited_date"]
#         castfield = data[field].astype(fieldtype) if field not in datetypes else pd.to_datetime(data[f"{field}"], errors='coerce')
#         return castfield
#
#
#     def send_to_pg(self,df):
#
#         # self.initial_dataframe.to_sql(self.checked_df, self.engine, index=False, dtype=self.sqlalchemy_types)
#         def chunker(seq, size):
#             # from http://stackoverflow.com/a/434328
#             return (seq[pos:pos + size] for pos in range(0, len(seq), size))
#
#         chunksize = int(len(df) / 10) # 10%
#         with tqdm(total=len(df)) as pbar:
#             for i, cdf in enumerate(chunker(df, chunksize)):
#                 # replace = "replace" if i == 0 else "append"
#                 cdf.to_sql(con=self.engine, name=self.tablename, if_exists="append", index=False, dtype=self.sqlalchemy_types)
#                 pbar.update(chunksize)
#
#     def create_empty_table(self, con):
#         conn = con
#         cur = con.cursor()
#         try:
#             cur.execute(self.psycopg2_command)
#             conn.commit()
#             # cur.execute("selec")
#         except Exception as e:
#             conn = con
#             cur = conn.cursor()
#             print(e)

class ingesterv2:

    con = None
    cur = None
    # data pull on init
    __tablenames = []
    __seen = set()

    def __init__(self, con):
        """ clearing old instances """
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
        self.__tablenames = []
        self.__seen = set()

        """ init connection objects """
        self.con = con
        self.cur = self.con.cursor()
        """ populate properties """
        self.pull_tablenames()
    def clear(self,var):
        var = None
        return var

    def pull_tablenames(self):
        if self.__tablenames is not None:
            if self.con is not None:

                try:
                    self.cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name;""")
                    query_results = self.cur.fetchall()

                    for table in query_results:
                        if table not in self.__seen:
                            self.__seen.add(re.search(r"\(\'(.*?)\'\,\)",
                            str(table)).group(1))
                            self.__tablenames.append(re.search(r"\(\'(.*?)\'\,\)",
                            str(table)).group(1))
                except Exception as e:
                    print(e)
                    self.con = db.str
                    self.cursor = self.con.cursor
            else:
                print("connection object not initialized")

    @staticmethod
    def drop_fk(self, table):
        conn = self.con
        cur = conn.cursor()
        key_str = "{}_PrimaryKey_fkey".format(str(table))
        print('try: dropping keys...')
        try:
            # print(table)
            cur.execute(
            sql.SQL("""ALTER TABLE gisdb.public.{0}
                   DROP CONSTRAINT IF EXISTS {1}""").format(
                   sql.Identifier(table),
                   sql.Identifier(key_str))
            )
            conn.commit()
        except Exception as e:
            print(e)
            conn = self.con
            cur = conn.cursor()
        print(f"Foreign keys on {table} dropped")

    def drop_table(self, table):
        conn = self.con
        cur = conn.cursor()
        try:
            cur.execute(
            sql.SQL("DROP TABLE IF EXISTS gisdb.public.{};").format(
            sql.Identifier(table))
            )
            conn.commit()
            print(table +' dropped')
        except Exception as e:
            print(e)
            conn = self.con
            cur = conn.cursor()

    def reestablish_fk(self,table):
        conn = self.con
        cur = conn.cursor()
        key_str = "{}_PrimaryKey_fkey".format(str(table))

        try:

            cur.execute(
            sql.SQL("""ALTER TABLE gisdb.public.{0}
                   ADD CONSTRAINT {1}
                   FOREIGN KEY ("PrimaryKey")
                   REFERENCES "dataHeader"("PrimaryKey");
                   """).format(
                   sql.Identifier(table),
                   sql.Identifier(key_str))
            )
            conn.commit()
        except Exception as e:
            print(e)
            conn = self.con
            cur = conn.cursor()

    @staticmethod
    def main_ingest( df: pd.DataFrame, table:str,
                    connection: psycopg2.extensions.connection,
                    chunk_size:int = 10000):
                """needs a table first"""
                print(connection)
                cursor = connection.cursor()
                df = df.copy()

                escaped = {'\\': '\\\\', '\n': r'\n', '\r': r'\r', '\t': r'\t',}
                for col in df.columns:
                    if df.dtypes[col] == 'object':
                        for v, e in escaped.items():
                            df[col] = df[col].apply(lambda x: x.replace(v, '') if (x is not None) and (isinstance(x,str)) else x)
                try:
                    for i in tqdm(range(0, df.shape[0], chunk_size)):
                        f = StringIO()
                        chunk = df.iloc[i:(i + chunk_size)]

                        chunk.to_csv(f, index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
                        f.seek(0)
                        cursor.copy_from(f, f'"{table}"', columns=[f'"{i}"' for i in df.columns])
                        connection.commit()
                except psycopg2.Error as e:
                    print(e)
                    connection.rollback()
                cursor.close()

    @staticmethod
    def composite_pk(self,*field,con,maintable):
        """ Creates composite primary keys in postgres for a given table
        """
        conn = con
        cur = conn.cursor()
        key_str = "{}_PrimaryKey_fkey".format(str(maintable))
        fields = [f'"{i}"' for i in field]
        fields_str = ', '.join(fields)
        fields_str2 = f'{fields_str}'


        try:

            cur.execute(
            sql.SQL("""ALTER TABLE gisdb.public.{0}
                   ADD CONSTRAINT {1}
                   PRIMARY KEY ({2})
                   """).format(
                   sql.Identifier(maintable),
                   sql.Identifier(key_str),
                   sql.Identifier(fields_str2))
            )

            conn.commit()
        except Exception as e:
            print(e)
            conn = con
            cur = conn.cursor()

    @staticmethod
    def drop_rows(self, con, maintable, field, result):
        """ removing rows that fit a specific value from a given table

        - need to implement graceful closing of pg session
        """
        conn = con
        cur = conn.cursor()
        try:

            cur.execute(
            sql.SQL("""DELETE from gisdb.public.{0}
                  WHERE {1} = '%s'
                   """ % result).format(
                   sql.Identifier(maintable),
                   sql.Identifier(field))
            )

            conn.commit()
        except Exception as e:
            print(e)
            conn = con
            cur = conn.cursor()


# def protocol_typecast( protocol_choice : str, type : str):
#     # customtype = None
#     customsize = None
#     if 'v_' in type:
#         # customtype = 'varchar'
#         customsize = int(type.split('_')[1])
#
#
#
#     """ dictionary with kv pairs of type-protocol for each field type"""
#     text={
#         'sqlalchemy':TEXT(),
#         'pandas':"object",
#         'pg': "TEXT",
#         "custom" : VARCHAR(customsize),
#         "custompg": f'VARCHAR({customsize})'
#     }
#     float = {
#         'sqlalchemy' : NUMERIC(),
#         'pandas' : 'float64',
#         'pg' : "NUMERIC"
#     }
#     integer = {
#         'sqlalchemy' : INTEGER(),
#         'pandas' : 'Int64',
#         'pg' : 'INTEGER'
#     }
#     date = {
#         'sqlalchemy' : DATE(),
#         'pandas' : 'datetime64[ns]',
#         'pg' : 'DATE'
#     }
#     geom = {
#         'sqlalchemy' : Geometry('POINT', srid=4326),
#         'pandas' : 'object',
#         'pg' : 'GEOMETRY(POINT, 4326)'
#     }
#
#
#     """ executed pattern will depend on function parameters """
#
#     if 'sqlalchemy' in protocol_choice:
#         if 'text' in type:
#             return text['sqlalchemy']
#         elif 'float' in type:
#             return float['sqlalchemy']
#         elif 'integer' in type:
#             return integer['sqlalchemy']
#         elif 'date' in type:
#             return date['sqlalchemy']
#         elif 'v_' in type:
#             return text['custom']
#         elif 'geom' in type:
#             return geom['sqlalchemy']
#         else:
#             print('type not yet implemented')
#
#     elif 'pandas' in protocol_choice:
#         if 'text' in type:
#             return text['pandas']
#         elif 'float' in type:
#             return float['pandas']
#         elif 'integer' in type:
#             return integer['pandas']
#         elif 'date' in type:
#             return date['pandas']
#         elif 'v_' in type:
#             return text['pandas']
#         elif 'geom' in type:
#             return geom['pandas']
#         else:
#             print('type not yet implemented')
#
#     elif 'pg' in protocol_choice:
#         if 'text' in type:
#             return text['pg']
#         elif 'float' in type:
#             return float['pg']
#         elif 'integer' in type:
#             return integer['pg']
#         elif 'date' in type:
#             return date['pg']
#         elif 'v_' in type:
#             return text['custompg']
#         elif 'geom' in type:
#             return geom['pg']
#         else:
#             print('type not yet implemented')

def field_parse(prot:str, dictionary:{}):
    """ takes a dictionary with rudimentary field definitions and fieldtype
    protocol, and returns a dictionary with protocol-parsed fields
    it understands:

    - 'text', 'float', 'integer', 'date', and 'v_*NUMBER*' for varchar where
    *NUMBER* is the size of the varchar field,

    """
    return_d = {}

    try:
        if 'sql' in prot:
            protocol = 'sqlalchemy'
            for k,v in dictionary.items():
                return_d.update({k:protocol_typecast(protocol,v)})

        elif 'pandas' in prot:
            protocol = 'pandas'
            for k,v in dictionary.items():
                return_d.update({k:protocol_typecast(protocol,v)})

        elif 'pg' in prot:
            protocol = 'pg'
            for k,v in dictionary.items():
                return_d.update({k: protocol_typecast(protocol,v)})

    except Exception as e:
        print(e)
    finally:
        return return_d

def sql_command(typedict, name):
    inner_list = [f"\"{k}\" {v}" for k,v in typedict.items()]
    part_1 = f""" CREATE TABLE gisdb.public.\"{name}\" ("""
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
