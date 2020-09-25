from celery.task import task
import requests, csv, os, os.path, pandas as pd
from contextlib import closing
from src.dima.tabletools import tablecheck, table_create
from src.utils.tools import db
from src.tall_tables.talltables_handler import ingesterv2
# help(table_create)
@task()
def test_task():
    print("test ok")
    # check if table exists
    d = db("met")
    if tablecheck("raw_met_data", "met"):
        print("table found")
        # create dataframes
        for i in historic_files.items():
            for j in i[1]:
                # extract
                fullpath = os.path.join(files_path,j)
                projk = projectkey_extractor(fullpath)
                ins = datScraper(fullpath)
                df = ins.df
                df['ProjectKey'] = projk
                date_key = dataframe_range_extract(df)
                # check if datetime range AND project key exists
                if sql_command_daterange(date_key):
                    print(f"MET data for range: {date_key[0]} to {date_key[1]} with projectkey {date_key[2]} already exists; moving on..")
                else:
                    print(f" Ingesting Met data for date range: {date_key[0]} to {date_key[1]} with projectkey {date_key[2]}")
                    ingesterv2.main_ingest(df,"raw_met_data", d.str, 100000)
                    print("ingestion finished. Moving on...")
    else:
        print("table not found")
        # creating new table
        pg_table_template_PATH = os.path.join(files_path,historic_files['akron'][0])
        INSTANCE = datScraper(pg_table_template_PATH)
        pg_table_template_DATAFRAME = INSTANCE.df
        table_create(pg_table_template_DATAFRAME, "raw_met_data", "met")

        for i in historic_files.items():
            for j in i[1]:
                # extract
                fullpath = os.path.join(files_path,j)
                projk = projectkey_extractor(fullpath)
                ins = datScraper(fullpath)
                df = ins.df
                df['ProjectKey'] = projk
                date_key = dataframe_range_extract(df)
                # check if datetime range AND project key exists
                if sql_command_daterange(date_key):
                    print(f"MET data for range: {date_key[0]} to {date_key[1]} with projectkey {date_key[2]} already exists; moving on..")
                else:
                    print(f" Ingesting Met data for date range: {date_key[0]} to {date_key[1]} with projectkey {date_key[2]}")
                    ingesterv2.main_ingest(df,"raw_met_data", d.str, 100000)
                    print("ingestion finished. Moving on...")

        #create table
# test_task()

# p = os.path.join(files_path,historic_files['akron'][0])
# def batch_ingest():
#     for i in historic_files.items():
#         for j in i[1]:
#             print(j)
# batch_ingest()
#



# quick_ingest(historic_files)
def quick_ingest(whichset):

    internal_dict = {}
    selectedset=whichset
    # for current data
    count=1
    if isinstance(whichset['akron'],str):
        for i in selectedset.items():
            if "BellevueTable1" not in i:
                print("processing: ",i)
                fullpath = os.path.join(files_path,i[1])
                projk = projectkey_extractor(fullpath)
                inst = datScraper(fullpath)
                df = inst.df
                df['ProjectKey'] = projk

                internal_dict.update({f"df{count}":df})
                count+=1

        # return internal_dict
        # for historic data
    elif isinstance(whichset['akron'],list):
        for i in selectedset.items():
            for j in i[1]:
                # print(j)
                print("processing: ",j)
                fullpath = os.path.join(files_path,j)
                projk = projectkey_extractor(fullpath)
                inst = datScraper(fullpath)
                df = inst.df
                df['ProjectKey'] = projk

                internal_dict.update({f"df{count}":df})
                count+=1
    # for i in internal_dict.items():
    finaldf = pd.concat([i[1] for i in internal_dict.items()])
    table_create(finaldf, "raw_met_data","met") if tablecheck("raw_met_data", "met")!=True else None
    try:
        d = db("met")
        ingesterv2.main_ingest(finaldf, "raw_met_data", d.str, 100000)
    except Exception as e:
        print(e)
        d = db("met")

# help(ingesterv2.main_ingest)
# tablecheck("raw_met_data", "met")
        # return internal_dict
# help(table_create)
# help(tablecheck)
# for i in

# row_check(df10)
#
# df.iloc[0:1].TIMESTAMP.values[0]
def row_check(df):
    for i in range(len(df)):

        try:
            if pg_check(df.iloc[i:i+1].TIMESTAMP.values[0], df.iloc[i:i+1].ProjectKey.values[0]):
                print("found timestap:",df.iloc[i:i+1])
            else:
                print("did not find timestamp")
        except Exception as e:
            print(e)

# pg_check('2020/01/01 00:01:00', "Akron")
def pg_check(timestamp, projectkey):
    qry=f"""SELECT EXISTS(
                SELECT *
                FROM public.met_data
                WHERE
                    "TIMESTAMP"='{timestamp}'
                and
                    "ProjectKey"='{projectkey}');;;
        """
    try:
        d= db('met')
        con = d.str
        cur = con.cursor()
        cur.execute(qry)
        if cur.fetchone()[0]:
            return True
        else:
            return False
    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()










def projectkey_extractor(path):
    if os.path.splitext(os.path.basename(path))[1]=='.csv':
        return [i.lower() for i in os.path.basename(path).split('_') if ("Met" not in i) and (os.path.splitext(i)[1]=='')][0]
    elif os.path.splitext(os.path.basename(path))[1]=='.dat':
        return [i.lower() for i in os.path.basename(path).split('Table') if (os.path.splitext(i)[1]=='')][0]


# def timestamp_check(dataframe):
#     pass

# d = datScraper(df2)
def pg_timestamp_check():
    pass

def sql_command_daterange(date_key_tuple):
    date1=date_key_tuple[0]
    date2=date_key_tuple[1]
    pk=date_key_tuple[2]
    str=f"""
    SELECT EXISTS(
        SELECT *
        FROM public.raw_met_data
        WHERE (
            "TIMESTAMP" >= '{date1}'::timestamp
            AND
            "TIMESTAMP" < '{date2}'::timestamp
              ) and "ProjectKey"='{pk}'
    );

    """
    try:
        d= db('met')
        con = d.str
        cur = con.cursor()
        cur.execute(str)
        if cur.fetchone()[0]:
            return True
        else:
            return False
    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()


#
# sql_command_daterange(dataframe_range_extract(df))
def dataframe_range_extract(df):
    datapack = {}
    # REPORTBACK: CPER table 2017 has a bunch of mising timestamps
    minimum = min(df.TIMESTAMP.astype("datetime64"))
    maximum = max(df.TIMESTAMP.astype("datetime64"))
    projk = df.ProjectKey.unique()[0]
    return (minimum,maximum, projk)
# isinstance(current_data['akron'],list)
def batch_check(whichset):
    internal_dict = {}
    selectedset=whichset
    if isinstance(whichset['akron'],str):
        for i in selectedset.items():
            print("processing: ",i)
            fullpath = os.path.join(files_path,i[1])
            r = requests.get(fullpath)
            if r.status_code == requests.codes.ok:
                projk = projectkey_extractor(fullpath)
                # d = datScraper(fullpath)
                # internal_dict.update({projk:d.df.shape})
                internal_dict.update({projk:r.status_code})
            else:
                print(f"path {i} was not found; status code: ",r.status_code)
        return internal_dict
    elif isinstance(whichset['akron'],list):
        for i in selectedset.items():
            for j in i[1]:
                print(j)
                print("processing: ",j)
                fullpath = os.path.join(files_path,j)
                r = requests.get(fullpath)
                if r.status_code == requests.codes.ok:
                    projk = projectkey_extractor(fullpath)
                    # d = datScraper(fullpath)
                    # internal_dict.update({projk:d.df.shape})
                    internal_dict.update({projk:r.status_code})
                else:
                    print(f"path {j} was not found; status code: ",r.status_code)
        return internal_dict

# batch_check(historic_files)
# batch_check()


# requests.get(p).status_code
# p = os.path.join(files_path,current_data['heartrockranch'])
# p
# d = datScraper(p)
# df = d.df
# df['ProjectKey'] = projectkey_extractor(p)
# dataframe_range_extract(df)
# dataframe_range_extract(df)

# sql_command_daterange(dataframe_range_extract(df))
class datScraper:
    """
    path handler 1st
    """
    def __init__(self,path):
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
        self.arrays = []
        self.path = path



        if (os.path.splitext(self.path)[1]=='.dat') and ('https' in self.path):
            """
            if its an online .dat file
            """
            with closing(requests.get(self.path, stream=True)) as r:
                all_lines =(line.decode('utf-8') for line in r.iter_lines())
                for index,each_line in enumerate(all_lines):
                    if index<=3:
                        split_line = each_line.split(",")
                        self.arrays.append([each_character.replace("\"","") for each_character in split_line])

        elif (os.path.splitext(self.path)[1]==".csv") and ('https' in self.path):
            with closing(requests.get(self.path, stream=True)) as r:
                f=(line.decode('utf-8') for line in r.iter_lines())
                reader = csv.reader(f, delimiter=',', quotechar='"')
                for index, each_line in enumerate(reader):
                    if index<=3:
                        self.arrays.append([each_character.replace("\"","") for each_character in each_line])
        # while len(self.arrays[0])<25:
        #     temp_space = ''
        #     self.arrays[0].append(temp_space)

        self.arrays = self.arrays[1]
        for n,i in enumerate(self.arrays):
            if '%' in self.arrays[n]:
                self.arrays[n]=i.replace('%', 'perc')

        if os.path.splitext(self.path)[1]=='.dat':
            #REPORTBACK: line 11138(or line 11139 if not 0-index) is malformed in the currently posted DAT file for Pullman
            self.df = pd.read_table(self.path, sep=",", skiprows=4, low_memory=False) if ('PullmanTable1' not in self.path) else pd.read_table(path, sep=",", skiprows=[0,1,2,3,11138], low_memory=False)
        elif os.path.splitext(self.path)[1]==".csv":
            self.df = pd.read_csv(self.path, skiprows=4, low_memory=False)

        self.df.columns = self.arrays

    def clear(self,var):
        var = [] if isinstance(var,list) else None
        return var

def path_handler(site,historic=False,year=None):
    if historic==False and year==None:
        prepath = m_path
        realpath = os.path.join(prepath,current_data[site])
        return realpath
    elif historic==True and year!=None and isinstance(int(year),int)==True:
        prepath = m_path
        for i in historic_files[site]:
            if str(year) in i:
                specific_file = i
        realpath = os.path.join(prepath, specific_file)
        return realpath
    else:
        print("site or year not found")

files_path = r"https://winderosionnetwork.org/files/"

sites = ["akron", "bigspring", "cper", "heartrockranch",
        "holloman", "jornada", "lordsburg",
        "moab", "morton", "northernplains", "pullman", "redhills",
        "sanluis", "southernplains", "twinvalley"]

historic_files = {
"akron":["Akron_MetData_2018.csv"],
"bigspring":["BigSpring_MetData_2018.csv","BigSpring_MetData_2017.csv",
             "BigSpring_MetData_2016.csv"],
"cper":["CPER_MetData_2015.csv","CPER_MetData_2016.csv","CPER_MetData_2017.csv",
        "CPER_MetData_2018.csv"],
"heartrockranch": ["Bellevue_MetData_2015.csv","Bellevue_MetData_2016.csv",
                    "Bellevue_MetData_2017.csv","Bellevue_MetData_2018.csv"],
"holloman":["Holloman_MetData_2015.csv","Holloman_MetData_2016.csv",
            "Holloman_MetData_2017.csv","Holloman_MetData_2018.csv"],
"jornada": ["JER_MetData_2015.csv","JER_MetData_2016.csv",
            "JER_MetData_2017.csv","JER_MetData_2018.csv"],
# "lordsburg":[],
"moab": ["Moab_MetData_2016.csv","Moab_MetData_2017.csv","Moab_MetData_2018.csv"],
# "morton":[],
"northernplains":["Mandan_MetData_2015.csv", "Mandan_MetData_2016.csv",
                    "Mandan_MetData_2017.csv","Mandan_MetData_2018.csv"],
"pullman": ["Pullman_MetData_2016.csv","Pullman_MetData_2017.csv","Pullman_MetData_2018.csv"],

"redhills": ["RedHills_MetData_2019.csv"],
"sanluis": ["SanLuis_MetData_2015.csv","SanLuis_MetData_2016.csv","SanLuis_MetData_2017.csv",
            "SanLuis_MetData_2018.csv"],
"southernplains":["ElReno_MetData_2017.csv", "ElReno_MetData_2018.csv"],
"twinvalley":["TwinValley_MetData_2019.csv"]
}

current_data = {
"akron": r"datfiles/Akron/AkronTable1.dat",
"bigspring":"datfiles/BigSpring/BigSpringTable1.dat",
"cper":"datfiles/CPER/CPERTable1.dat",
"heartrockranch":"datfiles/Bellevue/BellevueTable1.dat",
"holloman":"datfiles/Holloman/HollomanTable1.dat",
"jornada":"datfiles/JER/JERTable1.dat",
"lordsburg":"datfiles/Lordsburg/LordsburgTable1.dat",
"moab":"datfiles/Moab/MoabTable1.dat",
"morton":"datfiles/Morton/MortonTable1.dat",
"northernplains":"datfiles/Mandan/MandanTable1.dat",
"pullman":"datfiles/Pullman/PullmanTable1.dat",
"redhills":"datfiles/RedHills/RedHillsTable1.dat",
"sanluis":"datfiles/SanLuis/SanLuisTable1.dat",
"southernplains":"datfiles/ElReno/ElRenoTable1.dat",
"twinvalley":"datfiles/TwinValley/TwinValleyTable1.dat"
}
