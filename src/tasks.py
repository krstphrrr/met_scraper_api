from celery.task import task
import requests, csv, os, os.path, pandas as pd, numpy as np
from contextlib import closing
from src.dima.tabletools import tablecheck, table_create
from src.utils.tools import db
from src.tall_tables.talltables_handler import ingesterv2
import time
import io

@task()
def test_task():
    print("test ok")
    df_dict = {}
    # check if table exists

    count=1
    if tablecheck("raw_met_data", "met"):
        print("table found")
        # create dataframes
        # while len(df_dict)<3:
        for i in current_data.items():
            """
            1. Bellevue is currently offline on the winderosion site
            2. datScraper will create a dataframe with the .dat file provided
            by the fullpath variable
            3. getdf method will do some initial cleanup related to .dat files:
            - removing escape characters like forward slashes
            - fixing float precision across the whole dataframe
            - casting timestamp as a timestamp data type if it is anything else
            - removing backslashes and dots from fieldnames
            4. col_name_fix will look for fieldname inconsistencies across all
            the dataframes, it will change their name to make the dataframes
            easier to concat later on
            5. new_instrumentation will add new fields/sensors found in some newer
            instrument setups
            6. projectkey (or the name of the site ) is added to dataframe
            7. data_slice_df will create a subset of the dataframe with the whole
            .dat file based on dataranges not currently on postgres.
            8. after the whole sliced dataframe is concatenated, row_check will
            send row by row up to PG  ONLY if timestamp + projectkey is not found.


            """
            if 'BellevueTable1' not in i[1]:

                print(i[1])
                fullpath = os.path.join(files_path,i[1])
                projk = projectkey_extractor(fullpath)
                print(f"entering scraper 2 df function for {i}")
                ins = datScraper(fullpath)
                df = ins.getdf()
                df = col_name_fix(df)
                df = new_instrumentation(df)
                df = remove_emptytimestamps(df)
                df['ProjectKey'] = projk # adding projectkey to dataframe
                print(f'creating timestamp slice with dataframe {count} of {len(current_data)}..')
                smallerdf = date_slice_df(df,name_in_pg[projk])


                df_dict.update({f'df{count}':smallerdf})
                count+=1
        # return df_dict
        print("assembling new dataframe")
        finaldf = pd.concat([i[1] for i in df_dict.items()])
        finaldf = type_fix(finaldf)
        # return finaldf
        print("starting row check and ingest")
        row_check(finaldf)


    else:
        print("table not found")

def type_fix(df):
    df = df.copy()
    for i in df.columns:
        if (df[i].dtype == "object") and ("ProjectKey" not in i) and ("TIMESTAMP" not in i):
            df[i] = df[i].astype(float)
        if "RECORD" in i:
            df.RECORD = df.RECORD.astype("int64")
    return df

def remove_emptytimestamps(dataframe):
    if "TIMESTAMP" in dataframe.columns:
        dataframe = dataframe.loc[~pd.isnull(dataframe.TIMESTAMP)==True].copy()
        dataframe = dataframe.reset_index(drop=True)
    return dataframe

def quick_check(path):
    arrays = []
    if (os.path.splitext(path)[1]=='.dat') and ('https' in path):
        """
        if its an online .dat file
        """
        with closing(requests.get(path, stream=True)) as r:
            all_lines =(line.decode('utf-8') for line in r.iter_lines())
            for index,each_line in enumerate(all_lines):
                if index<=3:
                    split_line = each_line.split(",")
                    arrays.append([each_character.replace("\"","") for each_character in split_line])
    arrays = arrays[1]

    if os.path.splitext(path)[1]=='.dat':
        #REPORTBACK: line 11138(or line 11139 if not 0-index) is malformed in the currently posted DAT file for Pullman
        df = pd.read_table(path, sep=",", skiprows=4, low_memory=False) if ('PullmanTable1' not in path) else pd.read_table(path, sep=",", skiprows=[0,1,2,3,11138], low_memory=False)
    df.columns = arrays
    return df

def quick_ingest(whichset):

    internal_dict = {}
    selectedset=whichset
    # for current data -- dat files
    count=1
    if isinstance(whichset['akron'],str):
        for i in selectedset.items():
            if "BellevueTable1" not in i:
                print("processing: ",i)
                fullpath = os.path.join(files_path,i[1])
                projk = projectkey_extractor(fullpath)
                inst = datScraper(fullpath)
                df = inst.getdf()
                df['ProjectKey'] = projk
                df = second_round(df)
                df = df.loc[pd.isnull(df.TIMESTAMP)!=True] if any(pd.isnull(df.TIMESTAMP.unique())) else df

                internal_dict.update({f"df{count}":df})
                count+=1

        # return internal_dict
        # for historic data -- csv
    elif isinstance(whichset['akron'],list):
        for i in selectedset.items():
            for j in i[1]:
                # print(j)
                print("processing: ",j)
                fullpath = os.path.join(files_path,j)
                projk = projectkey_extractor(fullpath)
                inst = datScraper(fullpath)
                df = inst.getdf()
                df['ProjectKey'] = projk
                df = second_round(df)
                df = df.loc[pd.isnull(df.TIMESTAMP)!=True] if any(pd.isnull(df.TIMESTAMP.unique())) else df

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

def row_check(df):
    d = db("met")
    for i in range(len(df)):

        try:
            if pg_check(df.iloc[i:i+1].TIMESTAMP.values[0], df.iloc[i:i+1].ProjectKey.values[0]):
                print(f'timestamp:"{df.iloc[i:i+1].TIMESTAMP.values[0]}" and projectkey: "{df.iloc[i:i+1].ProjectKey.values[0]}" already in database, moving on..' )
            else:
                print(f'ingesting timestamp:"{df.iloc[i:i+1].TIMESTAMP.values[0]}" and projectkey: "{df.iloc[i:i+1].ProjectKey.values[0]}" ')
                ingesterv2.main_ingest(df.iloc[i:i+1],"raw_met_data", d.str, 10000)
        except Exception as e:
            print(e)
            d = db("met")

def pg_check(timestamp, projectkey):
    qry=f"""SELECT EXISTS(
                SELECT *
                FROM public.raw_met_data
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


def pull_minimum_date(projectkey):
    qry=f"""
    SELECT MIN("TIMESTAMP")
    FROM public.raw_met_data
    WHERE "ProjectKey"='{projectkey}';
    """
    try:
        d=db("met")
        con = d.str
        cur = con.cursor()
        cur.execute(qry)
        return cur.fetchone()[0]
    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()

def pull_max_date(projectkey):
    qry=f"""
    SELECT MAX("TIMESTAMP")
    FROM public.raw_met_data
    WHERE "ProjectKey"='{projectkey}';
    """
    try:
        d=db("met")
        con = d.str
        cur = con.cursor()
        cur.execute(qry)
        return cur.fetchone()[0]
    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()


def date_slice_df(df, projectkey):
    # projectkey = projectkey.capitalize() if 'jer' in projectkey else projectkey
    max = pull_max_date(projectkey)
    min = pull_minimum_date(projectkey)
    print(f"slicing dataframe '{projectkey}' with min:'{min}', max:'{max}'" )
    if 'TIMESTAMP' in df.columns:
        if df.TIMESTAMP.dtype=='<M8[ns]' or df.TIMESTAMP.dtype=='>M8[ns]':
            sliced_df = df.loc[~((df.TIMESTAMP>=min) & (df.TIMESTAMP<=max))]
        else:
            df.TIMESTAMP = df.TIMESTAMP.astype('datetime64')
            sliced_df = df.loc[~((df.TIMESTAMP>=min) & (df.TIMESTAMP<=max))]
        return sliced_df
    else:
        return 'Not MET-derived dataframe or TIMESTAMP field not available'

def projectkey_extractor(path):
    if os.path.splitext(os.path.basename(path))[1]=='.csv':
        return [i.lower() for i in os.path.basename(path).split('_') if ("Met" not in i) and (os.path.splitext(i)[1]=='')][0]
    elif os.path.splitext(os.path.basename(path))[1]=='.dat':
        if "JER" in os.path.basename(path):
            return [i.upper() for i in os.path.basename(path).split('Table') if (os.path.splitext(i)[1]=='')][0]
        else:
            return [i.lower() for i in os.path.basename(path).split('Table') if (os.path.splitext(i)[1]=='')][0]

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


def dataframe_range_extract(dataframe):
    datapack = {}
    # REPORTBACK: CPER table 2017 has a bunch of mising timestamps
    if ('TIMESTAMP' in dataframe.columns):
        minimum = min(dataframe.TIMESTAMP.astype("datetime64"))
        maximum = max(dataframe.TIMESTAMP.astype("datetime64"))
        projk = dataframe.ProjectKey.unique()[0]
        return (minimum,maximum, projk)
    elif ('TMSTAMP' in dataframe.columns):
        minimum = min(dataframe.TMSTAMP.astype("datetime64"))
        maximum = max(dataframe.TMSTAMP.astype("datetime64"))
        projk = dataframe.ProjectKey.unique()[0]
        return (minimum,maximum, projk)
    else:
        print(dataframe.columns)
# isinstance(current_data['akron'],list)

def batch_check(whichset):
    internal_dict = {}
    selectedset=whichset
    count=1
    if isinstance(whichset['akron'],str):
        for i in selectedset.items():
            print("processing: ",i)
            fullpath = os.path.join(files_path,i[1])
            r = requests.get(fullpath)
            if r.status_code == requests.codes.ok:
                projk = projectkey_extractor(fullpath)
                d = datScraper(fullpath)
                df= d.df
                df = df.loc[pd.isnull(df.TIMESTAMP)!=True] if any(pd.isnull(df.TIMESTAMP.unique())) else df
                # internal_dict.update({projk:d.df.shape})
                internal_dict.update({f"{projk}_{count}":df.shape})
                count+=1
            else:
                print(f"path {i} was not found; status code: ",r.status_code)
        return internal_dict
    elif isinstance(whichset['akron'],list):
        for i in selectedset.items():
            for j in i[1]:
                # print(j)
                print("processing: ",j)
                fullpath = os.path.join(files_path,j)
                r = requests.get(fullpath)
                if r.status_code == requests.codes.ok:
                    projk = projectkey_extractor(fullpath)
                    d = datScraper(fullpath)
                    df= d.df
                    df = df.loc[pd.isnull(df.TIMESTAMP)!=True] if any(pd.isnull(df.TIMESTAMP.unique())) else df
                    internal_dict.update({f"{projk}_{count}":df.shape})
                else:
                    print(f"path {j} was not found; status code: ",r.status_code)
        return internal_dict


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
            print("assembling dataframe from dat..")
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
    def getdf(self):
        print("fixing fields..")
        # print("fixing '\\n' in field names..")
        for i in self.df.columns:
            if '\n' in i:
                self.df.rename(columns={f'{i}':'{0}'.format(i.replace("\n",""))}, inplace=True)
        # print("fixing float precision..")
        for i in self.df.columns:
            if self.df[i].dtype =='float64':
                self.df.round({f'{i}':6})
        # print("fixing slash characters in field name..")
        for i in self.df.columns:
            # print(i,"pre slash taker")
            if '/' in i:
                self.df.rename(columns={f'{i}':'{0}'.format(i.replace("/","_"))}, inplace=True)
            # print(i, "post slash")
        # print("fixing '.' characters in field name..")
        # for i in self.df.columns:
        #     if '.' in i:
        #         self.df.rename(columns={f'{i}':'{0}'.format(i.replace(".",""))}, inplace=True)
        # print("casting timestamp as datetime..")
        for i in self.df.columns:
            if 'TIMESTAMP' in i:
                self.df.TIMESTAMP = pd.to_datetime(self.df.TIMESTAMP)
            elif 'TMSTAMP' in i:
                self.df.TMSTAMP = pd.to_datetime(self.df.TMSTAMP)

        for i in self.df.columns:
            if 'Albedo_Avg' in i:
                self.df.Albedo_Avg = self.df.Albedo_Avg.astype(float)
                self.df.Albedo_Avg = self.df.Albedo_Avg.apply(lambda x: np.nan if (x==np.inf) or (x==-np.inf) else x)
        print("returning dataframe")
        return self.df

def second_round(df):
    for i in df.columns:
        if "/" in i:
            df.rename(columns={f'{i}':'{0}'.format(i.replace("/","_"))}, inplace=True)
    return df


def col_name_fix(df):
    for i in df.columns:
        rep = ['TMSTAMP']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'TIMESTAMP'))}, inplace=True)

        rep = ['RECNBR']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'RECORD'))}, inplace=True)

        rep = ['CNR1TK_Avg']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'CNR4TK_Avg'))}, inplace=True)

        rep = ['AvgTemp_10M_DegC', 'AvgTemp_10m_DegC']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'AvgTemp_10M_DegC'))}, inplace=True)

        rep = ['AvgTemp_4M_DegC','AvgTemp_4m_Deg']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'AvgTemp_4M_DegC'))}, inplace=True)

        rep = ['AvgTemp_2M_DegC','AvgTemp_2m_Deg']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'AvgTemp_2M_DegC'))}, inplace=True)

        rep = ['Total_Rain_mm','Rain_Tot_mm']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'Total_Rain_mm'))}, inplace=True)

        rep = ['MaxWS6_10M_m_s', 'MaxWS_10m_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'MaxWS6_10M_m_s'))}, inplace=True)

        rep = ['MaxWS5_5M_m_s','MaxWS_5m_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'MaxWS5_5M_m_s'))}, inplace=True)

        rep = ['MaxWS4_25M_m_s','MaxWS_25m_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'MaxWS4_25M_m_s'))}, inplace=True)

        rep = ['MaxWS3_15M_m_s','MaxWS_15m_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'MaxWS3_15M_m_s'))}, inplace=True)

        rep = ['MaxWS2_1M_m_s','MaxWS_1m_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'MaxWS2_1M_m_s'))}, inplace=True)

        rep = ['MaxWS1_05M_m_s','MaxWS_05m_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'MaxWS1_05M_m_s'))}, inplace=True)

        rep = ['StdDevWS2_1M_m_s','StDevWS_2M_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'StdDevWS2_1M_m_s'))}, inplace=True)

        rep = ['AvgWS6_10M_m_s','AvgWS_10m_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'AvgWS6_10M_m_s'))}, inplace=True)

        rep = ['AvgWS5_5M_m_s','AvgWS_5m_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'AvgWS5_5M_m_s'))}, inplace=True)

        rep = ['AvgWS4_25M_m_s','AvgWS_25m_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'AvgWS4_25M_m_s'))}, inplace=True)

        rep = ['AvgWS3_15M_m_s','AvgWS_15m_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'AvgWS3_15M_m_s'))}, inplace=True)

        rep = ['AvgWS_1m__m_s','AvgWS2_1M_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'AvgWS2_1M_m_s'))}, inplace=True)

        rep = ['AvgWS1_05M_m_s','AvgWS_05m_m_s','AvgWS1_05m_m_s']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'AvgWS1_05M_m_s'))}, inplace=True)

        rep = ['Switch','Switch12V']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'Switch'))}, inplace=True)

        rep = ['Sensit_Tot','Sensit']
        if i in rep:
            df.rename(columns={f'{i}':'{0}'.format(i.replace(f"{i}",'Sensit_Tot'))}, inplace=True)

    return df

def new_instrumentation(df):
    for i in df.columns:
        if 'SWUpper_Avg' not in i:
            df['SWUpper_Avg'] = np.nan

        if 'SWLower_Avg' not in i:
            df['SWLower_Avg'] = np.nan

        if 'LWUpperCo_Avg' not in i:
            df['LWUpperCo_Avg'] = np.nan

        if 'LWLowerCo_Avg' not in i:
            df['LWLowerCo_Avg'] = np.nan

        if 'CNR4TK_Avg' not in i:
            df['CNR4TK_Avg'] = np.nan

        if 'RsNet_Avg' not in i:
            df['RsNet_Avg'] = np.nan

        if 'RlNet_Avg' not in i:
            df['RlNet_Avg'] = np.nan

        if 'Albedo_Avg' not in i:
            df['Albedo_Avg'] = np.nan

        if 'Rn_Avg' not in i:
            df['Rn_Avg'] = np.nan

        #new as of sept 30 (most recent ingest)
        if 'Sampling_time_2m' not in i:
            df['Sampling_time_2m'] = np.nan

        if 'PM1_2m_Avg' not in i:
            df['PM1_2m_Avg'] = np.nan

        if 'PM2_5_2m_Avg' not in i:
            df['PM2_5_2m_Avg'] = np.nan

        if 'PM2_5_2m_Avg' not in i:
            df['PM2_5_2m_Avg'] = np.nan

        if 'PM4_2m_Avg' not in i:
            df['PM4_2m_Avg'] = np.nan

        if 'PM10_2m_Avg' not in i:
            df['PM10_2m_Avg'] = np.nan

        if 'TtlMeas_2m_Avg' not in i:
            df['TtlMeas_2m_Avg'] = np.nan

        if 'Sampling_time_4m' not in i:
            df['Sampling_time_4m'] = np.nan

        if 'PM1_4m_Avg' not in i:
            df['PM1_4m_Avg'] = np.nan

        if 'PM2_5_4m_Avg' not in i:
            df['PM2_5_4m_Avg'] = np.nan

        if 'PM2_5_2m_Avg' not in i:
            df['PM2_5_2m_Avg'] = np.nan

        if 'PM4_4m_Avg' not in i:
            df['PM4_4m_Avg'] = np.nan

        if 'PM10_4m_Avg' not in i:
            df['PM10_4m_Avg'] = np.nan

        if 'TtlMeas_4m_Avg' not in i:
            df['TtlMeas_4m_Avg'] = np.nan

        if 'BattV_Min' in i:
            df.drop(['BattV_Min'], axis=1, inplace=True)
    return df

def remove_emptytimestamps(dataframe):
    if "TIMESTAMP" in dataframe.columns:
        dataframe = dataframe.loc[~pd.isnull(dataframe.TIMESTAMP)==True].copy()
        dataframe = dataframe.reset_index(drop=True)
    return dataframe

# def path_handler(site,historic=False,year=None):
#     if historic==False and year==None:
#         prepath = files_path
#         realpath = os.path.join(prepath,current_data[site])
#         return realpath
#     elif historic==True and year!=None and isinstance(int(year),int)==True:
#         prepath = files_path
#         for i in historic_files[site]:
#             if str(year) in i:
#                 specific_file = i
#         realpath = os.path.join(prepath, specific_file)
#         return realpath
#     else:
#         print("site or year not found")

files_path = r"https://winderosionnetwork.org/files/"

sites = ["akron", "bigspring", "cper", "bellevue",
        "holloman", "JER", "lordsburg",
        "moab", "morton", "northernplains", "pullman", "redhills",
        "sanluis", "southernplains", "twinvalley"]

historic_files = {
"akron":["Akron_MetData_2018.csv"],
"bigspring":["BigSpring_MetData_2018.csv","BigSpring_MetData_2017.csv",
             "BigSpring_MetData_2016.csv"],
"cper":["CPER_MetData_2015.csv","CPER_MetData_2016.csv","CPER_MetData_2017.csv",
        "CPER_MetData_2018.csv"],
"bellevue": ["Bellevue_MetData_2015.csv","Bellevue_MetData_2016.csv",
                    "Bellevue_MetData_2017.csv","Bellevue_MetData_2018.csv"],
"holloman":["Holloman_MetData_2015.csv","Holloman_MetData_2016.csv",
            "Holloman_MetData_2017.csv","Holloman_MetData_2018.csv"],
"JER": ["JER_MetData_2015.csv","JER_MetData_2016.csv",
            "JER_MetData_2017.csv","JER_MetData_2018.csv"],
# "lordsburg":[],
"moab": ["Moab_MetData_2016.csv","Moab_MetData_2017.csv","Moab_MetData_2018.csv"],
# "morton":[],
"mandan":["Mandan_MetData_2015.csv", "Mandan_MetData_2016.csv",
                    "Mandan_MetData_2017.csv","Mandan_MetData_2018.csv"],
"pullman": ["Pullman_MetData_2016.csv","Pullman_MetData_2017.csv","Pullman_MetData_2018.csv"],

"redhills": ["RedHills_MetData_2019.csv"],
"sanluis": ["SanLuis_MetData_2015.csv","SanLuis_MetData_2016.csv","SanLuis_MetData_2017.csv",
            "SanLuis_MetData_2018.csv"],
"southernplains":["ElReno_MetData_2017.csv", "ElReno_MetData_2018.csv"],
"twinvalley":["TwinValley_MetData_2019.csv"]
}
name_in_pg = {
'akron':'Akron',
'JER':'JER',
'bigspring':'BigSpring',
'lordsburg':'Lordsburg',
'moab':'Moab',
'morton':'Morton',
'holloman':'Holloman',
'bellevue': 'Bellevue',
'mandan':'Mandan',
'redhills':'RedHills',
'twinvalley':'TwinValley',
'sanluis':'SanLuis',
'pullman':'Pullman',
'elreno':'ElReno',
'cper':'CPER'
}


current_data = {
"akron": "datfiles/Akron/AkronTable1.dat",
"bigspring":"datfiles/BigSpring/BigSpringTable1.dat",
"cper":"datfiles/CPER/CPERTable1.dat",
"bellevue":"datfiles/Bellevue/BellevueTable1.dat",
"holloman":"datfiles/Holloman/HollomanTable1.dat",
"JER":"datfiles/JER/JERTable1.dat",
"lordsburg":"datfiles/Lordsburg/LordsburgTable1.dat",
"moab":"datfiles/Moab/MoabTable1.dat",
"morton":"datfiles/Morton/MortonTable1.dat",
"mandan":"datfiles/Mandan/MandanTable1.dat",
"pullman":"datfiles/Pullman/PullmanTable1.dat",
"redhills":"datfiles/RedHills/RedHillsTable1.dat",
"sanluis":"datfiles/SanLuis/SanLuisTable1.dat",
"elreno":"datfiles/ElReno/ElRenoTable1.dat",
"twinvalley":"datfiles/TwinValley/TwinValleyTable1.dat"
}
