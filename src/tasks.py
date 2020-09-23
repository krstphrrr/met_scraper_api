from celery.task import task
import requests
from contextlib import closing
import csv
import os, os.path
import pandas as pd
from dima.tabletools import tablecheck


@task()
def test_task():
    # print("test ok")
    # check if table exists
    if tablecheck("met_data", "met"):
        print("table found")
    else:
        print("table not found")


def timestamp_check():
    pass

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
        while len(self.arrays[0])<25:
            temp_space = ''
            self.arrays[0].append(temp_space)

        self.arrays = self.arrays[1]
        for n,i in enumerate(self.arrays):
            if '%' in self.arrays[n]:
                self.arrays[n]=i.replace('%', 'perc')

        if os.path.splitext(self.path)[1]=='.dat':
            self.df = pd.read_table(self.path, sep=",", skiprows=4, low_memory=False)
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
