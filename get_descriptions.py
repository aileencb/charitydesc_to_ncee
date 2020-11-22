import xml.etree.ElementTree as ET
import requests
import numpy as np
import pandas as pd
import time as time
from urllib.request import urlopen

# function to read in each url from text file produced by get_urls.py
def parse(path):
    f = open(path, 'r')
    for l in f:
        yield l

# the following tags can occur multiple times in an xml filing, so we remove them because column names must be unique.
start = time.time()
colnames = ['EIN', 'Name', 'ActivityOrMissionDesc', 'MissionDesc', 'Desc']
records = []
i = 0
for url in parse('data/URL_LIST_2015.txt'):
    j = 0
    # https://stackoverflow.com/questions/36142149/python-run-try-again-after-exception-caught-and-worked-out
    # error handling for internet service interruptions, tries 5 times to reach url and then skips for next url
    while j < 6:
        try:
            f = urlopen(url)
            network_error = False
            break # quit the loop if successful
        except:
            network_error = True
            time.sleep(1)
            j += 1
            
    if network_error == True:
        print(f'skipped record {i}: {url}')
    else:
        # get tree from xml filing
        tree = ET.parse(f)
        # get root from xml filing
        root = tree.getroot()
        # get organization EIN and name
        header = root.find('{http://www.irs.gov/efile}ReturnHeader')
        filer = header.find('{http://www.irs.gov/efile}Filer')
        ein = filer.find('{http://www.irs.gov/efile}EIN').text
        try:
            name_grp = filer.find('{http://www.irs.gov/efile}BusinessName').tag
        except:
            name_grp = filer.find('{http://www.irs.gov/efile}Name').tag
            
        try:
            name = filer.find(name_grp).find('{http://www.irs.gov/efile}BusinessNameLine1Txt').text
        except:
            name = filer.find(name_grp).find('{http://www.irs.gov/efile}BusinessNameLine1').text
        
        # get organization description(s)
        data = root.find('{http://www.irs.gov/efile}ReturnData')
        irs990 = data.find('{http://www.irs.gov/efile}IRS990')
        # get ActivityOrMissionDesc
        try:
            am_desc = irs990.find('{http://www.irs.gov/efile}ActivityOrMissionDesc').text
            desc = irs990.find('{http://www.irs.gov/efile}Desc').text
            m_desc = irs990.find('{http://www.irs.gov/efile}MissionDesc').text
        except:
            continue

        
        # get record
        record = dict(zip(colnames, np.array([ein, name, am_desc, desc, m_desc])))
        # append record to dataframe
        records.append(record)
        i += 1
       
    if i % 1000 == 0:
        df = pd.DataFrame(records, columns = colnames)
        print(time.time() - start)
        print(i)
        if i == 1000:
            head = True
        else:
            head = False
        df.to_csv('data/descriptions.csv', mode='a', header=head, index=False)
        records = []
        
        
        
            
