import sys
import suds
import os

from random import randint
from time import sleep
from os import walk

import pandas as pd

from get_api import build_sel, targeting_idea_service

# GETTING NAMES

main = pd.read_csv("sent_keywords.csv", encoding='utf-8', nrows=100)

keywords = list()
competition = list()
cpc = list()
search_volumes = list()
criteria_id = list()
hard_criteria = list()
bad_list = list()
new_field = list()
locode = list()
country = list()

er = main['Criteria ID'][0]

print(len(list(main['Criteria ID'].unique())))

super_list= list(main['Criteria ID'].unique())[list(main['Criteria ID'].unique()).index(er):len(list(main['Criteria ID'].unique()))]
#super_list = to_hard[to_hard.index(er):len(to_hard)]
print(len(super_list))
print (len(keywords))
print (len(set(keywords)))

for criteria in super_list:
    sleep(randint(1,3))
    bad_criteria = False
    if criteria not in bad_list:
        print ('working on '+str(criteria))
        start = 0
        end = 700
        df = main[main['Criteria ID'] == criteria]
        er = criteria
        #locode = df["Locode"]
        #country = df["country"]
        while start < len(df):
            sleep(randint(1,4))
            some_list = list(df['Phrase'].unique()[start:end])
            selector = build_sel(some_list, criteria)
            
            attempts = 0
            while attempts <= 3:
                print ("attempt n _" + str(attempts))
                if attempts == 3:
                    bad_list.append(er)
                    bad_criteria = True
                    break
                try:
                    page = targeting_idea_service.get(selector)
                    break
                except:
                    attempts += 1
                    print ("RATE LIMIT HELL")
                    sleep(randint(40,50))
            if bad_criteria:
                break
            try:
                if 'entries' in page:
                    for result in page['entries']:

                        attributes = {}
                        for attribute in result['data']:
                            attributes[attribute['key']] = getattr(attribute['value'], 'value','0')

                        keywords.append(attributes['KEYWORD_TEXT'])
                        search_volumes.append(attributes['SEARCH_VOLUME'])
                        competition.append(attributes['COMPETITION'])
                        cpc.append(attributes['AVERAGE_CPC'])
                        new_field.append(attributes['TARGETED_MONTHLY_SEARCHES'])
                        criteria_id.append(criteria)
                        #locode.append(locode)
                        #country.append(country)
            except:
                pass
            start = end
            end = end + 700


df_new = pd.DataFrame()
df_new['keyword'] = keywords
df_new['search_volume'] = search_volumes
df_new['criteria_id'] = criteria_id
df_new['cpc'] = cpc
df_new['competition'] = competition
df_new['new_field'] = new_field

# getting dates from new_field

dates = []

for date in new_field[0]:
    year = str(date[0])
    month = str(date[1])
    if len(month) == 1:
        dates.append(year + "-" + "0" + month)
    else:
        dates.append(year + "-" + month)

names = ['phrase','avarage_msv','google_id','cpc','competition']
names.extend(dates)
names.extend(['locode','google_canonical'])



# result handling
main = pd.read_csv("sent_keywords.csv", encoding='utf-8')

RESULT_FOLDER = os.getcwd() + "/result/"

# get all result  files in a list

results_names = []
for (dirpath, dirnames, filenames) in walk(RESULT_FOLDER):
    results_names.extend(filenames)
    break

# combine result files into one frame 

result_frame = pd.DataFrame()
for result in results_names:
    temp =  pd.read_csv(RESULT_FOLDER + result, encoding='utf-8')
    result_frame = result_frame.append(temp, ignore_index=True)

main.columns = ['Locode', 'keyword','criteria_id','country']

result_frame = pd.merge(result_frame, main, on=["keyword","criteria_id"])

del result_frame['Unnamed: 0']

result_frame.columns = names

right_order = ['locode','phrase','google_id','google_canonical','avarage_msv','cpc','competition']
right_order.extend(dates)

result_frame = result_frame[right_order]

print(result_frame.head())