import sys
import suds

from random import randint
from time import sleep
import os
from os import walk

import pandas as pd

from get_api import build_sel, targeting_idea_service

# get list of input file names

INPUT_FOLDER = os.getcwd() + "/input/"
RESULT_FOLDER = os.getcwd() + "/result/"

f = []
for (dirpath, dirnames, filenames) in walk(INPUT_FOLDER):
    f.extend(filenames)
    break


for file in f:
    
    file_name = INPUT_FOLDER + file
    main = pd.read_csv(file_name, encoding = 'utf-8')
    
    keywords = list()
    competition = list()
    cpc = list()
    search_volumes = list()
    criteria_id = list()
    hard_criteria = list()
    bad_list = list()
    new_field = list()
    er = main['Criteria ID'][0]

    super_list= list(main['Criteria ID'].unique())[list(main['Criteria ID'].unique()).index(er):len(list(main['Criteria ID'].unique()))]
    print(file)
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
            
            while start < len(df):
                
                #  sleep(randint(1,4))
                some_list = list(df['Phrase'].unique()[start:end])
                selector = build_sel(some_list, criteria)
                attempts = 0
                n = 30
                
                while attempts < n:
                    
                    try:
                        page = targeting_idea_service.get(selector)
                        break
                    except suds.WebFault as e:
                        for error in e.fault.detail.ApiExceptionFault.errors:
                            # print (error)
                            if error['ApiError.Type'] == 'CriterionError':
                                print ("CriterionError + " + str(er))
                                bad_list.append(er)
                                attempts = n
                                bad_criteria = True
                                break
                            if error['ApiError.Type'] == 'RateExceededError':
                                print ("RATE_LIMIT_HELL")
                                sleep(randint(40,50))
                                attempts += 1
                                # break
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
    #df_new['country'] = country
    #df_new['locode'] = locode

    #1 month
    def f_x(x):
        if len(x[0]) == 3:
            return x[0][2]
        else:
            return 0
    df_new['1_date'] = df_new['new_field'].apply(lambda x: f_x(x))
    #2 month
    def f_x(x):
        if len(x[1]) == 3:
            return x[1][2]
        else:
            return 0
    df_new['2_date'] = df_new['new_field'].apply(lambda x: f_x(x))
    #3 month
    def f_x(x):
        if len(x[2]) == 3:
            return x[2][2]
        else:
            return 0
    df_new['3_date'] = df_new['new_field'].apply(lambda x: f_x(x))
    #4 month
    def f_x(x):
        if len(x[3]) == 3:
            return x[3][2]
        else:
            return 0
    df_new['4_date'] = df_new['new_field'].apply(lambda x: f_x(x))
    #5 month
    def f_x(x):
        if len(x[4]) == 3:
            return x[4][2]
        else:
            return 0
    df_new['5_date'] = df_new['new_field'].apply(lambda x: f_x(x))
    #6 month
    def f_x(x):
        if len(x[5]) == 3:
            return x[5][2]
        else:
            return 0
    df_new['6_date'] = df_new['new_field'].apply(lambda x: f_x(x))
    #7 month
    def f_x(x):
        if len(x[6]) == 3:
            return x[6][2]
        else:
            return 0
    df_new['7_date'] = df_new['new_field'].apply(lambda x: f_x(x))
    #8 month
    def f_x(x):
        if len(x[7]) == 3:
            return x[7][2]
        else:
            return 0
    df_new['8_date'] = df_new['new_field'].apply(lambda x: f_x(x))
    #9 month
    def f_x(x):
        if len(x[8]) == 3:
            return x[8][2]
        else:
            return 0
    df_new['9_date'] = df_new['new_field'].apply(lambda x: f_x(x))
    #10 month
    def f_x(x):
        if len(x[9]) == 3:
            return x[9][2]
        else:
            return 0
    df_new['10_date'] = df_new['new_field'].apply(lambda x: f_x(x))
    #11 month
    def f_x(x):
        if len(x[10]) == 3:
            return x[10][2]
        else:
            return 0
    df_new['11_date'] = df_new['new_field'].apply(lambda x: f_x(x))
    #12 month
    def f_x(x):
        if len(x[11]) == 3:
            return x[11][2]
        else:
            return 0
    df_new['12_date'] = df_new['new_field'].apply(lambda x: f_x(x))

    #cpc column to int

    def make_cpc(x):
        if len(x) > 1:
            return x[1]
        else:
            return x
    df_new['cpc'] = df_new['cpc'].apply(lambda x: make_cpc(x))

    #delete new field
    del df_new['new_field']
    #df_new.drop_duplicates(inplace = True)

    result_path = RESULT_FOLDER + "result_" + file
    df_new.to_csv(result_path, encoding = 'utf-8')