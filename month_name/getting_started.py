import os

import pandas as pd


def split_data(csv):
    main = pd.read_csv(csv, encoding = 'utf-8')
    main.columns = ['Locode', 'Phrase','Criteria ID','country']

    # split input data into 100 000 rows files

    step = 100000
    start = 0
    end = 100000
    k = main.shape[0] // step * step

    n = 1
    while end <= k:
        sub_df =  main.iloc[start:end,:]
        file_path = INPUT_FOLDER + "input_n_" + str(n) + ".csv"
        sub_df.to_csv(file_path, encoding = 'utf-8')
        n = n + 1
        start = start + step
        end = end + step


def split_tail(csv):
    step = 100000
    main = pd.read_csv(csv, encoding = 'utf-8')
    main.columns = ['Locode', 'Phrase','Criteria ID','country']
    tail_length = main.shape[0] - (main.shape[0] // step * step)
    tail = main.tail(tail_length)
    file_path = INPUT_FOLDER + "tail" + ".csv"
    tail.to_csv(file_path, encoding = 'utf-8')


INPUT_FOLDER = os.getcwd() + "/input/"
RESULT_FOLDER = os.getcwd() + "/result/"

# TODO: check if file is consists
os.makedirs(INPUT_FOLDER)
os.makedirs(RESULT_FOLDER)

# read main csv file
csv = "sent_keywords.csv"

split_data(csv)
split_tail(csv)
