import pandas as pd


def check_data(csv):
    data = pd.read_csv(csv, encoding = 'utf-8')
    data.columns = ['Locode', 'Phrase','Criteria ID','country']
    print(data.shape)
    print(data.head())


try:
    check_data("sent_keywords.csv")
except FileNotFoundError:
    print("does folder contain dataset?")
except Exception as e:
    raise e
