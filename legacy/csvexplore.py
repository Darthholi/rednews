# Load the Pandas libraries with alias 'pd'
import pandas as pd
import datetime as dt
from langdetect import detect
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True, nb_workers=4)

data = pd.read_csv("worldnews.csv", delimiter=';')
print(data.describe())
print(data.tail())
data = data[(data['lang'] != 'fa') & (data['lang'] != 'ar') & (data['lang'] != 'vi')]
print(data.describe())
print(data.tail())
data.to_csv("worldnewsen.csv", sep=';')