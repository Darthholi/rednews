# Load the Pandas libraries with alias 'pd'
import pandas as pd
import datetime as dt
from langdetect import detect
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True, nb_workers=4)

# Read data from file 'filename.csv'
# (in the same directory that your python process is based)
# Control delimiters, rows, column names with read_csv (see later)
data = pd.read_csv("reddit_worldnews_start_to_2016-11-22.csv")
data['time_created'] = data['time_created'].apply(lambda x: dt.datetime.fromtimestamp(x))

# Preview the first 5 lines of the loaded data
print(data.head())


import sqlite3
conn = sqlite3.connect("worldnews.sqlite")
datanewer = pd.read_sql_query("select * from submissions", conn)
print(datanewer.shape)

datanewer = datanewer.drop(columns=['id'])
data = data.drop(columns=['over_18', 'author', 'subreddit', 'down_votes'])
data = data.rename(columns={"time_created": "published", "date_created": "published_date", "up_votes": "score"})  # title stays

def safedetect(text):
    try:
        return detect(text)
    except:
        return 'no'

datanewer = datanewer.sort_values(['published'], ascending=True)
data = data.append(datanewer, ignore_index=True, sort='published')
data['title'] = data['title'].apply(lambda text: text.replace('\n', ' ').replace('\r', ''))
#data['lang'] = data['title'].apply(lambda text: safedetect(text))
data['lang'] = data['title'].parallel_apply(safedetect)
#data = data[data['lang'] != 'en']
print(data.tail())
data.to_csv("worldnews.csv", sep=';')