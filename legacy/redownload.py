import psaw
import datetime as dt
import sqlite3
import numpy as np
import io
import os
from tqdm import tqdm

from psaw import PushshiftAPI

api = PushshiftAPI()


#print(dt.datetime.fromtimestamp(start_epoch))
#print(start_epoch, end_epoch)
"""
data example:

[submission(created_utc=1514850550, score=1, title='President Trump Voices Support for Protesters Against the Islamic Regime in Iran', url='https://www.theissue.com/politics/president-trump-voices-support-for-protesters-against-the-islamic-regime-in-iran', created=1514850550.0, d_={'created_utc': 1514850550, 'score': 1, 'title': 'President Trump Voices Support for Protesters Against the Islamic Regime in Iran', 'url': 'https://www.theissue.com/politics/president-trump-voices-support-for-protesters-against-the-islamic-regime-in-iran', 'created': 1514850550.0}),
 submission(created_utc=1514850540, score=1, title="Twitter blocks AfD lawmaker's account over racist remarks against Arabs", url='https://www.dailysabah.com/islamophobia/2018/01/01/twitter-blocks-afd-lawmakers-account-over-racist-remarks-against-arabs', created=1514850540.0, d_={'created_utc': 1514850540, 'score': 1, 'title': "Twitter blocks AfD lawmaker's account over racist remarks against Arabs", 'url': 'https://www.dailysabah.com/islamophobia/2018/01/01/twitter-blocks-afd-lawmakers-account-over-racist-remarks-against-arabs', 'created': 1514850540.0}),
 
"""

impo
data = pd.read_csv("worldnewsen.csv", delimiter=';')


def dataset_to_sqlite(sqlite_target="worldnews.sqlite",
                      sqlite_erase=True,
                    start_epoch=int(dt.datetime(2016, 11, 22).timestamp()),
                    end_epoch=int(dt.datetime(2018, 12, 31).timestamp()),
                    subreddit='worldnews',
                      top_scored_num=None
                      ):
    # sqlite_target = ":memory:
    # ds_filename = 'small190429_bosboxes191017T115024.txt'

    if sqlite_erase and os.path.exists(sqlite_target):
        os.remove(sqlite_target)
    
    # display using cur.execute("select * from docs").fetchall()
    table_news_def = """create table if not exists submissions(id INTEGER PRIMARY KEY, published timestamp, published_date date, title text, score integer
     );
                     """
    
    con = sqlite3.connect(sqlite_target, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = con.cursor()
    cur.execute(table_news_def)

    pbar = tqdm(api.search_submissions(after=start_epoch, before=end_epoch,
                                subreddit=subreddit,
                                filter=['title', 'score'],
                                limit=None))
    
    lastdate = None
    lastsubmissions = []
    
    for submission in pbar:
        created = dt.datetime.fromtimestamp(submission.created_utc)
        if lastdate is None or created.date() != lastdate.date():
            pbar.set_description("{}".format(created))
            
            lastsubmissions.sort(key=lambda item: item.score, reverse=True)
            
            for item in lastsubmissions[:top_scored_num] if top_scored_num is not None else lastsubmissions:
                insert = (
                    created, created.date(), item.title, item.score)
                cur.execute("insert into submissions(published, published_date, title, score) values (?, ?, ?, ?)",
                            insert)
                
            lastsubmissions = []
            lastdate = created
        lastsubmissions.append(submission)
        
    cur.close()
    con.commit()
    con.close()
    
#dataset_to_sqlite()