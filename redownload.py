import datetime as dt

import click
import pandas as pd
from langdetect import detect
from pandarallel import pandarallel
from psaw import PushshiftAPI
from tqdm import tqdm

pandarallel.initialize(progress_bar=True, nb_workers=4)

api = PushshiftAPI()


def safedetect(text):
    try:
        return detect(text)
    except:
        return 'nan'


def get_date(datetime):
    return datetime.date()


@click.command()
@click.option('--csv_data', default="worldnewsen.csv",
              help='The models file name to update',
              )
@click.option('--csv_data_save', default=None,
              help='The models file name to save. None to keep the same as csv_data.',
              )
@click.option('--start_epoch', default=None,
              help='start downloading from Y-m-d. None means the last day present in the dataset to modify.')
@click.option('--end_epoch', default=None,
              help='end downloading from Y-m-d. None means no limit.')
@click.option('--subreddit', default="worldnews",
              help='reddit channel')
@click.option('--discard_languages', default=['fa', 'ar', 'vi'],
              help='list of languages to discard')
def redownload(csv_data, csv_data_save, start_epoch, end_epoch, subreddit,
               discard_languages):
    """
    Updates existing csv or creates a new one by downloading data from pushift reddit api.
    """
    data = None
    if csv_data:
        data = pd.read_csv(csv_data, delimiter=';',
                           date_parser=lambda text: dt.datetime.strptime(text, "%Y-%m-%d %H:%M:%S"),
                           parse_dates=['published'])
        data['published_date'] = data['published'].parallel_apply(get_date)
        
        print("Last news present:")
        print(data.tail())
    
    if start_epoch is None:
        if data is None:
            raise ValueError("start_epoch must be specified if csv_data is None.")
        start_epoch = data['published_date'].iloc[-1] + dt.timedelta(days=0)
        
        print("Starting at")
        print(start_epoch)
        # + dt.timedelta(days=1) nope,  download the last day again in case it was incomplete
    else:
        start_epoch = dt.datetime.strptime(start_epoch, '%Y-%m-%d')
    if end_epoch is not None:
        end_epoch = dt.datetime.strptime(end_epoch, '%Y-%m-%d')
    
    # drop data that will be replaced:
    if data is not None:
        if end_epoch is not None:
            data = data[(data['published_date'] < start_epoch) | (data['published_date'] >= end_epoch)]
        else:
            data = data[data['published_date'] < start_epoch]
    
    del data['published_date']
    
    pbar = tqdm(api.search_submissions(after=int(dt.datetime.combine(start_epoch, dt.datetime.min.time()).timestamp())
                                                  if start_epoch else None,
                                       before=int(dt.datetime.combine(end_epoch, dt.datetime.min.time().timestamp())
                                                  if end_epoch else None,
                                       subreddit=subreddit,
                                       filter=['title', 'score'],
                                       limit=None))
    
    lastdate = None
    allitems = []
    for item in pbar:
        created = dt.datetime.fromtimestamp(item.created_utc)
        if lastdate is None or created.date() != lastdate.date():
            pbar.set_description("{}".format(created))
        
        data_point = {'published': created, 'score': item.score,
                      'title': item.title,
                      # 'lang': detect(item.title)
                      }
        allitems.append(data_point)
        lastdate = created
    newitems = pd.DataFrame(allitems)
    newitems['title'] = newitems['title'].apply(lambda text: text.replace('\n', ' ').replace('\r', ''))
    newitems['lang'] = newitems['title'].parallel_apply(safedetect)
    newitems = newitems[~newitems['lang'].isin(discard_languages)]
    newitems = newitems.sort_values(['published'], ascending=True)
    if data is not None:
        data = data.append(newitems, ignore_index=True, sort='published')
    else:
        data = newitems
    if csv_data_save is None:
        csv_data_save = csv_data
    
    print("Last news up to downloaded:")
    print(data.tail())
    
    data.to_csv(csv_data_save, sep=';', index=False)


if __name__ == "__main__":
    redownload()
