###### Downloading subreddit contents for data analysis

Includes predownloaded subreddit 'worldnews' in git-lfs csv file (';' separated) together with their published dates, scores, title text and guessed language.
Script redownload.py is able to download the whole pack again (and filter languages) or to just update a portion of the original file (scores can change).

Processes data in memory using pandas.
Uses click, pandarallel, psaw PushiftApi.

Example usage:  
`python redownload.py --csv_data="worldnewsen.csv" --csv_data_save="updated.csv" --end_epoch="2020-01-01"`  
(downloads new content beginning from the last day in worldnewen.csv to 2020and saves it to different file to see the difference)
