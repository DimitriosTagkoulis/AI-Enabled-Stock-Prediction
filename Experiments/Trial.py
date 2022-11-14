from searchtweets import ResultStream, gen_request_parameters, load_credentials, read_config
from datetime import datetime, timedelta
import time
import pickle
import argparse
from datetime import timedelta

# define date range function
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def save_object(obj, filename):
    with open(filename, 'ab') as outp:  # Overwrites any existing file.
        pickle.dump(obj, outp, pickle.HIGHEST_PROTOCOL)

# Set up the argument parser
ap = argparse.ArgumentParser()

# Get starting date
ap.add_argument("-sd", "--startdate", required=True,
                type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                help="Start date of the collection in the following form: "
                " YEAR-MO-DA for example 2018-01-28")

# Get end date
ap.add_argument("-ed", "--enddate", required=True,
                type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                help="End date of the collection in the following form: "
                " YEAR-MO-DA for example 2018-02-18")

# get wait time
ap.add_argument("-w", "--wait", required=False, default=45,
                help="Set wait time between requests to avoid Twitter rate limits. "
                "Default: 45")

# Parse arguments
args = vars(ap.parse_args())

# get waittime
waittime = int(args['wait'])

# load twitter keys
search_creds = load_credentials('.twitter_keys.yaml',
                               yaml_key = 'search_tweets_v2',
                               env_overwrite = False)

# load configuration for search query
config = read_config('search_config.yaml')

# fields for v2 api
tweetfields = ",".join(['created_at','geo','id','lang', 'public_metrics',
                        'source','text'])
userfields = ",".join(["created_at", "description", "location",
                       "name", "public_metrics","username"])
expansions = ",".join(["author_id"])

# set interval to loop through
start_date = args['startdate'].date()
end_date = args['enddate'].date()

for single_date in daterange(start_date, end_date):
        
    # set start timestamp
    start_ts = single_date

    # set end timestamp
    end_ts =  single_date + timedelta(days=1)

    # payload rules for v2 api
    rule = gen_request_parameters(query = config['query'],
                            results_per_call = config['results_per_call'],
                            start_time = start_ts.isoformat(),
                            end_time = end_ts.isoformat(),
                            tweet_fields = tweetfields,
                            user_fields = userfields,
                            expansions = expansions,
                            granularity=None,
                            stringify = False)

    # result stream from twitter v2 api
    rs = ResultStream(request_parameters = rule,
                      max_results=100000,
                      max_pages=1,
                      max_tweets = config['max_tweets'],
                      **search_creds)

    # number of reconnection tries
    tries = 10

    while True:
        tries -= 1
        try:
                # indicate which day is getting retrieved
            print(f'[INFO] - Retrieving tweets from {str(start_ts)}')

            # get json response to list
            tweets = list(rs.stream())

            # break free from while loop
            break
        except Exception as err:
            if tries == 0:
                raise err
            print(f'[INFO] - Got connection error, waiting 15 seconds and trying again. {tries} tries left.')

            time.sleep(15)

    # parse results to dataframe
    print(f'[INFO] - Saving tweets from {str(start_ts)}')
    file_prefix_w_date = config['filename_prefix'] + start_ts.isoformat()
    outpickle = f'{file_prefix_w_date}.pkl'
    save_object(tweets, outpickle)