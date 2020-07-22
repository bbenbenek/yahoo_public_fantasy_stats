import pandas as pd
from yahoo_oauth import OAuth2
import json
from json import dumps
import datetime
from datetime import datetime
import time
from tqdm.notebook import tqdm
import numpy as np
from requests.exceptions import HTTPError

import logging
oauth_logger = logging.getLogger('yahoo_oauth')

class Yahoo_Api():
    def __init__(self, consumer_key, consumer_secret,
                access_key):
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._access_key = access_key
        self._authorization = None
    def _login(self):
        global oauth
        oauth = OAuth2(None, None, from_file='./auth/oauth2yahoo.json')
        if not oauth.token_is_valid():
            oauth.refresh_access_token()

def main():
    #### Yahoo Keys ####
    with open('./auth/oauth2yahoo.json') as json_yahoo_file:
        auths = json.load(json_yahoo_file)
    yahoo_consumer_key = auths['consumer_key']
    yahoo_consumer_secret = auths['consumer_secret']
    yahoo_access_key = auths['access_token']
    #yahoo_access_secret = auths['access_token_secret']
    json_yahoo_file.close()

    #global yahoo_api
    yahoo_api = Yahoo_Api(yahoo_consumer_key, yahoo_consumer_secret, yahoo_access_key)#, yahoo_access_secret)

    #global game_key
    game_key = '390' # 2019 Yahoo game key

    return yahoo_api, game_key

# Instantiate the yahoo_api and game_key
yahoo_api, game_key = main()

# Do a quick log in check.
oauth_logger.disabled = False
yahoo_api._login()
oauth_logger.disabled = True # Disable logging because otehrwise you'll see the red box below Every. Single. Iteration.

# Check to see where the scraper left off at
#with open('yahoo_leagues.txt') as json_file:
#    league_dict = json.load(json_file)
#print("Current league ID:", league_dict['counter'])

# Do the magic

with open('yahoo_leagues.txt') as json_file:
    league_dict = json.load(json_file)

oauth_logger.disabled = True

def get_response(retries=0):
    #if not oauth.token_is_valid():
    #    oauth.refresh_access_token()
    # ^^ This did not work inside the function, so i had to revert to signing in each iteration
    yahoo_api._login()

    url = 'https://fantasysports.yahooapis.com/fantasy/v2/league/'+game_key+'.l.'+league_id+'/'
    response = oauth.session.get(url, params={'format': 'json',
                                              #'oauth_signature_method': 'HMAC-SHA1', # still can't figure out how to properly sign each API call
                                              #'oauth_signature_method': 'PLAINTEXT'
                                             })
    '''
    This section taken from yfpy Yahoo API wrapper and adapted to fit my repeated login attempts
    https://github.com/uberfastman/yfpy
    '''
    try:
        #response.raise_for_status()
        if response.status_code == 999 or str(response.status_code)[0] == '5':
            raise HTTPError("Yahoo data unavailable due to rate limiting. Please try again later.")
    except HTTPError as e:
        # retry with incremental back-off
        if retries < 26:
            retries += 1
            print("Request failed at {} with status code {}. Retrying {} more times...".format(
                datetime.now().strftime("%H:%M:%S"), response.status_code, 26 - retries
            ))
            time.sleep(300) # Wait 5 minutes (300 seconds) to retry
            response = get_response(retries)
        else:
            # log error and terminate query if status code is not 200 after 3 retries
            print('Tried 25 times to restart, Daily Rate Limit probably reached')
    return response


counter = league_dict['counter'] # Set the counter to begin the for-loop. Useful if the internet goes out,
                                 # or your computer wants to randomly restart and close your script
#counter = 7918 # alternatively, you can manually set the counter

for league_id in tqdm(range(counter + 1, 300000)): # Start looping through an arbitrary number of league IDs
    league_id = str(league_id)

    response = get_response() # This will try 25 times to get a non-999 or 500 response. If still 999/500 after 25 tries, then this next part will run
    if response.status_code == 999:
        print('API rate limit reached at League ID:', league_id)
        league_dict['counter'] = counter
        with open('yahoo_leagues.txt', 'w') as file:
            file.write(json.dumps(league_dict))
        print("Current Time =", datetime.now().strftime("%H:%M:%S"), 'Rate limit is 1 hour')
        break

    r = response.json()

    if 'error' in r:
        if r['error']['description'] == 'There was a temporary problem with the server. Please try again shortly.':
            league_type = 'DNE'
            league_dict['Does Not Exist'].append(int(league_id))
            counter+=1
            if counter % 500 == 0:# Avoid writing to a file each iteration because once the counter gets high enough, the file gets significantly larger and takes more time to write.
                league_dict['counter'] = counter
                with open('yahoo_leagues.txt', 'w') as file:
                    file.write(json.dumps(league_dict))
            time.sleep(.1) # Avoid calling the API too quickly
            continue

        elif r['error']['description'] == 'You are not allowed to view this page because you are not in this league.':
            league_type = 'Private'
            league_dict['Private'].append(int(league_id))
            counter+=1
            if counter % 500 == 0:
                league_dict['counter'] = counter
                with open('yahoo_leagues.txt', 'w') as file:
                    file.write(json.dumps(league_dict))
            time.sleep(.1)
            continue

    elif 'fantasy_content' in r:
        league_type = 'PUBLIC'
        league_dict['Public'][league_id] = r['fantasy_content']['league'][0]
        counter+=1
        if counter % 500 == 0:
            league_dict['counter'] = counter
            with open('yahoo_leagues.txt', 'w') as file:
                file.write(json.dumps(league_dict))
        time.sleep(.1)
        continue
    else:
        print('Something else went wrong! Time to diagnose!')
        print(r)
        counter+=1
        break

    print(league_id, league_type)
    print(r, "\n")
