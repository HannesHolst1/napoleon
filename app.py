from pymongo import MongoClient, ASCENDING
from datetime import datetime, timedelta
from flask import Flask, request as rq
from backend import TwitterAPI, DataProcessor, config

app = Flask(__name__)

def error_response(desc):
    payload = { "success": False, "error": desc}
    return payload

def error_http_code():
    return 200

@app.route('/')
def status_check():
    return {"success": True}, 200

@app.route('/valet', methods=['POST'])
def main():
    request_info = rq.get_json()

    if not request_info:
        return error_response(desc="The request format is empty."), error_http_code()

    if not 'name' in request_info:
        return error_response(desc="The request format is wrong."), error_http_code()

    client = MongoClient(config.mongodb['uri'])
    db = client[config.mongodb['database']]
    api_requests = db[config.mongodb['request_collection']]

    request = api_requests.find_one(request_info) 

    if not request:
        return error_response(desc="The request {} does not exists.".format(request_info['name'])), error_http_code()

    ## 1st step
    ## download & process all new tweets since last data pull

    tweets = TwitterAPI.Tweets(bearer=config.twitter['bearer'], tweet_search_uri=config.twitter['tweet_search_uri'], 
                               search_query=request['query'], search_parameters=request['parameters'], since_tweet_id=request['since_id'], next_token=None)
    
    tweets.getdata()

    if tweets.is_success:
        if tweets.is_json:
            json_response = tweets.data
        else:
            error_response(desc="Something went wrong. API did not return a JSON-formatted file."), error_http_code() 

    if tweets.is_error:
        print(tweets.data)
        if ('detail' in tweets.data) and ('status' in tweets.data):
            return error_response(desc="Something went wrong. API-response: {} - {}".format(tweets.data['status'], tweets.data['detail'])), error_http_code()
        else:
            return error_response(desc="Something went wrong. API did not return a JSON-formatted file."), error_http_code()

    newest_id = ''
    tweet_count = 0

    if json_response['meta']['result_count'] > 0:
        # save "newest_id" to be used in the next request as "since_id"
        newest_id = json_response['meta']['newest_id']
        tweet_count = json_response['meta']['result_count']

        # handle first page received
        DataProcessor.process_new_data(request_info, json_response, db, config.mongodb)

        # if more pages exists, download & handle additional pages
        while 'next_token' in json_response['meta']:
            tweets.next_token = json_response['meta']['next_token']
            tweets.getdata()
            json_response = tweets.data
            if json_response['meta']['result_count'] > 0:
                tweet_count = tweet_count + json_response['meta']['result_count']
                DataProcessor.process_new_data(request_info, json_response, db, config.mongodb)

    if not newest_id:
        update_db_query = { "$set": {"last_pull": datetime.now()}}
    else:
        update_db_query = { "$set": {"since_id": newest_id, "last_pull": datetime.now()}}

    api_requests.find_one_and_update(request_info, update_db_query)

    ## 2nd step
    ## refresh public metrics of existing tweets
    tweets_maintained = 0

    db_tweets = db[config.mongodb['tweets_collection']]
    maintenance_from = datetime.today() - timedelta(hours=request['maintenance_delta'])
    oldest_tweet_to_maintain = db_tweets.find_one(filter={"requests": request['name'], "id": { "$lt": request['since_id'] }, "created_at": { "$gte": maintenance_from.isoformat() }},
                                                  sort=[("id", ASCENDING)]) # ASCENDING comes from pymongo
    
    ## could be that the maintenance-period doesn't contain any tweets
    if oldest_tweet_to_maintain:
        ## the tweets object is still warm
        tweets.since_tweet_id = oldest_tweet_to_maintain['id']
        tweets.until_tweet_id = request['since_id']
        tweets.next_token = None
        tweets.getdata()

        if tweets.is_json:
            json_response = tweets.data

        if json_response['meta']['result_count'] > 0:
            tweets_maintained = json_response['meta']['result_count']

            DataProcessor.update_existing_data(json_response, request_info['name'], db, config.mongodb)

            while 'next_token' in json_response['meta']:
                tweets.next_token = json_response['meta']['next_token']
                tweets.getdata()
                json_response = tweets.data
                if json_response['meta']['result_count'] > 0:
                    tweets_maintained = tweets_maintained + json_response['meta']['result_count']
                    DataProcessor.update_existing_data(json_response, request_info['name'], db, config.mongodb)

    return { "success": True, "tweets_new": tweet_count, "tweets_maintained": tweets_maintained }, 200

if __name__ == "__main__":
    app.run()