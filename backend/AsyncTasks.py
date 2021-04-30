from zappa.asynchronous import task
from datetime import datetime
from pymongo import UpdateOne, MongoClient
from backend import DataProcessor, TwitterAPI, config

@task
def update_user_statistics(newest_id, oldest_id, request_name, db_details):
    '''
        This function takes a tweet and will increase the statistics-count in the user-document.
        It is important to consider that this function should only be used with NEW tweets.
        For tweets that are downloaded multiple times, please use the function "update_existing_data()" to update the user-document.
    '''
    client = MongoClient(db_details['uri'])
    database = client[db_details['database']]    
    users = database[db_details['users_collection']]
    tweets_collection = database[db_details['tweets_collection']]

    tweets = tweets_collection.find({'id': {'$gte': oldest_id, '$lte': newest_id}, 'requests': request_name, 'user_stat_update': {'$exists': False}})

    user_bulk_request = []
    tweet_bulk_request = []
    for tweet in tweets:
        hashtags_udpated = set()
        requests_udpated = set()
        new_user_hashtags = list()
        new_request_list = list()
        updateset = {}

        ## get current data of user from database
        if 'author_id' in tweet:
            user = users.find_one({ "id": tweet['author_id'] })      

        ## step 1: add request_name to request-list
        if user:

            if 'requests' in user:
                for request in user['requests']:
                    if request['name'] == request_name:
                        new_request_list.append({"name": request_name, "count": request['count'] + 1, "last_used": tweet['created_at']})
                        requests_udpated.add(request['name'])
                    else:
                        new_request_list.append(request)
                        requests_udpated.add(request['name'])

                if not request_name in requests_udpated:
                    new_request_list.append({"name": request_name, "count": 1, "last_used": tweet['created_at']})    
            else:
                new_request_list.append({"name": request_name, "count": 1, "last_used": tweet['created_at']})

            # add data to the set-dict that we use to bulk-update later
            updateset['requests'] = new_request_list

            ## step 2: update hashtag statistics in user-document
            if 'entities' in tweet:
                if 'hashtags' in tweet['entities']:
                    # when here, we have hashtags in the tweet-data

                    for tag_data in tweet['entities']['hashtags']:
                        # first, update all already existing hashtags that are assigned to a user
                        if not tag_data['tag'] in hashtags_udpated:
                            if 'hashtags' in user:
                                for user_hashtag in user['hashtags']:
                                    if user_hashtag['name'] == tag_data['tag']:
                                        new_user_hashtags.append({"name": user_hashtag['name'], "count": user_hashtag['count'] + 1, "last_used": tweet['created_at']})
                                        hashtags_udpated.add(tag_data['tag'])
                                        break

                        # second, assign new hashtags to user
                        if not tag_data['tag'] in hashtags_udpated:
                            new_user_hashtags.append({"name": tag_data['tag'], "count": 1, "last_used": tweet['created_at']})
                            hashtags_udpated.add(tag_data['tag'])

                    # add data to the set-dict that we use to bulk-update later
                    updateset['hashtags'] = new_user_hashtags

            ## step 3: update total tweet_score
            score = DataProcessor.calc_tweet_score(tweet)
            keyvalue = 'tweet_scores.'+request_name

            # create entry in the bulk_write-dicts that is used to do the database-transaction outside this loop
            user_bulk_request.append(UpdateOne({ "id": tweet['author_id']}, { "$inc": { keyvalue: score }, "$set": updateset }))
            tweet_bulk_request.append(UpdateOne({ "id": tweet['id']}, {"$set": {'user_stat_update': datetime.now()}}))

    if user_bulk_request:
        users.bulk_write(user_bulk_request)

    if tweet_bulk_request:
        tweets_collection.bulk_write(tweet_bulk_request)

@task
def download_more_tweets(request_info, api_bearer, api_tweet_search_uri, api_search_query, api_search_parameters, api_since_tweet_id, api_next_token, api_tweet_count):
    # step one: init new request to twitter-API with "next_token"
    client = MongoClient(config.mongodb['uri'])
    db = client[config.mongodb['database']]
    api_requests = db[config.mongodb['request_collection']]

    current_request = api_requests.find_one(request_info)
    # below is an emergency kill switch in case the recursive Lambda-invocations do get out of control
    if 'kill_download' in current_request:
        if current_request['kill_download']:
            print(f"download killed at {datetime.now()}")
            return {}

    tweets = TwitterAPI.Tweets(bearer=api_bearer, tweet_search_uri=api_tweet_search_uri, 
                               search_query=api_search_query, search_parameters=api_search_parameters, 
                               since_tweet_id=api_since_tweet_id, next_token=api_next_token)
     
    tweets.getdata()
    
    if tweets.is_success:
        if tweets.is_json:
            json_response = tweets.data
        else:
            print("Something went wrong. API did not return a JSON-formatted file.")
            return {}

    if tweets.is_error:
        print(tweets.data)
        if ('detail' in tweets.data) and ('status' in tweets.data):
            print("Something went wrong. API-response: {} - {}".format(tweets.data['status'], tweets.data['detail']))
            return {}
        else:
            print("Something went wrong. API did not return a JSON-formatted file.")
            return {}

    continue_download_cycle = True
    if 'max_results' in current_request:
        if (api_tweet_count + json_response['meta']['result_count']) >= current_request['max_results']:
            continue_download_cycle = False

    if not 'next_token' in json_response['meta']:
        continue_download_cycle = False

    # step two: check if new next_token exists, if yes call download_more_tweets() again
    if continue_download_cycle:
        download_more_tweets(request_info, 
                            api_bearer, 
                            api_tweet_search_uri, 
                            api_search_query, 
                            api_search_parameters, 
                            api_since_tweet_id, 
                            api_next_token=json_response['meta']['next_token'],
                            api_tweet_count=api_tweet_count + json_response['meta']['result_count'])

    # step three: process_new_data
    if json_response['meta']['result_count'] > 0:
        DataProcessor.process_new_data(request_info, json_response, config.mongodb)

    # step four: wrap up activities
    if not continue_download_cycle:
        api_requests.find_one_and_update(request_info, {"$set": {'active': False, 'last_pull_tweets_downloaded': api_tweet_count + json_response['meta']['result_count'], 'last_pull_finished': datetime.now()}})