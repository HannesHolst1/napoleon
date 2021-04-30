from datetime import datetime
from pymongo import UpdateOne, MongoClient
from backend import config
from backend import oembedAPI
import asyncio
from zappa.asynchronous import task

def calc_tweet_score(tweet):
    tweet_score = 0
    if 'public_metrics' in tweet:
        tweet_score = (tweet['public_metrics']['like_count'] *2) + (tweet['public_metrics']['reply_count'] * 2) + tweet['public_metrics']['retweet_count'] + tweet['public_metrics']['quote_count']
    return tweet_score

def calc_synergy(tweet):
    synergy = 0
    if 'created_at' and 'public_metrics' in tweet:
        synergy = datetime.now() - datetime.strptime(tweet['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ')
        synergy = synergy.total_seconds() / 100
        if tweet['public_metrics']['like_count'] > 0:
            synergy = synergy - (tweet['public_metrics']['like_count'] / 4)

        if tweet['public_metrics']['reply_count'] > 0:
            synergy = synergy - (tweet['public_metrics']['reply_count'] * 250)

        if (tweet['public_metrics']['reply_count'] + tweet['public_metrics']['quote_count']) > 0:
            synergy = synergy - ((tweet['public_metrics']['reply_count'] + tweet['public_metrics']['quote_count']) * 100)

    return synergy
 
def move_include_into_data(json):
    '''
        The enriched information about media and user is not part of the data-array that contains the Tweet. 
        To avoid joins in the database later, the media- and user-arrays are moved into the corresponding data-array.
        The data-array is stored in the database.
    '''
    if not 'includes' in json:
        return

    if 'media' in json['includes']:
        for includes_element in json['includes']['media']:
            for data_element in json['data']:
                if 'attachments' in data_element:
                    if includes_element['media_key'] in data_element['attachments']['media_keys']:
                        if not 'media' in data_element:
                            data_element['media'] = []

                        data_element['media'].append(includes_element)

    if 'users' in json['includes']:
        for includes_element in json['includes']['users']:
            for data_element in json['data']:
                if includes_element['id'] in data_element['author_id']:
                    if not 'author' in data_element:
                        data_element['author'] = []

                    data_element['author'].append(includes_element)         

    return

def copy_tweethashtags_into_media(json):
    if not 'includes' in json:
        return

    if not 'media' in json['includes']:
        return

    if not 'data' in json:
        return

    for data_element in json['data']:
        if ('attachments' in data_element) and ('hashtags' in data_element['entities']):
            for media_key in data_element['attachments']['media_keys']:
                for media_element in json['includes']['media']:
                    if media_element['media_key'] == media_key:
                        media_element['hashtags'] = [tag['tag'] for tag in data_element['entities']['hashtags']]

@task
def update_user_statistics(newest_id, oldest_id, request_name, db_details):
    '''
        This function takes a tweet and will increase the statistics-count in the user-document.
        It is important to consider that this function should only be used with NEW tweets.
        For tweets that are downloaded multiple times, please use the function "update_existing_data()" to update the user-document.
    '''
    print(f'update_user_statistics - start: {datetime.now()}')
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
            score = calc_tweet_score(tweet)
            keyvalue = 'tweet_scores.'+request_name

            # create entry in the bulk_write-dicts that is used to do the database-transaction outside this loop
            user_bulk_request.append(UpdateOne({ "id": tweet['author_id']}, { "$inc": { keyvalue: score }, "$set": updateset }))
            tweet_bulk_request.append(UpdateOne({ "id": tweet['id']}, {"$set": {'user_stat_update': datetime.now()}}))

    if user_bulk_request:
        users.bulk_write(user_bulk_request)

    if tweet_bulk_request:
        tweets_collection.bulk_write(tweet_bulk_request)

    print(f'update_user_statistics - end: {datetime.now()}')

def add_request_name(json, request_name):
    '''
        Add the name of the request-config to the json-array.
        This is done to be able to query the database better. 
    '''
    new_entry = [request_name]
    for element in json:
        element['requests'] = new_entry

    return

def update_existing_data(tweets, request_name, database, db_details):
    '''
        This function will take the payload data to update existing tweets.
        This is useful to update the public metrics (likes, replies etc.) of a tweet.
        The tweet_score in the user-document will also be updated.
    '''
    if 'data' in tweets:
        tweets_db = database[db_details['tweets_collection']]
        users = database[db_details['users_collection']]
        tweet_bulk = []
        user_bulk = []
        for tweet in tweets['data']:
            existing_tweet = tweets_db.find_one({"id": tweet['id']})

            if not 'tweet_score' in existing_tweet:
                existing_tweet['tweet_score'] = 0

            new_score = calc_tweet_score(tweet)

            if existing_tweet['tweet_score'] != new_score:
                score_diff = new_score - existing_tweet['tweet_score']
                if 'author_id' in tweet:
                    keyvalue = 'tweet_scores.'+request_name
                    user_bulk.append(UpdateOne({ "id": tweet['author_id']}, { "$inc": { keyvalue: score_diff } }))

            tweet['tweet_score'] = new_score

            tweet['synergy'] = calc_synergy(tweet)

            tweet_bulk.append(UpdateOne({ "id": tweet['id']}, { "$set": tweet }, upsert=True))

        if tweet_bulk:
            tweets_db.bulk_write(tweet_bulk)

        if user_bulk:
            users.bulk_write(user_bulk)

def process_new_data(request_info, json, database, db_details):
    '''
        This function takes the whole payload received from the Twitter-API and processes it for the dashboard.
    '''
    if 'users' in json['includes']:
        print('debug: new tweets - handle new users - {}'.format(datetime.now()))
        users = database[db_details['users_collection']]
        bulk_request = []
        for user in json['includes']['users']:
            bulk_request.append(UpdateOne({ "id": user['id']}, { "$set": user }, upsert=True))

        print('debug: new tweets - start write new users - {}'.format(datetime.now()))
        users.bulk_write(bulk_request)
        print('debug: new tweets - end write new users - {}'.format(datetime.now()))

    if 'data' in json:
        # add_request_name(json['data'], request_info['name'])
        print('debug: new tweets - move_include_into_data - {}'.format(datetime.now()))
        move_include_into_data(json)

        tweets = database[db_details['tweets_collection']]
        bulk_request = []
        print(f'debug: new tweets - oembedAPI start - {datetime.now()}')
        asyncio.run(oembedAPI.get_embedded_html(json, config))
        print(f'debug: new tweets - oembedAPI end - {datetime.now()}')
        print('debug: new tweets - handle new tweets - {}'.format(datetime.now()))
        for tweet in json['data']:
            tweet['tweet_score'] = calc_tweet_score(tweet)
            tweet['synergy'] = calc_synergy(tweet)

            bulk_request.append(UpdateOne({ "id": tweet['id']}, { "$set": tweet}, upsert=True))
            bulk_request.append(UpdateOne({ "id": tweet['id']}, {"$addToSet": { "requests": request_info['name'] } }))
            #{$addToSet: {"requests":"something new"}

        print('debug: new tweets - start write new tweets - {}'.format(datetime.now()))
        tweets.bulk_write(bulk_request)
        print('debug: new tweets - end write new tweets - {}'.format(datetime.now()))

        print('debug: new tweets - start execute update_user_statistics - {}'.format(datetime.now()))
        update_user_statistics(json['meta']['newest_id'], json['meta']['oldest_id'], request_info['name'], db_details)
        print('debug: new tweets - end execute update_user_statistics - {}'.format(datetime.now()))

    if 'media' in json['includes']:
        # add_request_name(json['includes']['media'], request_info['name'])
        copy_tweethashtags_into_media(json)

        media = database[db_details['media_collection']]
        bulk_request = []
        for media_element in json['includes']['media']:
            bulk_request.append(UpdateOne({ "media_key": media_element['media_key']}, { "$set": media_element }, upsert=True))
            bulk_request.append(UpdateOne({ "media_key": media_element['media_key']}, {"$addToSet": { "requests": request_info['name'] } }))

        media.bulk_write(bulk_request)

def main():
    print('Test OK')

if __name__ == '__main__':
    main()
