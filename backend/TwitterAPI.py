import requests
import urllib.parse

class Tweets:
    class RateLimit:
        def __init__(self) -> None:
            self.EndpointLimit = None
            self.Remaining = None
            self.ResetIn = None

        def toJSON(self):
            return {'EndpointLimit': self.EndpointLimit, 'Remaining': self.Remaining, 'ResetIn': self.ResetIn}

    def __init__(self, bearer=None, tweet_search_uri=None, search_query=None, search_parameters=None, since_tweet_id=None, until_tweet_id=None, next_token=None) -> None:
        self.bearer = bearer
        self.tweet_search_uri = tweet_search_uri
        self.search_query = search_query
        self.parameters = search_parameters
        self.since_tweet_id = since_tweet_id
        self.until_tweet_id = until_tweet_id
        self.next_token = next_token
        self.url = None
        self.data = None
        self.is_json = False
        self.is_error = False
        self.is_success = False
        self.ratelimit = self.RateLimit()

    def create_headers(self):
        if not self.bearer:
            self.bearer = ''

        self.headers = {"Authorization": "Bearer {}".format(self.bearer)}

    def create_url(self):
        if not self.tweet_search_uri:
            raise Exception('The Twitter Search URI is missing. Please set as "tweet_search_uri"-attribute.')

        url_parameter = None

        if self.search_query:
            url_parameter = "query=" + urllib.parse.quote(self.search_query)

        if self.parameters:
            url_parameter = url_parameter + "&" + self.parameters

        if self.since_tweet_id:
            url_parameter = url_parameter + "&since_id=" + self.since_tweet_id

        if self.until_tweet_id:
            url_parameter = url_parameter + "&until_id=" + self.until_tweet_id

        if self.next_token:
            url_parameter = url_parameter + "&next_token=" + self.next_token

        if url_parameter:
            self.url = self.tweet_search_uri + url_parameter
        else:
            self.url = self.tweet_search_uri


    def connect_to_endpoint(self):
        if not self.headers:
            raise Exception('Headers missing. Set the headers first.')

        if not self.url:
            raise Exception('The URL is missing. Please execute "create_url()" first.')

        response = requests.request("GET", self.url, headers=self.headers)
        if response.status_code != 200:            
            self.is_error = True
            try:
                self.data = response.json()
                self.is_json = True
            except:
                self.data = response.text
        else:
            self.is_success = True
            try:
                self.data = response.json()
                self.is_json = True
            except:
                self.data = response.text

        if response.headers['x-rate-limit-limit']:
            self.ratelimit.EndpointLimit = response.headers['x-rate-limit-limit']

        if response.headers['x-rate-limit-remaining']:
            self.ratelimit.Remaining = response.headers['x-rate-limit-remaining']

        if response.headers['x-rate-limit-reset']:
            self.ratelimit.ResetIn = response.headers['x-rate-limit-reset']

    def getdata(self):
        self.create_headers()
        self.create_url()
        self.connect_to_endpoint()

def main():
    test = Tweets(tweet_search_uri='https://www.twitter.com/')
    test.getdata()
    print('Connection test: {}'.format(test.is_success))

if __name__ == "__main__":
    main()