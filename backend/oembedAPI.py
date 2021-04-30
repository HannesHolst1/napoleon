import aiohttp
import asyncio
import urllib

async def get_embedded_html(tweets, config):
    if 'data' in tweets:

            async with aiohttp.ClientSession() as session:

                tasks = []
                for tweet in tweets['data']:
                    tweet_requested = config.twitter['twitter_url_root'] + tweet['author'][0]['username'] + '/status/' + tweet['id']
                    url = config.twitter['oembed'] + urllib.parse.quote(tweet_requested)
                    tasks.append(asyncio.ensure_future(get_data(session, url, tweet)))

                await asyncio.gather(*tasks)


async def get_data(session, url, tweet):
    async with session.get(url) as resp:
        if resp.ok:
            response_content = await resp.json()
            try:
                tweet['tweet_html'] = response_content['html']
            except:
                tweet['tweet_html'] = str(resp.status) + ': ' + resp.reason + ' - error loading tweet ' + url
        else:
            tweet['tweet_html'] = str(resp.status) + ': ' + resp.reason + ' - error loading tweet ' + url
