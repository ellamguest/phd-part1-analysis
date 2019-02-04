import requests
import time
import json
import credentials
import pandas as pd

username = credentials.login['username']

#set username and password values
username = credentials.login['username']
password = credentials.login['password']
useragent = credentials.login['useragent']

#create dict with username and password
user_pass_dict = {'user': username,
                  'passwd': password,
                  'api_type': 'json'}

#set the header for all the following requests
headers = {'user-agent': useragent, }

#create a requests.session that'll handle our cookies for us
client = requests.session()
client.headers=headers

#make a login request, passing in the user and pass as data
r = client.post(r'http://www.reddit.com/api/login', data=user_pass_dict)

#optional print to confirm error-free response
#pprint(r.content)
 
#turns the response's JSON to a native python dict
j = json.loads(r.content)
 
#grabs the modhash from the response
client.modhash = j['json']['data']['modhash']
 
#prints the users modhash
print('{USER}\'s modhash is: {mh}'.format(USER=username, mh=client.modhash))


import requests
import requests.auth

def accessToken():
    client_auth = requests.auth.HTTPBasicAuth('uIwyxbv96Ymmjw', 'mi2PCzdAgcklpWfkx3xTeeNmJaQ')
    post_data = {"grant_type": "password", "username": username, "password": password}
    headers = {"User-Agent": useragent}
    response = requests.post("https://www.reddit.com/api/v1/access_token", auth=client_auth, data=post_data, headers=headers)
    j = response.json()

    return j['access_token']

def subredditData(subreddit, token):
    headers = {"Authorization": f"""bearer {token}""", "User-Agent": useragent}
    response = requests.get(f"""https://oauth.reddit.com/r/{subreddit}/about""", headers=headers)
    j = response.json()
    try:
        return j['data']
    except:
        return j
    

def run():
    token = accessToken()
    subreddits = df.index

    results = {}

    n = 1
    for subreddit in subreddits:
        if subreddit not in results.keys():
            data = subredditData(subreddit, token)
            results[subreddit] = data

        if n%10==0:
            print(n)

        n+=1

    return pd.DataFrame(results).T