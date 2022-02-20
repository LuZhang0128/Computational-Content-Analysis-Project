import json
import re
import time
import traceback
import requests


def run():
    while True:
        try:
            proxies = {'http': 'socks5://127.0.0.1:10808', "https": "socks5://127.0.0.1:10808"}
            url_token = 'https://api.twitter.com/1.1/guest/activate.json'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0',
                'Accept': '*/*',
                'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                'x-guest-token': '',
                'x-twitter-client-language': 'zh-cn',
                'x-twitter-active-user': 'yes',
                'x-csrf-token': '25ea9d09196a6ba850201d47d7e75733',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
                'Referer': 'https://twitter.com/',
                'Connection': 'keep-alive',
            }
            token = json.loads(requests.post(url_token, headers=headers, proxies=proxies,verify=False).text)['guest_token']
            if token:
                with open('token.txt', 'w') as f:
                    f.write(token)
                print(token)
                time.sleep(10)

        except Exception as e:
            print("获取token出错:", e)


if __name__ == '__main__':
    run()