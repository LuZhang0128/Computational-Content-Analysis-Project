#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @Time : 2020/7/17 16:33
# @Author : ljx
# @Version：V 0.1
# @File : user_twitter.py
import datetime
import random
import time
import urllib

import pymysql
import requests
from ODtools import RedisClient
from dateparser.search import search_dates

proxies = {'http': 'socks5://127.0.0.1:10808', "https": "socks5://127.0.0.1:10808"}

#
# def get_queues():
#     rds = RedisClient(host="192.168.129.211", port=6380).redis
#     return rds

def get_queues():
    rds = RedisClient(host="127.0.0.1", port=6379).redis
    return rds

save_fas = get_queues()
# save_fas = get_queues()

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2 ",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.4.3.4000 Chrome/30.0.1599.101 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
    "Opera/12.80 (Windows NT 5.1; U; en) Presto/2.10.289 Version/12.02",
]

conn = pymysql.connect(host='', user='', passwd='', db='',
                       port=3306, charset='utf8',
                       cursorclass=pymysql.cursors.DictCursor)
cur = conn.cursor()


def get_spider_time():
    g_spider_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    return g_spider_time


def get_time(a):
    a = a.replace("+0000", "")
    time_str = search_dates(a)
    times = time_str[0][-1]
    print(times, type(times))
    publish_time_end = (times + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    return publish_time_end


def get_session():
    s = requests.session()
    s.keep_alive = False
    s.proxies = proxies
    s.allow_redirects = False
    s.verify = False
    return s


def get_token():
    while True:
        try:
            with open('token.txt', 'r') as f:
                tokens = f.read()
            return tokens
        except Exception as e:
            print("获取token出错:", e)


def get_html(url):
    s = get_session()
    # print("第一次获取到的tokens：",tokens)
    num = 0
    while num<50:
        try:
            num += 1
            tokens = get_token()
            headers = {
                'Referer': 'https://twitter.com/algore',
                'Origin': 'https://twitter.com',
                'User-Agent': random.choice(user_agents),
                'x-guest-token': tokens,
                'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA'
            }
            print("开始请求url", url)
            """{'errors': [{'code': 200, 'message': 'Forbidden.'}]},Rate limit exceeded"""
            """{'errors': [{'code': 34, 'message': 'Sorry, that page does not exist.'}]}"""
            r = s.get(url, headers=headers, proxies=proxies, timeout=20)
            res = str(r.json())
            if "Rate limit exceeded" in res or 'Forbidden' in res or "Sorry, that page does not exist" in res:
                s = get_session()
                continue
            else:
                return r
        except Exception as e:
            s = get_session()
            print(e)
            tokens = get_token()


def get_twitter_info(tim, threeDayAgosss, j):
    print("!!!!!!!!!!!!!!!!!!!!!!!!", tim, threeDayAgosss)
    cursors = ''
    num = 0
    while num < 500:
        num += 1
        try:
            url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&send_error_codes=true&simple_quoted_tweet=true&q=(%23BLM)%20lang%3Aen%20until%3A{}%20since%3A{}&count=20&query_source=typed_query{}&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CsuperFollowMetadata'.format(
                  tim, threeDayAgosss,cursors)
            # url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&send_error_codes=true&simple_quoted_tweet=true&q=(%23BLM)%20lang%3Aen%20until%3A{}%20since%3A{}&count=20&query_source=typed_query{}&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CsuperFollowMetadata'
            res = get_html(url)
            res_dict = res.json()
            # print(res_dict)
            if res_dict['globalObjects']['tweets']:
                print("aaaa")
                get_twitter_article(res_dict['globalObjects'],j)
                try:
                    if res_dict['timeline']['instructions'][0]['addEntries']['entries'][-1]:
                        page_next = res_dict['timeline']['instructions'][0]['addEntries']['entries'][-1]
                        print(page_next)
                        cursor = page_next['content']['operation']['cursor']['value']
                        print(cursor)
                        cursor = urllib.parse.quote(cursor)
                        cursors = "&cursor={}".format(cursor)
                except Exception as e:
                    print("获取下一页参数出错", e)
                    break
            else:
                break
        except Exception as e:

            print("下一页出错:", e)
            break


def get_twitter_article(data,keyword):
    try:
        if data:
            for key, i in data['tweets'].items():
                print('------------------------------')
                info = {}
                forward_info = {}
                user = data['users']
                info['m_content_location'] = ''
                try:
                    m_parent_id = i['retweeted_status_id_str']
                    info['m_parent_id'] = m_parent_id
                    forward_info['r_parent_id'] = m_parent_id
                except:
                    info['m_parent_id'] = ''
                    forward_info['r_parent_id'] = ''

                # 转发推文
                if info['m_parent_id']:
                    # 转发推文的url
                    # m_parent_url = '{}/status/'.format(user_url) + info['m_parent_id']
                    # info['m_parent_url'] = m_parent_url
                    # print("转发推文的url，",m_parent_url)
                    # 转发推文的内容
                    m_parent_content = i['full_text']
                    info['m_parent_content'] = m_parent_content
                    # print("转发推文的内容,",m_parent_content)
                    r_is_trans = 1
                    info['r_is_trans'] = r_is_trans
                    print(r_is_trans)
                    info['m_content'] = ''
                else:
                    # r_is_trans = 0
                    info['r_is_trans'] = 0
                    m_content = i['full_text']
                    info['m_content'] = m_content
                    # print("正文内容:",m_content)
                try:
                    # 是否是转发并评论
                    quoted_status_id_str = i['quoted_status_id_str']
                    info['quoted_status_id_str'] = quoted_status_id_str
                except:
                    info['quoted_status_id_str'] = ''
                try:
                    if info['quoted_status_id_str'] and info['m_parent_id'] == '':
                        # 是否是转发并评论
                        r_is_comment_replay = 1
                        info['r_is_comment_replay'] = r_is_comment_replay
                        # print(r_is_comment_replay)
                        m_parent_id = i['quoted_status_id_str']
                        info['m_parent_id'] = m_parent_id
                        forward_info['r_parent_id'] = m_parent_id
                        # 转发推文的url
                        # m_parent_url = '{}/status/'.format(user_url) + m_parent_id
                        # info['m_parent_url'] = m_parent_url
                        # 转发推文的内容
                        m_parent_content = data['tweets'][m_parent_id]['full_text']
                        info['m_parent_content'] = m_parent_content
                        # print("转发并评论的内容",m_parent_content)
                        r_is_trans = 0
                        info['r_is_trans'] = 0
                        m_content = ''
                        info['m_content'] = m_content
                    else:
                        r_is_comment_replay = 0
                        info['r_is_comment_replay'] = r_is_comment_replay
                except:
                    pass
                # 推文的url
                # m_content_url = '{}/status/'.format(user_url) + i['id_str']
                # info['m_content_url'] = m_content_url
                # forward_info['m_content_url'] = m_content_url
                # 推文的id
                m_content_id = i['conversation_id_str']
                info['m_mid'] = m_content_id
                info['m_content_id'] = m_content_id
                info['rowkey'] = m_content_id
                forward_info['m_content_id'] = m_content_id
                forward_info['rowkey'] = forward_info['m_content_id'] + '-' + info['m_parent_id']
                # print(m_content_id)
                # # 推文的正文
                # m_content = i['full_text']
                # print(m_content)
                # 推文图片地址
                try:
                    m_images_lis = i['extended_entities']['media']
                    img_list = []
                    for j in m_images_lis:
                        img_list.append(j['media_url'])
                    info['m_images'] = img_list
                except:
                    info['m_images'] = ''
                # 推文视频地址
                try:
                    m_videos = i['entities']['media'][0]['expanded_url']
                    if "video" in m_videos:
                        info['m_videos'] = m_videos
                    else:
                        info['m_videos'] = ''

                except:
                    info['m_videos'] = ''
                # 视频时长
                m_video_length = ''
                info['m_video_length'] = m_video_length
                # 视频的封面
                try:
                    m_cover = i['entities']['media']['media_url']
                    info['m_cover'] = ''
                    print(m_cover)
                except:
                    m_cover = ''
                    info['m_cover'] = m_cover
                # 推文的音频地址
                m_audios = ''
                info['m_audios'] = m_audios
                # 评论数量
                try:
                    r_comment_num = i['reply_count']
                    info['r_comment_num'] = r_comment_num
                    print(r_comment_num)
                except:
                    info['r_comment_num'] = ''
                # 推文发布时间 英文时间
                try:
                    print(i['created_at'])
                    g_publish_time = get_time(i['created_at'])
                    # g_publish_time = i['created_at']
                    info['g_publish_time'] = g_publish_time.split(" ")[0]
                    forward_info['g_publish_time'] = g_publish_time
                    print(g_publish_time)
                except:
                    info['g_publish_time'] = ''
                    forward_info['g_publish_time'] = ''
                forward_info['m_is_remove'] = 1
                # 喜欢数量
                try:
                    r_like_num = i['favorite_count']
                    info['r_like_num'] = r_like_num
                    print(r_like_num)
                except:
                    info['r_praised_num'] = ''
                # 转推数量
                try:
                    r_trans_num = i['retweet_count']
                    print(r_trans_num)
                    info['r_trans_num'] = r_trans_num
                except:
                    info['r_trans_num'] = ''
                # 推文是否有效
                # 用户id
                try:
                    user_id_str = i['user_id_str']
                    info['u_id'] = user_id_str
                except:
                    info['u_id'] = ''
                try:
                    info['in_reply_to_screen_name'] = i['in_reply_to_screen_name']
                except:
                    info[''] = ''
                try:
                    info['in_reply_to_status_id'] = i['in_reply_to_status_id']
                except:
                    info['in_reply_to_status_id'] = ''
                # 爬取时间
                user_info = user.get(info['u_id'],'')
                info['screen_name'] = user_info['screen_name']
                info['u_nickname'] = user_info['name']
                info['u_area'] = user_info['location']

                print(info)

                try:
                    insert_info_sql = """insert ignore twitter_info_reply(u_id,m_content,m_content_id,m_images,m_videos,r_comment_num,g_publish_time,r_like_num,r_trans_num,keyword,in_reply_to_screen_name,in_reply_to_status_id,u_nickname,u_area,screen_name) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                    if info and info['m_content']:
                        return_info_result = []
                        return_info_result.append(
                            (info['u_id'], info['m_content'],
                             info['m_content_id'], str(info['m_images']), info['m_videos'],
                             info['r_comment_num'], info['g_publish_time'], str(info['r_like_num']),
                             str(info['r_trans_num']), keyword,info['in_reply_to_screen_name'],str(info['in_reply_to_status_id']),info['u_nickname'],info['u_area'],info['screen_name'])
                        )
                        cur.executemany(insert_info_sql, return_info_result)
                        conn.commit()
                        print("存储mysql成功")
                except Exception as e:
                    import traceback
                    traceback.print_exc()

                save_fas.sadd("twitter_comment_tj:ss", m_content_id)
            for key, value in data['users'].items():
                user_info = {}

                # 用户ID
                u_id = value['screen_name']
                user_info['u_id'] = u_id
                user_info['uu_id'] = value['id']
                user_info['id_str'] = value['id_str']
                # 用户地址
                u_url = 'https://twitter.com/' + u_id
                user_info['u_url'] = u_url
                # 用户昵称
                u_nickname = '' + value['name']
                user_info['u_nickname'] = u_nickname
                # 头像地址
                u_logo = value['profile_image_url_https']
                user_info['u_logo'] = u_logo
                # 所在地
                u_area = value['location']
                user_info['u_area'] = u_area
                # 出生日期
                u_birthday = ''
                user_info['u_birthday'] = u_birthday
                # 加入twitter的时间
                try:
                    u_login_time = value['created_at']
                    user_info['u_login_time'] = u_login_time
                except:
                    user_info['u_login_time'] = ''
                # 个人网址
                try:
                    u_relation_url = value['entities']['url'][0]['expanded_url']
                    user_info['u_relation_url'] = u_relation_url
                except:
                    user_info['u_relation_url'] = ''
                try:
                    # 个人简介
                    u_introduction = value['description']
                    user_info['u_introduction'] = u_introduction
                except:
                    user_info['u_introduction'] = ''
                # 关注数
                r_follow_num = value['friends_count']
                user_info['r_follow_num'] = r_follow_num
                # 推文发布数
                try:
                    m_publish_num = value['m_publish_num']
                    user_info['m_publish_num'] = m_publish_num
                except:
                    user_info['m_publish_num'] = ''
                # 点赞数
                r_like_num = ''
                user_info['r_like_num'] = r_like_num
                try:
                    user_info['verified'] = value['verified']
                except:
                    user_info['verified'] = 0
                # 用户是否有效
                m_is_remove = ''
                user_info['m_is_remove'] = m_is_remove
                # 粉丝数
                r_fans_num = value['followers_count']
                user_info['r_fans_num'] = r_fans_num
                # 爬取时间
                g_spider_time = get_spider_time()
                user_info['g_spider_time'] = g_spider_time
                user_info['g_update_time'] = g_spider_time
                user_info['rowkey'] = user_info['u_id']
                user_info['m_project_name'] = "bjws11"
                user_info['u_is_remove'] = ''
                user_info['m_data_source'] = 'twitter'
                insert_user_sql = """insert ignore twitter_user_copy1_copy1(id_str,uu_id,u_id,u_url,u_nickname,u_logo,u_area,u_birthday,u_login_time,u_relation_url,u_introduction,r_follow_num,m_publish_num,r_like_num,m_is_remove,r_fans_num,verified) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

                if user_info:
                    return_user_result = []
                    return_user_result.append(
                        (user_info['id_str'], user_info['uu_id'], user_info['u_id'], user_info['u_url'],
                         user_info['u_nickname'], user_info['u_logo'],
                         user_info['u_area'],
                         user_info['u_birthday'], user_info['u_login_time'], user_info['u_relation_url'],
                         user_info['u_introduction'], user_info['r_follow_num'], user_info['m_publish_num'],
                         user_info['r_like_num'], user_info['m_is_remove'], user_info['r_fans_num'],
                         user_info['verified'])
                    )
                    cur.executemany(insert_user_sql, return_user_result)
                    conn.commit()


        else:
            print("最后一页")
    except Exception as e:
        traceback.print_exc()
        print(e)


def time_end_start(i,start_time):
    # threeDayAgo = (datetime.datetime.now() - datetime.timedelta(days=8))
    # # 转换为时间戳
    # # timeStamp = int(time.mktime(threeDayAgo.timetuple()))
    # # 转换为其他字符串格式
    # otherStyleTime = threeDayAgo.strftime("%Y-%m-%d")
    # timeArray_after = time.strptime(otherStyleTime, "%Y-%m-%d")
    # start_time = int(time.mktime(timeArray_after))
    aaa = datetime.datetime.strptime(start_time, '%Y-%m-%d')
    threeDayAgo = (aaa + datetime.timedelta(days=i))
    threeDayAgosss = (threeDayAgo - datetime.timedelta(days=1))
    return threeDayAgo, threeDayAgosss

"""https://twitter.com/search?f=top&q=%23Ford%20lang%3Aen%20until%3A2021-12-14%20since%3A2010-01-01&src=typed_query"""
"""
Axie Infinity  AxieInfinity
TheSandboxGame TheSandboxGame
decentraland decentraland
mobox  MOBOX_Official
wemix  WemixNetwork
galagames galagames_fc

"""
def run():
    lis = ['BLM']
    start_time = "2008-10-15"
    end_time = "2022-02-15"
    d1 = datetime.datetime.strptime(start_time, '%Y-%m-%d')
    d2 = datetime.datetime.strptime(end_time, '%Y-%m-%d')
    delta = d2 - d1
    ccc = delta.days
    print(ccc)
    for i in range(0, int(ccc)+1):
        tim, threeDayAgosss = time_end_start(i,start_time)
        tim = str(tim).replace("00:00:00", "").replace(" ", "")
        threeDayAgosss = str(threeDayAgosss).replace("00:00:00", "").replace(" ", "")
        print(tim)
        if tim:
            get_token()
            for j in lis:
                get_twitter_info(tim, threeDayAgosss, j)
        else:
            time.sleep(60)

    # for user_info in user_infos:77
    #     if user_info:
    #         get_token()
    #         time.sleep(random.randint(3, 5))
    #         get_twitter_info(user_info)
    #     else:
    #         time.sleep(60)


if __name__ == '__main__':
    run()
