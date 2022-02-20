#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @Time : 2020/7/17 16:33
# @Author : ljx
# @Version：V 0.1
# @File : user_twitter.py
import datetime
import random
import re
import time
import urllib
from concurrent.futures import ThreadPoolExecutor
import pymysql
import requests
from ODtools import user_agents, RedisClient
import requests
from redis import Redis
from tools import get_spider_time, get_time



proxies = {'http': 'socks5://127.0.0.1:10808', "https": "socks5://127.0.0.1:10808"}

def get_session():
    s = requests.session()
    s.keep_alive = False
    s.proxies = proxies
    s.allow_redirects = False
    s.verify = False
    return s

def get_queues():
    rds = RedisClient(host="127.0.0.1", port=6379).redis
    return rds

save_fas = get_queues()




def get_token():
    while True:
        try:
            with open('token.txt','r') as f:
                tokens = f.read()
            return tokens
        except Exception as e:
            print("获取token出错:",e)

def get_html(url):
    s = get_session()
    # print("第一次获取到的tokens：",tokens)
    num = 0
    while num<50:
        num += 1
        try:
            tokens = get_token()
            headers = {
                'Referer': 'https://twitter.com/akaihaato',
                'Origin': 'https://twitter.com',
                'User-Agent': random.choice(user_agents),
                'x-guest-token': tokens,
                'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA'
            }
            print("开始请求url",url)
            """{'errors': [{'code': 200, 'message': 'Forbidden.'}]}"""
            r = s.get(url,headers=headers,proxies=proxies,timeout=20)
            res = str(r.json())
            if "Rate limit exceeded" in res or 'Forbidden' in res:
                s = get_session()
                continue
            else:
                return r

        except Exception as e:
            print(e)
            tokens = get_token()

def get_twitter_info(article_id,conn,cur):
    cursors = ''
    while True:
        try:
            url = 'https://api.twitter.com/2/timeline/conversation/{}.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&count=20&referrer=tweet{}&ext=mediaStats%2ChighlightedLabel'.format(
                article_id,cursors)
            res = get_html(url)
            res_dict = res.json()
            if res_dict['globalObjects']:
                get_twitter_article(res_dict['globalObjects'],article_id,conn,cur)
                try:
                    try:
                        if res_dict['timeline']['instructions'][0]['addEntries']['entries'][-1]:
                            page_next = res_dict['timeline']['instructions'][0]['addEntries']['entries'][-1]
                            print(page_next)
                            cursor = page_next['content']['operation']['cursor']['value']
                            print(cursor)
                            cursor = urllib.parse.quote(cursor)
                            cursors = "&cursor={}".format(cursor)
                    except:
                        if res_dict['timeline']['instructions'][-1]['replaceEntry']['entry']:
                            page_next = \
                                res_dict['timeline']['instructions'][-1]['replaceEntry']['entry']['content']['operation'][
                                    'cursor']['value']
                            print(page_next)
                            cursor = page_next
                            print(cursor)
                            cursor = urllib.parse.quote(cursor)
                            cursors = "&cursor={}".format(cursor)
                except:
                    print("获取下一页参数出错", e)
                    break
            else:
                break
        except Exception as e:
            print("下一页出错:",e)
            break

def get_twitter_article(data,article_id,conn,cur):
    if data:

        for key,i in data['tweets'].items():
            try:
                info = {}
                print('------------------------------')
                # 原推文id
                m_origin_id = article_id
                info['r_origin_mid'] = m_origin_id
                # 用户id
                u_id = i['user_id_str']
                info['u_id'] = u_id
                info['u_url'] = 'https://twitter.com/' + u_id
                # 上级评论id
                try:
                    r_parent_id = i['in_reply_to_status_id_str']
                    info['u_origin_url'] = r_parent_id
                except:
                    info['u_origin_url'] = ''
                # 被回复或评论的用户ID
                try:
                    u_parent_id = i['in_reply_to_user_id_str']
                    info['r_parent_mid'] = u_parent_id
                except:
                    info['r_parent_mid'] = ''
                # 评论点赞量
                info['favorite_count'] = i['favorite_count']
                # 评论url
                # m_content_url ='https://twitter.com/{}/status/'.format(i['in_reply_to_screen_name']) + i['id_str']
                # info['m_content_url'] = m_content_url
                # 评论ID
                r_id = i['id_str']
                # save_fas.sadd("twitter_comment_tj:ss", r_id)
                info['r_id'] = r_id
                # 评论正文
                r_content = i['full_text']
                info['m_content'] = r_content
                # 发布时间
                g_publish_time = get_time(i['created_at'])
                # info['g_publish_time'] = g_publish_time
                info['g_publish_time'] = g_publish_time
                eee = article_id + '-'+ info['r_id']
                row_key = "{}--{}".format(datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
                                          "{}".format(eee))
                info['rowkey'] = row_key
                g_spider_time = get_spider_time()
                # 爬取时间
                info["g_spider_time"] = g_spider_time
                # 更新时间
                info["g_update_time"] = g_spider_time
                info['m_project_name'] = ''
                info['m_is_remove'] = ""
                info['m_data_source'] = 'twitter'
                info['m_relation'] = 'comment'
                user = data['users']
                user_info = user.get(info['u_id'], '')
                info['screen_name'] = user_info['screen_name']
                info['u_nickname'] = user_info['name']
                info['u_area'] = user_info['location']

                print(info)
                insert_sql = """insert ignore twitter_comm(m_origin_id,u_id,r_id,r_content,g_publish_time,favorite_count,screen_name,u_nickname,u_area) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


                return_user_result = []
                return_user_result.append(
                    (m_origin_id,u_id,r_id,r_content,g_publish_time,info['favorite_count'],info['screen_name'],info['u_nickname'],info['u_area'])
                )
                cur.executemany(insert_sql, return_user_result)
                conn.commit()
                print("存储mysql成功")

            except Exception as e:
                import traceback
                traceback.print_exc()
                print(e)
        # for key, value in data['users'].items():
        #     try:
        #         user_info = {}
        #         # 用户ID
        #         u_id = value['screen_name']
        #         user_info['u_id'] = u_id
        #         try:
        #             user_info['uu_id'] = value['id']
        #         except:
        #             user_info['uu_id'] = ''
        #         user_info['id_str'] = value['id_str']
        #         # 用户地址
        #         u_url = 'https://twitter.com/' + u_id
        #         user_info['u_url'] = u_url
        #         # 用户昵称
        #         u_nickname = '' + value['name']
        #         user_info['u_nickname'] = u_nickname
        #         # 头像地址
        #         u_logo = value['profile_image_url_https']
        #         user_info['u_logo'] = u_logo
        #         # 所在地
        #         u_area = value['location']
        #         user_info['u_area'] = u_area
        #         # 出生日期
        #         u_birthday = ''
        #         user_info['u_birthday'] = u_birthday
        #         # 加入twitter的时间
        #         try:
        #             u_login_time = get_time(value['created_at'])
        #             user_info['u_login_time'] = u_login_time
        #         except:
        #             user_info['u_login_time'] = ''
        #         # 个人网址
        #         try:
        #             u_relation_url = value['entities']['url'][0]['expanded_url']
        #             user_info['u_relation_url'] = u_relation_url
        #         except:
        #             user_info['u_relation_url'] = ''
        #         try:
        #             # 个人简介
        #             u_introduction = value['description']
        #             user_info['u_introduction'] = u_introduction
        #         except:
        #             user_info['u_introduction'] = ''
        #         # 关注数
        #         r_follow_num = value['friends_count']
        #         user_info['r_follow_num'] = r_follow_num
        #         # 推文发布数
        #         try:
        #             m_publish_num = value['m_publish_num']
        #             user_info['m_publish_num'] = m_publish_num
        #         except:
        #             user_info['m_publish_num'] = ''
        #         # 点赞数
        #         r_like_num = ''
        #         user_info['r_like_num'] = r_like_num
        #         # 用户是否有效
        #         m_is_remove = ''
        #         user_info['m_is_remove'] = m_is_remove
        #         # 粉丝数
        #         r_fans_num = value['followers_count']
        #         user_info['r_fans_num'] = r_fans_num
        #         try:
        #             user_info['verified'] = value['verified']
        #         except:
        #             user_info['verified'] = 0
        #         # 爬取时间
        #         g_spider_time = get_spider_time()
        #         user_info['g_spider_time'] = g_spider_time
        #         user_info['g_update_time'] = g_spider_time
        #         user_info['rowkey'] = user_info['u_id']
        #         user_info['m_project_name'] = "bjws11"
        #         user_info['u_is_remove'] = ''
        #         user_info['m_data_source'] = 'twitter'
        #         user_info['u_is_active'] = ''
        #         user_info['u_gender'] = ''
        #         user_info['u_marriage'] = ''
        #         user_info['u_blood'] = ''
        #         user_info['u_real_name'] = ''
        #         user_info['u_sexual'] = ''
        #         user_info['u_friend_num'] = ''
        #         print(user_info)
        #         insert_user_sql = """insert ignore twitter_user_comm(id_str,uu_id,u_id,u_url,u_nickname,u_logo,u_area,u_birthday,u_login_time,u_relation_url,u_introduction,r_follow_num,m_publish_num,r_like_num,m_is_remove,r_fans_num,verified) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        #
        #         if user_info:
        #             return_user_result = []
        #             return_user_result.append(
        #                 (user_info['id_str'],user_info['uu_id'],user_info['u_id'], user_info['u_url'], user_info['u_nickname'], user_info['u_logo'],
        #                  user_info['u_area'],
        #                  user_info['u_birthday'], user_info['u_login_time'], user_info['u_relation_url'],
        #                  user_info['u_introduction'], user_info['r_follow_num'], user_info['m_publish_num'],
        #                  user_info['r_like_num'], user_info['m_is_remove'], user_info['r_fans_num'],
        #                  user_info['verified'])
        #             )
        #             cur.executemany(insert_user_sql, return_user_result)
        #             conn.commit()
        #             print("存储mysql成功")
        #     except Exception as e:
        #         import traceback
        #         traceback.print_exc()


    else:
        print("最后一页")
def run():
    conn = pymysql.connect(host='', user='', passwd='', db='',
                           port=3306, charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    cur = conn.cursor()
    while True:
        url_cont = save_fas.spop("twitter_comment_tj:ss")
        if url_cont:
            # ids = re.findall(r"status/(.*)", url_cont[1], re.S)
            article_id = url_cont
            # article_url = url_cont[1]
            get_twitter_info(article_id,conn,cur)
        else:
            time.sleep(60)


if __name__ == '__main__':
    tokens = get_token()
    with ThreadPoolExecutor(max_workers=5) as pool:
        for _ in range(5):
            pool.submit(run)

