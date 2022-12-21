import requests,json,random
from datetime import datetime
import pandas as pd
import logging
from configparser import ConfigParser

parser = ConfigParser()
parser.read('C:/Users/ZMO-WIN-KedarS-01/Desktop/Power BI Tutorial/twitter-airflow-project/sta_project/sta.properties')
aws_access_key = parser.get('details', 'aws_access_key')
aws_secret_access_key = parser.get('details', 'aws_secret_access_key')
username = parser.get('details', 'username')
password = parser.get('details', 'password')
list1 = parser.get('details', 'list1')

agent=[
"Mozilla/5.0 (Linux; Android 8.0.0; SM-G960F Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.84 Mobile Safari/537.36",
"Mozilla/5.0 (Linux; Android 6.0; HTC One X10 Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/61.0.3163.98 Mobile Safari/537.36",
"Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1",
"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36",
"Mozilla/5.0 (X11; FreeBSD i386) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.121 Safari/535.2"
]

csrf="csrftoken="
time = int(datetime.now().timestamp())
login_url = 'https://www.instagram.com/accounts/login/ajax/'
payload  = {
    'username': username,
    'enc_password': f'#PWD_INSTAGRAM_BROWSER:0:{time}:{password}',
    'queryParams': {},
    'optIntoOneTap': 'false'
}

agent_ = random.choice(agent)
login_header = {
    "User-Agent": agent_,    
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.instagram.com/accounts/login/",
    "x-csrftoken": f"csrftoken={csrf}"
}


def IG_LOGIN():
    try:
        with open('cookies.json','r') as f:
            a = json.loads(f.read())
        return a
    except:
        login_response = requests.post(login_url, data=payload, headers=login_header)
        json_data = json.loads(login_response.text)
        if json_data["authenticated"]:
            cookies = login_response.cookies
            cookie_jar = cookies.get_dict()
            json_object = json.dumps(cookie_jar,indent = 4)
            with open("cookies.json","w") as f:
                f.write(json_object)
            return cookie_jar
        else:
            return "login failed", login_response.text

def scrap_followers(username,password,list1):
    cookies_jar = IG_LOGIN()
    csrf_token = cookies_jar['csrftoken']
    headers = {
    "User-Agent": agent_
}
    username=[]
    id_num=[]
    full_name=[]
    external_url=[]
    audit_ts=[]
    followers_count = []    
    following_count = [] 
    private = []
    verified = []
    profile_pic = []
    no_of_posts = []
    blocked_in_country = []
    business_account = []
    business_category = []
    contact_method = []
    business_address = []
    email = []
    phone_number = []
    post_unique_code = []
    post_url = []
    video = []
    audio_inclusion = []
    tagged_users = []
    view_count = []    
    comment_count = []
    like_count = []
    hashtag1 = []
    upload_ts = []
    id_acc = []
    id_bizz = []
    id_post = []
    id_user = []
    _ = 1
    q = 1
    p = 1
    bizz_username = []
    bizz_user_id = []
    bizz_ext_url =[]
    bizz_audit_ts = []
    post_audit_ts = []
    post_user_id_num = []
    for target in list1:
        url = f'https://instagram.com/{target}/?__a=1&__d=dis'
        user_id_req = requests.get(url=url,headers=headers, cookies=cookies_jar).json()
        #user_info
        a = user_id_req['graphql']['user']
        id_user.append(_)
        id_num.append(a['id'])
        username.append(a['username'])
        full_name.append(a['full_name'])
        external_url.append(a['external_url'])
        ts1 = datetime.now()
        audit_ts.append(ts1)
        
        #user_account_info
        id_acc.append(_)
        followers_count.append(a['edge_followed_by']['count'])    
        following_count.append(a['edge_follow']['count']) 
        private.append(a['is_private'])
        verified.append(a['is_verified'])
        profile_pic.append(a['profile_pic_url'])
        no_of_posts.append(a['edge_owner_to_timeline_media']['count'])
        blocked_in_country.append(a['country_block'])
        business_account.append(a['is_business_account'])
        
        #business acount_info
        b = a['is_business_account']
        if b:
            ts = datetime.now()
            bizz_audit_ts.append(ts)
            id_bizz.append(q)
            bizz_username.append(a['username'])
            bizz_user_id.append(a['id'])
            bizz_ext_url.append(a['external_url'])
            business_category.append(a['business_category_name'])
            contact_method.append(a['business_contact_method'])
            business_address.append(a['business_address_json'])
            email.append(a['business_email'])
            phone_number.append(a['business_phone_number'])
            q = q + 1
        
        #posts_info
        c = a['edge_owner_to_timeline_media']['edges']
        if len(c)>0:
            for j in range(len(c)):
                post_user_id_num.append(a['id']) 
                ts2 = datetime.now()
                post_audit_ts.append(ts2)
                id_post.append(p)
                shortcode = c[j]['node']['shortcode']
                post_unique_code.append(shortcode)
                post_url.append(f'https://www.instagram.com/p/{shortcode}/')
                
                ########################################################
                audio = c[j]['node']['is_video']
                video.append(audio)
                if audio:
                    audio_inclusion.append(c[j]['node']['has_audio'])
                    view_count.append(c[j]['node']['video_view_count'])
                else:
                    audio_inclusion.append('NA')
                    view_count.append('NA')
                ########################################################
                
                ########################################################    
                n = c[j]['node']['edge_media_to_tagged_user']['edges']
                if len(n) > 0:
                    users_tagged = []
                    for i in range(len(n)):
                        user = n[i]['node']['user']['username']
                        users_tagged.append(user)
                    tagged_users.append(users_tagged)
                else:
                    tagged_users.append(None)
                ########################################################
                
                
                comment_count.append(c[j]['node']['edge_media_to_comment']['count'])    
                like_count.append(c[j]['node']['edge_liked_by']['count']) 
                
                
                ########################################################
                caption = c[j]['node']['edge_media_to_caption']['edges'][0]['node']['text']
                hashtags = []
                if len(caption) > 0:
                    for i in caption.split('#')[1:]:
                        hashtags.append((i.split('\n')[0]).strip())
                    hashtag1.append(hashtags)
                else:
                    hashtag1.append('NA')
                ########################################################
                
                upload_ts.append(c[j]['node']['taken_at_timestamp'])
                p = p + 1
        _ = _ + 1
        
    
                                   
    dict1 = {"id_user":id_user,"id_num":id_num,"username":username, "full_name":full_name, "external_url":external_url, "audit_ts":audit_ts}
    dict2 = {"id_acc":id_acc, "user_id":id_num, "followers_count":followers_count,"following_count":following_count,"private":private,"verified":verified,"profile_pic_url":profile_pic,"no_of_posts":no_of_posts,"blocked_in_country":blocked_in_country,"business_account":business_account,"audit_ts":audit_ts}
    dict3 = {"id_bizz":id_bizz, "user_id":bizz_user_id,"bizz_ext_url":bizz_ext_url,"business_category":business_category,"contact_method":contact_method,"business_address":business_address,"email":email,"phone_number":phone_number,"audit_ts":bizz_audit_ts}
    dict4 = {"id_post":id_post, "user_id":post_user_id_num,"post_unique_code":post_unique_code,"post_url":post_url,"video":video,"audio_inclusion":audio_inclusion,"tagged_users":tagged_users,"view_count":view_count,"comment_count":comment_count,"like_count":like_count,"hashtag":hashtag1,"upload_ts":upload_ts,"audit_ts":post_audit_ts}
    
    user_data = pd.DataFrame(dict1)
    user_account_data = pd.DataFrame(dict2)
    business_account_data = pd.DataFrame(dict3)
    account_post = pd.DataFrame(dict4)
                              
    user_data.to_csv('user_info/user.csv',index=False,header=True)
    user_account_data.to_csv('user_acc_info/user_account.csv',index=False,header=True)
    business_account_data.to_csv('business_acc_info/business_account.csv',index=False,header=True)
    account_post.to_csv('user_post_info/account_post.csv',index=False,header=True)
    
    
scrap_followers(username,password,list1)