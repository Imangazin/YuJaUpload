import requests
from requests.auth import HTTPBasicAuth

import json
import logging
from datetime import datetime

import pandas as pd


#log file
logging.basicConfig(filename='output.log', encoding='utf-8', level=logging.INFO)

logging.info(f'Start YuJa Upload at {datetime.now}')

config=''

with open('config.json', 'r') as f:
    config = json.load(f)


def apicall_with_auth(endpoint,  body=None):
    headers = {'authToken': config['access_token']}
    response = requests.get(endpoint, headers=headers, data=body)
    return response

def postcall_with_auth(endpoint, param):
    headers = {'authToken': config['access_token'], 'Content-Type': 'application/json'}
    response = requests.post(endpoint, headers=headers, data=param)
    return response


def readcsv(path):
    return pd.read_csv(path, na_values=['nan'], keep_default_na=False)

# prints yuja user list with emails, yuja user_id, yuja login_id
def print_yuja_users():
    user_call = apicall_with_auth(config['baseUrl']+'/users').json()
    users = {}
    email = []
    login_id = []
    user_id = []
    for user in user_call:
        email.append(user['email_address'])
        login_id.append(user['login_id'])
        user_id.append(user['user_id'])
    users['email'] = email
    users['login_id'] = login_id
    users['user_id'] = user_id

    df = pd.DataFrame(users)
    df.to_csv('yuja_users.csv', index=False)



def upload_to_yuja(file_path, title, login_id):
    upload_link = apicall_with_auth(config['baseUrl']+'/media/createuploadlink').json()
   
    with open(file_path, 'rb') as file:
        video_data = file.read()

    upload = requests.put(upload_link['url'], data=video_data, headers={'Content-Type': 'video/mp4'}, stream=True)

    params = {
        'login_id': login_id,
        'title': title,
        'file_key': upload_link['key'],
        'description': ''
    }

    #time.sleep(200)

    if upload.status_code == 200:
        set_video = postcall_with_auth(config['baseUrl']+'/media/upload/video/', json.dumps(params))
        if set_video.status_code ==200:
            print(f'Success Video uploaded {set_video.status_code}, login_id: {login_id}, Title: {title}')
            logging.info(f'Success Video uploaded {set_video.status_code}, login_id: {login_id}, Title: {title}')
            return set_video.json()['id']
        else:
            print(f'Error occured: Video was not uploaded {set_video.status_code}, login_id: {login_id}, Title: {title}')
            logging.info(f'Error occured: Video was not uploaded {set_video.status_code}, login_id: {login_id}, Title: {title}')
            return 'noid'


def add_metadata(video_id, published_date, last_view, create_date, echo_media_id):
    params = {
        'newMetadata':[
            {
                'Name':'Create Date',
                'Type': 'Date',
                'Value': create_date
            },
            {
                'Name':'Published Date',
                'Type': 'Date',
                'Value': published_date
            },
            {
                'Name':'Last View',
                'Type': 'Date',
                'Value': last_view
            },
            {
                'Name':'echo media id',
                'Type': 'String',
                'Value': echo_media_id
            },
        ]
    }

    metadata = postcall_with_auth(config['baseUrl']+'/media/metadata/'+video_id, json.dumps(params))
    print(f'Metadata status_code: {metadata.status_code}')
    logging.info(f'Metadata status_code: {metadata.status_code}')


def add_caption(video_id, caption_path, user_id):
    params ={
        "videoPID": video_id,
        "extension": "vtt",
        "type": "caption",
        "language": "english"
    }

    upload_link = postcall_with_auth(config['baseUrl']+'/media/captionFileUploadLink', json.dumps(params))


    with open(caption_path, 'rb') as file:
        vtt_data = file.read()
    response = requests.put(upload_link.json()['url'], data=vtt_data, headers={'Content-Type': 'text/vtt'})
    # Wait until the upload is complete
    while response.status_code != 200:
        response = requests.get(upload_link.json()['url'])
    
    caption_params = {
        "login_id": user_id,
        "videoPID": video_id,
        "file_key": upload_link.json()['key'],
        "type": "caption"
    }
    set_caption = postcall_with_auth(config['baseUrl']+'/media/captionFileToVideo', json.dumps(caption_params))

    if (set_caption.status_code==200):
        print(f'Success Caption file uploaded: {caption_path}')
        logging.info(f'Success Caption file uploaded: {caption_path}')


df = readcsv()

for each in df.index:
    video_id = upload_to_yuja('media/'+df['video_path'][each],df['video_name'][each],df['login_id'][each])
     
    add_metadata(f'{video_id}',df['published_date'][each],df['last_view'][each],df['create_date'][each], df['mediaid'][each])

    if df['caption_path'][each] != '':
        add_caption(f'{video_id}', 'caption/'+df['caption_path'][each], f"{df['user_id'][each]}")
