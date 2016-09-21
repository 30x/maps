import requests
import base64
import json
from os import environ as env
from urlparse import urljoin

PG_HOST = env['PG_HOST']
PG_USER = env['PG_USER']
PG_PASSWORD = env['PG_PASSWORD']
PG_DATABASE = env['PG_DATABASE']
EXTERNAL_ROUTER = env['EXTERNAL_ROUTER']
EXTERNAL_SCHEME = env['EXTERNAL_SCHEME']
BASE_URL = '%s://%s' % (EXTERNAL_SCHEME, EXTERNAL_ROUTER)

def b64_decode(data):
    missing_padding = (4 - len(data) % 4) % 4
    if missing_padding:
        data += b'='* missing_padding
    return base64.decodestring(data)

if 'APIGEE_TOKEN1' in env:
    TOKEN1 = env['APIGEE_TOKEN1']
else:
    with open('token.txt') as f:
        TOKEN1 = f.read()
USER1 = json.loads(b64_decode(TOKEN1.split('.')[1]))['user_id']

if 'APIGEE_TOKEN2' in env:
    TOKEN2 = env['APIGEE_TOKEN2']
else:
    with open('token2.txt') as f:
        TOKEN2 = f.read()
USER2 = json.loads(b64_decode(TOKEN2.split('.')[1]))['user_id']

if 'APIGEE_TOKEN3' in env:
    TOKEN3 = env['APIGEE_TOKEN3']
else:
    with open('token3.txt') as f:
        TOKEN3 = f.read()
USER3 = json.loads(b64_decode(TOKEN3.split('.')[1]))['user_id']

def main():
    
    print 'sending requests to %s' % BASE_URL 

    map = {
        'isA': 'Map',
        'test-data': True
        }

    maps_url = urljoin(BASE_URL, '/maps') 
    
    # Create map

    headers = {'Content-Type': 'application/json','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.post(maps_url, headers=headers, json=map)
    if r.status_code == 201:
        print 'correctly created map %s' % r.headers['Location']
        map_url = urljoin(BASE_URL, r.headers['Location'])
        map_etag = r.headers['Etag'] 
    else:
        print 'failed to create map %s %s %s' % (maps_url, r.status_code, r.text)
        return
        
    headers = {'Accept': 'application/json','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.get(map_url, headers=headers, json=map)
    if r.status_code == 200:
        map_url2 = urljoin(BASE_URL, r.headers['Content-Location'])
        if map_url == map_url2:
            map = r.json()
            print 'correctly retrieved map: %s' % map_url 
        else:
            print 'retrieved map at %s but Content-Location is wrong: %s' % (map_url, map_url2)
            return
    else:
        print 'failed to retrieve map %s %s %s' % (map_url, r.status_code, r.text)
        return
        
    entry = {
        'isA': 'MapEntry',
        'key': 'HumptyDumpty',
        'test-data': True
        }

    entries_url = urljoin(BASE_URL, map['entries']) 
    
    headers = {'Content-Type': 'application/json','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.post(entries_url, headers=headers, json=entry)
    if r.status_code == 201:
        entry_url = urljoin(BASE_URL, r.headers['Location'])
        entry_etag = r.headers['Etag'] 
        value_ref  = urljoin(BASE_URL, r.json()['valueRef'])
        print 'correctly created entry: %s etag: %s valueRef: %s' % (entries_url, entry_etag, value_ref)
    else:
        print 'failed to create entry %s %s %s' % (entries_url, r.status_code, r.text)
        return

    headers = {'Content-Type': 'text/plain','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.put(value_ref, headers=headers, data='first entry value')
    if r.status_code == 201 or r.status_code == 200:
        print 'correctly created value: %s etag: %s' % (r.headers['Location'], r.headers['Etag'])
        value_url = urljoin(BASE_URL, r.headers['Location'])
        value_etag = r.headers['Etag'] 
    else:
        print 'failed to create valiue %s %s %s' % (value_ref, r.status_code, r.text)
        return
        
        
if __name__ == '__main__':
    main()