import requests
import base64
import json
from os import environ as env
from urlparse import urljoin

PG_HOST = env['PG_HOST']
PG_USER = env['PG_USER']
PG_PASSWORD = env['PG_PASSWORD']
PG_DATABASE = env['PG_DATABASE']
EXTERNAL_SCHEME = env['EXTERNAL_SCHEME']
BASE_URL = '%s://%s:%s' % (EXTERNAL_SCHEME, env['EXTERNAL_SY_ROUTER_HOST'], env['EXTERNAL_SY_ROUTER_PORT']) if 'EXTERNAL_SY_ROUTER_PORT' in env else '%s://%s' % (EXTERNAL_SCHEME, env['EXTERNAL_SY_ROUTER_HOST'])

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

    # DELETE map

    map_url = urljoin(BASE_URL, '/maps;acme:nursery-rhymes')
    headers = {'Authorization': 'Bearer %s' % TOKEN1}
    r = requests.delete(map_url, headers=headers)
    if r.status_code == 200:
        print 'deleted test map %s etag: %s' % (r.headers['Content-Location'], r.headers['etag'])
    elif r.status_code == 404:
        print 'test map was not there %s' % (map_url)
    else:
        print 'failed to delete map %s %s %s' % (map_url, r.status_code, r.text)
        return

    map = {
        'isA': 'Map',
        'test-data': True
        }

    maps_url = urljoin(BASE_URL, '/maps') 
    
    # Create map using POST

    headers = {'Content-Type': 'application/json','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.post(maps_url, headers=headers, json=map)
    if r.status_code == 201:
        print 'correctly created map %s etag: %s' % (r.headers['Location'], r.headers['etag'])
        map_url = urljoin(BASE_URL, r.headers['Location'])
        map_entries = urljoin(BASE_URL, r.json()['entries'])
    else:
        print 'failed to create map %s %s %s' % (maps_url, r.status_code, r.text)
        return
        
    # GET Map

    headers = {'Accept': 'application/json','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.get(map_url, headers=headers, json=map)
    if r.status_code == 200:
        map_url2 = urljoin(BASE_URL, r.headers['Content-Location'])
        if map_url == map_url2:
            map = r.json()
            print 'correctly retrieved map: %s etag: %s' % (map_url, r.headers['etag'])
        else:
            print 'retrieved map at %s but Content-Location is wrong: %s' % (map_url, map_url2)
            return
    else:
        print 'failed to retrieve map %s %s %s' % (map_url, r.status_code, r.text)
        return
        
    # POST entry for Humpty Dumpty

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
        value_ref  = urljoin(BASE_URL, r.json()['value'])
        print 'correctly created entry: %s value: %s map: %s etag: %s' % (entry_url, value_ref, urljoin(BASE_URL, r.json()['map']), r.headers['etag'])
    else:
        print 'failed to create entry %s %s %s' % (entries_url, r.status_code, r.text)
        return

    # GET entry for Humpty Dumpty

    headers = {'Accept': 'application/json','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.get(entry_url, headers=headers)
    if r.status_code == 200:
        value_ref  = urljoin(BASE_URL, r.json()['value'])
        print 'correctly retrieved entry: %s value: %s map: %s etag: %s' % (entry_url, value_ref, urljoin(BASE_URL, r.json()['map']), r.headers['etag'])
    else:
        print 'failed to retrieve entry %s %s %s' % (entry_url, r.status_code, r.text)
        return

    # POST value for HumptyDumpty

    headers = {'Content-Type': 'text/plain','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.put(value_ref, headers=headers, data='Humpty Dumpty Sat on a wall')
    if r.status_code == 200:
        loc = r.headers['Content-Location']
        print 'correctly created value: %s etag: %s' % (loc, r.headers['etag'])
        value_url = urljoin(BASE_URL, r.headers['Content-Location'])
    else:
        print 'failed to create value %s %s %s' % (value_ref, r.status_code, r.text)
        return
        
    # PUT value for LittleMissMuffet

    headers = {'Content-Type': 'text/plain','Authorization': 'Bearer %s' % TOKEN1}
    value_ref2 = '%s/entries;%s/value' % (map_url, 'LittleMissMuffet')
    r = requests.put(value_ref2, headers=headers, data='Little Miss Muffet\nSat on a tuffet')
    if r.status_code == 200:
        loc = r.headers['Content-Location']
        print 'correctly created value: %s etag: %s' % (loc, r.headers['etag'])
        value_url = urljoin(BASE_URL, loc)
    else:
        print 'failed to create value %s %s %s' % (value_ref2, r.status_code, r.text)
        return

    # GET entry for LittleMissMuffet

    entry_ref2 = '%s/entries;%s' % (map_url, 'LittleMissMuffet')
    headers = {'Accept': 'application/json','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.get(entry_ref2, headers=headers)
    if r.status_code == 200:
        value_ref  = urljoin(BASE_URL, r.json()['value'])
        assert(value_ref == value_url)
        print 'correctly retrieved entry: %s value: %s map: %s etag: %s' % (entry_ref2, value_ref, urljoin(BASE_URL, r.json()['map']), r.headers['etag'])
    else:
        print 'failed to retrieve entry %s %s %s' % (entry_ref2, r.status_code, r.text)
        return

    # GET value for LittleMissMuffet

    headers = {'Authorization': 'Bearer %s' % TOKEN1}
    r = requests.get(value_ref2, headers=headers)
    if r.status_code == 200:
        loc = r.headers['Content-Location']
        print 'correctly got value at %s length: %s etag: %s text: %s' % (loc, len(r.text), r.headers['etag'], r.text)
    else:
        print 'failed to get value %s %s %s' % (value_ref2, r.status_code, r.text)
        return

    # GET all entries for map

    headers = {'Accept': 'application/json','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.get(map_entries, headers=headers, json=map)
    if r.status_code == 200:
        print 'correctly retrieved map entries: %s' % map_url
    else:
        print 'failed to retrieve map entries %s %s %s' % (map_url, r.status_code, r.text)
        return


    map = {
        'isA': 'Map',
        'name': 'nursery-rhymes',
        'namespace': 'acme',
        'test-data': True
    }

    # Create map with name

    headers = {'Content-Type': 'application/json','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.post(maps_url, headers=headers, json=map)
    if r.status_code == 201:
        print 'correctly created map with name %s' % (r.text)
    else:
        print 'failed to create map with name %s %s %s' % (maps_url, r.status_code, r.text)
        return

    # GET map by name

    name_url = urljoin(BASE_URL, '/maps;acme:nursery-rhymes')
    headers = {'Accept': 'application/json','Authorization': 'Bearer %s' % TOKEN1}
    r = requests.get(name_url, headers=headers, json=map)
    if r.status_code == 200:
        print 'correctly retrieved map by name: %s etag: %s' % (name_url, r.headers['etag'])
    else:
        print 'failed to retrieve map by name %s %s %s' % (name_url, r.status_code, r.text)
        return

    # TODO add more tests for getting and putting values by name ( not the PATCH use-case )

if __name__ == '__main__':
    main()