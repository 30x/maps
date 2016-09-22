'use strict';
var http = require('http')
var url = require('url')
var lib = require('http-helper-functions')
var uuid = require('node-uuid')
var ps = require('./maps-persistence.js')

var MAPS    = '/bWFw-'
var ENTRIES = '/ZW50-'
var VALUES  = '/dmFs-'

function verifyMapName(req, res, user, map, callback) {
  if (!(map.name === undefined && map.namespace === undefined)) 
    if (map.name === undefined || map.namespace === undefined)
      lib.badRequest(res, `must provide both name and namespace or neither. name: ${map.name} namespace: ${map.namespace}`)
    else
      ps.db.withMapByNameDo(map.namespace, map.name, function(err, map) {
        if (err != 404) 
          if (err)
            lib.badRequest(res, `unable to check for map name collision. err: ${err}`)
          else
            lib.duplicate(res, `duplicate map name ${map.namespace}:${map.name}`)
      })
  callback()  
}

function verifyMap(req, map, user, callback) {
  var rslt = lib.setStandardCreationProperties(req, map, user)
  if (map.isA != 'Map') 
    return `invalid JSON: "isA" property not set to "Map" ${JSON.stringify(map)}`
  return null
}

function makeMapURL(req, mapID) {
  return `//${req.headers.host}${MAPS}${mapID}`
}

function addCalculatedMapProperties(req, map, selfURL) {
  map.self = selfURL; 
  map.entries = `${selfURL}/entries`
  map._permissions = `protocol://authority/permissions?${map.self}`;
  map._permissionsHeirs = `protocol://authority/permissions-heirs?${map.self}`;
}

function createMap(req, res, map) {
  var user = lib.getUser(req);
  if (user == null) {
    lib.unauthorized(req, res);
  } else { 
    var err = verifyMap(req, map, user)
    if (err !== null) 
      lib.badRequest(res, err);
    else {
      verifyMapName(req, res, user, map, function() {
        lib.internalizeURLs(map, req.headers.host) 
        var permissions = map.permissions
        if (permissions !== undefined)
          delete map.permissions;
        var id = uuid()
        var selfURL = makeMapURL(req, id)
        lib.createPermissonsFor(req, res, selfURL, permissions, function(permissionsURL, permissions){
          // Create permissions first. If we fail after creating the permissions resource but before creating the main resource, 
          // there will be a useless but harmless permissions document.
          // If we do things the other way around, a map without matching permissions could cause problems.
          ps.createMapThen(req, res, id, selfURL, map, function(etag) {
            addCalculatedMapProperties(req, map, selfURL)
            lib.created(req, res, map, map.self, etag)
          })
        })
      })
    }
  }
}

function verifyEntry(req, entry, user) {
  var rslt = lib.setStandardCreationProperties(req, entry, user)
  if (entry.isA != 'MapEntry') 
    return `invalid JSON: "isA" property not set to "MapEntry" ${JSON.stringify(map)}`
  if (entry.key == null) 
    return `must provide non-null key: ${entry.key}`
  if (typeof entry.key != 'string') 
    return `key must be string: ${entry.key} is of type ${typeof entry.key}`
  return null  
}

function makeEntryURL(req, mapID, key) {
  return `//${req.headers.host}${ENTRIES}${mapID}:${key}`
}

function makeValueURL(req, mapID, key) {
  return `//${req.headers.host}${VALUES}${mapID}:${key}`
}

function addCalculatedEntryProperties(req, entry, mapID, key) {
  entry.self = makeEntryURL(req, mapID, key)
  entry.value = makeValueURL(req, mapID, key)
}

function createEntry(req, res, mapID, entry) {
  var key = entry.key
  var user = lib.getUser(req)
  if (user == null)
    lib.unauthorized(req, res)
  else { 
    var err = verifyEntry(req, entry, user)
    if (err !== null)
      lib.badRequest(res, err)
    else {
      lib.internalizeURLs(entry, req.headers.host)
      lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'create', function() {
        ps.createEntryThen(req, res, mapID, key, entry, function(etag) {
          addCalculatedEntryProperties(req, entry, mapID, key)
          lib.created(req, res, entry, entry.self, etag)
        })
      })
    } 
  }  
}

function createValue(req, res, mapID, key, value) {
  var user = lib.getUser(req);
  if (user == null)
    lib.unauthorized(req, res)
  else {
    if (req.headers['content-type'] != null) {
      lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'create', function() {
        ps.upsertValueThen(req, res, mapID, key, value, function(etag) {
          lib.created(req, res, null, makeValueURL(req, mapID, key), etag)
        })
      })
    } else
      lib.badRequest(res, 'must provide Content-Type header')
  }  
}

function getValue(req, res, mapID, key) {
  var user = lib.getUser(req);
  if (user == null)
    lib.unauthorized(req, res);
  else
    lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'read', function() {
      ps.withValueDo(req, res, mapID, key, function(metadata, value) {
        lib.found(req, res, value, null, makeValueURL(req, mapID, key), metadata['Content-Type'])
      })
    })
}

function returnMap(req, res, map, id, etag) {
  addCalculatedMapProperties(req, map, makeMapURL(req, id))
  map._permissions = `protocol://authority/permissions?${map.self}`
  map._permissionsHeirs = `protocol://authority/permissions-heirs?${map.self}`
  lib.externalizeURLs(map, req.headers.host)
  lib.found(req, res, map, etag)
}

function getMap(req, res, id) {
  lib.ifAllowedThen(req, res, null, '_resource', 'read', function() {
    ps.withMapDo(req, res, id, function(map, etag) {
      returnMap(req, res, map, id, etag)
    });
  });
}

function getMapByName(req, res, ns, name) {
  ps.withMapByNameDo(req, res, ns, name, function(map, id, etag) {
    lib.ifAllowedThen(req, res, makeMapURL(req, id), '_resource', 'read', function() {
      returnMap(req, res, map, id, etag)
    })
  })
}

function deleteMap(req, res, id) {
  lib.ifAllowedThen(req, res, null, '_resource', 'delete', function() {
    ps.deleteMapThen(req, res, id, function (map) {
      lib.found(req, res, map);
    });
  });
}

function updateMap(req, res, id, patch) {
  lib.ifAllowedThen(req, res, makeMapURL(req, id), '_resource', 'update', function(map) {
    ps.withMapDo(req, res, id, function(map) {
      var patchedMap = lib.mergePatch(map, patch)
      verifyMapName(req, res, lib.getUser(req), map, function() {
        ps.updateMapThen(req, res, id, map, patchedMap, function (etag) {
          addCalculatedMapProperties(req, patchedMap, makeMapURL(req, id)) 
          lib.found(req, res, patchedMap, etag);
        })
      })
    })
  })
}

function getEntries(req, res, mapID) {
  lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'read', function(map) {
    ps.withEntriesDo(req, res, mapID, function (entries) {
      var apiEntries = entries.map(x=>{
        x.data.etag = x.etag
        addCalculatedEntryProperties(req, x.data, x.mapid, x.key)
        return x.data
      }) 
      lib.found(req, res, {isA: 'Collection', self: '//' + req.headers.host + req.url, contents: apiEntries});
    });
  });
}

function getNameParts(req, res, id, callback) {
  let splitID = id.split(':')
  if (splitID.length == 2) {
    let [part1, part2] = splitID
    callback(part1, part2)
  } else
    lib.badRequest(res, `id must be composed of two simple names separated by a ":" (${id})`)
}

function requestHandler(req, res) {
  if (req.url == '/maps') 
    if (req.method == 'POST')
      lib.getServerPostObject(req, res, createMap)
    else
      lib.methodNotAllowed(req, res, ['POST'])
  else {
    var req_url = url.parse(req.url);
    if (req_url.pathname.lastIndexOf(MAPS, 0) > -1) { /* url of form /MAPS-xxxxxx */
      let splitPath = req_url.pathname.split('/')
      if (splitPath.length == 2) { // first entry is always '' because pathname always begins with '/'
        let id = req_url.pathname.substring(MAPS.length);
        if (req.method == 'GET') 
          getMap(req, res, id);
        else if (req.method == 'DELETE') 
          deleteMap(req, res, id);
        else if (req.method == 'PATCH') 
          lib.getServerPostObject(req, res, function (req, res, jso) {
            updateMap(req, res, id, jso)
          })
        else 
          lib.methodNotAllowed(req, res, ['GET', 'DELETE', 'PATCH'])
      } else if (splitPath.length == 3 && splitPath[2] == 'entries') { /* url of form /MAPS-xxxxxx/entries */
        var mapID = splitPath[1].substring(MAPS.length-1)
        if (req.method == 'POST') 
          lib.getServerPostObject(req, res, function(req, res, entry) {
            createEntry(req, res, mapID, entry)
          })
        else if (req.method == 'GET')
          getEntries(req, res, mapID)
        else
          lib.methodNotAllowed(req, res, ['GET', 'POST'])
      } else if (splitPath.length == 4 && splitPath[2].lastIndexOf('entries;',0) > -1 && splitPath[3] == 'value') { /* url of form /MAPS-xxxxxx/entries;{key}/value */
        var mapID = splitPath[1].substring(MAPS.length-1)
        let key = splitPath[2].substring('entries;'.length);          
        getValue(req, res, mapID, key)
      } else
        lib.notFound(req, res)
    } else if (req_url.pathname.lastIndexOf(VALUES, 0) > -1) {
      let splitPath = req_url.pathname.split('/')
      if (splitPath.length == 2)
        if (req.method == 'PUT')
          getNameParts(req, res, req_url.pathname.substring(VALUES.length), function(mapID, key) {
            lib.getServerPostBuffer(req, res, function(req, res, value) {
              createValue(req, res, mapID, key, value)
            })
          })
        else if (req.method == 'GET')
          getNameParts(req, res, req_url.pathname.substring(VALUES.length), function(mapID, key) {
            getValue(req, res, mapID, key)
          })          
        else 
          lib.methodNotAllowed(req, res, ['GET', 'PUT'])
      else  
        lib.notFound(req, res)      
    } else if (req_url.pathname.lastIndexOf('/maps;', 0) > -1) {
      let splitPath = req_url.pathname.split('/')
      if (splitPath.length == 2)
        if (req.method == 'GET') {
          let id = req_url.pathname.substring('/maps;'.length);
          getNameParts(req, res, req_url.pathname.substring(VALUES.length), function(ns, name) {
            getMapByName(req, res, ns, name);         
          })
        } else 
          lib.methodNotAllowed(req, res, ['GET'])
      else if (splitPath.length == 3 && splitPath[2] == 'entries')  /* url of form /maps;ns:name/entries */
        lib.internalError(res, 'not yet implemented')
      else if (splitPath.length == 4 && splitPath[2].lastIndexOf('entries',0) > -1 && splitPath[3] == 'value')  /* url of form /maps;ns:name/entries;{key}/value */
        lib.internalError(res, 'not yet implemented')
      else
        lib.notFound(req, res)
    } else 
      lib.notFound(req, res)
  }
}
ps.init(function(){
  var port = process.env.PORT;
  http.createServer(requestHandler).listen(port, function() {
    console.log(`server is listening on ${port}`);
  });
});