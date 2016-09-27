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
  if (!(map.name === undefined && map.namespace === undefined)) // named map
    if (map.name === undefined || map.namespace === undefined)
      lib.badRequest(res, `must provide both name and namespace or neither. name: ${map.name} namespace: ${map.namespace}`)
    else // both parts of the name present
      ps.db.withMapByNameDo(map.namespace, map.name, function(err, map) {
        if (err != 404) 
          if (err)
            lib.badRequest(res, `unable to check for map name collision. err: ${err}`)
          else
            lib.duplicate(res, `duplicate map name ${map.namespace}:${map.name}`)
        else {// not already there
          callback(map.namespace, map.name)}
      })
  else
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
  map._permissions = `protocol://authority/permissions?${map.self}`
  map._permissionsHeirs = `protocol://authority/permissions-heirs?${map.self}`
}

function createMap(req, res, map) {
  function primCreateMap() {
    lib.internalizeURLs(map, req.headers.host) 
    var permissions = map.permissions
    if (permissions !== undefined)
      delete map.permissions
    var mapID = uuid()
    var selfURL = makeMapURL(req, mapID)
    lib.createPermissonsFor(req, res, selfURL, permissions, function(permissionsURL, permissions){
      // Create permissions first. If we fail after creating the permissions resource but before creating the main resource, 
      // there will be a useless but harmless permissions document.
      // If we do things the other way around, a map without matching permissions could cause problems.
      ps.createMapThen(req, res, mapID, selfURL, map, function(etag) {
        addCalculatedMapProperties(req, map, selfURL)
        lib.created(req, res, map, map.self, etag)
      })
    })    
  }
  var user = lib.getUser(req)
  if (user == null)
    lib.unauthorized(req, res)
  else { 
    var err = verifyMap(req, map, user)
    if (err !== null) 
      lib.badRequest(res, err)
    else
      verifyMapName(req, res, user, map, function(namespace, name) {
        if (namespace === undefined)
          primCreateMap()
        else
          lib.ifAllowedThen(req, res, `/namespaces;${namespace}`, '_resource', 'create', primCreateMap)
      })
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
  entry = entry ? entry : {isA: 'MapEntry'}
  entry.key = key
  entry.self = makeEntryURL(req, mapID, key)
  entry.value = makeValueURL(req, mapID, key)
  entry.map = makeMapURL(req, mapID)
  return entry
}

function createEntry(req, res, mapID, entry) {
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
        var key = entry.key
        delete entry.key
        ps.createEntryThen(req, res, mapID, key, entry, function(etag) {
          addCalculatedEntryProperties(req, entry, mapID, key)
          lib.created(req, res, entry, entry.self, etag)
        })
      })
    } 
  }  
}

function upsertValue(req, res, mapID, key, value) {
  var user = lib.getUser(req);
  if (user == null)
    lib.unauthorized(req, res)
  else {
    if (req.headers['content-type'] != null) {
      lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'create', function() {
        ps.upsertValueThen(req, res, mapID, key, value, function(etag) {
          lib.found(req, res, null, etag, makeValueURL(req, mapID, key)) // Cassanda can't tell us if it was a create or an update, so we just pick one'
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
      ps.withValueDo(req, res, mapID, key, function(valuedata, value, etag) {
        if (valuedata)
          lib.found(req, res, value, etag, makeValueURL(req, mapID, key), valuedata['Content-Type'])
        else
          lib.notFound(req, res)
      })
    })
}

function returnMap(req, res, map, mapID, etag) {
  addCalculatedMapProperties(req, map, makeMapURL(req, mapID))
  map._permissions = `protocol://authority/permissions?${map.self}`
  map._permissionsHeirs = `protocol://authority/permissions-heirs?${map.self}`
  lib.externalizeURLs(map, req.headers.host)
  lib.found(req, res, map, etag)
}

function getMap(req, res, mapID) {
  lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'read', function() {
    ps.withMapDo(req, res, mapID, function(map, etag) {
      returnMap(req, res, map, mapID, etag)
    })
  })
}

function deleteMap(req, res, mapID) {
  lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'delete', function() {
    ps.deleteMapThen(req, res, mapID, function (map, etag) {
      lib.found(req, res, map, etag)
    })
  })
}

function updateMap(req, res, mapID, patch) {
  lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'update', function() {
    ps.withMapDo(req, res, mapID, function(map) {
      securedUpdateMap(req, res, mapID, map, patch)
    })
  })
}

function securedUpdateMap(req, res, mapID, map, patch) {
  var patchedMap = lib.mergePatch(map, patch)
  function primUpdateMap() {
    ps.updateMapThen(req, res, mapID, patchedMap, function (etag) {
      addCalculatedMapProperties(req, patchedMap, makeMapURL(req, mapID)) 
      lib.found(req, res, patchedMap, etag);
    })    
  }
  verifyMapName(req, res, lib.getUser(req), map, function(namespace, name) {
    if (namespace === undefined)
      primUpdateMap()
    else
      lib.ifAllowedThen(req, res, `/namespaces;${namespace}`, '_resource', 'create', primUpdateMap)        
  })  
}

function getEntry(req, res, mapID, key) {
  lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'read', function(map) {
    ps.withEntryDo(req, res, mapID, key, function (entry, etag) {
      entry = addCalculatedEntryProperties(req, entry, mapID, key)
      console.log(entry)
      lib.found(req, res, entry, etag, entry.self)
    });
  });
}

function deleteEntry(req, res, mapID, key) {
  lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'delete', function(map) {
    ps.deleteEntryThen(req, res, mapID, key, function (entry, etag) {
      entry = addCalculatedEntryProperties(req, entry, mapID, key)
      lib.found(req, res, entry, etag, entry.self)
    });
  });
}

function updateEntry(req, res, mapID, key, patch) {
  lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'update', function() {
    ps.withEntryDo(req, res, mapID, key, function(entry, etag) {
      var patchedEntry = lib.mergePatch(entry || {}, patch)
      ps.updateEntryThen(req, res, mapID, key, patchedEntry, function (etag) {
        addCalculatedMapProperties(req, patchedMap, makeEntryURL(req, mapID, key)) 
        lib.found(req, res, patchedMap, etag);
      })    
    })
  })
}

function getEntries(req, res, mapID) {
  lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'read', function(map) {
    ps.withEntriesDo(req, res, mapID, function (entries) {
      var apiEntries = entries.map(x=>{
        x.valuedata.etag = x.etag
        return addCalculatedEntryProperties(req, x.entrydata, x.mapid, x.key)
      }) 
      lib.found(req, res, {isA: 'Collection', self: '//' + req.headers.host + req.url, contents: apiEntries});
    });
  });
}

function getNameParts(req, res, mapFullName, callback) {
  let splitID = mapFullName.split(':')
  if (splitID.length == 2) {
    let [part1, part2] = splitID
    callback(part1, part2)
  } else
    lib.badRequest(res, `name must be composed of two simple names separated by a ":" (${mapFullName})`)
}

function requestHandler(req, res) {
  function handleEntriesRequest(mapID) {
    if (req.method == 'POST') 
      lib.getServerPostObject(req, res, function(req, res, entry) {
        createEntry(req, res, mapID, entry)
      })
    else if (req.method == 'GET')
      getEntries(req, res, mapID)
    else
      lib.methodNotAllowed(req, res, ['GET', 'POST'])  
  }
  function handleMapMethods(mapID) {
    if (req.method == 'GET') 
      getMap(req, res, mapID);
    else if (req.method == 'DELETE') 
      deleteMap(req, res, mapID);
    else if (req.method == 'PATCH') 
      lib.getServerPostObject(req, res, function (req, res, jso) {
        updateMap(req, res, mapID, jso)
      })
    else 
      lib.methodNotAllowed(req, res, ['GET', 'DELETE', 'PATCH'])    
  }
  function handleEntryMethods(mapID, key) {
      if (req.method == 'GET') 
        getEntry(req, res, mapID, key)
      else if (req.method == 'PATCH')
        lib.getServerPostBuffer(req, res, function(req, res, value) {
          upsertVupdateEntryalue(req, res, mapID, key, value) 
        })
      else if (req.method == 'DELETE') 
        deleteEntry(req, res, mapID, key)
      else
        lib.methodNotAllowed(req, res, ['GET', 'PATCH', 'DELETE'])    
  }
  function handleValueMethods(mapID, key) {
    if (req.method == 'GET') 
      getValue(req, res, mapID, key)
    else if (req.method == 'PUT')
      lib.getServerPostBuffer(req, res, function(req, res, value) {
        upsertValue(req, res, mapID, key, value) 
      })
    else if (req.method == 'DELETE') 
      deleteValue(req, res, mapID, key)
    else
      lib.methodNotAllowed(req, res, ['GET', 'PUT', 'DELETE'])
  }
  if (req.url == '/maps') 
    if (req.method == 'POST')
      lib.getServerPostObject(req, res, createMap)
    else
      lib.methodNotAllowed(req, res, ['POST'])
  else {
    var req_url = url.parse(req.url);
    if (req_url.pathname.lastIndexOf(MAPS, 0) > -1) { /* url of form /MAPS-xxxxxx */
      let splitPath = req_url.pathname.split('/')
      if (splitPath.length == 2) // first entry is always '' because pathname always begins with '/'
        handleMapMethods(req_url.pathname.substring(MAPS.length))
      else if (splitPath.length == 3 && splitPath[2] == 'entries')  /* url of form /MAPS-xxxxxx/entries */
        handleEntriesRequest(splitPath[1].substring(MAPS.length-1))
      else if (splitPath.length == 3 && splitPath[2].lastIndexOf('entries;',0) > -1 ) { /* url of form /MAPS-xxxxxx/entries;{key} */
        var mapID = splitPath[1].substring(MAPS.length-1)
        let key = splitPath[2].substring('entries;'.length);    
        handleEntryMethods(mapID, key)
      } else if (splitPath.length == 4 && splitPath[2].lastIndexOf('entries;',0) > -1 && splitPath[3] == 'value') { /* url of form /MAPS-xxxxxx/entries;{key}/value */
        var mapID = splitPath[1].substring(MAPS.length-1)
        let key = splitPath[2].substring('entries;'.length);    
        handleValueMethods(mapID, key)
      } else
        lib.notFound(req, res)
    } else if (req_url.pathname.lastIndexOf(ENTRIES, 0) > -1) { /* url of form /ENTRIES-mapID:{key} */
      let splitPath = req_url.pathname.split('/')
      let entryID = splitPath[1].substring(ENTRIES.length-1)
      if (splitPath.length == 2)
        getNameParts(req, res, entryID, handleEntryMethods)
      else  
        lib.notFound(req, res)      
    } else if (req_url.pathname.lastIndexOf(VALUES, 0) > -1) { /* url of form /VALUES-mapID:{key} */
      let splitPath = req_url.pathname.split('/')
      let valueID = splitPath[1].substring(VALUES.length-1)
      if (splitPath.length == 2)
        getNameParts(req, res, valueID, handleValueMethods)
      else  
        lib.notFound(req, res)      
    } else if (req_url.pathname.lastIndexOf('/maps;', 0) > -1) { /* url of form /maps;ns:name?????? */
      let splitPath = req_url.pathname.split('/')
      let mapFullName = splitPath[1].substring('maps;'.length)
      getNameParts(req, res, mapFullName, function(ns, name) {
        ps.withMapByNameDo(req, res, ns, name, function(map, mapID, etag) {
          if (splitPath.length == 2)
            handleMapMethods(mapID)
          else if (splitPath.length == 3 && splitPath[2] == 'entries') /* url of form /maps;ns:name/entries */
            if (req.method == 'GET')
              handleEntriesRequest(mapID)
            else
              lib.methodNotAllowed(req, res, ['GET'])
          else if (splitPath.length == 3 && splitPath[2].lastIndexOf('entries;',0) > -1) /* url of form /maps;ns:name/entries;{key} */
            handleEntryMethods(mapID, splitPath[2].substring('entries;'.length))
          else if (splitPath.length == 4 && splitPath[2].lastIndexOf('entries;',0) > -1 && splitPath[3] == 'value') /* url of form /maps;ns:name/entries;{key}/value */
            handleValueMethods(mapID, splitPath[2].substring('entries;'.length))
          else
            lib.notFound(req, res)
        })
      })
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