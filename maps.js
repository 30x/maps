'use strict';
const http = require('http')
const url = require('url')
const lib = require('http-helper-functions')
const pLib = require('permissions-helper-functions')
const ps = require('./maps-persistence.js')

var MAPS    = '/bWFw-'
var ENTRIES = '/ZW50-'
var VALUES  = '/dmFs-'

function verifyMapName(req, res, user, map, callback) {
  if (map.name === undefined) // unnamed map
    callback()
  else {
    var nameSplit = map.name.split(':')
    if (nameSplit.length < 2)
      lib.badRequest(res, `name must include a namespace identifier, a colon, and then a remainder`)
    else if (!/^\w+$/.test(nameSplit[0])) 
      lib.badRequest(res, `namespace must be alphanumeric or underscore`)    
    else if (nameSplit.slice(1).join(':').length == 0)
      lib.badRequest(res, `name must include a namespace identifier, a colon, and then a remainder`)
    else // both parts of the name present
      ps.db.withMapFromNameDo(map.name, function (err, map) {
        if (err != 404) 
          if (err)
            lib.badRequest(res, `unable to check for map name collision. err: ${err}`)
          else
            lib.duplicate(res, `duplicate map name ${map.namespace}:${map.name}`)
        else // not already there 
          callback()
      })
    }
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
    ps.makeMapID(req, res, map, function(mapID) {
      var selfURL = makeMapURL(req, mapID)
      pLib.createPermissionsFor(req.headers, selfURL, permissions, function(err, permissionsURL, permissions){
        if (err == 401)
          lib.forbidden(req, res)
        else if (err == 400)
          lib.badRequest(res, permissionsURL)
        else if (err == 500)
          lib.internalError(res, permissionsURL)
        else if (err == 403)
          lib.forbidden(req, res)
        else 
          // Create permissions first. If we fail after creating the permissions resource but before creating the main resource, 
          // there will be a useless but harmless permissions document.
          // If we do things the other way around, a map without matching permissions could cause problems.
          ps.createMapThen(req, res, mapID, selfURL, map, function(etag) {
            addCalculatedMapProperties(req, map, selfURL)
            lib.created(req, res, map, map.self, etag)
          })
      })    
    })
  }
  var user = lib.getUser(req.headers.authorization)
  if (user == null)
    lib.unauthorized(req, res)
  else { 
    var err = verifyMap(req, map, user)
    if (err !== null) 
      lib.badRequest(res, err)
    else {
      verifyMapName(req, res, user, map, function () {
        if (map.namespace === undefined)
          primCreateMap()
        else
          pLib.ifAllowedThen(req.headers, `/namespaces;${map.namespace}`, '_self', 'create', function(err, reason) {
            if (err)
              lib.internalError(res, reason)
            else
              primCreateMap()
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
  entry = entry ? entry : {isA: 'MapEntry'}
  entry.key = key
  entry.self = makeEntryURL(req, mapID, key)
  entry.value = makeValueURL(req, mapID, key)
  entry.map = makeMapURL(req, mapID)
  return entry
}

function createEntry(req, res, mapID, entry) {
  var user = lib.getUser(req.headers.authorization)
  if (user == null)
    lib.unauthorized(req, res)
  else { 
    var err = verifyEntry(req, entry, user)
    if (err !== null)
      lib.badRequest(res, err)
    else {
      lib.internalizeURLs(entry, req.headers.host)
      pLib.ifAllowedThen(req.headers, makeMapURL(req, mapID), '_self', 'create', function(err, reason) {
        if (err)
          lib.internalError(res, reason)
        else {
          var key = entry.key
          delete entry.key
          ps.createEntryThen(req, res, mapID, key, entry, function(etag) {
            addCalculatedEntryProperties(req, entry, mapID, key)
            lib.created(req, res, entry, entry.self, etag)
          })
        }
      })
    } 
  }  
}

function upsertValue(req, res, mapID, key, value) {
  var user = lib.getUser(req.headers.authorization);
  if (user == null)
    lib.unauthorized(req, res)
  else {
    if (req.headers['content-type'] != null) {
      pLib.ifAllowedThen(req.headers, makeMapURL(req, mapID), '_self', 'create', function(err, reason) {
        if (err)
          lib.internalError(res, reason)
        else
          ps.upsertValueThen(req, res, mapID, key, value, function(etag) {
            lib.found(req, res, null, etag, makeValueURL(req, mapID, key)) // Cassanda can't tell us if it was a create or an update, so we just pick one'
          })
      })
    } else
      lib.badRequest(res, 'must provide Content-Type header')
  }  
}

function getValue(req, res, mapID, key) {
  var user = lib.getUser(req.headers.authorization);
  if (user == null)
    lib.unauthorized(req, res);
  else
    pLib.ifAllowedThen(req.headers, makeMapURL(req, mapID), '_self', 'read', function(err, reason) {
      if (err)
        lib.internalError(res, reason)
      else
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
  pLib.ifAllowedThen(req.headers, makeMapURL(req, mapID), '_self', 'read', function(err, reason) {
    if (err)
      lib.internalError(res, reason)
    else
      ps.withMapDo(req, res, mapID, function(map, etag) {
        returnMap(req, res, map, mapID, etag)
      })
  })
}

function deleteMap(req, res, mapID) {
  var mapURL = makeMapURL(req, mapID)
  pLib.ifAllowedThen(req.headers, mapURL, '_self', 'delete', function(err, reason) {
    if (err)
      lib.internalError(res, reason)
    else
      ps.deleteMapThen(req, res, mapID, function (map, etag) {
        var permissionsURL = `/permissions?${mapURL}`
        lib.sendInternalRequest(req.headers, permissionsURL, 'DELETE', null, function (err, clientRes) {
          if (err)
            lib.internalError(res, err)
          else
            lib.getClientResponseBody(clientRes, function(body) {
              if (clientRes.statusCode == 200)  
                lib.found(req, res, map, etag)
              else
                lib.internalError(res, `failed to delete permissions: ${permissionsURL} status_code: ${clientRes.status_code} data: ${body}`);
            })
        })
      })
  })
}

function updateMap(req, res, mapID, patch) {
  pLib.ifAllowedThen(req.headers, makeMapURL(req, mapID), '_self', 'update', function(err, reason) {
    if (err)
      lib.internalError(res, reason)
    else
      ps.withMapDo(req, res, mapID, function(map) {
        doUpdateMap(req, res, mapID, map, patch)
      })
  })
}

function doUpdateMap(req, res, mapID, map, patch) {
  lib.applyPatch(req, res, map, patch, function(patchedMap) {
    function primUpdateMap() {
      ps.updateMapThen(req, res, mapID, patchedMap, function (etag) {
        addCalculatedMapProperties(req, patchedMap, makeMapURL(req, mapID)) 
        lib.found(req, res, patchedMap, etag);
      })    
    }
    verifyMapName(req, res, lib.getUser(req.headers.authorization), map, function() {
      if (patchedMap.name === undefined)
        primUpdateMap()
      else
        pLib.ifAllowedThen(req.headers, `/namespaces;${patchedMap.name.split(':')[0]}`, '_self', 'create', function(err, reason) {
          if (err)
            lib.internalError(res, reason)
          else
            primUpdateMap()
        })
    })
  })  
}

function getEntry(req, res, mapID, key) {
  pLib.ifAllowedThen(req.headers, makeMapURL(req, mapID), '_self', 'read', function(err, reason) {
    if (err)
      lib.internalError(res, reason)
    else
      ps.withEntryDo(req, res, mapID, key, function (entry, etag) {
        entry = addCalculatedEntryProperties(req, entry, mapID, key)
        lib.found(req, res, entry, etag, entry.self)
      });
  });
}

function deleteEntry(req, res, mapID, key) {
  pLib.ifAllowedThen(req.headers, makeMapURL(req, mapID), '_self', 'delete', function(err, reason) {
    if (err)
      lib.internalError(res, reason)
    else
      ps.deleteEntryThen(req, res, mapID, key, function (entry, etag) {
        entry = addCalculatedEntryProperties(req, entry, mapID, key)
        lib.found(req, res, entry, etag, entry.self)
      });
  });
}

function updateEntry(req, res, mapID, key, patch) {
  pLib.ifAllowedThen(req.headers, makeMapURL(req, mapID), '_self', 'update', function(err, reason) {
    if (err)
      lib.internalError(res, reason)
    else
      ps.withEntryDo(req, res, mapID, key, function(entry, etag) {
        lib.applyPatch(req, res, entry || {}, patch, function(patchedEntry) {
          ps.updateEntryThen(req, res, mapID, key, patchedEntry, function (etag) {
            addCalculatedMapProperties(req, patchedMap, makeEntryURL(req, mapID, key)) 
            lib.found(req, res, patchedMap, etag);
          })
        })    
      })
  })
}

function getEntries(req, res, mapID) {
  pLib.ifAllowedThen(req.headers, makeMapURL(req, mapID), '_self', 'read', function(err, reason) {
    if (err)
      lib.internalError(res, reason)
    else
      ps.withEntriesDo(req, res, mapID, function (entries) {
        var apiEntries = entries.map(x=>{
          var entrydata = x.entrydata || {}
          entrydata.etag = x.etag
          return addCalculatedEntryProperties(req, x.entrydata, x.mapid, x.key)
        }) 
        lib.found(req, res, {isA: 'Collection', self: '//' + req.headers.host + req.url, contents: apiEntries});
      })
  })
}

function getIDParts(req, res, mapFullName, callback) {
  var splitID = mapFullName.split(':')
  if (splitID.length > 1) {
    var mapID = splitID.slice(0,-1).join(':')
    var key = splitID[splitID.length - 1]
    callback(mapID, key)
  } else
    lib.badRequest(res, `ID must be composed of at least 2 parts separated by ":"s (${mapFullName})`)
}

function requestHandler(req, res) {
  function handleEntriesMethods(mapID) {
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
        updateEntry(req, res, mapID, key, value) 
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
  function handleMapPaths(splitPath, mapID) {
    if (splitPath.length == 2)
      handleMapMethods(mapID)
    else if (splitPath.length == 3 && splitPath[2] == 'entries') /* url of form /maps;ns:name/entries */
      handleEntriesMethods(mapID)
    else if (splitPath.length == 3 && splitPath[2].lastIndexOf('entries;',0) > -1) /* url of form /maps;ns:name/entries;{key} */
      handleEntryMethods(mapID, splitPath[2].substring('entries;'.length))
    else if (splitPath.length == 4 && splitPath[2].lastIndexOf('entries;',0) > -1 && splitPath[3] == 'value') /* url of form /maps;ns:name/entries;{key}/value */
      handleValueMethods(mapID, splitPath[2].substring('entries;'.length))
    else
      lib.notFound(req, res)  
  }
  if (req.url == '/maps') {
    if (req.method == 'POST')
      lib.getServerPostObject(req, res, createMap)
    else
      lib.methodNotAllowed(req, res, ['POST'])
  }
  else {
    var req_url = url.parse(req.url);
    if (req_url.pathname.lastIndexOf(MAPS, 0) > -1) { /* url of form /MAPS-xxxxxx */
      let splitPath = req_url.pathname.split('/')
      handleMapPaths(splitPath, splitPath[1].substring(MAPS.length-1))
    } else if (req_url.pathname.lastIndexOf(ENTRIES, 0) > -1) { /* url of form /ENTRIES-mapID:{key} */
      let splitPath = req_url.pathname.split('/')
      let entryID = splitPath[1].substring(ENTRIES.length-1)
      getIDParts(req, res, entryID, handleEntryMethods)
    } else if (req_url.pathname.lastIndexOf(VALUES, 0) > -1) { /* url of form /VALUES-mapID:{key} */
      let splitPath = req_url.pathname.split('/')
      let valueID = splitPath[1].substring(VALUES.length-1)
      getIDParts(req, res, valueID, handleValueMethods)
    } else if (req_url.pathname.lastIndexOf('/mapFromName;', 0) > -1) { /* url of form /maps;ns:name?????? */
      let splitPath = req_url.pathname.split('/')
      let name = splitPath[1].substring('mapFromName;'.length)
      ps.withMapFromNameDo(req, res, name, function(map, mapID, etag) { // todo align mapID returned from PG and CASS so we can use it
        handleMapPaths(splitPath, mapID)
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