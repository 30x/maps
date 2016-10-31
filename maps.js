'use strict';
const http = require('http')
const url = require('url')
const lib = require('http-helper-functions')
const pLib = require('permissions-helper-functions')
const ps = require('./maps-persistence.js')

const MAPS    = '/bWFw-'
const ENTRIES = '/ZW50-'
const VALUES  = '/dmFs-'
const MAPWIHORGID = '/bWFwd2lk-'

function verifyMapUpdate(req, res, map, patchedMap, callback) {
  if (map.org != patchedMap.org)
    return lib.badRequest(res, `may not change org of a map: ${map.org} -> ${patchedMap.org}`)
  if (map.name != patchedMap.name)
    return lib.badRequest(res, `may not change name of a map: ${map.name} -> ${patchedMap.name}`)
  if (map.fullName != patchedMap.fullName)
    return lib.badRequest(res, `may not change fullName of a map: ${map.fullName} -> ${patchedMap.fullName}`)
  callback()
}

function verifyMapOrg(req, res, map, callback) {
  console.log('verifyMapOrg')
  if (typeof map.org != 'string')
    return lib.badRequest(res, `invalid JSON: "org" property not set or not a string: ${map.org}`)
  var parsedOrg = url.parse(map.org)
  if (!parsedOrg.pathname.startsWith('/v1/o/'))
    return lib.badRequest(res, `org URL path must begin with /v1/o/ : ${map.org}`)
  var pathSplit = parsedOrg.pathname.split('/')
  if (pathSplit.length != 4)
    return lib.badRequest(res, `org URL path must be of form  /v1/o/{orgname} : ${map.org}`)
  callback(pathSplit[pathSplit.length - 1])
}

function verifyMap(req, res, orgID, map, user, callback) {
  var rslt = lib.setStandardCreationProperties(req, map, user)
  if (map.isA != 'Map') 
    return lib.badRequest(res, `invalid JSON: "isA" property not set to "Map" ${JSON.stringify(map)}`)
  if (map.fullName)
    return lib.badRequest(res, `may not set fullName of map directly. FullName is calculated from name and org`)
  if (typeof map.name != 'string')
    return lib.badRequest(res, `map name must be a string: ${map.name}`)
  map.orgID = orgID
  map.fullName = `${orgID}:${map.name}`
  ps.db.withMapFromOrgIDAndMapNameDo(map.fullName, function (err) {
    if (err != 404) 
      if (err)
        lib.badRequest(res, `unable to check for map name collision. err: ${err}`)
      else
        lib.duplicate(res, `duplicate map name ${map.name}`)
    else // not already there       
      callback()
  })
}

function makeMapURL(req, mapID) {
  return `//${req.headers.host}${MAPS}${mapID}`
}

function addCalculatedMapProperties(req, map, selfURL) {
  map.self = selfURL; 
  map.entries = `${selfURL}/entries`
  map._permissions = `scheme://authority/permissions?${map.self}`
  map._permissionsHeirs = `scheme://authority/permissions-heirs?${map.self}`
}

function createMap(req, res, orgID, map) {
  var user = lib.getUser(req.headers.authorization)
  if (user == null)
    lib.unauthorized(req, res)
  else
    verifyMap(req, res, orgID, map, user, function () {
      pLib.ifAllowedThen(req, res, map.org, 'maps', 'create', function() {
        lib.internalizeURLs(map, req.headers.host) 
        var permissions = map.permissions
        if (permissions !== undefined)
          delete map.permissions
        ps.makeMapID(req, res, orgID, map, function(mapID) {
          var selfURL = makeMapURL(req, mapID)
          pLib.createPermissionsThen(req, res, selfURL, permissions, function(permissionsURL, permissions){
            // Create permissions first. If we fail after creating the permissions resource but before creating the main resource, 
            // there will be a useless but harmless permissions document.
            // If we do things the other way around, a map without matching permissions could cause problems.
            ps.createMapThen(req, res, mapID, selfURL, map, function(etag) {
              addCalculatedMapProperties(req, map, selfURL)
              lib.created(req, res, map, map.self, etag)
            })
          })    
        })
      })
    })
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
  return `scheme://authority${ENTRIES}${mapID}:${key}`
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
      pLib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_self', 'create', function() {
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
  var user = lib.getUser(req.headers.authorization);
  if (user == null)
    lib.unauthorized(req, res)
  else {
    if (req.headers['content-type'] != null) {
      pLib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_self', 'create', function() {
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
    pLib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_self', 'read', function() {
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
  map._permissions = `scheme://authority/permissions?${map.self}`
  map._permissionsHeirs = `scheme://authority/permissions-heirs?${map.self}`
  lib.externalizeURLs(map, req.headers.host)
  lib.found(req, res, map, etag)
}

function getMap(req, res, mapID) {
  pLib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_self', 'read', function() {
    ps.withMapDo(req, res, mapID, function(map, etag) {
      returnMap(req, res, map, mapID, etag)
    })
  })
}

function deleteMap(req, res, mapID) {
  var mapURL = makeMapURL(req, mapID)
  pLib.ifAllowedThen(req, res, mapURL, '_self', 'delete', function() {
    ps.deleteMapThen(req, res, mapID, function (map, etag) {
      var permissionsURL = `/permissions?${mapURL}`
      lib.sendInternalRequestThen(req, res, permissionsURL, 'DELETE', null, function (clientRes) {
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
  pLib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_self', 'update', function() {
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
    verifyMapUpdate(req, res, map, patchedMap, function() {
      primUpdateMap()
    })
  })  
}

function getEntry(req, res, mapID, key) {
  pLib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_self', 'read', function() {
    ps.withEntryDo(req, res, mapID, key, function (entry, etag) {
      entry = addCalculatedEntryProperties(req, entry, mapID, key)
      lib.found(req, res, entry, etag, entry.self)
    });
  });
}

function deleteEntry(req, res, mapID, key) {
  pLib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_self', 'delete', function() {
    ps.deleteEntryThen(req, res, mapID, key, function (entry, etag) {
      entry = addCalculatedEntryProperties(req, entry, mapID, key)
      lib.found(req, res, entry, etag, entry.self)
    });
  });
}

function updateEntry(req, res, mapID, key, patch) {
  pLib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_self', 'update', function() {
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
  pLib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_self', 'read', function() {
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

function getOrgID(req, res, orgName, callback){
  lib.sendInternalRequestThen(req, res, '/orgs;'+orgName, 'GET', null, {}, function(clientRes) {
    lib.getClientResponseBody(clientRes, function(body) {
      if (clientRes.statusCode !== 200)
        lib.internalError(res, `unable to retrieve orgID. statusCode = ${clientRes.statusCode} url: /orgs;${orgName} body: ${body}`)
      else {
          var org = JSON.parse(body)
          callback(`${org.kvmRing}:${org.kvmKeyspace}:${org.tenantID}`)
      }
    })
  })
}

function resolveOrgThenForward(req, res, orgName, remainder, buffer) {
  function sendInternal(url, body) {
    lib.sendInternalRequestThen(req, res, url, req.method, body, req.headers, function (clientRes) {
      lib.getClientResponseBuffer(clientRes, function(buffer) {
        delete clientRes.headers.connection
        delete clientRes.headers['content-length']
        lib.respond(req, res, clientRes.statusCode, clientRes.headers, buffer)
      })
    })
  }  
  getOrgID(req, res, orgName, function(orgID) {
    var url = `${MAPWIHORGID}${orgID}${remainder}` 
    if (buffer)
      sendInternal(url, buffer)
    else
      lib.getServerPostBuffer(req, function(buffer) {
        sendInternal(url, buffer)
      })
  })
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
      lib.getServerPostBuffer(req, function(value) {
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
      lib.getServerPostBuffer(req, function(value) {
        upsertValue(req, res, mapID, key, value) 
      })
    else if (req.method == 'DELETE') 
      deleteValue(req, res, mapID, key)
    else
      lib.methodNotAllowed(req, res, ['GET', 'PUT', 'DELETE'])
  }
  function handleMapPaths(splitPath, mapID) {
    if (splitPath.length == 2) // url of form /MAPS-{mapID}
      handleMapMethods(mapID)
    else if (splitPath.length == 3 && splitPath[2] == 'entries') // url of form /MAPS-{mapID}/entries 
      handleEntriesMethods(mapID)
    else if (splitPath.length == 3 && splitPath[2].lastIndexOf('entries;',0) > -1) // url of form /MAPS-{mapID}/entries;{key} 
      handleEntryMethods(mapID, splitPath[2].substring('entries;'.length))
    else if (splitPath.length == 4 && splitPath[2].lastIndexOf('entries;',0) > -1 && splitPath[3] == 'value') // url of form /MAPS-{mapID}/entries;{key}/value 
      handleValueMethods(mapID, splitPath[2].substring('entries;'.length))
    else
      lib.notFound(req, res)  
  }
  if (req.url == '/maps') {
    if (req.method == 'POST')
      lib.getServerPostBuffer(req, function(buffer) {
        var map = JSON.parse(buffer)
        verifyMapOrg(req, res, map, function(orgName) {
          resolveOrgThenForward(req, res, orgName, '', buffer)
        })
      })
    else
      lib.methodNotAllowed(req, res, ['POST'])
  }
  else {
    var req_url = url.parse(req.url);
    if (req_url.pathname.startsWith(MAPS)) { // url of form /MAPS{map-id}...
      let splitPath = req_url.pathname.split('/')
      handleMapPaths(splitPath, splitPath[1].substring(MAPS.length-1))
    } else if (req_url.pathname.startsWith(ENTRIES)) { /* url of form /ENTRIES{map-id}:{key} */
      let splitPath = req_url.pathname.split('/')
      let entryID = splitPath[1].substring(ENTRIES.length-1)
      getIDParts(req, res, entryID, handleEntryMethods)
    } else if (req_url.pathname.startsWith(VALUES)) { /* url of form /VALUES{map-id}:{key} */
      let splitPath = req_url.pathname.split('/')
      let valueID = splitPath[1].substring(VALUES.length-1)
      getIDParts(req, res, valueID, handleValueMethods)
    } else if (req_url.pathname.startsWith(MAPWIHORGID)) { /* url of form /MAPWIHORGID{org-id}:{map-name}... */
      let splitPath = req_url.pathname.split('/')
      let orgIDAndMapName = splitPath[1].substring(MAPWIHORGID.length - 1)
      if (req.method == 'POST')
        lib.getServerPostObject(req, res, function(req, res, map) {createMap(req, res, orgIDAndMapName, map)})
      else
        ps.withMapFromOrgIDAndMapNameDo(req, res, orgIDAndMapName, function(mapID) {
          handleMapPaths(splitPath, mapID)
        })
    } else if (req_url.pathname.startsWith('/maps;')) { /* url of form /maps;{org-name}:{map-name}... */
      let splitPath = req_url.pathname.split('/')
      let orgName = splitPath[1].substring('maps;'.length).split(':')[0]
      resolveOrgThenForward(req, res, orgName, req_url.pathname.substring(`/maps;${orgName}`.length))
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