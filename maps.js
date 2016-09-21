'use strict';
var http = require('http')
var url = require('url')
var lib = require('http-helper-functions')
var uuid = require('node-uuid')
var ps = require('./maps-persistence.js')

var MAPS    = '/bWFw-'
var ENTRIES = '/ZW50-'
var VALUES  = '/dmFs-'

function verifyMap(req, map, user) {
  var rslt = lib.setStandardCreationProperties(req, map, user)
  if (map.isA != 'Map') 
    return `invalid JSON: "isA" property not set to "Map" ${JSON.stringify(map)}`
  return null
}

function makeMapURL(req, mapID) {
  return `//${req.headers.host}${MAPS}${mapID}`
}

function addCalculatedMapProperties(req, map, selfURL) {
  map._self = selfURL; 
  map.entries = `${selfURL}/entries`
  map._permissions = `protocol://authority/permissions?${map._self}`;
  map._permissionsHeirs = `protocol://authority/permissions-heirs?${map._self}`;
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
        ps.createMapThen(req, res, id, selfURL, map, function() {
          addCalculatedMapProperties(req, map, selfURL)
          lib.created(req, res, map, map._self)
        });
      });
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
  entry._self = makeEntryURL(req, mapID, key)
  entry.valueRef = makeValueURL(req, mapID, key)
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
        ps.createEntryThen(req, res, mapID, key, entry, function() {
          addCalculatedEntryProperties(req, entry, mapID, key)
          lib.created(req, res, entry, entry._self)
        })
      })
    } 
  }  
}

function createValue(req, res, mapID, key, value) {
  var user = lib.getUser(req);
  if (user == null) {
    lib.unauthorized(req, res);
  } else {
    if (req.headers['content-type'] != null) {
      lib.ifAllowedThen(req, res, makeMapURL(req, mapID), '_resource', 'create', function() {
        ps.upsertValueThen(req, res, mapID, key, value, function() {
          lib.created(req, res, null, makeValueURL(req, mapID, key));
        })
      })
    } else
      lib.badRequest(res, 'must provide Content-Type header')
  }  
}

function getMap(req, res, id) {
  lib.ifAllowedThen(req, res, null, '_resource', 'read', function() {
    ps.withMapDo(req, res, id, function(map) {
      addCalculatedMapProperties(req, map, makeMapURL(req, id))
      map._permissions = `protocol://authority/permissions?${map._self}`;
      map._permissionsHeirs = `protocol://authority/permissions-heirs?${map._self}`;
      lib.externalizeURLs(map, req.headers.host);
      lib.found(req, res, map);
    });
  });
}

function deleteMap(req, res, id) {
  lib.ifAllowedThen(req, res, null, 'delete', function() {
    ps.deleteMapThen(req, res, id, function (map) {
      lib.found(req, res, map);
    });
  });
}

function updateMap(req, res, id, patch) {
  lib.ifAllowedThen(req, res, null, 'update', function(map) {
    var patchedMap = lib.mergePatch(map, patch);
    ps.updateMapThen(req, res, id, map, patchedMap, function () {
      patchedPermissions._self = selfURL(id, req); 
      lib.found(req, res, map);
    });
  });
}

function requestHandler(req, res) {
  if (req.url == '/maps') 
    if (req.method == 'POST')
      lib.getServerPostObject(req, res, createMap)
    else
      lib.methodNotAllowed(req, res, ['POST'])
  else {
    var req_url = url.parse(req.url);
    if (req_url.pathname.lastIndexOf(MAPS, 0) > -1) {
      let splitPath = req_url.pathname.split('/')
      if (splitPath.length == 2) {
        let id = req_url.pathname.substring(MAPS.length);
        if (req.method == 'GET') {
          getMap(req, res, id);
        } else if (req.method == 'DELETE') { 
          deleteMap(req, res, id);
        } else if (req.method == 'PATCH') { 
          lib.getPostBody(req, res, function (req, res, jso) {
            updateMap(req, res, id, jso)
          });
        } else 
          lib.methodNotAllowed(req, res, ['GET', 'DELETE', 'PATCH']);
      } else if (splitPath.length == 3 && splitPath[2] == 'entries') { /* url of form /MAPS-xxxxxx/entries */
        if (req.method == 'POST') {
          lib.getServerPostObject(req, res, function(req, res, entry) {
            createEntry(req, res, splitPath[1].substring(MAPS.length-1), entry)
          })
        } else 
          lib.methodNotAllowed(req, res, ['GET', 'DELETE', 'PATCH'])
      }
    } else if (req_url.pathname.lastIndexOf(VALUES, 0) > -1) {
      let splitPath = req_url.pathname.split('/')
      if (splitPath.length == 2) {
        if (req.method == 'PUT') {
          let id = req_url.pathname.substring(VALUES.length);
          let splitID = id.split(':')
          if (splitID.length == 2) {
            let [mapID, key] = splitID
            lib.getServerPostText(req, res, function(req, res, value) {
              createValue(req, res, mapID, key, value)
            })
          } else
            lib.badRequest(res, `invalid value id: ${id}`)
        } else 
          lib.methodNotAllowed(req, res, ['PUT'])
      } else  
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