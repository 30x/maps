'use strict';
const lib = require('http-helper-functions')
const db = require(process.env.DBMS == 'pg' ? './maps-pg.js' : './maps-cass-cps.js')

function withErrorHandling(req, res, callback) {
  return function (err) {
    if (err == 404) 
      lib.notFound(req, res)
    else if (err)
      lib.internalError(res, err)
    else 
      callback.apply(this, Array.prototype.slice.call(arguments, 1))
  }
}

function makeMapID(req, res, orgID, map, callback) {
  db.makeMapID(orgID, map, withErrorHandling(req, res, callback))   
}

function createMapThen(req, res, mapID, selfURL, map, callback) {
  lib.internalizeURLs(map, req.headers.host);
  db.createMapThen(mapID, map, withErrorHandling(req, res, callback)) 
}

function createEntryThen(req, res, mapID, key, entry, callback) {
  lib.internalizeURLs(entry, req.headers.host);
  db.createEntryThen(mapID, key, entry, withErrorHandling(req, res, callback));
}

function upsertValueThen(req, res, mapID, key, value, callback) {
  var valueData = {isA: 'MapEntry', 'Content-Type': req.headers['content-type'], key: key, modifier: lib.getUser(req.headers.authorization), modified: new Date().toISOString()}
  db.upsertValueThen(mapID, key, valueData, value, withErrorHandling(req, res, callback)); 
}

function withMapDo(req, res, mapID, callback) {
  db.withMapDo(mapID, withErrorHandling(req, res, callback));
}

function withEntryDo(req, res, mapID, key, callback) {
  db.withEntryDo(mapID, key, withErrorHandling(req, res, callback));
}

function withValueDo(req, res, mapID, key, callback) {
  db.withValueDo(mapID, key, withErrorHandling(req, res, callback));
}

function withMapFromNameDo(req, res, name, callback) {
  db.withMapFromNameDo(name, withErrorHandling(req, res, callback));
}

function withMapFromOrgIDAndMapNameDo(req, res, name, callback) {
  db.withMapFromOrgIDAndMapNameDo(name, withErrorHandling(req, res, callback));
}

function withEntriesDo(req, res, mapID, callback) {
  db.withEntriesDo(mapID, withErrorHandling(req, res, callback));
}

function deleteMapThen(req, res, mapID, callback) {
  db.deleteMapThen(mapID, withErrorHandling(req, res, callback));
}

function updateMapThen(req, res, mapID, patchedMap, callback) {
  lib.internalizeURLs(patchedMap, req.headers.host);
  var key = lib.internalizeURL(mapID, req.headers.host);
  db.updateMapThen(mapID, patchedMap, withErrorHandling(req, res, callback));
}

function init(callback) {
  db.init(callback)
}

exports.createMapThen = createMapThen
exports.updateMapThen = updateMapThen
exports.deleteMapThen = deleteMapThen
exports.withMapDo = withMapDo
exports.createEntryThen = createEntryThen
exports.upsertValueThen = upsertValueThen
exports.withEntriesDo = withEntriesDo
exports.withEntryDo=withEntryDo
exports.withMapFromOrgIDAndMapNameDo = withMapFromOrgIDAndMapNameDo
exports.withValueDo = withValueDo
exports.makeMapID = makeMapID
exports.db=db
exports.init = init