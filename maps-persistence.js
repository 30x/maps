'use strict';
var db = require('./maps-pg.js');
var lib = require('http-helper-functions');

function withErrorHandling(callback) {
  return function (err) {
    if (err == 404) 
      lib.notFound(req, res)
    else if (err)
      lib.internalError(res, err)
    else 
      callback.apply(this, Array.prototype.slice.call(arguments, 1))
  }
}

function createMapThen(req, res, id, selfURL, map, callback) {
  lib.internalizeURLs(map, req.headers.host);
  db.createMapThen(id, map, withErrorHandling(callback)) 
}

function createEntryThen(req, res, mapID, key, entry, callback) {
  lib.internalizeURLs(entry, req.headers.host);
  db.createEntryThen(mapID, key, entry, withErrorHandling(callback));
}

function upsertValueThen(req, res, mapID, key, value, callback) {
  db.upsertValueThen(mapID, key, {'Content-Type': req.headers['content-type']}, value, withErrorHandling(callback));
}

function withMapDo(req, res, id, callback) {
  db.withMapDo(id, withErrorHandling(callback));
}

function withValueDo(req, res, mapID, key, callback) {
  db.withValueDo(mapID, key, withErrorHandling(callback));
}

function withMapByNameDo(req, res, ns, name, callback) {
  db.withMapByNameDo(ns, name, withErrorHandling(callback));
}

function withEntriesDo(req, res, id, callback) {
  db.withEntriesDo(id, withErrorHandling(callback));
}

function deleteMapThen(req, res, id, callback) {
  db.deleteMapThen(id, withErrorHandling(callback));
}

function updateMapThen(req, res, id, map, patchedMap, callback) {
  lib.internalizeURLs(patchedMap, req.headers.host);
  var key = lib.internalizeURL(id, req.headers.host);
  db.updateMapThen(id, patchedMap, withErrorHandling(callback));
}

function executeQuery(query, callback) {
  pool.query(query, withErrorHandling(callback));
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
exports.withMapByNameDo = withMapByNameDo
exports.withValueDo = withValueDo
exports.init = init