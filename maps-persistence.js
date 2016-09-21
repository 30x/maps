'use strict';
var db = require('./maps-pg.js');
var lib = require('http-helper-functions');

function createMapThen(req, res, id, selfURL, map, callback) {
  lib.internalizeURLs(map, req.headers.host);
  db.createMapThen(id, map, function (err, etag) {
    if (err) 
      lib.internalError(res, err)
    else 
      callback(etag)
  }); 
}

function createEntryThen(req, res, mapID, key, entry, callback) {
  lib.internalizeURLs(entry, req.headers.host);
  db.createEntryThen(mapID, key, entry, function (err) {
    if (err) 
      lib.internalError(res, err)
    else 
      callback()
  });
}

function upsertValueThen(req, res, mapID, key, value, callback) {
  db.upsertValueThen(mapID, key, {'Content-Type': req.headers['content-type']}, value, function (err) {
    if (err) 
      lib.internalError(res, err)
    else 
      callback()
  });
}

function withMapDo(req, res, id, callback) {
  db.withMapDo(id, function (err, map) {
    if (err == 404) 
      lib.notFound(req, res)
    else if (err)
      lib.internalError(res, err)
    else 
      callback(map)
  });
}

function withValueDo(req, res, mapID, key, callback) {
  db.withValueDo(mapID, key, function (err, metadata, value) {
    if (err == 404) 
      lib.notFound(req, res)
    else if (err)
      lib.internalError(res, err)
    else 
      callback(metadata, value)
  });
}

function withMapByNameDo(req, res, ns, name, callback) {
  db.withMapByNameDo(ns, name, function (err, map, id) {
    if (err == 404) 
      lib.notFound(req, res)
    else if (err)
      lib.internalError(res, err)
    else 
      callback(map, id)
  });
}

function withEntriesDo(req, res, id, callback) {
  db.withEntriesDo(id, function (err, entries) {
    if (err == 404) 
      lib.notFound(req, res)
    else if (err)
      lib.internalError(res, entries)
    else 
      callback(entries)
  });
}

function deleteMapThen(req, res, id, callback) {
  db.deleteMapThen(id, function (err, map) {
    if (err == 404)
      lib.notFound(req, res)
    else if (err) 
      lib.internalError(res, err)
    else 
      callback(map)
  });
}

function updateMapThen(req, res, id, map, patchedMap, callback) {
  lib.internalizeURLs(patchedMap, req.headers.host);
  var key = lib.internalizeURL(id, req.headers.host);
  db.updateMapThen(id, patchedMap, function (err) {
    if (err == 404)
      lib.notFound(req, res)
    else if (err) 
      lib.internalError(res, err)
    else 
        callback()
  });
}

function executeQuery(query, callback) {
  pool.query(query, function(err, pgResult) {
    if(err) 
      console.error(`error executing query ${query}`, err);
    callback();
  });
}

function init(callback) {
  db.init(callback)
}

process.on('unhandledRejection', function(e) {
  console.log(e.message, e.stack)
})

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