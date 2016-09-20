'use strict';
var Pool = require('pg').Pool;
var lib = require('http-helper-functions');

var config = {
  host: process.env.PG_HOST,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE
};

var pool = new Pool(config);

function createMapThen(req, res, id, selfURL, map, callback) {
  lib.internalizeURLs(map, req.headers.host);
  var query = `INSERT INTO maps (id, data) values('${id}', '${JSON.stringify(map)}') RETURNING etag`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      lib.internalError(res, err);
    }
    else {
      var row = pg_res.rows[0];
      callback(row.etag);
    }
  });
}

function withMapDo(req, res, id, callback) {
  pool.query('SELECT etag, data FROM maps WHERE id = $1', [id], function (err, pg_res) {
    if (err) {
      lib.internalError(res, err);
    }
    else {
      if (pg_res.rowCount === 0) { 
        lib.notFound(req, res);
      }
      else {
        var row = pg_res.rows[0];
        callback(row.data, row.etag);
      }
    }
  });
}

function deleteMapThen(req, res, id, callback) {
  var query = `DELETE FROM maps WHERE id = '${id}' RETURNING *`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      lib.internalError(res, err);
    }
    else {
      if (pg_res.rowCount === 0) { 
        lib.notFound(req, res);
      }
      else {
        var row = pg_res.rows[0];
        callback(row.data, row.etag);
      }
    }
  });
}

function updateMapThen(req, res, id, map, patchedMap, etag, callback) {
  lib.internalizeURLs(patchedMap, req.headers.host);
  var key = lib.internalizeURL(id, req.headers.host);
  var query = `UPDATE maps SET data = ('${JSON.stringify(patchedMap)}') WHERE subject = '${key}' AND etag = ${etag} RETURNING etag`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      lib.internalError(res, err);
    }
    else {
      if (pg_res.rowCount === 0) { 
        lib.notFound(req, res);
      }
      else {
        var row = pg_res.rows[0];
        callback(row.data, row.etag);
      }
    }
  });
}

function createDB(table, callback) {
  var query = `CREATE TABLE IF NOT EXISTS ${table} (id text primary key, etag serial, data jsonb);`
  pool.query(query, function(err, pgResult) {
    if(err) 
      console.error(`error creating ${table} table`, err);
    callback();
  });
}

function init(callback) {
  createDB('maps', function() {
    createDB('entries', function() {
      createDB('values', function() {
        console.log(`connected to PG at ${config.host}`);
        callback();
      });
    });
  });
}

process.on('unhandledRejection', function(e) {
  console.log(e.message, e.stack)
})

exports.createMapThen = createMapThen;
exports.updateMapThen = updateMapThen;
exports.deleteMapThen = deleteMapThen;
exports.withMapDo = withMapDo;
exports.init = init;