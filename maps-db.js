'use strict';
var Pool = require('pg').Pool;

var config = {
  host: process.env.PG_HOST,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE
};

var pool = new Pool(config);

function createMapThen(id, map, callback) {
  var query = `INSERT INTO maps (id, data) values('${id}', '${JSON.stringify(map)}') RETURNING etag`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      var row = pg_res.rows[0];
      callback(null, row.etag);
    }
  });
}

function withMapDo(id, callback) {
  pool.query('SELECT etag, data FROM maps WHERE id = $1', [id], function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      if (pg_res.rowCount === 0) { 
        callback(404);
      }
      else {
        var row = pg_res.rows[0];
        callback(null, row.data, row.etag);
      }
    }
  });
}

function deleteMapThen(id, callback) {
  var query = `DELETE FROM maps WHERE id = '${id}' RETURNING *`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      if (pg_res.rowCount === 0) { 
        callback(404);
      }
      else {
        var row = pg_res.rows[0];
        callback(null, row.data, row.etag);
      }
    }
  });
}

function updateMapThen(id, patchedMap, etag, callback) {
  var query = `UPDATE maps SET data = ('${JSON.stringify(patchedMap)}') WHERE subject = '${id}' AND etag = ${etag} RETURNING etag`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      if (pg_res.rowCount === 0) { 
        callback(404);
      }
      else {
        var row = pg_res.rows[0];
        callback(null, row.data, row.etag);
      }
    }
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
  var query = 'CREATE TABLE IF NOT EXISTS map (id text primary key, etag serial, data jsonb);'
  executeQuery(query, function() {
    var query = 'CREATE TABLE IF NOT EXISTS entries (mapid text, key text, etag serial, data jsonb, PRIMARY KEY (mapid, key));'
    executeQuery(query, function() {
      var query = 'CREATE TABLE IF NOT EXISTS values (mapid text, key text, metadata jsonb, value bytea, PRIMARY KEY (mapid, key));'
      executeQuery(query, function() {
        console.log('maps-db: connected to PG, config: ', config);
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