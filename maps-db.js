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
  var query = `INSERT INTO maps (id, data) values('${id}', '${JSON.stringify(map)}')`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      callback();
    }
  });
}

function createEntryThen(mapid, key, map, callback) {
  var query = `INSERT INTO entries (mapid, key, data) values('${mapid}', '${key}', '${JSON.stringify(map)}')`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      callback();
    }
  });
}

function upsertValueThen(mapid, key, metadata, value, callback) {
  var query = `INSERT INTO values (mapid, key, metadata, value) values('${mapid}', '${key}', '${JSON.stringify(metadata)}', '${value}') ON CONFLICT (mapid, key) DO UPDATE SET (metadata, value) = (EXCLUDED.metadata, EXCLUDED.value)`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      callback();
    }
  });
}

function withMapDo(id, callback) {
  pool.query(`SELECT etag, data FROM maps WHERE id = '${id}'`, function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      if (pg_res.rowCount === 0) { 
        callback(404);
      }
      else {
        var row = pg_res.rows[0];
        callback(null, row.data);
      }
    }
  });
}

function withEntriesDo(mapid, callback) {
  pool.query(`SELECT * FROM entries WHERE mapid = '${mapid}'`, function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      callback(null, pg_res.rows);
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
        callback(null, row.data);
      }
    }
  });
}

function updateMapThen(id, patchedMap, callback) {
  var query = `UPDATE maps SET data = ('${JSON.stringify(patchedMap)}') WHERE id = '${id}'`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      if (pg_res.rowCount === 0) { 
        callback(404);
      }
      else {
        callback(null);
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
  var query = 'CREATE TABLE IF NOT EXISTS map (id text primary key, data jsonb);'
  executeQuery(query, function() {
    var query = 'CREATE TABLE IF NOT EXISTS entries (mapid text, key text, data jsonb, PRIMARY KEY (mapid, key));'
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

exports.createMapThen = createMapThen
exports.updateMapThen = updateMapThen
exports.deleteMapThen = deleteMapThen
exports.withMapDo = withMapDo
exports.createEntryThen = createEntryThen
exports.upsertValueThen = upsertValueThen
exports.withEntriesDo = withEntriesDo
exports.init = init