'use strict';
var Pool = require('pg').Pool;
var uuid = require('node-uuid')

var config = {
  host: process.env.PG_HOST,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE
};

var pool = new Pool(config);

function createMapThen(id, map, callback) {
  var etag = uuid()
  var query = `INSERT INTO maps (id, etag, data) values('${id}', '${etag}', '${JSON.stringify(map)}')`;
  pool.query(query, function (err, pg_res) {
    if (err) 
      callback(err)
    else
      callback(null, etag)
  })
}

function createEntryThen(mapid, key, entry, callback) {
  var etag = uuid()
  var query = `INSERT INTO values (mapid, key, etag, entrydata) values('${mapid}', '${key}', '${etag}', '${JSON.stringify(entry)}')`;
  pool.query(query, function (err, pg_res) {
    if (err)
      callback(err)
    else
      callback(null, etag)
  })
}

function upsertValueThen(mapid, key, valueData, value, callback) {
  var etag = uuid()
  var query = `INSERT INTO values (mapid, key, etag, valuedata, value) values('${mapid}', '${key}', '${etag}', '${JSON.stringify(valueData)}', '${value}') ON CONFLICT (mapid, key) DO UPDATE SET (etag, valuedata, value) = (EXCLUDED.etag, EXCLUDED.valuedata, EXCLUDED.value)`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      callback(null, etag);
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
        callback(null, row.data, row.etag);
      }
    }
  });
}

function withValueDo(mapID, key, callback) {
  var query = `SELECT etag, valuedata, value FROM values WHERE mapid = '${mapID}' AND key = '${key}'`
  pool.query(query, function (err, pg_res) {
    if (err) 
      callback(err)
    else
      if (pg_res.rowCount === 0)
        callback(404)
      else {
        var row = pg_res.rows[0];
        callback(null, row.valuedata, row.value, row.etag)
      }
  })
}

function withMapByNameDo(ns, name, callback) {
  pool.query(`SELECT id, etag, data FROM maps WHERE data @> '{"namespace": "${ns}", "name": "${name}"}'`, function (err, pg_res) {
    if (err) 
      callback(err)
    else if (pg_res.rowCount === 0) 
      callback(404)
    else {
      var row = pg_res.rows[0];
      callback(null, row.data, row.id, row.etag);
    }
  })
}

function withEntriesDo(mapid, callback) {
  pool.query(`SELECT mapid, key, etag, entrydata, valuedata FROM values WHERE mapid = '${mapid}'`, function (err, pg_res) {
    if (err)
      callback(err)
    else
      callback(null, pg_res.rows)
  })
}

function withEntryDo(mapid, key, callback) {
  pool.query(`SELECT mapid, key, etag, entrydata, valuedata FROM values WHERE mapid = '${mapid}' and key = '${key}'`, function (err, pg_res) {
    if (err) 
      callback(err)
    else if (pg_res.rowCount === 0)  
      callback(404)
    else {
      var row = pg_res.rows[0]
      callback(null, row.valuedata, row.etag)
    }
  })
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
  var etag = uuid()
  var query = `UPDATE maps SET (etag, data) = ('${etag}', '${JSON.stringify(patchedMap)}') WHERE id = '${id}'`;
  pool.query(query, function (err, pg_res) {
    if (err) {
      callback(err);
    }
    else {
      if (pg_res.rowCount === 0) { 
        callback(404);
      }
      else {
        callback(null, etag);
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
  var query = 'CREATE TABLE IF NOT EXISTS maps (id text primary key, etag text, data jsonb);'
  executeQuery(query, function() {
    var query = 'CREATE TABLE IF NOT EXISTS values (mapid text, key text, etag text, entrydata jsonb, valuedata jsonb, value bytea, PRIMARY KEY (mapid, key));'
    executeQuery(query, function() {
      console.log('maps-db: connected to PG, config: ', config);
      callback();
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
exports.withEntryDo = withEntryDo
exports.withMapByNameDo = withMapByNameDo
exports.withValueDo = withValueDo
exports.init = init