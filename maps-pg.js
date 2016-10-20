'use strict';
var Pool = require('pg').Pool;

var config = {
  host: process.env.PG_HOST,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE
};

var pool = new Pool(config);

// The following function adapted from https://github.com/broofa/node-uuid4 under MIT License
// Copyright (c) 2010-2012 Robert Kieffer
const randomBytes = require('crypto').randomBytes
var toHex = Array(256)
for (var val = 0; val < 256; val++) 
  toHex[val] = (val + 0x100).toString(16).substr(1)
function uuid() {
  var buf = randomBytes(16)
  buf[6] = (buf[6] & 0x0f) | 0x40
  buf[8] = (buf[8] & 0x3f) | 0x80
  var i=0
  return  toHex[buf[i++]] + toHex[buf[i++]] +
          toHex[buf[i++]] + toHex[buf[i++]] + '-' +
          toHex[buf[i++]] + toHex[buf[i++]] + '-' +
          toHex[buf[i++]] + toHex[buf[i++]] + '-' +
          toHex[buf[i++]] + toHex[buf[i++]] + '-' +
          toHex[buf[i++]] + toHex[buf[i++]] +
          toHex[buf[i++]] + toHex[buf[i++]] +
          toHex[buf[i++]] + toHex[buf[i++]]
}
// End of section of code adapted from https://github.com/broofa/node-uuid4 under MIT License

function makeMapID(map, callback) {
  callback(null, uuid())
}

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
  valueData.etag = uuid()
  var query = `INSERT INTO values (mapid, key, valuedata, value) values('${mapid}', '${key}', '${JSON.stringify(valueData)}', '${value}') ON CONFLICT (mapid, key) DO UPDATE SET (etag, valuedata, value) = (EXCLUDED.etag, EXCLUDED.valuedata, EXCLUDED.value)`;
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
        callback(null, row.valuedata, row.value, row.valuedata.etag)
      }
  })
}

function withMapFromNameDo(name, callback) {
  pool.query(`SELECT id, etag, data FROM maps WHERE data @> '{"fullName": "${name}"}'`, function (err, pg_res) {
    if (err) 
      callback(err)
    else if (pg_res.rowCount === 0)
      callback(404)
    else if (pg_res.rowCount > 1)
      callback(409)
    else {
      var row = pg_res.rows[0];
      callback(null, row.id, row.data, row.etag);
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
  pool.query(`SELECT mapid, key, etag, entrydata FROM values WHERE mapid = '${mapid}' and key = '${key}'`, function (err, pg_res) {
    if (err) 
      callback(err)
    else if (pg_res.rowCount === 0)  
      callback(404)
    else {
      var row = pg_res.rows[0]
      callback(null, row.entrydata, row.etag)
    }
  })
}

function deleteMapThen(id, callback) {
  pool.connect(function(err, client, release) {
    if (err)
      lib.internalError(res, err);
    else
      client.query('BEGIN', function(err) {
        if(err) {
          client.query('ROLLBACK', release);
          lib.internalError(res, err);
        } else 
          client.query(`DELETE FROM maps WHERE id = '${id}' RETURNING *`, function(err, pgResult) {
            if(err) {
              client.query('ROLLBACK', release);
              lib.badRequest(res, err)
            } else 
              if (pgResult.rowCount === 0) {
                client.query('COMMIT', release)
                callback(404)
              } else 
                client.query(`DELETE from values WHERE mapid = '${id}'`, function(err) {
                  if(err) {
                    client.query('ROLLBACK', release);
                    lib.internalError(res, err);
                  } else {
                    client.query('COMMIT', release);
                    var row = pgResult.rows[0];
                    callback(null, row.data, row.etag);
                  }
                })
          })
      })
  })
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

function init(callback) {
  var query = 'CREATE TABLE IF NOT EXISTS maps (id text primary key, etag text, data jsonb);'
  pool.query(query, function(err, pgResult) {
    if(err) 
      console.error(`error executing query ${query}`, err)
    query = 'CREATE TABLE IF NOT EXISTS values (mapid text, key text, etag text, entrydata jsonb, valuedata jsonb, value bytea, PRIMARY KEY (mapid, key));'
    pool.query(query, function(err, pgResult) {
      if(err) 
        console.error(`error executing query ${query}`, err)
      console.log('maps-db: connected to PG, config: ', config)
      callback()
    })
  })
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
exports.withMapFromNameDo = withMapFromNameDo
exports.withValueDo = withValueDo
exports.makeMapID = makeMapID
exports.init = init