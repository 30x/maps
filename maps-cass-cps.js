'use strict'
const cassandra = require('cassandra-driver')
const kvmEntryPartitionCount = process.env.MAPS_KVM_ENTRY_PARTITION_COUNT || 32
const config = {
  databaseHosts: process.env.CASS_HOSTS ? process.env.CASS_HOSTS.split(",") : ['localhost'],
  databasePort: process.env.CASS_PORT ? process.env.CASS_PORT : 9042,
  databaseKeyspace: process.env.CASS_KEYSPACE || 'kvm_acme_prod',
  databaseReplication: process.env.CASS_REPLICATION ? process.env.CASS_REPLICATION : "'datacenter1':'1'", // comma separate single quote key/value pairs
  databaseLocalDC: process.env.CASS_LOCAL_DC || 'datacenter1',
  databaseLoadBalancingPolicy: cassandra.policies.loadBalancing.DCAwareRoundRobinPolicy(this.databaseLocalDC),
  databaseReadTimeoutMillis: process.env.CASS_READ_TIMEOUT ? parseInt(process.env.CASS_READ_TIMEOUT) : 10000,
  databaseUsername: process.env.CASS_USERNAME || 'cassandra',
  databasePassword: process.env.CASS_PASSWORD || 'cassandra',
  databaseConnectionsPerHost: process.env.CASS_CONN_PER_HOST ? parseInt(process.env.CASS_CONN_PER_HOST) : 30,
  databaseReadConsistency: getCassandraCL(process.env.CASS_READ_CL),
  databaseWriteConsistency: getCassandraCL(process.env.CASS_WRITE_CL),
  databaseReadConsistencyStrong: getCassandraCL(process.env.CASS_READ_CL_STRONG)
}
const distance = cassandra.types.distance;
const client = new cassandra.Client({
  contactPoints: config.databaseHosts,
  pooling: {
    coreConnectionsPerHost: {
      [distance.local]: config.databaseConnectionsPerHost,
      [distance.remote]: config.databaseConnectionsPerHost
    }
  },
  socketOptions: {
    port: config.databasePort,
    readTimeout: config.databaseReadTimeoutMillis
  },
  authProvider: new cassandra.auth.PlainTextAuthProvider(config.databaseUsername, config.databasePassword),
  queryOptions: {
    consistency: config.databaseReadConsistency
  }

})
const SEPARATOR = ':'


// inserts
function insertMap(keyspace){ return 'INSERT INTO ' + keyspace + '.kvm_map_descriptor (tid, a_scp, map, attr, c_at, c_by, u_at, u_by, ver) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'}
function insertIndex(keyspace){ return 'INSERT INTO ' + keyspace + '.indexes (name, mapid) VALUES (?, ?)'}
function insertMapEntry(keyspace){ return  'INSERT INTO ' + keyspace + '.kvm_map_keys_descriptor (tid, a_scp, map, ver, part_id, key, c_at, c_by, u_at, u_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'}
function insertMapValue(keyspace){ return 'INSERT INTO ' + keyspace + '.kvm_map_entry (tid, a_scp, map, ver, key, c_at, c_by, u_at, u_by, val) VALUES (? ,?, ?, ?, ?, ?, ?, ?, ?, ?)'}

// selects
function selectMap(keyspace ){ return 'SELECT * FROM ' + keyspace + '.kvm_map_descriptor WHERE tid = ? and a_scp = ? and map = ?'}
function selectmapIdFromName(keyspace){ return 'SELECT mapid FROM ' + keyspace + '.indexes WHERE name = ?'}
function selectEntries(keyspace){ return 'SELECT * FROM ' + keyspace + '.kvm_map_keys_descriptor WHERE tid = ? and a_scp = ? and map = ? and ver = ? and part_id = ?'}
function selectEntry(keyspace){ return 'SELECT * FROM ' + keyspace + '.kvm_map_keys_descriptor WHERE tid = ? and a_scp = ? and map = ? and ver = ? and part_id = ? and key = ?'}
function selectValue(keyspace){ return 'SELECT * FROM ' + keyspace + '.kvm_map_entry WHERE tid = ? and a_scp = ? and map = ? and ver = ? and key = ?'}

//deletes
function deleteMap(keyspace){ return 'DELETE FROM ' + keyspace + '.kvm_map_descriptor WHERE tid = ? and a_scp = ? and map = ?'}
function deleteMapIndex(keyspace){ return 'DELETE FROM ' + keyspace + '.indexes WHERE name = ?'}
function deleteMapEntry(keyspace){ return 'DELETE FROM ' + keyspace + '.kvm_map_keys_descriptor WHERE tid = ? and a_scp = ? and map = ? and ver = ? and part_id = ? and key = ?'}
function deleteMapValue(keyspace){ return 'DELETE FROM ' + keyspace + '.kvm_map_entry WHERE tid = ? and a_scp = ? and map = ? and ver = ? and key = ?'}


function createMapThen(mapId, map, callback) {
  let [ns, scope, name] = mapId.split(':')
  const batchQueries = [
    {query: insertMap(config.databaseKeyspace), params: [ns, scope, name, {}, map.created, map.creator, map.created, map.creator, cassandra.types.TimeUuid.now()]},
  ]
  if (map && map.id && map.id != mapId) // change to a map name here
    batchQueries.push({query: insertIndex(config.databaseKeyspace), params: [mapId, map.name]})
  client.batch(batchQueries, {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else {
      //console.log('Added map=' + JSON.stringify(map)+')
      callback(null, null) // inserts do not generate result rows
    }
  })
}

function updateMapThen(mapId, patchedMap, callback) {
  let [ns, scope, name] = mapId.split(':')
  const batchQueries = [
    {query: insertMap(config.databaseKeyspace), params: [ns, scope, name, {}, patchedMap.created, patchedMap.creator, patchedMap.created, patchedMap.creator, cassandra.types.TimeUuid.now()]},
  ]
  withMapDo(mapId, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else {
      // handle name changes
      if( patchedMap.name && patchedMap.name !== map.name){
        batchQueries.push({query: deleteMapIndex(config.databaseKeyspace), params: [map.name]})
        batchQueries.push({query: insertIndex(config.databaseKeyspace), params: [patchedMap.name, map.name]})
      }
      client.batch(batchQueries, {prepare: true}, function (err, result) {
        if (err) {
          callback(err)
        }
        else {
          callback(null, null) // inserts do not generate result rows
        }
      })
    }
  })
}


function createEntryThen(mapId, key, entry, callback) {
  withMapDo(mapId, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else{
      let [ns, scope, name] = map.name.split(':')
      client.execute(insertMapEntry(config.databaseKeyspace), [ns, scope, name, cassandra.types.Uuid.fromString(map.version), getMapEntryPartitionId(key), key, entry.created, entry.creator, entry.created, entry.creator], {prepare: true}, function (err, result) {
        if (err)
          callback(err)
        else {
          callback(null, null) // inserts do not generate result rows
        }

      })
    }
  })
}

function upsertValueThen(mapId, key, valuedata, value, callback) {
  createMapValue(mapId, key, valuedata, value, callback)
}

function createMapValue(mapId, key, valuedata, value, callback) {
  if (!(value instanceof String)) {
    value = value.toString()
    //callback(true, {error: 'Value must be a string'})
  }
  withMapDo(mapId, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else{
      let [ns, scope, name] = mapId.split(':')
      // purposely need two separate calls to cassandra as you can't execute a batch with 2 partitions ( tables ) with the condition IF NOT EXISTS
      client.execute(insertMapValue(config.databaseKeyspace), [ns, scope, name, cassandra.types.Uuid.fromString(map.version), key, valuedata.created, valuedata.creator, valuedata.created, valuedata.creator, value], {prepare: true}, function (err, result) {
        if (err)
          callback(err)
        else {
          client.execute(insertMapEntry(config.databaseKeyspace) + 'IF NOT EXISTS',  [ns, scope, name, cassandra.types.Uuid.fromString(map.version), getMapEntryPartitionId(key), key, valuedata.created, valuedata.creator, valuedata.created, valuedata.creator], {prepare: true}, function (err, result) {
            if (err)
              callback(err)
            else
              callback(null, null) // inserts do not generate result rows
          })
        }
      })
    }
  })
}


function withMapDo(mapId, callback) {
  let [ns, scope, name] = mapId.split(':')
  client.execute(selectMap(config.databaseKeyspace), [ns, scope, name], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else if ( result.rows.length === 0)
      callback(404)
    else {
      var map = {}
      if (result.rows.length > 0) {
        map.name = getMapId(result.rows[0].get('tid'), result.rows[0].get('a_scp'), result.rows[0].get('map'))
        map.created = result.rows[0].get('c_at')
        map.createdBy = result.rows[0].get('c_by')
        map.updated = result.rows[0].get('u_at')
        map.updatedBy = result.rows[0].get('u_by')
        map.version = result.rows[0].get('ver').toString()
        map.attributes = result.rows[0].get('attr')
      }
      callback(null, map, null)
    }
  })
}

function withMapByNameDo(compoundName, callback) {
  getMapAlias(compoundName, function(err, compoundNameFromIndex){
    if (err)
      callback(err)
    else{
     var mapId =  compoundNameFromIndex !== null ? compoundNameFromIndex : compoundName
     withMapDo(mapId, function (err, map, etag) {
        if (err)
          callback(err)
        else if (map == null)
          callback(404)
        else
          callback(null, map, compoundNameFromIndex, etag)
      })
    }
  })
}


function getMapAlias(compoundName, callback) {
  client.execute(selectmapIdFromName(config.databaseKeyspace), [compoundName], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else if ( result.rows.length === 0)
      callback(404)
    else if ( result.rows.length > 1)
      callback(true, {error:'More than one UUID mapped to map name'})
    else
      callback(null, result.rows.length > 0 ? result.rows[0].get('mapid').toString() : null)
  })
}


function withEntriesDo(mapId, callback) {
  withMapDo(mapId, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else {
      let [ns, scope, name] = map.name.split(':')
      // TODO page this and accept in a cursor string in withEntriesDo, will need to calculate part_id with getMapEntryPartitionId(key)
      client.execute(selectEntries(config.databaseKeyspace), [ns, scope, name, cassandra.types.Uuid.fromString(map.version), 0], {prepare: true}, function (err, result) {
        if (err)
          callback(err)
        else {
          if (result.rows.length > 0) {
            var processed = 0;
            result.rows.forEach(function (entry) {
              processed++
              var key = entry.key;
              var name = getMapId(entry.tid, entry.scope, entry.map);
              var entrydata = {
                name: name,
                created: entry.created,
                createdBy: entry.createdBy,
                updated: entry.updated,
                updatedBy: entry.updatedyBy

              }
              entry = {
                mapid: name,
                key: key,
                entrydata: entrydata,
                etag: null
              }

              if (processed === result.rows.length)
                callback(null, result.rows) // we're done looping, now invoke callback
            })
          }
          else
            callback(null, [])
        }
      })
    }
  })
}


function withEntryDo(mapId, key, callback) {
  withMapDo(mapId, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else{
      let [ns, scope, name] = map.name.split(':')
      client.execute(selectEntry(config.databaseKeyspace), [ns, scope, name, cassandra.types.Uuid.fromString(map.version), getMapEntryPartitionId(key), key], {prepare: true}, function (err, result) {
        if (err)
          callback(err)
        else {
          var key, etag, entrydata
          if (result.rows.length > 0) {
            var name = getMapId(result.rows[0].get('tid'), result.rows[0].get('a_scp'), result.rows[0].get('map'));
            key = result.rows[0].get('key')
            entrydata = {
              name: name,
              created: result.rows[0].get('c_at'),
              createdBy: result.rows[0].get('c_by'),
              updated: result.rows[0].get('u_at'),
              updatedBy: result.rows[0].get('u_by')
            }
            etag = null
          }
          callback(null, entrydata, etag)
        }
      })
    }
  })
}


function withValueDo(mapId, key, callback) {
  withMapDo(mapId, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else{
      let [ns, scope, name] = map.name.split(':')
      client.execute(selectValue(config.databaseKeyspace), [ns, scope, name, cassandra.types.Uuid.fromString(map.version), key], {prepare: true}, function (err, result) {
        if (err)
          callback(err)
        else {
          var value, valuedata, etag
          if (result.rows.length > 0) {
            var name = getMapId(result.rows[0].get('tid'), result.rows[0].get('a_scp'), result.rows[0].get('map'));
            value = result.rows[0].get('val')
            valuedata = {
              name: name,
              created: result.rows[0].get('c_at'),
              createdBy: result.rows[0].get('c_by'),
              updated: result.rows[0].get('u_at'),
              updatedBy: result.rows[0].get('u_by')
            }
            etag = null
          }
          callback(null, valuedata, value, etag)
        }
      })
    }
  })
}


function removeMapEntry(mapId, key, callback) {
  withMapDo(mapId, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else{
      let [ns, scope, name] = map.name.split(':')
      client.execute(deleteMapEntry(config.databaseKeyspace), [ns, scope, name, cassandra.types.Uuid.fromString(map.version), getMapEntryPartitionId(key), key], {prepare: true}, function (err, result) {
        if (err)
          console.error('Error removing entry for mapid= ' + mapId + ', key=' + key)
        else
          callback(null, result)
      })
    }
  })
}


function removeMapValue(mapId, key, callback) {
  withMapDo(mapId, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else{
      let [ns, scope, name] = map.name.split(':')
      client.execute(deleteMapValue(config.databaseKeyspace), [ns, scope, name, cassandra.types.Uuid.fromString(map.version), key], {prepare: true}, function (err, result) {
        if (err)
          console.error('Error removing value for mapid= ' + mapId + ', key=' + key)
        else
          callback(null)
      })
    }
  })
}


function removeMapIndex(name, callback) {
  client.execute(deleteMapIndex(config.databaseKeyspace), [name], {prepare: true}, function (err, result) {
    if (err)
      console.error('Error removing map index for name= ' + name)
    else
      callback(null)
  })
}

function getMapId(ns, scope, name){
  return ns+SEPARATOR+scope+SEPARATOR+name;
}

function getMapEntryPartitionId(key){
  return Math.abs(javaStringHashCode(key) % kvmEntryPartitionCount )
}


/**
 * 1. Delete all entries and values for the given map
 * 2. Load map to obtain its version
 * 3. Delete the map
 * 4. Delete the map indexes
 */
function deleteMapThen(mapId, callback) {
  withMapDo(mapId, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else{
      let [ns, scope, name] = map.name.split(':')
      client.eachRow(selectEntries(config.databaseKeyspace), [ns, scope, name, cassandra.types.Uuid.fromString(map.version), 0], {prepare: true}, function (n, row) {
          //the callback will be invoked per each row as soon as they are received
          removeMapEntry(getMapId(row.get('tid'), row.get('a_scp'), row.get('map')), row.get('key'), function (err, result) {})
          removeMapValue(getMapId(row.get('tid'), row.get('a_scp'), row.get('map')), row.get('key'), function (err, result) {})
      }, function (err) {
          if (err)
            callback(err)
          else {
            client.execute(deleteMap(config.databaseKeyspace), [ns, scope, name], {prepare: true}, function (err, deleteResult) {
              if (err)
                console.error('Error removing map for mapid= ' + mapId)
              else {
                // remove any indexes
                removeMapIndex(mapId, function (err, result) {})
                removeMapIndex(map.name, function (err, result) {})
                callback(null, map, null)

              }
            })
          }
        })
    }
  })
}

function javaStringHashCode(string){
  string = string.toString()
  var hash = 0, i, chr, len;
  if (string.length === 0) return hash;
  for (i = 0, len = string.length; i < len; i++) {
    chr   = string.charCodeAt(i);
    hash  = ((hash << 5) - hash) + chr;
    hash |= 0; // Convert to 32bit integer
  }
  return hash;
}

function init(callback) {

  var keyspace = "CREATE KEYSPACE IF NOT EXISTS " + config.databaseKeyspace + " WITH replication = {'class': 'NetworkTopologyStrategy', " + config.databaseReplication + "}  AND durable_writes = true"
  var maps = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".kvm_map_descriptor ( tid text, a_scp text, map text, attr map < text, text >, c_at timestamp, c_by text, u_at timestamp, u_by text, ver timeuuid, PRIMARY KEY ((tid, a_scp), map)) WITH CLUSTERING ORDER BY (map ASC)"
  var names = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".indexes ( name text, mapid text, PRIMARY KEY ((name), mapid))"
  var entries = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".kvm_map_keys_descriptor ( tid text, a_scp text, map text, ver timeuuid, part_id int, key text, c_at timestamp, c_by text, u_at timestamp, u_by text, PRIMARY KEY ((tid, a_scp, map, ver, part_id), key)) WITH CLUSTERING ORDER BY (key ASC)"
  var values = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".kvm_map_entry ( tid text, a_scp text, map text, ver timeuuid, key text, c_at timestamp, c_by text, u_at timestamp, u_by text, val text, PRIMARY KEY ((tid, a_scp, map, ver, key)))"

  client.execute(keyspace, {}, function (err, result) {
    if (err)
      callback(err)
    else {
      client.execute(maps, {}, function (err, result) {
        if (err)
          callback(err)
        else {
          client.execute(names, {}, function (err, result) {
            if (err)
              callback(err)
            else {
              client.execute(entries, {}, function (err, result) {
                if (err)
                  callback(err)
                else {
                  client.execute(values, {}, function (err, result) {
                    if (err)
                      callback(err)
                    else {
                      console.log('Schema init success')
                      callback(null, {}) // creates do not generate result rows
                    }
                  })
                }
              })
            }
          })
        }
      })
    }
  })
}

function getCassandraCL(stringConsistencyLevel) {

  switch (stringConsistencyLevel) {

    case 'any':
      return cassandra.types.consistencies.any;
    case 'one':
      return cassandra.types.consistencies.one;
    case 'three':
      return cassandra.types.consistencies.three;
    case 'quorum':
      return cassandra.types.consistencies.quorum;
    case 'all':
      return cassandra.types.consistencies.all;
    case 'localQuorum':
      return cassandra.types.consistencies.localQuorum;
    case 'eachQuorum':
      return cassandra.types.consistencies.eachQuorum;
    case 'serial':
      return cassandra.types.consistencies.serial;
    case 'localSerial':
      return cassandra.types.consistencies.localSerial;
    case 'localOne':
      return cassandra.types.consistencies.localOne;
    default:
      return cassandra.types.consistencies.localQuorum;

  }
}

exports.createMapThen = createMapThen
exports.updateMapThen = updateMapThen
exports.createEntryThen = createEntryThen
exports.createMapValue = createMapValue
exports.upsertValueThen = upsertValueThen
exports.withMapDo = withMapDo
exports.withMapByNameDo = withMapByNameDo
exports.getMapId = getMapId
exports.withEntriesDo = withEntriesDo
exports.withEntryDo = withEntryDo
exports.withValueDo = withValueDo
exports.removeMapEntry = removeMapEntry
exports.removeMapValue = removeMapValue
exports.removeMapIndex = removeMapIndex
exports.deleteMapThen = deleteMapThen
exports.init = init