'use strict'
const cassandra = require('cassandra-driver')
var uuidgen = require('node-uuid')
const config = {
  databaseHosts: process.env.CASS_HOSTS ? process.env.CASS_HOSTS.split(",") : ['localhost'],
  databasePort: process.env.CASS_PORT ? process.env.CASS_PORT : 9042,
  databaseKeyspace: process.env.CASS_KEYSPACE || 'kvm',
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
const SEPARATOR = '?'


// inserts
const insertNamespace = 'INSERT INTO ' + config.databaseKeyspace + '.namespaces (namespace, mapid, mapname) VALUES (?, ?, ?)'
const insertMap = 'INSERT INTO ' + config.databaseKeyspace + '.maps (mapid, map, etag) VALUES (?, ?, ?)'
const insertIndex = 'INSERT INTO ' + config.databaseKeyspace + '.indexes (name, mapid) VALUES (?, ?)'
const insertMapEntry = 'INSERT INTO ' + config.databaseKeyspace + '.entries (mapid, key, data, etag) VALUES (?, ?, ?, ?)'
const insertMapValue = 'INSERT INTO ' + config.databaseKeyspace + '.values (mapid, key, data, value, etag) VALUES (? ,?, ?, ?, ?)'

// selects
const selectNamespace = 'SELECT * FROM ' + config.databaseKeyspace + '.namespaces WHERE namespace = ?'
const selectMap = 'SELECT * FROM ' + config.databaseKeyspace + '.maps WHERE mapid = ?'
const selectmapIdFromName = 'SELECT mapid FROM ' + config.databaseKeyspace + '.indexes WHERE name = ?'
const selectEntries = 'SELECT * FROM ' + config.databaseKeyspace + '.entries WHERE mapid = ?'
const selectEntry = 'SELECT * FROM ' + config.databaseKeyspace + '.entries WHERE mapid = ? and key = ?'
const selectValue = 'SELECT data, value, etag FROM ' + config.databaseKeyspace + '.values WHERE mapid = ? and key = ?'

//deletes
const deleteMapNamespace = 'DELETE FROM ' + config.databaseKeyspace + '.namespaces WHERE namespace = ? and mapid = ?'
const deleteMap = 'DELETE FROM ' + config.databaseKeyspace + '.maps WHERE mapid = ?'
const deleteMapIndex = 'DELETE FROM ' + config.databaseKeyspace + '.indexes WHERE name = ? and mapid = ?'
const deleteMapEntry = 'DELETE FROM ' + config.databaseKeyspace + '.entries WHERE mapid = ? and key = ?'
const deleteMapValue = 'DELETE FROM ' + config.databaseKeyspace + '.values WHERE mapid = ? and key = ?'


function createMapThen(mapId, map, callback) {
  var etag = uuidgen.v4()
  map.namespace = map.namespace ? map.namespace : null // default map namespace to null if it doesn't exist
  map.name = map.name ? map.name : null // default map name to null if it doesn't exist
  const batchQueries = [
    {query: insertMap, params: [cassandra.types.Uuid.fromString(mapId), JSON.stringify(map), etag]},
  ]
  if (map && map.name && !map.namespace)
    batchQueries.push({query: insertIndex, params: [map.name, cassandra.types.Uuid.fromString(mapId)]})
  if (map.namespace)
    batchQueries.push({query: insertNamespace, params: [map.namespace, cassandra.types.Uuid.fromString(mapId), map.name]})
    if (map.name) // add index entry for namespace:mapname if mapname wasn't empty
      batchQueries.push({query: insertIndex, params: [getMapName(map.namespace, map.name), cassandra.types.Uuid.fromString(mapId)]})
  client.batch(batchQueries, {prepare: true}, function (err, result) {
    if (err) {
      callback(err)
    }
    else {
      //console.log('Added map with namespace=' + namespace + ', uuid=' + uuid + ', map=' + JSON.stringify(map)+', etag='+etag)
      callback(null, etag) // inserts do not generate result rows
    }

  })
}

/**
 * A bit confusing on the updating of a map due to possible name and/or namespace changes and
 * separate maps,indexes,name tables that need to be updated.
 *
 *  ** An atomic batch statement is used so it all succeeds or all fails. ** //todo test this somehow
 *
 * Use cases handled:
 *
 * 1. Namespace change
 *    a. name added
 *    b. name removed
 *    c. name changed
 * 2. Name changes
 *    a. namespace added
 *    b. namespace removed
 *    c. namespace unchanged
 * 3. Name added
 *    a. namespace added
 *    b. namespace removed
 *    c. namespace unchanged
 * 4. Name removed
 *    a. namespace added
 *    b. namespace removed
 *    c. namespace unchanged
 *
 */
function updateMapThen(mapId, patchedMap, callback) {
  var etag = uuidgen.v4()
  const batchQueries = [
    {query: insertMap, params: [cassandra.types.Uuid.fromString(mapId), JSON.stringify(patchedMap), etag]},
  ]
  withMapDo(mapId, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else {
      // handle namespace changes
      if (patchedMap.namespace && map.namespace && patchedMap.namespace !== map.namespace) { // TODO double check all cases are covered
        // if name is new to the map, insert the name index
        if (patchedMap.name && !map.name) {
          batchQueries.push({query: insertIndex, params: [getMapName(patchedMap.namespace, patchedMap.name), mapId]})
        }
        // if name is being removed from the map, remove the name index
        if (!patchedMap.name && map.name) {
          batchQueries.push({query: deleteMapIndex, params: [getMapName(map.namespace, map.name), mapId]})
        }
        // if name is existed and being changed, we need to update the index
        if (patchedMap.name && map.name && patchedMap.name !== map.name) {
          batchQueries.push({query: deleteMapIndex, params: [getMapName(map.namespace, map.name), mapId]})
          batchQueries.push({query: insertIndex, params: [getMapName(patchedMap.namespace, patchedMap.name), mapId]})
        }
        // always update the namespace table
        batchQueries.push({query: deleteMapNamespace, params: [map.namespace, mapId]})
        batchQueries.push({query: insertNamespace, params: [patchedMap.namespace, mapId, patchedMap.name ? patchedMap.name : null]})
      }
      // handle name changes
      else if (patchedMap.name && map.name && patchedMap.name !== map.name) {
        // if namespace is new to the map, insert the index and namespace table, remove the old index w/o namespace
        if (patchedMap.namespace && !map.namespace) {
          batchQueries.push({query: insertIndex, params: [getMapName(patchedMap.namespace, patchedMap.name), mapId]})
          batchQueries.push({query: insertNamespace, params: [patchedMap.namespace, mapId, patchedMap.namespace]})
          batchQueries.push({query: deleteMapIndex, params: [map.name, mapId]})
        }
        // if namespace is being removed from the map, remove from namespace table, index, and write index w/o namespace
        else if (!patchedMap.namespace && map.namespace) {
          batchQueries.push({query: deleteMapNamespace, params: [map.namespace, mapId]})
          batchQueries.push({query: deleteMapIndex, params: [getMapName(map.namespace, map.name), mapId]})
          batchQueries.push({query: insertIndex, params: [patchedMap.name, mapId]})
        }
        // namespace isn't touched
        else {
          batchQueries.push({query: deleteMapIndex, params: [map.name, mapId]})
          batchQueries.push({query: insertIndex, params: [patchedMap.name, mapId]})
        }
      }
      // handle name removals
      else if (!patchedMap.name && map.name) {
        // if namespace is new to the map, insert the index and namespace table
        if (patchedMap.namespace && !map.namespace) {
          batchQueries.push({query: insertNamespace, params: [patchedMap.namespace, mapId, '']})
          batchQueries.push({query: deleteMapIndex, params: [map.name, mapId]})
        }
        // if namespace is being removed from the map, remove from namespace table, index, and write index w/o namespace
        else if (!patchedMap.namespace && map.namespace) {
          batchQueries.push({query: deleteMapNamespace, params: [map.namespace, mapId]})
          batchQueries.push({query: deleteMapIndex, params: [getMapName(map.namespace, map.name), mapId]})
        }
        // namespace is unchanged, remove the index as we no longer have a map name and update namespace table too
        else {
          batchQueries.push({query: deleteMapIndex, params: [getMapName(map.namespace, map.name), mapId]})
          batchQueries.push({query: insertNamespace, params: [patchedMap.namespace, mapId, '']})
        }
      }
      // handle name addition
      else if (patchedMap.name && !map.name) {
        // if namespace is new to the map, insert the index and namespace table
        if (patchedMap.namespace && !map.namespace) {
          batchQueries.push({query: insertNamespace, params: [patchedMap.namespace, mapId, patchedMap.name]})
          batchQueries.push({query: insertIndex, params: [getMapName(patchedMap.namespace, patchedMap.name), mapId]})
        }
        // if namespace is being removed from the map, remove from namespace table, there should be no indexes to remove
        else if (!patchedMap.namespace && map.namespace) {
          batchQueries.push({query: deleteMapNamespace, params: [map.namespace, mapId]})
        }
        // namespace is unchanged, remove the index as we no longer have a map name and update namespace table too
        else if (patchedMap.namespace && map.namespace) {
          batchQueries.push({query: insertIndex, params: [getMapName(patchedMap.namespace, patchedMap.name), mapId]})
          batchQueries.push({query: insertNamespace, params: [patchedMap.namespace, mapId, patchedMap.name]})
        }
      }
    }

    client.batch(batchQueries, {prepare: true}, function (err, result) {
      if (err) {
        callback(err)
      }
      else {
        callback(null, etag) // inserts do not generate result rows
      }
    })
  })
}


function createEntryThen(mapId, key, entry, callback) {
  var etag = uuidgen.v4()
  client.execute(insertMapEntry, [cassandra.types.Uuid.fromString(mapId), key, JSON.stringify(entry), etag], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else {
      callback(null, etag) // inserts do not generate result rows
    }

  })
}

function upsertValueThen(mapId, key, data, value, callback) {
  createMapValue(mapId, key, data, value, callback)
}

function createMapValue(mapId, key, data, value, callback) {
  if (!(value instanceof Buffer)) {
    callback(true, {error: 'Value must be a Buffer'})
  }
  var etag = uuidgen.v4()
  client.execute(insertMapValue, [cassandra.types.Uuid.fromString(mapId), key, JSON.stringify(data), value, etag], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else {
      callback(null, etag) // inserts do not generate result rows
    }

  })
}


function withMapDo(mapId, callback) {
  client.execute(selectMap, [cassandra.types.Uuid.fromString(mapId)], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else
      var map, etag;
      if(result.rows.length > 0){
        map = result.rows[0].get('map')
        etag = result.rows[0].get('etag')
      }
      map = JSON.parse(map)
      if(map.name === null)
        delete map.name
      if(map.namespace === null)
        delete map.namespace
      callback(null, map, etag)

  })
}

function withMapByNameDo(namespace, name, callback) {
  getMapId(namespace, name, function(err, mapId){
    if (err)
      callback(err)
    else{
     withMapDo(mapId, function (err, map, etag) {
        if (err)
          callback(err)
        else if (map == null)
          callback(404)
        else
          callback(null, map, mapId, etag)
      })
    }
  })
}


function getMapId(namespace, name, callback) {
  var indexName = getMapName(namespace,name)
  client.execute(selectmapIdFromName, [indexName], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else if ( result.rows.length > 1)
      callback(true, {error:'More than one UUID mapped to map name'})
    else
      callback(null, result.rows.length > 0 ? result.rows[0].get('mapid').toString() : null)
  })
}


function withEntriesDo(mapId, callback) {
  client.execute(selectEntries, [cassandra.types.Uuid.fromString(mapId)], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else
      if(result.rows.length > 0){
        result.rows.forEach(function(entry){
          entry.data = JSON.parse(entry.data)
        })
      }
      callback(null, result.rows.length > 0 ? result.rows : null)
  })
}


function withEntryDo(mapId, key, callback) {
  client.execute(selectEntry, [cassandra.types.Uuid.fromString(mapId), key], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else
      var key, etag, data
      if(result.rows.length > 0){
        key = result.rows[0].get('key')
        data = result.rows[0].get('data') ? JSON.parse(result.rows[0].get('data')) : null
        etag = result.rows[0].get('etag')
      }
      callback(null, key, data, etag)
  })
}


function withValueDo(mapId, key, callback) {
  client.execute(selectValue, [cassandra.types.Uuid.fromString(mapId), key], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else
      var value, data, etag
      if(result.rows.length > 0){
        value = result.rows[0].get('value')
        data = result.rows[0].get('data')
        etag = result.rows[0].get('etag')
      }
      callback(null, JSON.parse(data), value, etag)
  })
}


function removeMapEntry(mapId, key, callback) {
  client.execute(deleteMapEntry, [cassandra.types.Uuid.fromString(mapId), key], {prepare: true}, function (err, result) {
    if (err)
      console.error('Error removing entry for mapid= ' + mapId + ', key=' + key)
    else
      callback(null, result)
  })

}


function removeMapValue(mapId, key, callback) {
  client.execute(deleteMapValue, [cassandra.types.Uuid.fromString(mapId), key], {prepare: true}, function (err, result) {
    if (err)
      console.error('Error removing value for mapid= ' + mapId + ', key=' + key)
    else
      callback(null)
  })

}


function removeMapIndex(namespace, name, mapId, callback) {
  if (namespace && namespace !== '')
    name = namespace + SEPARATOR + name
  client.execute(deleteMapIndex, [name, cassandra.types.Uuid.fromString(mapId)], {prepare: true}, function (err, result) {
    if (err) {
      console.log(err)
      console.error('Error removing map index for namespace=' + namespace + ', name= ' + name)
    }
    else
      callback(null)
  })
}

function getMapsFromNamespace(namespace, callback) {
  client.execute(selectNamespace, [namespace], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else
      callback(null, result.rows.length > 0 ? result.rows : null)
  })
}

function removeMapFromNamespace(namespace, mapId, callback) {
  client.execute(deleteMapNamespace, [namespace, cassandra.types.Uuid.fromString(mapId)], {prepare: true}, function (err, result) {
    if (err)
      console.error('Error removing map from  namespace=' + namespace + ', map name= ' + name)
    else
      callback(null)
  })
}

/**
 * 1. Delete all entries and values for the given map
 * 2. Load map to obtain its namespace
 * 3. Delete the map
 * 4. Delete the map indexes
 * 5. Remove the map from the namespace
 */
function deleteMapThen(mapId, callback) {
  client.eachRow(selectEntries, [mapId], function (n, row) {
      //the callback will be invoked per each row as soon as they are received
      removeMapEntry(row.get('mapid').toString(), row.get('key'), function (err, result) {})
      removeMapValue(row.get('mapid').toString(), row.get('key'), function (err, result) {})
    },
    function (err) {
      if (err)
        callback(err)
      else {
        var id = mapId;
        withMapDo(mapId, function (err, map, etag) {
          if (err)
            callback(err)
          else {
            var oMap = map ? JSON.parse(map) : null
            var name = oMap ? oMap.name : null
            var namespace = oMap ? oMap.namespace : null

            client.execute(deleteMap, [mapId], {prepare: true}, function (err, deleteResult) {
              if (err)
                console.error('Error removing map index for mapid= ' + mapId)
              else {
                // remove indexes
                if (name)
                  removeMapIndex(null, name, mapId, function (err, result) {})
                if (namespace)
                  removeMapIndex(namespace, name, mapId, function (err, result) {})
                // remove association to namespace
                removeMapFromNamespace(namespace, mapId, function (err) {
                  if (err)
                    callback(err)
                  else
                    callback(null)
                })
              }
            })
          }
        })
      }
    })
}

function getMapName(namespace, name){
  if (namespace && namespace !== '')
    name = namespace + SEPARATOR + name
  return name;
}

function init(callback) {

  var keyspace = "CREATE KEYSPACE IF NOT EXISTS " + config.databaseKeyspace + " WITH replication = {'class': 'NetworkTopologyStrategy', " + config.databaseReplication + "}  AND durable_writes = true"
  var namespaces = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".namespaces ( namespace text, mapid uuid, mapname text, PRIMARY KEY ((namespace), mapid ))"
  var maps = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".maps ( mapid uuid, map text, etag text, PRIMARY KEY (( mapid )))"
  var names = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".indexes ( name text, mapid uuid, PRIMARY KEY ((name), mapid))"
  var entries = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".entries (mapid uuid, key text, data text, etag text, PRIMARY KEY ((mapid), key)) WITH CLUSTERING ORDER BY (key ASC)"
  var values = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".values (mapid uuid, key text, data text, value blob, etag text, PRIMARY KEY ((mapid, key)))"

  client.execute(keyspace, {}, function (err, result) {
    if (err)
      callback(err)
    else {
      client.execute(namespaces, {}, function (err, result) {
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
exports.getMapsFromNamespace = getMapsFromNamespace
exports.init = init