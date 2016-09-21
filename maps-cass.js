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
const insertNamespace = 'INSERT INTO ' + config.databaseKeyspace + '.namespaces (namespace, mapuuid, mapname) VALUES (?, ?, ?)'
const insertMap = 'INSERT INTO ' + config.databaseKeyspace + '.maps (mapuuid, map, etag) VALUES (?, ?, ?)'
const insertIndex = 'INSERT INTO ' + config.databaseKeyspace + '.indexes (name, mapuuid) VALUES (?, ?)'
const insertMapEntry = 'INSERT INTO ' + config.databaseKeyspace + '.entries (mapuuid, key, etag) VALUES (?, ?, ?)'
const insertMapValue = 'INSERT INTO ' + config.databaseKeyspace + '.values (mapuuid, key, type, value, etag) VALUES (? ,?, ?, ?, ?)'

// selects
const selectNamespace = 'SELECT * FROM ' + config.databaseKeyspace + '.namespaces WHERE namespace = ?'
const selectMap = 'SELECT * FROM ' + config.databaseKeyspace + '.maps WHERE mapuuid = ?'
const selectMapUuidFromName = 'SELECT mapuuid FROM ' + config.databaseKeyspace + '.indexes WHERE name = ?'
const selectEntries = 'SELECT * FROM ' + config.databaseKeyspace + '.entries WHERE mapuuid = ?'
const selectEntry = 'SELECT * FROM ' + config.databaseKeyspace + '.entries WHERE mapuuid = ? and key = ?'
const selectValue = 'SELECT type, value, etag FROM ' + config.databaseKeyspace + '.values WHERE mapuuid = ? and key = ?'

//deletes
const deleteMapNamespace = 'DELETE FROM ' + config.databaseKeyspace + '.namespaces WHERE namespace = ? and mapuuid = ?'
const deleteMap = 'DELETE FROM ' + config.databaseKeyspace + '.maps WHERE mapuuid = ?'
const deleteMapIndex = 'DELETE FROM ' + config.databaseKeyspace + '.indexes WHERE name =?'
const deleteMapEntry = 'DELETE FROM ' + config.databaseKeyspace + '.entries WHERE mapuuid = ? and key = ?'
const deleteMapValue = 'DELETE FROM ' + config.databaseKeyspace + '.values WHERE mapuuid = ? and key = ?'


function createMap(namespace, mapuuid, map, callback) {
  var etag = uuidgen.v4()
  map.name = map.name ? map.name : '' // default map name to empty string if it doesn't exist
  const batchQueries = [
    {query: insertMap, params: [cassandra.types.Uuid.fromString(mapuuid), JSON.stringify(map), etag]},
  ]
  if (map && map.name && !namespace)
    batchQueries.push({query: insertIndex, params: [map.name, cassandra.types.Uuid.fromString(mapuuid)]})
  if (namespace)
    batchQueries.push({query: insertNamespace, params: [namespace, cassandra.types.Uuid.fromString(mapuuid), map.name]})
    if (map.name !== '') // add index entry for namespace:mapname if mapname wasn't empty
      var nsName = namespace + SEPARATOR + map.name
      batchQueries.push({query: insertIndex, params: [nsName, cassandra.types.Uuid.fromString(mapuuid)]})
  client.batch(batchQueries, {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else {
      //console.log('Added map with namespace=' + namespace + ', uuid=' + uuid + ', map=' + JSON.stringify(map)+', etag='+etag)
      callback(null, etag) // inserts do not generate result rows
    }

  })
}


function updateMap(mapuuid, patchedMap, callback) {
  var etag = uuidgen.v4()
  client.execute(insertMap, [cassandra.types.Uuid.fromString(mapuuid), JSON.stringify(patchedMap), etag], {prepare: true}, function (err, result) {
    if (err) {
      callback(err)
    }
    else {
      callback(null, etag) // inserts do not generate result rows
    }

  })
}


function createMapEntry(mapuuid, key, callback) {
  var etag = uuidgen.v4()
  client.execute(insertMapEntry, [cassandra.types.Uuid.fromString(mapuuid), key, etag], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else {
      callback(null, etag) // inserts do not generate result rows
    }

  })
}


function createMapValue(mapuuid, key, contentType, value, callback) {
  if (!(value instanceof Buffer)) {
    callback(true, {error: 'Value must be a Buffer'})
  }
  var etag = uuidgen.v4()
  client.execute(insertMapValue, [cassandra.types.Uuid.fromString(mapuuid), key, contentType, value, etag], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else {
      callback(null, etag) // inserts do not generate result rows
    }

  })
}


function getMap(mapuuid, callback) {
  client.execute(selectMap, [cassandra.types.Uuid.fromString(mapuuid)], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else
      var map, etag;
      if(result.rows.length > 0){
        map = result.rows[0].get('map')
        etag = result.rows[0].get('etag')
      }
      callback(null, map, etag)

  })
}

function getMapByName(namespace, name, callback) {
  getMapUuid(namespace, name, function(err, mapuuid){
    if (err)
      callback(err)
    else{
     getMap(mapuuid, function (err, map, etag) {
        if (err)
          callback(err)
        else
          callback(null, map, etag)
      })
    }
  })
}


function getMapUuid(namespace, name, callback) {
  var indexName = getMapName(namespace,name)
  client.execute(selectMapUuidFromName, [indexName], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else if ( result.rows.length > 1)
      callback(true, {error:'More than one UUID mapped to map name'})
    else
      callback(null, result.rows.length > 0 ? result.rows[0].get('mapuuid').toString() : null)
  })
}


function getMapEntries(mapuuid, callback) {
  client.execute(selectEntries, [cassandra.types.Uuid.fromString(mapuuid)], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else
      callback(null, result.rows.length > 0 ? result.rows : null)
  })
}


function getMapEntry(mapuuid, key, callback) {
  client.execute(selectEntry, [cassandra.types.Uuid.fromString(mapuuid), key], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else
      var key, etag
      if(result.rows.length > 0){
        key = result.rows[0].get('key')
        etag = result.rows[0].get('etag')
      }
      callback(null, key, etag)
  })
}


function getMapValue(mapuuid, key, callback) {
  client.execute(selectValue, [cassandra.types.Uuid.fromString(mapuuid), key], {prepare: true}, function (err, result) {
    if (err)
      callback(err)
    else
      var value, type, etag
      if(result.rows.length > 0){
        value = result.rows[0].get('value')
        type = result.rows[0].get('type')
        etag = result.rows[0].get('etag')
      }
      callback(null, value, type, etag)
  })
}


function removeMapEntry(mapuuid, key, callback) {
  client.execute(deleteMapEntry, [mapuuid, key], {prepare: true}, function (err, result) {
    if (err)
      console.error('Error removing entry for mapuuid= ' + mapuuid + ', key=' + key)
    else
      callback(null, result)
  })

}


function removeMapValue(mapuuid, key, callback) {
  client.execute(deleteMapValue, [mapuuid, key], {prepare: true}, function (err, result) {
    if (err)
      console.error('Error removing value for mapuuid= ' + mapuuid + ', key=' + key)
    else
      callback(null)
  })

}


function removeMapIndex(namespace, name, callback) {
  if (namespace && namespace !== '')
    name = namespace + SEPARATOR + name
  client.execute(deleteMapIndex, [name], {prepare: true}, function (err, result) {
    if (err)
      console.error('Error removing map index for namespace=' + namespace + ', name= ' + name)
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

function removeMapFromNamespace(namespace, mapuuid, callback) {
  client.execute(deleteMapNamespace, [namespace, mapuuid], {prepare: true}, function (err, result) {
    if (err)
      console.error('Error removing map from  namespace=' + namespace + ', map name= ' + name)
    else
      callback(null)
  })
}

function removeMap(namespace, mapuuid, callback) {
  client.eachRow(selectEntries, [mapuuid], function (n, row) {
      //the callback will be invoked per each row as soon as they are received
      removeMapEntry(row.get('mapuuid'), row.get('key'), function (err, result) {
      })
      removeMapValue(row.get('mapuuid'), row.get('key'), function (err, result) {
      })
    },
    function (err) {
      if (err)
        callback(err)
      else {
        getMap(mapuuid, function (err, map, etag) {
          if (err)
            callback(err)
          else {
            if (map && JSON.parse(map).name) {
              var name = JSON.parse(map).name
              removeMapIndex(null, name, function (err, result) {
              })
              removeMapIndex(namespace, name, function (err, result) {
              })
            }
            client.execute(deleteMap, [mapuuid], {prepare: true}, function (err, deleteResult) {
              if (err)
                console.error('Error removing map index for mapuuid= ' + mapuuid)
              else
                removeMapFromNamespace(namespace, mapuuid, function(err){
                  if (err)
                    callback(err)
                  else
                    callback(null)
                })
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
  var namespaces = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".namespaces ( namespace text, mapuuid uuid, mapname text, PRIMARY KEY ((namespace), mapuuid ))"
  var maps = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".maps ( mapuuid uuid, map text, etag text, PRIMARY KEY (( mapuuid )))"
  var names = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".indexes ( name text, mapuuid uuid, PRIMARY KEY ((name), mapuuid))"
  var entries = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".entries (mapuuid uuid, key text, etag text, PRIMARY KEY ((mapuuid), key)) WITH CLUSTERING ORDER BY (key ASC)"
  var values = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".values (mapuuid uuid, key text, type text, value blob, etag text, PRIMARY KEY ((mapuuid, key)))"

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

exports.createMap = createMap
exports.updateMap = updateMap
exports.createMapEntry = createMapEntry
exports.createMapValue = createMapValue
exports.getMap = getMap
exports.getMapByName = getMapByName
exports.getMapUuid = getMapUuid
exports.getMapEntries = getMapEntries
exports.getMapEntry = getMapEntry
exports.getMapValue = getMapValue
exports.removeMapEntry = removeMapEntry
exports.removeMapValue = removeMapValue
exports.removeMapIndex = removeMapIndex
exports.removeMap = removeMap
exports.getMapsFromNamespace = getMapsFromNamespace
exports.init = init