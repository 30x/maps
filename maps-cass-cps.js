'use strict'
const cassandra = require('cassandra-driver')
const kvmEntryPartitionCount = process.env.MAPS_KVM_ENTRY_PARTITION_COUNT || 32
const config = {
  databaseHosts: process.env.CASS_HOSTS ? process.env.CASS_HOSTS.split(",") : ['localhost'],
  databasePort: process.env.CASS_PORT ? process.env.CASS_PORT : 9042,
  databaseKeyspace: process.env.CASS_KEYSPACE || 'kvm_free1_us_west_2',
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

const lib = require('http-helper-functions')
const SEPARATOR = ':'


// inserts
function insertMap(keyspace){ return 'INSERT INTO ' + keyspace + '.kvm_map_descriptor (tid, a_scp, map, attr, c_at, c_by, u_at, u_by, ver) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'}
function insertMapEntry(keyspace){ return  'INSERT INTO ' + keyspace + '.kvm_map_keys_descriptor (tid, a_scp, map, ver, part_id, key, c_at, c_by, u_at, u_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'}
function insertMapValue(keyspace){ return 'INSERT INTO ' + keyspace + '.kvm_map_entry (tid, a_scp, map, ver, key, c_at, c_by, u_at, u_by, val) VALUES (? ,?, ?, ?, ?, ?, ?, ?, ?, ?)'}

// selects
function selectMap(keyspace ){ return 'SELECT * FROM ' + keyspace + '.kvm_map_descriptor WHERE tid = ? and a_scp = ? and map = ?'}
function selectEntries(keyspace){ return 'SELECT * FROM ' + keyspace + '.kvm_map_keys_descriptor WHERE tid = ? and a_scp = ? and map = ? and ver = ? and part_id = ?'}
function selectEntry(keyspace){ return 'SELECT * FROM ' + keyspace + '.kvm_map_keys_descriptor WHERE tid = ? and a_scp = ? and map = ? and ver = ? and part_id = ? and key = ?'}
function selectValue(keyspace){ return 'SELECT * FROM ' + keyspace + '.kvm_map_entry WHERE tid = ? and a_scp = ? and map = ? and ver = ? and key = ?'}

//deletes
function deleteMap(keyspace){ return 'DELETE FROM ' + keyspace + '.kvm_map_descriptor WHERE tid = ? and a_scp = ? and map = ?'}
function deleteMapEntry(keyspace){ return 'DELETE FROM ' + keyspace + '.kvm_map_keys_descriptor WHERE tid = ? and a_scp = ? and map = ? and ver = ? and part_id = ? and key = ?'}
function deleteMapValue(keyspace){ return 'DELETE FROM ' + keyspace + '.kvm_map_entry WHERE tid = ? and a_scp = ? and map = ? and ver = ? and key = ?'}


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
  if( map.org && map.name){
    //console.log('makeMapID org: '+map.org)
    var orgParts = map.org.split("/")
    map.org = orgParts[orgParts.length -1] // for some reason this has scheme and authority on the org name for permissions
    getTenantAndKeyspace(map.org, function(err, ring, keyspace, tenantID){
      if(err)
        callback(`Failed to make mapID from org and name. Unable to get keyspace and tenant for org: `+map.org)
      else{
        let parts = map.name.split(':')
        let scope = parts.slice(0, -1).join(':')
        let name = parts[parts.length - 1]
        let version = cassandra.types.TimeUuid.now().toString()
        callback(null, getMapID(ring, keyspace, tenantID, scope, name, version))
      }
    })
  }
  else
    callback(null, uuid())
}

function createMapThen(mapID, map, callback) {
  getMapIDParts(mapID, function(err, ring, keyspace, tenantID, scope, name, version){
   if(err)
     callback(err)
   else{
     const batchQueries = [
       {query: insertMap(keyspace), params: [tenantID, scope, name, {'mapFullName': map.fullName }, map.created, map.creator, map.created, map.creator, cassandra.types.Uuid.fromString(version)]},
     ]
     client.batch(batchQueries, {prepare: true}, function (err, result) {
       if (err)
         callback(err)
       else {
         //console.log('Added map=' + JSON.stringify(map)+')
         callback(null, null) // inserts do not generate result rows
       }
     })
   }
  })
}

function updateMapThen(mapID, patchedMap, callback) {
  // in Cassandra the insert will overwrite
  createMapThen(mapID, patchedMap, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else
      callback(null,null)
  })
}


function createEntryThen(mapID, key, entry, callback) {
    getMapIDParts(mapID, function(err, ring, keyspace, tenantID, scope, name, version){
      client.execute(insertMapEntry(keyspace), [tenantID, scope, name, cassandra.types.Uuid.fromString(version), getMapEntryPartitionId(key), key, entry.created, entry.creator, entry.created, entry.creator], {prepare: true}, function (err, result) {
        if (err)
          callback(err)
        else {
          callback(null, null) // inserts do not generate result rows
        }
      })
    })

}

function upsertValueThen(mapID, key, valuedata, value, callback) {
  createMapValue(mapID, key, valuedata, value, callback)
}

function createMapValue(mapID, key, valuedata, value, callback) {
  if (!(value instanceof String)) {
    value = value.toString()
    //callback(true, {error: 'Value must be a string'})
  }
  getMapIDParts(mapID, function(err, ring, keyspace, tenantID, scope, name, version){
    // purposely need two separate calls to cassandra as you can't execute a batch with 2 partitions ( tables ) with the condition IF NOT EXISTS
    client.execute(insertMapValue(keyspace), [tenantID, scope, name, cassandra.types.Uuid.fromString(version), key, valuedata.created, valuedata.creator, valuedata.created, valuedata.creator, value], {prepare: true}, function (err, result) {
      if (err)
        callback(err)
      else {
        client.execute(insertMapEntry(config.databaseKeyspace) + 'IF NOT EXISTS',  [tenantID, scope, name, cassandra.types.Uuid.fromString(version), getMapEntryPartitionId(key), key, valuedata.created, valuedata.creator, valuedata.created, valuedata.creator], {prepare: true}, function (err, result) {
          if (err)
            callback(err)
          else
            callback(null, null) // inserts do not generate result rows
        })
      }
    })

  })
}

function getMapIDParts(mapID, callback){
  var parts = mapID.split(':')
  if(parts < 2 )
    callback(400)
  else {
    let ring = parts[0]
    let keyspace = parts[1]
    let tenantID = parts[2]
    let scope = parts.slice(3, -2).join(':')
    let name = parts[parts.length - 2]
    let version = parts[parts.length - 1]
    callback(null, ring, keyspace, tenantID, scope, name, version)
  }
}

function withMapDo(mapID, callback) {
  getMapIDParts(mapID, function(err, ring, keyspace, tenantID, scope, name, version){
    if(err)
      callback(err)
    else {
      client.execute(selectMap(keyspace), [tenantID, scope, name], {prepare: true}, function (err, result) {
        if (err)
          callback(err, null, null)
        else if (result.rows.length === 0)
          callback(404, null, null)
        else {
          var map = {}
          if (result.rows.length > 0) {
            map.id = getMapID(ring, keyspace, result.rows[0].get('tid'), result.rows[0].get('a_scp'), result.rows[0].get('map'), result.rows[0].get('ver').toString())
            var scope = result.rows[0].get('a_scp')
            var mapName = result.rows[0].get('map')
            if (scope){
              mapName = scope + ":" + mapName
            }
            map.name = mapName
            map.created = result.rows[0].get('c_at')
            map.createdBy = result.rows[0].get('c_by')
            map.updated = result.rows[0].get('u_at')
            map.updatedBy = result.rows[0].get('u_by')
            map.version = result.rows[0].get('ver').toString()
            var mapAttrs =  result.rows[0].get('attr')
            if (mapAttrs.mapFullName)
              map.fullName = mapAttrs.mapFullName
              map.org = '/v1/o/'+mapAttrs.mapFullName.split(":")[0]
          }
          callback(null, map, null)
        }
      })
    }
  })
}

function withMapFromNameDo(compoundName, callback) {
  var parts = compoundName.split(':')
  if (parts.length < 2)
    callback(400)
  else {
    let org = parts[0]
    let scope = parts.slice(1, -1).join(':')
    let name = parts[parts.length - 1]
    //console.log('withMapFromNameDo org: '+org)
    getTenantAndKeyspace(org, function(err, ring, keyspace, tenantID){
      if(err)
        callback(`Unable to load map from name. Failed to get keyspace and tenant for org: `+org)
      else{
        var mapID = getMapID(ring, keyspace, tenantID, scope, name, "version_not_used_when_looking_up_map")
        withMapDo(mapID, function (err, map, etag) {
          if (err)
            callback(err)
          else if (map == null)
            callback(404)
          else {
            callback(null, map.id, map, etag)
          }
        })
      }
    })
  }
}

function withEntriesDo(mapID, callback) {
  getMapIDParts(mapID, function(err, ring, keyspace, tenantID, scope, name, version){
    // todo page this and accept in a cursor string in withEntriesDo, will need to calculate part_id with getMapEntryPartitionId(key)
    client.execute(selectEntries(keyspace), [tenantID, scope, name, cassandra.types.Uuid.fromString(version), 0], {prepare: true}, function (err, result) {
      if (err)
        callback(err)
      else {
        if (result.rows.length > 0) {
          var processed = 0;
          result.rows.forEach(function (entry) {
            processed++
            var key = entry.key;
            var name = getMapID(ring, keyspace, entry.tid, entry.scope, entry.map, entry.ver);
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
  })
}


function withEntryDo(mapID, key, callback) {
  getMapIDParts(mapID, function(err, ring, keyspace, tenantID, scope, name, version){
    client.execute(selectEntry(keyspace), [tenantID, scope, name, cassandra.types.Uuid.fromString(version), getMapEntryPartitionId(key), key], {prepare: true}, function (err, result) {
      if (err)
        callback(err)
      else {
        var key, etag, entrydata
        if (result.rows.length > 0) {
          var name = getMapID(ring, keyspace, result.rows[0].get('tid'), result.rows[0].get('a_scp'), result.rows[0].get('map'), result.rows[0].get('ver'));
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
  })
}


function withValueDo(mapID, key, callback) {
  getMapIDParts(mapID, function(err, ring, keyspace, tenantID, scope, name, version){
    client.execute(selectValue(keyspace), [tenantID, scope, name, cassandra.types.Uuid.fromString(version), key], {prepare: true}, function (err, result) {
      if (err)
        callback(err)
      else {
        var value, valuedata, etag
        if (result.rows.length > 0) {
          var name = getMapID(ring, keyspace, result.rows[0].get('tid'), result.rows[0].get('a_scp'), result.rows[0].get('map'), result.rows[0].get('ver'));
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
  })
}


function removeMapEntry(mapID, key, callback) {
  getMapIDParts(mapID, function(err, ring, keyspace, tenantID, scope, name, version){
    client.execute(deleteMapEntry(keyspace), [tenantID, scope, name, cassandra.types.Uuid.fromString(version), getMapEntryPartitionId(key), key], {prepare: true}, function (err, result) {
      if (err)
        console.error('Error removing entry for mapid= ' + mapID + ', key=' + key)
      else
        callback(null, result)
    })
  })
}


function removeMapValue(mapID, key, callback) {
  getMapIDParts(mapID, function(err, ring, keyspace, tenantID, scope, name, version){
    client.execute(deleteMapValue(keyspace), [tenantID, scope, name, cassandra.types.Uuid.fromString(version), key], {prepare: true}, function (err, result) {
      if (err)
        console.error('Error removing value for mapid= ' + mapID + ', key=' + key)
      else
        callback(null)
    })
  })
}


function getMapID(ring, keyspace, tenantID, scope, name, version){
  var id = ring + SEPARATOR + keyspace + SEPARATOR + tenantID
  if( scope )
    id += SEPARATOR + scope + SEPARATOR + name + SEPARATOR + version
  else
    id += SEPARATOR + name + SEPARATOR + version
  return id
}

function getMapEntryPartitionId(key){
  return Math.abs(javaStringHashCode(key) % kvmEntryPartitionCount )
}


/**
 * 1. Delete all entries and values for the given map
 * 2. Load map to obtain its version
 * 3. Delete the map
 */
function deleteMapThen(mapID, callback) {
  withMapDo(mapID, function (err, map, etag) {
    if (err)
      callback(err)
    else if (map == null)
      callback(404)
    else{
      // todo just write entry into Perses table and bump map version?
      getMapIDParts(mapID, function(err, ring, keyspace, tenantID, scope, name, version){
        client.eachRow(selectEntries(keyspace), [tenantID, scope, name, cassandra.types.Uuid.fromString(version), 0], {prepare: true}, function (n, row) {
          //the callback will be invoked per each row as soon as they are received
          removeMapEntry(getMapID(ring, keyspace, row.get('tid'), row.get('a_scp'), row.get('map'), row.get('ver')), row.get('key'), function (err, result) {})
          removeMapValue(getMapID(ring, keyspace, row.get('tid'), row.get('a_scp'), row.get('map'), row.get('ver')), row.get('key'), function (err, result) {})
        }, function (err) {
          if (err)
            callback(err, null, null)
          else {
            client.execute(deleteMap(config.databaseKeyspace), [tenantID, scope, name], {prepare: true}, function (err, deleteResult) {
              if (err) {
                console.error('Error removing map for mapid= ' + mapID)
                callback(err, null, null)
              }
              else {
                callback(null, map, null)
              }
            })
          }
        })
      })
    }
  })
}

// Start - Source of function: http://werxltd.com/wp/2010/05/13/javascript-implementation-of-javas-string-hashcode-method/
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
// End - Source of function: http://werxltd.com/wp/2010/05/13/javascript-implementation-of-javas-string-hashcode-method/


function init(callback) {

  var keyspace = "CREATE KEYSPACE IF NOT EXISTS " + config.databaseKeyspace + " WITH replication = {'class': 'NetworkTopologyStrategy', " + config.databaseReplication + "}  AND durable_writes = true"
  var maps = "CREATE TABLE IF NOT EXISTS " + config.databaseKeyspace + ".kvm_map_descriptor ( tid text, a_scp text, map text, attr map < text, text >, c_at timestamp, c_by text, u_at timestamp, u_by text, ver timeuuid, PRIMARY KEY ((tid, a_scp), map)) WITH CLUSTERING ORDER BY (map ASC)"
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

function getTenantAndKeyspace(orgName, callback){
  // todo what to actually send for host here because sendInternalRequest requires it
  lib.sendInternalRequest({host:process.env.INTERNAL_SY_ROUTER_HOST}, '/orgs;'+orgName, 'GET', null, {}, function(err, clientRes) {
    if (clientRes.statusCode !== 200)
      callback('unable to obtain tenantID and keyspace')
    else {
      var body = ''
      clientRes.on('data', function (d) {body += d})
      clientRes.on('end', function () {
        var org = JSON.parse(body)
        callback(null, org.kvmRing, org.kvmKeyspace, org.tenantID)
      })
    }
  })
}

// same as PG exported
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

// some extra stuff exported
exports.removeMapEntry = removeMapEntry
exports.removeMapValue = removeMapValue
